import numpy as np
import os
import re
import sys
import glob
import json
import socket
import time
import base64
import threading
import pika
import pickle
import src.Model
import src.Log
from ultralytics import YOLO

from src.Clustering import (
    ManualExperimentConfig,
    DeterministicSimilarityAssignmentSolver,
    run_manual_hungarian_case,
    print_result,
    get_cut_data_sizes,
    get_raw_input_mb,
)

_MAP_BACKEND_ERR = None   # set once, the first time the metric can't be built
_MAP_WARNED = set()       # messages already printed, so per-window calls don't spam


def _map_warn_once(msg, color="red"):
    if msg not in _MAP_WARNED:
        _MAP_WARNED.add(msg)
        src.Log.print_with_color(msg, color)


def _sum_dicts(dicts):
    """Element-wise sum of {key: number} mappings — used to pool the per-kind and
    per-reason free-time breakdowns of several devices into one."""
    out = {}
    for d in dicts:
        for k, v in (d or {}).items():
            out[k] = out.get(k, 0) + v
    return out


def _new_map_metric():
    """A fresh MeanAveragePrecision, or None (reason logged once) if mAP can't run.

    Three separate ways this fails, all of them handled here because each one
    otherwise surfaces late and destructively:

      1. torchmetrics not installed        -> ImportError on the import;
      2. no COCO backend installed         -> ImportError from the CONSTRUCTOR,
         not the import, so guarding only the import lets a fully finished run
         die at shutdown, losing the mAP and the clean disconnect;
      3. backend installed but not the one torchmetrics defaults to — it
         hardcodes backend="pycocotools" and only checks inside compute(), i.e.
         AFTER a whole run's worth of updates. So the backend is chosen here from
         what is actually importable.

    pycocotools is preferred when present (it's the reference implementation);
    faster-coco-eval is the drop-in fallback and needs no MSVC toolchain on
    Windows. They agree numerically, but keep the same one installed on every
    device so numbers stay strictly comparable.

    Each window needs its own metric (they accumulate state), so this runs many
    times per report — hence the cached failure flag."""
    global _MAP_BACKEND_ERR
    if _MAP_BACKEND_ERR is not None:
        return None
    try:
        from torchmetrics.detection import MeanAveragePrecision
    except ImportError as e:
        _MAP_BACKEND_ERR = str(e)
        _map_warn_once(f"[mAP] disabled — {e}")
        _map_warn_once("[mAP] fix: pip install torchmetrics faster-coco-eval", "yellow")
        return None

    import importlib.util
    backend = next((b for b in ("pycocotools", "faster_coco_eval")
                    if importlib.util.find_spec(b)), None)
    if backend is None:
        _MAP_BACKEND_ERR = "no COCO backend installed (pycocotools / faster-coco-eval)"
        _map_warn_once(f"[mAP] disabled — {_MAP_BACKEND_ERR}")
        _map_warn_once("[mAP] fix: pip install faster-coco-eval", "yellow")
        return None
    try:
        metric = MeanAveragePrecision(iou_type="bbox", backend=backend)
    except TypeError:
        # torchmetrics < 1.3 predates the `backend` kwarg (pycocotools only).
        metric = MeanAveragePrecision(iou_type="bbox")
    except ImportError as e:
        _MAP_BACKEND_ERR = str(e)
        _map_warn_once(f"[mAP] disabled — {e}")
        return None
    _map_warn_once(f"[mAP] backend: {backend}", "green")
    metric.warn_on_many_detections = False
    return metric


class Server:
    def __init__(self, config):
        # One-time cleanup of shared metrics/lock files from a previous run.
        # Must happen here (server starts once) — doing this in each Scheduler
        # caused later-starting clients to wipe out files already being
        # written by clients that started earlier.
        # Per-device free_time_<role>_<cluster>_<id>.log files are cleaned here for
        # the same reason: the client id is new every run, so a device that ran on
        # this filesystem last time would leave a file that no longer belongs to
        # anything and _archive_results would fold it into the new run.
        for f in (
            glob.glob("metrics_raw_*.csv")
            + glob.glob("metrics_pivoted_*.csv")
            + glob.glob("metrics_pivot_*.lock")
            + glob.glob("free_time_edge_*.log")
            + glob.glob("free_time_cloud_*.log")
            + ["detections_stream.jsonl"]
        ):
            if os.path.exists(f):
                try:
                    os.remove(f)
                except PermissionError:
                    src.Log.print_with_color(f"[!] Cannot delete {f} (file is open). Close it and retry.", "red")

        # map/pred is write-once per frame index (Scheduler._write_pred_file skips
        # a frame that already has a file), so leftovers from a previous run would
        # otherwise "win" forever and silently poison every future run's mAP.
        # map/pred_collected is this server's own scratch space, rebuilt every
        # shutdown collection (_collect_map_pred) — stale content there is
        # harmless but cleared anyway for a consistent fresh start.
        for d in ("map/pred", "map/pred_collected"):
            if os.path.isdir(d):
                import shutil
                shutil.rmtree(d, ignore_errors=True)

        self.config = config
        self.address = config["rabbit"]["address"]
        self.username = config["rabbit"]["username"]
        self.password = config["rabbit"]["password"]
        self.virtual_host = config["rabbit"]["virtual-host"]

        self.model_name = config["server"]["model"]
        self.total_clients = config["server"]["clients"]
        self.cut_layer = config["server"]["cut-layer"]
        self.batch_size = config["server"]["batch-size"]

        # Adaptive split-point controller (Mechanic 1). Runtime state built in
        # notify_clients once cut assignments are known.
        self.adaptive_cfg = config.get("adaptive", {})
        self.multithreading_cfg = config.get("multithreading", {})
        self.backpressure_cfg = config.get("backpressure", {})
        self.detections_cfg = config.get("detections", {})
        self.map_cfg = config.get("map", {})
        self.free_time_cfg = config.get("free_time", {})
        self.broker_ram_cfg = config.get("broker_ram", {})
        self.msg_size_cfg = config.get("message_size", {}) or {}
        # Which device measures published message size. Chosen in notify_clients
        # as the FIRST client that registered at layer 1, and told to measure via
        # its START message — no client decides this for itself.
        self._msg_size_client = None
        self.cluster_state = {}       # {queue_name: {"queue", "cut", "edges": [client_id,...]}}
        self._num_layers = None       # L, total model layers (for clamping the cut)
        self._adaptive_thread = None
        self._cut_sizes = None        # per-cut estimated message size (MB), size guard

        credentials = pika.PlainCredentials(self.username, self.password)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.address,
                port=5672,
                virtual_host=f"{self.virtual_host}",
                credentials=credentials,
                heartbeat=3600,
                blocked_connection_timeout=600
            )
        )
        self.channel = self.connection.channel()

        self._assert_only_server()

        self.channel.queue_declare(queue='rpc_queue', durable=False)
        self.channel.queue_purge(queue='rpc_queue')

        # Discard any messages left over from a previous (crashed) run, so
        # depth-based back-pressure starts from an empty queue instead of
        # being thrown off by stale large messages still sitting in RabbitMQ.
        self.channel.queue_declare(queue='intermediate_queue', durable=False)
        self.channel.queue_purge(queue='intermediate_queue')

        # FPS meter: whichever tier finishes a batch publishes one bare b"DONE" to
        # 'fps_queue'. We record the ARRIVAL TIME of every DONE (server clock only,
        # so device clock skew is irrelevant) and derive system throughput from
        # frames/time. Purge so a new run doesn't inherit stale DONEs.
        self.channel.queue_declare(queue='fps_queue', durable=False)
        self.channel.queue_purge(queue='fps_queue')

        # Utilization reports: each device computes ONE whole-run busy/total
        # ratio from its own timing log and publishes it to 'utilization_queue'
        # when it finishes. Reports sit on the broker until _collect_utilization
        # drains them at shutdown (a cloud publishes AFTER rpc_queue consuming
        # stopped, so a dedicated queue is required). Purge so a new run
        # discards stale reports from a crashed run.
        self.channel.queue_declare(queue='utilization_queue', durable=False)
        self.channel.queue_purge(queue='utilization_queue')

        # Free-time reports: each device merges every one of its lanes' work
        # intervals and publishes how much of its run was spent doing NOTHING
        # (no inference, no send, no compress/decompress, no postprocess). Same
        # collect-at-shutdown contract as utilization_queue; purged so a new run
        # can't inherit a crashed run's reports.
        self.channel.queue_declare(queue='freetime_queue', durable=False)
        self.channel.queue_purge(queue='freetime_queue')

        # Message-size report: the ONE designated edge (first registered at layer
        # 1) publishes the sizes of every message it put on the wire, measured
        # before publish. Same collect-at-shutdown contract as the two queues
        # above; purged so a new run can't inherit a crashed run's report.
        self.channel.queue_declare(queue='msgsize_queue', durable=False)
        self.channel.queue_purge(queue='msgsize_queue')

        # mAP pred files: whichever tier runs postprocess_yolo (last_layer/
        # only_edge) zips up its own map/pred/*.txt files and publishes them to
        # 'map_pred_queue' (tagged with its cluster id) when it finishes. The
        # server unpacks them per cluster at shutdown, matches against its own
        # local map/label/ ground truth, and runs TWO independent mAP pipelines
        # over the result (sliding window + all frames) into map.log /
        # map_window.log — see _collect_map_pred.
        self.channel.queue_declare(queue='map_pred_queue', durable=False)
        self.channel.queue_purge(queue='map_pred_queue')
        self._fps_times = []       # arrival time of every DONE (one per batch)
        # Same arrivals, bucketed by the cluster that produced them (the DONE body
        # carries the cluster id — see Scheduler._send_fps_done). The system list
        # above stays the authoritative total; this is purely an added breakdown,
        # so a mis-tagged DONE can never change the system number.
        self._fps_by_cluster = {}  # {cluster_id: [arrival_s, ...]}
        self._fps_start_t = None   # when START was broadcast (system-fps t0)
        self._fps_printed = False
        # Shutdown: after the edges finish the clouds keep draining their backlog,
        # so we DON'T exit on edge-done. We keep collecting while the work queues
        # still hold batches, plus a short grace for the last in-flight batch. A
        # hard cap guards against a peer that dies without draining.
        fps_cfg = config.get("fps", {})
        self._fps_grace_s = float(fps_cfg.get("grace_s", 10.0))
        self._fps_hardcap_s = float(fps_cfg.get("shutdown_timeout_s", 300))
        self._fps_stop_bcast_t = None   # when all edges reported done
        self._fps_empty_since = None    # when work queues were first seen empty
        self._fps_work_queues = set()   # queues whose depth we watch while draining
        self._fps_window = 16           # DONEs per live smoothed window_fps sample

        self.register_clients = [0 for _ in range(len(self.total_clients))]
        self.list_clients = []
        self.registered_ids = set()
        self.notified = False
        self.count_clients = 0
        self.client_assignments = {}    # {client_id: {"splits": int, "queue_name": str}}
        self.client_profile_data = {}   # {client_id_str: np.array of per-layer times}
        self.client_bandwidth_data = {} # {client_id_str: float MB/s}
        self.client_name_data = {}      # {client_id_str: str name}
        # Row order of the solver's feature matrices, set by _run_hungarian and
        # consumed by notify_clients to map row -> client (see _ordered_clients).
        self.cluster_edge_order = []
        self.cluster_cloud_order = []
        self._stopping = False
        self.channel.basic_qos(prefetch_count=1)
        self.reply_channel = self.connection.channel()
        self.channel.basic_consume(queue='rpc_queue', on_message_callback=self.on_request)
        # exclusive=True is the race-proof half of _assert_only_server: the
        # pre-flight consumer_count check can't see a server that is starting up
        # at the same moment, but the broker will only ever grant this claim once.
        try:
            self.channel.basic_consume(queue='fps_queue',
                                       on_message_callback=self.on_fps,
                                       exclusive=True)
        except Exception as e:
            self._fatal_second_server(f"exclusive consume on 'fps_queue' refused: {e}")

        self.data = config["data"]
        self.compress = config["compress"]

        log_path = config["log-path"]
        self.logger = src.Log.Logger(config["debug-mode"])
        # One line per finished batch: ns-epoch arrival time of its DONE
        # (e.g. 1782962149610671139). Truncated here so a new run never
        # mixes timestamps with the previous one.
        self.batch_log_path = f"{log_path}/batch_done_ns.log"
        open(self.batch_log_path, "w").close()
        # One line per device: "<ns-epoch arrival> client=... role=... packages=...
        # busy_s=... total_s=... utilization=...%", appended by
        # _collect_utilization at shutdown. Truncated here so runs never mix.
        self.util_log_path = f"{log_path}/utilization.log"
        open(self.util_log_path, "w").close()
        # ── per-cluster breakdowns (the system-wide files above are unchanged) ──
        # Every DONE, tagged with its cluster + that cluster's live window fps.
        # The plottable per-cluster throughput series; batch_done_ns.log keeps its
        # documented two-column format so existing parsers still work.
        self.fps_cluster_ns_log_path = f"{log_path}/fps_cluster_ns.log"
        open(self.fps_cluster_ns_log_path, "w").close()
        # One line per cluster + a SYSTEM line, written by _finish_fps.
        self.fps_cluster_log_path = f"{log_path}/fps_cluster.log"
        open(self.fps_cluster_log_path, "w").close()
        # One line per cluster/role + a SYSTEM line, written by _collect_utilization.
        self.util_cluster_log_path = f"{log_path}/utilization_cluster.log"
        open(self.util_cluster_log_path, "w").close()
        self.latency_cluster_log_path = f"{log_path}/latency_cluster.log"
        open(self.latency_cluster_log_path, "w").close()
        # ── free time (the synthetic view of every device's idle time) ──
        # One line per device, mirroring utilization.log's per-device view.
        self.free_time_log_path = f"{log_path}/free_time.log"
        open(self.free_time_log_path, "w").close()
        # Per cluster, per cluster/role, per MACHINE, + SYSTEM. The machine lines
        # are why devices ship their busy intervals and not just a ratio: several
        # device processes can share one host, and that host is only free when
        # none of them is working (see _collect_free_time).
        self.free_time_cluster_log_path = f"{log_path}/free_time_cluster.log"
        open(self.free_time_cluster_log_path, "w").close()
        # Plottable series: one line per device per time bucket, free% in that
        # bucket. This is the file a 'who was idle, when' heat-map reads.
        self.free_time_series_log_path = f"{log_path}/free_time_series.log"
        open(self.free_time_series_log_path, "w").close()
        self._server_cpu0 = None   # host idle sample for the server's own machine
        # ── message size (what one edge actually puts on the wire) ──
        # One summary line for the measured device, and the plottable series it
        # shipped: one line per published message. Written by _collect_msg_size
        # at shutdown from the report the designated edge published.
        self.msg_size_log_path = f"{log_path}/message_size.log"
        open(self.msg_size_log_path, "w").close()
        self.msg_size_series_log_path = f"{log_path}/message_size_series.log"
        open(self.msg_size_series_log_path, "w").close()
        # ── RAM of the queue host (the machine running RabbitMQ) ──
        # Nothing of ours runs on that box, so the server pulls its memory from
        # outside over SSH for the whole run — see src/BrokerRam.py. One line per
        # sample in the _ns file (the plottable series), one summary block in the
        # other. Both truncated here like every other result file.
        self.broker_ram_ns_log_path = f"{log_path}/broker_ram_ns.log"
        open(self.broker_ram_ns_log_path, "w").close()
        self.broker_ram_log_path = f"{log_path}/broker_ram.log"
        open(self.broker_ram_log_path, "w").close()
        self._broker_ram = self._make_broker_ram_monitor()
        # Start sampling HERE, in __init__ — not at dispatch. Nothing has been
        # published yet and no client has even registered, so these first samples
        # are the queue host at rest: the same machine, measured the same way,
        # with the system not running. Without that reference every later number
        # is "RAM this box happens to be using", and the question the curve is
        # supposed to answer — how much does running the system cost the broker —
        # has no denominator. The run's own window is marked out inside the
        # series (see mark('dispatch') / mark('finish')), so nothing is lost by
        # measuring more.
        self._start_broker_ram()
        # mAP summary: two lines per cluster (WINDOW = pipeline 1, ALL = pipeline 2)
        # plus one OVERALL line per pipeline, appended by _collect_map_pred at
        # shutdown. Truncated here so runs never mix.
        self.map_log_path = f"{log_path}/map.log"
        open(self.map_log_path, "w").close()
        # mAP pipeline 1 detail: one line per sliding window (16 consecutive
        # batches, stepped one batch at a time) — the plottable mAP-over-time
        # series, mirroring how batch_done_ns.log carries the window_fps series.
        self.map_window_log_path = f"{log_path}/map_window.log"
        open(self.map_window_log_path, "w").close()
        # One line per adaptive cut change:
        # "<ns-epoch> <queue>: cut <old>-><new> <deeper|shallower>".
        # Truncated only when the adaptive controller is enabled, so a
        # non-adaptive run keeps the previous adaptive run's log intact.
        self.cut_log_path = f"{log_path}/cut_change_ns.log"
        if self.adaptive_cfg.get("enable", False):
            open(self.cut_log_path, "w").close()
        self.logger.log_info(f"Application start. Server is waiting for {self.total_clients} clients.")
        src.Log.print_with_color(f"Application start. Server is waiting for {self.total_clients} clients.", "green")

    def _fatal_second_server(self, detail):
        """Abort: another server owns this broker. Never degrade to a warning —
        two servers silently divide every meter between them."""
        src.Log.print_with_color(
            f"[FATAL] A server is already running against this broker ({detail}).\n"
            "        Two servers round-robin the DONE stream, so each one counts "
            "only its share and every FPS number reads low by the number of live "
            "servers (a run with 4 servers reported 5.5 fps instead of 22.5).\n"
            "        Kill the leftover server process(es), then start again.",
            "red")
        sys.exit(1)

    def _assert_only_server(self):
        """Refuse to start if another server is already consuming 'fps_queue'.

        Read-only on purpose, and called BEFORE the queue_purge block below: a
        losing second server must exit without having touched the winner's
        in-flight state. A passive declare on a queue that doesn't exist yet is
        the normal first-run case (nothing to collide with) and closes the
        channel, so the channel is reopened before returning."""
        try:
            res = self.channel.queue_declare(queue='fps_queue', passive=True)
        except Exception:
            # 404: no 'fps_queue' yet -> no other server. The broker closed the
            # channel to report it, so replace it before anyone else uses it.
            self.channel = self.connection.channel()
            return
        n = res.method.consumer_count
        if n > 0:
            self._fatal_second_server(f"'fps_queue' already has {n} consumer(s)")

    def _get_mode(self):
        exp = self.config.get("experiment", {})
        if exp.get("enable", True):
            return exp.get("mode", "split")
        return "split"

    def on_request(self, ch, method, _, body):
        message = pickle.loads(body)
        action = message["action"]

        if action == "REGISTER":
            client_id = message["client_id"]
            layer_id = message["layer_id"]

            src.Log.print_with_color(f"[<<<] Received REGISTER from client {client_id} layer={layer_id}", "blue")

            if layer_id < 1 or layer_id > len(self.register_clients):
                src.Log.print_with_color(
                    f"[!] Ignored client with unexpected layer_id={layer_id} (expected 1..{len(self.register_clients)})", "red")
                return

            if str(client_id) in self.registered_ids:
                src.Log.print_with_color(f"[!] Duplicate REGISTER from {client_id}, ignored.", "yellow")
                return

            self.registered_ids.add(str(client_id))
            self.list_clients.append((str(client_id), layer_id))

            layer_times = message.get("layer_times", None)
            if layer_times is not None:
                self.client_profile_data[str(client_id)] = np.array(layer_times, dtype=float)
                src.Log.print_with_color(
                    f"[Profile] Stored profiling data from client {client_id} "
                    f"({len(layer_times)} layers, total={sum(layer_times)*1000:.1f} ms)", "cyan")

            bandwidth_mb_s = message.get("bandwidth_mb_s", None)
            if bandwidth_mb_s is not None:
                self.client_bandwidth_data[str(client_id)] = float(bandwidth_mb_s)
                src.Log.print_with_color(
                    f"[Bandwidth] Stored bandwidth from client {client_id}: {bandwidth_mb_s:.1f} MB/s", "cyan")

            client_name = message.get("client_name", None)
            if client_name:
                self.client_name_data[str(client_id)] = client_name

            self.register_clients[layer_id - 1] += 1

            if self.register_clients == self.total_clients and not self.notified:
                self.notified = True
                src.Log.print_with_color("All clients connected. Sending notifications.", "green")
                self.notify_clients()

        elif action == "BW_TEST":
            client_id = message["client_id"]
            self.send_to_response(str(client_id), pickle.dumps({"action": "BW_ACK"}))

        elif action == "NOTIFY":
            self.count_clients += 1
            if self.count_clients == self.total_clients[0]:
                self.logger.log_info("Stop Inference !!!")
                self._stopping = True
                self.notify_clients(start=False)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                # Do NOT stop consuming yet: the clouds are still draining their
                # backlog and emitting DONEs. Keep the fps meter alive while the
                # work queues hold batches, plus a grace for the last in-flight
                # batch, so late DONEs from cloud backlog are still counted.
                self._fps_stop_bcast_t = time.time()
                self._fps_work_queues = {
                    a.get("queue_name", "intermediate_queue")
                    for a in self.client_assignments.values()
                } or {"intermediate_queue"}
                src.Log.print_with_color(
                    f"[FPS] all edges done; draining {sorted(self._fps_work_queues)} "
                    f"(grace {self._fps_grace_s:.0f}s, hard cap {self._fps_hardcap_s:.0f}s)",
                    "yellow")
                try:
                    self.connection.call_later(1.0, self._fps_drain_check)
                except Exception:
                    self._finish_fps("scheduler unavailable at edge-done")
                return

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def on_fps(self, ch, method, _, body):
        """Consumer for fps_queue. Every message is one finished batch and the
        ARRIVAL is the event — we record the server-clock arrival time and all
        throughput math happens in _finish_fps. Timing never depends on the body,
        so device clocks still need no syncing.

        The body carries the producing cluster's id, used only to bucket the
        arrival for the per-cluster breakdown. A smoothed window_fps is logged live
        (system-wide) so progress is visible during the run; each arrival is
        appended to batch_done_ns.log as "<ns-epoch> <fps>" (unchanged format) and
        to fps_cluster_ns.log with its cluster tag and that cluster's own window
        fps."""
        t_ns = time.time_ns()
        t_s = t_ns / 1e9
        self._fps_times.append(t_s)
        n = len(self._fps_times)
        W = self._fps_window
        window_fps = None
        if n >= W:
            span = self._fps_times[-1] - self._fps_times[-W]
            if span > 0:
                window_fps = (W - 1) * self.batch_size / span
                src.Log.print_with_color(
                    f"[FPS] DONE #{n}  window_fps={window_fps:6.2f} "
                    f"(last {W} batches)", "cyan")
        with open(self.batch_log_path, "a") as f:
            if window_fps is None:
                f.write(f"{t_ns}\n")
            else:
                f.write(f"{t_ns} {window_fps:.2f}\n")

        # Per-cluster bucket. A producer that predates the tagged body sends
        # b"DONE" — bucket it as 'unknown' rather than dropping it, so the
        # breakdown degrades to one lump instead of silently losing batches.
        try:
            tag = body.decode("utf-8", "replace").strip()
        except Exception:
            tag = ""
        if not tag or tag == "DONE":
            tag = "unknown"
        ct = self._fps_by_cluster.setdefault(tag, [])
        ct.append(t_s)
        nc = len(ct)
        cluster_fps = None
        if nc >= W:
            span_c = ct[-1] - ct[-W]
            if span_c > 0:
                cluster_fps = (W - 1) * self.batch_size / span_c
        with open(self.fps_cluster_ns_log_path, "a") as f:
            if cluster_fps is None:
                f.write(f"{t_ns} cluster={tag} done={nc}\n")
            else:
                f.write(f"{t_ns} cluster={tag} done={nc} window_fps={cluster_fps:.2f}\n")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _fps_total_work_depth(self):
        """Total messages still queued across the pipeline's work queues, or None
        if the broker's management API can't be reached for any of them."""
        total = 0
        any_ok = False
        for q in self._fps_work_queues:
            depth, _ = self._queue_stats(q)
            if depth is not None:
                any_ok = True
                total += depth
        return total if any_ok else None

    def _fps_drain_check(self):
        """Periodic tail-watcher (scheduled once the edges finish). Keeps the fps
        meter alive while the work queues still hold batches, then finalises after
        a short grace for the last in-flight batch. A hard cap prevents hanging if
        a peer dies without draining."""
        if self._fps_printed:
            return
        now = time.time()
        if self._fps_stop_bcast_t is not None and (now - self._fps_stop_bcast_t) >= self._fps_hardcap_s:
            self._finish_fps(f"hard cap {self._fps_hardcap_s:.0f}s reached")
            return

        depth = self._fps_total_work_depth()
        if depth is None:
            # No queue stats: fall back to "no DONE for grace seconds".
            last = self._fps_times[-1] if self._fps_times else self._fps_stop_bcast_t
            if last is not None and (now - last) >= self._fps_grace_s:
                self._finish_fps("grace elapsed (no queue stats)")
                return
        elif depth > 0:
            self._fps_empty_since = None   # still draining, reset grace
        else:
            if self._fps_empty_since is None:
                self._fps_empty_since = now
            elif (now - self._fps_empty_since) >= self._fps_grace_s:
                self._finish_fps("work queues drained + grace")
                return

        try:
            self.connection.call_later(1.0, self._fps_drain_check)
        except Exception:
            self._finish_fps("scheduler unavailable")

    def _report_cluster_fps(self):
        """Per-cluster throughput breakdown, printed under the system summary and
        written to fps_cluster.log (one line per cluster + a SYSTEM line).

        Each cluster gets the same two measures as the system: whole-run fps from
        the shared START (so the numbers are additive — they sum to roughly the
        system fps, exactly so when the clusters finish together) and steady-state
        fps across that cluster's own first->last DONE, which drops its warm-up and
        is the fair number for comparing one cluster against another.

        'share' is the cluster's portion of all batches — the quickest read on
        whether the Hungarian assignment actually balanced the clusters."""
        by_cluster = self._fps_by_cluster
        n_total = len(self._fps_times)
        if not by_cluster or n_total == 0:
            return
        bs = self.batch_size
        start = self._fps_start_t
        t_ns = time.time_ns()
        lines = []
        print("  " + "-" * 56)
        for tag, ts in sorted(by_cluster.items()):
            nc = len(ts)
            share = 100.0 * nc / n_total
            fps_c = nc * bs / (ts[-1] - start) if start and ts[-1] > start else 0.0
            steady_c = ((nc - 1) * bs / (ts[-1] - ts[0])
                        if nc >= 2 and ts[-1] > ts[0] else 0.0)
            print(f"  [cluster] {tag:<24} {fps_c:8.3f} fps   "
                  f"steady={steady_c:8.3f}   {nc} DONE x {bs}   share={share:5.1f}%")
            lines.append(
                f"{t_ns} cluster={tag} fps={fps_c:.3f} steady_fps={steady_c:.3f} "
                f"done={nc} frames={nc * bs} share={share:.1f}%")
        sys_fps = (n_total * bs / (self._fps_times[-1] - start)
                   if start and self._fps_times[-1] > start else 0.0)
        lines.append(f"{t_ns} SYSTEM fps={sys_fps:.3f} done={n_total} "
                     f"frames={n_total * bs} clusters={len(by_cluster)}")
        try:
            with open(self.fps_cluster_log_path, "a") as f:
                f.write("\n".join(lines) + "\n")
        except Exception as e:
            src.Log.print_with_color(f"[FPS] cluster log write failed: {e}", "yellow")

    def _finish_fps(self, reason=""):
        """Print the system-FPS summary once and stop the server's consumer.
        Idempotent — whichever of drain-grace / hard-cap fires first wins."""
        if self._fps_printed:
            return
        self._fps_printed = True

        t = self._fps_times
        n = len(t)
        bs = self.batch_size
        print("=" * 60)
        if n >= 1 and self._fps_start_t is not None and t[-1] > self._fps_start_t:
            # PRIMARY: whole-run throughput. Weights every second equally, so
            # bursty DONE arrivals can't inflate it (frames / total time).
            total_time = t[-1] - self._fps_start_t
            system_fps = n * bs / total_time
            print(f"  [SYSTEM FPS]      {system_fps:8.3f} fps   "
                  f"= {n} DONE x {bs} / {total_time:.2f}s  (START -> last DONE)")
            # Steady-state: drop the warm-up. The first DONE only starts the clock
            # (its batch finished before the measured span), hence (n-1).
            if n >= 2 and t[-1] > t[0]:
                span = t[-1] - t[0]
                steady = (n - 1) * bs / span
                print(f"  [steady-state]    {steady:8.3f} fps   "
                      f"= {n - 1} x {bs} / {span:.2f}s  (first -> last DONE)")
            # Reference only: mean of per-gap 1/dt fps. Over-weights bursts, so it
            # reads high vs the true rate — kept for comparison, do not use.
            if n >= 2:
                gaps = [t[i] - t[i - 1] for i in range(1, n) if t[i] > t[i - 1]]
                if gaps:
                    ref_mean = sum(bs / g for g in gaps) / len(gaps)
                    print(f"  [ref mean, N/U]   {ref_mean:8.3f} fps   "
                          f"(arithmetic mean of 1/dt — reference only, biased high)")
        else:
            print("  [SYSTEM FPS]      no DONEs received — nothing to report")
        print(f"  batches counted: {n}   stop reason: {reason}")
        self._report_cluster_fps()
        print("=" * 60)
        try:
            self.channel.stop_consuming()
        except Exception:
            pass

    def _collect_utilization(self, timeout_s=30.0):
        """Shutdown step: drain every device's UTILIZATION report from
        utilization_queue and append one line per device to utilization.log.
        Runs after the FPS drain + summary, so every cloud has already seen STOP
        and published its report. Polls with basic_get until all registered
        clients reported or the timeout elapses — a partial collection prints a
        warning and the run still shuts down cleanly."""
        expected = len(self.registered_ids)
        reported = set()
        reports = []            # kept for the per-cluster utilization/latency roll-up
        deadline = time.time() + timeout_s
        while len(reported) < expected and time.time() < deadline:
            method_frame, _, body = self.channel.basic_get(queue='utilization_queue', auto_ack=True)
            if not method_frame:
                time.sleep(0.2)
                continue
            try:
                msg = pickle.loads(body)
            except Exception:
                continue
            if not isinstance(msg, dict) or msg.get("action") != "UTILIZATION":
                continue
            t_ns = time.time_ns()   # server-clock arrival timestamp for the log line
            client_id = msg.get("client_id")
            reported.add(str(client_id))
            reports.append(msg)
            line = (f"{t_ns} client={client_id} role={msg.get('role')} "
                    f"packages={msg.get('packages')} "
                    f"busy_s={msg.get('busy_ns', 0) / 1e9:.3f} "
                    f"total_s={msg.get('total_ns', 0) / 1e9:.3f} "
                    f"utilization={msg.get('utilization', 0.0) * 100:.2f}%")
            with open(self.util_log_path, "a") as f:
                f.write(line + "\n")
            src.Log.print_with_color(f"[Utilization] {line}", "cyan")
        if len(reported) < expected:
            src.Log.print_with_color(
                f"[Utilization] Collected {len(reported)}/{expected} reports before timeout", "yellow")
        self._report_cluster_util_latency(reports)

    @staticmethod
    def _stats_ms(vals):
        """n / mean / p50 / p95 / max over pooled latency samples, or None if empty.
        Nearest-rank percentiles over the sorted samples — no interpolation, so
        every number reported is a latency that was actually observed. Pooling the
        raw samples (rather than averaging per-device percentiles, which is not a
        valid operation) is why the devices ship samples instead of summaries."""
        if not vals:
            return None
        s = sorted(vals)
        n = len(s)

        def pct(q):
            return s[min(n - 1, max(0, int(round((n - 1) * q))))]

        return {"n": n, "mean": sum(s) / n, "p50": pct(0.50), "p95": pct(0.95), "max": s[-1]}

    def _report_cluster_util_latency(self, reports):
        """Roll the shutdown reports up per cluster into utilization_cluster.log and
        latency_cluster.log, plus a console block. utilization.log keeps its
        per-device view untouched — this adds the grouping, it doesn't replace it.

        Utilization is reported pooled (sum busy / sum total across the group),
        which weights each device by how long it actually ran; the plain mean of
        the per-device ratios is printed alongside, since a pooled number can hide
        one idle device in a group of busy ones.

        Latency comes in three flavours and they answer different questions:
          * kind=service — each device's own get_input -> output, one clock, so it
            is exact. Its samples sum to that role's busy_s, which makes it the
            only one comparable against utilization;
          * kind=pipeline — batch ready -> published. Same clock, but on the edge
            it also contains the wait in the two hand-off queues, so it tracks
            queue_size rather than device speed. This is what feeds E2E;
          * kind=e2e — edge batch start -> completing tier's output. It spans two
            machines, so it inherits any clock offset between them. Only the
            completing tier reports it, hence one E2E series per cluster.
        """
        if not reports:
            return
        t_ns = time.time_ns()
        by_cluster = {}
        for r in reports:
            by_cluster.setdefault(str(r.get("cluster_id", "unknown")), []).append(r)

        util_lines, lat_lines = [], []
        all_busy = all_total = 0
        all_ratios, all_e2e = [], []
        print("=" * 60)
        print("  [PER-CLUSTER UTILIZATION & LATENCY]")
        for tag, rs in sorted(by_cluster.items()):
            busy = sum(r.get("busy_ns", 0) for r in rs)
            total = sum(r.get("total_ns", 0) for r in rs)
            ratios = [r.get("utilization", 0.0) for r in rs]
            all_busy += busy
            all_total += total
            all_ratios += ratios
            pooled = busy / total if total else 0.0
            mean_r = sum(ratios) / len(ratios) if ratios else 0.0
            print(f"  [cluster] {tag:<24} devices={len(rs)}  "
                  f"utilization={pooled * 100:6.2f}%  (mean of devices={mean_r * 100:6.2f}%)")
            util_lines.append(
                f"{t_ns} cluster={tag} ALL devices={len(rs)} "
                f"utilization={pooled * 100:.2f}% utilization_mean={mean_r * 100:.2f}% "
                f"busy_s={busy / 1e9:.3f} total_s={total / 1e9:.3f} "
                f"packages={sum(r.get('packages', 0) for r in rs)}")

            by_role = {}
            for r in rs:
                by_role.setdefault(str(r.get("role", "unknown")), []).append(r)
            for role, rr in sorted(by_role.items()):
                b = sum(r.get("busy_ns", 0) for r in rr)
                tt = sum(r.get("total_ns", 0) for r in rr)
                util_lines.append(
                    f"{t_ns} cluster={tag} role={role} devices={len(rr)} "
                    f"utilization={(b / tt * 100) if tt else 0.0:.2f}% "
                    f"busy_s={b / 1e9:.3f} total_s={tt / 1e9:.3f} "
                    f"packages={sum(r.get('packages', 0) for r in rr)}")
                # Two per-role series, and mixing them up is the whole reason for
                # reporting both:
                #   service  = get_input -> output, the device's own compute. Sums
                #              to exactly the busy_s on the line above.
                #   pipeline = batch ready -> published, so on the edge it also
                #              carries the wait in the two hand-off queues. It is
                #              the number that explains e2e, not the device's speed.
                for kind, key, label in (("service", "svc_samples_ms", "service "),
                                         ("pipeline", "lat_samples_ms", "pipeline")):
                    st = self._stats_ms([v for r in rr for v in r.get(key, [])])
                    if not st:
                        continue
                    print(f"      {role:<8} {label} latency n={st['n']:<5} "
                          f"mean={st['mean']:8.1f}ms  p50={st['p50']:8.1f}  "
                          f"p95={st['p95']:8.1f}  max={st['max']:8.1f}")
                    lat_lines.append(
                        f"{t_ns} cluster={tag} role={role} kind={kind} n={st['n']} "
                        f"mean_ms={st['mean']:.3f} p50_ms={st['p50']:.3f} "
                        f"p95_ms={st['p95']:.3f} max_ms={st['max']:.3f}")

            e2e = [v for r in rs for v in r.get("e2e_samples_ms", [])]
            all_e2e += e2e
            st = self._stats_ms(e2e)
            if st:
                print(f"      {'E2E':<8} pipeline latency n={st['n']:<5} "
                      f"mean={st['mean']:8.1f}ms  p50={st['p50']:8.1f}  "
                      f"p95={st['p95']:8.1f}  max={st['max']:8.1f}")
                lat_lines.append(
                    f"{t_ns} cluster={tag} kind=e2e n={st['n']} "
                    f"mean_ms={st['mean']:.3f} p50_ms={st['p50']:.3f} "
                    f"p95_ms={st['p95']:.3f} max_ms={st['max']:.3f}")

        # System-wide lines: the whole point is that the per-cluster breakdown
        # never replaces the total, so both files end with one.
        pooled_all = all_busy / all_total if all_total else 0.0
        mean_all = sum(all_ratios) / len(all_ratios) if all_ratios else 0.0
        print(f"  [SYSTEM]  devices={len(reports)}  clusters={len(by_cluster)}  "
              f"utilization={pooled_all * 100:6.2f}%  (mean of devices={mean_all * 100:6.2f}%)")
        util_lines.append(
            f"{t_ns} SYSTEM devices={len(reports)} clusters={len(by_cluster)} "
            f"utilization={pooled_all * 100:.2f}% utilization_mean={mean_all * 100:.2f}% "
            f"busy_s={all_busy / 1e9:.3f} total_s={all_total / 1e9:.3f}")
        st = self._stats_ms(all_e2e)
        if st:
            print(f"  [SYSTEM]  E2E pipeline latency  n={st['n']:<5} "
                  f"mean={st['mean']:8.1f}ms  p50={st['p50']:8.1f}  "
                  f"p95={st['p95']:8.1f}  max={st['max']:8.1f}")
            lat_lines.append(
                f"{t_ns} SYSTEM kind=e2e n={st['n']} mean_ms={st['mean']:.3f} "
                f"p50_ms={st['p50']:.3f} p95_ms={st['p95']:.3f} max_ms={st['max']:.3f}")
        print("=" * 60)

        for path, lines in ((self.util_cluster_log_path, util_lines),
                            (self.latency_cluster_log_path, lat_lines)):
            if not lines:
                continue
            try:
                with open(path, "a") as f:
                    f.write("\n".join(lines) + "\n")
            except Exception as e:
                src.Log.print_with_color(f"[Utilization] log write failed ({path}): {e}", "yellow")

    def _collect_free_time(self, timeout_s=None):
        """Shutdown step: drain every device's FREETIME report and turn them into
        the synthetic fleet view — free_time.log (per device), free_time_cluster.log
        (per cluster, per cluster/role, per MACHINE, SYSTEM) and
        free_time_series.log (per device per time bucket, for plotting).

        Free time is the wall-clock time a device spent doing NO pipeline work:
        no capture, inference, compress, send, receive, decompress, postprocess or
        metrics. Each device computes it by merging the busy intervals of all its
        threads (see src/FreeTime.py) — a sum would double-count the two pipeline
        lanes and can exceed the wall clock.

        Two aggregations, and they answer different questions:

          * per cluster / role — pooled free (Σfree / Σspan), which weights each
            device by how long it actually ran, plus the plain mean of the device
            percentages, since a pooled number hides one idle device among busy
            ones. Same convention as _report_cluster_util_latency.
          * per machine — the union of the busy intervals of every device process
            on that host. A machine with two device processes is only free when
            NEITHER is working, so this cannot be derived from the per-device
            ratios; it needs the intervals, which is why the devices ship them.
            Device clocks are irrelevant here: processes on one host share a
            clock, and intervals are never compared across hosts.

        Also reports host_idle: the OS's own idle/total CPU accounting over the
        run, i.e. the machine's free time across ALL processes including anything
        that isn't ours. Pipeline free time and host idle disagreeing is the
        signal that something else on the box is eating the CPU.
        """
        from src.FreeTime import (WORK_KINDS, merge_intervals, subtract_intervals,
                                  clip_intervals, total_ns)

        # Same contract as _collect_map_pred: free_time.enable travels to the
        # devices in START, so when it is off nobody will ever publish here and
        # waiting the full timeout only delays the shutdown.
        if self.free_time_cfg.get("enable", True) is False:
            src.Log.print_with_color(
                "[FreeTime] skipped (free_time.enable=False) — devices ran without "
                "free-time accounting", "yellow")
            return

        if timeout_s is None:
            timeout_s = float(self.free_time_cfg.get("collect_timeout_s") or 30.0)
        expected = len(self.registered_ids)
        if expected == 0:
            return
        reported, reports = set(), []
        deadline = time.time() + timeout_s
        while len(reported) < expected and time.time() < deadline:
            method_frame, _, body = self.channel.basic_get(queue='freetime_queue', auto_ack=True)
            if not method_frame:
                time.sleep(0.2)
                continue
            try:
                msg = pickle.loads(body)
            except Exception:
                continue
            if not isinstance(msg, dict) or msg.get("action") != "FREETIME":
                continue
            reported.add(str(msg.get("client_id")))
            reports.append(msg)
        if not reports:
            src.Log.print_with_color(
                "[FreeTime] no reports collected — devices ran without free-time "
                "accounting, or none of them finished", "yellow")
            return
        if len(reported) < expected:
            src.Log.print_with_color(
                f"[FreeTime] Collected {len(reported)}/{expected} reports before timeout", "yellow")

        t_ns = time.time_ns()

        # ── per device ────────────────────────────────────────────────────────
        dev_lines, series_lines = [], []
        for r in sorted(reports, key=lambda m: (str(m.get("cluster_id")), str(m.get("role")))):
            span_s = r.get("span_ns", 0) / 1e9
            line = (f"{t_ns} client={r.get('client_id')} role={r.get('role')} "
                    f"machine={r.get('machine')} cluster={r.get('cluster_id')} "
                    f"device={r.get('device')} span_s={span_s:.3f} "
                    f"busy_s={r.get('busy_ns', 0) / 1e9:.3f} "
                    f"free_s={r.get('free_ns', 0) / 1e9:.3f} "
                    f"free={r.get('free_pct', 0.0):.2f}% "
                    f"gaps={r.get('free_gaps', 0)} "
                    f"longest_free_ms={r.get('longest_free_ms', 0.0):.3f}")
            if r.get("host_idle_pct") is not None:
                line += f" host_idle={r['host_idle_pct']:.2f}%"
            dev_lines.append(line)
            bucket_s = float(r.get("bucket_s", 1.0))
            for i, f in enumerate(r.get("free_series", [])):
                series_lines.append(
                    f"{t_ns} client={r.get('client_id')} role={r.get('role')} "
                    f"machine={r.get('machine')} cluster={r.get('cluster_id')} "
                    f"i={i} t_offset_s={i * bucket_s:.3f} bucket_s={bucket_s:.3f} "
                    f"free={100.0 * f:.2f}%")

        # ── roll-ups ──────────────────────────────────────────────────────────
        grp_lines = []
        by_cluster = {}
        for r in reports:
            by_cluster.setdefault(str(r.get("cluster_id", "unknown")), []).append(r)

        def pooled(rs):
            span = sum(r.get("span_ns", 0) for r in rs)
            free = sum(r.get("free_ns", 0) for r in rs)
            pcts = [r.get("free_pct", 0.0) for r in rs]
            return (span, free,
                    (100.0 * free / span) if span else 0.0,
                    (sum(pcts) / len(pcts)) if pcts else 0.0)

        print("=" * 60)
        print("  [FREE TIME]  wall clock with no inference / send / compress /")
        print("               decompress / postprocess — i.e. doing nothing")
        for tag, rs in sorted(by_cluster.items()):
            span, free, pool, mean = pooled(rs)
            print(f"  [cluster] {tag:<24} devices={len(rs)}  free={pool:6.2f}%  "
                  f"(mean of devices={mean:6.2f}%)  free_s={free / 1e9:.1f}")
            grp_lines.append(
                f"{t_ns} cluster={tag} ALL devices={len(rs)} free={pool:.2f}% "
                f"free_mean={mean:.2f}% free_s={free / 1e9:.3f} span_s={span / 1e9:.3f}")
            by_role = {}
            for r in rs:
                by_role.setdefault(str(r.get("role", "unknown")), []).append(r)
            for role, rr in sorted(by_role.items()):
                span_r, free_r, pool_r, mean_r = pooled(rr)
                print(f"      {role:<8} devices={len(rr)}  free={pool_r:6.2f}%  "
                      f"(mean of devices={mean_r:6.2f}%)")
                grp_lines.append(
                    f"{t_ns} cluster={tag} role={role} devices={len(rr)} "
                    f"free={pool_r:.2f}% free_mean={mean_r:.2f}% "
                    f"free_s={free_r / 1e9:.3f} span_s={span_r / 1e9:.3f}")
            # Where this cluster's free time went, and where its busy time went.
            # Both are shares of the same denominator (Σ span), so they read
            # against each other directly.
            for reason, ns in sorted(
                    _sum_dicts(r.get("free_reasons", {}) for r in rs).items(),
                    key=lambda kv: -kv[1]):
                grp_lines.append(
                    f"{t_ns} cluster={tag} FREE reason={reason} free_s={ns / 1e9:.3f} "
                    f"share={(100.0 * ns / free) if free else 0.0:.2f}%")
            kinds = _sum_dicts({k: v.get("ns", 0) for k, v in (r.get("kinds") or {}).items()}
                               for r in rs)
            for kind in WORK_KINDS:
                if kind in kinds:
                    grp_lines.append(
                        f"{t_ns} cluster={tag} KIND kind={kind} busy_s={kinds[kind] / 1e9:.3f} "
                        f"share={(100.0 * kinds[kind] / span) if span else 0.0:.2f}%")

        # ── per machine: union of every device process on that host ───────────
        by_machine = {}
        for r in reports:
            by_machine.setdefault(str(r.get("machine") or r.get("hostname") or "unknown"), []).append(r)
        print("  " + "-" * 56)
        for name, rs in sorted(by_machine.items()):
            spans = merge_intervals([(r["t_start_ns"], r["t_end_ns"]) for r in rs
                                     if r.get("t_start_ns") and r.get("t_end_ns")])
            busy = merge_intervals([tuple(iv) for r in rs
                                    for iv in r.get("busy_intervals_ns", [])])
            busy = [iv for s, e in spans for iv in clip_intervals(busy, s, e)]
            busy = merge_intervals(busy)
            span_ns = total_ns(spans)
            free_ns = total_ns(subtract_intervals(spans, busy))
            pct = (100.0 * free_ns / span_ns) if span_ns else 0.0
            slop = sum(r.get("busy_intervals_slop_ns", 0) for r in rs)
            idle = [r["host_idle_pct"] for r in rs if r.get("host_idle_pct") is not None]
            host_idle = (sum(idle) / len(idle)) if idle else None
            print(f"  [machine] {name:<24} devices={len(rs)}  free={pct:6.2f}%"
                  + (f"   host_idle={host_idle:6.2f}%" if host_idle is not None else ""))
            grp_lines.append(
                f"{t_ns} MACHINE machine={name} devices={len(rs)} free={pct:.2f}% "
                f"free_s={free_ns / 1e9:.3f} span_s={span_ns / 1e9:.3f} "
                f"merge_slop_s={slop / 1e9:.3f}"
                + (f" host_idle={host_idle:.2f}%" if host_idle is not None else ""))

        # The server's own machine. It runs no pipeline stage, so only the OS-level
        # number means anything for it — reported so the fleet view covers every
        # host involved, not only the ones running devices.
        host_idle = self._server_host_idle_pct()
        if host_idle is not None:
            print(f"  [machine] {'(server)':<24} host_idle={host_idle:6.2f}%")
            grp_lines.append(
                f"{t_ns} MACHINE machine={socket.gethostname()} role=server devices=0 "
                f"host_idle={host_idle:.2f}%")

        span, free, pool, mean = pooled(reports)
        print(f"  [SYSTEM]  devices={len(reports)}  machines={len(by_machine)}  "
              f"free={pool:6.2f}%  (mean of devices={mean:6.2f}%)")
        grp_lines.append(
            f"{t_ns} SYSTEM devices={len(reports)} clusters={len(by_cluster)} "
            f"machines={len(by_machine)} free={pool:.2f}% free_mean={mean:.2f}% "
            f"free_s={free / 1e9:.3f} span_s={span / 1e9:.3f}")
        reasons = _sum_dicts(r.get("free_reasons", {}) for r in reports)
        for reason, ns in sorted(reasons.items(), key=lambda kv: -kv[1]):
            print(f"      free because {reason:<14} {ns / 1e9:9.1f}s   "
                  f"{(100.0 * ns / free) if free else 0.0:5.1f}% of all free time")
            grp_lines.append(
                f"{t_ns} SYSTEM FREE reason={reason} free_s={ns / 1e9:.3f} "
                f"share={(100.0 * ns / free) if free else 0.0:.2f}%")
        kinds = _sum_dicts({k: v.get("ns", 0) for k, v in (r.get("kinds") or {}).items()}
                           for r in reports)
        for kind in WORK_KINDS:
            if kind not in kinds:
                continue
            print(f"      busy on     {kind:<14} {kinds[kind] / 1e9:9.1f}s   "
                  f"{(100.0 * kinds[kind] / span) if span else 0.0:5.1f}% of all device time")
            grp_lines.append(
                f"{t_ns} SYSTEM KIND kind={kind} busy_s={kinds[kind] / 1e9:.3f} "
                f"share={(100.0 * kinds[kind] / span) if span else 0.0:.2f}%")
        print("=" * 60)

        for path, out in ((self.free_time_log_path, dev_lines),
                          (self.free_time_cluster_log_path, grp_lines),
                          (self.free_time_series_log_path, series_lines)):
            if not out:
                continue
            try:
                with open(path, "a") as f:
                    f.write("\n".join(out) + "\n")
            except Exception as e:
                src.Log.print_with_color(f"[FreeTime] log write failed ({path}): {e}", "yellow")

    def _collect_msg_size(self, timeout_s=None):
        """Shutdown step: drain the designated edge's MSGSIZE report and write
        message_size.log (one summary line) + message_size_series.log (one line
        per published message).

        Exactly ONE report is expected, because exactly one device was told to
        measure — the first client that registered at layer 1. Every edge in a
        cluster publishes the same feature map from the same cut, so measuring
        all nine costs nine times as much and answers the same question.

        Same flag-honoured-at-both-ends contract as free time: when the feature is
        off nobody will ever publish here, so waiting the full timeout would only
        stall the shutdown and then warn about a queue that was never going to
        receive anything."""
        if self.msg_size_cfg.get("enable", True) is False:
            src.Log.print_with_color(
                "[MsgSize] skipped (message_size.enable=False) — no device measured "
                "published message size", "yellow")
            return
        if self._msg_size_client is None:
            src.Log.print_with_color(
                "[MsgSize] no edge (layer_id=1) registered — nothing to collect", "yellow")
            return
        if timeout_s is None:
            timeout_s = float(self.msg_size_cfg.get("collect_timeout_s") or 30.0)
        deadline = time.time() + timeout_s
        report = None
        while report is None and time.time() < deadline:
            method_frame, _, body = self.channel.basic_get(queue='msgsize_queue', auto_ack=True)
            if not method_frame:
                time.sleep(0.2)
                continue
            try:
                msg = pickle.loads(body)
            except Exception:
                continue
            if isinstance(msg, dict) and msg.get("action") == "MSGSIZE":
                report = msg
        if report is None:
            src.Log.print_with_color(
                f"[MsgSize] no report from {self._msg_size_client} within "
                f"{timeout_s:.0f}s — message_size.log will be empty", "yellow")
            return
        self._report_msg_size(report)

    def _report_msg_size(self, rep):
        """Turn the measured edge's report into the two result files + a console
        block.

        The device shipped sample times as OFFSETS from its own first publish, so
        nothing here carries a device clock: every line starts with the server's
        t_ns, exactly like free_time_series.log, and the offsets locate a sample
        inside the run without ever being compared against another machine."""
        t_ns = time.time_ns()
        sizes = [int(b) for b in rep.get("bytes", [])]
        if not sizes:
            return
        st = self._stats_ms(sizes)          # generic n/mean/p50/p95/max, nearest-rank
        span_s = rep.get("span_ns", 0) / 1e9
        total_b = sum(sizes)
        bs = int(rep.get("batch_size") or 0)
        # MB everywhere (10^6, matching broker_ram.log so the two files compare
        # directly), 3 decimals so a 1 KB message is still resolved.
        MB = 1e6

        summary = (
            f"{t_ns} client={rep.get('client_id')} role={rep.get('role')} "
            f"machine={rep.get('machine')} cluster={rep.get('cluster_id')} "
            f"mode={rep.get('mode')} splits={rep.get('splits')} "
            f"compress={'on' if rep.get('compress') else 'off'}"
            + (f" num_bit={rep.get('num_bit')}" if rep.get("compress") else "")
            + f" batch_size={bs} n={st['n']} "
            f"total_mb={total_b / MB:.3f} "
            f"mean_mb={st['mean'] / MB:.3f} p50_mb={st['p50'] / MB:.3f} "
            f"p95_mb={st['p95'] / MB:.3f} max_mb={st['max'] / MB:.3f} "
            f"min_mb={min(sizes) / MB:.3f} "
            f"span_s={span_s:.3f} "
            f"rate_mb_s={(total_b / MB / span_s) if span_s else 0.0:.3f}"
            + (f" per_frame_mb={st['mean'] / bs / MB:.4f}" if bs else "")
        )

        series = []
        for i, (off_ns, batch_id, nbytes) in enumerate(rep.get("samples", [])):
            series.append(
                f"{t_ns} client={rep.get('client_id')} cluster={rep.get('cluster_id')} "
                f"i={i} t_offset_s={off_ns / 1e9:.3f} batch_id={batch_id} "
                f"bytes={int(nbytes)} mb={nbytes / MB:.3f}")

        print("=" * 60)
        print("  [MESSAGE SIZE]  bytes handed to the broker per message,")
        print("                  measured before publish on ONE edge")
        print(f"  [device]  {rep.get('client_id')}  role={rep.get('role')}  "
              f"cluster={rep.get('cluster_id')}  "
              f"compress={'on' if rep.get('compress') else 'off'}")
        print(f"  [size]    n={st['n']}  mean={st['mean'] / MB:6.2f}MB  "
              f"p50={st['p50'] / MB:6.2f}MB  p95={st['p95'] / MB:6.2f}MB  "
              f"max={st['max'] / MB:6.2f}MB")
        print(f"  [total]   {total_b / 1e9:.3f}GB over {span_s:.1f}s  "
              f"= {(total_b / MB / span_s) if span_s else 0.0:.2f} MB/s from this device"
              + (f"   ({st['mean'] / bs / MB:.3f} MB/frame)" if bs else ""))
        if len(rep.get("samples", [])) < st["n"]:
            print(f"  [series]  {len(rep.get('samples', []))} of {st['n']} samples "
                  f"(decimated for transport; stats above use all {st['n']})")
        print("=" * 60)

        for path, out in ((self.msg_size_log_path, [summary]),
                          (self.msg_size_series_log_path, series)):
            if not out:
                continue
            try:
                with open(path, "a") as f:
                    f.write("\n".join(out) + "\n")
            except Exception as e:
                src.Log.print_with_color(f"[MsgSize] log write failed ({path}): {e}", "yellow")

    def _make_broker_ram_monitor(self):
        """Build (but don't start) the queue-host RAM sampler.

        Defaults to the broker this run actually uses, so a config that only
        flips `enable` still measures the right machine. The SSH credentials are
        the HOST login of that machine — deliberately separate from the
        rabbit.username/password above, which are AMQP credentials and cannot
        open a shell.

        Returns None rather than raising: this runs in __init__, and a telemetry
        module that fails to import must not stop the server from starting."""
        try:
            from src.BrokerRam import BrokerRamMonitor
        except Exception as e:
            src.Log.print_with_color(f"[BrokerRAM] disabled — {e}", "yellow")
            return None
        cfg = self.broker_ram_cfg or {}
        return BrokerRamMonitor(
            host=cfg.get("host") or self.address,
            user=cfg.get("user"),
            password=cfg.get("password"),
            port=cfg.get("ssh_port") or 22,
            interval_s=cfg.get("interval_s") or 1.0,
            ns_log_path=self.broker_ram_ns_log_path,
            enable=cfg.get("enable", False) is not False,
            api_port=cfg.get("api_port") or 15672,
            api_user=self.username,
            api_password=self.password,
            api_vhost=self.virtual_host,
        )

    def _start_broker_ram(self):
        """Begin sampling the queue host. Called from __init__, before any client
        has registered and long before anything is published, so the series opens
        on the host at rest. That idle stretch is the baseline every other number
        in broker_ram.log is read against; the run's own boundaries are recorded
        inside the series by mark('dispatch') and mark('finish').

        Never fatal: a telemetry channel that can't open must not stop a run from
        happening."""
        m = self._broker_ram
        if m is None or not m.enable:
            return
        try:
            ok = m.start()
        except Exception as e:
            src.Log.print_with_color(f"[BrokerRAM] sampler failed to start: {e}", "yellow")
            return
        if ok and m.source == "ssh":
            src.Log.print_with_color(
                f"[BrokerRAM] sampling {m.host} host RAM every {m.interval_s:.1f}s "
                f"-> {self.broker_ram_ns_log_path}", "cyan")
        elif ok:
            src.Log.print_with_color(
                f"[BrokerRAM] SSH unavailable ({m.error}); falling back to the "
                f"management API — 'used' is then the BROKER PROCESS's memory, "
                f"not {m.host}'s host RAM", "yellow")
        else:
            src.Log.print_with_color(
                f"[BrokerRAM] no RAM samples from {m.host}: {m.error}", "yellow")

    def _report_broker_ram(self):
        """Shutdown step: close the window, stop sampling, write broker_ram.log.

        Runs after every other collection, so the window covers the run AND the
        shutdown drain — the drain is exactly when a backed-up broker gives its
        memory back, and a curve that doesn't fall there is the signal that
        something is still holding messages.

        Then it keeps sampling for a short TAIL past the end of the run. Stopping
        at the last collection would make the final sample the one taken while the
        drain was still in flight, and the question this meter exists to answer —
        what does the host look like when the system is NOT running — would be
        answered with the busiest moment of the shutdown. A couple of seconds is
        enough to catch the release; it is deliberately not long enough to wait
        out the broker's own garbage collection, so a positive tail is 'not back
        yet', not proof of a leak."""
        m = self._broker_ram
        if m is None or not m.enable:
            return
        try:
            m.mark("finish")
            tail_s = float((self.broker_ram_cfg or {}).get("tail_s") or 2.0)
            if tail_s > 0:
                src.Log.print_with_color(
                    f"[BrokerRAM] run finished; sampling {tail_s:.1f}s more to see "
                    f"{m.host} settle", "cyan")
                time.sleep(tail_s)
            m.stop()
            s = m.write_summary(self.broker_ram_log_path)
        except Exception as e:
            src.Log.print_with_color(f"[BrokerRAM] report failed: {e}", "yellow")
            return
        if s is None:
            src.Log.print_with_color(
                f"[BrokerRAM] no samples collected: {m.error or 'unknown reason'}", "yellow")
            return
        host_ram = s["source"] == "ssh"
        print("=" * 60)
        print(f"  [QUEUE HOST RAM]  {s['host']}   "
              f"{'host memory (/proc/meminfo)' if host_ram else 'BROKER PROCESS only (management API)'}")
        print(f"  samples={s['samples']}  every {s['interval_s']:.1f}s  "
              f"over {s['span_s']:.1f}s"
              + (f"   total={s['total_mb']:.0f} MB" if host_ram else ""))
        print(f"  used   mean={s['used']['mean']:8.1f} MB   p95={s['used']['p95']:8.1f}   "
              f"max={s['used']['max']:8.1f}   ({s['used_pct']['max']:.1f}% at peak)")
        print(f"  delta  start={s['start_mb']:.1f} MB -> end={s['end_mb']:.1f} MB   "
              f"growth={s['growth_mb']:+.1f} MB   peak over start={s['peak_over_start_mb']:+.1f} MB")
        if host_ram:
            print(f"  rabbitmq process  mean={s['rss']['mean']:.1f} MB   "
                  f"max={s['rss']['max']:.1f} MB   swap_max={s['swap_max_mb']:.1f} MB")
        # The comparison the phases exist for: this host with the system running
        # against this host at rest, measured the same way in the same series.
        ph = s.get("phases") or {}
        if ph:
            print("  " + "-" * 56)
            for name, label in (("idle", "idle (before dispatch)"),
                                ("run", "running"),
                                ("tail", "after finish")):
                p = ph.get(name)
                if not p:
                    continue
                print(f"  [{name:<4}] {label:<24} {p['span_s']:8.1f}s  "
                      f"mean={p['used']['mean']:8.1f} MB   max={p['used']['max']:8.1f} MB   "
                      f"({p['samples']} samples)")
            idle, run, tail = ph.get("idle"), ph.get("run"), ph.get("tail")
            if idle and run:
                base = idle["used"]["mean"]
                print(f"  [cost] running the system costs {m.host} "
                      f"{run['used']['mean'] - base:+.1f} MB on average, "
                      f"{run['used']['max'] - base:+.1f} MB at peak, over idle")
                if tail:
                    print(f"         {tail['span_s']:.1f}s after finish it sits "
                          f"{tail['used']['mean'] - base:+.1f} MB over idle")
            elif not idle:
                print("  [cost] no idle samples — sampler started after dispatch, "
                      "so there is no at-rest reference this run")
        print("=" * 60)

    def _server_host_idle_pct(self):
        """OS-level idle share of the server's own machine over the run."""
        from src.FreeTime import host_cpu_times
        c0, c1 = self._server_cpu0, host_cpu_times()
        if not c0 or not c1:
            return None
        d_idle, d_total = c1[0] - c0[0], c1[1] - c0[1]
        if d_total <= 0:
            return None
        return max(0.0, min(100.0, 100.0 * d_idle / d_total))

    def _load_map_label_gt(self, gt_dir="map/label"):
        """Ground truth for server-side mAP: this server's own local copy of
        map/label/frame_NNNNNN.txt, 'class_id cx cy w h' normalized to the
        640x640 network input — same layout/convention as
        Scheduler._load_gt_dict, just read from map/label instead of
        datasets/groundtruth."""
        import torch
        gt_dict = {}
        if not os.path.isdir(gt_dir):
            return gt_dict
        for fname in sorted(os.listdir(gt_dir)):
            if not fname.endswith(".txt"):
                continue
            try:
                num = int(os.path.splitext(fname)[0].split("_")[-1])
            except ValueError:
                continue
            boxes, labels = [], []
            with open(os.path.join(gt_dir, fname)) as f:
                for line in f:
                    parts = line.strip().split()
                    if len(parts) < 5:
                        continue
                    cls, cx, cy, bw, bh = map(float, parts[:5])
                    boxes.append([(cx - bw / 2) * 640, (cy - bh / 2) * 640,
                                  (cx + bw / 2) * 640, (cy + bh / 2) * 640])
                    labels.append(int(cls))
            gt_dict[num] = {
                "boxes":  torch.tensor(boxes,  dtype=torch.float32) if boxes  else torch.zeros((0, 4)),
                "labels": torch.tensor(labels, dtype=torch.int64)   if labels else torch.zeros(0, dtype=torch.int64),
            }
        return gt_dict

    def _load_cluster_preds(self, pred_dir):
        """Parse one cluster's collected map/pred/frame_NNNNNN.txt files (written
        by Scheduler._write_pred_file: 'class_id cx cy w h confidence', 640-
        normalized) into {frame_num: {boxes, scores, labels}} tensors."""
        import torch
        preds = {}
        for fname in sorted(os.listdir(pred_dir)):
            if not fname.endswith(".txt"):
                continue
            try:
                num = int(os.path.splitext(fname)[0].split("_")[-1])
            except ValueError:
                continue
            boxes, scores, labels = [], [], []
            with open(os.path.join(pred_dir, fname)) as f:
                for line in f:
                    vals = line.strip().split()
                    if len(vals) < 6:
                        continue
                    cls = int(vals[0])
                    cx, cy, bw, bh, conf = map(float, vals[1:6])
                    boxes.append([(cx - bw / 2) * 640, (cy - bh / 2) * 640,
                                  (cx + bw / 2) * 640, (cy + bh / 2) * 640])
                    scores.append(conf)
                    labels.append(cls)
            preds[num] = {
                "boxes":  torch.tensor(boxes,  dtype=torch.float32) if boxes  else torch.zeros((0, 4)),
                "scores": torch.tensor(scores, dtype=torch.float32) if scores else torch.zeros(0),
                "labels": torch.tensor(labels, dtype=torch.int64)   if labels else torch.zeros(0, dtype=torch.int64),
            }
        return preds

    def _map_for_frames(self, gt_dict, pred_dict, frames):
        """mAP@50:95 and mAP@50 over exactly `frames` (frame numbers that exist in
        BOTH pred_dict and gt_dict). Returns (map50_95, map50, n_frames), or None
        if the metric is unavailable (see _new_map_metric) or the compute failed.

        Both pipelines below are just different framings of this one call, which is
        why they are guaranteed comparable. torchmetrics takes parallel lists, so
        this updates once with every frame instead of once per frame — with a
        sliding window the same frame is scored in up to W windows, and per-frame
        update() calls dominate the runtime."""
        metric = _new_map_metric()
        if metric is None:
            return None
        preds, targets = [], []
        for fn in frames:
            preds.append({"boxes":  pred_dict[fn]["boxes"],
                          "scores": pred_dict[fn]["scores"],
                          "labels": pred_dict[fn]["labels"]})
            targets.append(gt_dict[fn])
        if not preds:
            return None
        metric.update(preds, targets)
        try:
            res = metric.compute()
        except Exception as e:
            # Once per distinct message: this runs per window, so a systemic
            # failure would otherwise print the same line a dozen-plus times.
            _map_warn_once(f"[mAP] compute failed: {e}")
            return None
        return float(res["map"]), float(res["map_50"]), len(preds)

    def _frames_by_batch(self, gt_dict, pred_dict, batch_size):
        """Group the scorable frames of one cluster back into the batches they were
        processed in: frame_num = batch_id * batch_size + img_idx + 1 (see
        Scheduler._update_map), so batch_id = (frame_num - 1) // batch_size.

        Ground truth is the cap: a frame with no label file is dropped here, and
        the workers already stop writing pred files past the last labelled frame
        (Scheduler._batch_past_gt), so batches beyond the labelled range simply
        never appear. Values are sorted so a window's frames stay in frame order."""
        frames_by_batch = {}
        for frame_num in pred_dict:
            if frame_num not in gt_dict:
                continue
            b = (frame_num - 1) // batch_size
            frames_by_batch.setdefault(b, []).append(frame_num)
        for b in frames_by_batch:
            frames_by_batch[b].sort()
        return frames_by_batch

    def _map_pipeline_window(self, gt_dict, pred_dict, batch_size, cluster_id,
                             t_ns, window_batches=16):
        """PIPELINE 1 — sliding-window mAP, the accuracy counterpart of window_fps.

        Slide a window of `window_batches` consecutive PRESENT batches one batch at
        a time and compute a full mAP over each window's frames. Every window is
        logged to map_window.log as its own line, giving an mAP-over-the-run series
        that can be lined up against batch_done_ns.log / cut_change_ns.log to see
        how accuracy responded to a split-point change. The returned mean over
        windows is the single number for the summary.

        Fewer batches than the window size → one window covering everything (so a
        short run still reports something instead of nothing).

        Returns (mean_map50_95, mean_map50, n_windows, W) or None."""
        frames_by_batch = self._frames_by_batch(gt_dict, pred_dict, batch_size)
        if not frames_by_batch:
            return None
        batch_ids = sorted(frames_by_batch)
        W = min(window_batches, len(batch_ids))
        vals, vals_50 = [], []
        with open(self.map_window_log_path, "a") as f:
            for i, start in enumerate(range(0, len(batch_ids) - W + 1)):
                win = batch_ids[start:start + W]
                frames = [fn for b in win for fn in frames_by_batch[b]]
                r = self._map_for_frames(gt_dict, pred_dict, frames)
                if r is None:
                    continue
                m, m50, n = r
                if m < 0:      # torchmetrics returns -1 when a window has nothing to score
                    continue
                vals.append(m)
                vals_50.append(m50)
                line = (f"{t_ns} cluster={cluster_id} window={i} "
                        f"batches={win[0]}-{win[-1]} frames={n} "
                        f"mAP50_95={m:.4f} mAP50={m50:.4f}")
                f.write(line + "\n")
                src.Log.print_with_color(f"[mAP] {line}", "cyan")
        if not vals:
            return None
        return (sum(vals) / len(vals), sum(vals_50) / len(vals_50), len(vals), W)

    def _map_pipeline_all(self, gt_dict, pred_dict, batch_size):
        """PIPELINE 2 — whole-run mAP over ALL frames at once, the accuracy
        counterpart of SYSTEM FPS: no windowing, one metric fed every scorable
        frame of the run, so each frame is weighted exactly once (a sliding-window
        mean over-weights the frames that sit in more windows).

        The frame count is bounded by the ground truth, not by the video: map/label/
        labels only the first N frames, so anything the workers processed past that
        has no label (and no pred file) and is excluded. Returns
        (map50_95, map50, n_matched, n_gt) or None."""
        frames_by_batch = self._frames_by_batch(gt_dict, pred_dict, batch_size)
        frames = sorted(fn for fns in frames_by_batch.values() for fn in fns)
        r = self._map_for_frames(gt_dict, pred_dict, frames)
        if r is None:
            return None
        m, m50, n = r
        if m < 0:
            return None
        return (m, m50, n, len(gt_dict))

    def _print_map_summary(self, rows):
        """Final mAP summary, one block per pipeline, in the same framed style as
        the FPS summary. `rows` is one dict per cluster: {cluster_id, win, all}
        where win/all are the pipeline return tuples (or None if that pipeline had
        nothing to score)."""
        print("=" * 60)
        wins = [r for r in rows if r["win"]]
        alls = [r for r in rows if r["all"]]
        if wins:
            W = wins[0]["win"][3]
            print(f"  [mAP PIPELINE 1]  sliding window, W={W} batches, step 1 batch")
            for r in wins:
                m, m50, nw, _ = r["win"]
                print(f"    cluster={r['cluster_id']:<24} mAP@50:95={m:7.4f}   "
                      f"mAP@50={m50:7.4f}   (mean of {nw} window(s))")
            if len(wins) > 1:
                mm = sum(r["win"][0] for r in wins) / len(wins)
                mm50 = sum(r["win"][1] for r in wins) / len(wins)
                print(f"    {'OVERALL':<32} mAP@50:95={mm:7.4f}   "
                      f"mAP@50={mm50:7.4f}   (avg over {len(wins)} cluster(s))")
        if alls:
            n_gt = alls[0]["all"][3]
            print(f"  [mAP PIPELINE 2]  all frames, GT-limited ({n_gt} labelled frame(s))")
            for r in alls:
                m, m50, n, ngt = r["all"]
                print(f"    cluster={r['cluster_id']:<24} mAP@50:95={m:7.4f}   "
                      f"mAP@50={m50:7.4f}   ({n}/{ngt} GT frame(s) matched)")
            if len(alls) > 1:
                mm = sum(r["all"][0] for r in alls) / len(alls)
                mm50 = sum(r["all"][1] for r in alls) / len(alls)
                print(f"    {'OVERALL':<32} mAP@50:95={mm:7.4f}   "
                      f"mAP@50={mm50:7.4f}   (avg over {len(alls)} cluster(s))")
        if not wins and not alls:
            # Distinguish "ran, found nothing to score" from "couldn't run at all" —
            # they look identical here but need completely different fixes.
            if _MAP_BACKEND_ERR:
                print("  [mAP]  unavailable — torchmetrics has no COCO backend "
                      "(pip install faster-coco-eval)")
            else:
                print("  [mAP]  no scorable frames — nothing to report")
        print("=" * 60)

    def _collect_map_pred(self, timeout_s=None, window_batches=None):
        """Shutdown step: drain every cloud's zipped map/pred/ directory from
        map_pred_queue (tagged with cluster_id), unpack into a per-cluster folder
        under map/pred_collected/ (write-once — a frame index already unpacked
        for a cluster is kept, never overwritten, so a second edge in the same
        cluster reprocessing the same video can't clobber it), match against this
        server's own local map/label/ ground truth, then run BOTH mAP pipelines
        over each cluster's predictions:

          pipeline 1 — sliding window of `window_batches` consecutive batches,
                       stepped one batch at a time (_map_pipeline_window)
          pipeline 2 — every scorable frame in one metric, capped by the number
                       of labelled frames (_map_pipeline_all)

        Both are reported on the console and appended to map.log (pipeline 1 also
        writes its per-window series to map_window.log). Only the tier that ran
        postprocess_yolo ever publishes here (only_edge edges, or the cloud in
        split/only_cloud), so 'expected' is scoped to that tier."""
        import zipfile
        import io
        import shutil

        # map.enable is a SERVER-side switch that reaches the devices in the START
        # message, so the devices already know not to produce pred files. This
        # collection has to honour the same flag or it polls an empty queue for the
        # whole timeout and then warns '0/N pred report(s)' — a 30s stall plus a
        # scary message on every run that deliberately turned mAP off.
        # Same default as Scheduler.map_on, so the two can't disagree.
        if not bool(self.map_cfg.get("enable", True)):
            src.Log.print_with_color(
                "[mAP] skipped (map.enable=False) — no pred files were produced", "yellow")
            return

        if timeout_s is None:
            timeout_s = float(self.map_cfg.get("collect_timeout_s", 30.0))
        if window_batches is None:
            window_batches = int(self.map_cfg.get("window_batches", 16))

        mode = self._get_mode()
        if mode == "only_edge":
            expected = sum(1 for _, lid in self.list_clients if lid == 1)
        else:
            expected = sum(1 for _, lid in self.list_clients if lid == len(self.total_clients))
        if expected == 0:
            return

        collect_root = "map/pred_collected"
        if os.path.isdir(collect_root):
            shutil.rmtree(collect_root, ignore_errors=True)

        reported = set()
        cluster_dirs = {}   # cluster_id -> directory path
        deadline = time.time() + timeout_s
        while len(reported) < expected and time.time() < deadline:
            method_frame, _, body = self.channel.basic_get(queue='map_pred_queue', auto_ack=True)
            if not method_frame:
                time.sleep(0.2)
                continue
            try:
                msg = pickle.loads(body)
            except Exception:
                continue
            if not isinstance(msg, dict) or msg.get("action") != "MAP_PRED":
                continue
            reported.add(str(msg.get("client_id")))
            cluster_id = str(msg.get("cluster_id", "intermediate_queue"))
            safe_name = "".join(c if c.isalnum() or c in "_-" else "_" for c in cluster_id)
            cluster_dir = os.path.join(collect_root, safe_name)
            os.makedirs(cluster_dir, exist_ok=True)
            cluster_dirs[cluster_id] = cluster_dir
            try:
                with zipfile.ZipFile(io.BytesIO(msg["zip_bytes"])) as zf:
                    for name in zf.namelist():
                        dest = os.path.join(cluster_dir, name)
                        if os.path.exists(dest):
                            continue
                        with open(dest, "wb") as out:
                            out.write(zf.read(name))
                src.Log.print_with_color(
                    f"[mAP] Received pred files for cluster '{cluster_id}' "
                    f"from client {msg.get('client_id')}", "cyan")
            except Exception as e:
                src.Log.print_with_color(f"[mAP] Failed to unpack pred zip: {e}", "yellow")
        if len(reported) < expected:
            src.Log.print_with_color(
                f"[mAP] Collected {len(reported)}/{expected} pred report(s) before timeout", "yellow")
        if not cluster_dirs:
            return

        gt_dict = self._load_map_label_gt()
        if not gt_dict:
            src.Log.print_with_color("[mAP] Skipped: no ground truth found in map/label/ on this server", "yellow")
            return

        t_ns = time.time_ns()
        rows = []
        for cluster_id, cluster_dir in sorted(cluster_dirs.items()):
            pred_dict = self._load_cluster_preds(cluster_dir)
            if not pred_dict:
                continue
            # Pipeline 1 first: it streams its per-window lines as it goes, so the
            # console shows progress before pipeline 2's single long compute.
            win = self._map_pipeline_window(gt_dict, pred_dict, self.batch_size,
                                           cluster_id, t_ns, window_batches)
            alll = self._map_pipeline_all(gt_dict, pred_dict, self.batch_size)
            rows.append({"cluster_id": cluster_id, "win": win, "all": alll})

        with open(self.map_log_path, "a") as f:
            for r in rows:
                if r["win"]:
                    m, m50, nw, W = r["win"]
                    line = (f"{t_ns} cluster={r['cluster_id']} WINDOW "
                            f"mAP50_95={m:.4f} mAP50={m50:.4f} "
                            f"(mean of {nw} window(s) x {W} batches, step 1)")
                    f.write(line + "\n")
                if r["all"]:
                    m, m50, n, ngt = r["all"]
                    line = (f"{t_ns} cluster={r['cluster_id']} ALL "
                            f"mAP50_95={m:.4f} mAP50={m50:.4f} "
                            f"({n}/{ngt} GT frame(s) matched)")
                    f.write(line + "\n")
            # OVERALL goes in the log whenever at least one cluster scored, even
            # with a single cluster (where it just repeats that cluster's numbers) —
            # this is the machine-read file, so a parser can always find one
            # authoritative line per pipeline. The console summary omits it in the
            # single-cluster case, where it would only be visual duplication.
            for key, tag in (("win", "WINDOW"), ("all", "ALL")):
                vals = [r[key] for r in rows if r[key]]
                if vals:
                    m = sum(v[0] for v in vals) / len(vals)
                    m50 = sum(v[1] for v in vals) / len(vals)
                    f.write(f"{t_ns} OVERALL {tag} mAP50_95={m:.4f} mAP50={m50:.4f} "
                            f"(avg over {len(vals)} cluster(s))\n")

        self._print_map_summary(rows)

    # ─── Run archive ──────────────────────────────────────────────────────────

    def _run_tag(self):
        """Which configuration produced this run — the suffix of the archive dir.

        A non-split run is tagged with its experiment mode (only_cloud /
        only_edge). A split run is 'dynamic' when the adaptive controller was
        free to move the cut point during the run, and 'split' when the cut
        stayed where clustering / cut-layer put it.
        """
        mode = self._get_mode()
        if mode != "split":
            return mode
        return "dynamic" if self.adaptive_cfg.get("enable", False) else "split"

    def _archive_results(self):
        """Gather this run's result logs into results/results_<MMDD>_<HHMM>_<tag>/.

        Called once, after the last shutdown pipeline (mAP) has written its
        files, so the archive is a complete snapshot of the run.

        Copies rather than moves: log-path keeps its own copies where every
        existing reader expects them, and the next run truncates them itself
        (see __init__) instead of starting against a half-empty directory.
        """
        import shutil
        # Everything a run produces. cut_change_ns.log only exists when the
        # adaptive controller ran; empty files are skipped rather than archived
        # as misleading zero-length results.
        result_files = (
            "batch_done_ns.log",
            "fps_cluster.log",
            "fps_cluster_ns.log",
            "latency_cluster.log",
            "map.log",
            "map_window.log",
            "utilization.log",
            "utilization_cluster.log",
            "free_time.log",
            "free_time_cluster.log",
            "free_time_series.log",
            "message_size.log",
            "message_size_series.log",
            "broker_ram.log",
            "broker_ram_ns.log",
            "cut_change_ns.log",
        )
        log_path = self.config["log-path"]
        base = os.path.join(log_path, "results",
                            f"results_{time.strftime('%m%d_%H%M')}_{self._run_tag()}")
        # Two runs finishing inside the same minute must not overwrite each other.
        out_dir, n = base, 2
        while os.path.exists(out_dir):
            out_dir, n = f"{base}-{n}", n + 1
        os.makedirs(out_dir)

        # cut_change_ns.log is only truncated when the adaptive controller runs
        # (see __init__), so with adaptive off the file still holds the PREVIOUS
        # adaptive run's changes. Archiving it then produces a result folder whose
        # cut log describes a different run — the 0730_0917 split archive shipped
        # cut changes for an intermediate_queue_2 that run never had. A run with no
        # controller has no cut changes, so the honest archive omits the file.
        skip = set()
        if not self.adaptive_cfg.get("enable", False):
            skip.add("cut_change_ns.log")

        copied = []
        for name in result_files:
            if name in skip:
                continue
            path = os.path.join(log_path, name)
            if not os.path.isfile(path) or os.path.getsize(path) == 0:
                continue
            shutil.copy2(path, os.path.join(out_dir, name))
            copied.append(name)

        # Per-batch metrics (metrics_raw_<queue>_<clientid>.csv, written by
        # Scheduler.write_metrics) are the ONLY record of per-batch message size,
        # cut and latency. Without them a run's summary logs can show a shared
        # bottleneck without saying what saturated — diagnosing the 0730 edge
        # ceiling needed exactly this file and it had never been archived. Only
        # devices that share the server's filesystem contribute; the rest keep
        # theirs locally, which is why this is best-effort and never fatal.
        raw = sorted(glob.glob(os.path.join(log_path, "metrics_raw_*.csv")))
        if raw:
            raw_dir = os.path.join(out_dir, "metrics_raw")
            try:
                os.makedirs(raw_dir, exist_ok=True)
                for path in raw:
                    if os.path.getsize(path) == 0:
                        continue
                    shutil.copy2(path, os.path.join(raw_dir, os.path.basename(path)))
                    copied.append(os.path.basename(path))
            except OSError as e:
                src.Log.print_with_color(f"[Archive] metrics_raw not copied: {e}", "yellow")
        else:
            src.Log.print_with_color(
                "[Archive] no metrics_raw_*.csv here — collect them from the edge/cloud "
                "machines if you need per-batch message sizes", "yellow")

        # Per-device free-time logs (free_time_<role>_<cluster>_<id>.log, written
        # by Scheduler._send_free_time). Same best-effort rule as metrics_raw:
        # only devices sharing this filesystem contribute, and the server's own
        # roll-up above already carries every device's numbers either way — this
        # just keeps the per-device breakdowns (per-kind, per-lane, per-bucket)
        # with the run they belong to.
        dev_ft = sorted(glob.glob(os.path.join(log_path, "free_time_edge_*.log"))
                        + glob.glob(os.path.join(log_path, "free_time_cloud_*.log")))
        if dev_ft:
            ft_dir = os.path.join(out_dir, "free_time_devices")
            try:
                os.makedirs(ft_dir, exist_ok=True)
                for path in dev_ft:
                    if os.path.getsize(path) == 0:
                        continue
                    shutil.copy2(path, os.path.join(ft_dir, os.path.basename(path)))
                    copied.append(os.path.basename(path))
            except OSError as e:
                src.Log.print_with_color(f"[Archive] free_time device logs not copied: {e}", "yellow")

        # The config that produced these numbers, so the archive reads on its own
        # months later without having to guess the cut/batch/cluster settings.
        try:
            shutil.copy2("config.yaml", os.path.join(out_dir, "config.yaml"))
        except OSError as e:
            src.Log.print_with_color(f"[Archive] config.yaml not copied: {e}", "yellow")

        src.Log.print_with_color(
            f"[Archive] {len(copied)} result file(s) -> {out_dir}", "green")
        if not copied:
            src.Log.print_with_color(
                "[Archive] WARNING: every result log was missing or empty", "yellow")
        return out_dir

    def send_to_response(self, client_id, message):
        reply_queue_name = f"reply_{client_id}"
        self.reply_channel.queue_declare(reply_queue_name, durable=False)
        src.Log.print_with_color(f"[>>>] Sent notification to client {client_id}", "red")
        self.reply_channel.basic_publish(exchange='', routing_key=reply_queue_name, body=message)

    def start(self):
        self.channel.start_consuming()
        # start_consuming returns once _finish_fps stopped the consumer (FPS
        # drain + summary done) — now gather every device's utilization report
        # before closing the connection.
        self._collect_utilization()
        self._collect_free_time()
        self._collect_msg_size()
        self._collect_map_pred()
        # Last meter to close: it is the only one still sampling, and the drain
        # above is part of what it is measuring.
        self._report_broker_ram()
        # Every result file is final by here — snapshot them into one run folder.
        # Guarded so a filesystem problem in the archive can't leave the broker
        # connection open or skip the clean exit.
        try:
            self._archive_results()
        except Exception as e:
            src.Log.print_with_color(f"[Archive] failed: {e}", "red")
        self.connection.close()
        sys.exit(0)

    # ─── Adaptive split-point controller (Mechanic 1) ──────────────────────────

    def _count_layers(self):
        """Total layer count L of the model (cut is clamped to [1, L-1])."""
        try:
            import torch
            ckpt = torch.load(f"{self.model_name}.pt", map_location="cpu", weights_only=False)
            n = len(ckpt["model"].model)
            del ckpt
            return int(n)
        except Exception as e:
            src.Log.print_with_color(f"[Adaptive] could not count layers: {e}", "yellow")
            return None

    def _build_cluster_state(self, mode, clients_to_notify, splits):
        """Group edge clients by their intermediate queue and record the initial
        cut, so the controller can nudge each cluster independently."""
        self.cluster_state = {}
        if mode in ("only_edge", "only_cloud") or not self.adaptive_cfg.get("enable", False):
            return

        for (client_id, layer_id) in clients_to_notify:
            if layer_id != 1:
                continue
            assign = self.client_assignments.get(client_id, {})
            queue = assign.get("queue_name", "intermediate_queue")
            cut = assign.get("splits", splits)
            if cut is None:
                continue
            st = self.cluster_state.setdefault(queue, {"queue": queue, "cut": int(cut), "edges": []})
            st["edges"].append(client_id)

    def _start_adaptive_controller(self):
        if not self.cluster_state:
            return
        self._num_layers = self._count_layers()
        if not self._num_layers:
            src.Log.print_with_color("[Adaptive] disabled (layer count unknown)", "yellow")
            return
        # Keep the server's tracked cut in the same [1, L-1] range the edge clamps to.
        for st in self.cluster_state.values():
            st["cut"] = max(1, min(int(st["cut"]), self._num_layers - 1))

        # Per-cut estimated message size (MB), so the controller never moves the cut
        # to a point whose feature map would exceed the broker's max_message_size.
        try:
            # Same wire-size scaling as the initial-cut cap, so the controller and
            # the clustering guard can never disagree about which cuts are legal.
            self._cut_sizes = self._wire_cut_sizes(
                get_cut_data_sizes(self.model_name, self.batch_size))
        except Exception as e:
            src.Log.print_with_color(f"[Adaptive] cut-size table unavailable ({e}); size guard off", "yellow")
            self._cut_sizes = None
        cap = float(self.adaptive_cfg.get("max_message_mb", 15.0))
        for q, st in self.cluster_state.items():
            est = self._cut_msg_mb(st["cut"])
            if est is not None and est > cap:
                src.Log.print_with_color(
                    f"[Adaptive] WARNING: initial cut={st['cut']} for {q} est ~{est:.1f}MB > cap {cap}MB. "
                    f"First send may exceed the broker limit — raise RabbitMQ max_message_size "
                    f"and adaptive.max_message_mb, or lower batch-size.", "red")

        self._adaptive_thread = threading.Thread(target=self._adaptive_loop, daemon=True)
        self._adaptive_thread.start()
        src.Log.print_with_color(
            f"[Adaptive] controller started for {len(self.cluster_state)} cluster(s), "
            f"L={self._num_layers}, cap={cap}MB, initial cuts="
            f"{ {q: s['cut'] for q, s in self.cluster_state.items()} }", "green")

    def _cut_msg_mb(self, cut):
        """Estimated intermediate-message size (MB) when the edge runs `cut` layers,
        from the model's cut-size table. Returns None if unknown. The table is
        indexed by solver cut = splits-1 (see get_cut_data_sizes)."""
        sizes = getattr(self, "_cut_sizes", None)
        if sizes is None:
            return None
        idx = int(cut) - 1
        if 0 <= idx < len(sizes):
            return float(sizes[idx])
        return None

    def _nearest_safe_cut(self, cur, direction, step, cap, min_cut, max_cut):
        """Nearest cut to `cur` in `direction` (+1 deeper / -1 shallower) whose
        estimated message fits under `cap` MB. Starts `step` away and skips over
        any unsafe cuts. Returns `cur` if none is safe in that direction."""
        c = cur + direction * max(1, step)
        while min_cut <= c <= max_cut:
            est = self._cut_msg_mb(c)
            if est is None or est <= cap:
                return c
            c += direction
        return cur

    def _queue_stats(self, queue_name):
        """Return (depth, cumulative_batch_count) for a queue via the RabbitMQ
        management HTTP API, or (None, None) if unavailable. Batch count uses
        message publish stats (one publish == one edge batch)."""
        import requests
        from requests.auth import HTTPBasicAuth
        from urllib.parse import quote
        url = (f"http://{self.address}:15672/api/queues/"
               f"{quote(self.virtual_host, safe='')}/{quote(queue_name, safe='')}")
        try:
            r = requests.get(url, auth=HTTPBasicAuth(self.username, self.password), timeout=2)
            if r.status_code != 200:
                return None, None
            data = r.json()
            depth = int(data.get("messages", 0) or 0)
            stats = data.get("message_stats", {}) or {}
            count = stats.get("publish")
            if count is None:
                count = stats.get("get_no_ack", stats.get("deliver_get", 0))
            return depth, int(count or 0)
        except Exception:
            return None, None

    def _adaptive_loop(self):
        cfg = self.adaptive_cfg
        poll     = float(cfg.get("poll_interval_s", 0.25))
        N        = int(cfg.get("batches_per_check", 20))
        high_t   = int(cfg.get("high_threshold", 8))
        low_t    = int(cfg.get("low_threshold", 1))
        high_r   = float(cfg.get("high_ratio", 0.6))
        low_r    = float(cfg.get("low_ratio", 0.6))
        step     = int(cfg.get("step", 1))
        cooldown = int(cfg.get("cooldown_batches", 20))
        cap      = float(cfg.get("max_message_mb", 15.0))
        min_cut, max_cut = 1, self._num_layers - 1

        credentials = pika.PlainCredentials(self.username, self.password)
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(
                host=self.address, port=5672, virtual_host=f"{self.virtual_host}",
                credentials=credentials, heartbeat=0, blocked_connection_timeout=600))
            ch = conn.channel()
        except Exception as e:
            src.Log.print_with_color(f"[Adaptive] control connection failed, controller off: {e}", "yellow")
            return

        samples      = {q: [] for q in self.cluster_state}
        baseline     = {q: None for q in self.cluster_state}   # batch count at window start
        last_change  = {q: None for q in self.cluster_state}   # batch count at last cut change

        while not self._stopping:
            time.sleep(poll)
            for q, st in self.cluster_state.items():
                depth, count = self._queue_stats(q)
                if depth is None:
                    continue
                samples[q].append(depth)
                if baseline[q] is None:
                    baseline[q] = count
                if last_change[q] is None:
                    last_change[q] = count
                if count - baseline[q] < N:
                    continue

                window = samples[q]
                high_frac = sum(1 for d in window if d >= high_t) / len(window)
                low_frac  = sum(1 for d in window if d <= low_t) / len(window)
                cur = st["cut"]
                direction = 0
                if high_frac >= high_r and cur < max_cut:
                    direction = +1     # cloud bottleneck -> cut deeper (edge does more)
                elif low_frac >= low_r and cur > min_cut:
                    direction = -1     # cloud starved   -> cut shallower (cloud does more)

                # Move in the desired direction to the NEAREST cut whose feature map
                # fits under the broker's max_message_size. Skips over unsafe cuts
                # (the size profile is non-monotonic) instead of giving up at cur±step.
                new = cur
                if direction != 0:
                    new = self._nearest_safe_cut(cur, direction, step, cap, min_cut, max_cut)
                    if new == cur:
                        src.Log.print_with_color(
                            f"[Adaptive] {q}: want {'deeper' if direction > 0 else 'shallower'} "
                            f"from cut {cur} but no cut ≤ cap {cap}MB in that direction — staying", "yellow")

                if new != cur and (count - last_change[q]) >= cooldown:
                    st["cut"] = new
                    last_change[q] = count
                    self._broadcast_setcut(ch, st["edges"], q, cur, new, high_frac, low_frac)

                samples[q] = []
                baseline[q] = count

        try:
            conn.close()
        except Exception:
            pass

    def _broadcast_setcut(self, ch, edges, queue, old_cut, new_cut, high_frac, low_frac):
        t_ns = time.time_ns()
        for eid in edges:
            ctrl_q = f"ctrl_{eid}"
            try:
                ch.queue_declare(ctrl_q, durable=False)
                ch.basic_publish(exchange='', routing_key=ctrl_q,
                                 body=pickle.dumps({"action": "SET_CUT", "cut": int(new_cut)}))
            except Exception as e:
                src.Log.print_with_color(f"[Adaptive] SET_CUT publish failed for {eid}: {e}", "yellow")
        word = "deeper" if new_cut > old_cut else "shallower"
        with open(self.cut_log_path, "a") as f:
            f.write(f"{t_ns} {queue}: cut {old_cut}->{new_cut} {word}\n")
        direction = f"{word} ({'edge+' if new_cut > old_cut else 'cloud+'})"
        src.Log.print_with_color(
            f"[Adaptive] {queue}: cut {old_cut}->{new_cut} {direction} "
            f"(high={high_frac:.2f} low={low_frac:.2f})", "green")

    @staticmethod
    def _natural_key(s):
        """Sort key where machine-2 < machine-10 (lexicographic puts 10 first)."""
        return [int(p) if p.isdigit() else p for p in re.split(r"(\d+)", str(s))]

    def _ordered_clients(self, layer_id, require_profile=False):
        """Clients of one role in an order that means the same thing every run.

        list_clients is in REGISTER arrival order, and with 9 edge processes
        started in parallel that order is a race: the same VM is row 3 in one run
        and row 7 in the next. Everything downstream is indexed by that position —
        the solver's feature matrix, and Clustering.agglomerative_cluster which
        numbers clusters by their smallest member index — so the printed cluster
        ids and their member lists moved between runs even when the partition of
        real machines did not. Sorting by --name (naturally, so machine-2 comes
        before machine-10) makes row i the same machine in every run.

        The name is used ONLY for ordering; identity on the wire is still the
        uuid. Unnamed clients sort last, by uuid, so a partly-named fleet is still
        deterministic — but pass --name if you want to read the output.

        Never re-derive this order at the call site: notify_clients maps solver
        row i back to a client, so a different order there would send cluster k's
        cut to the wrong machines.
        """
        out = [(cid, lid) for cid, lid in self.list_clients if lid == layer_id]
        if require_profile:
            out = [e for e in out if str(e[0]) in self.client_profile_data]
        return sorted(out, key=lambda e: (
            self.client_name_data.get(str(e[0])) is None,
            self._natural_key(self.client_name_data.get(str(e[0]), "")),
            str(e[0]),
        ))

    def _run_hungarian(self):
        cfg = self.config.get("clustering", {})
        network_rate = float(cfg.get("network_rate_mb_s", 1000.0))
        max_clusters = cfg.get("max_clusters", 1)

        # Dùng real profiling data nếu tất cả client đã gửi
        edge_entries = self._ordered_clients(1, require_profile=True)
        cloud_entries = self._ordered_clients(len(self.total_clients), require_profile=True)
        edge_times_list = [self.client_profile_data[str(cid)] for cid, _ in edge_entries]
        cloud_times_list = [self.client_profile_data[str(cid)] for cid, _ in cloud_entries]
        n_edge = sum(1 for _, lid in self.list_clients if lid == 1)
        n_cloud = sum(1 for _, lid in self.list_clients if lid == len(self.total_clients))
        profile_source = cfg.get("profile_source", "auto")
        has_real = (len(edge_times_list) == n_edge and len(cloud_times_list) == n_cloud
                    and n_edge > 0 and n_cloud > 0)

        if profile_source == "real" and not has_real:
            raise RuntimeError("[Clustering] profile_source=real nhưng chưa có đủ profiling từ clients")

        use_real = has_real if profile_source == "auto" else (profile_source == "real")

        if use_real:
            src.Log.print_with_color(
                f"[Clustering] Using REAL profiles ({n_edge} edge, {n_cloud} cloud) [profile_source={profile_source}]", "cyan")
            N = len(edge_times_list)
            M = len(cloud_times_list)
            edge_clients = [cid for cid, _ in edge_entries]
            rates_matrix = np.array([
                [self.client_bandwidth_data.get(str(cid), network_rate)] * M
                for cid in edge_clients
            ]) if edge_clients else np.full((N, M), network_rate)
            self._audit_bandwidth(edge_clients, network_rate)
            cloud_clients = [cid for cid, _ in cloud_entries]
            solver = DeterministicSimilarityAssignmentSolver(
                client_layer_times=np.vstack(edge_times_list),
                server_layer_times=np.vstack(cloud_times_list),
                cut_data_sizes=get_cut_data_sizes(self.model_name, self.batch_size),
                input_data_size=get_raw_input_mb(self.batch_size),
                network_rates=rates_matrix,
            )
            solver.client_type_names = [
                self.client_name_data.get(str(cid), f"edge_{str(cid)[:8]}")
                for cid in edge_clients
            ]
            solver.cloud_type_names = [
                self.client_name_data.get(str(cid), f"cloud_{str(cid)[:8]}")
                for cid in cloud_clients
            ]
            result = solver.solve_best_over_k("hungarian", max_clusters=max_clusters)["best_result"]
            print_result(result, solver, title="HUNGARIAN MATCHING RESULT (real profiles)")
            self._dump_clustering_input(solver, edge_clients, cloud_clients,
                                        rates_matrix, network_rate, max_clusters)
            # Exactly the rows the solver saw — the profiled clients, in the order
            # they were stacked. An edge with no profile is deliberately absent:
            # it has no row, so it has no cluster, and notify_clients falls back
            # to the default queue for it rather than reading someone else's.
            edge_order, cloud_order = edge_entries, cloud_entries
        else:
            src.Log.print_with_color(
                f"[Clustering] Using SIMULATED profiles (DEVICE_A/B/C hardcoded) [profile_source={profile_source}]", "yellow")
            manual_cfg = ManualExperimentConfig(
                num_A=cfg.get("num_A", 1),
                num_B=cfg.get("num_B", 0),
                num_C=cfg.get("num_C", 0),
                num_cloud=cfg.get("num_cloud", 1),
                network_rate_mb_s=network_rate,
                max_clusters=max_clusters,
                exact_max_k=max_clusters,
                model_name=self.model_name,
                batch_size=self.batch_size,
                input_data_mb=get_raw_input_mb(self.batch_size),
            )
            results = run_manual_hungarian_case(manual_cfg)
            solver = results["solver"]
            result = results["hungarian"]
            # Simulated rows are num_A A's then num_B B's then num_C C's, with no
            # tie to any real machine, so which machine lands on which row is a
            # free choice. Take the stable one.
            edge_order = self._ordered_clients(1)
            cloud_order = self._ordered_clients(len(self.total_clients))

        # The order the solver's rows were built in. notify_clients maps row i
        # back to a client and MUST use this same list, never re-derive it.
        self.cluster_edge_order = edge_order
        self.cluster_cloud_order = cloud_order
        return solver, result

    def _dump_clustering_input(self, solver, edge_clients, cloud_clients,
                               rates_matrix, network_rate, max_clusters):
        """Write everything the solver was given to clustering_input.json.

        The server prints only the solver's ANSWER (K, best_cuts, throughput), so
        when the answer changes between runs there is nothing to compare — you
        cannot tell whether an edge profiled slower, a bandwidth sample came out
        high, or nothing moved at all and only the row order changed. This is the
        input side of that pair, and tools/replay_clustering.py re-runs the exact
        same solver on it offline (no broker, no model needed) and sweeps the one
        input that is genuinely uncertain, the per-edge egress rate.

        Never fatal: a failed dump costs a diagnostic, not the run."""
        try:
            path = f"{self.config['log-path']}/clustering_input.json"
            names = getattr(solver, "client_type_names", [])
            cnames = getattr(solver, "cloud_type_names", [])
            data = {
                "model_name": self.model_name,
                "batch_size": self.batch_size,
                "max_clusters": int(max_clusters),
                "network_rate_mb_s": float(network_rate),
                # No per-edge divisor is applied anywhere (see _audit_bandwidth):
                # each edge's own measurement IS its share when the edges measure
                # concurrently. Recorded so the replay prints the same assumption.
                "egress_share": 1,
                "compress": dict(self.compress),
                "input_data_size_mb": float(get_raw_input_mb(self.batch_size)),
                "cut_data_sizes_mb": [
                    float(v) for v in get_cut_data_sizes(self.model_name, self.batch_size)
                ],
                "edges": [
                    {
                        "client_id": str(cid),
                        "name": names[i] if i < len(names) else None,
                        "layer_times_s": [float(v) for v in solver.client_layer_times[i]],
                        "measured_mb_s": self.client_bandwidth_data.get(str(cid)),
                        "rate_used_mb_s": float(rates_matrix[i][0]),
                    }
                    for i, cid in enumerate(edge_clients)
                ],
                "clouds": [
                    {
                        "client_id": str(cid),
                        "name": cnames[j] if j < len(cnames) else None,
                        "layer_times_s": [float(v) for v in solver.server_layer_times[j]],
                    }
                    for j, cid in enumerate(cloud_clients)
                ],
            }
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            src.Log.print_with_color(
                f"[Clustering] solver input saved to {path} "
                f"(replay: python tools/replay_clustering.py {path})", "cyan")
        except Exception as e:
            src.Log.print_with_color(
                f"[Clustering] could not save clustering_input.json: {e}", "yellow")

    def _audit_bandwidth(self, edge_clients, network_rate):
        """Print every edge's measured egress rate plus the spread, and warn when
        the spread says the measurements did not overlap.

        Why this matters more than it looks: the solver treats edges as independent
        parallel producers (Clustering.pair_metrics_for_cut sums 1/tau_i over
        edges), so each rate must be that edge's own achievable SHARE, not the
        link's total. That is what you get for free IF the co-located edges measure
        simultaneously — contention is then already inside each sample and no
        divisor belongs anywhere. It is NOT what you get if they measure one after
        another: each one then sees an idle link, reports the full rate, and the
        solver believes the cluster has N times the egress it really has, which
        makes shallow cuts (big feature maps) look cheap.

        Clients measure right after profiling, so a profile cache miss on some
        machines is enough to stagger them. A tight spread is consistent with a
        concurrent measurement; a wide one means the number is optimistic and the
        chosen cut should not be trusted."""
        rates = [self.client_bandwidth_data.get(str(cid)) for cid in edge_clients]
        known = [r for r in rates if r is not None]
        if not known:
            src.Log.print_with_color(
                f"[Bandwidth] no client measurements; using network_rate_mb_s="
                f"{network_rate} MB/s for all edges", "yellow")
            return
        lo, hi = min(known), max(known)
        src.Log.print_with_color(
            f"[Bandwidth] {len(known)}/{len(rates)} edges measured: "
            f"min={lo:.1f} median={float(np.median(known)):.1f} max={hi:.1f} MB/s "
            f"(spread {hi / max(lo, 1e-9):.1f}x)", "cyan")
        if hi / max(lo, 1e-9) > 3.0:
            src.Log.print_with_color(
                "[Bandwidth] WARNING spread > 3x across edges. The measurements "
                "probably did NOT overlap, so the fast ones saw an idle link and "
                "over-report their share. The solver sums per-edge rates, so it "
                "will think this cluster has more egress than it does and may pick "
                "a cut with a large feature map. For a trustworthy cut set "
                "clustering.measure_bandwidth: False and pin network_rate_mb_s to "
                "the per-edge share you actually observe.", "yellow")

    def _cap_initial_cuts(self, best_cuts):
        """Clamp each cluster's Hungarian cut to one whose feature map fits under
        adaptive.max_message_mb.

        The adaptive controller has always refused to MOVE to an oversized cut
        (_nearest_safe_cut), but the cut clustering STARTS at went through no such
        check — so with adaptive off, or before the first nudge, a cluster could sit
        on a cut whose message is several times the broker's comfortable size for
        the whole run. The size curve is not monotonic in the cut index (for
        yolo26n@bs32 cut 4 is a local minimum, and cuts 3 and 5 are ~2.5x larger),
        so 'shallower' is not a safe direction to guess: search outward from the
        solver's choice and take the nearest cut that fits, preferring the smaller
        message when both directions tie.

        Returns the (possibly unchanged) array. Never raises: a missing size table
        just means no cap, exactly as before."""
        cap = float(self.adaptive_cfg.get("max_message_mb", 15.0))
        try:
            sizes = get_cut_data_sizes(self.model_name, self.batch_size)
        except Exception as e:
            src.Log.print_with_color(
                f"[Clustering] cut-size table unavailable ({e}); initial-cut size cap off", "yellow")
            return best_cuts
        sizes = self._wire_cut_sizes(sizes)
        n = len(sizes)

        def est(cut):
            """Wire size for a solver cut, or None when nothing is sent.

            The solver's valid_cuts run -1..L-1, wider than the size table:
              cut = -1    -> the edge runs no layers and ships the RAW input, which
                             Clustering costs with input_data_size. At bs32 that is
                             150MB float32 / 37.5MB at 8 bits — past the cap AND past
                             RabbitMQ's 16MB default, i.e. a publish that fails at
                             runtime. It must be capped, not skipped.
              cut >= L-1  -> everything on the edge, net_time is 0, nothing to cap.
            """
            if cut < 0:
                raw = float(get_raw_input_mb(self.batch_size))
                return float(self._wire_cut_sizes(np.array([raw]))[0])
            if cut >= n:
                return None
            return float(sizes[cut])

        out = list(int(c) for c in best_cuts)
        for k, cut in enumerate(out):
            over = est(cut)
            if over is None or over <= cap:
                continue
            # Nearest fitting cut by distance; ties go to the smaller message.
            cands = [(abs(c - cut), float(sizes[c]), c) for c in range(n) if sizes[c] <= cap]
            if not cands:
                src.Log.print_with_color(
                    f"[Clustering] WARNING cluster {k}: cut={cut} est ~{over:.1f}MB > cap "
                    f"{cap}MB and NO cut fits. Raise RabbitMQ max_message_size and "
                    f"adaptive.max_message_mb, or lower batch-size.", "red")
                continue
            _, newsz, newcut = min(cands)
            src.Log.print_with_color(
                f"[Clustering] cluster {k}: cut {cut} (~{over:.1f}MB) exceeds cap {cap}MB "
                f"-> using cut {newcut} (~{newsz:.1f}MB)", "yellow")
            out[k] = newcut
        return np.array(out, dtype=int)

    def _wire_cut_sizes(self, sizes):
        """CUT_DATA_SIZES_MB scaled to what actually goes on the wire.

        The table is the float32 tensor size, but the pipeline quantises to
        compress.num_bit before publishing, so the real message is num_bit/32 of
        the table (8-bit -> a quarter). Comparing an uncompressed estimate against
        a broker limit that the compressed message has to satisfy overstates every
        cut by 4x and pushes every decision toward deeper cuts.

        NOTE this is still an upper bound: a measured run at cut=4 (bs32) put
        1.65MB on the wire against a scaled estimate of ~2.97MB, so the table
        itself is ~1.8x pessimistic. Re-measure with
        `python tools/measure_cut_sizes.py --model <m> --batch_size <bs> --compress
        --num_bit 8` to make it exact."""
        if not self.compress.get("enable", False):
            return np.asarray(sizes, dtype=float)
        num_bit = int(self.compress.get("num_bit", 8))
        return np.asarray(sizes, dtype=float) * (num_bit / 32.0)

    def notify_clients(self, start=True):
        if start:
            default_splits = {"a": 4, "b": 11, "c": 17, "d": 23}

            if os.path.exists(f"{self.model_name}.pt"):
                src.Log.print_with_color(f"Exist {self.model_name}.pt", "green")
            else:
                src.Log.print_with_color(f"Download {self.model_name}", "yellow")
                _ = YOLO(f"{self.model_name}.pt")

            mode = self._get_mode()
            splits = None

            if mode in ["only_edge", "only_cloud"]:
                src.Log.print_with_color(f"[Benchmark] mode={mode}, skip split selection", "yellow")

            else:
                clustering_cfg = self.config.get("clustering", {})
                use_hungarian = clustering_cfg.get("enable", False)

                if use_hungarian:
                    try:
                        _, h = self._run_hungarian()
                        edge_labels  = h.edge_labels
                        cloud_labels = h.cloud_labels
                        matching     = h.matching
                        best_cuts    = h.best_cuts
                        K            = h.num_clusters
                        inv_matching = {int(matching[k]): k for k in range(K)}
                        best_cuts    = self._cap_initial_cuts(best_cuts)

                        # The SAME order _run_hungarian built the solver's rows
                        # in (see _ordered_clients). Re-deriving it here would
                        # silently ship cluster k's cut to the wrong machines.
                        edge_ord  = self.cluster_edge_order
                        cloud_ord = self.cluster_cloud_order

                        self.client_assignments = {}
                        for i, (cid, _) in enumerate(edge_ord):
                            k = int(edge_labels[i]) if i < len(edge_labels) else 0
                            self.client_assignments[cid] = {
                                "splits":     int(best_cuts[k]) + 1,
                                "queue_name": f"intermediate_queue_{k}",
                            }
                        for j, (cid, _) in enumerate(cloud_ord):
                            l = int(cloud_labels[j]) if j < len(cloud_labels) else 0
                            k = inv_matching.get(l, 0)
                            self.client_assignments[cid] = {
                                "splits":     int(best_cuts[k]) + 1,
                                "queue_name": f"intermediate_queue_{k}",
                            }

                        splits = int(best_cuts[0]) + 1 if len(best_cuts) > 0 else None
                        src.Log.print_with_color(
                            f"[Clustering] K={K}  best_cuts={best_cuts.tolist()}", "green")

                        # Discard leftovers from a previous (crashed) run for each
                        # per-cluster queue, same reasoning as 'intermediate_queue' above.
                        for k in range(K):
                            qname = f"intermediate_queue_{k}"
                            self.channel.queue_declare(queue=qname, durable=False)
                            self.channel.queue_purge(queue=qname)

                    except Exception as e:
                        raise RuntimeError(f"Hungarian clustering failed: {e}")

                elif self.cut_layer in default_splits:
                    splits = default_splits[self.cut_layer]
                    src.Log.print_with_color(
                        f"[Benchmark] Fixed split '{self.cut_layer}' -> splits={splits}", "yellow")
                else:
                    raise ValueError(f"Invalid cut-layer: '{self.cut_layer}'. Use a/b/c/d or set clustering.enable: True")

            file_path = f"{self.model_name}.pt"
            if not os.path.exists(file_path):
                src.Log.print_with_color(f"{self.model_name}.pt does not exist.", "yellow")
                self.connection.close()
                sys.exit(1)

            with open(file_path, "rb") as f:
                encoded = base64.b64encode(f.read()).decode('utf-8')

            # Deduplicate list_clients trong trường hợp pika callback reentrant
            seen_notify = set()
            clients_to_notify = []
            for entry in self.list_clients:
                if entry[0] not in seen_notify:
                    seen_notify.add(entry[0])
                    clients_to_notify.append(entry)

            src.Log.print_with_color(
                f"Sending model {self.model_name} to {len(clients_to_notify)} clients "
                f"(list_clients={len(self.list_clients)}).", "green")

            self._build_cluster_state(mode, clients_to_notify, splits)

            # Message size is measured on exactly ONE device: the first client
            # that registered at layer 1 — clients_to_notify preserves REGISTER
            # arrival order, so [0] of the layer-1 entries IS that client. Chosen
            # here, on the server, and carried in that client's START message, so
            # the job can never land on two machines or on none (README
            # invariant 9: the run's configuration has exactly one home).
            if self.msg_size_cfg.get("enable", True) is not False:
                self._msg_size_client = next(
                    (cid for cid, lid in clients_to_notify if lid == 1), None)
                if self._msg_size_client is None:
                    src.Log.print_with_color(
                        "[MsgSize] no client registered at layer 1 — message size "
                        "will not be measured this run", "yellow")
                else:
                    src.Log.print_with_color(
                        f"[MsgSize] measuring published message size on "
                        f"{self._msg_size_client} (first edge to register)", "cyan")

            for (client_id, layer_id) in clients_to_notify:
                assignment = self.client_assignments.get(client_id, {})
                response = {
                    "action":     "START",
                    "message":    "Server accept the connection",
                    "model":      encoded,
                    "splits":     assignment.get("splits",     splits),
                    "queue_name": assignment.get("queue_name", "intermediate_queue"),
                    "batch_size": self.batch_size,
                    "num_layers": len(self.total_clients),
                    "model_name": self.model_name,
                    "data":       self.data,
                    "compress":   self.compress,
                    "mode":       self._get_mode(),
                    "adaptive":   self.adaptive_cfg,
                    "multithreading": self.multithreading_cfg,
                    "backpressure": self.backpressure_cfg,
                    "detections": self.detections_cfg,
                    "map":        self.map_cfg,
                    "free_time":  self.free_time_cfg,
                    # measure=True for the designated edge only; every other
                    # client gets the same block with measure=False.
                    "message_size": {**self.msg_size_cfg,
                                     "measure": client_id == self._msg_size_client},
                }
                self.send_to_response(client_id, pickle.dumps(response))

                # Single quotes inside the f-string on purpose: nesting the same
                # quote character only parses on Python 3.12+ (PEP 701), and the
                # DAI server runs 3.10 — the double-quoted form made this whole
                # module a SyntaxError there, so nothing could import Server at all.
                print(f"QUEUE NAME {response['queue_name']}")
                print(f"SPLIT POINT {response['splits']}")

            # t0 for SYSTEM FPS (frames / (START -> last DONE)) — captured right
            # after the START fan-out, so warm-up is included in the whole-run rate.
            self._fps_start_t = time.time()
            # Same t0 for the server machine's own OS-level idle share, so its
            # window matches the devices' run span as closely as it can.
            from src.FreeTime import host_cpu_times
            self._server_cpu0 = host_cpu_times()
            # The queue host has been sampled since __init__; this only records
            # where the idle stretch ends and the run begins, so the summary can
            # report the two separately.
            if self._broker_ram is not None:
                self._broker_ram.mark("dispatch")

            self._start_adaptive_controller()
        else:
            response = {"action": "STOP", "message": "Stop inference !!!"}
            for (client_id, layer_id) in self.list_clients:
                self.send_to_response(client_id, pickle.dumps(response))
