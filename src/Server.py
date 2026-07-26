import numpy as np
import os
import sys
import glob
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
        for f in (
            glob.glob("metrics_raw_*.csv")
            + glob.glob("metrics_pivoted_*.csv")
            + glob.glob("metrics_pivot_*.lock")
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
        self._stopping = False
        self.channel.basic_qos(prefetch_count=1)
        self.reply_channel = self.connection.channel()
        self.channel.basic_consume(queue='rpc_queue', on_message_callback=self.on_request)
        self.channel.basic_consume(queue='fps_queue', on_message_callback=self.on_fps)

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

        Latency comes in two flavours and they answer different questions:
          * per-role service latency — each device's own get_input -> output, one
            clock, so it is exact;
          * E2E — edge batch start -> completing tier's output. It spans two
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
                st = self._stats_ms([v for r in rr for v in r.get("lat_samples_ms", [])])
                if st:
                    print(f"      {role:<8} service latency  n={st['n']:<5} "
                          f"mean={st['mean']:8.1f}ms  p50={st['p50']:8.1f}  "
                          f"p95={st['p95']:8.1f}  max={st['max']:8.1f}")
                    lat_lines.append(
                        f"{t_ns} cluster={tag} role={role} kind=service n={st['n']} "
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
        self._collect_map_pred()
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
            self._cut_sizes = get_cut_data_sizes(self.model_name, self.batch_size)
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

    def _run_hungarian(self):
        cfg = self.config.get("clustering", {})
        network_rate = float(cfg.get("network_rate_mb_s", 1000.0))
        max_clusters = cfg.get("max_clusters", 1)

        # Dùng real profiling data nếu tất cả client đã gửi
        edge_times_list = [
            self.client_profile_data[str(cid)]
            for cid, lid in self.list_clients
            if lid == 1 and str(cid) in self.client_profile_data
        ]
        cloud_times_list = [
            self.client_profile_data[str(cid)]
            for cid, lid in self.list_clients
            if lid == len(self.total_clients) and str(cid) in self.client_profile_data
        ]
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
            edge_clients = [cid for cid, lid in self.list_clients
                            if lid == 1 and str(cid) in self.client_profile_data]
            rates_matrix = np.array([
                [self.client_bandwidth_data.get(str(cid), network_rate)] * M
                for cid in edge_clients
            ]) if edge_clients else np.full((N, M), network_rate)
            cloud_clients = [cid for cid, lid in self.list_clients
                             if lid == len(self.total_clients) and str(cid) in self.client_profile_data]
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

        return solver, result

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

                        edge_ord  = [(cid, lid) for cid, lid in self.list_clients if lid == 1]
                        cloud_ord = [(cid, lid) for cid, lid in self.list_clients if lid == len(self.total_clients)]

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
                }
                self.send_to_response(client_id, pickle.dumps(response))

            # t0 for SYSTEM FPS (frames / (START -> last DONE)) — captured right
            # after the START fan-out, so warm-up is included in the whole-run rate.
            self._fps_start_t = time.time()

            self._start_adaptive_controller()
        else:
            response = {"action": "STOP", "message": "Stop inference !!!"}
            for (client_id, layer_id) in self.list_clients:
                self.send_to_response(client_id, pickle.dumps(response))
