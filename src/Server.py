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
        # local map/label/ ground truth, and computes a sliding-window mAP into
        # map.log — see _collect_map_pred.
        self.channel.queue_declare(queue='map_pred_queue', durable=False)
        self.channel.queue_purge(queue='map_pred_queue')
        self._fps_times = []       # arrival time of every DONE (one per batch)
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
        self.logger = src.Log.Logger(f"{log_path}/app.log", config["debug-mode"])
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
        # One line per cluster (cluster_id) + one OVERALL line, appended by
        # _collect_map_pred at shutdown. Truncated here so runs never mix.
        self.map_log_path = f"{log_path}/map.log"
        open(self.map_log_path, "w").close()
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
        """Consumer for fps_queue. Every message is one finished batch — the body
        (bare b"DONE") is never read; the ARRIVAL is the event. We just record the
        server-clock arrival time; all throughput math happens in _finish_fps.
        A smoothed window_fps is logged live so progress is visible during the run.
        Each arrival is also appended to batch_done_ns.log as "<ns-epoch> <fps>",
        one line per batch — the fps column holds the bare window_fps value and
        is absent until the first full window."""
        t_ns = time.time_ns()
        self._fps_times.append(t_ns / 1e9)
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

    def _windowed_cluster_map(self, gt_dict, pred_dict, batch_size, window_batches=16):
        """mAP@50:95 for one cluster, using the same sliding-window idea as the
        live window_fps rate (on_fps): group frames into batches of batch_size
        (recovered from frame_num), then slide a window of `window_batches`
        consecutive PRESENT batches one step at a time, compute mAP over each
        window, and average the per-window values. If fewer batches exist than
        the window size, one window covering everything is used instead."""
        try:
            from torchmetrics.detection import MeanAveragePrecision
        except ImportError:
            src.Log.print_with_color("[mAP] torchmetrics not installed on server, mAP disabled", "red")
            return None

        frames_by_batch = {}
        for frame_num in pred_dict:
            if frame_num not in gt_dict:
                continue
            b = (frame_num - 1) // batch_size
            frames_by_batch.setdefault(b, []).append(frame_num)
        if not frames_by_batch:
            return None

        batch_ids = sorted(frames_by_batch.keys())
        W = min(window_batches, len(batch_ids))
        window_values = []
        for start in range(0, len(batch_ids) - W + 1):
            metric = MeanAveragePrecision(iou_type="bbox")
            metric.warn_on_many_detections = False
            for b in batch_ids[start:start + W]:
                for frame_num in frames_by_batch[b]:
                    metric.update(
                        [{"boxes":  pred_dict[frame_num]["boxes"],
                          "scores": pred_dict[frame_num]["scores"],
                          "labels": pred_dict[frame_num]["labels"]}],
                        [gt_dict[frame_num]]
                    )
            try:
                val = float(metric.compute()["map"])
            except Exception:
                continue
            if val >= 0:
                window_values.append(val)
        if not window_values:
            return None
        return sum(window_values) / len(window_values)

    def _collect_map_pred(self, timeout_s=30.0, window_batches=16):
        """Shutdown step: drain every cloud's zipped map/pred/ directory from
        map_pred_queue (tagged with cluster_id), unpack into a per-cluster folder
        under map/pred_collected/ (write-once — a frame index already unpacked
        for a cluster is kept, never overwritten, so a second edge in the same
        cluster reprocessing the same video can't clobber it), match against this
        server's own local map/label/ ground truth, and compute a sliding-window
        mAP (window = `window_batches` consecutive batches) per cluster. Appends
        one line per cluster plus an OVERALL line to map.log. Only the tier that
        ran postprocess_yolo ever publishes here (only_edge edges, or the cloud in
        split/only_cloud), so 'expected' is scoped to that tier."""
        import zipfile
        import io
        import shutil

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
        cluster_means = []
        with open(self.map_log_path, "a") as f:
            for cluster_id, cluster_dir in sorted(cluster_dirs.items()):
                pred_dict = self._load_cluster_preds(cluster_dir)
                cluster_map = self._windowed_cluster_map(gt_dict, pred_dict, self.batch_size, window_batches)
                if cluster_map is None:
                    continue
                cluster_means.append(cluster_map)
                line = (f"{t_ns} cluster={cluster_id} mAP={cluster_map:.4f} "
                        f"(sliding window={window_batches} batches, {len(pred_dict)} frame(s))")
                f.write(line + "\n")
                src.Log.print_with_color(f"[mAP] {line}", "cyan")
            if cluster_means:
                overall = sum(cluster_means) / len(cluster_means)
                line = f"{t_ns} OVERALL mAP={overall:.4f} (avg over {len(cluster_means)} cluster(s))"
                f.write(line + "\n")
                src.Log.print_with_color(f"[mAP] {line}", "cyan")

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
