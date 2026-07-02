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

        # FPS meter: clouds ping 'fps_queue' after each finished batch; we compute
        # throughput from the gap between consecutive pings (see on_fps). Purge so
        # a new run doesn't inherit stale pings from a crashed one.
        self.channel.queue_declare(queue='fps_queue', durable=False)
        self.channel.queue_purge(queue='fps_queue')
        self._fps_prev_t = None  # server arrival time of the previous 'done'
        self._fps_count = 0      # number of fps samples aggregated so far
        self._fps_sum = 0.0      # running sum of per-batch fps (for the average)

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
                self.channel.stop_consuming()
                return

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def on_fps(self, ch, method, _, body):
        """Consumer for fps_queue: each 'done' means a cloud finished one batch.
        FPS is computed entirely server-side from the gap between the arrival
        times of two consecutive 'done' messages:
            fps = batch_size / (t_now - t_prev)
        (batch_size comes from config; the message carries no payload)."""
        now = time.time()
        prev = self._fps_prev_t
        self._fps_prev_t = now
        if prev is not None and now > prev:
            fps = self.batch_size / (now - prev)
            self._fps_count += 1
            self._fps_sum += fps
            avg = self._fps_sum / self._fps_count
            src.Log.print_with_color(
                f"[FPS] batch done: {fps:6.2f} fps "
                f"(batch_size={self.batch_size}, gap={(now - prev) * 1000:.1f} ms)  "
                f"| running avg {avg:6.2f} fps over {self._fps_count} batches", "cyan")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def send_to_response(self, client_id, message):
        reply_queue_name = f"reply_{client_id}"
        self.reply_channel.queue_declare(reply_queue_name, durable=False)
        src.Log.print_with_color(f"[>>>] Sent notification to client {client_id}", "red")
        self.reply_channel.basic_publish(exchange='', routing_key=reply_queue_name, body=message)

    def start(self):
        self.channel.start_consuming()
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
        for eid in edges:
            ctrl_q = f"ctrl_{eid}"
            try:
                ch.queue_declare(ctrl_q, durable=False)
                ch.basic_publish(exchange='', routing_key=ctrl_q,
                                 body=pickle.dumps({"action": "SET_CUT", "cut": int(new_cut)}))
            except Exception as e:
                src.Log.print_with_color(f"[Adaptive] SET_CUT publish failed for {eid}: {e}", "yellow")
        direction = "deeper (edge+)" if new_cut > old_cut else "shallower (cloud+)"
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

            self._start_adaptive_controller()
        else:
            response = {"action": "STOP", "message": "Stop inference !!!"}
            for (client_id, layer_id) in self.list_clients:
                self.send_to_response(client_id, pickle.dumps(response))
