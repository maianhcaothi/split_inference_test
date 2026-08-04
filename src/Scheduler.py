import torch
import cv2
import pickle
import traceback
import threading
import queue as _queue
from tqdm import tqdm
import time
import csv
import os
import psutil
import numpy as np

from src.Compress import Encoder,Decoder
import src.Log as Log
from src.Model import inference, postprocess_yolo

# Fixed cap on intermediate_queue depth (messages) before an edge waits.
# Only only_cloud sends large raw frames (~150MB/msg), which can blow up
# RabbitMQ broker memory on the Hub if too many pile up. split/Hungarian
# sends small compressed feature maps and has never overflowed, so it's
# left unconstrained.
MAX_QUEUE_ONLY_CLOUD = 15

# Sentinel returned by non-blocking gets when the queue is empty. Distinct from
# None, which is the stream-end sentinel put into the pipeline queues.
_MT_EMPTY = object()


def _cap_samples(vals, cap=20000):
    """Round latency samples for the wire, taking an even stride if a run produced
    absurdly many. An even stride preserves the shape of the distribution (so the
    server's percentiles stay representative) while keeping a multi-hour run from
    turning its shutdown report into a multi-megabyte message. Typical runs are
    far under the cap and pass through untouched."""
    if len(vals) <= cap:
        return [round(v, 3) for v in vals]
    step = len(vals) / cap
    return [round(vals[int(i * step)], 3) for i in range(cap)]


class Scheduler:
    def __init__(self, client_id, layer_id, channel, device):
        self.client_id = client_id
        self.layer_id = layer_id
        self.channel = channel
        self.device = device

        # In-memory (ns_timestamp, event) pairs — replaces the old
        # timing_edge_*.log / timing_cloud_*.log files. Only this process
        # needs them (to compute its own utilization ratio at finish), so
        # nothing is written to disk.
        self._timing_events_edge = []
        self._timing_events_cloud = []

        self.size_message = None
        self.intermediate_queue = f"intermediate_queue"
        self.channel.queue_declare(self.intermediate_queue, durable=False)
        self._my_metrics_queue = None  # set by _setup_metrics_fanout_queue

        # FPS reporting: whichever tier finishes a batch publishes one bare b"DONE"
        # to 'fps_queue' (cloud in split/only_cloud, edge in only_edge — never
        # both). The server counts arrivals and derives system throughput.
        self.fps_queue = "fps_queue"
        self._fps_q = None   # thread-safe hand-off, used only by the mt cloud path

        # Utilization: when a device finishes it re-reads its own timing log,
        # computes ONE whole-run busy/total ratio, and publishes the report to
        # 'utilization_queue'; the server appends every device's report to
        # utilization.log during its shutdown collection step.
        self.utilization_queue = "utilization_queue"
        self.channel.queue_declare(self.utilization_queue, durable=False)

        # Adaptive split-point (Mechanic 1). When on, the model is held whole and
        # the cut is applied per-batch; the edge follows SET_CUT from the server.
        self.adaptive_on = False
        self.current_cut = None
        self.ctrl_queue = None
        self._L = None

        # Multithreading pipeline (split mode): 1 inference thread + 1 transfer
        # thread per device, handed off through a bounded in-process queue.
        self.mt_on = False
        self.mt_queue_size = 4
        self._mt_stop = threading.Event()

        # Broker back-pressure (RAM guard): stall the edge when its intermediate
        # queue gets too deep so messages can't pile up in the broker.
        self.backpressure_on = False
        self.backpressure_max = MAX_QUEUE_ONLY_CLOUD

        self.map_metric = None
        self.gt_dict = {}
        # Highest frame index covered by local ground truth (e.g. 905 when the
        # video has 1700 frames but only the first 905 are labelled). None until
        # _load_gt_dict finds a groundtruth dir. Lets mAP work stop once the run
        # moves past the labelled range instead of running on unlabelled frames.
        self._max_gt_frame = None
        # Detections are streamed to detections_stream.jsonl during the run (flat
        # RAM); detections.json is rebuilt from that file at the end. Keep only a
        # count in RAM, not every frame's boxes.
        self.save_detections_json = True
        # Per-batch mAP work (2nd postprocess at conf=0.001, pred files,
        # torchmetrics updates). Set from map.enable in inference_func; defaults on
        # so nothing changes for callers that never pass the config.
        self.map_on = True
        self._det_count = 0
        self._map_updated = False
        # 'map_pred_queue': whichever tier runs postprocess_yolo (last_layer/
        # only_edge) zips up its own map/pred/*.txt files and publishes them here,
        # tagged with its cluster id (self.intermediate_queue), when its pipeline
        # finishes. The server unpacks them per cluster, matches against its own
        # local map/label/ ground truth, and computes a sliding-window mAP into
        # map.log during its shutdown collection step — see _send_pred_dir_to_server
        # and Server._collect_map_pred.
        self.map_pred_queue = "map_pred_queue"
        self.channel.queue_declare(self.map_pred_queue, durable=False)
        # Per-batch latency samples, harvested in write_metrics (the one choke
        # point every path already funnels through) and shipped to the server in
        # this device's shutdown report, so the server can break latency down per
        # cluster as well as system-wide. Raw samples, not pre-reduced stats:
        # percentiles cannot be pooled across devices after the fact, and a run's
        # worth of floats is a trivially small message.
        # Pipeline latency: batch ready -> this device's output published. On the
        # edge that INCLUDES the wait in the two hand-off queues, so it is not the
        # device's service time — see _compute_utilization, which derives the true
        # get_input -> output series from the timing markers instead.
        self._lat_ms = []
        self._e2e_ms = []   # edge start -> this device's output (crosses devices)
        # get_ram_mb is on the per-batch path; probe tegrastats once, not 56 times.
        self._tegrastats_ok = None    # None = not probed, True/False = cached answer
        self._psutil_proc = None
        self._load_gt_dict()

    def get_ram_mb(self):
        """Resident memory in MB, from tegrastats on Jetson and psutil elsewhere.

        Called once per batch, so the tegrastats availability probe runs ONCE and
        the answer is cached: on a non-Jetson host every call used to pay a failed
        CreateProcess plus a PATH scan, and a 12-device run spent ~6000 process
        spawns finding out the same thing over and over. The psutil handle is
        cached for the same reason."""
        if self._tegrastats_ok is not False:
            try:
                import subprocess, re
                result = subprocess.run(
                    ['tegrastats', '--once'],
                    capture_output=True, text=True, timeout=2
                )
                m = re.search(r'RAM (\d+)/\d+MB', result.stdout)
                if m:
                    self._tegrastats_ok = True
                    return int(m.group(1))
            except Exception:
                pass
            # Either the binary is missing or its output didn't parse — both mean
            # "don't try again", so every later call goes straight to psutil.
            self._tegrastats_ok = False
        if self._psutil_proc is None:
            self._psutil_proc = psutil.Process(os.getpid())
        return self._psutil_proc.memory_info().rss / (1024 * 1024)

    def _check_backpressure(self, max_queue):
        """Stall the caller while the intermediate queue is at/above max_queue, so
        the edge can't outrun the cloud and flood the broker (RAM guard)."""
        depth = self.channel.queue_declare(self.intermediate_queue, passive=True).method.message_count
        if depth < max_queue:
            return

        Log.print_with_color(
            f"[BackPressure] '{self.intermediate_queue}' depth={depth} >= max_queue={max_queue}, waiting", "yellow")
        while depth >= max_queue and not self._mt_stop.is_set():
            time.sleep(0.1)
            depth = self.channel.queue_declare(self.intermediate_queue, passive=True).method.message_count
        Log.print_with_color(
            f"[BackPressure] '{self.intermediate_queue}' depth={depth} < max_queue={max_queue}, resuming", "green")

    def _mark_edge(self, event):
        self._timing_events_edge.append((time.time_ns(), event))

    def _mark_cloud(self, event):
        self._timing_events_cloud.append((time.time_ns(), event))

    def write_metrics(self, mode, role, best_cut, batch_id, batch_size, latency_ms, fps, ram_mb,
                      message_size_bytes=0, e2e_latency_ms=0, edge_start_time=None,
                      inference_ms=0.0, transfer_ms=0.0, decode_ms=0.0, wait_ms=0.0):
        """One CSV row per batch.

        inference_ms / transfer_ms / decode_ms / wait_ms are the per-stage split of
        latency_ms. Every one of them was already being computed and then thrown
        away (the only consumers were commented-out log lines), which is why
        'where does the time go' could never be answered from an archived run —
        the summary logs show that a pipeline is slow, never which stage is slow.

          inference_ms  model forward on this device
          transfer_ms   publish (edge): Encoder + pickle + basic_publish
          decode_ms     pickle.loads + Decoder (cloud recv thread)
          wait_ms       time the INFERENCE thread sat blocked waiting for input.
                        Big wait_ms == this device is starved by the stage in
                        front of it, and no amount of extra CPU here will help.
        """
        file_path = f"metrics_raw_{self.intermediate_queue}_{str(self.client_id).replace('-', '')}.csv"
        file_exists = os.path.exists(file_path)

        with open(file_path, "a", newline="") as f:
            writer = csv.writer(f)

            if not file_exists:
                writer.writerow([
                    "mode",
                    "role",
                    "best_cut",
                    "batch_id",
                    "batch_size",
                    "latency_ms",
                    "fps",
                    "ram_mb",
                    "message_size_bytes",
                    "e2e_latency_ms",
                    "edge_start_time",
                    "inference_ms",
                    "transfer_ms",
                    "decode_ms",
                    "wait_ms",
                ])

            writer.writerow([
                mode,
                role,
                best_cut,
                batch_id,
                batch_size,
                round(latency_ms, 3),
                round(fps, 3) if fps > 0 else "",  # fps=0 (first batch) → empty
                round(ram_mb, 3),
                message_size_bytes,
                round(e2e_latency_ms, 3),
                edge_start_time if edge_start_time is not None else "",
                round(inference_ms, 3),
                round(transfer_ms, 3),
                round(decode_ms, 3),
                round(wait_ms, 3),
            ])

        # Same numbers the CSV row just recorded, kept in memory for this device's
        # shutdown latency report. Every path (edge/cloud, sequential/threaded)
        # calls write_metrics, so hooking here covers them all with one line.
        # e2e is 0 on the edge (only the completing tier can know it) — drop those
        # so they don't drag the mean toward zero.
        if latency_ms:
            self._lat_ms.append(float(latency_ms))
        if e2e_latency_ms:
            self._e2e_ms.append(float(e2e_latency_ms))

    def _setup_metrics_fanout_queue(self):
        """Cloud client gọi trước khi inference: tạo queue riêng bind vào fanout exchange.
        Mỗi cloud nhận một bản copy metrics từ tất cả edge trong cluster."""
        exchange = f"metrics_fanout_{self.intermediate_queue}"
        my_queue = f"mfq_{str(self.client_id).replace('-', '')}"
        try:
            self.channel.exchange_declare(exchange=exchange, exchange_type='fanout', durable=False)
            self.channel.queue_declare(my_queue, durable=False)
            self.channel.queue_bind(queue=my_queue, exchange=exchange)
            self._my_metrics_queue = my_queue
        except Exception as e:
            Log.print_with_color(f"[Metrics] Fanout setup failed: {e}", "yellow")
            self._my_metrics_queue = None

    def send_next_layer(self, intermediate_queue, data, compress):

        if compress["enable"]:
            data["data"] = [t.cpu().numpy() if isinstance(t, torch.Tensor) else None for t in
                                     data["data"]]
            data["data"], data["shape"] = Encoder(data_output=data["data"], num_bits=compress["num_bit"])

        else:
            data["data"] = [t.cpu() if isinstance(t, torch.Tensor) else None for t in
                                     data["data"]]
        message = pickle.dumps({
            "action": "OUTPUT",
            "data": data
        })
        self.size_message = len(message)


        self.channel.basic_publish(
            exchange='',
            routing_key=intermediate_queue,
            body=message,
            #body= "."
        )

    def _load_gt_dict(self, gt_dir="datasets/groundtruth"):
        if not os.path.isdir(gt_dir):
            return
        try:
            from torchmetrics.detection import MeanAveragePrecision
            self.map_metric = MeanAveragePrecision(iou_type="bbox")
            self.map_metric.warn_on_many_detections = False
        except ImportError:
            Log.print_with_color("[!] torchmetrics not installed, mAP disabled", "red")
            return
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
                    boxes.append([(cx - bw/2)*640, (cy - bh/2)*640,
                                  (cx + bw/2)*640, (cy + bh/2)*640])
                    labels.append(int(cls))
            self.gt_dict[num] = {
                "boxes":  torch.tensor(boxes,  dtype=torch.float32) if boxes  else torch.zeros((0, 4)),
                "labels": torch.tensor(labels, dtype=torch.int64)   if labels else torch.zeros(0, dtype=torch.int64),
            }
        if self.gt_dict:
            self._max_gt_frame = max(self.gt_dict)
        Log.print_with_color(
            f"[mAP] Loaded GT for {len(self.gt_dict)} frames from '{gt_dir}'"
            + (f" (up to frame {self._max_gt_frame})" if self._max_gt_frame else ""),
            "green")

    def _batch_past_gt(self, batch_id, batch_size):
        """True once every frame in this batch is beyond the last frame index
        covered by local ground truth — the video can run longer than the
        labelled subset (e.g. 1700 frames but GT only for the first 905), so
        mAP postprocessing for later batches would be pure waste."""
        if not self.map_on:
            return True
        if self._max_gt_frame is None:
            return False
        return batch_id * batch_size + 1 > self._max_gt_frame

    def _update_map(self, batch_results, batch_id, batch_size, map_results=None):
        import json
        # Only claim ownership of the mAP report when mAP work actually ran —
        # otherwise the shutdown path would ship an empty map/pred/ zip and print
        # a mAP summary over nothing.
        if self.map_on:
            self._map_updated = True
        # map_results uses conf≈0.001 so torchmetrics gets the full PR curve;
        # batch_results (conf=0.25) is only for the detection stream / display.
        _map = map_results if map_results is not None else batch_results
        for img_idx, (r, rm) in enumerate(zip(batch_results, _map)):
            frame_num = batch_id * batch_size + img_idx + 1
            dets = [
                {
                    "box":   r["boxes"][i].cpu().tolist(),
                    "score": round(float(r["scores"][i]), 4),
                    "class": int(r["classes"][i]),
                }
                for i in range(len(r["boxes"]))
            ]
            # Stream to disk instead of holding every frame's boxes in RAM. RAM
            # stays flat over long videos; detections.json is rebuilt at the end.
            # Skipped entirely when nobody will read it: this is an open+write+close
            # per FRAME (batch_size of them per batch) inside the timed window.
            if self.save_detections_json:
                self._det_count += 1
                with open("detections_stream.jsonl", "a") as f:
                    f.write(json.dumps({"frame": frame_num, "dets": dets}) + "\n")
            if not self.map_on:
                # map.enable=False: detections above still stream, but every mAP
                # cost (pred file + torchmetrics update) is skipped.
                continue
            if self._max_gt_frame is not None and frame_num > self._max_gt_frame:
                # Past the labelled range: detections keep streaming above (for
                # detections.json/tracker), but mAP stops here — no pred file,
                # no metric update.
                continue
            self._write_pred_file(frame_num, rm)
            if self.map_metric is None or frame_num not in self.gt_dict:
                continue
            self.map_metric.update(
                [{"boxes":  rm["boxes"].cpu().float(),
                  "scores": rm["scores"].cpu().float(),
                  "labels": rm["classes"].cpu().long()}],
                [self.gt_dict[frame_num]]
            )

    def _cluster_tag(self):
        """Filesystem-safe cluster id derived from self.intermediate_queue (e.g.
        'intermediate_queue_0' under Hungarian, or plain 'intermediate_queue'
        otherwise), used to keep each cluster's predictions in their own
        subdirectory even when multiple clouds share one machine/filesystem."""
        return "".join(c if c.isalnum() or c in "_-" else "_" for c in self.intermediate_queue)

    def _write_pred_file(self, frame_num, rm):
        """Write this frame's low-threshold detections (conf~=0.001 sweep, same set
        fed to torchmetrics) to map/pred/<cluster>/frame_NNNNNN.txt, mirroring the
        'class_id x_center y_center width height' layout of map/label/ (normalized
        to the 640x640 network input, matching how _load_gt_dict decodes that
        folder) with confidence appended, so an external mAP tool can diff the two
        folders frame-for-frame. Scoped under the cluster's own subdirectory so
        multiple clouds sharing one filesystem never mix predictions.
        Write-once: if this frame index already has a file (e.g. a second edge in
        the same cluster reprocessing the same video, or a stale batch_id from a
        previous run), skip it instead of overwriting — one frame index keeps
        exactly one prediction file, same as map/label/."""
        cluster_dir = f"map/pred/{self._cluster_tag()}"
        path = f"{cluster_dir}/frame_{frame_num:06d}.txt"
        if os.path.exists(path):
            return
        os.makedirs(cluster_dir, exist_ok=True)
        boxes = rm["boxes"].cpu()
        scores = rm["scores"].cpu()
        classes = rm["classes"].cpu()
        lines = []
        for i in range(len(boxes)):
            x1, y1, x2, y2 = boxes[i].tolist()
            cx = (x1 + x2) / 2 / 640
            cy = (y1 + y2) / 2 / 640
            w = (x2 - x1) / 640
            h = (y2 - y1) / 640
            lines.append(f"{int(classes[i])} {cx} {cy} {w} {h} {float(scores[i]):.4f}")
        with open(path, "w") as f:
            f.write("\n".join(lines))
            if lines:
                f.write("\n")

    def _print_map(self):
        if self.map_metric is None:
            Log.print_with_color("[mAP] Skipped: groundtruth not found on this device (datasets/groundtruth/ missing)", "yellow")
            return
        if not self.gt_dict:
            Log.print_with_color("[mAP] Skipped: groundtruth folder exists but no valid .txt files loaded", "yellow")
            return
        try:
            result = self.map_metric.compute()
            print("=" * 50)
            print(f"  [mAP]   mAP@50={result['map_50']:.4f}  mAP@50:95={result['map']:.4f}")
            print("=" * 50)
        except Exception as e:
            Log.print_with_color(f"[mAP] compute failed: {e}", "red")

    def _write_detections_json(self):
        """Rebuild detections.json ({frame: dets}) by streaming detections_stream.jsonl
        line by line, so peak RAM stays flat regardless of video length. (tracker
        post-mode looks up frames by key, so ordering is irrelevant.)"""
        import json
        if not self.save_detections_json:
            return
        stream = "detections_stream.jsonl"
        out = "detections.json"
        if not os.path.exists(stream):
            return
        n = 0
        with open(stream) as fin, open(out, "w") as fout:
            fout.write("{")
            first = True
            for line in fin:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except Exception:
                    continue
                if not first:
                    fout.write(",")
                fout.write(f"{json.dumps(str(entry['frame']))}:{json.dumps(entry['dets'])}")
                first = False
                n += 1
            fout.write("}")
        Log.print_with_color(f"[Tracker] Saved {out} ({n} frames, streamed)", "green")

    def send_to_server(self, message):
        self.channel.queue_declare('rpc_queue', durable=False)
        self.channel.basic_publish(exchange='',
                                   routing_key='rpc_queue',
                                   body=pickle.dumps(message))

    def _send_fps_done(self):
        """Publish exactly one DONE to fps_queue per finished batch. The tier that
        COMPLETES the batch sends it (cloud in split/only_cloud, edge in only_edge)
        — never both, so one DONE == exactly batch_size frames.

        The body carries this cluster's id so the server can break throughput down
        per cluster on top of the system total. It is an identity only: timing
        still comes purely from the server's arrival clock, so no device clocks
        need syncing and a garbled body can at worst mis-bucket a batch, never
        distort a rate. Must be called on the channel-owning thread (pika channels
        aren't thread-safe)."""
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.fps_queue,
                body=str(self.intermediate_queue).encode(),
            )
        except Exception as e:
            Log.print_with_color(f"[FPS] send DONE failed: {e}", "yellow")

    def _drain_fps_events(self):
        """Publish one DONE per batch finished by the mt inference thread. Runs on
        the channel-owning (recv) thread because pika channels are not thread-safe
        — the infer thread only enqueues a marker onto _fps_q."""
        q = self._fps_q
        if q is None:
            return
        while True:
            try:
                q.get_nowait()
            except _queue.Empty:
                break
            self._send_fps_done()

    def _compute_utilization(self, events, role):
        """Fold this device's in-memory timing events (one (ns_timestamp, event)
        pair per lifecycle event, recorded by _mark_edge/_mark_cloud) into ONE
        whole-run utilization ratio:
            utilization = busy / total = sum(output_i - get_input_i) / (end - start)
        Numerator and denominator come from the same device's own clock, so clock
        skew between machines cannot distort the ratio. Unknown events (e.g.
        queue_wait_start/queue_wait_end) are ignored, so the event set stays
        forward-extensible; an unmatched 'get input' with no following 'output'
        (crash mid-batch) is dropped. Returns the stats dict, or None if the
        events are incomplete."""
        t_start = None
        t_end = None
        t_input = None
        busy_ns = 0
        n_packages = 0
        # The individual busy intervals, i.e. the device's TRUE per-batch service
        # latency. Summing them gives busy_ns, so this series and the utilization
        # ratio can never disagree. It is reported separately from self._lat_ms,
        # which measures batch-ready -> published and therefore also contains the
        # hand-off queue waits (that is why the edge's number used to look ~7x
        # worse than its actual per-batch compute).
        svc_ms = []
        for ts, event in events:
            if event == "start":
                t_start = ts
            elif event == "end":
                t_end = ts
            elif event == "get input":
                t_input = ts
            elif event == "output":
                if t_input is not None:
                    busy_ns += ts - t_input
                    svc_ms.append((ts - t_input) / 1e6)
                    n_packages += 1
                    t_input = None
        if t_start is None or t_end is None or t_end <= t_start:
            Log.print_with_color(
                f"[Utilization] incomplete timing events for {role} (missing/invalid start-end)", "yellow")
            return None
        total_ns = t_end - t_start
        stats = {
            "role": role,
            "packages": n_packages,
            "busy_ns": busy_ns,
            "total_ns": total_ns,
            "utilization": busy_ns / total_ns,
            # Which cluster this device belongs to, so the server can group both
            # utilization and latency per cluster instead of per device only.
            "cluster_id": self.intermediate_queue,
            "svc_samples_ms": _cap_samples(svc_ms),      # get_input -> output
            "lat_samples_ms": _cap_samples(self._lat_ms),  # batch ready -> published
            "e2e_samples_ms": _cap_samples(self._e2e_ms),
        }
        Log.print_with_color(
            f"[Utilization][{role}] packages={n_packages} "
            f"busy={busy_ns / 1e9:.3f}s total={total_ns / 1e9:.3f}s "
            f"utilization={stats['utilization'] * 100:.2f}%", "cyan")
        return stats

    def _send_utilization(self, stats):
        """Publish this device's finished utilization report to utilization_queue.
        The report sits on the broker until the server's shutdown collection step
        drains it, so publisher and consumer never need to be alive at the same
        moment (a cloud finishes only after the server stopped consuming
        rpc_queue). Utilization is telemetry — every failure path degrades to a
        warning, never an exception that could kill the run."""
        if stats is None:
            return
        message = {
            "action": "UTILIZATION",
            "client_id": self.client_id,
            "layer_id": self.layer_id,
        }
        message.update(stats)
        try:
            self.channel.queue_declare(self.utilization_queue, durable=False)
            self.channel.basic_publish(
                exchange='',
                routing_key=self.utilization_queue,
                body=pickle.dumps(message),
            )
        except Exception as e:
            Log.print_with_color(f"[Utilization] send failed: {e}", "yellow")

    def _send_pred_dir_to_server(self):
        """Zip this device's own map/pred/<cluster>/*.txt files and publish them
        to map_pred_queue, tagged with the cluster id (self.intermediate_queue),
        once the pipeline finishes. Only called by the tier that actually ran
        postprocess_yolo (guarded by self._map_updated at the call site) —
        mirrors _send_utilization's report-then-let-the-server-collect-at-
        shutdown pattern. The server unpacks these per cluster, matches them
        against its own local map/label/ ground truth, and computes a
        sliding-window mAP (see Server._collect_map_pred)."""
        import glob as _glob
        import zipfile
        import io
        files = sorted(_glob.glob(f"map/pred/{self._cluster_tag()}/frame_*.txt"))
        if not files:
            return
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for fpath in files:
                zf.write(fpath, arcname=os.path.basename(fpath))
        message = {
            "action": "MAP_PRED",
            "client_id": self.client_id,
            "layer_id": self.layer_id,
            "cluster_id": self.intermediate_queue,
            "zip_bytes": buf.getvalue(),
        }
        try:
            self.channel.queue_declare(self.map_pred_queue, durable=False)
            self.channel.basic_publish(
                exchange='',
                routing_key=self.map_pred_queue,
                body=pickle.dumps(message),
            )
            Log.print_with_color(
                f"[mAP] Sent {len(files)} pred file(s) for cluster "
                f"'{self.intermediate_queue}' to server", "cyan")
        except Exception as e:
            Log.print_with_color(f"[mAP] send pred dir failed: {e}", "yellow")

    def first_layer(self, model, data, batch_size, splits, logger, compress, mode="split", save_set=None):
        input_image = []
        if mode != "only_cloud":
            model.eval()
            model.to(self.device)

        video_path = data
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            Log.print_with_color(f"Not open video", "red")
            return False

        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

        pbar = tqdm(desc="Processing video (while loop)", unit="frame")
        batch_id = 0
        prev_batch_end = None

        # In only_edge the edge is the tier that completes each batch, so it (not
        # the cloud) publishes the DONE ping — declare the queue up front.
        if mode == "only_edge":
            self.channel.queue_declare(self.fps_queue, durable=False)

        if self.adaptive_on:
            self._L = len(model)
            self.current_cut = max(1, min(int(splits) if splits else 1, self._L - 1))
            self.ctrl_queue = f"ctrl_{self.client_id}"
            self.channel.queue_declare(self.ctrl_queue, durable=False)
            Log.print_with_color(
                f"[Adaptive][edge] enabled, L={self._L}, start cut={self.current_cut}", "cyan")

        self._mark_edge("start")
        while True:
            ret, frame = cap.read()
            if not ret:
                break
            frame = cv2.resize(frame, (640, 640))
            frame = frame.astype('float32') / 255.0
            tensor = torch.from_numpy(frame).permute(2, 0, 1)  # shape: (3, 640, 640)
            input_image.append(tensor)

            if len(input_image) == batch_size:
                if self.adaptive_on:
                    self._poll_ctrl()
                t_batch_ready = time.perf_counter()
                gap_ms = (t_batch_ready - prev_batch_end) * 1000 if prev_batch_end is not None else 0.0
                self._mark_edge("get input")
                batch_start = time.perf_counter()
                edge_start_wall = time.time()

                _stack_start = time.perf_counter()
                input_image = torch.stack(input_image)
                if mode != "only_cloud":
                    # only_cloud: edge does no GPU inference, keep frames on CPU
                    # to avoid a wasted CPU->GPU->CPU round trip before sending.
                    input_image = input_image.to(self.device)
                stack_ms = (time.perf_counter() - _stack_start) * 1000

                inference_ms = 0.0
                queue_wait_ms = 0.0
                send_ms = 0.0
                edge_best_cut = "N/A" if splits is None else splits  # overridden per-batch when adaptive

                # ===== ONLY CLOUD =====
                if mode == "only_cloud":
                    frames_cpu = input_image
                    y = {
                        "data": [frames_cpu[i].clone() for i in range(len(frames_cpu))],
                        "width": width,
                        "height": height,
                        "edge_start_time": edge_start_wall,
                        "batch_id": batch_id,
                    }

                    _wait_start = time.perf_counter()
                    self._mark_edge("queue_wait_start")
                    self._check_backpressure(MAX_QUEUE_ONLY_CLOUD)
                    self._mark_edge("queue_wait_end")
                    queue_wait_ms = (time.perf_counter() - _wait_start) * 1000

                    _send_start = time.perf_counter()
                    self.send_next_layer(
                        self.intermediate_queue,
                        y,
                        {"enable": False}
                    )
                    send_ms = (time.perf_counter() - _send_start) * 1000

                # ===== ONLY EDGE =====
                elif mode == "only_edge":

                    _inf_start = time.perf_counter()
                    y = []
                    with torch.no_grad():
                        x, y = inference(model, input_image, y, 0, save_set)
                    inference_ms = (time.perf_counter() - _inf_start) * 1000

                    results = postprocess_yolo(x, conf_thres=0.25, iou_thres=0.5)
                    map_results = None if self._batch_past_gt(batch_id, batch_size) else \
                        postprocess_yolo(x, conf_thres=0.001, iou_thres=0.5)
                    self._update_map(results, batch_id, batch_size, map_results=map_results)

                    _send_start = time.perf_counter()
                    payload = {
                        "width": width,
                        "height": height,
                        "results": [
                            {
                                "boxes":   r["boxes"].cpu().numpy(),
                                "scores":  r["scores"].cpu().numpy(),
                                "classes": r["classes"].cpu().numpy(),
                            }
                            for r in results
                        ],
                        "edge_start_time": edge_start_wall,
                    }
                    body = pickle.dumps({"action": "OUTPUT", "data": payload})
                    self.size_message = len(body)
                    self.channel.basic_publish(exchange='', routing_key=self.intermediate_queue, body=body)
                    send_ms = (time.perf_counter() - _send_start) * 1000

                # ===== SPLIT INFERENCE =====
                else:

                    if self.adaptive_on:
                        cut = self.current_cut
                        sub_model = model[:cut]      # full model held; slice per-batch
                    else:
                        cut = splits
                        sub_model = model            # already sliced at load time

                    _inf_start = time.perf_counter()
                    y = []
                    with torch.no_grad():
                        x, y = inference(sub_model, input_image, y, 0, save_set)
                    y[-1] = x
                    inference_ms = (time.perf_counter() - _inf_start) * 1000

                    y = {
                        "data": y,
                        "width": width,
                        "height": height,
                        "edge_start_time": edge_start_wall,
                        "batch_id": batch_id,
                    }
                    if self.adaptive_on:
                        y["cut"] = int(cut)
                        edge_best_cut = int(cut)

                    if self.backpressure_on:
                        _wait_start = time.perf_counter()
                        self._check_backpressure(self.backpressure_max)
                        queue_wait_ms = (time.perf_counter() - _wait_start) * 1000

                    _send_start = time.perf_counter()
                    self.send_next_layer(
                        self.intermediate_queue,y,compress
                    )
                    send_ms = (time.perf_counter() - _send_start) * 1000
                batch_end = time.perf_counter()
                self._mark_edge("output")
                latency_ms = (batch_end - batch_start) * 1000
                fps = batch_size / (batch_end - prev_batch_end) if prev_batch_end is not None else 0.0
                e2e_latency_ms = 0.0
                _ram_start = time.perf_counter()
                ram_mb = self.get_ram_mb()
                ram_ms = (time.perf_counter() - _ram_start) * 1000
                msg_size = self.size_message if self.size_message is not None else 0

                _write_start = time.perf_counter()
                self.write_metrics(
                    mode=mode,
                    role="edge_sender" if mode == "only_cloud" else "edge",
                    best_cut=edge_best_cut,
                    batch_id=batch_id,
                    batch_size=batch_size,
                    latency_ms=latency_ms,
                    fps=fps,
                    ram_mb=ram_mb,
                    message_size_bytes=msg_size,
                    e2e_latency_ms=e2e_latency_ms,
                    edge_start_time=edge_start_wall,
                )
                # write_ms = (time.perf_counter() - _write_start) * 1000

                # batch_interval_ms = (batch_end - prev_batch_end) * 1000 if prev_batch_end is not None else 0.0
                # Log.print_with_color(
                #     f"[Timing][edge] gap={gap_ms:.1f}ms stack={stack_ms:.1f}ms "
                #     f"inference={inference_ms:.1f}ms queue_wait={queue_wait_ms:.1f}ms send={send_ms:.1f}ms "
                #     f"ram={ram_ms:.1f}ms write={write_ms:.1f}ms "
                #     f"| latency={latency_ms:.1f}ms | batch_interval={batch_interval_ms:.1f}ms",
                #     "magenta"
                # )

                # only_edge: the edge completes the batch here, so it emits the DONE
                # (in split/only_cloud the cloud does it instead — never both).
                if mode == "only_edge":
                    self._send_fps_done()

                batch_id += 1
                prev_batch_end = batch_end

                input_image = []
                pbar.update(batch_size)
            else:
                continue
        self._mark_edge("end")
        print(f'size message: {self.size_message} bytes.')
        cap.release()
        pbar.close()

        self._finish_edge()

    def _finish_edge(self):
        """Broadcast this edge's metrics CSV to the cluster, tell the server this
        edge is done, then block until the server replies STOP. Shared by the
        sequential (first_layer) and threaded (_first_layer_mt) edge paths."""
        # Whole-run utilization from this edge's own timing log — 'end' is already
        # logged by both callers, and the report must go out BEFORE NOTIFY so it
        # is on the broker when the server starts its shutdown sequence.
        self._send_utilization(self._compute_utilization(self._timing_events_edge, "edge"))
        # only_edge: this device ran postprocess_yolo itself (_map_updated flags
        # that _update_map ran), so it — not a cloud — owns the mAP report.
        if self._map_updated:
            self._send_pred_dir_to_server()

        # Broadcast metrics CSV lên tất cả cloud trong cluster qua fanout exchange
        metrics_file = f"metrics_raw_{self.intermediate_queue}_{str(self.client_id).replace('-', '')}.csv"
        if os.path.exists(metrics_file):
            try:
                with open(metrics_file, 'rb') as f:
                    metrics_data = f.read()
                exchange = f"metrics_fanout_{self.intermediate_queue}"
                self.channel.exchange_declare(exchange=exchange, exchange_type='fanout', durable=False)
                self.channel.basic_publish(
                    exchange=exchange,
                    routing_key='',
                    body=pickle.dumps({"action": "METRICS", "filename": os.path.basename(metrics_file), "data": metrics_data})
                )
                Log.print_with_color(f"[Metrics] Broadcast metrics via fanout ({len(metrics_data)} bytes)", "cyan")
            except Exception as e:
                Log.print_with_color(f"[Metrics] Failed to send metrics: {e}", "yellow")

        notify_data = {"action": "NOTIFY", "client_id": self.client_id, "layer_id": self.layer_id,
                       "message": "Finish training!"}

        self.send_to_server(notify_data)

        broadcast_queue_name = f'reply_{self.client_id}'
        while True:
            method_frame, header_frame, body = self.channel.basic_get(queue=broadcast_queue_name, auto_ack=True)
            if body:

                received_data = pickle.loads(body)
                Log.print_with_color(f"[<<<] Received message from server {received_data}", "blue")
                if received_data["action"] == "STOP":
                    Log.print_with_color("[>>>] Finish!", "red")
                    break
            time.sleep(0.5)


    def last_layer(self, model, batch_size, splits, logger, compress, mode="split", save_set=None):
        if mode != "only_edge":
            model.eval()
            model.to(self.device)

        pbar = tqdm(desc="Processing video (while loop)", unit="frame")
        batch_id = 0
        prev_batch_end = None
        self.channel.queue_declare(self.fps_queue, durable=False)
        self._mark_cloud("start")
        while True:
            method_frame, header_frame, body = self.channel.basic_get(queue=self.intermediate_queue, auto_ack=True)
            if method_frame and body:
                t_batch_ready = time.perf_counter()
                gap_ms = (t_batch_ready - prev_batch_end) * 1000 if prev_batch_end is not None else 0.0
                self._mark_cloud("get input")
                batch_start = time.perf_counter()
                received_message_size = len(body)
                received_data = pickle.loads(body)
                y = received_data["data"]
                edge_start_time = y.get("edge_start_time", time.time())
                # Prefer the edge's own batch_id (rides in the message) over this
                # loop's local receive-order counter, so frame_num stays correct
                # even if arrival order ever diverges from send order.
                msg_batch_id = y.get("batch_id", batch_id)
                cloud_best_cut = "N/A" if splits is None else splits  # overridden per-batch when adaptive

                # ===== ONLY EDGE (cloud just receives lightweight results) =====
                if mode == "only_edge":
                    decode_ms = 0.0
                    inference_ms = 0.0
                # ===== ONLY CLOUD =====
                elif mode == "only_cloud":
                    _decode_start = time.perf_counter()
                    input_tensor = y["data"]

                    if isinstance(input_tensor, list):
                        input_tensor = torch.stack(input_tensor)

                    input_tensor = input_tensor.to(self.device)
                    decode_ms = (time.perf_counter() - _decode_start) * 1000

                    _inf_start = time.perf_counter()
                    with torch.no_grad():
                        x, _ = inference(model, input_tensor, [], 0, save_set)
                    inference_ms = (time.perf_counter() - _inf_start) * 1000
                # ===== SPLIT INFERENCE =====
                else:
                    _decode_start = time.perf_counter()
                    if compress["enable"]:
                        y["data"] = Decoder(y["data"], y["shape"])

                        y["data"] = [
                            torch.from_numpy(t) if t is not None else None
                            for t in y["data"]
                        ]

                    y["data"] = [
                        t.to(self.device) if t is not None else None
                        for t in y["data"]
                    ]

                    list_output = y["data"]

                    x = list_output[-1]
                    decode_ms = (time.perf_counter() - _decode_start) * 1000

                    if self.adaptive_on:
                        # Cut travels inside the message; the edge held layers[:cut]
                        # so this device runs the matching tail layers[cut:].
                        cut = int(y.get("cut", splits if splits is not None else 1))
                        sub_model = model[cut:]      # full model held; slice per-batch
                        cloud_best_cut = cut
                    else:
                        cut = splits
                        sub_model = model            # already sliced at load time

                    _inf_start = time.perf_counter()
                    with torch.no_grad():
                        x, _ = inference(sub_model, x, list_output, cut, save_set)
                    inference_ms = (time.perf_counter() - _inf_start) * 1000

                if mode == "only_edge":
                    postprocess_ms = 0.0
                else:
                    _post_start = time.perf_counter()
                    results = postprocess_yolo(x, conf_thres=0.25, iou_thres=0.5)
                    map_results = None if self._batch_past_gt(msg_batch_id, batch_size) else \
                        postprocess_yolo(x, conf_thres=0.001, iou_thres=0.5)
                    self._update_map(results, msg_batch_id, batch_size, map_results=map_results)
                    postprocess_ms = (time.perf_counter() - _post_start) * 1000

                batch_end = time.perf_counter()
                self._mark_cloud("output")
                cloud_end_wall = time.time()
                latency_ms = (batch_end - batch_start) * 1000
                fps = batch_size / (batch_end - prev_batch_end) if prev_batch_end is not None else 0.0
                e2e_latency_ms = (cloud_end_wall - edge_start_time) * 1000
                _ram_start = time.perf_counter()
                ram_mb = self.get_ram_mb()
                ram_ms = (time.perf_counter() - _ram_start) * 1000

                _write_start = time.perf_counter()
                self.write_metrics(
                    mode=mode,
                    role="cloud",
                    best_cut=cloud_best_cut,
                    batch_id=batch_id,
                    batch_size=batch_size,
                    latency_ms=latency_ms,
                    fps=fps,
                    ram_mb=ram_mb,
                    message_size_bytes=received_message_size,
                    e2e_latency_ms=e2e_latency_ms,
                    edge_start_time=edge_start_time,
                )
                # write_ms = (time.perf_counter() - _write_start) * 1000

                # batch_interval_ms = (batch_end - prev_batch_end) * 1000 if prev_batch_end is not None else 0.0
                # Log.print_with_color(
                #     f"[Timing][cloud] gap={gap_ms:.1f}ms decode={decode_ms:.1f}ms "
                #     f"inference={inference_ms:.1f}ms postprocess={postprocess_ms:.1f}ms "
                #     f"ram={ram_ms:.1f}ms write={write_ms:.1f}ms "
                #     f"| latency={latency_ms:.1f}ms | batch_interval={batch_interval_ms:.1f}ms",
                #     "magenta"
                # )

                # One DONE per completed batch. In only_edge the EDGE completes the
                # batch and sends DONE, so the cloud must not (avoid double count).
                if mode != "only_edge":
                    self._send_fps_done()

                batch_id += 1
                prev_batch_end = batch_end

                pbar.update(batch_size)

            else:
                broadcast_queue_name = f'reply_{self.client_id}'
                method_frame, header_frame, body = self.channel.basic_get(queue=broadcast_queue_name, auto_ack=True)
                if body:
                    received_data = pickle.loads(body)
                    Log.print_with_color(f"[<<<] Received message from server {received_data}", "blue")
                    if received_data["action"] == "STOP":
                        Log.print_with_color("[>>>] Finish!", "red")
                        break
                else:
                    time.sleep(0.5)

        self._mark_cloud("end")
        # Last step on this device: compute the whole-run ratio from the timing
        # events and park the report on utilization_queue for the server to collect.
        self._send_utilization(self._compute_utilization(self._timing_events_cloud, "cloud"))
        if self._map_updated:
            self._send_pred_dir_to_server()
        try:
            cv2.destroyAllWindows()
        except Exception:
            pass
        pbar.close()

    # ─── Multithreading pipeline (split mode): infer thread ‖ transfer thread ──

    def _mt_put(self, q, item):
        """Bounded put that yields to the stop event so a dead peer can't deadlock."""
        while not self._mt_stop.is_set():
            try:
                q.put(item, timeout=0.2)
                return True
            except _queue.Full:
                continue
        return False

    def _mt_get(self, q):
        """Blocking get that returns None once the stream ends or the peer stopped."""
        while True:
            try:
                return q.get(timeout=0.2)
            except _queue.Empty:
                if self._mt_stop.is_set():
                    return None
                continue

    def _mt_put_nowait(self, q, item):
        try:
            q.put_nowait(item)
            return True
        except _queue.Full:
            return False

    def _mt_get_nowait(self, q):
        try:
            return q.get_nowait()
        except _queue.Empty:
            return _MT_EMPTY

    def _first_layer_mt(self, model, data, batch_size, splits, logger, compress, save_set=None):
        """Edge, split mode, pipelined with 2 threads:
          - transfer thread (this/main thread, owns the pika channel): captures
            video frames -> in_q, and drains out_q -> compress + publish + ctrl poll.
          - inference thread: pure compute, in_q -> head model -> out_q.
        Two bounded queues + non-blocking multiplex on the transfer side so a slow
        network (out_q full) can't deadlock a full in_q."""
        model.eval()
        model.to(self.device)

        if self.adaptive_on:
            self._L = len(model)
            self.current_cut = max(1, min(int(splits) if splits else 1, self._L - 1))
            self.ctrl_queue = f"ctrl_{self.client_id}"
            self.channel.queue_declare(self.ctrl_queue, durable=False)
            Log.print_with_color(
                f"[Adaptive][edge] enabled, L={self._L}, start cut={self.current_cut}", "cyan")

        cap = cv2.VideoCapture(data)
        if not cap.isOpened():
            Log.print_with_color("Not open video", "red")
            return False
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

        self._mt_stop = threading.Event()
        in_q = _queue.Queue(maxsize=self.mt_queue_size)
        out_q = _queue.Queue(maxsize=self.mt_queue_size)
        self._mark_edge("start")

        infer_t = threading.Thread(
            target=self._edge_infer_worker,
            args=(model, in_q, out_q, width, height, splits, save_set),
            daemon=True)
        infer_t.start()
        # Transfer loop (capture + send) runs in THIS (main) thread — owns the channel.
        self._edge_transfer_worker(cap, in_q, out_q, batch_size, compress, splits)
        infer_t.join()

        self._mark_edge("end")
        print(f'size message: {self.size_message} bytes.')
        cap.release()
        self._finish_edge()

    def _edge_transfer_worker(self, cap, in_q, out_q, batch_size, compress, splits):
        """I/O thread: reads/preprocesses frames into batches (in_q) and, in the
        same loop, publishes finished head outputs pulled from out_q."""
        pbar = tqdm(desc="Processing video (edge-mt)", unit="frame")
        frames = []
        video_done = False
        in_sentinel_sent = False
        batch_id = 0
        prev_done = None
        try:
            # Loop until the inference thread's None sentinel arrives on out_q.
            # (Do NOT guard on _mt_stop here — the inference thread sets stop right
            #  after emitting its sentinel, and we must still drain the tail of out_q.)
            while True:
                progressed = False

                # ---- INPUT: capture one frame, emit a batch when full ----
                if not in_sentinel_sent:
                    if not video_done and len(frames) < batch_size:
                        ret, frame = cap.read()
                        if not ret:
                            video_done = True
                        else:
                            # Stay uint8 here; _edge_infer_worker does .float()/255
                            # after the H2D copy. Same arithmetic (an exact float32
                            # division of the same integers, so the head model sees
                            # bit-identical input and mAP is unchanged), but a
                            # queued batch is 39MB instead of 157MB and the divide
                            # runs on the inference device instead of stealing CPU
                            # from the 9 edge processes sharing this host.
                            frame = cv2.resize(frame, (640, 640))
                            frames.append(torch.from_numpy(frame).permute(2, 0, 1))
                            progressed = True
                    if len(frames) == batch_size:
                        item = (time.perf_counter(), time.time(), torch.stack(frames))
                        if self._mt_put_nowait(in_q, item):
                            frames = []
                            progressed = True
                    if video_done and len(frames) < batch_size:
                        # Drop the final partial batch (matches the sequential path)
                        # and signal end-of-input to the inference thread.
                        if self._mt_put_nowait(in_q, None):
                            in_sentinel_sent = True
                            progressed = True

                # ---- OUTPUT: publish one finished head output if ready ----
                out = self._mt_get_nowait(out_q)
                if out is not _MT_EMPTY:
                    if out is None:
                        break   # inference thread finished — all outputs drained
                    self._edge_publish(out, batch_id, batch_size, compress, splits, prev_done, pbar)
                    batch_id += 1
                    prev_done = out["_done"]
                    progressed = True
                elif in_sentinel_sent and self._mt_stop.is_set():
                    break   # safety valve: peer stopped abnormally, nothing left to drain

                if not progressed:
                    time.sleep(0.002)
        except Exception as e:
            Log.print_with_color(f"[edge-mt][transfer] {e!r}", "yellow")
            traceback.print_exc()
        finally:
            self._mt_stop.set()
            pbar.close()

    def _edge_publish(self, out, batch_id, batch_size, compress, splits, prev_done, pbar):
        batch_start   = out["batch_start"]
        edge_start_wall = out["edge_start_wall"]
        inference_ms  = out["inference_ms"]
        cut           = out["cut"]
        payload       = out["payload"]

        if self.adaptive_on:
            self._poll_ctrl()   # updates self.current_cut for FUTURE batches
            edge_best_cut = int(cut)
        else:
            edge_best_cut = "N/A" if splits is None else splits

        _bp = time.perf_counter()
        if self.backpressure_on:
            self._check_backpressure(self.backpressure_max)
        bp_ms = (time.perf_counter() - _bp) * 1000

        payload["batch_id"] = batch_id

        _send = time.perf_counter()
        self.send_next_layer(self.intermediate_queue, payload, compress)
        send_ms = (time.perf_counter() - _send) * 1000

        done = time.perf_counter()
        out["_done"] = done
        latency_ms = (done - batch_start) * 1000
        fps = batch_size / (done - prev_done) if prev_done is not None else 0.0
        ram_mb = self.get_ram_mb()
        msg_size = self.size_message if self.size_message is not None else 0

        self.write_metrics(
            mode="split", role="edge", best_cut=edge_best_cut,
            batch_id=batch_id, batch_size=batch_size,
            latency_ms=latency_ms, fps=fps, ram_mb=ram_mb,
            message_size_bytes=msg_size, e2e_latency_ms=0.0,
            edge_start_time=edge_start_wall,
            inference_ms=inference_ms,
            # Everything the transfer thread does for this batch: the
            # back-pressure stall plus Encoder + pickle + basic_publish.
            transfer_ms=bp_ms + send_ms,
            wait_ms=out.get("wait_ms", 0.0))
        pbar.update(batch_size)

    def _edge_infer_worker(self, model, in_q, out_q, width, height, splits, save_set):
        """Pure compute thread: batch (CPU) -> H2D -> head model -> D2H -> out_q."""
        try:
            while True:
                # Time spent blocked here is this device being STARVED by the
                # capture/transfer thread. Recorded per batch so a run can say
                # whether the edge is compute-bound or input-bound.
                _w = time.perf_counter()
                item = self._mt_get(in_q)
                wait_ms = (time.perf_counter() - _w) * 1000
                if item is None:
                    break
                # Utilization markers live on THIS thread: it is the only stage
                # that handles batches strictly one-at-a-time, so its busy
                # intervals never overlap (summing them stays valid). Idle =
                # blocked in _mt_get above, i.e. waiting for an incoming batch.
                self._mark_edge("get input")
                batch_start, edge_start_wall, x_in = item
                # The capture thread hands over uint8 NCHW to keep the hand-off
                # queues (and the H2D copy) 4x smaller; normalise here, on the
                # inference device. float()/255.0 on the same integers is exactly
                # what the old CPU-side astype('float32')/255.0 produced.
                x_in = x_in.to(self.device).float().div_(255.0)

                if self.adaptive_on:
                    cut = self.current_cut
                    sub_model = model[:cut]
                else:
                    cut = splits
                    sub_model = model

                _inf = time.perf_counter()
                y = []
                with torch.no_grad():
                    x, y = inference(sub_model, x_in, y, 0, save_set)
                y[-1] = x
                # Move to CPU here so the transfer thread does no GPU work.
                y = [(t.detach().cpu() if isinstance(t, torch.Tensor) else None) for t in y]
                inference_ms = (time.perf_counter() - _inf) * 1000

                payload = {"data": y, "width": width, "height": height,
                           "edge_start_time": edge_start_wall}
                if self.adaptive_on:
                    payload["cut"] = int(cut)

                out = {"batch_start": batch_start, "edge_start_wall": edge_start_wall,
                       "inference_ms": inference_ms, "cut": cut, "payload": payload,
                       "wait_ms": wait_ms}
                if not self._mt_put(out_q, out):
                    break
                # Blocking on a full out_q (slow network) counts as busy —
                # occupancy, same as the sequential path's send/queue_wait time.
                self._mark_edge("output")
        except Exception as e:
            Log.print_with_color(f"[edge-mt][infer] {e!r}", "yellow")
            traceback.print_exc()
        finally:
            self._mt_put(out_q, None)   # sentinel
            self._mt_stop.set()

    def _last_layer_mt(self, model, batch_size, splits, logger, compress, save_set=None):
        """Cloud, split mode, pipelined: transfer thread (recv + decompress) hands
        each batch to the inference thread (H2D copy + tail model + postprocess)."""
        model.eval()
        model.to(self.device)

        self._mt_stop = threading.Event()
        local_q = _queue.Queue(maxsize=self.mt_queue_size)
        self.channel.queue_declare(self.fps_queue, durable=False)
        self._fps_q = _queue.Queue()   # infer thread -> recv thread: fps 'done' pings
        self._mark_cloud("start")

        infer_t = threading.Thread(
            target=self._cloud_infer_worker,
            args=(model, batch_size, splits, save_set, local_q),
            daemon=True)
        infer_t.start()
        # Receive loop runs in THIS (main) thread — it owns the pika channel.
        self._cloud_recv_worker(local_q, splits, compress)
        infer_t.join()
        self._drain_fps_events()   # flush DONEs for the final in-flight batches

        self._mark_cloud("end")
        # Last step on this device: compute the whole-run ratio from the timing
        # events and park the report on utilization_queue for the server to collect.
        # (Runs on the recv/main thread, which owns the pika channel.)
        self._send_utilization(self._compute_utilization(self._timing_events_cloud, "cloud"))
        if self._map_updated:
            self._send_pred_dir_to_server()

    def _cloud_recv_worker(self, local_q, splits, compress):
        try:
            while True:
                # Publish fps 'done' pings for batches the infer thread finished
                # (done here because this thread owns the pika channel).
                self._drain_fps_events()
                method_frame, header_frame, body = self.channel.basic_get(
                    queue=self.intermediate_queue, auto_ack=True)
                if method_frame and body:
                    received_message_size = len(body)
                    received_data = pickle.loads(body)
                    y = received_data["data"]
                    edge_start_time = y.get("edge_start_time", time.time())
                    if self.adaptive_on:
                        cut = int(y.get("cut", splits if splits is not None else 1))
                    else:
                        cut = splits

                    _dec = time.perf_counter()
                    if compress["enable"]:
                        y["data"] = Decoder(y["data"], y["shape"])
                        y["data"] = [torch.from_numpy(t) if t is not None else None
                                     for t in y["data"]]
                    # Leave tensors on CPU; inference thread does the H2D copy.
                    decode_ms = (time.perf_counter() - _dec) * 1000

                    if not self._mt_put(local_q, (received_message_size, y, edge_start_time, cut, decode_ms)):
                        break
                else:
                    m2, h2, b2 = self.channel.basic_get(
                        queue=f'reply_{self.client_id}', auto_ack=True)
                    if b2:
                        rd = pickle.loads(b2)
                        Log.print_with_color(f"[<<<] Received message from server {rd}", "blue")
                        if rd.get("action") == "STOP":
                            Log.print_with_color("[>>>] Finish!", "red")
                            break
                    else:
                        time.sleep(0.5)
        except Exception as e:
            Log.print_with_color(f"[cloud-mt][recv] {e!r}", "yellow")
            traceback.print_exc()
        finally:
            self._mt_put(local_q, None)   # sentinel
            self._mt_stop.set()

    def _cloud_infer_worker(self, model, batch_size, splits, save_set, local_q):
        pbar = tqdm(desc="Processing video (cloud-mt)", unit="frame")
        batch_id = 0
        prev_done = None
        try:
            while True:
                # Blocked here == starved by the recv thread (which is itself
                # blocked downloading). This is the number that distinguishes
                # "cloud is slow" from "cloud is waiting for bytes".
                _w = time.perf_counter()
                item = self._mt_get(local_q)
                wait_ms = (time.perf_counter() - _w) * 1000
                if item is None:
                    break
                # Utilization markers live on THIS thread: it is the only stage
                # that handles batches strictly one-at-a-time, so its busy
                # intervals never overlap (summing them stays valid). Idle =
                # blocked in _mt_get above, i.e. waiting for an incoming batch.
                self._mark_cloud("get input")
                received_message_size, y, edge_start_time, cut, decode_ms = item

                t0 = time.perf_counter()
                # Prefer the edge's own batch_id (rides in the message) over this
                # loop's local receive-order counter, so frame_num stays correct
                # even if arrival order ever diverges from send order.
                msg_batch_id = y.get("batch_id", batch_id)
                y["data"] = [t.to(self.device) if t is not None else None for t in y["data"]]
                list_output = y["data"]
                x = list_output[-1]

                if self.adaptive_on:
                    use_cut = cut
                    sub_model = model[use_cut:]
                    cloud_best_cut = use_cut
                else:
                    use_cut = splits
                    sub_model = model
                    cloud_best_cut = "N/A" if splits is None else splits

                with torch.no_grad():
                    x, _ = inference(sub_model, x, list_output, use_cut, save_set)

                results = postprocess_yolo(x, conf_thres=0.25, iou_thres=0.5)
                map_results = None if self._batch_past_gt(msg_batch_id, batch_size) else \
                    postprocess_yolo(x, conf_thres=0.001, iou_thres=0.5)
                self._update_map(results, msg_batch_id, batch_size, map_results=map_results)
                # Same point as the sequential path: right after postprocess,
                # before the metrics bookkeeping.
                self._mark_cloud("output")

                done = time.perf_counter()
                cloud_end_wall = time.time()
                latency_ms = (done - t0) * 1000
                fps = batch_size / (done - prev_done) if prev_done is not None else 0.0
                e2e_latency_ms = (cloud_end_wall - edge_start_time) * 1000
                ram_mb = self.get_ram_mb()

                self.write_metrics(
                    mode="split", role="cloud", best_cut=cloud_best_cut,
                    batch_id=batch_id, batch_size=batch_size,
                    latency_ms=latency_ms, fps=fps, ram_mb=ram_mb,
                    message_size_bytes=received_message_size,
                    e2e_latency_ms=e2e_latency_ms, edge_start_time=edge_start_time,
                    # latency_ms here is tail model + both postprocess passes.
                    inference_ms=latency_ms,
                    # Measured on the recv thread for THIS batch, so it is the
                    # decode cost that had to finish before this batch could start.
                    decode_ms=decode_ms,
                    wait_ms=wait_ms)

                # Hand this batch's fps 'done' ping to the recv thread to publish
                # (the recv thread owns the pika channel; this thread must not).
                if self._fps_q is not None:
                    self._fps_q.put(1)

                batch_id += 1
                prev_done = done
                pbar.update(batch_size)
        except Exception as e:
            Log.print_with_color(f"[cloud-mt][infer] {e!r}", "yellow")
            traceback.print_exc()
        finally:
            self._mt_stop.set()
            pbar.close()
            try:
                cv2.destroyAllWindows()
            except Exception:
                pass

    def middle_layer(self, model):
        pass

    def _pivot_and_save(self):
        import glob as _glob

        # Namespaced theo intermediate_queue: mỗi cluster Hungarian (intermediate_queue_k)
        # pivot độc lập, không xóa/ghi đè file của cluster khác chạy chung thư mục.
        lock_path = f"metrics_pivot_{self.intermediate_queue}.lock"
        out_path = f"metrics_pivoted_{self.intermediate_queue}.csv"
        raw_glob = f"metrics_raw_{self.intermediate_queue}_*.csv"

        # Chỉ 1 client thắng lock mới làm pivot (atomic exclusive create)
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
        except FileExistsError:
            return  # Client khác đang làm pivot

        # Đợi các client còn lại ghi xong hàng cuối
        time.sleep(2.0)

        # Thu thập metrics CSV từ personal fanout queue (mỗi cloud có bản copy riêng)
        my_q = self._my_metrics_queue
        if my_q:
            try:
                while True:
                    method_frame, _, body = self.channel.basic_get(queue=my_q, auto_ack=True)
                    if not method_frame:
                        break
                    msg = pickle.loads(body)
                    if msg.get("action") == "METRICS":
                        fname = msg["filename"]
                        with open(fname, 'wb') as f:
                            f.write(msg["data"])
                        Log.print_with_color(f"[Metrics] Received remote metrics: {fname}", "cyan")
            except Exception as e:
                Log.print_with_color(f"[Metrics] Warning collecting remote metrics: {e}", "yellow")

        edge_rows = []
        cloud_rows = []

        edge_seq_counter = 0
        cloud_seq_counter = 0

        for fpath in sorted(_glob.glob(raw_glob)):
            with open(fpath, newline="") as f:
                rows_in_file = list(csv.DictReader(f))
            if not rows_in_file:
                continue
            role = rows_in_file[0]["role"]
            if role in ("edge", "edge_sender"):
                edge_seq_counter += 1
                for row in rows_in_file:
                    row["device_seq"] = edge_seq_counter
                    edge_rows.append(row)
            elif role == "cloud":
                cloud_seq_counter += 1
                for row in rows_in_file:
                    row["device_seq"] = cloud_seq_counter
                    cloud_rows.append(row)

        # Join edge ↔ cloud bằng edge_start_time (timestamp edge nhúng vào mỗi message)
        edge_by_time = {
            row["edge_start_time"]: row
            for row in edge_rows
            if row.get("edge_start_time")
        }
        matched_pairs = []
        matched_edge_times = set()
        for c in cloud_rows:
            t = c.get("edge_start_time", "")
            e = edge_by_time.get(t, {})
            matched_pairs.append((e, c))
            if t:
                matched_edge_times.add(t)
        # Edge rows không có cloud tương ứng (only_edge mode)
        for e in edge_rows:
            if e.get("edge_start_time", "") not in matched_edge_times:
                matched_pairs.append((e, {}))
        # Sắp xếp theo edge_start_time tăng dần
        matched_pairs.sort(key=lambda p: float(p[0].get("edge_start_time") or p[1].get("edge_start_time") or 0))

        n_rows = len(matched_pairs)
        fieldnames = [
            "batch_id", "batch_size", "best_cut",
            "edge_device", "edge_latency_ms", "edge_fps", "edge_ram_mb", "edge_message_size_bytes",
            "cloud_device", "cloud_arrival_order", "cloud_latency_ms", "cloud_fps", "cloud_ram_mb", "cloud_message_size_bytes",
            "e2e_latency_ms",
        ]

        with open(out_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for i, (e, c) in enumerate(matched_pairs):
                writer.writerow({
                    "batch_id":                i,
                    "batch_size":              e.get("batch_size") or c.get("batch_size", ""),
                    "best_cut":                e.get("best_cut")   or c.get("best_cut", ""),
                    "edge_device":             e.get("device_seq", ""),
                    "edge_latency_ms":         e.get("latency_ms", ""),
                    "edge_fps":                e.get("fps", ""),
                    "edge_ram_mb":             e.get("ram_mb", ""),
                    "edge_message_size_bytes": e.get("message_size_bytes", ""),
                    "cloud_device":            c.get("device_seq", ""),
                    "cloud_arrival_order":     c.get("batch_id", ""),
                    "cloud_latency_ms":        c.get("latency_ms", ""),
                    "cloud_fps":               c.get("fps", ""),
                    "cloud_ram_mb":            c.get("ram_mb", ""),
                    "cloud_message_size_bytes":c.get("message_size_bytes", ""),
                    "e2e_latency_ms":          c.get("e2e_latency_ms") or e.get("e2e_latency_ms", ""),
                })

        for fpath in _glob.glob(raw_glob):
            try:
                os.remove(fpath)
            except FileNotFoundError:
                pass
        try:
            os.remove(lock_path)
        except FileNotFoundError:
            pass

        def avg(rows, key, skip_zero_fps=False):
            filtered = rows
            if skip_zero_fps:
                filtered = [r for r in rows if float(r.get("fps") or 0) > 0]
            vals = [float(r[key]) for r in filtered if r.get(key)]
            return round(sum(vals) / len(vals), 3) if vals else None

        def total_fps(rows):
            # Với mỗi device: tính trung bình FPS qua các batch (bỏ batch fps=0)
            # Tổng hệ thống = cộng trung bình FPS của từng device
            by_device = {}
            for r in rows:
                seq = r.get("device_seq")
                val = float(r.get("fps") or 0)
                if val > 0 and seq is not None:
                    by_device.setdefault(seq, []).append(val)
            device_avgs = [sum(v) / len(v) for v in by_device.values() if v]
            return round(sum(device_avgs), 3) if device_avgs else None

        def mb(val):
            return round(val / 1024 / 1024, 3) if val is not None else "N/A"

        cuts = set(r.get("best_cut", "N/A") for r in (edge_rows or cloud_rows))
        cut_str = "/".join(sorted(str(c) for c in cuts))
        all_rows = cloud_rows if cloud_rows else edge_rows
        final_rows = cloud_rows if cloud_rows else edge_rows
        system_fps = total_fps(final_rows)
        valid_batches = len([r for r in final_rows if float(r.get("fps") or 0) > 0])
        # Edge metrics chỉ tính trên batch có cloud match (batch kia tính ở cloud kia)
        # Fallback về tất cả edge_rows nếu không có cloud (only_edge mode)
        matched_edge_rows = [e for e, c in matched_pairs if c and e]
        summary_edge_rows = matched_edge_rows if cloud_rows else edge_rows
        print("=" * 50)
        print(f"  SUMMARY  |  batches={n_rows} (valid={valid_batches})  cut={cut_str}")
        print("=" * 50)
        print(f"  [EDGE]  latency={avg(summary_edge_rows,'latency_ms',True)} ms  fps={avg(summary_edge_rows,'fps',True)}  ram={avg(summary_edge_rows,'ram_mb',True)} MB  msg={mb(avg(summary_edge_rows,'message_size_bytes'))} MB")
        print(f"  [CLOUD] latency={avg(cloud_rows,'latency_ms',True)} ms  fps={avg(cloud_rows,'fps',True)}  ram={avg(cloud_rows,'ram_mb',True)} MB  msg={mb(avg(cloud_rows,'message_size_bytes'))} MB")
        print(f"  [E2E]   latency={avg(all_rows,'e2e_latency_ms',True)} ms")
        print(f"  [SYSTEM TOTAL FPS] {system_fps} fps  (sum of avg fps across {len(set(r.get('device_seq') for r in final_rows))} final device(s))")
        print("=" * 50)
        Log.print_with_color(f"Saved {out_path} ({n_rows} batches)", "green")
        n_edge_devices = len(set(r.get("device_seq") for r in edge_rows))
        if n_edge_devices > 1:
            Log.print_with_color(
                f"[mAP] Skipped: {n_edge_devices} edge devices in this cluster — "
                f"frame alignment undefined for multi-edge mAP.", "yellow")
        elif self._map_updated:
            self._print_map()

        if self.save_detections_json and self._det_count > 0:
            self._write_detections_json()

    def _poll_ctrl(self):
        """Edge: drain SET_CUT control messages from the server, applying the
        latest requested cut (clamped to [1, L-1])."""
        method_frame, _, body = self.channel.basic_get(queue=self.ctrl_queue, auto_ack=True)
        while body:
            try:
                msg = pickle.loads(body)
                if msg.get("action") == "SET_CUT":
                    new_cut = max(1, min(int(msg["cut"]), self._L - 1))
                    if new_cut != self.current_cut:
                        Log.print_with_color(
                            f"[Adaptive][edge] cut {self.current_cut} -> {new_cut}", "cyan")
                        self.current_cut = new_cut
            except Exception:
                pass
            method_frame, _, body = self.channel.basic_get(queue=self.ctrl_queue, auto_ack=True)

    def inference_func(self, model, data, num_layers, splits, batch_size, logger, compress, mode="split", queue_name="intermediate_queue", save_set=None, adaptive=None, multithreading=None, backpressure=None, detections=None, map_cfg=None):
        adaptive = adaptive or {}
        multithreading = multithreading or {}
        backpressure = backpressure or {}
        detections = detections or {}
        map_cfg = map_cfg or {}
        self.adaptive_on = bool(adaptive.get("enable", False)) and mode == "split"
        self.mt_on = bool(multithreading.get("enable", False)) and mode == "split"
        self.mt_queue_size = int(multithreading.get("queue_size", 4))
        # Back-pressure: split uses the configurable cap; only_cloud keeps its own
        # tuned cap (large raw messages) inside its branch.
        self.backpressure_on = bool(backpressure.get("enable", False)) and mode == "split"
        self.backpressure_max = int(backpressure.get("max_queue", 20))
        self.save_detections_json = bool(detections.get("save_json", True))
        # mAP work is measured OFF the hot path when disabled. Everything it costs
        # per batch sits inside the get_input -> output window (_mark_*("output")
        # is written after _update_map), so it is charged to this device's busy
        # time and to e2e: a second full postprocess_yolo at conf=0.001 (which at
        # that threshold keeps nearly every anchor, so its NMS dwarfs the conf=0.25
        # pass), one pred .txt per frame, and one torchmetrics update per frame.
        # Turn it off when the run is measuring throughput/latency — mAP and FPS
        # should not be measured in the same run.
        self.map_on = bool(map_cfg.get("enable", True))
        if not self.map_on:
            Log.print_with_color(
                "[mAP] per-batch mAP OFF (no conf=0.001 pass, no pred files, no metric "
                "updates). map.log/map_window.log will be empty for this run.", "yellow")
        if self.mt_on:
            Log.print_with_color(f"[Pipeline] multithreading ON (queue_size={self.mt_queue_size})", "cyan")
        if self.backpressure_on:
            Log.print_with_color(f"[BackPressure] split-mode guard ON (max_queue={self.backpressure_max})", "cyan")
        if queue_name != self.intermediate_queue:
            self.intermediate_queue = queue_name
            self.channel.queue_declare(self.intermediate_queue, durable=False)

        if self.layer_id == 1:
            try:
                if self.mt_on:
                    self._first_layer_mt(model, data, batch_size, splits, logger, compress, save_set)
                else:
                    self.first_layer(model, data, batch_size, splits, logger, compress, mode, save_set)
            except Exception as e:
                Log.print_with_color(f"[!] Error during inference: {e!r} — saving metrics anyway.", "yellow")
                traceback.print_exc()
            if mode == "only_edge":
                self._pivot_and_save()
        elif self.layer_id == num_layers:
            self._setup_metrics_fanout_queue()
            try:
                if self.mt_on:
                    self._last_layer_mt(model, batch_size, splits, logger, compress, save_set)
                else:
                    self.last_layer(model, batch_size, splits, logger, compress, mode, save_set)
            except Exception as e:
                Log.print_with_color(f"[!] Error during inference: {e!r} — saving metrics anyway.", "yellow")
                traceback.print_exc()
            self._pivot_and_save()
        else:
            self.middle_layer(model)
