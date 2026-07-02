# Split Inference

This project implements **Split Inference for YOLO (YOLOv11 / YOLO26)** to enable real-time object detection on low-power **edge devices (Jetson Nano)** by dividing the neural network across multiple machines.

Instead of transmitting full video frames, the edge device executes the first part of the model (**head**) and sends only **intermediate feature maps** to another device that runs the remaining layers (**tail**).

Beyond the basic split, the system automatically **chooses where to cut** for a fleet of heterogeneous devices, **re-balances the cut at runtime**, **pipelines** compute and I/O, **compresses** feature maps, and **guards against RAM overflow**.

---

# Key Features

| Feature | Summary |
|---|---|
| **Split inference** | Edge runs `layers[:cut]`, cloud runs `layers[cut:]`; only the intermediate feature map crosses the network. |
| **Execution modes** | `split` (distributed), `only_cloud` (cloud does everything), `only_edge` (edge does everything) — for benchmarking. |
| **Automatic partitioning (Hungarian)** | Profiles every device + the network, clusters similar devices, and solves an optimal edge↔cloud matching + cut point to maximize throughput. |
| **Adaptive split-point controller** | At runtime the server watches each cluster's queue depth and nudges the cut deeper/shallower to keep the pipeline balanced. |
| **Message-size guard** | The controller never moves the cut to a point whose feature map would exceed the broker's message-size limit (skips to the nearest safe cut). |
| **Multithreading pipeline** | Each device runs 2 threads — one for data transfer (capture / send / recv), one for model inference — overlapping compute with I/O. |
| **Feature-map compression** | Quantized **delta** codec (1–16 bit) exploiting frame-to-frame similarity to shrink messages. |
| **RAM overflow protection** | Broker back-pressure (edge stalls when the queue is too deep) + bounded pipeline queues + flat-RAM detection streaming. |
| **Real profiling & bandwidth** | Per-layer timing (CUDA events / perf-counter) and live uplink bandwidth measurement feed the partitioner. |
| **Metrics & mAP** | Per-batch latency / FPS / RAM / message size / end-to-end latency CSV, plus optional mAP against ground truth. |
| **Tracker / visualization** | Render detections onto the video in real time or after the run. |

---

# Table of Contents

* [Overview](#overview)
* [Architecture](#architecture)
* [Pipeline](#pipeline)
* [Execution Modes](#execution-modes)
* [Automatic Partitioning (Hungarian Clustering)](#automatic-partitioning-hungarian-clustering)
* [Adaptive Split-Point Controller](#adaptive-split-point-controller)
* [Multithreading Pipeline](#multithreading-pipeline)
* [Feature-Map Compression](#feature-map-compression)
* [RAM Overflow Protection](#ram-overflow-protection)
* [Project Structure](#project-structure)
* [How to Run](#how-to-run)

  * [Clone Repository](#1-clone-the-repository)
  * [Install Dependencies](#2-install-dependencies)
  * [Start RabbitMQ](#3-start-rabbitmq)
* [Configuration](#configuration)
* [Running the System](#running-the-system)
* [Visualization (Tracker)](#visualization-tracker)
* [Metrics](#metrics)
* [Tested Hardware](#tested-hardware)
* [Application Scenarios](#application-scenarios)
* [License](#license)

---

# Overview

<p align="center">
  <img src="imgs/overview.png" width="850">
</p>

In traditional edge AI pipelines, raw video frames are transmitted to a centralized server for processing. This creates high network bandwidth usage and latency.

**Split inference** solves this by dividing the neural network into two parts:

1. **Head (Edge Device)** – processes the early layers of the model.
2. **Tail (Server / Cloud)** – processes the remaining layers.

Only **intermediate feature maps** are transmitted instead of full images, reducing bandwidth and improving scalability.

---

# Architecture

The system consists of three roles communicating over **RabbitMQ**.

## Stage 1 – Edge Device (Head)

Devices located at the edge such as **traffic cameras or embedded devices (Jetson Nano)**.

Responsibilities:

* Capture video frames
* Run the first layers of YOLO (`layers[:cut]`)
* Compress intermediate feature maps using quantization
* Publish feature maps to the cluster's queue

## Stage 2 – Tail Device (Cloud)

Devices located in the **cloud or high-performance servers**.

Responsibilities:

* Receive feature maps from edge devices
* Decompress and run the remaining layers (`layers[cut:]`)
* Post-process (NMS) and produce final detection results

## Server – Controller

Central coordination service responsible for:

* Registering clients and collecting their profiles + bandwidth
* Selecting model cut-layers (Hungarian partitioner)
* Running the **adaptive controller** that re-balances the cut at runtime
* Coordinating communication using **RabbitMQ**

> Multiple edges and clouds can run at once. The partitioner groups them into **clusters**; each cluster gets its own intermediate queue (`intermediate_queue_k`) and its own cut point.

---

# Pipeline

<p align="center">
  <img src="imgs/SI-Inference.jpg" width="900">
</p>

Pipeline steps:

1. Clients register with the server (sending per-layer timing + bandwidth).
2. Server profiles the fleet, clusters devices, and picks the initial cut per cluster.
3. Server ships the model + assignment to every client; inference begins.
4. During the run, the server monitors queue depth and adjusts cuts adaptively.

---

# Execution Modes

Set via the `experiment` block in `config.yaml`. When `experiment.enable: False`, the mode is always **split**.

| Mode | Edge does | Cloud does | Purpose |
|---|---|---|---|
| **split** (default) | `layers[:cut]` → send feature map | receive → `layers[cut:]` → NMS | Real distributed split inference. |
| **only_cloud** | just sends raw frames | runs the whole model | Baseline: all compute on cloud. |
| **only_edge** | runs the whole model | just collects results | Baseline: all compute on edge. |

> **Adaptive**, **multithreading**, and split-mode **back-pressure** only apply in `split` mode.

---

# Automatic Partitioning (Hungarian Clustering)

Instead of a fixed cut, the server can choose the split automatically for a fleet of heterogeneous devices. Enabled with `clustering.enable: True` (split mode).

**How it works** (`src/Clustering.py`, `src/Profiler.py`):

1. **Profile** — each client measures its per-layer inference time (CUDA events on GPU, `perf_counter` on CPU; cached to `profile_<model>_<device>_bs<N>.npy`) and its **uplink bandwidth** to the server (timed RabbitMQ round-trip). Both are sent at registration.
2. **Cost model** — for any candidate cut the solver estimates `edge_time + network_time + cloud_time` using prefix/suffix sums of layer times and the measured feature-map size per cut.
3. **Cluster** — edges (and clouds) are grouped into `K` clusters by similarity of their timing profiles (agglomerative clustering).
4. **Match + cut** — for every (edge-cluster, cloud-cluster) pair the best cut is chosen by a queue-theoretic **throughput** model; the **Hungarian algorithm** (`scipy.linear_sum_assignment`) then finds the optimal one-to-one cluster matching. `K` is swept 1…`max_clusters` and the best is kept.

**Profiles source** (`clustering.profile_source`): `real` (use measured client profiles), `simulated` (built-in DEVICE_A/B/C profiles), or `auto` (real if available).

If `clustering.enable: False`, a **fixed** cut is used from `server.cut-layer` (`a`/`b`/`c`/`d` → preset layer indices).

> Feature-map sizes per cut live in `src/Clustering.py`. To measure them for a new model, run `python tools/measure_cut_sizes.py --model <name> --batch_size <N> --compress --num_bit 8`.

---

# Adaptive Split-Point Controller

The Hungarian pass picks a good **initial** cut, but the optimum drifts as load/network change. The server runs a background controller that **re-balances each cluster's cut at runtime** (`src/Server.py`).

**Mechanism:**

* The server samples each cluster's `intermediate_queue_k` depth (RabbitMQ management API) and counts batches processed.
* Every `batches_per_check` batches it looks at the recent samples:
  * **Queue overflowing** (`high_ratio` of samples ≥ `high_threshold`) ⇒ the cloud is the bottleneck ⇒ **cut deeper** (edge does more, cloud less) so the queue drains.
  * **Queue empty** (`low_ratio` of samples ≤ `low_threshold`) ⇒ the cloud is starved ⇒ **cut shallower** (cloud does more).
* A `cooldown_batches` guard prevents oscillation.

**Per-batch dynamic cut:** both edge and cloud hold the **full** model; the cut index travels **inside each message**. So a cut change takes effect on the next batch with no synchronization barrier — in-flight messages are always decoded with the cut they were produced at. The server pushes the new cut to edges via a per-edge control queue (`ctrl_<client_id>`).

**Message-size guard:** shallower cuts produce **larger** feature maps (early layers have big spatial maps). The controller never moves to a cut whose estimated message exceeds `adaptive.max_message_mb`; it **skips over** unsafe cuts to the nearest safe one in the desired direction, and refuses to move if none is safe. This prevents the broker from rejecting an oversized message.

> If you use large batches, raise RabbitMQ's `max_message_size` **and** `adaptive.max_message_mb` together, or lower `batch-size`.

---

# Multithreading Pipeline

Enabled with `multithreading.enable: True` (split mode). Each device runs **two threads** that overlap compute with I/O, handing batches off through bounded in-process queues.

**Edge:**

```
        in_q                          out_q
 ┌───────────────┐   batch    ┌──────────────┐  head out  ┌───────────────┐
 │ TRANSFER thr  │ ─────────▶ │ INFERENCE thr│ ─────────▶ │ TRANSFER thr  │
 │ read frames   │            │  head model  │            │ compress+pub  │
 │ resize/stack  │            │  (GPU only)  │            │ ctrl poll     │
 └───────────────┘            └──────────────┘            └───────────────┘
        (owns the RabbitMQ channel)              (pure compute, no network)
```

* **Transfer thread** — all I/O: captures video frames, and publishes finished head outputs (compress + send). Owns the RabbitMQ channel.
* **Inference thread** — pure model compute.

**Cloud:** transfer thread receives + decompresses; inference thread runs the tail + post-processing. So the next batch's receive/decode overlaps the current batch's inference.

`multithreading.queue_size` bounds each hand-off queue (back-pressure between threads). Pika channels are not thread-safe, so all queue I/O stays on one thread by design.

---

# Feature-Map Compression

Enabled with `compress.enable: True` (`src/Compress.py`). A **quantized delta codec** shrinks the intermediate message:

* **Quantization** — floats mapped to `num_bit` integers using the batch's global min/max.
* **Delta encoding** — the first frame is stored fully; each subsequent frame stores only the values that changed vs the previous frame, plus a change bitmask. Consecutive video frames are highly similar, so this compresses well.
* **Bit-packing** — supports non-byte-aligned widths (e.g. 2- or 4-bit); 8/16-bit use fast numpy paths.

`num_bit` (1–16, default 8) trades accuracy for size. Lower bits → smaller messages, lower fidelity.

---

# RAM Overflow Protection

The system bounds memory at every stage so long runs / fast edges can't blow up RAM:

* **Broker back-pressure** (`backpressure` block) — an edge **stalls before publishing** when its intermediate queue reaches `max_queue` messages, so feature maps can't pile up in the broker faster than the cloud drains them. `only_cloud` uses its own tuned cap (large raw frames). Composes with the adaptive controller (which rebalances *before* the hard cap is hit).
* **Bounded pipeline queues** — the multithreading hand-off queues are size-capped (`multithreading.queue_size`), so the edge holds at most a few batches in flight.
* **Flat-RAM detections** (`detections` block) — detections are streamed to `detections_stream.jsonl` during the run (append-and-forget); `detections.json` is rebuilt from that file at the end. Nothing grows in RAM with video length.
* **One-at-a-time cloud receive** — the cloud pulls one message at a time, never building an in-process backlog.

> For the broker itself, also set RabbitMQ's `vm_memory_high_watermark` appropriately — it blocks publishers when broker RAM gets high.

---

# Project Structure

```
split_inference/
│
├── client.py            # Edge or tail inference node
├── server.py            # Central controller entry point
├── tracker.py           # Detection visualizer (realtime / post)
├── config.yaml          # System configuration
├── requirements.txt     # Python dependencies
│
├── src/
│   ├── Server.py        # Controller: registration, Hungarian, adaptive controller
│   ├── Scheduler.py     # Edge/cloud inference loops (sequential + multithreaded), metrics
│   ├── RpcClient.py     # Client-side registration + model loading/slicing
│   ├── Model.py         # Split-aware layer execution + YOLO post-processing (NMS)
│   ├── Compress.py      # Quantized delta codec for feature maps
│   ├── Clustering.py    # Profiling cost model + clustering + Hungarian matching
│   ├── Profiler.py      # Per-layer timing + bandwidth measurement
│   ├── Utils.py         # Queue cleanup, IoU/mAP helpers
│   └── Log.py           # Colored logging
│
├── cfg/                 # YOLO architecture definitions
├── tools/
│   └── measure_cut_sizes.py   # Measure feature-map size per cut for a model
├── imgs/                # Images used in README
│
├── metrics_pivoted_<queue>.csv  # Per-cluster performance results (one row per batch)
├── detections.json              # Final detections (frame → boxes)
└── detections_stream.jsonl      # Streamed detections (for the realtime tracker)
```

---

# How to Run

## 1. Clone the repository

```bash
git clone https://github.com/filrg/split_inference
cd split_inference
```

## 2. Install dependencies

Python **3.8 or higher** is required.

```bash
pip install -r requirements.txt
```

## 3. Start RabbitMQ

RabbitMQ is used for communication between distributed components. The **management plugin** must be enabled (the controller and queue cleanup use the HTTP API on port 15672).

RabbitMQ admin interface:

```
http://localhost:15672
```

Default credentials:

```
username: guest
password: guest
```

---

# Configuration

Edit **config.yaml** before running the system. Full annotated example:

```yaml
name: YOLO
server:
  cut-layer: a            # fixed-cut preset (a/b/c/d) — used only when clustering.enable: False
  clients:
    - 1                   # number of EDGE clients (layer_id 1)
    - 1                   # number of CLOUD clients (layer_id 2)
  model: yolo26n
  batch-size: 32

experiment:
  enable: False           # False → mode = split
  mode: only_cloud        # split | only_cloud | only_edge (used only when enable: True)

rabbit:
  address: localhost
  username: guest
  password: guest
  virtual-host: /

debug-mode: False
data: video.mp4
log-path: .

compress:
  enable: True
  num_bit: 8              # 1–16; lower = smaller messages, lower fidelity

clustering:
  enable: True            # True → Hungarian auto-selects the cut; False → fixed cut-layer
  num_A: 3                # simulated-profile device counts (num_A+num_B+num_C = edge total)
  num_B: 3
  num_C: 3
  num_cloud: 3            # = cloud total
  max_clusters: 3         # ≤ min(edge, cloud)
  network_rate_mb_s: 100.0
  measure_bandwidth: True # True → measure live each run; False → use network_rate_mb_s
  profile_source: real    # real | simulated | auto

multithreading:
  enable: True            # split mode: each device runs 2 pipelined threads
  queue_size: 4           # max batches buffered between inference and transfer threads

backpressure:
  enable: True            # edge stalls when its queue is too deep (RAM guard)
  max_queue: 20           # split-mode queue-depth cap (only_cloud uses its own cap)

detections:
  save_json: True         # write detections.json at the end (streamed → flat RAM)

adaptive:
  enable: True            # split mode: server nudges the cut based on queue depth
  batches_per_check: 20   # evaluate every N batches
  high_threshold: 8       # queue depth ≥ this = "overflow"
  low_threshold: 1        # queue depth ≤ this = "empty"
  high_ratio: 0.6         # ≥60% overflow samples → cut deeper (edge does more)
  low_ratio: 0.6          # ≥60% empty samples    → cut shallower (cloud does more)
  poll_interval_s: 0.25   # queue-depth sampling period
  step: 1                 # layers to shift per adjustment
  cooldown_batches: 20    # min batches between adjustments (anti-oscillation)
  max_message_mb: 15.0    # never move to a cut whose message would exceed this (broker guard)
```

---

# Running the System

## Step 1 – Start Server

```bash
python server.py
```

## Step 2 – Start Clients

Edge device (head, `layer_id 1`):

```bash
python client.py --layer_id 1
```

Optional CPU mode / device name:

```bash
python client.py --layer_id 1 --device cpu --name edge-1
```

Tail device (cloud, `layer_id 2`):

```bash
python client.py --layer_id 2 --name cloud-1
```

Start as many edges/clouds as the `server.clients` counts require. `--name` is used to label devices in the partitioner output.

---

# Visualization (Tracker)

`tracker.py` draws the detections onto the original video.

```bash
# Realtime — run alongside inference, displays as detections arrive
python tracker.py

# Post — render detections.json to an output video after the run
python tracker.py --mode post --output output.mp4
```

* **realtime** mode polls `detections_stream.jsonl` and shows frames as they are produced (press `q` to quit).
* **post** mode reads `detections.json` and writes an annotated `output.mp4`.

---

# Tested Hardware

| Device           | Role                   |
| ---------------- | ---------------------- |
| Jetson Nano      | Edge Client (Head)     |
| Jetson Nano      | Tail Client            |
| Laptop / Desktop | Tracker                |
| LAN Network      | RabbitMQ communication |

---

# Application Scenarios

* Smart traffic monitoring
* Edge surveillance AI
* Distributed deep learning research
* Bandwidth reduction experiments

---

# Metrics

After each run, the system produces `metrics_pivoted_<queue>.csv` (one file per cluster), with one row per batch. Below is a description of each column.

## Column Descriptions

| Column | Description |
|---|---|
| `batch_id` | Row index in the CSV. When multiple devices run simultaneously, rows from all devices are interleaved into one file. |
| `batch_size` | Number of frames processed together in one model forward pass. |
| `best_cut` | Layer index used to split the model. Edge runs layers up to `best_cut`, cloud runs the rest. With the adaptive controller this **varies per batch** as the cut moves. |

## Latency

Measured independently at each device using:
```
batch_start = time.perf_counter()   # immediately before processing
# run assigned model layers, compress / decompress, send / receive
batch_end   = time.perf_counter()

latency_ms = (batch_end - batch_start) × 1000
```

- **edge_latency_ms** — total time for the edge device to process one batch: includes running its assigned model layers, compressing the feature map, and publishing the message to the queue.
- **cloud_latency_ms** — total time for the cloud device to process one batch: includes receiving the message, decompressing the feature map, running its assigned model layers, and postprocessing the results.

> In **multithreading** mode the per-stage timings measure one thread's portion (decode/send are overlapped, not added in); `e2e_latency_ms` and `fps` remain the correct end-to-end / throughput numbers.

## FPS

Measured independently at each device using:
```
fps = batch_size / (batch_end - prev_batch_end)
```

`prev_batch_end` is the finish time of the previous batch on the same device, so FPS reflects how many frames that device completes per second between two consecutive batches. The first batch of every device always reports **0.0** because there is no previous batch to compare against.

The **total system FPS** is the sum of the per-device average FPS across all final devices (cloud devices in split/only-cloud mode, edge devices in only-edge mode), since all final devices process frames in parallel. The first-batch **0.0** values are excluded from the per-device average so they do not distort the result.

## RAM

```
ram_mb = psutil.Process(os.getpid()).memory_info().rss / (1024 × 1024)
```

Reports the **Resident Set Size (RSS)** — physical RAM occupied by that process at the end of each batch. Does not include memory used by other processes on the same machine. (On Jetson, `tegrastats` RAM is used when available.)

## Message Size

Both values are measured on every batch.

- **edge_message_size_bytes** — size in bytes of the pickle-serialized message measured by the edge immediately before publishing to RabbitMQ. When compression is enabled, the message contains the first frame in full (quantized) plus delta-encoded subsequent frames, so this value varies per batch depending on motion between frames within the batch. When compression is disabled, all frames are sent as raw tensors and the size is fixed.

- **cloud_message_size_bytes** — size in bytes of the raw message received by the cloud from RabbitMQ. This is the same bytes as `edge_message_size_bytes` arriving on the receiving end, so the two columns reflect the same message and should be equal each batch.

## End-to-End Latency

The edge embeds its processing start time inside every message it sends:
```
edge side : y = {"edge_start_time": batch_start, ...}

cloud side: e2e_latency_ms = (cloud_batch_end - edge_start_time) × 1000
```

This captures the full pipeline latency for one batch:
```
e2e = edge_latency + queue_wait_time + cloud_latency
```

**Queue wait time** is the time a batch spends waiting inside the RabbitMQ intermediate queue before the cloud picks it up. If edge devices send faster than cloud devices can process, batches accumulate in the queue and each subsequent batch waits longer, causing e2e to grow over time. A growing e2e is a sign that the system is unbalanced at the chosen cut point — which the adaptive controller and back-pressure are designed to counter.

> **Note:** `batch_start` is recorded after all frames in the batch have been read from the video, so video I/O time is **not** included. E2E measures inference pipeline latency only.

## mAP (optional)

If a `datasets/groundtruth/` folder with YOLO-format `.txt` labels is present on the cloud device, the system computes **mAP@50** and **mAP@50:95** (via `torchmetrics`) and prints them at the end of the run.

---

# License

See [LICENSE](./LICENSE)
