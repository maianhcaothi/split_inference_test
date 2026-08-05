# Claude Design Prompt — Split Inference Visual System

> **How to use:** paste this whole file into Claude (claude.ai, Claude Code, or an
> Artifact request) and add one line at the top saying which deliverable you want,
> e.g. *"Build deliverable D (the metrics dashboard)"*. Everything Claude needs —
> context, data schemas, visual identity, and acceptance criteria — is below.

---

## 1. Project context (what you are visualizing)

**Split Inference** runs YOLO object detection across multiple machines. An **edge
device** (Jetson Nano) runs the first layers of the model (`layers[:cut]`), compresses
the intermediate feature map, and publishes it to **RabbitMQ**. A **cloud device**
consumes it, runs the remaining layers (`layers[cut:]`), and produces detections.
A central **server (controller)** registers all clients, profiles them, picks the cut
point per cluster (Hungarian matching), and **moves the cut at runtime** based on
queue depth (adaptive controller).

Key concepts every visual must get right:

- **Three roles:** Edge (head), Cloud (tail), Server (controller). The broker
  (RabbitMQ) is infrastructure between them, not a compute role.
- **Two planes:** the **data plane** (frames → feature maps → detections, high
  volume) and the **control plane** (registration, cut updates via `ctrl_<id>`
  queues, `fps_queue` DONE signals — low volume). These must be visually distinct.
- **Clusters:** edges and clouds are grouped; each cluster has its own
  `intermediate_queue_k` and its own cut point.
- **The cut is dynamic:** the cut index travels inside each message, so it can
  change per batch with no synchronization barrier.
- **Feedback loop:** deep queue ⇒ cut deeper (edge does more); empty queue ⇒ cut
  shallower (cloud does more); a message-size guard blocks cuts whose feature map
  would exceed `max_message_mb`.

Measured headline result (use it in any hero/summary visual): dynamic cut =
**31.3 fps** vs static cut = **29.4 fps** (**+6.4 %**), 252 batches × 32 frames.

### Per-layer scripts

Each role has its own detailed design script — responsibilities, threads, exact
queue names, state machine, animation beats, and accuracy rules. When a
deliverable focuses on (or includes) a role, paste its script alongside this
file:

- [design/edge_layer.md](design/edge_layer.md) — Edge (head): capture → head inference → compress → publish; back-pressure; ctrl polling.
- [design/cloud_layer.md](design/cloud_layer.md) — Cloud (tail): receive → decompress → tail + NMS; detections stream; DONE signals; backlog drain.
- [design/server_layer.md](design/server_layer.md) — Server (controller): registration, Hungarian partitioning, adaptive cut control, FPS meter.

---

## 2. Visual identity (design tokens)

Use these tokens consistently across every diagram, chart, and dashboard so the
whole project reads as one system.

### Color roles

| Token | Role | Light | Dark |
|---|---|---|---|
| `--edge` | Edge devices / edge-side series | `#0E7490` (teal 700) | `#22D3EE` |
| `--cloud` | Cloud devices / cloud-side series | `#7C3AED` (violet 600) | `#A78BFA` |
| `--server` | Server / controller / control plane | `#B45309` (amber 700) | `#FBBF24` |
| `--broker` | RabbitMQ, queues, messages in flight | `#475569` (slate 600) | `#94A3B8` |
| `--data` | Feature maps / payload / message size | `#15803D` (green 700) | `#4ADE80` |
| `--alert` | Overflow, back-pressure, guard refusals | `#B91C1C` (red 700) | `#F87171` |
| `--ink` | Text | `#1E293B` | `#E2E8F0` |
| `--bg` | Background | `#FFFFFF` | `#0F172A` |
| `--surface` | Cards / panels | `#F8FAFC` | `#1E293B` |

Rules:
- One role = one hue, **everywhere**. Edge is always teal, cloud always violet —
  in diagrams, chart series, table headers, legends.
- The palette must stay legible for colorblind readers: never distinguish two
  series by red vs green alone; pair color with position, label, or line style.
- Support **both light and dark** themes via `prefers-color-scheme` plus a manual
  override on `:root[data-theme]`.

### Shapes & line conventions (diagrams)

| Element | Convention |
|---|---|
| Edge device | Rounded rectangle, `--edge` border, small camera glyph |
| Cloud device | Rounded rectangle, `--cloud` border, chip/server glyph |
| Server/controller | Hexagon or distinct rounded rect, `--server` border |
| Queue | Horizontal "pill with segments" (stacked messages), `--broker` |
| Data-plane flow | **Solid thick arrow**, labeled with what crosses (e.g. "feature map, ~1.7 MB") |
| Control-plane flow | **Dashed thin arrow**, `--server` color |
| The cut | A bold vertical scissor line through a layer strip; layers left = teal fill, right = violet fill |
| Cluster boundary | Faint dashed rounded box grouping its edges + queue + clouds |

### Typography & layout

- Font: system UI stack (`system-ui, Segoe UI, Roboto, sans-serif`); numbers in
  tables/axes use `font-variant-numeric: tabular-nums`.
- Diagram labels ≥ 12 px equivalent; never rotate text more than 45°.
- Spacing on an 8 px grid; cards with 12–16 px radius, subtle 1 px border.
- Wide content (tables, timelines) scrolls inside its own container — the page
  never scrolls horizontally.

### Motion & animation guide

Animation in this project has one job: **show the system's dynamics** — batches
flowing, queues filling, the cut moving. Never animate for decoration.

**Motion tokens** (define once as CSS variables, reuse everywhere):

| Token | Value | Use |
|---|---|---|
| `--dur-instant` | 120 ms | hover/focus feedback, tooltip show |
| `--dur-state` | 250 ms | state changes: queue slot fill, stat tile update |
| `--dur-move` | 600 ms | the cut line sliding to a new layer index |
| `--dur-travel` | data-driven | a message crossing edge → queue → cloud (see below) |
| `--ease-standard` | `cubic-bezier(0.2, 0, 0, 1)` | almost everything |
| `--ease-pulse` | `cubic-bezier(0.4, 0, 0.6, 1)` | attention pulses (guard refusal, overflow) |

**Semantic motion mapping** — each system event always animates the same way:

| System event | Animation |
|---|---|
| Batch published (edge → queue) | A small rounded "message chip" (`--data` fill) travels left→right along the solid data-plane path; its **size scales with `message_size_bytes`** |
| Queue depth change | The queue pill gains/loses a segment with a `--dur-state` fill transition; at depth ≥ `high_threshold` the pill border shifts to `--alert` |
| Cloud consumes a batch | Chip slides out of the queue into the cloud node, which glows briefly (`--dur-state` opacity pulse on its border) |
| Cut moves | The scissor line **slides** (never jumps) to the new layer over `--dur-move`; the layer blocks between old and new cut cross-fade teal↔violet; a small `cut 4 → 5` label fades in above it |
| Control message (ctrl_/fps_queue) | A tiny dashed-outline chip travels along the dashed amber path — visually lighter than data chips |
| Back-pressure stall | The edge node dims to 60 % opacity with a "paused" badge; no shaking or blinking |
| Guard refusal (max_message_mb) | The blocked cut position pulses once in `--alert` (`--ease-pulse`), then a static ⃠ marker remains |

**Timing & choreography rules:**

- Drive replay from data, not from arbitrary delays: reconstruct each batch's
  timeline from the CSV (`edge_latency_ms`, `e2e_latency_ms`; queue wait =
  `e2e − edge − cloud`) and scale it by a user-controllable speed factor
  (0.5× / 1× / 4× / 16×). One on-screen clock shows replay time vs real time.
- Multiple chips may be in flight at once — that *is* the pipeline; don't
  serialize them. But cap simultaneous animated elements (~30) and recycle DOM
  nodes.
- Stagger, don't swarm: when several clusters act in the same instant, offset
  their animations by 40–80 ms so the eye can separate them.
- Nothing loops forever. Idle state = static diagram. Ambient looping motion
  (spinners, floating particles) is banned.

**Technical rules:**

- Animate only `transform` and `opacity` (compositor-friendly); never animate
  `width`/`left`/`top` on moving elements. Position chips with
  `transform: translate(...)`.
- Use CSS transitions/keyframes for state changes; use one
  `requestAnimationFrame` loop for the data-driven replay (chips, clock). No
  animation libraries, no SMIL.
- Pause the rAF loop when the tab is hidden (`visibilitychange`) and when the
  user presses pause. Provide **play / pause / speed / restart** controls and a
  scrubbing timeline.
- **`prefers-reduced-motion: reduce` must be honored:** replace travel/slide
  animations with instant state changes + a highlighted event log (the story
  stays readable, only the motion is removed).

---

## 3. Real data sources (exact schemas)

### `metrics_pivoted_intermediate_queue_<k>.csv` — one row per batch, one file per cluster

Real header and sample rows:

```csv
batch_id,batch_size,best_cut,edge_device,edge_latency_ms,edge_fps,edge_ram_mb,edge_message_size_bytes,cloud_device,cloud_arrival_order,cloud_latency_ms,cloud_fps,cloud_ram_mb,cloud_message_size_bytes,e2e_latency_ms
0,32,4,1,391.973,,1272.316,1729855,1,0,709.457,,1256.781,1729855,1689.771
1,32,4,1,367.974,243.89,1277.0,1720209,1,1,178.083,177.376,1205.203,1720209,1714.378
```

Notes for charting:
- `best_cut` changes over the run when the adaptive controller is on — plot it as
  a **step chart**, it is the star of the story.
- First batch per device has empty `edge_fps`/`cloud_fps` (no previous batch) —
  skip, don't zero-fill.
- `e2e_latency_ms ≈ edge_latency + queue_wait + cloud_latency`; a growing e2e
  means the queue is filling (imbalance) — worth annotating.
- Per-gap FPS is bursty; **never** present the arithmetic mean of `1/Δt` as
  throughput. Whole-run throughput = total frames / total time.

### `detections_stream.jsonl` — one JSON object per frame (streamed during the run)

```json
{"frame": 1, "dets": [{"box": [222.33, 244.95, 402.64, 639.90], "score": 0.3787, "class": 27}]}
```

`box` is `[x1, y1, x2, y2]` in pixels; `class` is a COCO class index.
`detections.json` is the same data rebuilt as one map at the end of the run.

### Run summary block (printed by the server)

```
  [SYSTEM FPS]        31.313 fps   = 252 DONE x 32 / 257.53s  (START -> last DONE)
  [steady-state]      32.329 fps   = 251 x 32 / 248.45s  (first -> last DONE)
  batches counted: 252   stop reason: work queues drained + grace
```

### Config knobs that matter visually (`config.yaml`)

`batch-size` (32), `compress.num_bit` (8), `clustering.max_clusters`,
`adaptive.high_threshold` (8) / `low_threshold` (1) / `max_message_mb` (15),
`backpressure.max_queue`, `multithreading.queue_size` (4). Threshold lines on
charts should come from these values.

---

## 4. Deliverables (pick one per request)

### A. System architecture diagram (SVG)

One landscape SVG for the README (`imgs/`): N edge devices → per-cluster
`intermediate_queue_k` in a RabbitMQ band → cloud devices → detections; server on
top with dashed control arrows (registration/profiles in, cut updates out via
`ctrl_<id>`, DONE signals in via `fps_queue`). Show **two clusters** to make the
clustering concept visible. Follow every convention in §2.

### B. "Anatomy of the cut" explainer (SVG)

A single YOLO model drawn as a horizontal strip of layer blocks with the scissor
cut line; annotate: feature-map size vs cut depth (early cuts = big messages),
the `max_message_mb` guard zone shaded in `--alert`, and arrows showing "deeper ⇢
edge does more / shallower ⇢ cloud does more".

### C. Adaptive-controller feedback loop diagram (SVG)

A circular loop: queue depth sampled → window vote (≥60 % high / ≥60 % low) →
cut moves ±1 (respecting cooldown + size guard) → throughput changes → queue
depth responds. Use `--server` for the controller steps, `--broker` for the queue,
`--alert` for guard refusals.

### D. Metrics dashboard (single self-contained HTML file)

Loads a `metrics_pivoted_*.csv` via a file picker (no server, no CDN — inline all
JS/CSS). Layout:

1. **Header stat tiles:** total frames, run duration, mean e2e latency, final
   cut, number of cut changes.
2. **Cut timeline** — step chart of `best_cut` vs `batch_id` (the hero chart).
3. **E2E latency** vs `batch_id`, with edge/cloud latency as a stacked area under
   it (teal + violet), so queue-wait time is visible as the gap.
4. **Message size** vs `batch_id` (`--data` color) with the 15 MB guard as a
   dashed `--alert` reference line.
5. **RAM** per device over time (edge teal, cloud violet).
6. Optional: per-device FPS as faint dots with a rolling frames/time line on top.

Charts share the batch_id x-axis and a synced hover cursor. Annotate cut-change
batches with a small marker on every chart.

### E. Animated pipeline replay (single self-contained HTML file)

An animated page that **replays a real run from the CSV** — no live broker
connection. Follow the Motion & animation guide in §2 exactly. Scene layout is
the architecture diagram from deliverable A (edges → per-cluster queue → clouds,
server above with dashed control paths), brought to life:

1. **Message chips** flow edge → queue → cloud per batch, sized by
   `message_size_bytes`, timed from the reconstructed batch timeline.
2. **Queue pills** fill and drain; crossing `adaptive.high_threshold` turns the
   border `--alert` — the viewer should *see* the imbalance before the cut moves.
3. **The cut strip** (deliverable B's layer strip, docked at the bottom) slides
   its scissor line whenever `best_cut` changes, with the `4 → 5` label.
4. **Control chips** travel the dashed amber paths when a cut update is pushed.
5. **HUD:** replay clock, speed control (0.5×–16×), play/pause/restart, scrub
   bar, and a live event log (`batch 87: queue high 8/10 → cut 4 → 5`).
6. Stat tiles (frames done, current cut, queue depth, rolling fps) update with
   `--dur-state` transitions as the replay progresses.

Load the CSV via file picker; if no file is chosen, offer a small embedded demo
run so the page is never blank.

### F. Interactive pipeline editor (full web application)

Design a modern interactive web-based visualization **editor** for Split
Inference pipelines. The purpose is to visually describe how AI inference is
executed across multiple devices (Edge, Mobile, Gateway, Cloud, …), how data
flows between them, and what each device is responsible for — and to let
researchers **design, modify, optimize, and understand** such pipelines
interactively. Follow §2 for all colors, shapes, and motion; §5 constraints
apply (aim for one self-contained HTML file — no CDN, no build step).

#### F.1 System visualization

Every device is a **node** on an interactive canvas. Each node displays:

- Device name
- Hardware type (Phone, Jetson, Raspberry Pi, Server, …)
- Model running on the device
- Current inference stage
- Source folder
- Running algorithm
- Optimization techniques enabled

Each **connection** between devices visualizes:

- Which files are executed
- Which tensors/features are transmitted
- Data format, data size
- Bandwidth usage
- Estimated latency

Example vertical flow:

```
Input Image
      │
      ▼
Mobile Device        Run: src/mobile/backbone.py
                     Output: Intermediate Feature Tensor
      │  send tensor
      ▼
Edge Server          Run: src/server/head.py
                     Output: Prediction
```

Connections must clearly indicate **what** is sent, **why** it is sent, and the
**expected communication cost**.

#### F.2 Interactive editing

Every component is editable through buttons or menus: **Add Device, Delete
Device, Edit Device, Move Layer, Split Model Here, Merge Layers, Change
Communication Method, Add Optimization, Remove Optimization, Duplicate
Pipeline.**

Clicking a device opens a property panel to modify: executed file, Python
module, model layer range, input/output tensor, compression algorithm,
quantization, pruning, distillation, tensor serialization, scheduler, runtime.

#### F.3 Optimization configuration

Toggleable optimizations (checkbox list):

Quantization · Mixed Precision · Tensor Compression · Feature Compression ·
Knowledge Distillation · Early Exit · Dynamic Layer Skipping · TensorRT ·
ONNX Runtime · CUDA · OpenVINO · TVM · Operator Fusion · Batch Scheduling ·
Memory Optimization

Toggling an optimization automatically indicates:

- Which files need modification
- Which algorithm should run
- Required input/output
- Expected latency improvement
- Expected bandwidth reduction

#### F.4 Source code mapping

Every execution block links to the project source tree, e.g.:

```
src/
 ├── mobile/          backbone.py, preprocess.py
 ├── server/          head.py, postprocess.py
 ├── communication/   socket.py, serializer.py
 ├── optimizer/       quantization.py, compression.py
 └── scheduler/       pipeline.py
```

> For **this** repository the real tree is: `client.py`, `server.py`,
> `tracker.py`, and `src/` (`Server.py`, `Scheduler.py`, `RpcClient.py`,
> `Model.py`, `Compress.py`, `Clustering.py`, `Profiler.py`, `Utils.py`) —
> seed the default project with these instead of the generic example.

Instead of showing source code, each folder gets a small **Input → Processing →
Output** flowchart; clicking a folder expands its internal workflow.

#### F.5 Pipeline flowchart

Auto-generate a complete flowchart of the inference pipeline:

`Input Image → Preprocessing → Backbone → Split Point → Compress Feature →
Network Transmission → Receive Feature → Decode → Head Network → Postprocess →
Prediction`

The flowchart **updates automatically** whenever the pipeline changes (device
added, cut moved, optimization toggled).

#### F.6 Communication analysis

For every connection display: bandwidth, latency, tensor size, serialization
method, compression ratio, transfer time, protocol. When the user toggles an
optimization, the affected connection values **recalculate immediately** so the
effect is visible at once.

#### F.7 Project planner (Markdown generator)

A "Generate Plan" action produces a Markdown implementation document with the
sections: **Project Overview, Folder Structure, Components, Device
Configuration, Communication, Optimization Modules, Flowcharts, Development
Tasks, TODO List.** Each TODO item must be detailed enough for Claude Code to
implement independently, e.g.:

- [ ] Create Device Graph component
- [ ] Build Split Layer Editor
- [ ] Implement Flowchart Renderer
- [ ] Implement Pipeline State Manager
- [ ] Implement Optimization Manager
- [ ] Build Communication Simulator
- [ ] Visualize Tensor Flow
- [ ] Generate Markdown Documentation
- [ ] Export JSON Configuration
- [ ] Save/Load Project

#### F.8 UI design

Clean modern node-editor interface in the spirit of Draw.io, Figma, Node-RED,
Langflow, ComfyUI. Must support:

- Drag & drop, zoom, **mini map**
- Dark mode (§2 tokens)
- Right-click context menu
- Connection lines with **animated data flow** (per §2 motion guide — chips on
  paths, no ambient loops while idle)
- Property panel, search, **undo/redo**

#### F.9 Export features

Export the project as **Markdown, JSON, PNG, SVG, PDF** (PDF via print
stylesheet is acceptable). The JSON must fully describe the pipeline —
devices, connections, cut points, optimizations, layout positions — so it can
be **reloaded later** (Save/Load round-trip).

#### F. Goal

An interactive visual editor for Split Inference systems where researchers can
easily design, modify, optimize, and understand distributed AI inference
pipelines. The generated Markdown (F.7) serves as a detailed implementation
roadmap that Claude Code can follow to build the complete system.

---

## 5. Hard constraints

- **Self-contained:** single HTML/SVG file, no external fonts, scripts, or images;
  embed assets as data URIs if needed.
- **Theme-aware:** correct in light *and* dark mode (§2 tokens for both).
- **Responsive:** works from ~360 px phone width to widescreen; charts resize.
- **Truthful metrics:** throughput is frames/time; never the mean of `1/Δt`.
- **Language:** all labels in English.
- Do not invent data — if a value isn't in the CSV/JSONL/summary formats above,
  leave the element out or mark it clearly as illustrative.

## 6. Acceptance checklist

- [ ] Edge is teal, cloud is violet, control plane is dashed amber — everywhere.
- [ ] Data plane vs control plane distinguishable without reading labels.
- [ ] Clusters visible as groups with their own queue and cut.
- [ ] The cut is depicted as dynamic (it moves), not a fixed property.
- [ ] Dark mode checked, colorblind-safe pairings checked.
- [ ] File opens standalone (double-click, no network).
- [ ] Animations follow §2 motion tokens and semantic mapping (same event =
      same motion, everywhere); timing driven by CSV data, not arbitrary delays.
- [ ] Play/pause/speed/restart controls present; `prefers-reduced-motion`
      honored (motion replaced by state changes + event log).
- [ ] No infinite/decorative loops; only `transform`/`opacity` animated.
- [ ] (F only) JSON export → reload reproduces the exact same pipeline,
      including layout positions.
- [ ] (F only) Flowchart and communication metrics update immediately on any
      edit or optimization toggle.
- [ ] (F only) Undo/redo covers every editing action, including deletes and
      optimization toggles.
