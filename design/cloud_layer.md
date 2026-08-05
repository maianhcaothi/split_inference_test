# Cloud Layer (Tail) — Design Script

> Companion to [../claude_design_prompt.md](../claude_design_prompt.md). Use its §2
> tokens and motion guide. This file is the complete script for depicting the
> **Cloud** in any diagram, dashboard, or animation.

---

## 1. Identity card

| Property | Value |
|---|---|
| Role | Runs the **tail** of the model: `layers[cut:]` + NMS post-processing |
| Hardware | Server / high-performance machine, started as `client.py --layer_id 2` |
| Color token | `--cloud` — violet (`#7C3AED` light / `#A78BFA` dark) |
| Shape | Rounded rectangle, violet border, **chip/server glyph** |
| Position in scene | Right side, one node per cloud device, grouped inside its cluster box |

## 2. What the cloud does (per batch)

1. **Receive** — pull **one message at a time** from `intermediate_queue_k`
   (never builds an in-process backlog).
2. **Decompress** — invert the quantized delta codec. The message carries the
   cut it was produced at, so decoding is always correct even mid-cut-change.
3. **Infer (tail)** — run `layers[cut:]` from that cut index.
4. **Post-process** — NMS → final detection boxes.
5. **Stream results** — append per-frame detections to
   `detections_stream.jsonl` (flat RAM); `detections.json` is rebuilt at the end.
6. **Signal completion** — publish one bare `DONE` to `fps_queue` per finished
   batch (in `split` and `only_cloud` modes). This is the system's heartbeat.

The cloud keeps consuming **after the edges finish** — draining the queue
backlog is part of its story. Optionally computes mAP@50 / mAP@50:95 if ground
truth labels exist.

## 3. Threads (multithreading.enable: True)

Two lanes inside the cloud node, mirror image of the edge:

| Lane | Thread | Work | Owns |
|---|---|---|---|
| Top (I/O) | TRANSFER | receive from `intermediate_queue_k` → decompress → `local_q` | the RabbitMQ channel |
| Bottom (compute) | INFERENCE | `local_q` → tail model → NMS → detections → `DONE` | the GPU |

Show: while INFERENCE runs batch *N*, TRANSFER is already receiving and
decompressing batch *N+1*.

## 4. Queues & messages

| Direction | Queue | Payload | Plane |
|---|---|---|---|
| in | `intermediate_queue_k` | compressed feature map + `edge_start_time` + cut | **data** (solid thick arrow) |
| out | `fps_queue` | bare `DONE` per batch | control (dashed, toward server) |
| out | `rpc_queue` | registration: profile + bandwidth | control (dashed) |
| in | `reply_<client_id>` | model assignment + START | control (dashed) |
| out (files) | `detections_stream.jsonl` → `detections.json` | frame → boxes | local artifact (small document icon, not an arrow) |

## 5. Metrics this layer owns (CSV columns)

`cloud_device`, `cloud_arrival_order`, `cloud_latency_ms` (receive + decompress
+ tail + post-process), `cloud_fps`, `cloud_ram_mb`,
`cloud_message_size_bytes` (equals the edge's value for the same batch), and it
computes `e2e_latency_ms = cloud_batch_end − edge_start_time` — the number that
exposes queue wait time.

## 6. Config knobs that shape cloud behavior

`server.batch-size`, `compress.num_bit` (decode side), `multithreading.enable`
/ `queue_size`, `detections.save_json`, ground-truth folder presence (mAP).

## 7. State machine

```
IDLE → REGISTERING → WAITING_START → RUNNING → DRAINING(edges done, queue > 0) → DONE
```

| State | Visual |
|---|---|
| RUNNING | full opacity; chips arriving, brief border glow on each consume (`--dur-state`) |
| STARVED (queue empty, waiting) | subtle: inner fill drops to 80 %; **no alert styling** — starvation is the *shallower-cut* signal, not an error |
| DRAINING | edges already hollow; cloud still glowing per batch — make this phase visible, it explains why the run isn't over |
| DONE | hollow outline, DONE count frozen |

## 8. Animation beats (cloud-specific)

| # | Event (from data) | Motion |
|---|---|---|
| C1 | message consumed | chip slides out of the queue pill into the node; border glows once (`--dur-state`) |
| C2 | decompress + tail inference | INFERENCE lane shimmer for `cloud_latency_ms` (scaled) |
| C3 | batch finished | tiny dashed chip (DONE) travels the amber path to the server; server's frame counter ticks up |
| C4 | detections written | small document icon pulse next to the node (`--dur-instant`) — subtle, don't compete with C1–C3 |
| C5 | queue starved | node at 80 % fill; queue pill visibly empty — the viewer should predict "cut will move shallower" |
| C6 | backlog drain | after edges hollow out, C1–C3 keep firing until the queue pill empties, then grace period, then DONE |

## 9. Accuracy rules — never show these wrong

- One `DONE` = exactly one batch = `batch_size` frames — never per frame.
- The cloud decodes with the cut **in the message**, so during a cut change two
  consecutive chips can have different cuts; this is normal, never an error.
- `cloud_message_size_bytes` = `edge_message_size_bytes` for the same batch —
  same bytes, two measurement points.
- Empty queue = cloud **starved** (wants a shallower cut); full queue = cloud
  **bottleneck** (wants a deeper cut). Don't flip these.
- Detections go to a local file, not back across the network — no return arrow
  to the edge.
