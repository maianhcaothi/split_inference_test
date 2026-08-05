# Edge Layer (Head) — Design Script

> Companion to [../claude_design_prompt.md](../claude_design_prompt.md). Use its §2
> tokens and motion guide. This file is the complete script for depicting the
> **Edge** in any diagram, dashboard, or animation.

---

## 1. Identity card

| Property | Value |
|---|---|
| Role | Runs the **head** of the model: `layers[:cut]` on captured video frames |
| Hardware | Jetson Nano / low-power embedded device, started as `client.py --layer_id 1` |
| Color token | `--edge` — teal (`#0E7490` light / `#22D3EE` dark) |
| Shape | Rounded rectangle, teal border, **camera glyph** |
| Position in scene | Left side, one node per edge device, grouped inside its cluster box |

## 2. What the edge does (per batch)

1. **Capture** — read `batch_size` frames from the video, resize and stack them.
2. **Infer (head)** — run `layers[:cut]` to produce an intermediate feature map.
3. **Compress** — quantized delta codec (`compress.num_bit`, default 8-bit):
   first frame full, later frames only changed values + bitmask.
4. **Back-pressure check** — if `intermediate_queue_k` depth ≥
   `backpressure.max_queue`, **stall** (wait, don't publish).
5. **Publish** — send `{edge_start_time, cut, compressed feature map, ...}` to
   `intermediate_queue_k`. The current cut index rides **inside the message**.
6. **Poll control** — check `ctrl_<client_id>` for a `setcut` message from the
   server; if present, the *next* batch uses the new cut. No pause, no barrier.

When the video ends the edge reports done to the server and exits; in
`only_edge` mode it runs the *whole* model and publishes `DONE` to `fps_queue`
itself.

## 3. Threads (multithreading.enable: True)

Two threads, joined by bounded in-process queues (`multithreading.queue_size`,
default 4) — draw them as **two lanes inside the edge node**:

| Lane | Thread | Work | Owns |
|---|---|---|---|
| Top (I/O) | TRANSFER | capture frames → `in_q`; take head outputs from `out_q` → compress → publish; poll `ctrl_` | the RabbitMQ channel (all network) |
| Bottom (compute) | INFERENCE | `in_q` → run head model → `out_q` | the GPU, no network |

The point to show: while INFERENCE runs batch *N*, TRANSFER is already reading
batch *N+1* and publishing batch *N−1*. Compute and I/O overlap.

## 4. Queues & messages

| Direction | Queue | Payload | Plane |
|---|---|---|---|
| out | `intermediate_queue_k` | compressed feature map + `edge_start_time` + cut (≈ 1.7 MB at 8-bit, bs 32) | **data** (solid thick arrow) |
| out | `rpc_queue` | registration: layer profile + measured bandwidth | control (dashed) |
| in | `reply_<client_id>` | model assignment + START from server | control (dashed) |
| in | `ctrl_<client_id>` | `setcut` — new cut index | control (dashed) |
| out (`only_edge` only) | `fps_queue` | bare `DONE` per batch | control (dashed) |

## 5. Metrics this layer owns (CSV columns)

`edge_device`, `edge_latency_ms` (capture excluded — starts after frames are
read), `edge_fps`, `edge_ram_mb`, `edge_message_size_bytes`, and it stamps
`edge_start_time` into every message (the clock that e2e latency is measured
against).

## 6. Config knobs that shape edge behavior

`server.batch-size`, `compress.enable` / `num_bit`,
`multithreading.enable` / `queue_size`, `backpressure.enable` / `max_queue`.

## 7. State machine (drive node appearance from this)

```
IDLE → REGISTERING → WAITING_START → RUNNING ⇄ STALLED(back-pressure) → DRAINED/DONE
```

| State | Visual |
|---|---|
| REGISTERING | node at 60 % opacity, dashed chip traveling to server |
| RUNNING | full opacity, chips flowing out on the data path |
| STALLED | dimmed to 60 % + "paused" badge (per motion guide); queue pill it feeds is at/near `--alert` |
| DONE | node outlined only (hollow), no fill; stops emitting chips |

## 8. Animation beats (edge-specific)

| # | Event (from data) | Motion |
|---|---|---|
| E1 | batch captured | faint frame-stack icon pops into the TRANSFER lane (`--dur-state`) |
| E2 | head inference | INFERENCE lane shows a progress shimmer for `edge_latency_ms` (scaled) |
| E3 | compress + publish | message chip (`--data` fill, sized by `edge_message_size_bytes`) leaves the node along the solid path over `--dur-travel` |
| E4 | back-pressure hit | node dims + badge; **no chip leaves** until queue depth drops |
| E5 | `setcut` received | tiny dashed amber chip arrives; a `cut 4 → 5` label fades in on the node for ~1 s; next chip is visibly smaller/larger |
| E6 | video exhausted | node hollows out (DONE); its lane icons fade |

## 9. Accuracy rules — never show these wrong

- The edge **never** talks to the cloud directly; everything crosses the broker.
- A cut change affects the **next** batch only — never animate in-flight chips
  changing size mid-travel.
- Deeper cut ⇒ edge does **more** work and messages get **smaller** (usually);
  shallower ⇒ less edge work, bigger messages.
- Stall = waiting, not failure: never use error styling for back-pressure, only
  the dim + badge treatment.
- `edge_latency_ms` excludes video read time — don't animate capture as part of
  the latency bar.
