# Server Layer (Controller) — Design Script

> Companion to [../claude_design_prompt.md](../claude_design_prompt.md). Use its §2
> tokens and motion guide. This file is the complete script for depicting the
> **Server / Controller** in any diagram, dashboard, or animation.

---

## 1. Identity card

| Property | Value |
|---|---|
| Role | Central brain: registration, profiling, Hungarian partitioning, runtime cut control, system FPS measurement. **Touches no video data.** |
| Process | `python server.py` (`src/Server.py`) |
| Color token | `--server` — amber (`#B45309` light / `#FBBF24` dark) |
| Shape | Hexagon (or visually distinct rounded rect), amber border, **control-knobs glyph** |
| Position in scene | Top center, above the clusters; **all** its arrows are dashed (control plane only) |

## 2. What the server does — two phases

### Phase 1 — Setup (before START)

1. **Register** — consumes `rpc_queue`; each client sends its per-layer timing
   profile and measured uplink bandwidth.
2. **Partition (Hungarian)** — when all expected clients (`server.clients`) are
   in: cluster similar edges/clouds (agglomerative), estimate
   `edge_time + network_time + cloud_time` per candidate cut, pick the best
   cut per (edge-cluster, cloud-cluster) pair, solve the optimal matching with
   the Hungarian algorithm, sweeping K = 1…`max_clusters`.
3. **Assign + START** — replies on `reply_<client_id>` with each client's
   cluster, queue name, and initial cut; broadcasts START.

### Phase 2 — Runtime (after START)

4. **Adaptive controller** (background loop, per cluster): sample
   `intermediate_queue_k` depth via the RabbitMQ management API every
   `poll_interval_s`; every `batches_per_check` batches, vote over the window —
   ≥ `high_ratio` samples at depth ≥ `high_threshold` ⇒ **cut deeper**;
   ≥ `low_ratio` samples at depth ≤ `low_threshold` ⇒ **cut shallower**;
   respect `cooldown_batches` and the **message-size guard** (never move to a
   cut whose estimated message > `max_message_mb`; skip to the nearest safe
   cut, or refuse).
5. **Push cut updates** — `setcut` messages to each affected edge's
   `ctrl_<client_id>`.
6. **Count the run** — consume `fps_queue`; every `DONE` timestamps one batch on
   the **server's own clock** (clock skew irrelevant). Keeps counting after
   edges finish while work queues still hold batches, plus `fps.grace_s`;
   `fps.shutdown_timeout_s` caps a dead-peer hang.
7. **Final summary** — SYSTEM FPS = `N × batch_size / (START → last DONE)`;
   steady-state = `(N−1) × batch_size / (first → last DONE)`.

## 3. Queues & messages (all control plane — dashed amber)

| Direction | Queue | Payload |
|---|---|---|
| in | `rpc_queue` | registrations (profiles + bandwidth), edge-done reports |
| out | `reply_<client_id>` | assignment (cluster, queue, initial cut), START |
| out | `ctrl_<client_id>` | `setcut` — new cut index for that edge |
| in | `fps_queue` | bare `DONE` per finished batch, fleet-wide |
| read-only | management API (HTTP :15672) | queue depths — draw as a thin dotted "sensing" line to each queue pill, distinct from message arrows |

## 4. Data the server owns

The **run summary block** (SYSTEM FPS / steady-state / batches counted / stop
reason) and the cut-change log (`old_cut → new_cut` with high/low vote
fractions). In dashboards, every threshold line (`high_threshold`,
`low_threshold`, `max_message_mb`) belongs to the server's story.

## 5. Config knobs that shape server behavior

`server.clients` / `model` / `batch-size` / `cut-layer` (fixed-cut fallback),
the whole `clustering` block, the whole `adaptive` block, the `fps` block.

## 6. State machine

```
WAITING_CLIENTS → PARTITIONING → RUNNING(monitor + adapt) → DRAIN_WAIT → SUMMARY/DONE
```

| State | Visual |
|---|---|
| WAITING_CLIENTS | hexagon at 60 %; registration chips arriving; a `3/12 clients` counter |
| PARTITIONING | brief "matrix" motif inside the hexagon (cost matrix → matching); cluster boxes and queue pills materialize in the scene (`--dur-move`) |
| RUNNING | full opacity; sensing lines pulse softly on each poll; DONE counter ticking |
| DRAIN_WAIT | edges hollow, server still counting; show `grace` countdown |
| SUMMARY | hexagon docks a stat card: SYSTEM FPS, steady-state, batches, stop reason |

## 7. Animation beats (server-specific)

| # | Event (from data) | Motion |
|---|---|---|
| S1 | client registers | dashed chip arrives; client counter ticks (`--dur-state`) |
| S2 | Hungarian solve | one-shot sequence (~1.5 s): device dots cluster into groups → pairing lines lock in → each cluster's scissor line drops onto its layer strip. Never looped. |
| S3 | START broadcast | one pulse ring expands from the hexagon to all nodes (`--dur-move`); run clock starts |
| S4 | queue poll | sensing line to the queue pill brightens for `--dur-instant` — subtle background rhythm |
| S5 | vote triggers a move | the queue pill's window verdict flashes (`high 8/10`), then a `setcut` chip travels to the edge, then the cut strip slides (`--dur-move`) — strictly in that order: **sense → decide → command → effect** |
| S6 | guard refusal | intended cut position pulses `--alert` once, ⃠ marker remains, cut does **not** move; event log notes `blocked: 18.2 MB > 15 MB` |
| S7 | DONE arrives | counter ticks; rolling fps updates in the HUD |
| S8 | drain complete + grace | grace countdown ring completes; summary card slides in (`--dur-move`) |

## 8. Accuracy rules — never show these wrong

- **No video data ever touches the server** — never draw a solid data-plane
  arrow to or from it. Feature maps flow edge → broker → cloud only.
- The server **senses** queue depth by polling the management API — it is not a
  message recipient of the intermediate queues.
- Causality order in S5 is fixed: queue condition → vote → `setcut` → next-batch
  effect. Never animate the cut moving before the command chip arrives.
- Cut changes are **per cluster** — moving cluster 0's cut must not touch
  cluster 1's strip.
- SYSTEM FPS starts at the START broadcast (includes warm-up); steady-state
  starts at the first DONE. Label whichever one you show.
- One `DONE` per batch fleet-wide, arriving on one shared `fps_queue` — the
  server's count is already the aggregate; don't sum per-cluster counters on
  top of it.
