# 03 · Device utilization

How to produce `utilization.log` and `utilization_group.log`
([01 §3.4–3.5](01-result-format.md)).

**The mechanism in one line:** each device logs a nanosecond timestamp for every
lifecycle event to a local file; when it finishes it reads that file back, computes
**one** whole-run ratio, and publishes it to the server, which appends one line per
device.

```
utilization = busy time / total time
            = Σ (output_i − get_input_i)  /  (end − start)
```

---

## 1 · The four properties that make this correct

1. **One number per device per run** — not per unit. Per-unit intervals are raw
   material; they are summed *before* dividing.
2. **Self-contained per device.** Numerator and denominator both come from the *same
   device's own clock*, so clock skew between machines cannot distort the ratio. The
   cost: raw timestamps in these files are **not** comparable across devices (§7).
3. **Computed after the run, from the log file.** The device accumulates no state during
   processing; it re-reads its own log at the end. The log doubles as a human-inspectable
   artifact you can check by hand.
4. **Centrally persisted.** Each device ships its finished report to the server, which
   writes one line per device.

### Definitions

| Term | Meaning |
|---|---|
| **busy time** | Σ over every unit of (`output` ts − `get input` ts). Everything between taking work in and emitting its result counts — inference, encode, send, and any back-pressure wait. |
| **total time** | `end` ts − `start` ts: the device's whole processing span. |
| **package** | One unit: `get input` → `output`. |

---

## 2 · The per-device timing log

### File naming

One file per role, namespaced by device id, in the device's working directory:

```
timing_edge_<first-12-chars-of-client-id>.log
timing_cloud_<first-12-chars-of-client-id>.log
```

Delete old files at startup and open the `start` line in `"w"` mode, so a new run never
mixes with a previous one.

### Line format

`<time.time_ns()> <event name>` — one event per line. **Event names may contain spaces.**

```
1781690039392508349 start
1781690039685926577 get input
1781690050057340167 output
1781690050688480238 get input
1781690060180005299 output
1781690311081125546 end
```

### When each event fires

| Event | Head stage | Tail stage |
|---|---|---|
| `start` | just before the work-producing loop begins | on entering the consume loop — possibly before any input exists |
| `get input` | a full unit of work has been collected | a message is fetched off the work queue |
| `output` | the unit is fully handled (compute and/or publish done) | compute + post-processing for that unit done |
| `end` | right after the work loop exits | right after the loop breaks on `STOP` |

Extra events may appear. **The parser MUST ignore any event it does not recognize**, so
the format stays forward-extensible.

### 2.1 Pipelined workers — put the markers on one stage

If the receive/transfer stage and the compute stage overlap, per-unit intervals taken
across the whole device would overlap too, and their sum would over-count — possibly
above 100%.

Put `get input` / `output` on the **compute thread only** — the one stage that handles
units strictly one at a time:

| Event | Pipelined head | Pipelined tail |
|---|---|---|
| `get input` | unit dequeued from the in-process hand-off queue | unit dequeued from the hand-off queue |
| `output` | output handed to the transfer thread (a blocking put on a full queue counts as busy — that is occupancy) | compute + post-processing done |

`start` / `end` stay on the main thread, written before the compute thread starts and
after it joins, so the log never has two concurrent writers.

Utilization in this mode is **compute-stage occupancy**: idle time is exactly the time
the compute thread spent waiting for input. Recv-side work that overlaps compute is
deliberately not double-counted.

> A utilization above 100% always means overlapping intervals were summed. The
> [validator](01-result-format.md) treats it as a hard error.

---

## 3 · Computing the ratio

Called once, after the device writes its `end` line. Pure file parsing — no
processing-time state.

```python
def compute_utilization(log_path, role):
    t_start = t_end = t_input = None
    busy_ns, packages = 0, 0

    with open(log_path, "r") as f:
        for line in f:
            parts = line.strip().split(" ", 1)      # split ONCE: names contain spaces
            if len(parts) != 2 or not parts[0].isdigit():
                continue
            ts, event = int(parts[0]), parts[1]

            if event == "start":
                t_start = ts
            elif event == "end":
                t_end = ts
            elif event == "get input":
                t_input = ts
            elif event == "output":
                if t_input is not None:             # unmatched get-input (crash) is dropped
                    busy_ns += ts - t_input
                    packages += 1
                    t_input = None
            # any other event: ignored, by design

    if t_start is None or t_end is None or t_end <= t_start:
        log(f"[Utilization][{role}] incomplete timing log — skipping")
        return None

    total_ns = t_end - t_start
    return {
        "role": role,
        "packages": packages,
        "busy_ns": busy_ns,
        "total_ns": total_ns,
        "utilization": busy_ns / total_ns,
    }
```

Key details:
- `split(" ", 1)` — splitting on every space breaks `get input`.
- An unmatched `get input` with no following `output` (a crash mid-unit) is silently
  dropped rather than counted as infinite.
- Returns `None` on a bad log; the caller skips sending. Telemetry never raises.

Local console line:

```
[Utilization][edge] packages=4 busy=38.859s total=271.689s utilization=14.30%
```

(For the §2 sample log that is exactly right: 38.859 s busy over 4 packages, 271.689 s
total → 14.30%.)

---

## 4 · Shipping to the server

Publish the stats dict plus identity to a **dedicated queue**:

```text
{
    "action": "UTILIZATION",
    "client_id": <uuid>,
    "layer_id": <int>,
    "role": ..., "packages": ..., "busy_ns": ..., "total_ns": ..., "utilization": ...,
}
```

- Declare the queue at init and re-declare on reconnect and just before publish
  (declares are idempotent).
- On a connection error: reconnect once and retry. On any other error: warn and give up.
- `stats is None` → skip silently.

### Call it as the last step on each device

| Device | Where | Order |
|---|---|---|
| head | end of the work loop | after `end` is logged, **before** notifying the server it is done |
| tail | end of the consume loop | after `end` is logged, i.e. after `STOP` was received |

### Why a dedicated queue and not the control queue

Timing. The server stops consuming the control queue the moment the last **head** stage
reports done — but the **tail** stages finish later, only after draining their backlog
and seeing `STOP`. A tail's report sent to the control queue would never be consumed.

On a dedicated queue the reports simply sit on the broker until the server's shutdown
collection picks them up. **Publisher and consumer never need to be alive at the same
moment.** This is the whole reason the queue exists.

---

## 5 · Server-side collection

**Startup:** declare + **purge** the queue (a crashed run's stale reports are discarded),
and truncate `utilization.log`.

**Shutdown**, after the throughput drain and summary, before closing the connection:

1. `expected` = number of distinct registered device ids.
2. Poll the queue with `basic_get` every 0.2 s.
3. For each report: append one line to `utilization.log`, echo to console. Skip
   non-`UTILIZATION` or unpicklable bodies.
4. Stop when all expected devices have reported, or after `timeout_s` (default 30).

A partial collection prints `Collected k/n reports before timeout` and the run **still
shuts down cleanly**. 30 s is comfortable: by the time collection starts, the throughput
drain has already waited for the work queues to empty plus a grace period, so every tail
has seen `STOP` and published within a second or two.

---

## 6 · Rolling up per group

`utilization_group.log` ([01 §3.5](01-result-format.md)) aggregates the same per-device
reports. Two numbers, both required on `ALL`/`SYSTEM` lines:

```python
pooled = sum(d.busy_ns for d in devs) / sum(d.total_ns for d in devs)   # utilization
mean   = sum(d.busy_ns / d.total_ns for d in devs) / len(devs)          # utilization_mean
```

- **pooled** weights each device by how long it actually ran.
- **mean** is the plain average of the ratios.

Emit both, because a pooled figure can hide one idle device inside a busy group.
**When the two diverge, the group is imbalanced** — that divergence is the signal, and
it is the reason the second column exists.

---

## 7 · End-to-end flow

```
 HEAD                          TAIL                        SERVER
 ───────────────────────       ───────────────────────     ────────────────────────────
 log "start"                   log "start"                 startup: purge util queue,
 loop:                         loop:                                truncate utilization.log
   log "get input"               log "get input"
   process unit                  process unit
   log "output"                  log "output"
 work done → log "end"         ...
 compute util from own log     (heads all notify →
 publish UTILIZATION ─────┐     server broadcasts STOP)
 notify server            │    backlog empty → receive STOP
                          │    log "end"                    throughput drain finishes
                          │    compute util from own log    collect_utilization():
                          └──► publish UTILIZATION ──────►    basic_get until all devices
                                 [ utilization_queue ]         reported (or 30 s timeout),
                                                               append each line
```

---

## 8 · Caveats — know these before interpreting the numbers

1. **End times differ across devices, by design.** Each head ends when its own work is
   exhausted. Every tail ends later — only after all heads notify, its backlog drains,
   and it sees `STOP`. Multiple tails do not share an end time either: same trigger,
   different backlogs, plus poll jitter.
2. **Tail utilization is slightly deflated.** Its denominator includes idle time before
   the first unit arrives (its `start` is written when it begins *waiting*) and the
   post-backlog wait for `STOP`. Negligible on long runs, measurable on short ones. To
   change it, change only the denominator in `compute_utilization` — nothing else
   depends on it.
3. **Raw timestamps are not comparable across devices.** The *ratio* is immune to skew
   because numerator and denominator share a clock, but never subtract one device's
   timestamp from another's. For cross-device timing use the server-clock logs.
4. **Unknown events are ignored**, so new markers never break the parser.
5. **Telemetry must not kill the run.** Missing log, corrupt log, broker down,
   unpicklable message — every path degrades to a warning.

---

## 9 · Reading the numbers

| Observation | Means |
|---|---|
| head low, tail high | the work split is too shallow — the tail is the bottleneck |
| head high, tail low | too deep — the tail is starved |
| `utilization` ≫ `utilization_mean` | one idle device is hiding inside a busy group |
| `utilization` ≪ `utilization_mean` | one very long-running device is dragging the pool down |
| any value > 100% | **measurement bug** — overlapping intervals were summed (§2.1) |
| every device ≈ 100% | check whether back-pressure waits are being counted as busy |
