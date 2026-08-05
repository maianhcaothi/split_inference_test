# Device Utilization Guide (`utilization_queue`)

A self-contained spec for measuring **per-device utilization** in the distributed
inference pipeline over RabbitMQ. Read this and you can re-implement the exact same
mechanism in another project.

The idea in one line: **each device logs a nanosecond timestamp for every lifecycle
event (`start` / `get input` / `output` / `end`) to a local file; when it finishes it
reads the file back, computes ONE whole-run utilization ratio, and publishes it to the
server, which appends every device's report to a single log file.**

```
utilization = total busy time / total run time
            = Σ (output_i − get_input_i)  /  (end − start)
```

---

## 1. Concept & goals

- **One number per device per run** — not per batch/package. Per-package intervals are
  only the raw material; they are summed before dividing.
- **Self-contained per device.** Both the numerator and the denominator come from the
  *same device's own clock* (`time.time_ns()`), so clock skew between machines cannot
  distort the ratio. (It does mean raw timestamps are NOT comparable across devices —
  see §8.)
- **Computed after the run, from the log file.** The device never accumulates state
  during inference; it re-reads its own timing log at the end. The log doubles as a
  human-inspectable artifact.
- **Centrally persisted.** Every device sends its finished report to the server, which
  writes one line per device to `utilization.log`.

### Definitions

| Term | Meaning |
|---|---|
| **busy time** | Sum over every package of (`output` timestamp − `get input` timestamp). Everything between taking a batch in and emitting its result counts as busy — including inference, encode/send, and (in `only_cloud` mode) the back-pressure queue wait. |
| **total time** | `end` timestamp − `start` timestamp, i.e. the device's whole processing span. |
| **package** | One batch: from the moment the device has its input ready (`get input`) to the moment it has produced/sent its output (`output`). |

---

## 2. The timing log (per device)

### File names

Written to the device's working directory, one per role, namespaced by client id
([Scheduler.py:34-35](src/Scheduler.py#L34-L35)):

```
timing_edge_<first-12-chars-of-client-id>.log    # written by first_layer (edge)
timing_cloud_<first-12-chars-of-client-id>.log   # written by last_layer (cloud)
```

Old files are deleted in `Scheduler.__init__` and the `start` line opens the file in
`"w"` mode, so a new run never mixes with a previous one.

### Line format

One event per line: `<time.time_ns()> <event name>` (event names may contain spaces).

```
1781690039392508349 start
1781690039685926577 get input
1781690050057340167 output
1781690050688480238 get input
1781690060180005299 output
1781690060757187902 get input
1781690070219142706 output
1781690070715934131 get input
1781690080250369714 output
1781690311081125546 end
```

### When each event is written

| Event | Edge (`first_layer`) | Cloud (`last_layer`) |
|---|---|---|
| `start` | Just before the video frame-reading loop begins ([Scheduler.py:471-472](src/Scheduler.py#L471-L472)) | On entering the consume loop — possibly before any input exists ([Scheduler.py:755-756](src/Scheduler.py#L755-L756)) |
| `get input` | The moment a full batch of frames has been collected ([Scheduler.py:485-486](src/Scheduler.py#L485-L486)) | The moment a message is fetched off the intermediate/bbox queue ([Scheduler.py:774-775](src/Scheduler.py#L774-L775)) |
| `output` | After the batch is fully handled (inference and/or publish done) ([Scheduler.py:639-640](src/Scheduler.py#L639-L640)) | After inference + postprocess for that batch ([Scheduler.py:848-849](src/Scheduler.py#L848-L849)) |
| `end` | Right after the video loop exits (all frames consumed) ([Scheduler.py:705-706](src/Scheduler.py#L705-L706)) | Right after the loop breaks on receiving `STOP` ([Scheduler.py:924-925](src/Scheduler.py#L924-L925)) |

Extra events may appear (`queue_wait_start` / `queue_wait_end` in `only_cloud` mode);
the parser **ignores any event it doesn't recognize**, so the format is forward-extensible.

### Multithreaded pipeline (`multithreading.enable`)

In the pipelined paths (`_first_layer_mt` / `_last_layer_mt`) the recv/transfer stage and
the inference stage overlap, so per-batch intervals taken across the whole device would
overlap too and their sum would over-count (possibly > 100%). The markers therefore live
on the **inference thread** — the only stage that handles batches strictly one-at-a-time:

| Event | Edge (`_edge_infer_worker`) | Cloud (`_cloud_infer_worker`) |
|---|---|---|
| `get input` | Batch dequeued from the in-process hand-off queue | Batch dequeued from the in-process hand-off queue |
| `output` | Head output handed to the transfer thread (a blocking put on a full queue counts as busy — occupancy, like the sequential send) | Tail inference + postprocess done |

`start` / `end` are still written by the main thread, before the inference thread starts
and after it joins, so the log never has two concurrent writers. Utilization in mt mode
is thus the **inference-stage occupancy**: idle time is exactly the time the inference
thread spent waiting for an incoming batch; recv-side work that overlaps inference
(decode, capture, publish) is intentionally not double-counted.

---

## 3. Computing utilization — `Scheduler._compute_utilization(log_path, role)`

[Scheduler.py:370-417](src/Scheduler.py#L370-L417). Called once, after the device writes
its `end` line. Pure file-parsing — no inference-time state involved.

Algorithm:

1. Read the log line by line; split each line on the **first space only**
   (`line.strip().split(" ", 1)`) since event names contain spaces. Skip lines whose
   first field isn't all digits.
2. `start` → remember as `t_start`. `end` → remember as `t_end`.
3. `get input` → remember as the pending input time `t_input`.
4. `output` → if a pending `t_input` exists, add `ts − t_input` to `busy_ns`, count one
   package, clear `t_input`. (An unmatched `get input` with no following `output` — e.g.
   a crash mid-batch — is simply dropped.)
5. Any other event → ignored.
6. Validate: `t_start` and `t_end` must both exist and `t_end > t_start`, otherwise log
   a warning and return `None`.
7. Return the stats dict:

```python
{
    "role": role,               # "edge" | "cloud"
    "packages": n_packages,     # how many get-input→output pairs were summed
    "busy_ns": busy_ns,         # numerator (int, nanoseconds)
    "total_ns": total_ns,       # denominator = end − start (int, nanoseconds)
    "utilization": busy_ns / total_ns,   # the single whole-run ratio, 0.0–1.0
}
```

It also prints locally, e.g.:

```
[Utilization][edge] packages=4 busy=38.859s total=271.689s utilization=14.30%
```

(For the sample log in §2 that is exactly the expected result: busy = 38.859 s over
4 packages, total = 271.689 s → 14.30 %.)

---

## 4. Sending to the server — `Scheduler._send_utilization(stats)`

[Scheduler.py:419-442](src/Scheduler.py#L419-L442). Publishes the stats dict (plus
identity) as a pickled message to the dedicated **`utilization_queue`**:

```python
{
    "action": "UTILIZATION",
    "client_id": <uuid>,
    "layer_id": <int>,
    "role": ..., "packages": ..., "busy_ns": ..., "total_ns": ..., "utilization": ...,
}
```

- The queue is declared in `Scheduler.__init__` ([Scheduler.py:59-61](src/Scheduler.py#L59-L61))
  and re-declared on reconnect, and declared again just before publish (declares are idempotent).
- On a connection error it reconnects once and retries; on any other error it warns and
  gives up (utilization is telemetry — it must never crash the run).
- `stats is None` (bad/incomplete log) → silently skipped.

### Call sites (the "last step" on each device)

| Device | Where | Order |
|---|---|---|
| Edge | end of `first_layer` ([Scheduler.py:707](src/Scheduler.py#L707)) | after `end` is logged, **before** `NOTIFY` is sent to the server |
| Cloud | end of `last_layer` ([Scheduler.py:926](src/Scheduler.py#L926)) | after `end` is logged, i.e. after `STOP` was received |

### Why a dedicated queue and not `rpc_queue`?

Timing. The server stops consuming `rpc_queue` the moment the last **edge** sends
`NOTIFY` — but the **clouds** finish (and can only then compute their utilization)
*after* that, once they drain their backlog and receive `STOP`. A cloud's report sent to
`rpc_queue` would never be consumed. On the dedicated queue, reports simply sit on the
broker until the server's shutdown collection step (§5) picks them up — publisher and
consumer never need to be alive at the same moment.

---

## 5. Server-side collection — `Server._collect_utilization(timeout_s=30.0)`

[Server.py:322-360](src/Server.py#L322-L360).

Startup ([Server.py:87-93](src/Server.py#L87-L93), [141-142](src/Server.py#L141-L142)):
- Declare + **purge** `utilization_queue` (stale reports from a crashed run are discarded).
- Truncate `{log-path}/utilization.log` so runs never mix.

Shutdown (called from `Server.start()` after the FPS drain + summary, **before**
closing the connection):

1. `expected` = number of distinct registered client ids.
2. Poll `utilization_queue` with `basic_get` every 0.2 s.
3. For each `UTILIZATION` message: append one line to `utilization.log` and echo it to
   the console. Non-`UTILIZATION` / unpicklable bodies are skipped.
4. Stop when **all expected clients have reported** or after `timeout_s` (30 s). A
   partial collection prints a `Collected k/n reports before timeout` warning — the run
   still shuts down cleanly.

The 30 s window is comfortably enough: by the time collection starts, the FPS drain has
already waited for the work queues to empty plus a grace period, so every cloud has seen
`STOP` and published its report within a second or two.

### `utilization.log` format

One line per device, prefixed with the **server's** arrival timestamp (ns):

```
1781690311081125546 client=5f0be2c1-... role=edge packages=120 busy_s=38.859 total_s=271.689 utilization=14.30%
1781690312001125546 client=9a41d7e0-... role=cloud packages=120 busy_s=201.442 total_s=270.010 utilization=74.60%
```

---

## 6. End-to-end flow

```
 EDGE                                CLOUD                               SERVER
 ─────────────────────────────       ─────────────────────────────      ──────────────────────────────
 log "start"                         log "start"                        (startup: purge utilization_queue,
 loop:                               loop:                               truncate utilization.log)
   log "get input"                     log "get input"
   infer/send batch                    infer batch
   log "output"                        log "output"
 video done → log "end"              ...
 compute util from own log           (edges all NOTIFY → server
 publish UTILIZATION ──────────►┐     broadcasts STOP)
 send NOTIFY                    │    backlog empty → receive STOP
                                │    log "end"                          FPS drain finishes
                                │    compute util from own log          _collect_utilization():
                                └──► publish UTILIZATION ─────────────►   basic_get until all clients
                                        [ utilization_queue ]             reported (or 30 s timeout),
                                                                          append each line to
                                                                          utilization.log
```

---

## 7. Reading the results

- **Per-device console line** (on the device): `[Utilization][cloud] packages=... busy=...s total=...s utilization=...%`
- **Central file** (on the server): `{log-path}/utilization.log`, one line per device (§5).
- Interpretation:
  - **Edge** utilization ≈ fraction of its run it spent producing batches. In
    `only_cloud` mode the busy interval includes the back-pressure wait for the broker,
    so it measures *occupancy*, not pure compute.
  - **Cloud** utilization ≈ fraction of its run it spent processing batches; the rest is
    idle time waiting for input.

---

## 8. Caveats & design notes

1. **End times differ across devices — by design.** Each edge ends when *its own* video
   is exhausted. Every cloud ends later: only after **all** edges have notified, its own
   backlog is drained, and the `STOP` broadcast is seen (polled every 0.5 s). Multiple
   clouds therefore do **not** share an end time either — same `STOP` trigger, but each
   cloud drains a different backlog at a different speed, plus up to ~0.5 s poll jitter.
2. **Cloud utilization is slightly deflated.** Its denominator includes idle time before
   the first batch arrives (its `start` is written when it begins *waiting*, possibly
   before any edge started) and the post-backlog wait for `STOP` (~0.5–1 s tail).
   Negligible on long runs; measurable on short ones. If you want the denominator to be
   first-`get input` → last-`output` instead, change only `_compute_utilization`.
3. **Raw timestamps are not comparable across devices.** Each device stamps with its own
   local clock. The utilization *ratio* is immune to skew (numerator and denominator
   share a clock), but never subtract one device's timestamp from another's. For
   cross-device timing use the server-clock logs (`batch_done_ns.log`).
4. **Unknown events are ignored**, so adding new markers to the timing log (like the
   existing `queue_wait_start`/`queue_wait_end`) never breaks the parser.
5. **Telemetry must not kill the run**: every failure path (missing/corrupt log, broker
   down, unpicklable message) degrades to a warning, never an exception.

---

## 9. Code map

| What | Where |
|---|---|
| Timing-log writes (edge) | [Scheduler.py:471-472](src/Scheduler.py#L471-L472), [485-486](src/Scheduler.py#L485-L486), [639-640](src/Scheduler.py#L639-L640), [705-706](src/Scheduler.py#L705-L706) |
| Timing-log writes (cloud) | [Scheduler.py:755-756](src/Scheduler.py#L755-L756), [774-775](src/Scheduler.py#L774-L775), [848-849](src/Scheduler.py#L848-L849), [924-925](src/Scheduler.py#L924-L925) |
| Queue declare (client) | [Scheduler.py:59-61](src/Scheduler.py#L59-L61) (+ re-declare in `_reconnect`) |
| Compute (parse log → one ratio) | `Scheduler._compute_utilization` — [Scheduler.py:370-417](src/Scheduler.py#L370-L417) |
| Send to server | `Scheduler._send_utilization` — [Scheduler.py:419-442](src/Scheduler.py#L419-L442) |
| Call sites (last step per device) | [Scheduler.py:707](src/Scheduler.py#L707) (edge), [Scheduler.py:926](src/Scheduler.py#L926) (cloud) |
| Queue declare + purge (server) | [Server.py:87-93](src/Server.py#L87-L93) |
| Log file truncate (server) | [Server.py:141-142](src/Server.py#L141-L142) |
| Collect + write `utilization.log` | `Server._collect_utilization` — [Server.py:322-360](src/Server.py#L322-L360), called from `Server.start()` |
