# FPS Measurement Guide (`fps_queue`)

A self-contained spec for measuring **whole-system throughput (FPS)** in a distributed
inference pipeline over RabbitMQ. Read this and you can re-implement the exact same
mechanism in another project.

The idea in one line: **every time a tier finishes a batch it drops a `DONE`
message on a dedicated queue; a central server records the arrival times and reports
throughput as `frames / time`.** The message body carries only the producing cluster's id,
so the same arrivals yield both the system total and a per-cluster breakdown (§11.3).

---

## 1. Concept & goals

- **One authoritative number** for the whole run, no matter how many workers / clusters exist.
- **Server-side timing only.** FPS is computed from *arrival times on the server's clock*, so
  the wall clocks of the distributed devices never need to be synchronized.
- **Near-zero payload.** The body carries one short cluster id and no timing data whatsoever —
  the *arrival of the message* is the event.
- **Robust to bursty traffic.** Throughput = total frames / total time (not the mean of
  per-message rates), so bursts can't inflate the number.
- **Survives worker shutdown ordering.** The server keeps collecting after the producers that
  finish first (e.g. edges) stop, so late messages from slower tiers (e.g. clouds draining a
  backlog) are still counted.

### Roles

| Role | What it does for FPS |
|---|---|
| **Producer** (the tier that *completes* a batch) | Publishes exactly one `DONE` per finished batch. |
| **Server / consumer** | Consumes `DONE`s, timestamps each arrival, computes & prints FPS. |

> "Completes a batch" = produces the final output for that batch. In a split-inference setup
> that is the **cloud** (last stage) normally, or the **edge** when the edge runs the whole model.
> **Exactly one tier sends per batch — never both** (otherwise you double-count frames).

---

## 2. Data flow

```
 PRODUCER (worker that finishes a batch)          SERVER (consumer)
 ────────────────────────────────────────        ─────────────────────────────
 finish batch #k                                  on_fps():  t_ns = time.time_ns()
   └── publish b"<cluster>" ─► [ fps_queue ] ───►   append t_ns/1e9 to _fps_times[]
 finish batch #k+1                                  compute live smoothed FPS, and
   └── publish b"<cluster>" ─► [ fps_queue ] ───►   log "<t_ns> <fps>" to batch_done_ns.log
 ...
 (all producers finish)                           drain-watch → grace → _finish_fps()
                                                     prints SYSTEM FPS = N*bs / total_time
```

- `bs` = `batch_size` (frames per batch). One `DONE` == exactly `bs` frames.
- `N` = number of `DONE`s received = number of batches processed.

---

## 3. The queue

Create a single, non-durable queue named `fps_queue`. Both the producer and the server declare
it (declare is idempotent, so both doing it is safe). The **server purges it at startup** so a
new run doesn't inherit stale `DONE`s from a previous/crashed run.

```python
# Server, at startup:
channel.queue_declare(queue="fps_queue", durable=False)
channel.queue_purge(queue="fps_queue")   # start every run from empty

# Producer, before it starts sending:
channel.queue_declare(queue="fps_queue", durable=False)
```

Why non-durable: FPS pings are ephemeral; there's no value in persisting them across broker
restarts.

---

## 4. What is transferred

A **single short identity string** — the producing cluster's id:

```python
body = str(self.intermediate_queue).encode()   # e.g. b"intermediate_queue_0"
```

- No pickle, no JSON, **no timestamp**, no batch id.
- The body is an *identity*, never a measurement: all timing still comes from the server's
  arrival clock, so device clocks never need syncing, and a garbled body can at worst
  mis-bucket one batch — it can never distort a rate.
- The server uses it only to bucket the arrival for the **per-cluster** breakdown (§11.3).
  The system total counts every arrival regardless of body, so the aggregate number is
  unaffected by tagging.
- A producer that predates tagging sends `b"DONE"`; the server buckets those as `unknown`
  rather than dropping them, so an old worker degrades the breakdown instead of losing batches.

> If you only need the aggregate system-FPS number, a bare constant body (`b"DONE"`) is enough —
> arrivals from all workers into one queue naturally sum to system throughput. The cluster id
> costs a few bytes and is what makes the per-cluster split possible.

---

## 5. Who sends `DONE`, and when

**Rule:** whichever tier produces the final output for a batch sends exactly one `DONE`
immediately after that batch is done. Never two tiers for the same batch.

Example routing for a split-inference system with modes:

| Mode | Who completes the batch | Who sends `DONE` |
|---|---|---|
| `split` (edge head + cloud tail) | cloud (last stage) | **cloud** |
| `only_cloud` (edge forwards raw frames) | cloud | **cloud** |
| `only_edge` (edge runs whole model) | edge | **edge** |

Implementation pattern — gate the send on the mode so exactly one side fires:

```python
# Cloud side, after finishing/post-processing a batch:
if mode != "only_edge":      # cloud completes the batch in split / only_cloud
    send_fps_done(channel)

# Edge side, after finishing a batch:
if mode == "only_edge":      # edge completes the batch only in this mode
    send_fps_done(channel)
```

The publish helper:

```python
def send_fps_done(channel, cluster_id):
    """Publish exactly one DONE per finished batch, tagged with this cluster's id (§4).
    MUST run on the thread that owns the RabbitMQ channel (pika is not thread-safe)."""
    try:
        channel.basic_publish(exchange="", routing_key="fps_queue",
                              body=str(cluster_id).encode())
    except Exception as e:
        log(f"[FPS] send DONE failed: {e}")
```

### 5.1 Multiple workers / clusters

If several workers (or several clusters, each with its own cloud) all publish to the **same**
`fps_queue`, the server receives their `DONE`s interleaved. Because throughput is `frames/time`,
the merged stream automatically yields the **aggregate system throughput** across all workers —
no per-worker bookkeeping needed for the total.

Splitting that total per cluster needs nothing more than the id already in the body: the server
keeps a second, per-cluster list of the same arrival times (§11.3). The system list stays the
authoritative total, so the breakdown is strictly additive and can never move the aggregate.

---

## 6. Thread-safety gotcha (important)

**pika channels are NOT thread-safe.** Only publish `DONE` from the thread that owns the channel.

In a pipelined worker where a *separate compute thread* finishes batches but a *different thread*
owns the channel (e.g. the receive/transfer thread), do **not** publish from the compute thread.
Instead hand the event across with a thread-safe `queue.Queue`, and let the channel-owning thread
drain it and publish:

```python
import queue as _queue

# setup (channel-owning thread):
fps_q = _queue.Queue()          # in-process hand-off, holds one marker per finished batch

# compute thread, after each finished batch:
fps_q.put(1)                    # just a marker; the value is irrelevant

# channel-owning thread, each loop iteration:
def drain_fps_events(channel, fps_q):
    while True:
        try:
            fps_q.get_nowait()
        except _queue.Empty:
            break
        send_fps_done(channel)  # safe: we're on the channel's thread

# also drain once more after the compute thread joins, to flush the last in-flight batches.
```

If the worker is single-threaded (compute and channel on the same thread), skip all this and just
call `send_fps_done(channel)` directly after each batch.

---

## 7. Server: consuming and recording

Register a consumer on `fps_queue` (in addition to whatever control queue you already consume).
The callback just timestamps the arrival and (optionally) prints a smoothed live FPS.

```python
# during server init, on the same channel used by start_consuming():
channel.basic_consume(queue="fps_queue", on_message_callback=self.on_fps)

# state:
self._fps_times  = []     # arrival time of every DONE
self._fps_start_t = None  # when the run started (set when work is dispatched) — see §9
self._fps_window = 16     # DONEs per live smoothed sample
self.batch_log_path = f"{log_path}/batch_done_ns.log"
open(self.batch_log_path, "w").close()   # truncate: a new run never mixes with the previous one

def on_fps(self, ch, method, _props, body):
    # The arrival is the event; the body is read ONLY to bucket this batch under its
    # cluster (§11.3) and never for timing. Nothing below depends on it.
    t_ns = time.time_ns()          # ONE clock reading, used for both the FPS math and the log
    self._fps_times.append(t_ns / 1e9)
    n = len(self._fps_times)
    W = self._fps_window
    window_fps = None
    if n >= W:                                   # live smoothed view
        span = self._fps_times[-1] - self._fps_times[-W]
        if span > 0:
            window_fps = (W - 1) * self.batch_size / span
            log(f"[FPS] DONE #{n}  window_fps={window_fps:6.2f} (last {W} batches)")
    with open(self.batch_log_path, "a") as f:    # per-batch ns log — see §11.1
        if window_fps is None:
            f.write(f"{t_ns}\n")
        else:
            f.write(f"{t_ns} {window_fps:.2f}\n")
    ch.basic_ack(delivery_tag=method.delivery_tag)
```

Notes:
- Use **manual ack** (`basic_ack`) so a crash mid-callback doesn't silently drop the ping.
- The callback is tiny and non-blocking — safe to run on the same thread/ioloop as your control
  consumer. Heavy control work (if any) happens before the run, not while `DONE`s flow.
- Take **one** `time.time_ns()` reading per arrival and derive the float seconds from it
  (`t_ns / 1e9`), so the timestamp written to the log is exactly the arrival the FPS math uses.

---

## 8. Server: keep collecting past first-tier shutdown

The subtle bug this avoids: if the server stops the moment the **first** tier finishes (e.g. all
edges report done), the **slower** tier (e.g. clouds still draining their backlog) keeps emitting
`DONE`s that never get counted — undercounting FPS and losing the summary.

**Solution:** when the first tier finishes, broadcast STOP but **do not exit**. Keep consuming
`fps_queue` while the pipeline's *work queues* still hold batches, plus a short **grace** for the
last in-flight batch. A **hard cap** prevents an infinite hang if a worker dies mid-drain.

```python
# state:
self._fps_grace_s   = 10.0    # keep collecting this long after work queues drain
self._fps_hardcap_s = 300.0   # absolute safety cap after first-tier shutdown
self._fps_stop_bcast_t = None # time the first tier finished
self._fps_empty_since  = None # time work queues were first seen empty
self._fps_work_queues  = set()# names of the pipeline queues to watch
self._fps_printed = False

# when the first tier reports all-done:
def on_first_tier_done(self):
    broadcast_stop_to_all_workers()
    self._fps_stop_bcast_t = time.time()
    self._fps_work_queues  = discover_work_queue_names()   # e.g. {"intermediate_queue_0", ...}
    self.connection.call_later(1.0, self._fps_drain_check) # start the tail-watcher
    # NOTE: do NOT call stop_consuming() here.

def _fps_drain_check(self):
    if self._fps_printed:
        return
    now = time.time()
    # hard cap: never hang forever
    if self._fps_stop_bcast_t and (now - self._fps_stop_bcast_t) >= self._fps_hardcap_s:
        return self._finish_fps("hard cap reached")

    depth = self._total_work_queue_depth()   # sum of messages across work queues, or None
    if depth is None:
        # can't query queues → fall back to "no DONE for grace seconds"
        last = self._fps_times[-1] if self._fps_times else self._fps_stop_bcast_t
        if last and (now - last) >= self._fps_grace_s:
            return self._finish_fps("grace elapsed (no queue stats)")
    elif depth > 0:
        self._fps_empty_since = None          # still draining → reset grace
    else:
        if self._fps_empty_since is None:
            self._fps_empty_since = now
        elif (now - self._fps_empty_since) >= self._fps_grace_s:
            return self._finish_fps("work queues drained + grace")

    self.connection.call_later(1.0, self._fps_drain_check)  # re-arm
```

- `discover_work_queue_names()` = the set of queues batches flow through (in a single-queue
  pipeline that's just `{"intermediate_queue"}`; with clustering it's one per cluster).
- `_total_work_queue_depth()` = sum of `messages` for those queues via the broker's management
  HTTP API (`GET /api/queues/<vhost>/<queue>`), or `None` if unreachable → grace fallback kicks in.
- `call_later` schedules the check on pika's ioloop (works while `start_consuming()` runs).

> Why grace even after depth hits 0: when a work queue empties, the last batch may still be
> *in flight* on a worker (already pulled off the queue, not yet finished). The grace window lets
> its final `DONE` arrive and be counted.

---

## 9. When does the clock start?

`SYSTEM FPS` measures from **when the run started** to the **last `DONE`**. Record the start time
on the server at the moment you dispatch work to the workers (e.g. right after broadcasting the
START message):

```python
# after sending START to all workers:
self._fps_start_t = time.time()
```

Including the dispatch → first-result gap means "warm-up" (model load already happened, but the
first batch's pipeline fill) is part of the whole-run number. `steady_state` (below) excludes it.

---

## 10. How FPS is calculated

Let:
- `N`  = number of `DONE`s (`len(_fps_times)`)
- `bs` = `batch_size`
- `t[]`= sorted arrival times (`_fps_times`)
- `START` = `_fps_start_t`

| Metric | Formula | Meaning | When |
|---|---|---|---|
| **SYSTEM FPS** (primary) | `N * bs / (t[-1] - START)` | true whole-run throughput, warm-up included | final summary |
| **steady-state FPS** | `(N-1) * bs / (t[-1] - t[0])` | throughput excluding warm-up; best for comparing configs | final summary |
| **window_fps** | `(W-1) * bs / (t[-1] - t[-W])`, `W=16` | smoothed live view during the run | live log |
| ~~mean of 1/Δt~~ | `mean( bs / (t[i]-t[i-1]) )` | **reference only — biased high**, do not use | final summary (labeled) |

Why `(N-1)` for steady-state and window_fps: the first `DONE` of the span only *starts* the
clock (its batch finished before the measured interval began), so the interval covers `N-1`
batches.

**Why not the mean of per-gap rates:** `DONE`s arrive in bursts — several 0.1 s gaps (→ ~300 fps
entries) then one 5 s gap (→ a single ~6 fps entry). Averaging those entries over-weights the
bursts and reads far too high (e.g. 102 fps when the real rate is 26.6). Dividing **total frames
by total time** weights every second equally — the correct definition of throughput. Keep the old
mean only as a clearly-labeled reference.

---

## 11. What the server displays

**Live (during the run)** — one smoothed line every batch once ≥16 `DONE`s have arrived:

```
[FPS] DONE #128  window_fps= 41.02 (last 16 batches)
```

**Final summary (once, at shutdown):**

```
============================================================
  [SYSTEM FPS]        40.887 fps   = 137 DONE x 32 / 107.22s  (START -> last DONE)
  [steady-state]      41.203 fps   = 136 x 32 / 105.60s  (first -> last DONE)
  [ref mean, N/U]    102.450 fps   (arithmetic mean of 1/dt — reference only, biased high)
  batches counted: 137   stop reason: work queues drained + grace
============================================================
```

Reference implementation of the summary — **copied verbatim from this repo's `_finish_fps`;
the whitespace inside the f-strings IS the output format, so don't reflow it** (the labels are
padded so every fps number starts at the same column):

```python
def _finish_fps(self, reason=""):
    if self._fps_printed:
        return
    self._fps_printed = True
    t, n, bs = self._fps_times, len(self._fps_times), self.batch_size
    print("=" * 60)
    if n >= 1 and self._fps_start_t is not None and t[-1] > self._fps_start_t:
        total_time = t[-1] - self._fps_start_t
        system_fps = n * bs / total_time
        print(f"  [SYSTEM FPS]      {system_fps:8.3f} fps   "
              f"= {n} DONE x {bs} / {total_time:.2f}s  (START -> last DONE)")
        if n >= 2 and t[-1] > t[0]:
            span = t[-1] - t[0]
            steady = (n - 1) * bs / span
            print(f"  [steady-state]    {steady:8.3f} fps   "
                  f"= {n - 1} x {bs} / {span:.2f}s  (first -> last DONE)")
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
        self.channel.stop_consuming()   # now it's safe to end the run
    except Exception:
        pass
```

`stop_consuming()` here (not at first-tier shutdown) is what actually ends the server loop.

### 11.1 Log files (ns-epoch)

Besides the console output, the server writes two plain-text logs into `log-path`. Both use
**nanosecond epoch timestamps on the server's clock** (`time.time_ns()`, e.g.
`1782962149610671139`), so lines from the two files can be correlated on one timeline — e.g. to
see how throughput responds after a split-point change.

**`batch_done_ns.log`** — one line per finished batch, appended by `on_fps` (see §7). Format:
`<ns-epoch arrival> [<window_fps>]`. The fps column is the bare smoothed value from the live
`window_fps` line (two decimals) and is absent for the first `W-1` batches, before the first
full window exists:

```
1782962149610671139
1782962149742118400          <- batches 1..15: timestamp only
...
1782962151873560012 36.02
1782962152005011387 35.87
```

Truncated unconditionally at server start, so a new run never mixes with the previous one.

**`cut_change_ns.log`** — one line per **adaptive split-point change**, appended by the adaptive
controller (`_broadcast_setcut`) at the moment the change is committed, right before the
`SET_CUT` fan-out to the edges. Format:
`<ns-epoch> <queue>: cut <old>-><new> <deeper|shallower>`:

```
1782962151873560012 intermediate_queue_1: cut 4->5 deeper
1782962160221004518 intermediate_queue_1: cut 5->4 shallower
```

Only written when the adaptive controller is running; truncated at server start **only if**
`adaptive.enable` is true, so a non-adaptive run keeps the previous adaptive run's log intact.
The appends happen on the controller's background thread, but each write opens and closes the
file independently, so no file handle is shared across threads.

Reference implementation:

```python
# server init (log_path = same directory as batch_done_ns.log):
self.cut_log_path = f"{log_path}/cut_change_ns.log"
if adaptive_enabled:                       # e.g. config["adaptive"]["enable"]
    open(self.cut_log_path, "w").close()   # truncate only when the controller will run

# controller, at the moment a cut change is committed — take the timestamp
# BEFORE notifying the workers, so it marks when the decision was made:
t_ns = time.time_ns()
...  # broadcast SET_CUT (or equivalent) to the workers
word = "deeper" if new_cut > old_cut else "shallower"
with open(self.cut_log_path, "a") as f:
    f.write(f"{t_ns} {queue}: cut {old_cut}->{new_cut} {word}\n")
```

### 11.2 Output format contract

Every output surface, with its exact Python format string. A port reproduces this repo's result
format **iff** each surface below matches — the §7, §11 and §11.1 code blocks already emit
exactly these, so use them as-is and diff your outputs against this table to verify:

| Surface | Exact format |
|---|---|
| Live console, per batch (from batch `W`=16 on) | `f"[FPS] DONE #{n}  window_fps={window_fps:6.2f} (last {W} batches)"` |
| `batch_done_ns.log`, batches 1..`W-1` | `f"{t_ns}\n"` |
| `batch_done_ns.log`, batch `W` on | `f"{t_ns} {window_fps:.2f}\n"` |
| `cut_change_ns.log`, per cut change | `f"{t_ns} {queue}: cut {old_cut}->{new_cut} {word}\n"`, `word` ∈ `deeper`, `shallower` |
| Summary frame (first & last line) | `"=" * 60` |
| Summary: system fps | `f"  [SYSTEM FPS]      {system_fps:8.3f} fps   = {n} DONE x {bs} / {total_time:.2f}s  (START -> last DONE)"` |
| Summary: steady-state (`n >= 2`) | `f"  [steady-state]    {steady:8.3f} fps   = {n - 1} x {bs} / {span:.2f}s  (first -> last DONE)"` |
| Summary: reference mean (`n >= 2`) | `f"  [ref mean, N/U]   {ref_mean:8.3f} fps   (arithmetic mean of 1/dt — reference only, biased high)"` |
| Summary: no DONEs at all | `"  [SYSTEM FPS]      no DONEs received — nothing to report"` |
| Summary: last content line | `f"  batches counted: {n}   stop reason: {reason}"` |

Notes that make the numbers themselves comparable, not just the strings:
- `t_ns` is always `time.time_ns()` on the **server's** clock — 19 digits until the year 2262,
  no leading zeros, never scientific notation.
- The label padding differs per line (`[SYSTEM FPS]` + 6 spaces, `[steady-state]` + 4,
  `[ref mean, N/U]` + 3) so all three `{:8.3f}` numbers start at the same column.
- `window_fps` uses `{:6.2f}` on the console (space-padded, e.g. `window_fps= 35.87`) but bare
  `{:.2f}` in the log file (`... 35.87`) — same value, different padding, by design.
- In this repo the live line goes through a color-print helper (ANSI cyan); the text between the
  escape codes is exactly the format above. Plain `print` gives the same text.
### 11.3 Per-cluster breakdown files

The system-wide surfaces above are **unchanged** — `batch_done_ns.log` keeps its exact
two-column format and `[SYSTEM FPS]` still counts every arrival. These files are additive,
written from the same data, so the total and the breakdown can never disagree.

**`fps_cluster_ns.log`** — one line per `DONE`, tagged with the cluster that produced it and
that cluster's *own* smoothed window fps (absent until that cluster has `W` arrivals of its
own, so a cluster reaches its first full window later than the system does):

```
1785060014523418000 cluster=intermediate_queue_0 done=1
1785060014645912200 cluster=intermediate_queue_0 done=16 window_fps=41.02
```

**`fps_cluster.log`** — written by `_report_cluster_fps` at shutdown, one line per cluster
plus a `SYSTEM` line:

```
<ns> cluster=intermediate_queue_0 fps=27.412 steady_fps=28.004 done=40 frames=1280 share=51.9%
<ns> SYSTEM fps=52.883 done=77 frames=2464 clusters=3
```

- `fps` uses the **shared** START, so per-cluster values are additive — they sum to roughly the
  system fps (exactly, when the clusters finish together).
- `steady_fps` spans that cluster's own first→last `DONE`, dropping its warm-up. This is the
  fair number for comparing clusters, because a cluster that started late is not penalised.
- `share` is the cluster's portion of all batches — the fastest read on whether the assignment
  actually balanced the clusters.

**`utilization_cluster.log`** / **`latency_cluster.log`** — rolled up at shutdown from the same
per-device reports that feed `utilization.log` (which keeps its per-device view). Devices ship
**raw per-batch latency samples**, not pre-reduced stats, because percentiles cannot validly be
averaged across devices; the server pools the samples and takes nearest-rank percentiles, so
every value printed is a latency that was really observed.

```
<ns> cluster=intermediate_queue_0 ALL devices=3 utilization=56.67% utilization_mean=56.67% busy_s=170.000 total_s=300.000 packages=12
<ns> cluster=intermediate_queue_0 role=edge devices=2 utilization=45.00% busy_s=90.000 total_s=200.000 packages=8
<ns> SYSTEM devices=5 clusters=2 utilization=56.00% utilization_mean=56.00% busy_s=280.000 total_s=500.000

<ns> cluster=intermediate_queue_0 role=edge kind=service n=8 mean_ms=165.000 p50_ms=200.000 p95_ms=230.000 max_ms=230.000
<ns> cluster=intermediate_queue_0 role=edge kind=pipeline n=8 mean_ms=310.000 p50_ms=340.000 p95_ms=420.000 max_ms=420.000
<ns> cluster=intermediate_queue_0 kind=e2e n=4 mean_ms=532.500 p50_ms=420.000 p95_ms=900.000 max_ms=900.000
<ns> SYSTEM kind=e2e n=6 mean_ms=523.333 p50_ms=420.000 p95_ms=900.000 max_ms=900.000
```

- `utilization` is **pooled** (`sum busy / sum total`), which weights each device by how long it
  actually ran; `utilization_mean` (plain mean of per-device ratios) is printed beside it because
  a pooled figure can hide one idle device among busy ones.
- `kind=service` is a device's own `get_input -> output` on **one clock**, so it is exact. Its
  samples sum to that role's `busy_s`, which makes it the one latency you can line up against
  utilization.
- `kind=pipeline` is batch ready → published, same clock. On the edge it also contains the wait in
  the two hand-off queues, so it scales with `multithreading.queue_size` rather than with device
  speed — it explains `e2e`, it does not measure how fast the device is.
- `kind=e2e` is edge batch start → completing tier's output. It spans two machines and therefore
  inherits any clock offset between them — treat it as indicative, not exact. Only the completing
  tier reports it, so there is one `e2e` series per cluster.


---

## 12. Configuration knobs

```yaml
fps:                      # optional; defaults shown
  grace_s: 10             # keep collecting DONEs this long after the work queues drain
  shutdown_timeout_s: 300 # hard cap so a dead worker can't hang the server
```

- `grace_s` — trade-off: too small may cut off a slow last batch; too large delays shutdown.
- `shutdown_timeout_s` — only fires on failure (a worker dies mid-drain); on a healthy run the
  grace path finishes first.

---

## 13. End-to-end checklist (to port into a new project)

1. **Queue**: server `queue_declare` + `queue_purge` `fps_queue` at startup; producers `queue_declare` it.
2. **Producer**: after each finished batch, publish the cluster id (§4) — but only from the tier
   that completes the batch, and only from the channel-owning thread (use a `queue.Queue` hand-off
   if compute runs on another thread).
3. **Server consumer**: `basic_consume(fps_queue, on_fps)`; `on_fps` takes one `time.time_ns()`
   reading, appends `t_ns / 1e9` to the arrival list, and acks.
4. **Start clock**: set `_fps_start_t = time.time()` when work is dispatched.
5. **Live log** (optional): smoothed `window_fps` over the last 16 arrivals.
6. **Log files** (optional, §11.1): per DONE append `<ns-epoch> [<window_fps>]` to
   `batch_done_ns.log` (truncate at server start); if a runtime controller moves the split
   point, append `<ns-epoch> <queue>: cut <old>-><new> <deeper|shallower>` to
   `cut_change_ns.log` on every change.
7. **Shutdown**: on first-tier done → broadcast stop, start `_fps_drain_check` (queue-depth +
   grace + hard cap); **don't** stop consuming yet.
8. **Finalize**: `_finish_fps` prints SYSTEM FPS (= `N*bs/(last-START)`), steady-state, reference
   mean, then `stop_consuming()`.

---

## 14. Common pitfalls

| Pitfall | Symptom | Fix |
|---|---|---|
| Two tiers send `DONE` for the same batch | FPS ≈ 2× real | Gate the send so exactly one tier fires per mode. |
| Publish from a non-channel thread | Random pika errors / corruption | Hand off via `queue.Queue`; publish on the channel thread. |
| Server exits when first tier finishes | Undercount, missing late batches, no summary | Keep consuming; use drain-watch + grace. |
| Averaging per-gap `1/Δt` | FPS reads far too high | Use total frames / total time. |
| Not purging `fps_queue` at startup | Stale `DONE`s inflate count | `queue_purge` on server init. |
| Using timestamps inside the message | Needs synced device clocks | Use the server's arrival time only. |
| Forgetting the final drain after compute-thread join | Last few batches uncounted | Drain the hand-off queue once more after `join()`. |

---

## 15. Reference implementation in this repo

| Piece | Location |
|---|---|
| Queue name, hand-off init | `src/Scheduler.py` — `Scheduler.__init__` (`self.fps_queue`, `self._fps_q`) |
| Publish `DONE` | `src/Scheduler.py` — `_send_fps_done`, `_drain_fps_events` |
| Cloud send (sequential) | `src/Scheduler.py` — `last_layer` (gated `mode != "only_edge"`) |
| Edge send (only_edge) | `src/Scheduler.py` — `first_layer` (gated `mode == "only_edge"`) |
| Cloud send (multithreaded) | `src/Scheduler.py` — `_cloud_infer_worker` (marker) + `_cloud_recv_worker` / `_last_layer_mt` (drain) |
| Queue setup + consumer | `src/Server.py` — `Server.__init__` (declare/purge, `basic_consume`) |
| Record arrivals + live FPS | `src/Server.py` — `on_fps` |
| Per-batch ns log (`batch_done_ns.log`) | `src/Server.py` — `Server.__init__` (`batch_log_path`, truncate), `on_fps` (append) |
| Cut-change ns log (`cut_change_ns.log`) | `src/Server.py` — `Server.__init__` (`cut_log_path`, truncate), `_broadcast_setcut` (append) |
| Start clock | `src/Server.py` — `notify_clients` (`self._fps_start_t`) |
| Drain-watch shutdown | `src/Server.py` — NOTIFY branch, `_fps_total_work_depth`, `_fps_drain_check` |
| Final summary | `src/Server.py` — `_finish_fps` |
