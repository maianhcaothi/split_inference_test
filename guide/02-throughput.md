# 02 · Throughput measurement

How to produce `batch_done_ns.log`, `group_rate_ns.log`, and `group_rate.log`
([01 §3.1–3.3](01-result-format.md)).

**The mechanism in one line:** every time a stage finishes a unit it drops a message on a
dedicated queue; the server records the *arrival time* and reports throughput as
`total items / total time`.

---

## 1 · Why it is built this way

| Goal | Consequence |
|---|---|
| One authoritative number regardless of worker count | all workers publish to **one** queue; arrivals merge naturally into the system total |
| No clock synchronization between machines | timing comes **only** from the server's arrival clock |
| A garbled message can never distort a rate | the body carries an **identity**, never a measurement |
| Bursty traffic cannot inflate the number | throughput = total items / total time, never the mean of per-gap rates |
| Late units from slow stages still count | the server keeps consuming after the first stage finishes (§5) |

---

## 2 · The queue

One non-durable queue. Both sides declare it (declares are idempotent); the **server
purges it at startup**.

```python
# Server, at startup:
channel.queue_declare(queue="fps_queue", durable=False)
channel.queue_purge(queue="fps_queue")     # a crashed run's stale messages must not count

# Producer, before it starts sending:
channel.queue_declare(queue="fps_queue", durable=False)
```

Non-durable is correct: these pings are ephemeral and worthless across a broker restart.

**Forgetting the purge is a real bug** — stale completions from a crashed run inflate the
next run's count, and the inflation is invisible in the charts because it looks like a
fast start.

---

## 3 · The message

A single short identity string — the producing group's id.

```python
body = str(self.intermediate_queue).encode()     # e.g. b"intermediate_queue_0"
```

- No pickle, no JSON, **no timestamp**, no unit id.
- The server reads it **only** to bucket the arrival for the per-group breakdown. The
  system total counts every arrival regardless of body.
- A producer that predates tagging sends a bare constant; bucket those as `unknown`
  rather than dropping them, so an old worker degrades the breakdown instead of losing
  units.

If you only need the system number, a constant body is enough. The group id costs a few
bytes and is what makes `group_rate_ns.log` and `group_rate.log` possible.

---

## 4 · Who publishes, and when

**Exactly one stage publishes per unit.** Two stages publishing the same unit doubles
your throughput number, and it is the single most common porting bug.

| Topology | Stage that completes the unit | Publisher |
|---|---|---|
| head + tail split | tail | **tail** |
| head forwards raw work | tail | **tail** |
| head runs everything | head | **head** |

Gate the send on the mode so exactly one side fires:

```python
# Tail, after finishing a unit:
if mode != "only_edge":
    send_done(channel, cluster_id)

# Head, after finishing a unit:
if mode == "only_edge":
    send_done(channel, cluster_id)
```

```python
def send_done(channel, cluster_id):
    """Publish exactly one completion per finished unit, tagged with the group id.
    MUST run on the thread that owns the channel — pika is not thread-safe."""
    try:
        channel.basic_publish(exchange="", routing_key="fps_queue",
                              body=str(cluster_id).encode())
    except Exception as e:
        log(f"[FPS] send DONE failed: {e}")     # telemetry never kills the run
```

### 4.1 Thread-safety — the second most common bug

**pika channels are not thread-safe.** In a pipelined worker where a separate compute
thread finishes units but a different thread owns the channel, do **not** publish from
the compute thread. Hand the event across with a `queue.Queue`:

```python
import queue as _queue

fps_q = _queue.Queue()          # in-process hand-off, one marker per finished unit

# compute thread, after each finished unit:
fps_q.put(1)                    # the value is irrelevant; the marker is the event

# channel-owning thread, each loop iteration:
def drain_done_events(channel, fps_q, cluster_id):
    while True:
        try:
            fps_q.get_nowait()
        except _queue.Empty:
            break
        send_done(channel, cluster_id)          # safe: we own the channel here
```

**Drain once more after the compute thread joins**, or the last few in-flight units are
never counted. Symptom: your unit count is consistently 2–5 short.

Single-threaded worker? Skip all of this and call `send_done` directly.

---

## 5 · Server side

### 5.1 Consuming

```python
channel.basic_consume(queue="fps_queue", on_message_callback=self.on_fps)

self._fps_times   = []      # arrival time of every completion, seconds
self._fps_start_t = None    # shared START (§5.2)
self._fps_window  = 16      # W
self.batch_log_path = f"{log_path}/batch_done_ns.log"
open(self.batch_log_path, "w").close()          # truncate: runs never mix

def on_fps(self, ch, method, _props, body):
    # The arrival is the event. The body is read ONLY to bucket this unit by group,
    # never for timing. One clock reading serves both the math and the log.
    t_ns = time.time_ns()
    self._fps_times.append(t_ns / 1e9)
    n, W = len(self._fps_times), self._fps_window
    window_fps = None
    if n >= W:
        span = self._fps_times[-1] - self._fps_times[-W]
        if span > 0:
            window_fps = (W - 1) * self.batch_size / span
            log(f"[FPS] DONE #{n}  window_fps={window_fps:6.2f} (last {W} batches)")
    with open(self.batch_log_path, "a") as f:
        f.write(f"{t_ns}\n" if window_fps is None else f"{t_ns} {window_fps:.2f}\n")
    ch.basic_ack(delivery_tag=method.delivery_tag)
```

- **Manual ack**, so a crash mid-callback does not silently drop the ping.
- Take **one** `time.time_ns()` reading and derive seconds from it, so the logged
  timestamp is exactly the arrival the math used.
- Keep the callback tiny and non-blocking — it shares a thread with the control consumer.

### 5.2 When the clock starts

Record START on the server the moment work is dispatched:

```python
# immediately after broadcasting START to all workers:
self._fps_start_t = time.time()
```

This makes pipeline fill-up part of the whole-run number. `steady_fps` excludes it. Both
are reported because they answer different questions: whole-run is what the user
experienced, steady-state is what you compare configurations with.

### 5.3 Keep collecting past first-stage shutdown

**The bug this avoids:** if the server stops when the *first* stage finishes, the slower
stage keeps emitting completions that are never counted — undercounting throughput and
losing the summary entirely.

When the first stage finishes, broadcast stop but **do not exit**. Keep consuming while
the work queues still hold units, plus a grace window for the last in-flight unit, with a
hard cap so a dead worker cannot hang the server.

```python
self._fps_grace_s    = 10.0     # keep collecting this long after work queues drain
self._fps_hardcap_s  = 300.0    # absolute cap after first-stage shutdown
self._fps_stop_bcast_t = None
self._fps_empty_since  = None
self._fps_printed = False

def on_first_stage_done(self):
    broadcast_stop_to_all_workers()
    self._fps_stop_bcast_t = time.time()
    self._fps_work_queues  = discover_work_queue_names()
    self.connection.call_later(1.0, self._fps_drain_check)
    # NOTE: do NOT stop_consuming() here.

def _fps_drain_check(self):
    if self._fps_printed:
        return
    now = time.time()
    if self._fps_stop_bcast_t and (now - self._fps_stop_bcast_t) >= self._fps_hardcap_s:
        return self._finish_fps("hard cap reached")

    depth = self._total_work_queue_depth()      # broker mgmt API, or None if unreachable
    if depth is None:                           # fall back to "no completion for grace_s"
        last = self._fps_times[-1] if self._fps_times else self._fps_stop_bcast_t
        if last and (now - last) >= self._fps_grace_s:
            return self._finish_fps("grace elapsed (no queue stats)")
    elif depth > 0:
        self._fps_empty_since = None            # still draining -> reset grace
    else:
        if self._fps_empty_since is None:
            self._fps_empty_since = now
        elif (now - self._fps_empty_since) >= self._fps_grace_s:
            return self._finish_fps("work queues drained + grace")

    self.connection.call_later(1.0, self._fps_drain_check)
```

**Why grace even after depth hits 0:** when a work queue empties, the last unit may still
be in flight on a worker — already pulled off the queue, not yet finished. The grace
window lets its completion arrive.

`call_later` schedules on pika's ioloop, so it runs while `start_consuming()` is blocked.

---

## 6 · The formulas

Let `N` = completions, `bs` = unit size, `t[]` = arrival times, `START` = shared start.

| Metric | Formula | Use it for |
|---|---|---|
| **SYSTEM rate** | `N * bs / (t[-1] - START)` | the headline number; warm-up included |
| **steady-state** | `(N-1) * bs / (t[-1] - t[0])` | comparing configurations |
| **window rate** | `(W-1) * bs / (t[-1] - t[-W])`, `W=16` | the live series, and every time-axis chart |
| ~~mean of `1/Δt`~~ | `mean(bs / (t[i]-t[i-1]))` | **nothing — biased high, do not use** |

**Why `(N-1)` for steady-state and window:** the first completion of a span only *starts*
the clock — its unit finished before the measured interval began — so the interval covers
`N-1` units.

**Why not the mean of per-gap rates.** Completions arrive in bursts: several 0.1 s gaps
(→ ~300 fps each) then one 5 s gap (→ a single ~6 fps entry). Averaging over-weights the
bursts. Measured on a real run: the mean-of-gaps read **102 fps** where the true rate was
**26.6 fps**. Total items ÷ total time weights every second equally. If you keep the
mean at all, label it as a biased reference.

---

## 7 · Output format contract

A port reproduces this format **iff** every surface below matches. The code blocks above
already emit exactly these — use them as-is and diff your output against this table.

| Surface | Exact format |
|---|---|
| Live console, from unit `W` on | `f"[FPS] DONE #{n}  window_fps={window_fps:6.2f} (last {W} batches)"` |
| `batch_done_ns.log`, units 1..`W-1` | `f"{t_ns}\n"` |
| `batch_done_ns.log`, unit `W` on | `f"{t_ns} {window_fps:.2f}\n"` |
| `events_ns.log`, per event | `f"{t_ns} {queue}: cut {old}->{new} {word}\n"` |
| Summary frame (first & last line) | `"=" * 60` |
| Summary: system rate | `f"  [SYSTEM FPS]      {system_fps:8.3f} fps   = {n} DONE x {bs} / {total_time:.2f}s  (START -> last DONE)"` |
| Summary: steady-state (`n≥2`) | `f"  [steady-state]    {steady:8.3f} fps   = {n - 1} x {bs} / {span:.2f}s  (first -> last DONE)"` |
| Summary: reference mean (`n≥2`) | `f"  [ref mean, N/U]   {ref_mean:8.3f} fps   (arithmetic mean of 1/dt — reference only, biased high)"` |
| Summary: no completions | `"  [SYSTEM FPS]      no DONEs received — nothing to report"` |
| Summary: last content line | `f"  batches counted: {n}   stop reason: {reason}"` |

The label padding differs per line (`[SYSTEM FPS]` + 6 spaces, `[steady-state]` + 4,
`[ref mean, N/U]` + 3) so all three `{:8.3f}` numbers start at the same column. **Do not
reflow these f-strings** — the whitespace is the format.

`window_fps` uses `{:6.2f}` on the console (space-padded) but bare `{:.2f}` in the log.
Same value, different padding, deliberately.

Rendered:

```
============================================================
  [SYSTEM FPS]        40.887 fps   = 137 DONE x 32 / 107.22s  (START -> last DONE)
  [steady-state]      41.203 fps   = 136 x 32 / 105.60s  (first -> last DONE)
  [ref mean, N/U]    102.450 fps   (arithmetic mean of 1/dt — reference only, biased high)
  batches counted: 137   stop reason: work queues drained + grace
============================================================
```

`stop_consuming()` belongs in the finalizer — **not** at first-stage shutdown. That is
what actually ends the server loop.

---

## 8 · Per-group breakdown

The system surfaces above are unchanged by grouping. Keep a second, per-group list of the
same arrival times; the system list stays authoritative, so the breakdown is strictly
additive in `done`/`frames` and can never move the total.

Each group needs its own window counter — a group reaches its first full window later
than the system does. See [01 §3.2](01-result-format.md) for the line format and
[01 §3.3](01-result-format.md) for what is and is not additive (group `fps` values are
**not**; read that section before writing a check).

---

## 9 · Configuration

```yaml
fps:
  grace_s: 10             # keep collecting this long after the work queues drain
  shutdown_timeout_s: 300 # hard cap so a dead worker can't hang the server
```

- `grace_s` — too small cuts off a slow last unit; too large delays shutdown.
- `shutdown_timeout_s` — only fires on failure; on a healthy run the grace path wins.

---

## 10 · Pitfalls

| Pitfall | Symptom | Fix |
|---|---|---|
| Two stages publish for the same unit | rate ≈ 2× real | gate the send so exactly one stage fires |
| Publish from a non-channel thread | random pika errors, corruption | hand off via `queue.Queue` |
| No final drain after compute-thread join | last few units uncounted | drain once more after `join()` |
| Server exits when the first stage finishes | undercount, no summary | drain-watch + grace + hard cap |
| Averaging per-gap `1/Δt` | rate reads 3–4× high | total items / total time |
| Queue not purged at startup | stale completions inflate the count | `queue_purge` on server init |
| Timestamps inside the message | needs synced device clocks | use the server's arrival time only |
| Per-group START instead of shared | group rates stop relating to the system | one shared START for all `fps` values |
