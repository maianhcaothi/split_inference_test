# Result log files — what a finished run writes

Every file below is written by the **server**, into `log-path` during the run, then copied
into an archive folder when the run ends. This is the field-by-field description of each
one. For the portable spec these files implement, see [../guide/](../guide/); for the
naming difference (`*_cluster.log` here vs `*_group.log` there), see
[../guide/README.md](../guide/README.md).

## Rules that hold for every file

- **One clock.** Every line starts with a 19-digit nanosecond epoch timestamp
  (`time.time_ns()`) taken on the **server**. No device clock ever appears in a shared
  file. The single exception is `kind=e2e` latency, which spans two machines by
  definition.
- **`key=value` after the timestamp**, space separated. Bare uppercase words
  (`SYSTEM`, `ALL`, `MACHINE`, `FREE`, `KIND`, `BROKER`) are line-kind flags, not values.
- **Percentages carry a literal `%`** (`utilization=93.14%`), seconds are `_s`, milliseconds
  `_ms`, megabytes `_mb`.
- **Truncated at server startup** — a results directory always describes exactly one run.
- **live** = appended as the run happens (survives a crash); **shutdown** = written once
  during the shutdown sequence.

Sample lines are real lines from the archived runs, except for the two optional features
(free time, queue-host RAM), which no archived run has emitted yet — those examples are
built from the emitting code and use `…` for values.

| File | When | One line per |
|---|---|---|
| [`batch_done_ns.log`](#batch_done_nslog) | live | completed batch (system) |
| [`fps_cluster_ns.log`](#fps_cluster_nslog) | live | completed batch (cluster-tagged) |
| [`fps_cluster.log`](#fps_clusterlog) | shutdown | cluster + `SYSTEM` |
| [`utilization.log`](#utilizationlog) | shutdown | device |
| [`utilization_cluster.log`](#utilization_clusterlog) | shutdown | cluster, cluster/role, `SYSTEM` |
| [`latency_cluster.log`](#latency_clusterlog) | shutdown | cluster/role/kind, cluster e2e, `SYSTEM` |
| [`free_time.log`](#free_timelog) | shutdown | device |
| [`free_time_cluster.log`](#free_time_clusterlog) | shutdown | cluster, role, reason, kind, machine, `SYSTEM` |
| [`free_time_series.log`](#free_time_serieslog) | shutdown | device × time bucket |
| [`broker_ram_ns.log`](#broker_ram_nslog) | live | RAM sample of the queue host |
| [`broker_ram.log`](#broker_ramlog) | shutdown | `BROKER` / `USED` / `DELTA` / `RABBIT` |
| [`message_size.log`](#message_sizelog) | shutdown | the one measured edge |
| [`message_size_series.log`](#message_size_serieslog) | shutdown | published message |
| [`map.log`](#maplog) | shutdown | cluster × pipeline + `OVERALL` |
| [`map_window.log`](#map_windowlog) | shutdown | sliding window |
| [`cut_change_ns.log`](#cut_change_nslog) | live | adaptive cut change |

---

## Throughput

### `batch_done_ns.log`

Written by `on_fps` ([Server.py:479](../src/Server.py#L479)). One line each time a `DONE`
message arrives from a completing tier. **The arrival is the event** — the message body is
never used for timing, so device clocks need no syncing.

```
1785127877009606331
1785128503201196436 13.17
```

| Column | Meaning |
|---|---|
| 1 | ns-epoch arrival of the `DONE` on the server clock |
| 2 *(optional)* | smoothed live window fps, `(W-1) × batch_size / span of last W arrivals`. Absent for the first `W-1` lines, where the window isn't full yet |

This is the raw system throughput series — the file the fps charts are built from.

### `fps_cluster_ns.log`

Same arrivals, tagged with the producing cluster ([Server.py:502](../src/Server.py#L502)).
The line count and timestamps match `batch_done_ns.log` exactly, line for line.

```
1785127877009606331 cluster=intermediate_queue_0 done=1
1785127880115332188 cluster=intermediate_queue_0 done=15 window_fps=12.34
```

| Field | Meaning |
|---|---|
| `cluster` | producing cluster id; `unknown` if the `DONE` body carried no tag (bucketed, never dropped) |
| `done` | running count of completions **for that cluster**; strictly increasing per cluster |
| `window_fps` | that cluster's own smoothed window fps, same formula, appears once the cluster has `W` completions |

### `fps_cluster.log`

The throughput summary, `_report_cluster_fps` ([Server.py:583](../src/Server.py#L583)).
One line per cluster, then exactly one `SYSTEM` line.

```
1785128504858762215 cluster=intermediate_queue_0 fps=18.131 steady_fps=18.491 done=336 frames=10752 share=66.7%
1785128504858762215 SYSTEM fps=25.220 done=504 frames=16128 clusters=2
```

| Field | Meaning |
|---|---|
| `fps` | whole-run throughput: `done × batch_size / (last DONE − START)`. Measured from the **shared** START, which is what makes cluster values additive |
| `steady_fps` | warm-up removed: `(done−1) × batch_size / (that cluster's first → last DONE)`. The fair number when comparing clusters. **Cluster lines only** — never on `SYSTEM` |
| `done` | batches completed |
| `frames` | `done × batch_size` |
| `share` | that cluster's portion of all batches; the quickest read on whether the assignment balanced the clusters. Sums to 100% |
| `clusters` | number of clusters (`SYSTEM` line only) |

Both are total-work / total-time, never a mean of per-interval rates — bursty arrivals make
the latter read 3–4× high.

---

## Utilization

### `utilization.log`

`_collect_utilization` ([Server.py:665](../src/Server.py#L665)) drains one `UTILIZATION`
report per registered device at shutdown and writes it as one line.

```
1785128504872091881 client=c6fdabe4-63e1-45f7-ad21-ef4de628bd3b role=cloud packages=169 busy_s=558.047 total_s=599.152 utilization=93.14%
```

| Field | Meaning |
|---|---|
| `client` | device identity (uuid in older runs, machine name in current ones) |
| `role` | `edge` / `cloud` |
| `packages` | work items this device processed |
| `busy_s` | time this device spent doing work, by **its own clock** |
| `total_s` | that device's run span, same clock |
| `utilization` | `busy_s / total_s`. Both terms come from one device, so clock skew cannot distort the ratio. Never exceeds 100% |

The timestamp is the **arrival** of the report on the server, so lines are ordered by who
reported first — not a property of the device.

### `utilization_cluster.log`

The roll-up ([Server.py:739](../src/Server.py#L739)). Three line kinds, distinguished by
their flag:

```
1785128504874248492 cluster=intermediate_queue_0 ALL devices=8 utilization=55.06% utilization_mean=53.29% busy_s=2348.297 total_s=4264.621 packages=672
1785128504874248492 cluster=intermediate_queue_0 role=cloud devices=2 utilization=93.44% busy_s=1119.832 total_s=1198.399 packages=336
1785128504874248492 SYSTEM devices=12 clusters=2 utilization=60.75% utilization_mean=59.01% busy_s=4060.923 total_s=6684.796
```

| Line kind | Marked by | Covers |
|---|---|---|
| cluster total | `cluster=` + `ALL` | every device in that cluster |
| cluster × role | `cluster=` + `role=` | devices of one role in one cluster |
| system | `SYSTEM` | every device that reported |

| Field | Meaning |
|---|---|
| `utilization` | **pooled**: `Σbusy_s / Σtotal_s`, which weights each device by how long it actually ran |
| `utilization_mean` | plain mean of the per-device percentages. On `ALL`/`SYSTEM` lines only, because a pooled number can hide one idle device among busy ones — the two disagreeing is the signal |
| `busy_s` / `total_s` | sums over the scope; cluster values add up to the `SYSTEM` value exactly |

---

## Latency

### `latency_cluster.log`

`_report_cluster_util_latency` ([Server.py:771](../src/Server.py#L771)). Devices ship
**raw samples**; the server pools them and takes percentiles — averaging per-device
percentiles is not a valid operation.

```
1785128504874248492 cluster=intermediate_queue_0 role=cloud kind=service n=336 mean_ms=3332.833 p50_ms=3470.922 p95_ms=3966.246 max_ms=4367.941
1785128504874248492 cluster=intermediate_queue_0 kind=e2e n=336 mean_ms=69655.623 p50_ms=69379.207 p95_ms=102639.355 max_ms=111760.937
1785128504874248492 SYSTEM kind=e2e n=504 mean_ms=75320.528 p50_ms=77109.393 p95_ms=109986.265 max_ms=120801.164
```

| `kind` | Measures | Notes |
|---|---|---|
| `service` | that device's own `get_input → output` | one clock, so exact. Its samples sum to that role's `busy_s`, which makes it the only kind comparable against utilization |
| `pipeline` | batch ready → published | same clock, but on the edge it also contains the wait in the two hand-off queues, so it tracks queue depth rather than device speed |
| `e2e` | edge batch start → completing tier's output | spans two machines, so it inherits any clock offset between them — **indicative, not exact**. Only the completing tier reports it, so there is one e2e series per cluster |

| Field | Meaning |
|---|---|
| `n` | pooled sample count |
| `mean_ms` | arithmetic mean of the samples |
| `p50_ms`, `p95_ms`, `max_ms` | **nearest-rank** percentiles, no interpolation — every number printed is a latency that was actually observed |

`role=` is absent on `e2e` and `SYSTEM` lines. `p50 ≤ p95 ≤ max` always holds.

---

## Free time (optional — `free_time.enable`)

Free time is wall clock in which a device did **no** pipeline work of any kind: no capture,
inference, compress, send, receive, decompress, postprocess or metrics. Each device merges
the busy intervals of all its threads before reporting ([src/FreeTime.py](../src/FreeTime.py))
— summing them would double-count the two pipeline lanes and can exceed the wall clock.

### `free_time.log`

One line per device ([Server.py:895](../src/Server.py#L895)).

```
<ts> client=… role=edge machine=machine-2 cluster=intermediate_queue_0 device=cpu span_s=467.394 busy_s=176.760 free_s=290.634 free=62.18% gaps=214 longest_free_ms=8134.221 host_idle=54.10%
```

| Field | Meaning |
|---|---|
| `span_s` | the device's run window |
| `busy_s` | **merged** busy intervals — the union, not the sum |
| `free_s` | `span_s − busy_s`; `busy_s + free_s == span_s` exactly |
| `free` | `free_s / span_s` |
| `gaps` | number of separate idle stretches |
| `longest_free_ms` | the single worst stall |
| `host_idle` | the OS's own idle/total CPU accounting over the run, across **all** processes including anything that isn't ours. Present only when readable |

### `free_time_cluster.log`

Six line kinds ([Server.py:935](../src/Server.py#L935)–[1025](../src/Server.py#L1025)):

| Line kind | Marked by | Covers |
|---|---|---|
| cluster total | `cluster=` + `ALL` | pooled `free` + `free_mean`, same convention as utilization |
| cluster × role | `cluster=` + `role=` | one role in one cluster |
| free reason | `cluster=`/`SYSTEM` + `FREE` | `reason=` + `free_s` + `share`. Shares sum to **100%** of that scope's free time; attribution is priority-ordered so nothing is double counted, and whatever no reason covers is reported as `unaccounted` rather than dropped |
| busy kind | `cluster=`/`SYSTEM` + `KIND` | `kind=` + `busy_s` + `share` of Σspan. These **may** sum to more than 100% — per-kind timers overlap across lanes by construction; only the merged `busy_s` in `free_time.log` is exclusive |
| machine | `MACHINE` | union of the busy intervals of every device process on that host. A machine with two device processes is only free when **neither** is working, so this cannot be derived from the per-device percentages. `merge_slop_s` reports interval-merge error; the server's own host appears with `devices=0` and only `host_idle` |
| system | `SYSTEM` | every device, plus `clusters=` and `machines=` |

### `free_time_series.log`

Free time over the run, for plotting ([Server.py:908](../src/Server.py#L908)). One line per
device per bucket.

```
<ts> client=… role=edge machine=machine-2 cluster=intermediate_queue_0 i=37 t_offset_s=37.000 bucket_s=1.000 free=41.20%
```

`i` is the bucket index, `t_offset_s = i × bucket_s` measured from that device's start, and
`free` is the fraction of that bucket the device was idle.

---

## Queue-host RAM (optional — `broker_ram.enable`)

The RabbitMQ host runs none of our code, so the server pulls its memory from outside over
one long-lived SSH session. Rationale and method: [../guide/11-broker-ram.md](../guide/11-broker-ram.md).

### `broker_ram_ns.log`

The live series, one line per sample ([BrokerRam.py:386](../src/BrokerRam.py#L386)),
flushed as it goes.

```
1786282738811691751 host=192.168.101.91 source=ssh phase=idle total_mb=5921.5 used_mb=1586.2 used=26.79% avail_mb=4335.3 free_mb=3770.5 cached_mb=747.0 swap_used_mb=1032.3 rabbit_rss_mb=87.8
```

| Field | Meaning |
|---|---|
| `source` | **`ssh`** = host memory from `/proc/meminfo`. **`rabbitmq_api`** = management-API fallback, where `used_mb` is the **broker process**, not the host. Never substituted silently — the label is the whole point |
| `phase` | `idle` (before dispatch — the host at rest), `run` (dispatch → last collector), `tail` (after finish). Stamped as the line is written, so a plot can shade the run window without reading the summary |
| `used_mb` / `used` | `MemTotal − MemAvailable`. Not `− MemFree`, which counts reclaimable page cache as used and reads ~90% on any machine that has touched a disk |
| `avail_mb`, `free_mb`, `cached_mb` | the raw `/proc/meminfo` terms behind it |
| `swap_used_mb` | a host that is swapping invalidates any latency conclusion from that run |
| `rabbit_rss_mb` | the broker process's own RSS. `used` answers *is the box full*; RSS answers *is it full because of the thing I care about* |

Sampling starts when the **server process starts** (`_start_broker_ram` in `__init__`,
[Server.py:330](../src/Server.py#L330)) — before any client has registered and long before
anything is published, so the opening samples are the queue host **at rest**. It stops
`broker_ram.tail_s` (default 2.0 s) **after** the shutdown drain, so the last samples show
the host settling rather than the busiest moment of shutdown.

Two marks partition the series: `dispatch` at the START fan-out and `finish` when the run
ends. Sampling never pauses at a boundary — the marks only label what was already being
recorded.

### `broker_ram.log`

The shutdown summary ([BrokerRam.py:456](../src/BrokerRam.py#L456)): four whole-window
lines, then one per phase, then the comparison.

```
<ts> BROKER  host=… source=ssh samples=1187 interval_s=1.000 span_s=1186.4 total_mb=5921.5 t_start_ns=… t_end_ns=…
<ts> USED    min_mb=… mean_mb=… p50_mb=… p95_mb=… max_mb=… min=…% mean=…% p95=…% max=…%
<ts> DELTA   start_mb=… end_mb=… growth_mb=… peak_over_start_mb=…
<ts> RABBIT  mean_rss_mb=… max_rss_mb=… swap_max_mb=…
<ts> PHASE   phase=idle samples=30 span_s=29.000 min_mb=… mean_mb=… p50_mb=… p95_mb=… max_mb=… mean=…% max=…% mean_rss_mb=… max_rss_mb=… t_start_ns=… t_end_ns=…
<ts> PHASE   phase=run  samples=… …
<ts> PHASE   phase=tail samples=… …
<ts> COMPARE idle_mean_mb=… run_mean_mb=… run_minus_idle_mb=… run_peak_over_idle_mb=… idle_rss_mb=… run_rss_mb=… run_rss_over_idle_mb=… tail_mean_mb=… tail_minus_idle_mb=… tail_span_s=…
```

- **`COMPARE run_minus_idle_mb` / `run_peak_over_idle_mb`** — what running the system
  costs this host, average and peak, against the same host at rest. This is the headline:
  the only figure in the file that is a property of your system rather than of the machine.
- `COMPARE tail_minus_idle_mb` — whether it gave the memory back. The tail is deliberately
  short (1–2 s): long enough to catch the drain releasing, too short to wait out Erlang's
  GC, so a positive value reads "not back yet", not "leak". A real leak is still visible in
  the **next** run's `idle` phase.
- `DELTA growth_mb` — window ends against each other (first vs last sample). With the
  window opening at server start, `start_mb` is the at-rest figure.
- `DELTA peak_over_start_mb` — the headroom question; compare against
  `mean_mb` in `message_size.log` × `backpressure.max_queue`.
- Percentiles are nearest-rank over the raw samples, same rule as latency.
- A phase with no samples is omitted rather than written as zeros; `COMPARE` appears only
  when both `idle` and `run` exist.
- With no samples at all, a single `BROKER … samples=0 (reason)` line is written instead — a
  missing file is indistinguishable from a healthy host, `samples=0 (permission denied)` is not.

---

## Message size (optional — `message_size.enable`)

How many bytes an edge actually hands the broker per message, measured **before**
`basic_publish` ([Scheduler.py:288](../src/Scheduler.py#L288)). Rationale and method:
[../guide/12-message-size.md](../guide/12-message-size.md).

Only **one** device measures: the first client that registered at `layer_id=1`. The
server picks it in `notify_clients` ([Server.py:2408](../src/Server.py#L2408)) and sets
`message_size.measure=True` in that client's START message — no client decides for
itself, so the job can never land on two machines or none. Every edge in a cluster
publishes the same feature map from the same cut, so measuring all nine would cost nine
times as much and produce the same number.

That device also writes its own `message_size_<cluster>_<id>.log` as the run goes (one
line per message, flushed) — the copy that survives a broker or server problem. At finish
it publishes the samples to `msgsize_queue`; the server drains it in
`_collect_msg_size` ([Server.py:1059](../src/Server.py#L1059)) and writes the two files
below.

### `message_size.log`

One summary line for the measured device.

```
<ts> client=machine-2 role=edge machine=machine-2 cluster=intermediate_queue_0 mode=split splits=5 compress=on num_bit=8 batch_size=32 n=504 total_mb=19657.464 mean_mb=39.003 p50_mb=39.022 p95_mb=39.613 max_mb=40.098 min_mb=37.909 span_s=714.260 rate_mb_s=27.521 per_frame_mb=1.2188
```

| Field | Meaning |
|---|---|
| `n` | messages this edge published |
| `total_mb` | bytes it put on the wire over the whole run (MB = 10⁶, same unit as `broker_ram*`) |
| `mean_mb`, `p50_mb`, `p95_mb`, `max_mb`, `min_mb` | per-message size. **Nearest-rank** percentiles over the raw samples, same rule as latency |
| `span_s` | first → last publish, on that device's clock |
| `rate_mb_s` | `total_mb / span_s` — this one edge's egress. Multiply by the number of edges sharing the NIC to get offered load |
| `per_frame_mb` | `mean_mb / batch_size` |
| `mode`, `splits`, `compress`, `num_bit`, `batch_size` | the context that determines the size. A size without them is unreproducible |

Statistics come from **every** sample, even when the shipped series was decimated.

### `message_size_series.log`

The plottable series, one line per published message.

```
<ts> client=machine-2 cluster=intermediate_queue_0 i=0 t_offset_s=0.000 batch_id=0 bytes=38897647 mb=38.898
```

| Field | Meaning |
|---|---|
| col 1 | the **server's** write time, identical on every line — the samples came from a device, so their own timestamps never enter this file |
| `i` | sample index |
| `t_offset_s` | seconds since that device's own first publish |
| `batch_id` | the batch this message carried (`-1` if unknown) |
| `bytes` | exact integer, the authoritative value |
| `mb` | the same number in MB |

Runs longer than `max_samples` (default 5000) ship an evenly decimated series — it still
spans the whole run, only more coarsely.

**What to do with it:** `mean_mb` × `backpressure.max_queue` is the RAM the broker must
hold; compare it against `DELTA peak_over_start_mb` in `broker_ram.log`. `max_mb` against
RabbitMQ's `max_message_size` is the margin before a deeper cut kills the run. And the
series on the same x axis as `batch_done_ns.log` separates a transport problem (size flat,
throughput falling) from a workload one.

---

## Accuracy (outside the portable guide's scope)

### `map.log`

Two mAP pipelines, written at shutdown ([Server.py:1470](../src/Server.py#L1470)).

```
1785128505579200616 cluster=intermediate_queue_0 WINDOW mAP50_95=0.1004 mAP50=0.1857 (mean of 14 window(s) x 16 batches, step 1)
1785128505579200616 cluster=intermediate_queue_0 ALL mAP50_95=0.0882 mAP50=0.1632 (905/905 GT frame(s) matched)
<ts> OVERALL WINDOW mAP50_95=… mAP50=… (avg over 2 cluster(s))
<ts> OVERALL ALL mAP50_95=… mAP50=… (avg over 2 cluster(s))
```

`WINDOW` is the mean over sliding windows (the accuracy counterpart of window fps); `ALL`
is one metric fed every scorable frame of the run, each frame weighted exactly once — the
counterpart of SYSTEM FPS. `OVERALL` is always written, even with one cluster, so a parser
always finds one authoritative line per pipeline.

### `map_window.log`

The mAP-over-time series ([Server.py:1292](../src/Server.py#L1292)) — 16 consecutive
batches, stepped one batch at a time. Line it up against `batch_done_ns.log` and
`cut_change_ns.log` to see how accuracy responded to a split-point change.

```
1785128505579200616 cluster=intermediate_queue_0 window=1 batches=1-16 frames=512 mAP50_95=0.0833 mAP50=0.1544
```

### `cut_change_ns.log`

Control-plane events ([Server.py:1860](../src/Server.py#L1860)) — free-form text after the
timestamp, meant for overlaying on a timeline. Only written when the adaptive controller
ran.

```
1785375596567283487 intermediate_queue_1: cut 5->4 shallower
```

---

## What the console prints, and where it lands

The shutdown sequence prints six blocks, each the human view of the files above:

| Console block | File |
|---|---|
| `[SYSTEM FPS]` / `[steady-state]` / `[ref mean, N/U]` | `fps_cluster.log` `SYSTEM` line |
| `[cluster] … fps … steady= … share=` | `fps_cluster.log` cluster lines |
| `[PER-CLUSTER UTILIZATION & LATENCY]` | `utilization*.log`, `latency_cluster.log` |
| `[FREE TIME]` | `free_time*.log` |
| `[MESSAGE SIZE]` | `message_size*.log` |
| `[BrokerRAM]` | `broker_ram*.log` |
| `[mAP]` | `map*.log` |

`[ref mean, N/U]` is printed for comparison only and deliberately **not** logged — it is the
arithmetic mean of per-gap `1/dt`, which over-weights bursts and reads high.

## The archive

`_archive_results` ([Server.py:1514](../src/Server.py#L1514)) copies the files into:

```
results/results_<MMDD>_<HHMM>_<tag>/
```

`tag` is the experiment mode (`only_cloud`, `only_edge`), or for a split run `dynamic` when
the adaptive controller was free to move the cut point and `split` when it was not. A second
run finishing in the same minute gets `-2`, `-3`, … rather than overwriting.

Empty files are skipped rather than archived as misleading zero-length results, and
`cut_change_ns.log` is skipped entirely when the adaptive controller did not run — it is only
truncated when that controller starts, so archiving it otherwise ships the *previous* run's
cut changes.

## Checking a run

```bash
python guide/validate_results.py results/results_<MMDD>_<HHMM>_<tag>
```

It checks the line grammar, the additivity of the cluster→SYSTEM roll-ups, percentile
ordering, and the free-time invariants. It does not yet check `broker_ram*.log`.
