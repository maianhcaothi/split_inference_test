# 11 · Infrastructure host RAM — measuring a machine you don't run code on

Every measurement in [02](02-throughput.md)–[04](04-latency.md) is reported *by* a
process you wrote. This one is not. Between the workers sits at least one machine that
runs only third-party infrastructure — a message broker, a cache, an object store — and
it has no process of yours to instrument.

That machine is on the critical path anyway, so it can be the bottleneck, and when it is,
**every symptom shows up somewhere else**. This file is the method for measuring it.

> **Instance.** In the reference project the infrastructure host is the RabbitMQ box, and
> the two files are named `broker_ram_ns.log` / `broker_ram.log`. Substitute your own
> noun; nothing else changes.

---

## 1 · Why it earns a file

Everything handed between stages crosses this host and is buffered in its memory. In the
reference instance one unit is a ~39 MB uncompressed feature map, so a queue ten units
deep is ~400 MB resident in the broker.

The failure mode is the interesting part. A broker at its high-water mark does not fail —
it **blocks publishers**. On the worker this appears as a stall with no local cause: free
time attributed to `backpressure` or `downstream` ([10 §3](10-free-time.md)), utilization
falling, latency rising, and nothing wrong with the device itself. The two candidate
explanations — *the next stage is slow* and *the broker stopped accepting* — produce
almost identical worker-side telemetry.

The infrastructure host's memory curve is what separates them. Without it you will tune
the wrong stage.

---

## 2 · Pull it from the server, in one session

Two rules, both learned the hard way.

**The server samples it, not a worker.** The server is the only component that lives for
the whole run, already owns the shutdown and archive steps, and holds the authoritative
clock ([README, invariant 1](README.md)). A worker that sampled it would stop when it
finished, and workers do not finish together.

**One long-lived session, not one connection per sample.** The natural implementation —
open SSH, read, close, sleep, repeat — makes the machine *whose load you are measuring*
pay a TCP handshake plus an authentication every sample. At a 1 s interval over a
20-minute run that is 1200 logins you added to the thing under test. Instead open one
session running a remote loop that prints one line per sample, and read its output:

```sh
i=0
while [ $i -lt $MAX ]; do
  i=$((i+1))
  r=`ps -eo rss=,comm= | awk '$2 ~ /<infra-process>/ {s+=$1} END{print s+0}'`
  awk -v ts="`date +%s%N`" -v r="$r" '
    /^MemTotal:/{t=$2} /^MemFree:/{f=$2} /^MemAvailable:/{a=$2}
    /^Buffers:/{b=$2} /^Cached:/{c=$2}
    /^SwapTotal:/{st=$2} /^SwapFree:/{sf=$2}
    END{print "RAM", ts, t, f, a, b, c, st, sf, r}' /proc/meminfo
  sleep $INTERVAL
done
```

Details that are not optional:

- **All fields on one line.** A partial read can then never interleave two samples.
- **`^` anchors in the awk patterns.** `/Cached:/` also matches `SwapCached:`.
- **Bound the loop** (`$MAX`). A sampler orphaned by a hard kill of the server otherwise
  runs on the infrastructure host forever.
- **Ship the script base64-encoded** (`echo <b64> | base64 -d | sh`). The script crosses
  a local argv *and* a remote login shell; base64 removes both quoting layers.
- **Read `stderr` on its own thread** and keep the last few lines. When authentication
  fails, that tail is the entire diagnosis.

---

## 3 · What "used" means

Use **`MemTotal − MemAvailable`**.

`MemTotal − MemFree` counts reclaimable page cache as used and reads ~90% on any machine
that has touched a disk — a number that is always alarming and never actionable.
`MemAvailable` is the kernel's own estimate of what a new allocation could actually get,
so the difference is memory that is really committed.

Record the infrastructure process's own RSS in the same sample. `used` answers *is the
box full*; RSS answers *is it full because of the thing I care about*. Those have
different fixes.

Record swap too. A host that is swapping is already past the point where its latency
contribution is stable, and the reference broker was found holding ~1 GB of swap before
any run started — a fact no worker-side meter could ever have surfaced.

---

## 4 · Two sources, always labelled

Password-authenticated SSH from an unattended process is exactly the thing hardened
environments prevent, so a fallback is required. In the reference instance the broker's
management API can report the broker process's memory over HTTP.

**Never silently substitute one for the other.** They answer different questions: one is
host memory across all processes, the other is one process. Every line carries a
`source=` key, and the summary and console output say which one produced it. A fallback
that is not labelled is worse than no fallback — it produces a plausible number that
means something other than what the file name says.

---

## 5 · The files

Two files, one feature — emit both or neither ([01 §2](01-result-format.md), files
11–12). Same universal line grammar as everything else.

**`<infra>_ram_ns.log`** — the series, written live, one line per sample:

```
1786282738811691751 host=192.168.101.91 source=ssh phase=idle total_mb=5921.5 used_mb=1586.2 used=26.79% avail_mb=4335.3 free_mb=3770.5 cached_mb=747.0 swap_used_mb=1032.3 rabbit_rss_mb=87.8
```

Written **live**, not buffered to the end, for the same reason `batch_done_ns.log` is: a
run that dies still leaves the series behind, and the series is the part you cannot
reconstruct.

`phase=` is stamped as the line is written (`idle` / `run` / `tail`, see [§6](#6--the-window-measure-the-host-before-it-is-asked-to-do-anything)).
Marks only move forward, so a line written before dispatch is `idle` and stays `idle`.
Carrying it per sample makes the series self-describing: a plot can shade the run window
without cross-referencing the summary's mark timestamps.

**`<infra>_ram.log`** — the shutdown summary. Four whole-window lines, then one per phase,
then the comparison:

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

`DELTA` measures the window's own ends (first sample → last sample), so with the window
opening at controller start, `start_mb` **is** the at-rest figure and `growth_mb` is what
the whole session added. `COMPARE` measures phase against phase, which is the stronger
statement: it compares a stretch of idle samples with a stretch of running ones rather
than one endpoint with another.

A phase with no samples is **omitted**, not written as zeros — a run with no idle window
must look like one. `COMPARE` is written only when both `idle` and `run` exist; the tail
keys only when a tail exists.

Percentiles are **nearest-rank over the raw samples**, no interpolation — same rule as
latency ([04](04-latency.md)), so every number printed is a value that was actually
observed.

When there are no samples, still write a `BROKER` line with `samples=0` and the reason in
trailing parentheses. A missing file is indistinguishable from a run where the host was
fine; `samples=0 (permission denied)` is not.

---

## 6 · The window: measure the host before it is asked to do anything

**Start sampling when the controller starts** — in its constructor, before any worker has
registered and long before anything is published. **Stop a couple of seconds after the run
ends**, past the shutdown drain.

The instinct is to start at dispatch, where the throughput clock starts
([02](02-throughput.md)), so the first sample is "the baseline with every queue empty".
That is a baseline of the wrong thing. It answers *what did this host hold the moment
before the first message* — one sample, on a machine that has already been connected to,
had its queues declared and purged, and is running whatever else it runs. What you
actually need is the counterfactual: **this host, measured the same way, while the system
is not running at all.** That is a stretch of samples, not a point, and the only place to
get it is before the run exists.

It costs nothing. The controller is already alive, the sampler is already a background
thread, and the extra samples land in the same file. What it buys is a denominator: every
later number becomes *"running the system costs this host N MB"* instead of *"this host
was using N MB"*, which is unfalsifiable on a box you do not otherwise control.

Stopping needs the same care at the other end. The last collector finishing is not the
system being idle — the drain is still settling, and a curve stopped there ends on the
busiest moment of the shutdown. Sample a short **tail** past it, 1–2 seconds:

- The drain is precisely when a backed-up host gives memory back, so a curve that does
  **not** fall there is the signal that something is still holding units.
- A tail that is still far above idle means the host has not returned to rest. Keep the
  tail short on purpose: it is not long enough to wait out a runtime's own garbage
  collection, so a positive tail reads as *"not back yet"*, never as proof of a leak.
  A leak is a tail that is still there at the **next** run's idle phase.

### Phases

One continuous series, partitioned by two marks the controller records on its own clock:

| Phase | From → to | What it is |
|---|---|---|
| `idle` | controller start → dispatch | the host at rest — the reference |
| `run` | dispatch → last collector done | the host under load |
| `tail` | finish → sampler stop | the host settling |

Marks partition, they never gate: sampling does not pause at a boundary, so a missing mark
degrades to a coarser split and never to a gap in the data. A run that never dispatched is
all `idle`, which is exactly what it was.

Report each phase, and then the comparison the phases exist for: `run` mean and peak
against the `idle` mean. That single line — *running the system costs this host +X MB on
average, +Y MB at peak* — is the answer the whole file was built to give.

---

## 7 · Reading it

0. `COMPARE run_minus_idle_mb` and `run_peak_over_idle_mb` — **what running the system
   costs this host**, on average and at its worst, against the same host at rest. Start
   here: it is the only number in the file that is a property of your system rather than
   of the machine. `tail_minus_idle_mb` says whether it gave that back.
1. `DELTA growth_mb` — did the run leak? A positive growth across a run that drained
   completely means units are still buffered somewhere.
2. `DELTA peak_over_start_mb` — the real headroom question. Compare against
   `unit_size × max_queue_depth`; if it is far larger, something is buffering that you did
   not account for.
3. `USED max` vs `total_mb` — how close to the wall it came.
4. `RABBIT max_rss_mb` — whether it was the infrastructure process or something else on
   the box.
5. `swap_max_mb` — non-zero invalidates any latency conclusion drawn from the same run.

Plot it on the same x axis as `batch_done_ns.log`. Memory climbing while throughput falls
is the backpressure signature, and it is unmistakable once the two curves are stacked.

---

## 8 · Invariants

0. **Measure the host before it is asked to do anything, and after it stops being
   asked.** A meter that only runs while the system runs can report a level but never a
   cost. The window opens at controller start and closes a second or two past the drain;
   marks split it into `idle` / `run` / `tail` without ever pausing the sampling.
1. **The measured host pays as little as possible.** One session, one loop, one
   `sleep` — never a connection per sample.
2. **Labelled sources.** Every line says where the number came from; fallbacks are never
   silent.
3. **Live series, summary at shutdown.** The series survives a crash; the summary does
   not need to.
4. **Telemetry never kills the run** ([README, invariant 7](README.md)). Unreachable host,
   refused credentials, missing client — all degrade to a warning and a `samples=0` line.
5. **Credentials are host credentials.** The login for the machine is not the same as the
   application credentials for the service running on it, and confusing the two produces
   an authentication failure that looks like a network problem.
