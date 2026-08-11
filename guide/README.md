# Distributed Run Results — Portable Guide

A complete, self-contained specification for **measuring a distributed pipeline run and
reporting the results in one fixed format**, plus the method for turning those results
into charts.

Read this directory and you can make any project emit results that look identical to
every other project's — same filenames, same line formats, same charts, same colors.

> **Scope.** Throughput, latency, device utilization, and — optionally — free time
> ([10](10-free-time.md)): how much of each device and each machine was idle, the RAM
> of an infrastructure host you run no code on ([11](11-broker-ram.md)), and the size of
> the payload a worker puts on the wire ([12](12-message-size.md)).
> Model-accuracy metrics
> (mAP and friends) are deliberately **out of scope** — nothing here depends on
> ground-truth labels or a detection task. The pipeline being measured can be doing
> anything: inference, transcoding, ETL, simulation.

---

## The contract, in one paragraph

A run produces **six required plain-text log files** in one directory (plus optional
ones). Every line begins with a
nanosecond-epoch timestamp taken on the **server's** clock, followed by `key=value`
pairs. Every file is truncated at run start. Conform to
[01-result-format.md](01-result-format.md) and the notebook in
[08-build-pipeline.md](08-build-pipeline.md) renders your results with **zero changes** —
that is the entire point of fixing the format.

```
<run-dir>/
├── batch_done_ns.log         system throughput series      one line per completed unit
├── group_rate_ns.log         per-group throughput series   one line per completed unit
├── group_rate.log            throughput summary            one line per group + SYSTEM
├── utilization.log           per-device busy ratio         one line per device
├── utilization_group.log     utilization rolled up         per group, per group/role, SYSTEM
├── latency_group.log         latency distributions         per group/role + per group + SYSTEM
├── events_ns.log             control-plane events          one line per event (optional)
├── free_time.log             per-device idle time          one line per device (optional)
├── free_time_group.log       free time rolled up           per group, per machine, SYSTEM (optional)
├── free_time_series.log      free time over the run        one line per device per bucket (optional)
├── broker_ram_ns.log         infra-host RAM over the run   one line per sample (optional)
├── broker_ram.log            infra-host RAM summary        BROKER/USED/DELTA lines (optional)
├── message_size.log          payload size summary          one line per measured worker (optional)
└── message_size_series.log   payload size over the run     one line per published message (optional)
```

---

## Reading order

| # | File | Read it when |
|---|---|---|
| — | **this file** | first, always |
| 01 | [result-format.md](01-result-format.md) | **normative.** Implementing or validating the output format |
| 02 | [throughput.md](02-throughput.md) | implementing the throughput measurement |
| 03 | [utilization.md](03-utilization.md) | implementing per-device utilization |
| 04 | [latency.md](04-latency.md) | implementing latency collection |
| 05 | [archiving.md](05-archiving.md) | making runs reproducible and comparable |
| 06 | [visualization.md](06-visualization.md) | **before writing any chart code** |
| 07 | [chart-catalogue.md](07-chart-catalogue.md) | building a specific chart |
| 08 | [build-pipeline.md](08-build-pipeline.md) | building the notebook that produces the charts |
| 09 | [port-checklist.md](09-port-checklist.md) | last — verifying the port is complete |
| 10 | [free-time.md](10-free-time.md) | measuring how much of the fleet was idle (optional) |
| 11 | [broker-ram.md](11-broker-ram.md) | measuring an infrastructure host you run no code on (optional) |
| 12 | [message-size.md](12-message-size.md) | measuring the bytes a worker puts on the wire (optional) |

**If you are producing results:** 01 → 02 → 03 → 04 → 05 → 09.
**If you are visualizing existing results:** 01 → 06 → 07 → 08 → 09.
**If you are doing both:** in order.

---

## Terminology — map these onto your project

This guide is written in neutral terms. The reference implementation it was extracted
from is a split-inference system; the middle column is what those terms mean there.
**Substitute your own nouns in the third column and nothing else changes.**

| Guide term | Reference instance | Yours |
|---|---|---|
| **unit** | one batch of frames | the thing whose completion you count |
| **unit size** | `batch_size` (frames per batch) | items per unit (use `1` if units are atomic) |
| **worker / device** | an edge or cloud machine | any process that does work and reports |
| **role** | `edge` / `cloud` | the stage class a device belongs to |
| **group** | a cluster (`intermediate_queue_0`) | any partition whose totals must sum to the system |
| **server** | the central controller | whatever holds the authoritative clock |
| **completing tier** | the stage producing the final output | the stage that emits `DONE` |
| **control event** | an adaptive split-point change | any control-plane decision worth overlaying on the timeline |

If your system has no groups, emit a single group and the `SYSTEM` line still works.
If it has no roles, use one role name for everything. **Never drop a file** — an empty
but present file is a valid answer; a missing file breaks the reader.

---

## Design invariants — the reasons the format is what it is

These are not stylistic. Each one exists because the obvious alternative is broken.

1. **One clock.** Every timestamp in every shared file is `time.time_ns()` on the
   **server**. Distributed device clocks are never assumed to be in sync, and no shared
   number is ever computed by subtracting one device's timestamp from another's. The
   single exception is end-to-end latency, which spans two machines by definition and is
   labelled as indicative rather than exact.
2. **The arrival is the event.** Completion messages carry an *identity*, never a
   timing measurement. A garbled body can mis-bucket one unit; it can never distort a
   rate.
3. **Ratios stay inside one device.** Utilization's numerator and denominator both come
   from the same device's own clock, so clock skew cannot distort the ratio.
4. **Totals are additive.** Per-group values are computed against a shared start, so
   they sum to the system value. The system total is computed independently from every
   arrival, so a mis-tagged unit can shift the breakdown but never move the total.
5. **Truncate at startup.** Every file is emptied when the run begins. A results
   directory always describes exactly one run.
6. **Raw samples, not pre-reduced stats.** Devices ship raw latency samples; the server
   pools them and takes percentiles. Averaging per-device percentiles is not a valid
   operation.
7. **Telemetry never kills the run.** Every failure path in measurement code degrades to
   a warning. A broken metric loses a number; it must not lose the run.
8. **Aggregate before you divide.** Throughput is total items / total time, never the
   mean of per-interval rates — bursty arrivals make the latter read 3–4× high.
9. **The run's configuration has exactly one home.** Workers receive every runtime
   setting in the dispatch message and read **nothing** from their own config file at
   run time. Otherwise a measurement flag has to be changed on N machines, the copies
   drift, and a run silently mixes two configurations. Anything a worker still reads
   locally (connection details, per-machine thread caps) must be listed explicitly —
   what is *not* centralized is as important to write down as what is.
10. **A flag that travels must be honoured at both ends.** When the server turns a
   feature off, its own shutdown collector for that feature must skip too. A collector
   that still polls a queue nobody will ever publish to burns its full timeout on every
   run and then warns `0/N` — a stall and a scary message caused by a setting working
   exactly as intended.
11. **Measure every device where devices differ; measure one where they don't.**
   Utilization and free time are per-device because each device's answer is its own.
   A property fixed by the configuration — the size of the payload leaving a stage
   ([12](12-message-size.md)) — is the same on every device in a group, so measuring all
   of them costs N times as much and produces one number N times. Pick the worker by a
   rule the server can evaluate before dispatch (registration order), and put the choice
   in the dispatch message, never in the worker's own config.

---

## What "same result format" buys you

- One notebook renders every project's results. No per-project chart code.
- Runs from different projects are directly comparable, field by field.
- A results directory is self-describing months later, especially with the archived
  config ([05-archiving.md](05-archiving.md)).
- Conformance is testable — [01-result-format.md](01-result-format.md) ships a validator
  script that either passes or tells you which line is wrong.

---

## Quick start for a new project

```bash
# 1. Emit the format
#    Implement 02 (throughput), 03 (utilization), 04 (latency).
#    Validate as you go:
python guide/validate_results.py <run-dir>

# 2. Archive the run
#    Follow 05 — copy the logs plus the config that produced them.

# 3. Visualize
#    Read 06 BEFORE writing chart code. Then build with 08.
python build_nb.py && python run_nb.py

# 4. Verify
#    Walk the checklist in 09. Open every generated PNG.
```

Do not skip step 4. The measurement bugs that matter (double-counted units, a tier that
stops reporting early, a ratio above 100%) are visible in the charts and invisible in
the code.
