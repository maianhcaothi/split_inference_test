# 09 · Porting checklist

Work top to bottom. Every item is testable — no item says "make sure it's good".

---

## Phase 1 · Map your project onto the guide

- [ ] Filled in the terminology table from [README](README.md) — what is a **unit**, a
      **role**, a **group**, the **completing stage**, in your system
- [ ] Chose a naming scheme (`group_*` or `cluster_*`) and wrote it down. **One scheme
      per project**, never mixed ([01 §2](01-result-format.md))
- [ ] Decided `unit_size` (items per unit; `1` if units are atomic)
- [ ] Identified which stage completes a unit — and confirmed **exactly one** does
- [ ] No groups? Emit a single group; the `SYSTEM` line still works
- [ ] No roles? Use one role name for everything

---

## Phase 2 · Throughput ([02](02-throughput.md))

- [ ] Dedicated non-durable queue, declared by both sides
- [ ] Server **purges** it at startup
- [ ] Exactly one stage publishes per unit, gated on mode
- [ ] Body is an identity string only — no timestamp, no unit id
- [ ] Publishing happens on the channel-owning thread (`queue.Queue` hand-off if not)
- [ ] **Final drain after the compute thread joins**
- [ ] `on_fps` takes **one** `time.time_ns()` reading and uses it for both math and log
- [ ] Manual ack
- [ ] Shared START recorded when work is dispatched
- [ ] Drain-watch: queue depth + grace + hard cap; **`stop_consuming()` only in the
      finalizer**
- [ ] `W = 16`
- [ ] `batch_done_ns.log` written with **two arities** (1 col during warm-up, 2 after)
- [ ] `group_rate_ns.log` has its own per-group window counter
- [ ] `group_rate.log`: shared START for `fps`, own first-completion for `steady_fps`
- [ ] `SYSTEM` line carries **no** `steady_fps` and **no** `share`
- [ ] Console summary matches the format table in [02 §7](02-throughput.md) exactly —
      **do not reflow the f-strings**

---

## Phase 3 · Utilization ([03](03-utilization.md))

- [ ] Per-device timing log, one event per line, `<ns> <event name>`
- [ ] Old logs deleted at startup; `start` opens in `"w"` mode
- [ ] Parser splits on the **first space only** (`split(" ", 1)`)
- [ ] Parser **ignores unrecognized events**
- [ ] Unmatched `get input` (crash mid-unit) is dropped, not counted
- [ ] Pipelined workers put `get input`/`output` on the **compute thread only**
- [ ] `start`/`end` written by the main thread — never two concurrent writers
- [ ] Numerator and denominator both from the **same device's clock**
- [ ] Returns `None` on a bad log; caller skips sending
- [ ] Reports go to a **dedicated queue**, not the control queue
- [ ] Server purges that queue at startup
- [ ] Collection has a timeout, warns on partial, **never hangs the run**
- [ ] `utilization_group.log` emits **both** `utilization` (pooled) and
      `utilization_mean` on `ALL`/`SYSTEM` lines
- [ ] **No device reports above 100%** — if any does, intervals are overlapping

---

## Phase 4 · Latency ([04](04-latency.md))

- [ ] Devices ship **raw sample arrays**, never pre-reduced percentiles
- [ ] Server pools per `(group, role, kind)` before reducing
- [ ] **Nearest-rank** percentiles, no interpolation
- [ ] All three kinds emitted: `service`, `pipeline`, `e2e`
- [ ] `Σ service samples == busy_s` for the matching role
- [ ] `e2e` reported **only** by the completing stage, one series per group + `SYSTEM`
- [ ] `role=` absent on `e2e` lines
- [ ] `n=` on every line
- [ ] Everything in **milliseconds**, `{:.3f}`
- [ ] `p50 ≤ p95 ≤ max` on every line

---

## Phase 4b · Optional measurements ([10](10-free-time.md), [11](11-broker-ram.md), [12](12-message-size.md))

Skip the block for any feature you are not porting. Do **not** half-port one — each is
all its files or none ([01 §2](01-result-format.md)).

**Free time ([10](10-free-time.md))**

- [ ] Busy intervals are **merged across lanes**, never summed
- [ ] `busy_ns + free_ns == span_ns` exactly, per device
- [ ] Free reasons sum to `free_ns` exactly — attribution is priority-ordered, so no
      moment is claimed twice and none is left out
- [ ] `Σ per-kind durations ≥ merged busy` whenever two lanes overlap; if they are equal
      on a pipelined worker, the merge is not happening
- [ ] Machine free time comes from the **union of intervals** of the processes on that
      host, never from averaging their percentages
- [ ] A disabled tracker reports nothing rather than reporting zeros

**Infrastructure host RAM ([11](11-broker-ram.md))**

- [ ] Sampled by the **server**, over **one** long-lived session — not one connection per
      sample
- [ ] `used = MemTotal − MemAvailable`, never `− MemFree`
- [ ] Every line carries `source=`; a fallback source is labelled, never substituted
      silently
- [ ] Series written **live**; summary written at shutdown
- [ ] Window opens at **controller start** — before any worker registers or anything is
      published — so the series contains the host **at rest**, not just under load
- [ ] Window closes **1–2 s after** the run, past the drain, so the last sample is not the
      busiest moment of the shutdown
- [ ] `idle` / `run` / `tail` marks **partition** the series; sampling never pauses at a
      boundary, and a missing mark coarsens the split rather than leaving a gap
- [ ] The summary states what running the system **cost** the host (run vs idle), not only
      what the host was holding
- [ ] A phase with no samples is omitted, never written as zeros
- [ ] Remote loop is bounded, so an orphaned sampler cannot outlive the run
- [ ] No samples still writes a `samples=0` line naming the reason
- [ ] Host login credentials are not confused with the service's application credentials

**Message size ([12](12-message-size.md))**

- [ ] Exactly **one** worker measures — the first registered at the first stage — and the
      **server** chooses it; no worker reads that decision from its own config
- [ ] The size is recorded **before** the publish call, not after
- [ ] The value is the **serialized** byte count handed to the transport
- [ ] Local file written live, one line per message; report shipped at finish
- [ ] Sample times ship as **offsets**, so no device clock reaches a shared file
- [ ] Summary statistics use **all** samples even when the shipped series is decimated
- [ ] Percentiles are nearest-rank; `min ≤ p50 ≤ p95 ≤ max` holds
- [ ] The summary line carries the context that determines the size (compression, split
      point, unit size, mode)
- [ ] A failed write warns **once** and disables the recorder, rather than warning per
      message

**Feature flags** — for every optional feature above:

- [ ] The flag lives in the **server's** config and travels in the dispatch message; no
      worker reads it locally ([README, invariant 9](README.md))
- [ ] Turning it off also skips the **server's own collector** for it, so shutdown does
      not burn a timeout polling a queue nobody publishes to
      ([README, invariant 10](README.md))
- [ ] The files still exist (empty) when the feature is off

---

## Phase 5 · Lifecycle & archive ([01 §4](01-result-format.md), [05](05-archiving.md))

- [ ] **All** result files truncated at startup, before any worker can write
- [ ] Truncation happens **once, centrally** — not per worker
- [ ] Measurement queues purged at startup
- [ ] Write-once caches cleared at startup (leftovers otherwise win forever)
- [ ] Archive copies (not moves), skips empty files, is collision-safe
- [ ] **`config.yaml` archived alongside the numbers**
- [ ] No conditionally-truncated file can leak into a later run's archive
      ([05 §4](05-archiving.md))
- [ ] Archive failure is non-fatal; empty archive warns loudly

---

## Phase 6 · Format conformance

```bash
python guide/validate_results.py <run-dir> --names <scheme>
```

- [ ] **Exit code 0, zero errors**, on every run directory
- [ ] Ran the negative test: corrupt a copy and confirm the validator catches it
- [ ] Line counts match between `batch_done_ns.log` and `group_rate_ns.log`
- [ ] Group `done` and `frames` sum exactly to `SYSTEM`
- [ ] `SYSTEM` span == max group span (this is the shared-START check)
- [ ] Comparison runs have **identical** `done`/`frames`, or you are not comparing like
      with like ([05 §6](05-archiving.md))

---

## Phase 7 · Visualization ([06](06-visualization.md)–[08](08-build-pipeline.md))

**Read [06](06-visualization.md) before writing chart code.**

Data
- [ ] Every input file read; record counts sanity-checked
- [ ] Assumptions verified in code, not assumed ([08 §4](08-build-pipeline.md))
- [ ] Units normalized at the parser, not at the label
- [ ] **No column shadows a DataFrame attribute** ([08 §8](08-build-pipeline.md))
- [ ] Raw ids mapped to display labels at parse time

Encoding
- [ ] **No dual-axis plot anywhere**
- [ ] Categorical hues in slot order; ≤ 3 series in any all-pairs form
- [ ] Colors bound to entities via dicts, never positional index
- [ ] Palette validated by running `guide/validate_palette.py`
- [ ] Sub-3:1 fills (aqua / yellow / magenta) carry visible direct labels
- [ ] Delta charts colored by **verdict**, with the verdict spelled out in text

Marks
- [ ] Gridlines solid hairlines; top/right spines hidden
- [ ] Surface-colored gap between adjacent fills; no black borders
- [ ] Legend present for every ≥ 2-series chart, absent for single-series
- [ ] No legend covering a mark
- [ ] Direct labels selective; none clipped or overlapping
- [ ] Axis labels carry units; "lower is better" stated where relevant
- [ ] Percentage axes have a fixed ceiling, not autoscale

Delivery
- [ ] Notebook executes top-to-bottom with **zero** cell errors
- [ ] **Every output image opened and visually inspected**
- [ ] Every required log feeds at least one chart
      ([07 coverage check](07-chart-catalogue.md))
- [ ] Manifest cell lists every written file
- [ ] Builder script regenerates the whole notebook from scratch

---

## Phase 8 · Acceptance test

A port is done when all of these hold:

1. `validate_results.py` exits 0 on a fresh run directory.
2. The console summary is byte-identical in shape to
   [02 §7](02-throughput.md) — same labels, same column alignment.
3. Two runs of the **same** configuration produce throughput within a few percent of each
   other. Wild variance means the measurement, not the system, is unstable.
4. `Σ service` matches `busy_s`, and no utilization exceeds 100%.
5. Deleting the results directory and re-running reproduces it completely — nothing is
   carried over.
6. The notebook from [08](08-build-pipeline.md) runs against your results **with only
   the paths and label maps changed**. If it needs chart-code changes, the format is not
   conformant yet — go back to [01](01-result-format.md).

Point 6 is the whole purpose of this guide. If it fails, something in Phase 2–5 drifted.

---

## Common porting failures, ranked by how often they happen

| # | Failure | Caught by |
|---|---|---|
| 1 | Two stages publish per unit → rate 2× real | validator: group `done` > `SYSTEM` |
| 2 | Column named `agg`/`max`/`count` → silent empty filter | chart missing a series |
| 3 | Publishing from a non-channel thread | random broker errors |
| 4 | No final drain after thread join → 2–5 units short | validator: line-count mismatch |
| 5 | Server exits at first-stage shutdown → undercount | no summary printed |
| 6 | Overlapping busy intervals → utilization > 100% | validator: hard error |
| 7 | Averaged per-device percentiles | validator: `p50 > p95` |
| 8 | Queue not purged → stale units inflate the count | run twice, compare counts |
| 9 | Per-group START → group rates unrelated to system | validator: span mismatch |
| 10 | Dual-axis chart because two measures differ in scale | [07 C5](07-chart-catalogue.md) |
| 11 | Delta chart colored by sign, not verdict | [07 C10](07-chart-catalogue.md) |
| 12 | Conditionally-truncated file leaks into an archive | stale data in a tagged archive |
