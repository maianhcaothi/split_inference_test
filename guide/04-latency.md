# 04 · Latency

How to produce `latency_group.log` ([01 §3.6](01-result-format.md)).

**The mechanism in one line:** devices ship **raw per-unit latency samples** to the
server; the server pools them per scope and takes **nearest-rank** percentiles.

---

## 1 · Why raw samples and not per-device summaries

**Percentiles cannot validly be averaged.** The mean of two devices' p95 values is not
the p95 of their combined population — it has no statistical meaning at all. A device
that processed 5 units and one that processed 500 would contribute equally.

So devices ship arrays, the server concatenates, and the percentile is taken once over
the pool. The payload cost is a few thousand floats per device at shutdown, which is
nothing compared to being able to trust the number.

**Nearest-rank, no interpolation:** every value printed is a latency that was actually
observed on some unit. An interpolated p95 is a number that never happened.

```python
def pct(sorted_samples, q):
    """Nearest-rank percentile. q in [0, 100]."""
    if not sorted_samples:
        return None
    k = max(1, math.ceil(q / 100.0 * len(sorted_samples)))
    return sorted_samples[k - 1]

s = sorted(pooled_samples_ms)
stats = dict(n=len(s), mean_ms=sum(s)/len(s),
             p50_ms=pct(s, 50), p95_ms=pct(s, 95), max_ms=s[-1])
```

---

## 2 · The three kinds — do not confuse them

| `kind` | Span | Clock | Exact? | Reported per |
|---|---|---|---|---|
| `service` | the device's own `get input → output` | one | **yes** | group × role |
| `pipeline` | unit ready → published, incl. hand-off queue waits | one | yes | group × role |
| `e2e` | first stage start → completing stage output | **two machines** | indicative | group (and SYSTEM) |

### 2.1 `service` — the only latency comparable against utilization

`service` samples are exactly the `output − get input` intervals that
[03-utilization.md](03-utilization.md) sums into `busy_s`. Therefore:

```
Σ service samples for a role  ==  that role's busy_s in utilization_group.log
```

This is a **conformance check**. If it does not hold, one of the two measurements is
instrumented at the wrong point.

Use `service` to answer "how fast is this device?".

### 2.2 `pipeline` — includes buffering, so it measures the queue, not the device

`pipeline` additionally contains the wait in the in-process hand-off queues. It therefore
scales with **queue depth**, not with device speed.

Measured on a real run with a hand-off queue depth of 4:

| | head devices |
|---|---|
| `service` | ≈ 11.6 s |
| `pipeline` | ≈ 57.6 s |

A 5× gap that is **pure buffering**. Nothing about the device changed.

> **If `pipeline ≫ service`, lower the queue depth.** Throughput does not depend on it —
> you are only adding latency. This is the single most actionable reading in the file.

`pipeline` explains `e2e`; it does not measure how fast a device is.

### 2.3 `e2e` — spans two machines, so treat it as indicative

`e2e` runs from the first stage starting a unit to the completing stage emitting its
output. That is two clocks by definition, so it **inherits any offset between them**.

- Report it, but label it. Do not present it as exact.
- Only the **completing** stage reports it, so there is exactly one `e2e` series per
  group, plus a `SYSTEM` line pooled across groups.
- `role=` is absent on `e2e` lines — it is not a property of one role.

`e2e` is the number a user experiences. `service` is the number an engineer optimizes.
Report both; never substitute one for the other.

---

## 3 · Collection

Devices accumulate raw samples during the run and ship them with (or alongside) their
utilization report at shutdown — same dedicated-queue reasoning as
[03 §4](03-utilization.md): the publisher and the consumer need not be alive at the same
time.

```text
{
    "action": "UTILIZATION",
    "client_id": <uuid>,
    "role": "edge",
    "cluster": "intermediate_queue_0",
    # ... utilization fields ...
    "service_ms":  [11.4, 12.0, ...],     # raw, one per unit
    "pipeline_ms": [57.2, 58.1, ...],
    "e2e_ms":      [69655.6, ...],        # completing stage only
}
```

Server side, at shutdown:

1. Bucket samples by `(cluster, role, kind)` and by `(cluster, kind=e2e)`.
2. Concatenate — **never** pre-reduce per device.
3. Sort once per bucket, take nearest-rank percentiles.
4. Emit one line per bucket, plus the `SYSTEM` `e2e` line pooled over all groups.

Emit `n=` on every line. A percentile over 3 samples is not a percentile, and `n` is how
the reader knows.

---

## 4 · Line format

```
<ns> cluster=<g> role=<r> kind=service  n=336 mean_ms=3332.833 p50_ms=3470.922 p95_ms=3966.246  max_ms=4367.941
<ns> cluster=<g> role=<r> kind=pipeline n=336 mean_ms=3410.112 p50_ms=3502.664 p95_ms=4038.901  max_ms=4501.220
<ns> cluster=<g> kind=e2e n=336 mean_ms=69655.623 p50_ms=69379.207 p95_ms=102639.355 max_ms=111760.937
<ns> SYSTEM kind=e2e n=504 mean_ms=75320.528 p50_ms=77109.393 p95_ms=109986.265 max_ms=120801.164
```

All times in **milliseconds**, `{:.3f}`. Convert to seconds at the chart, not in the log
([07-chart-catalogue.md C5](07-chart-catalogue.md)).

Required keys on every line: `kind`, `n`, `mean_ms`, `p50_ms`, `p95_ms`, `max_ms`.
The validator enforces `p50 ≤ p95 ≤ max` — a violation means percentiles were computed
on pre-averaged data.

---

## 5 · Reading the numbers

| Observation | Means |
|---|---|
| `pipeline` ≫ `service` | buffering — lower the hand-off queue depth |
| `p95` ≫ `p50` | bursty arrivals or a straggler device; check per-device utilization |
| `max` ≈ `p95` | a smooth tail — the max is representative |
| `max` ≫ `p95` | one pathological unit; look for a retry or a GC pause |
| `e2e` ≫ Σ of stage `service` | queue waits dominate, not compute |
| `service` sum ≠ `busy_s` | **measurement bug** — the two are instrumented at different points |
| tiny `n` on a percentile line | the percentile is noise; report it but do not act on it |

---

## 6 · Pitfalls

| Pitfall | Symptom | Fix |
|---|---|---|
| Averaging per-device percentiles | plausible-looking but meaningless numbers | ship raw samples, pool, then reduce |
| Interpolated percentiles | a p95 no unit ever experienced | nearest-rank |
| Reporting only `e2e` | cannot tell compute cost from queue wait | always report `service` too |
| Reporting only `service` | latency looks fine while users wait | always report `e2e` too |
| Mixing ms and s in one file | silent 1000× errors in charts | everything is `_ms` in the log |
| Omitting `n` | percentiles over 3 samples read as authoritative | emit `n` on every line |
| `role=` on an `e2e` line | double-counts when charts group by role | `e2e` is per group, never per role |
