# 10 · Free time — when devices and machines were doing nothing

How to produce `free_time.log`, `free_time_group.log` and `free_time_series.log`
([01 §3.8–3.10](01-result-format.md)).

**The mechanism in one line:** every lane of a device records the intervals in which it
was doing real work; at the end the device **merges** those intervals and reports the
wall clock that is left over.

```
free = (end − start) − | union of every lane's busy intervals |
```

**Free time is the time a device did nothing at all** — no reading input, no compute, no
serialize/compress, no send, no receive, no decompress, no postprocess, no bookkeeping.
It is a property of the *device*, not of a stage.

---

## 1 · Why this is not utilization, and not `1 − utilization`

[03-utilization.md](03-utilization.md) measures `busy / total` over each unit's
`get input → output` window on **one** lane. That is the right number for "how loaded is
this stage", and the wrong number for "was this machine idle", for two reasons:

| | utilization | free time |
|---|---|---|
| scope | one lane's unit window | every lane, whole run |
| a wait *inside* the unit window (back-pressure, a full hand-off queue) | counts as **busy** | counts as **free** |
| work done on the *other* lane (capture, encode, publish) | counts as **nothing** | counts as **busy** |

So `free% + utilization% ≠ 100%`, and neither number can be derived from the other.
Emit both. When they disagree loudly the gap itself is the finding: a device at 40%
utilization and 3% free time is not idle — it is doing work the unit window never saw.

> **Never sum per-stage timers to get busy time.** A pipelined device runs its stages
> concurrently, so the sum can exceed the wall clock outright (this is normal, not a
> bug: two lanes each 90% busy sum to 180%). It also silently misses the gaps *between*
> stages, which is precisely where free time lives. Merge intervals; do not add
> durations.

---

## 2 · Recording, on the device

### Lanes

A **lane** is a thread. Each lane records its own intervals; a device is free only at
instants when **no lane** is inside one. Use the thread identity automatically — a lane
should never have to be named by the caller, or the instrumentation rots the moment a
stage moves to a different thread.

### Two span types

| Type | Counts as | Recorded around |
|---|---|---|
| **work** | busy | any real operation: read input, compute, encode, send, receive, decode, postprocess, write metrics |
| **wait** | free | any block: waiting for input, a full downstream queue, a back-pressure stall, an idle poll |

Work spans define the answer. Wait spans do **not** — they only *explain* it. A moment
covered by no wait span is still free; it lands in `unaccounted`, which is a useful
signal in itself (it is the overhead between instrumented stages).

Classify by what the device is doing, not by which subsystem it belongs to. A broker
round trip to read a queue depth is work; the stall that follows it is not.

### Retroactive classification

Some calls are only classifiable after they return — a non-blocking receive is *work*
when it yields a unit and *free* when the queue was empty. Take a timestamp before the
call and record the span after it, rather than guessing:

```python
t0 = ft.now()
frame = channel.get(queue)
if frame:
    ft.add_work("recv", t0)
else:
    poll_control_queue(); sleep(0.5)
    ft.add_wait("input", t0)    # the empty get, the control poll and the sleep are one idle stretch
```

### Cost control

Two details keep this off the hot path:

- **Coalesce wait spans.** A 2 ms poll loop produces ~500 intervals per idle second.
  Extend the lane's open span instead of appending when the gap is under ~1 ms, so a
  long stall stays one interval. Work spans MUST NOT be coalesced with a tolerance —
  busy has to stay exact.
- **Merge periodically.** Fold pending intervals into the merged list every few thousand
  spans, so memory is bounded by the number of *disjoint* busy periods rather than by
  the number of recorded operations.

Use a monotonic clock (`perf_counter_ns`) for the intervals and convert to epoch only on
export. A clock step mid-run must not be able to produce a negative interval.

---

## 3 · Reporting, on the server

Each device publishes one report to a dedicated queue when it finishes; the server
drains that queue at shutdown, exactly like the utilization reports
([03 §5](03-utilization.md)). The report carries:

| Field | Why it is in there |
|---|---|
| `span_ns`, `busy_ns`, `free_ns` | the answer, plus the two numbers it came from |
| per-kind busy sums | where the busy time went. These **overlap** — they sum to ≥ the merged busy total, and that is expected |
| per-reason free sums | why the device was free. Attributed in a fixed priority so they sum to **exactly** `free_ns` |
| per-lane busy sums | which thread carried the work |
| the merged **busy intervals** | so the server can union the device processes that share one machine (§4) |
| a bucketed free series | the plottable "when was it free" curve |
| host idle % | the OS's own idle/total CPU accounting, across *all* processes |

### Attributing free time to reasons

Several lanes can be waiting for different things at the same instant. Subtracting each
reason's intervals in a fixed priority order — each one excluding what busy time and
earlier reasons already claimed — makes the parts sum to the whole with no double
counting. Anything left over is `unaccounted`. Publish the priority order; it is part of
the definition, not an implementation detail.

---

## 4 · Devices vs machines

Several device processes can share one host. That host is free only when **none** of
them is working, which cannot be recovered from their individual ratios: two processes
that are each 50% free can keep a machine 100% busy by interleaving.

So the server unions the busy intervals of every device on a machine and measures what
is left. This is the one place intervals from different processes are compared, and it
is safe for exactly one reason: **they share a clock**. Intervals are never compared
across machines.

Shipping intervals has a size bound. Cap the list and close the **smallest gaps** first
until it fits, then report the swallowed total as `merge_slop_s`. That biases the answer
toward *less* free time — the safe direction — and states its own error bar.

Also report `host_idle`, the OS-level idle share for the machine. The two numbers answer
different questions and disagreeing is informative:

| Pipeline free | Host idle | Reading |
|---|---|---|
| high | high | genuinely spare capacity — add work, or use fewer machines |
| high | low | something else on the box is eating the CPU (a co-tenant VM, another run) |
| low | high | the pipeline is blocked on I/O or the network, not on compute |
| low | low | saturated |

Report the server's own host idle too, even though it runs no pipeline stage. A fleet
view that omits the machine holding the controller is not a fleet view.

---

## 5 · The three files

See [01 §3.8–3.10](01-result-format.md) for the normative line formats.

| File | Granularity | Answers |
|---|---|---|
| `free_time.log` | one line per device | which device was idle |
| `free_time_group.log` | per group, per group/role, per **machine**, `SYSTEM` | which group / machine was idle, and why |
| `free_time_series.log` | one line per device per bucket | **when** each device was idle |

Devices also keep their own copy locally. Do this even though the server has everything:
the local file survives a broker or server failure, and it is the artifact you read when
one device behaves differently from its peers.

---

## 6 · Plotting

`free_time_series.log` is a `(device, time-bucket, free%)` table — a heat-map, one row
per device, time on the x axis, free% as the color
([07-chart-catalogue.md](07-chart-catalogue.md) for the palette rules). Read it against
`batch_done_ns.log` on the same axis: a band of free time on one device that lines up
with a throughput dip names the stage that stalled.

The summary numbers plot as a stacked bar per device — busy split by kind, free split by
reason — where every bar is the same height because every bar is that device's whole
run. That is the chart that makes "the edges are waiting for the network, the clouds are
waiting for the edges" a picture instead of a paragraph.

**Sequence to read first, always:**

1. `SYSTEM free=` in `free_time_group.log` — how much of the fleet was idle at all.
2. The `MACHINE` lines — whether idleness is concentrated on specific hosts.
3. The `FREE reason=` lines — whether it is starvation (`input`), congestion
   (`backpressure` / `downstream`), or overhead (`unaccounted`).
4. Only then the per-device lines.

Free time is a **capacity** measurement, not a performance one. A fleet at 60% free is
not slow; it is three times larger than the workload needs.

---

## 7 · Self-check

Four invariants hold by construction. They are cheap to assert and each one fails loudly
when the implementation is wrong in the way this measurement is usually wrong:

| Invariant | What its failure means |
|---|---|
| `busy_ns + free_ns == span_ns`, exactly | intervals escaped the run window, or the clip step is missing |
| `Σ free_reasons == free_ns`, exactly | attribution double-counts (reasons overlap) or leaks (`unaccounted` not emitted) |
| `Σ per-kind durations > merged busy` on a pipelined worker | if equal, lanes are being **summed** instead of merged — the error this whole method exists to prevent |
| `busy_ns ≤ span_ns` | same bug, caught from the other side |

Test them on a synthetic device: run two threads doing known-length work that overlaps in
time. The sum of the per-kind timers will be about twice the merged busy interval, and
the merged value is the one that must appear in the report. A run where those two numbers
agree is a run where the merge is silently a sum — and free time will read far too low
with nothing else looking wrong.
