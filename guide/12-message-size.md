# 12 · Message size — what one worker actually puts on the wire

Every other measurement in this guide describes *time*. This one describes *bytes*: how
large the payload a worker hands to the transport is, measured on the worker, once per
published message.

It is the number that makes three other files readable. Utilization ([03](03-utilization.md))
says a worker was busy; message size says whether it was busy computing or busy shipping.
The infrastructure host's memory curve ([11](11-broker-ram.md)) shows the queue filling;
message size × queue depth says whether that is the payload or something else. And a
`send`-dominated free-time profile ([10 §3](10-free-time.md)) means nothing until you know
how many bytes each send moved.

> **Instance.** In the reference project the payload is one batch's intermediate feature
> map published to RabbitMQ, and the files are `message_size.log` /
> `message_size_series.log`. Substitute your own noun; nothing else changes.

---

## 1 · One worker measures, and the server picks which

The measurement is **not** fleet-wide. Exactly one worker records it, and that worker is
chosen by the server: the **first worker that registered at the first stage**.

Registration order is the right selector because it needs no configuration, is stable
within a run, and is already known to the server before it dispatches any work. "First at
the first stage" is what makes the number meaningful: the first stage is the one whose
output crosses the network, and every worker in a group publishes the same payload shape
from the same split point — so nine workers measuring produce one number nine times, at
nine times the cost.

**The worker never decides this for itself.** The flag travels in the dispatch message
([README, invariant 9](README.md)), so the job cannot land on two machines or on none. A
worker that read "am I the one?" from its own config would be one stale file away from a
run where every edge measured, or none did, and the summary line looks identical either
way.

The cost on the measured worker is one integer append plus one line of I/O per message.
On every other worker it is one attribute lookup per message — the flag arrives false and
the recorder returns immediately.

---

## 2 · Measure before the publish, not after

```
size = len(serialized_payload)      ← the measurement
transport.publish(serialized_payload)
```

Both orderings look equivalent until the transport blocks. A broker at its high-water
mark stops accepting; a saturated link stalls mid-write. Those are exactly the runs the
measurement exists to explain, and measuring after the call means the sample for the
message that stalled is written late — or, if the publish raises, never written at all.
The size is known before the call, so record it before the call.

Measure the **serialized** bytes — what the transport is handed, after your own
compression and framing, before the transport's own. That is the number that occupies
memory on the infrastructure host and moves over the link. A pre-serialization tensor
size is a different quantity and must not be logged under the same key.

---

## 3 · Write it locally as the run goes, ship it at the end

The same both-copies rule as free time ([10](10-free-time.md)), for the same reasons:

- **Local file, appended live.** The worker's own record, and the only copy that survives
  a broker or server problem. One line per message, flushed — a run that dies still leaves
  every sample it took.
- **One report at finish**, parked on the measurement queue until the server's shutdown
  collection drains it. The worker finishes before the server does; the queue absorbs the
  gap.

Sample times travel as **offsets from that worker's first publish**, never as absolute
device timestamps. The server writes them into a shared result file, and every timestamp
in a shared file is the server's own clock ([README, invariant 1](README.md)). Offsets
are computed within one device, so they are exact, and they locate a sample in the run
without ever being compared against another machine's clock.

A long run must not turn the report into a multi-megabyte message. Cap the shipped series
and **decimate evenly** rather than truncating, so the series still spans the whole run.
The summary statistics MUST be computed over the **full** sample set, not the decimated
one — decimation may coarsen a plot, never a number.

---

## 4 · The files

Two files, one feature — emit both or neither ([01 §2](01-result-format.md), files
13–14).

**`message_size.log`** — one line per measured worker (so, normally, exactly one line):

```
1786366279200770600 client=machine-2 role=edge machine=machine-2 cluster=intermediate_queue_0 mode=split splits=5 compress=on num_bit=8 batch_size=32 n=504 total_mb=19657.464 mean_mb=39.003 p50_mb=39.022 p95_mb=39.613 max_mb=40.098 min_mb=37.909 span_s=714.260 rate_mb_s=27.521 per_frame_mb=1.2188
```

| Key | Meaning |
|---|---|
| `n` | messages published by this worker |
| `total_mb` | bytes this worker put on the wire, over the whole run |
| `mean_mb`, `p50_mb`, `p95_mb`, `max_mb`, `min_mb` | per-message size. Percentiles **nearest-rank over the raw samples**, no interpolation — same rule as latency ([04](04-latency.md)), so every number printed is a size that actually occurred |
| `span_s` | first publish → last publish, on that worker's clock |
| `rate_mb_s` | `total_mb / span_s` — this worker's egress, the number to compare against its share of the link |
| `per_frame_mb` | `mean_mb / unit_size`, the size normalised per item so runs with different unit sizes compare |
| context keys | whatever determines the size: compression on/off and its parameter, the split point, the mode. A size without them is unreproducible |

**`message_size_series.log`** — the plottable series, one line per published message:

```
1786366279200770600 client=machine-2 cluster=intermediate_queue_0 i=0 t_offset_s=0.000 batch_id=0 bytes=38897647 mb=38.898
```

| Key | Meaning |
|---|---|
| col 1 | ns-epoch arrival of the report at the **server**, identical on every line of a report |
| `i` | sample index |
| `t_offset_s` | seconds since **that worker's own first publish** |
| `bytes` | exact integer — the authoritative value |
| `mb` | the same number in MB, for readers that plot without converting |

Both `bytes` and `mb` on purpose: `bytes` is exact and `mb` keeps the file readable, and
a reader that rounds its own MB from `bytes` still agrees with the summary.

Size keys use **MB = 10⁶ bytes**, matching [11](11-broker-ram.md) so a payload size and a
host's memory growth can be compared without a unit conversion in between.

---

## 5 · Reading it

1. `mean_mb` × the queue depth cap → the RAM the infrastructure host must hold. Compare
   against `DELTA peak_over_start_mb` in [11](11-broker-ram.md). They should agree; when
   the host's peak is far larger, something is buffering that you did not account for.
2. `rate_mb_s` × the number of workers sharing the link → offered load. Against measured
   link capacity, this is the first check on whether the network or the compute is the
   bottleneck.
3. `max_mb` against the transport's message-size limit. A run that dies at a deeper split
   point usually died here, and the margin is visible before the failure.
4. `p95_mb / p50_mb` — payload variance. A wide spread on a fixed split point means the
   payload depends on content (compression ratio varying with the scene), which makes any
   single-number bandwidth estimate optimistic.
5. The series against `batch_done_ns.log` on one x axis: message size flat while
   throughput falls is a transport problem; message size climbing is a workload problem.

---

## 6 · Invariants

1. **Exactly one worker measures**, chosen by the server, told via the dispatch message.
   Never self-selected, never all of them.
2. **The size is recorded before the publish call**, so a blocked or failed transport
   still leaves the sample behind.
3. **Serialized bytes**, the value the transport is handed — not a pre-serialization
   object size.
4. **Local file live, report at shutdown.** The local copy survives a crash; the report
   does not need to.
5. **Offsets, not device timestamps**, in anything the server writes to a shared file.
6. **Statistics over all samples, decimation only for the shipped series.**
7. **Telemetry never kills the run** ([README, invariant 7](README.md)). A failed write
   degrades to one warning and disables the recorder — one warning, not one per message.
8. **When the feature is off, the server's collector skips too**
   ([README, invariant 10](README.md)), and the files still exist, empty.
