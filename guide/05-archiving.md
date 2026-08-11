# 05 · Archiving runs

A results directory describes exactly one run ([01 §4](01-result-format.md)) and is
**truncated by the next one**. Archiving is what makes runs comparable weeks later.

---

## 1 · Layout

```
results/
├── <run-id>/
│   ├── batch_done_ns.log
│   ├── group_rate_ns.log
│   ├── group_rate.log
│   ├── utilization.log
│   ├── utilization_group.log
│   ├── latency_group.log
│   ├── events_ns.log          (only if the feature that writes it ran)
│   ├── broker_ram_ns.log      (only if the infra-host sampler ran — 11)
│   ├── broker_ram.log         (only if the infra-host sampler ran — 11)
│   ├── message_size.log       (only if a worker measured payload size — 12)
│   ├── message_size_series.log (only if a worker measured payload size — 12)
│   └── config.yaml            THE CONFIG THAT PRODUCED THESE NUMBERS
└── visual/
    └── <Name> Visualization.ipynb
```

Run id: `results_<MMDD>_<HHMM>_<tag>`, or `<date>/<variant>` when you are comparing a
small fixed set of configurations. Both appear in the reference project:
`results/July27th/dynamic` and `results/July27th/split` are two variants of one
experiment, which is the shape the comparison charts assume.

---

## 2 · The tag

The tag names **which configuration produced the run**. Pick a closed vocabulary and put
the selection logic in one place:

| Tag | Condition (reference instance) |
|---|---|
| `only_cloud` / `only_edge` | single-stage mode with that stage doing everything |
| `dynamic` | split mode **and** the adaptive controller enabled |
| `split` | split mode with a fixed configuration |

A tag is a label, not a description. If two runs could carry the same tag but differ in a
way that matters, the difference belongs in `config.yaml` — which is why it is archived.

---

## 3 · Rules

- **Copy, do not move.** The live log directory keeps its own copies where every existing
  reader expects them; the next run truncates them itself.
- **Skip empty files.** A zero-length log must never be archived as a misleading result.
- **Collision-safe.** Two runs finishing in the same minute get `…-2`, `…-3`.
- **Archive the config.** Without it the numbers are unreadable in a month — you will not
  remember the unit size, the split point, or the queue depth.
- **Archive after the last shutdown pipeline has written**, so the snapshot is complete.
- **Failure is non-fatal.** A filesystem problem prints a warning; the run still closes
  its connections and exits cleanly.
- **Warn on an empty archive.** If every log was missing or empty, say so loudly — that
  is a run that produced nothing, and it should not look like a success.

---

## 4 · The stale-file trap

An archiver that copies every non-empty file will happily copy a file **the current run
never wrote**.

This bites when a file is truncated *conditionally*. In the reference implementation
`cut_change_ns.log` is truncated only when the adaptive controller is enabled — so a
later non-adaptive run leaves the previous run's file in place, and the archiver stamps
it into a `split`-tagged archive where it does not belong.

**Two fixes, pick one:**

1. **Truncate every result file unconditionally at startup** ([01 §4](01-result-format.md)
   requires this). Preferred — it makes the invariant "everything here is from this run"
   true by construction.
2. Give the archiver an explicit manifest of files this run is expected to produce, and
   copy only those.

Do not rely on mtime. It is close enough to fool you and wrong often enough to matter.

---

## 5 · Files that are *not* results

Some artifacts are scratch space or per-device output, not run results. Keep them out of
the archive, and clear them at startup so leftovers cannot poison the next run:

| Artifact | Written by | Why it is not a result |
|---|---|---|
| per-device raw metric CSVs | each device | superseded by the pooled logs; huge |
| streamed output records | the producing stage | data, not measurement |
| any write-once cache directory | workers | **leftovers "win" forever and silently poison every future run** |

That last one is the dangerous case. A write-once cache keyed by item id, not cleared
between runs, means run N+1 silently reuses run N's outputs for every key they share.
The numbers look plausible. Clear these directories centrally at startup — in the
component that starts **once**, not per worker, or late-starting workers will wipe files
that earlier workers are already writing.

---

## 6 · Comparing archived runs

The comparison charts ([07-chart-catalogue.md](07-chart-catalogue.md)) assume two or more
run directories with **identical workloads and different configurations**. Before
charting a comparison:

```bash
python guide/validate_results.py results/<date>/<run-a>
python guide/validate_results.py results/<date>/<run-b>
```

then confirm the workloads really match:

```bash
# unit counts must be identical, or you are comparing different amounts of work
grep SYSTEM results/<date>/*/group_rate.log

# and diff the configs to see exactly what varied
diff results/<date>/<run-a>/config.yaml results/<date>/<run-b>/config.yaml
```

If the unit counts differ, the runs are not comparable and no chart will say so — it will
just draw two bars. Check first.

Also diff the *values* of any metric you expect to be configuration-independent
([08-build-pipeline.md §4](08-build-pipeline.md) shows how to assert this in the
notebook). A metric that is bit-identical across runs must be charted as one series, not
two overlapping ones.
