# Server Results Guide

Everything the **Server** writes to disk during a run: what each file is, who writes it,
when, the exact line format, and how to read it.

All paths are relative to `log-path` in [config.yaml](config.yaml) (currently `.`, i.e. the
project root). Source references point at [src/Server.py](src/Server.py).

---

## 1. Lifecycle of a run

```
__init__            truncate all result logs  +  delete stale metrics/pred files
   │
START broadcast     _fps_start_t = now                          (t0 for SYSTEM FPS)
   │
run                 batch_done_ns.log      ← every DONE          (live, append)
                    fps_cluster_ns.log     ← every DONE          (live, append)
                    cut_change_ns.log      ← every cut change     (adaptive only)
   │
all edges done      FPS drain watcher (grace + hard cap)
   │
_finish_fps         console FPS summary  +  fps_cluster.log
   │
_collect_utilization        utilization.log
   └ _report_cluster_util_latency   utilization_cluster.log, latency_cluster.log
   │
_collect_map_pred   map/pred_collected/<cluster>/*.txt   (unpacked zips)
   ├ pipeline 1     map_window.log
   └ pipeline 2     map.log
   │
_archive_results    results/results_<MMDD>_<HHMM>_<tag>/   (copy of everything + config.yaml)
   │
connection.close() → exit
```

Two properties hold for every file below:

- **Truncated at startup**, in [Server.py:229-266](src/Server.py#L229-L266) — a new run never
  mixes its numbers with the previous one. (One exception: `cut_change_ns.log`, see §2.9.)
- **Timestamped with `time.time_ns()` on the server's clock only** — device clock skew never
  affects any recorded time. The single exception is the E2E latency samples, which span two
  machines by definition (§2.7).

---

## 2. The nine result files

| File | Written by | When | Granularity |
|---|---|---|---|
| `batch_done_ns.log` | `on_fps` | live | one line per finished batch |
| `fps_cluster_ns.log` | `on_fps` | live | one line per finished batch, cluster-tagged |
| `fps_cluster.log` | `_report_cluster_fps` | shutdown | one line per cluster + `SYSTEM` |
| `utilization.log` | `_collect_utilization` | shutdown | one line per device |
| `utilization_cluster.log` | `_report_cluster_util_latency` | shutdown | per cluster, per cluster/role, + `SYSTEM` |
| `latency_cluster.log` | `_report_cluster_util_latency` | shutdown | per cluster/role (service) + per cluster (e2e) + `SYSTEM` |
| `map_window.log` | `_map_pipeline_window` | shutdown | one line per sliding window |
| `map.log` | `_collect_map_pred` | shutdown | 2 lines per cluster + 2 `OVERALL` |
| `cut_change_ns.log` | `_broadcast_setcut` | live | one line per cut change (adaptive only) |

### 2.1 `batch_done_ns.log` — system throughput series

Written in [Server.py:379-383](src/Server.py#L379-L383). One line per `DONE` message arriving
on `fps_queue`; the **arrival is the event**, the body is not used for timing.

```
1785127877009606331
1785127877009606331 24.13
```

| Field | Meaning |
|---|---|
| col 1 | ns-epoch arrival time of the DONE (server clock) |
| col 2 | smoothed `window_fps` over the last 16 batches — *absent* for the first 15 lines |

`window_fps = (W-1) * batch_size / (t[-1] - t[-W])`, `W = 16`. This is the system-wide
live throughput series — the plottable "FPS over time" curve. Format is deliberately
two-column and stable, so existing parsers keep working.

### 2.2 `fps_cluster_ns.log` — per-cluster throughput series

Written in [Server.py:402-406](src/Server.py#L402-L406). Same arrivals as above, but bucketed
by the cluster id carried in the DONE body.

```
1785127877009606331 cluster=intermediate_queue_0 done=1
1785127901123456789 cluster=intermediate_queue_0 done=16 window_fps=21.44
```

| Field | Meaning |
|---|---|
| `cluster` | producing cluster's queue name; `unknown` if a producer sent a bare `DONE` |
| `done` | running count of DONEs *for that cluster* |
| `window_fps` | that cluster's own 16-batch window fps — absent until it has 16 DONEs |

The system list in §2.1 stays authoritative: a mis-tagged DONE can shift this breakdown but
can never change the system total.

### 2.3 `fps_cluster.log` — final throughput summary

Written once by `_report_cluster_fps` in [Server.py:490-492](src/Server.py#L490-L492).

```
1785128504858762215 cluster=intermediate_queue_0 fps=18.131 steady_fps=18.491 done=336 frames=10752 share=66.7%
1785128504858762215 cluster=intermediate_queue_1 fps=8.407 steady_fps=8.740 done=168 frames=5376 share=33.3%
1785128504858762215 SYSTEM fps=25.220 done=504 frames=16128 clusters=2
```

| Field | Meaning |
|---|---|
| `fps` | whole-run rate: `frames / (START → that cluster's last DONE)`. **Additive** — cluster values sum to ≈ the SYSTEM value |
| `steady_fps` | `(n-1)*bs / (first DONE → last DONE)` — drops warm-up; the fair number for comparing clusters |
| `done` / `frames` | batches, and `batches × batch_size` |
| `share` | that cluster's % of all batches — the quickest read on whether the Hungarian assignment balanced the clusters |

The SYSTEM line has no `steady_fps`; system steady-state and the biased `ref mean` are printed
to the console only ([Server.py:516-528](src/Server.py#L516-L528)).

### 2.4 `utilization.log` — per-device busy ratio

Written in [Server.py:570-571](src/Server.py#L570-L571) as each device's `UTILIZATION` report is
drained from `utilization_queue` at shutdown.

```
1785128504862965418 client=7e3dd352-8b9a-4d6b-8173-412705d9bbe3 role=edge packages=56 busy_s=176.760 total_s=467.394 utilization=37.82%
```

| Field | Meaning |
|---|---|
| col 1 | ns-epoch **arrival** of the report (not a device timestamp) |
| `client` | device UUID |
| `role` | `edge` / `cloud` |
| `packages` | batches this device processed |
| `busy_s` / `total_s` | seconds computing vs. seconds alive, from the device's own timing log |
| `utilization` | `busy/total` — a low edge value with a high cloud value means the cut is too shallow |

If not every registered client reports within `timeout_s` (default 30 s), the collection is
partial, a yellow warning prints, and the run still shuts down cleanly.

### 2.5 `utilization_cluster.log` — utilization rolled up per cluster

Written in [Server.py:694-702](src/Server.py#L694-L702). Three line kinds:

```
1785128504874248492 cluster=intermediate_queue_0 ALL devices=8 utilization=55.06% utilization_mean=53.29% busy_s=2348.297 total_s=4264.621 packages=672
1785128504874248492 cluster=intermediate_queue_0 role=cloud devices=2 utilization=93.44% busy_s=1119.832 total_s=1198.399 packages=336
1785128504874248492 SYSTEM devices=11 clusters=2 utilization=... utilization_mean=... busy_s=... total_s=...
```

- `utilization` — **pooled**: `Σbusy / Σtotal`, weighting each device by how long it ran.
- `utilization_mean` — plain mean of the per-device ratios. Present on `ALL`/`SYSTEM` lines
  because a pooled number can hide one idle device inside a group of busy ones. When the two
  diverge, the group is imbalanced.

This groups; it does not replace `utilization.log`, which keeps the per-device view.

### 2.6 `latency_cluster.log` — latency distributions

Written alongside the file above, from pooled raw samples shipped by the devices.

```
1785128504874248492 cluster=intermediate_queue_0 role=cloud kind=service n=336 mean_ms=3332.833 p50_ms=3470.922 p95_ms=3966.246 max_ms=4367.941
1785128504874248492 cluster=intermediate_queue_0 role=cloud kind=pipeline n=336 mean_ms=3410.112 p50_ms=3502.664 p95_ms=4038.901 max_ms=4501.220
1785128504874248492 cluster=intermediate_queue_0 kind=e2e n=336 mean_ms=69655.623 p50_ms=69379.207 p95_ms=102639.355 max_ms=111760.937
1785128504874248492 SYSTEM kind=e2e n=... mean_ms=... p50_ms=... p95_ms=... max_ms=...
```

| `kind` | Span | Clock |
|---|---|---|
| `service` | that device's own `get_input → output`, reported per role | one clock — exact |
| `pipeline` | batch ready → published, reported per role | one clock — exact |
| `e2e` | edge batch start → completing tier's output, one series per cluster | two machines — inherits any clock offset between them |

**`service` vs `pipeline` — do not confuse them.** `service` is the device's own compute: its
samples sum to exactly the `busy_s` on the matching `utilization_cluster.log` line, so it is the
only latency comparable against utilization. `pipeline` additionally contains the wait in the two
in-process hand-off queues, so on the edge it scales with `multithreading.queue_size`, not with
device speed. A run with `queue_size: 4` showed `service` ≈ 11.6 s while `pipeline` ≈ 57.6 s on the
same edges — a 5× gap that is pure buffering, and it is what `e2e` is built from. If `pipeline`
greatly exceeds `service`, lower `queue_size`; the throughput does not depend on it.

Percentiles are **nearest-rank over the sorted pooled samples** (no interpolation), so every
number printed is a latency that was actually observed. Devices ship raw samples rather than
summaries precisely so the percentiles can be computed over the pool — averaging per-device
percentiles is not a valid operation ([Server.py:578-593](src/Server.py#L578-L593)).

### 2.7 `map_window.log` — mAP pipeline 1 (sliding window)

Written in [Server.py:840-855](src/Server.py#L840-L855). A window of `map.window_batches`
(default 16) consecutive batches slides one batch at a time; each window gets a full mAP.

```
1785128505579200616 cluster=intermediate_queue_0 window=0 batches=0-15 frames=512 mAP50_95=0.0787 mAP50=0.1465
1785128505579200616 cluster=intermediate_queue_0 window=1 batches=1-16 frames=512 mAP50_95=0.0833 mAP50=0.1544
```

This is the **accuracy counterpart of `window_fps`** — the mAP-over-the-run series. Line it up
against `batch_done_ns.log` and `cut_change_ns.log` on a shared ns-epoch axis to see how
accuracy responded to a split-point change. If a run has fewer batches than the window size,
one window covers everything.

### 2.8 `map.log` — mAP summary (both pipelines)

Written in [Server.py:1019-1044](src/Server.py#L1019-L1044). Two lines per cluster plus one
`OVERALL` line per pipeline.

```
1785128505579200616 cluster=intermediate_queue_0 WINDOW mAP50_95=0.1004 mAP50=0.1857 (mean of 14 window(s) x 16 batches, step 1)
1785128505579200616 cluster=intermediate_queue_0 ALL    mAP50_95=0.0882 mAP50=0.1632 (905/905 GT frame(s) matched)
1785128505579200616 OVERALL WINDOW mAP50_95=0.1006 mAP50=0.1859 (avg over 2 cluster(s))
1785128505579200616 OVERALL ALL    mAP50_95=0.0883 mAP50=0.1634 (avg over 2 cluster(s))
```

| Tag | Pipeline | Definition |
|---|---|---|
| `WINDOW` | 1 | mean over all sliding windows. Over-weights frames that sit in more windows — use it for *trend*, not for a headline number |
| `ALL` | 2 | one metric fed every scorable frame of the run, each frame weighted exactly once. The accuracy counterpart of SYSTEM FPS — **use this as the headline number** |

`ALL`'s frame count is capped by ground truth: `map/label/` labels only the first N frames, and
workers stop writing pred files past that, so `905/905` means every labelled frame was scored.

`OVERALL` is always written when at least one cluster scored — even for a single cluster, where
it just repeats that cluster's numbers — so a parser can always find one authoritative line per
pipeline. The console summary omits it in that case.

Ground truth is the server's **own local** `map/label/frame_NNNNNN.txt`
(`class_id cx cy w h`, normalized to 640×640). Predictions come from the workers as
`class_id cx cy w h confidence`. If `map/label/` is missing, mAP is skipped with a warning.

**When mAP is missing entirely:** `map.log`/`map_window.log` stay empty and the console
distinguishes the two causes — *no scorable frames* (ran, found nothing) vs. *no COCO backend*
(`pip install faster-coco-eval`, see [Server.py:33-85](src/Server.py#L33-L85)). They need
completely different fixes.

### 2.9 `cut_change_ns.log` — adaptive split-point changes

Written in [Server.py:1340-1341](src/Server.py#L1340-L1341), only when `adaptive.enable: true`.

```
1785127901123456789 intermediate_queue_0: cut 11->12 deeper
```

`deeper` = the edge now runs more layers (cloud was the bottleneck); `shallower` = the cloud
takes more (cloud was starved).

> **Caveat.** This file is truncated at startup *only when the adaptive controller is enabled*
> ([Server.py:264-266](src/Server.py#L264-L266)). A later non-adaptive run therefore leaves an
> earlier dynamic run's file in place — and `_archive_results` copies any non-empty result file,
> so a `split`-tagged archive can end up carrying a stale `cut_change_ns.log`. Ignore that file
> in any archive whose tag is not `dynamic`.

---

## 3. `results/` — the run archive

`_archive_results` ([Server.py:1063-1116](src/Server.py#L1063-L1116)) runs once, after the last
shutdown pipeline has written its files, so the archive is a complete snapshot.

```
results/results_<MMDD>_<HHMM>_<tag>/
├── batch_done_ns.log
├── fps_cluster.log
├── fps_cluster_ns.log
├── latency_cluster.log
├── map.log
├── map_window.log
├── utilization.log
├── utilization_cluster.log
├── cut_change_ns.log        (adaptive runs only)
└── config.yaml              (the config that produced these numbers)
```

**Tag** — which configuration produced the run:

| Tag | Condition |
|---|---|
| `only_cloud` / `only_edge` | `experiment.enable: true` with that mode |
| `dynamic` | split mode **and** `adaptive.enable: true` |
| `split` | split mode with a fixed cut |

**Rules**

- **Copies, not moves** — `log-path` keeps its own copies where every existing reader expects
  them; the next run truncates them itself.
- **Empty files are skipped**, so a zero-length log is never archived as a misleading result.
- **Collision-safe** — two runs finishing in the same minute get `…-2`, `…-3`, and so on.
- `config.yaml` is copied in, so the archive reads on its own months later without guessing the
  cut / batch-size / clustering settings.
- Failure is non-fatal: a filesystem problem prints `[Archive] failed: …` and the run still
  closes the broker connection and exits cleanly.
- If everything was missing or empty you get `[Archive] WARNING: every result log was missing
  or empty` — a run that produced nothing.

Existing archives live in [results/July27th/](results/July27th/) (`split/`, `dynamic/`, plus
`imgs/` with the rendered plots) and the notebooks in [results/visual/](results/visual/).

---

## 4. `map/pred_collected/` — collected prediction files

Built by `_collect_map_pred` ([Server.py:958-989](src/Server.py#L958-L989)): each tier that ran
`postprocess_yolo` zips its own `map/pred/*.txt` and publishes it to `map_pred_queue` tagged
with its cluster id. The server unpacks per cluster:

```
map/pred_collected/
├── intermediate_queue_0/frame_000001.txt …
└── intermediate_queue_1/frame_000001.txt …
```

- **Write-once**: a frame index already unpacked for a cluster is kept, never overwritten, so a
  second edge in the same cluster reprocessing the same video can't clobber it.
- Wiped at startup *and* rebuilt at every shutdown collection — it is the server's own scratch
  space, not a result you need to keep. The numbers derived from it live in `map.log`.
- Which tier is expected to publish depends on mode: `only_edge` → the edges, otherwise → the
  cloud tier.

---

## 5. Files the server *deletes*, and files it does *not* write

Cleared once at startup in [Server.py:94-115](src/Server.py#L94-L115) — this happens in the
Server (which starts once) rather than per-Scheduler, because doing it per-client made
later-starting clients wipe files earlier clients were already writing:

| Deleted at startup | Why |
|---|---|
| `metrics_raw_*.csv`, `metrics_pivoted_*.csv`, `metrics_pivot_*.lock` | client-written per-batch metrics from the previous run |
| `detections_stream.jsonl` | streamed detections from the previous run |
| `map/pred/`, `map/pred_collected/` | `map/pred` is write-once per frame, so leftovers would "win" forever and silently poison every future run's mAP |

These are produced by **[src/Scheduler.py](src/Scheduler.py) on the client devices**, not by the
server, and are **not** part of the `results/` archive:

| File | Written by | Content |
|---|---|---|
| `metrics_raw_<queue>_<client>.csv` | each device | per-batch raw metrics |
| `metrics_pivoted_<queue>.csv` | one device per cluster (lock-elected) | joined edge+cloud view: `batch_id, batch_size, best_cut, edge_device, edge_latency_ms, edge_ram_mb, edge_message_size_bytes, cloud_device, cloud_latency_ms, cloud_ram_mb, e2e_latency_ms` |
| `detections_stream.jsonl` | the postprocessing tier | one line per frame: `{"frame": 1, "dets": [{"box": […], "score": …, "class": …}]}` — flat RAM over long videos |
| `detections.json` | the postprocessing tier, at the end | the stream rebuilt as `{frame: dets}`; gated by `detections.save_json` |
| `map/pred/frame_NNNNNN.txt` | the postprocessing tier | `class_id cx cy w h confidence`, 640-normalized |

> Note: `detections.json` is **not** cleared at startup (only the `.jsonl` stream is), so an old
> one survives until the next run rebuilds it. Check its mtime before trusting it.

---

## 6. Console-only output

Not saved anywhere — capture the terminal if you need it:

- The framed `[SYSTEM FPS]` block: whole-run fps, steady-state fps, and the `[ref mean, N/U]`
  arithmetic mean of `1/dt` (biased high, kept for comparison only — do not use it), plus the
  batch count and stop reason ([Server.py:496-533](src/Server.py#L496-L533)).
- The `[PER-CLUSTER UTILIZATION & LATENCY]` block — same data as the two cluster logs, formatted.
- The `[mAP PIPELINE 1/2]` summary block ([Server.py:881-921](src/Server.py#L881-L921)).
- Hungarian clustering result (`print_result`), per-device profiling and bandwidth lines, and
  live `[FPS] DONE #n window_fps=…` progress.

---

## 7. Quick recipes

| Question | File | How |
|---|---|---|
| Headline throughput | `fps_cluster.log` | the `SYSTEM` line's `fps` |
| Did the clusters balance? | `fps_cluster.log` | compare `share=` across clusters |
| Throughput over time | `batch_done_ns.log` | plot col 2 against col 1 |
| Per-cluster throughput over time | `fps_cluster_ns.log` | group by `cluster=`, plot `window_fps` |
| Is the cut in the right place? | `utilization_cluster.log` | compare `role=edge` vs `role=cloud` utilization |
| Tail latency | `latency_cluster.log` | `kind=e2e`, `p95_ms` |
| One straggler device? | `utilization.log` | the per-device `utilization=` column |
| Headline accuracy | `map.log` | the `OVERALL ALL` line |
| Did accuracy move with the cut? | `map_window.log` + `cut_change_ns.log` | shared ns-epoch x-axis |
| What config produced this? | `results/…/config.yaml` | archived alongside the numbers |

Related specs: [fps_measure_guide.md](fps_measure_guide.md),
[utilization_guide.md](utilization_guide.md), [visual_guide.md](visual_guide.md).
