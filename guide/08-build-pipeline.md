# 08 · Build pipeline — parse, build, execute, verify

How to turn a conformant results directory ([01](01-result-format.md)) into the notebook
and PNGs.

---

## 1 · Layout

```
project/
├── guide/                        this directory
│   ├── validate_results.py       format conformance  (01 §5)
│   └── validate_palette.py       palette checks      (06 §4)
├── results/
│   ├── <run-id>/
│   │   ├── <run-a>/*.log         inputs
│   │   ├── <run-b>/*.log
│   │   └── imgs/                 outputs — charts land here
│   └── visual/
│       └── <Name> Visualization.ipynb     the deliverable
└── <scratch>/
    ├── build_nb.py               emits the notebook
    └── run_nb.py                 executes it, reports all errors
```

Keep `build_nb.py` / `run_nb.py` in a scratch directory unless asked to commit them. The
notebook is the deliverable.

---

## 2 · Parsers

The [universal grammar](01-result-format.md) means **one** parser core reads every file.
Do not write a bespoke regex per file.

```python
import re
import numpy as np
from pathlib import Path

KV = re.compile(r"(\w+)=([^\s]+)")

def parse_kv_line(line):
    """-> (timestamp, [UPPERCASE flags], {key: value})"""
    parts = line.split()
    if not parts:
        return None
    ts    = int(parts[0]) if parts[0].isdigit() else None
    kv    = {k: v for k, v in KV.findall(line)}
    flags = [p for p in parts[1:] if "=" not in p and p.isupper()]
    return ts, flags, kv

def num(v):
    """'55.06%' -> 55.06 ; '336' -> 336.0 ; junk -> nan"""
    if v is None:
        return np.nan
    try:
        return float(str(v).rstrip("%"))
    except ValueError:
        return np.nan

def read_lines(path):
    if not Path(path).exists():
        print(f"!! missing: {path}")
        return []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return [ln.rstrip("\n") for ln in f if ln.strip()]
```

Then one small function per file, all returning `list[dict]`:

```python
GROUP_LABEL = {"intermediate_queue_0": "Group 0", "intermediate_queue_1": "Group 1"}

def parse_rate_summary(run, path):                    # group_rate.log
    rows = []
    for ln in read_lines(path):
        ts, flags, kv = parse_kv_line(ln)
        scope = GROUP_LABEL.get(kv.get("cluster"),
                                "System" if "SYSTEM" in flags else None)
        if scope is None:
            continue                                  # skip lines this parser doesn't own
        rows.append(dict(run=run, scope=scope,
                         fps=num(kv.get("fps")),
                         # SYSTEM carries neither steady_fps nor share (01 §3.3).
                         # Leave them NaN — do NOT default steady_fps to fps, or the
                         # System row silently looks like it has a steady-state number.
                         steady_fps=num(kv.get("steady_fps")),
                         done=num(kv.get("done")), frames=num(kv.get("frames")),
                         share=num(kv.get("share"))))
    return rows

def parse_rate_timeline(run, path):                   # group_rate_ns.log
    rows = []
    for ln in read_lines(path):
        ts, _, kv = parse_kv_line(ln)
        if "window_fps" not in kv:
            continue                                  # warm-up rows, before the window fills
        rows.append(dict(run=run,
                         cluster=GROUP_LABEL.get(kv["cluster"], kv["cluster"]),
                         ts=ts, done=int(num(kv["done"])),
                         window_fps=num(kv["window_fps"])))
    return rows

def parse_latency(run, path):                         # latency_group.log
    rows = []
    for ln in read_lines(path):
        ts, flags, kv = parse_kv_line(ln)
        scope = GROUP_LABEL.get(kv.get("cluster"),
                                "System" if "SYSTEM" in flags else None)
        if scope is None:
            continue
        rows.append(dict(run=run, scope=scope,
                         role=kv.get("role", "all"), kind=kv.get("kind"),
                         n=num(kv.get("n")), mean_ms=num(kv.get("mean_ms")),
                         p50_ms=num(kv.get("p50_ms")), p95_ms=num(kv.get("p95_ms")),
                         max_ms=num(kv.get("max_ms"))))
    return rows

def parse_util_group(run, path):                      # utilization_group.log
    rows = []
    for ln in read_lines(path):
        ts, flags, kv = parse_kv_line(ln)
        scope = GROUP_LABEL.get(kv.get("cluster"),
                                "System" if "SYSTEM" in flags else None)
        if scope is None:
            continue
        rows.append(dict(run=run, scope=scope, role=kv.get("role", "all"),
                         devices=num(kv.get("devices")),
                         utilization=num(kv.get("utilization")),
                         utilization_mean=num(kv.get("utilization_mean")),
                         busy_s=num(kv.get("busy_s")), total_s=num(kv.get("total_s")),
                         packages=num(kv.get("packages"))))
    return rows

def parse_util_device(run, path):                     # utilization.log
    rows = []
    for ln in read_lines(path):
        ts, _, kv = parse_kv_line(ln)
        if "client" not in kv:
            continue
        rows.append(dict(run=run, client=kv["client"], role=kv.get("role"),
                         packages=num(kv.get("packages")),
                         busy_s=num(kv.get("busy_s")), total_s=num(kv.get("total_s")),
                         utilization=num(kv.get("utilization"))))
    return rows
```

### 2.1 The two-arity file

`batch_done_ns.log` has one column during warm-up and two after
([01 §3.1](01-result-format.md)). Handle it explicitly — this is the most common parsing
bug:

```python
def parse_batch_done(run, path):
    rows, idx = [], 0
    for ln in read_lines(path):
        parts = ln.split()
        idx += 1
        if len(parts) == 2:                 # warm-up rows carry no rate yet
            rows.append(dict(run=run, batch=idx, ts=int(parts[0]),
                             window_fps=float(parts[1])))
    return rows
```

Note `idx` increments on **every** line so the batch index stays true even though only
two-column rows are kept.

### 2.2 Free-form events

```python
def parse_events(run, path):                          # events_ns.log
    rows = []
    for ln in read_lines(path):
        parts = ln.split(None, 1)                     # split ONCE: description has spaces
        if len(parts) != 2 or not parts[0].isdigit():
            continue
        rows.append(dict(run=run, ts=int(parts[0]), description=parts[1]))
    return rows
```

### 2.3 Conventions that matter

- Return `list[dict]`, concatenate across runs, build the DataFrame **once**. Never
  `pd.concat` in a loop.
- Every parser tags rows with its `run`, so runs stack into one tidy frame.
- `continue` on lines a parser does not own, rather than raising — one file legitimately
  holds several line kinds.
- Map raw ids to display labels **at parse time** (`GROUP_LABEL`), so no chart code ever
  contains a raw queue name.
- Normalize units at the edge: strip `%` in `num()`, convert ms→s at the pivot.
- **Never name a column** `agg`, `max`, `min`, `sum`, `mean`, `count`, `size`, `mode`,
  `all`, `any`, `filter`, `pop`, `name`, `index`, `values`, `shape`, or `T` (§7).

---

## 3 · Load, and print what you loaded

```python
# An EMPTY list becomes a DataFrame with NO columns, so any later `df.run` raises
# KeyError instead of returning an empty frame. Always declare the columns.
COLS = {
    "rate":  ["run", "scope", "fps", "steady_fps", "done", "frames", "share"],
    "tl":    ["run", "cluster", "ts", "done", "window_fps"],
    "batch": ["run", "batch", "ts", "window_fps"],
    "lat":   ["run", "scope", "role", "kind", "n",
              "mean_ms", "p50_ms", "p95_ms", "max_ms"],
    "utg":   ["run", "scope", "role", "devices", "utilization",
              "utilization_mean", "busy_s", "total_s", "packages"],
    "utd":   ["run", "client", "role", "packages", "busy_s", "total_s", "utilization"],
    "ev":    ["run", "ts", "description"],          # events_ns.log is optional (01 §2)
}

def load_all():
    rate, tl, batch, lat, utg, utd, ev = [], [], [], [], [], [], []
    for run, d in RUNS.items():
        rate  += parse_rate_summary(run,  d / "fps_cluster.log")
        tl    += parse_rate_timeline(run, d / "fps_cluster_ns.log")
        batch += parse_batch_done(run,    d / "batch_done_ns.log")
        lat   += parse_latency(run,       d / "latency_cluster.log")
        utg   += parse_util_group(run,    d / "utilization_cluster.log")
        utd   += parse_util_device(run,   d / "utilization.log")
        ev    += parse_events(run,        d / "cut_change_ns.log")
    data = dict(rate=rate, tl=tl, batch=batch, lat=lat, utg=utg, utd=utd, ev=ev)
    return tuple(pd.DataFrame(rows, columns=COLS[k]) for k, rows in data.items())

df_rate, df_tl, df_batch, df_lat, df_utg, df_utd, df_events = load_all()

for name, df in [("rate summary", df_rate), ("rate timeline", df_tl),
                 ("batch timeline", df_batch), ("latency", df_lat),
                 ("utilization/group", df_utg), ("utilization/device", df_utd),
                 ("events", df_events)]:
    print(f"{name:<20} {df.shape}")
```

Shapes are a cheap smoke test. A frame with 0 rows means a parser silently matched
nothing — usually a filename or a label-map mismatch.

---

## 4 · Verify assumptions before charting them

Compute what you are about to assert visually, and **branch on the result**.

```python
# Is a metric actually configuration-dependent, or identical across runs?
piv = df_lat[df_lat.kind == "e2e"].pivot_table(index="scope", columns="run",
                                               values="mean_ms")
delta = (piv[RUN_ORDER[0]] - piv[RUN_ORDER[1]]).abs().max()
IDENTICAL = bool(delta == 0)
print(f"max |A - B| mean e2e = {delta:.6f}")
print("=> identical across runs; charting as ONE series."
      if IDENTICAL else "=> differs; both runs plotted separately.")
```

Then let the charts consume the flag:

```python
src  = df_lat[df_lat.run == RUN_ORDER[0]] if IDENTICAL else df_lat
note = "identical for both runs" if IDENTICAL else "per run"
```

**Why this matters.** Plotting two identical series draws two perfectly overlapping
lines: the reader sees one line and cannot tell whether the other is hidden or missing.
Keeping the branch in the notebook means it self-corrects when a future run diverges.

Other assumptions worth an explicit check:

```python
# Same workload? Otherwise the runs are not comparable and no chart will say so.
print(df_rate[df_rate.scope == "System"][["run", "done", "frames"]])

# service samples should sum to busy_s  (04 §2.1)
# utilization should never exceed 100%  (03 §2.1)
print(df_utd[df_utd.utilization > 100])          # must be empty
```

Run the format validator too, from inside the notebook if you like:

```bash
!python guide/validate_results.py results/<run-id>/<run-a> --names cluster
```

---

## 5 · Generate the notebook from a builder script

**Do not hand-author `.ipynb` JSON, and do not edit cells one at a time.** A builder
script makes the notebook regenerable after any fix, keeps cell order under version
control, and turns a style change into one edit instead of twenty.

```python
"""build_nb.py — emit the visualization notebook."""
import nbformat as nbf
from pathlib import Path

nb = nbf.v4.new_notebook()
cells = []
md   = lambda s: cells.append(nbf.v4.new_markdown_cell(s.strip("\n")))
code = lambda s: cells.append(nbf.v4.new_code_cell(s.strip("\n")))

md(r"""
# <Title>

| Run | Folder |
|---|---|
| **A** | `results/<run-id>/a` |
| **B** | `results/<run-id>/b` |

Both runs process an identical workload. Charts are written to
`results/<run-id>/imgs/`.

| Input file | Feeds |
|---|---|
| `batch_done_ns.log` | C3, C9 |
| `fps_cluster_ns.log` | C2, C4 |
| ... | ... |
""")

md("## 0 · Setup — paths, palette, chart style")
code(r'''
<the 06 §5 style block, plus ROOT / RESULTS / IMG_DIR>
''')

md("## 1 · Log parsers")
code(r'''
<the §2 parsers>
''')

# ... one md() + one code() per chart ...

nb["cells"]  = cells
nb.metadata = {
    "kernelspec":    {"display_name": "Python 3", "language": "python",
                      "name": "python3"},
    "language_info": {"name": "python", "version": "3"},
}

out = Path(r"D:\...\results\visual\Result Visualization.ipynb")
out.parent.mkdir(parents=True, exist_ok=True)
nbf.write(nb, str(out))
print("wrote", out, f"({len(cells)} cells)")
```

**Conventions**
- Use `r'''...'''` for code cells so Windows path backslashes survive.
- One markdown heading + one code cell per chart. **Never two charts in a cell** — an
  error in the first hides the second.
- Cell 0 states what the runs are, where images go, and which input feeds which chart.
- Kernel metadata must be present or `nbclient` cannot pick an executor.
- Pin the output directory in the setup cell and create it:
  `IMG_DIR.mkdir(parents=True, exist_ok=True)`.

---

## 6 · Execute headless, collect every error

```python
"""run_nb.py — execute the notebook in place, report all failures."""
import sys, nbformat
from nbclient import NotebookClient
from pathlib import Path

p  = Path(r"D:\...\results\visual\Result Visualization.ipynb")
nb = nbformat.read(str(p), as_version=4)

NotebookClient(nb, timeout=600, kernel_name="python3",
               resources={"metadata": {"path": str(p.parent)}},
               allow_errors=True).execute()      # collect, don't stop at the first
nbformat.write(nb, str(p))

fail = 0
for i, c in enumerate(nb.cells):
    for o in c.get("outputs", []):
        if o.get("output_type") == "error":
            fail += 1
            print(f"\n### ERROR in cell {i} ###")
            print(c.source[:300], "\n---")
            print("\n".join(o.get("traceback", []))[-2500:])
        elif o.get("output_type") == "stream" and o.get("text", "").strip():
            print(f"[cell {i}] {o['text'].rstrip()[:1200]}")

print(f"\n=== {fail} cell error(s) ===")
sys.exit(1 if fail else 0)
```

```bash
python build_nb.py && python run_nb.py
```

**Notes**
- `nbclient` + `ipykernel` are enough. **`nbconvert` is not required** and is often not
  installed.
- `allow_errors=True` is the important flag — every failing cell per run, instead of one
  round-trip per bug.
- Executing writes outputs (including base64 PNGs) back into the `.ipynb`, so the
  delivered notebook renders without a re-run. Expect ~0.5–2 MB.
- A `zmq` / `Proactor event loop` RuntimeWarning on Windows is harmless.
- `rm imgs/*.png` before a re-run if you want certainty that nothing stale survived a
  renumbering.

---

## 7 · Look at the output — the step that finds the defects

```
Read <project>/results/<run-id>/imgs/01_rate_by_group.png
```

Read them in batches of ~3 — they are large in context. Prioritize charts with
annotations, legends, or many bars; plain grouped bars rarely break.

| Defect | Look for |
|---|---|
| Legend over a mark | default `upper left` on a chart with a tall leading bar |
| Label / whisker collision | annotations anchored to a percentile inside the whisker |
| Clipped labels | `ylim` too tight, or long tick labels cut at the figure edge |
| Overlapping tick labels | too many categories for the width |
| Misleading color | delta charts colored by sign instead of verdict |
| Series order flipped | pivot not re-indexed, so colors moved between entities |
| Invisible series | two identical series overlapping (§4) |
| Wrong units | axis says `s`, numbers are clearly `ms` |
| A bar above 100% | a real measurement bug — fix the instrument, not the chart |

Fix in `build_nb.py`, then re-run **both** scripts. **Never patch the `.ipynb`
directly** — the next build silently reverts it.

Expected: ~10 charts, 2 rounds of fixes, most of them layout.

---

## 8 · The pandas trap that fails silently

`agg`, `max`, `min`, `sum`, `mean`, `count`, `size`, `mode`, `all`, `any`, `filter`,
`pop`, `name`, `index`, `values`, `shape`, `T`, `apply`, `where`, `first`, `last`,
`keys`, `items`, `plot`, `style`, `abs`, `add`, `rank`, `round` are DataFrame attributes.
A **column** with one of those names is shadowed: attribute access returns the bound
method, and comparing it yields an all-False mask **without raising**.

```python
df[df.agg == "ALL"]        # -> empty. No error. Compares a method to a str.
df[df["agg"] == "ALL"]     # -> correct
```

Symptom: a chart silently loses a series, or `.loc["X"]` raises a bewildering
`KeyError: 'X'` on a frame you can see contains `X`.

**Fix:** rename the column at parse time (`agg` → `agg_kind`). Bracket access alone only
fixes the call site you happened to notice.

---

## 9 · Other environment notes

- `fig.suptitle` needs `y=1.02` **and** `fig.tight_layout()`, or it overlaps panel titles.
- `ax.boxplot` returns no legend handles — build `plt.Rectangle` proxies.
- `bbox_inches="tight"` can still clip annotations placed outside the axes with
  `xycoords="axes fraction"`. Verify in the PNG.
- `textcoords="offset points"` keeps labels put across dpi and figsize changes; data
  coordinates do not.
- Set `savefig.facecolor` or the PNG background is transparent and renders black in
  dark-mode viewers.
- `font.family` needs a fallback chain. `Segoe UI` exists on Windows; most named fonts do
  not, and a missing font is a silent fallback plus console spam.
- Use absolute paths from a single `ROOT` constant, raw strings on Windows:
  `Path(r"D:\...")`.
- **Nanosecond timestamps overflow float precision.** Keep them as `int` and subtract a
  baseline before dividing to seconds (C9 does exactly this).
