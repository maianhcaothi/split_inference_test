# Visual Guide — result charts & visualization notebooks

Operating instructions for Claude Code when producing charts, plots, dashboards,
or visualization notebooks. Written to be dropped into any project.

> If a `dataviz` skill is available in the environment, load it first. This
> document is the matplotlib-specific operational version of it — concrete
> recipes, file conventions, and a build pipeline — and does not replace the
> full reference.

**Contents**

- [Quick start](#quick-start) — the loop, in six steps
- [Part I — Method](#part-i--method) — §0 order · §1 rules · §2 palette · §3 validation · §4 style
- [Part II — Chart catalogue](#part-ii--chart-catalogue) — 12 recipes with working code
- [Part III — Files](#part-iii--files) — parsing inputs, naming outputs
- [Part IV — Build pipeline](#part-iv--build-pipeline) — how to actually create and run it
- [Part V — Gotchas & checklist](#part-v--gotchas--checklist)

---

## Quick start

For "visualize these results" / "make charts from X" / "build a notebook":

1. **Look at the data first.** `head`/`tail` every input file. Count records.
   Diff runs against each other. You cannot pick chart forms for data you have
   not seen, and you will find things that change the plan (§3.4).
2. **Decide the chart list** before writing any plotting code. One line per
   chart: what question it answers, what form, how many series.
3. **Write a builder script** (`build_nb.py`) that emits the `.ipynb` — do not
   hand-author notebook JSON (§IV.2).
4. **Execute it headless** and collect *all* cell errors in one pass (§IV.3).
5. **Read every generated PNG.** Layout defects are invisible in code (§IV.4).
6. **Fix and re-run.** Budget at least one round; two is normal.

Never report "done" from step 4. Step 5 is where the defects are.

---

# Part I — Method

## §0 · The order of operations

Color comes **last**. Most bad charts pick colors first.

| Step | Decide | Driven by |
|---|---|---|
| 1 | **Form** | the data's job: magnitude, identity, change over time, polarity, one headline |
| 2 | **Encoding** | which variable maps to position, which to color, which to facet |
| 3 | **Color role** | categorical / sequential / diverging / status |
| 4 | **Validate** | computed checks, never eyeballed |
| 5 | **Marks** | thin marks, hairline grid, selective labels |
| 6 | **Verify** | render and *look* |

**Picking the form:**

| The data's job | Form |
|---|---|
| Compare magnitudes across categories | Bar (horizontal if labels are long) |
| Change over a continuous index | Line |
| Distribution / spread | Box, or violin if multi-modal matters |
| Two measures on different scales | **Two panels** — never a dual axis |
| Part-to-whole, ≤ 6 parts, at a glance | Stacked bar; pie only if truly at-a-glance |
| Relationship between two measures | Scatter (≤ 3 color classes) |
| One number that *is* the story | **Stat tile / hero number — not a chart** |
| Same shape across many entities | Small multiples |

A one-bar bar chart and a two-slice pie are both wrong. The number is the chart.

## §1 · Non-negotiables

- **Never a dual-axis chart** (two y-scales on one plot). The alignment between
  the scales is arbitrary, so the chart invents a correlation that isn't in the
  data. Two measures of different scale → two panels, small multiples, or both
  series indexed to a common base (=100 at t₀) on one axis.
- **Categorical hues in fixed slot order, never cycled.** A 9th series is never
  a generated hue — fold it into "Other" or facet into small multiples.
- **Color follows the entity, not its rank.** Filtering out a series must not
  repaint the survivors. Build `{entity: color}` dicts, never
  `color=palette[i]` over a filtered list.
- **Sequential = one hue, light→dark. Diverging = two hues + neutral gray
  midpoint.** Never a rainbow; never a hue at the diverging midpoint.
- **Gridlines and axes are solid hairlines**, one shade off the surface. Never
  dashed — dashing reads as "threshold" or "projection" when it is just a grid.
- **A legend whenever there are ≥ 2 series** (one series needs none — the title
  names it). Direct-label *selectively*: the endpoint, the extreme, the series
  that matters. Never a number on every point of a dense line.
- **Status colors are reserved** for good/warning/serious/critical and always
  ship with a text label, never color alone. Never reuse them as "series 4".
- **Text wears text tokens, never the series color.** A colored mark beside the
  label carries identity. (Exception: an endpoint label on a line may take the
  series color — it *is* the identity cue.)
- **No border drawn around marks to separate them.** Use a surface-colored gap.

## §2 · Palette

Validated categorical slots. Take them **in this order** — the ordering is the
colorblind-safety mechanism, not cosmetic.

| Slot | Hue | Light | Dark |
|---|---|---|---|
| 1 | blue | `#2a78d6` | `#3987e5` |
| 2 | orange | `#eb6834` | `#d95926` |
| 3 | aqua | `#1baf7a` | `#199e70` |
| 4 | yellow | `#eda100` | `#c98500` |
| 5 | magenta | `#e87ba4` | `#d55181` |
| 6 | green | `#008300` | `#008300` |
| 7 | violet | `#4a3aa7` | `#9085e9` |
| 8 | red | `#e34948` | `#e66767` |

**How many series may I use?**

| Chart form | Pairs that can touch | Cap |
|---|---|---|
| Bar, line, stacked area | adjacent only | 8 slots |
| Scatter, bubble, choropleth, small multiples | **all pairs** | **3 slots** |

Past the cap: fold the tail into "Other", or facet. Slot 4 (yellow) sits beside
slot 2 (orange) and that pair fails the all-pairs floors — this is why the
all-pairs cap is 3, not 4.

**Chrome & ink** (light / dark):

| Role | Light | Dark |
|---|---|---|
| Chart surface | `#fcfcfb` | `#1a1a19` |
| Page plane | `#f9f9f7` | `#0d0d0d` |
| Primary ink | `#0b0b0b` | `#ffffff` |
| Secondary ink | `#52514e` | `#c3c2b7` |
| Muted (axis/tick) | `#898781` | `#898781` |
| Gridline (hairline) | `#e1e0d9` | `#2c2c2a` |
| Baseline / axis | `#c3c2b7` | `#383835` |

**Sequential:** blue, light→dark —
`#cde2fb → #86b6ef → #3987e5 → #256abf → #104281`.
For a *discrete ordered* ramp (tiers, funnel stages) the pale end must still
read as a mark: start no lighter than `#86b6ef` on the light surface, and go no
darker than `#184f95` on the dark one.

**Diverging:** blue `#2a78d6` ↔ red `#e34948`, neutral gray midpoint `#f0efec`
(dark: `#383835`). Equal step count per arm.

**Status** (never themed, never reused as series colors):
good `#0ca30c` · warning `#fab219` · serious `#ec835a` · critical `#d03b3b`.

**Relief rule.** Three light-mode slots sit below 3:1 contrast on the light
surface — aqua (2.74), yellow (2.11), magenta (2.62). Using any of them
obligates visible direct labels or a table view. It is not dismissable.

## §3 · Validate the palette — compute, don't eyeball

Thresholds:

| Check | Threshold |
|---|---|
| Lightness band (OKLCH L) | light `0.43–0.77`, dark `0.48–0.67` |
| Chroma floor (OKLCH C) | `≥ 0.10` (below it a hue reads gray) |
| CVD separation (OKLab ΔE×100, protan/deutan, Machado 2009 @ severity 1.0) | `≥ 8` target; `6–8` legal **only** with secondary encoding |
| Normal-vision floor (unsimulated ΔE×100) | `≥ 15` — **hard gate**, secondary encoding does not excuse it |
| Contrast vs surface (WCAG) | `≥ 3:1`, else relief required |

Pairlist is **adjacent** for bars/lines/stacks, **all pairs** for
scatter/bubble/maps/small-multiples.

`node` is frequently absent on Windows dev boxes. Do not skip the checks — use
this Python implementation:

<details>
<summary><code>validate_palette.py</code> — full source</summary>

```python
import math, sys, itertools

BAND = {"light": (0.43, 0.77), "dark": (0.48, 0.67)}
CHROMA_FLOOR = 0.10
CVD_TARGET, CVD_FLOOR = 8.0, 6.0
NORMAL_FLOOR = 15.0
CONTRAST_MIN = 3.0
DEFAULT_SURFACE = {"light": "#fcfcfb", "dark": "#1a1a19"}

# Machado, Oliveira & Fernandes (2009) CVD transforms at severity 1.0, linear RGB.
MACHADO = {
    "protan": [[0.152286, 1.052583, -0.204868],
               [0.114503, 0.786281, 0.099216],
               [-0.003882, -0.048116, 1.051998]],
    "deutan": [[0.367322, 0.860646, -0.227968],
               [0.280085, 0.672501, 0.047413],
               [-0.011820, 0.042940, 0.968881]],
    "tritan": [[1.255528, -0.076749, -0.178779],
               [-0.078411, 0.930809, 0.147602],
               [0.004733, 0.691367, 0.303900]],
}

def hex2srgb(h):
    h = h.strip().lstrip("#")
    return [int(h[i:i+2], 16) / 255 for i in (0, 2, 4)]

def s2lin(c):
    return c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4

def lin(h):          return [s2lin(c) for c in hex2srgb(h)]
def rel_lum(h):
    r, g, b = lin(h); return 0.2126*r + 0.7152*g + 0.0722*b

def contrast(a, b):
    hi, lo = sorted([rel_lum(a), rel_lum(b)], reverse=True)
    return (hi + 0.05) / (lo + 0.05)

def oklab_from_lin(rgb):
    r, g, b = rgb
    l = (0.4122214708*r + 0.5363325363*g + 0.0514459929*b) ** (1/3)
    m = (0.2119034982*r + 0.6806995451*g + 0.1073969566*b) ** (1/3)
    s = (0.0883024619*r + 0.2817188376*g + 0.6299787005*b) ** (1/3)
    return [0.2104542553*l + 0.7936177850*m - 0.0040720468*s,
            1.9779984951*l - 2.4285922050*m + 0.4505937099*s,
            0.0259040371*l + 0.7827717662*m - 0.8086757660*s]

def oklch(h):
    L, a, b = oklab_from_lin(lin(h)); return L, math.hypot(a, b)

def simulate(h, kind):
    r, g, b = lin(h); M = MACHADO[kind]
    return [max(0.0, min(1.0, M[i][0]*r + M[i][1]*g + M[i][2]*b)) for i in range(3)]

def delta_e(h1, h2, kind=None):
    a = oklab_from_lin(simulate(h1, kind) if kind else lin(h1))
    b = oklab_from_lin(simulate(h2, kind) if kind else lin(h2))
    return 100 * math.dist(a, b)

def validate(palette, mode="light", surface=None, pairs="adjacent"):
    surface = surface or DEFAULT_SURFACE[mode]
    lo, hi = BAND[mode]
    report, ok = [], True

    offband = [(c, round(oklch(c)[0], 3)) for c in palette
               if not (lo <= oklch(c)[0] <= hi)]
    ok &= not offband
    report.append(("Lightness band", "PASS" if not offband else "FAIL",
                   f"outside band: {offband}" if offband else f"all inside L {lo}-{hi}"))

    lowc = [(c, round(oklch(c)[1], 3)) for c in palette if oklch(c)[1] < CHROMA_FLOOR]
    ok &= not lowc
    report.append(("Chroma floor", "PASS" if not lowc else "FAIL",
                   f"below floor: {lowc}" if lowc else f"all >= {CHROMA_FLOOR}"))

    n = len(palette)
    pairlist = (list(itertools.combinations(range(n), 2)) if pairs == "all"
                else [(i, i+1) for i in range(n-1)])
    label = "all-pairs" if pairs == "all" else "adjacent"

    worst = None
    for kind in ("protan", "deutan"):
        for i, j in pairlist:
            d = delta_e(palette[i], palette[j], kind)
            if worst is None or d < worst[0]:
                worst = (d, kind, palette[i], palette[j])
    tri = min((delta_e(palette[i], palette[j], "tritan") for i, j in pairlist), default=99)
    wd = worst[0] if worst else 99
    state = "PASS" if wd >= CVD_TARGET else ("WARN" if wd >= CVD_FLOOR else "FAIL")
    ok &= state != "FAIL"
    report.append(("CVD separation", state,
                   f"worst {label} {worst[3]}<->{worst[2]} dE {wd:.1f} ({worst[1]}) "
                   f"- tritan {tri:.1f}" if worst else "n/a"))

    nworst = None
    for i, j in pairlist:
        d = delta_e(palette[i], palette[j])
        if nworst is None or d < nworst[0]:
            nworst = (d, palette[i], palette[j])
    nd = nworst[0] if nworst else 99
    ok &= nd >= NORMAL_FLOOR
    report.append(("Normal-vision floor", "PASS" if nd >= NORMAL_FLOOR else "FAIL",
                   f"worst {label} {nworst[2]}<->{nworst[1]} dE {nd:.1f}" if nworst else "n/a"))

    low = [(c, round(contrast(c, surface), 2)) for c in palette
           if contrast(c, surface) < CONTRAST_MIN]
    report.append(("Contrast vs surface", "WARN" if low else "PASS",
                   f"below 3:1 - relief required: {low}" if low else "all >= 3:1"))
    return report, ok

if __name__ == "__main__":
    pal   = [c.strip() for c in sys.argv[1].split(",") if c.strip()]
    mode  = sys.argv[2] if len(sys.argv) > 2 else "light"
    pairs = sys.argv[3] if len(sys.argv) > 3 else "adjacent"
    rep, ok = validate(pal, mode=mode, pairs=pairs)
    print(f"\nPalette ({mode}, pairs={pairs}): {len(pal)} slots")
    for name, state, detail in rep:
        print(f"  [{state:<4}] {name:<22} {detail}")
    print(f"\n  -> {'ALL CHECKS PASS' if ok else 'FAILED'}\n")
    sys.exit(0 if ok else 1)
```

</details>

**Sanity-check the port before trusting it.** These are known-good outputs:

```bash
# 8 slots, adjacent  → worst CVD 9.1 (protan), worst normal-vision 19.6
python validate_palette.py "#2a78d6,#eb6834,#1baf7a,#eda100,#e87ba4,#008300,#4a3aa7,#e34948" light adjacent

# slots 1-3, all pairs → worst CVD 9.2 (deutan), worst normal-vision 24.0
python validate_palette.py "#2a78d6,#eb6834,#1baf7a" light all
```

If your numbers differ, the port is wrong — fix it before using the results.

## §4 · Style block and helpers

Everything below goes in the notebook's setup cell, verbatim.

```python
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
from pathlib import Path

# ---- tokens -------------------------------------------------------------
SURFACE = "#fcfcfb"; PAGE  = "#f9f9f7"
INK     = "#0b0b0b"; INK_2 = "#52514e"
MUTED   = "#898781"; GRID  = "#e1e0d9"; AXIS = "#c3c2b7"

S1, S2, S3 = "#2a78d6", "#eb6834", "#1baf7a"     # categorical slots 1-3
GOOD, BAD, NEUTRAL = "#0ca30c", "#d03b3b", MUTED  # status + neutral

# Color follows the ENTITY. Build dicts, never index a palette list by rank.
MODE_COLOR = {"Dynamic": S1, "Split": S2}
ROLE_COLOR = {"cloud":   S1, "edge":  S2}

# ---- rcParams -----------------------------------------------------------
mpl.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,
    "font.family": ["Segoe UI", "DejaVu Sans", "sans-serif"],
    "font.size": 10,
    "axes.titlesize": 13, "axes.titleweight": "semibold",
    "axes.titlecolor": INK, "axes.titlepad": 12,
    "axes.labelsize": 10.5, "axes.labelcolor": INK_2,
    "axes.edgecolor": AXIS, "axes.linewidth": 0.8,
    "axes.grid": True, "axes.axisbelow": True,
    "grid.color": GRID, "grid.linestyle": "-", "grid.linewidth": 0.8,  # solid
    "xtick.color": MUTED, "ytick.color": MUTED,
    "xtick.labelsize": 9.5, "ytick.labelsize": 9.5,
    "xtick.major.size": 0, "ytick.major.size": 0,
    "legend.frameon": False, "legend.fontsize": 9.5, "legend.labelcolor": INK_2,
    "figure.dpi": 110, "savefig.dpi": 300, "savefig.bbox": "tight",
})

# The surface-coloured edge IS the 2px gap between adjacent fills.
# It is not a contrasting border drawn to separate marks — never use black.
BAR_KW  = dict(edgecolor=SURFACE, linewidth=1.2)
LINE_KW = dict(linewidth=2.0, solid_capstyle="round")
MARK_KW = dict(markersize=6, markeredgecolor=SURFACE, markeredgewidth=1.4)

# ---- helpers ------------------------------------------------------------
SAVED = []   # running manifest

def finish(fig, filename, hide_spines=("top", "right")):
    """Tidy spines, save at 300 dpi, record in the manifest, show."""
    for ax in fig.get_axes():
        for side in hide_spines:
            ax.spines[side].set_visible(False)
        ax.set_axisbelow(True)
    out = IMG_DIR / filename
    fig.savefig(out, dpi=300, bbox_inches="tight", facecolor=SURFACE)
    SAVED.append(filename)
    print(f"saved -> {out}")
    plt.show()

def label_bars(ax, bars, fmt="{:.2f}", dy=3, fontsize=9, color=INK_2):
    """Direct value labels above bars — the relief for sub-3:1 fills."""
    for bar in bars:
        h = bar.get_height()
        if np.isnan(h):
            continue
        ax.annotate(fmt.format(h),
                    xy=(bar.get_x() + bar.get_width()/2, h),
                    xytext=(0, dy), textcoords="offset points",
                    ha="center", va="bottom", fontsize=fontsize, color=color)
```

Standard per-axes tidy-up, applied in every recipe below:

```python
ax.grid(axis="x", visible=False)      # vertical bars: no grid on the category axis
ax.set_ylim(0, values.max() * 1.18)   # headroom so labels clear the top spine
```

---

# Part II — Chart catalogue

Twelve recipes. Each states **when to use**, the **data shape** it needs,
working **code**, and the **pitfalls** that actually bite.

Throughout: `RUN_ORDER = ["Dynamic", "Split"]` is the fixed series order, and
`x, width = np.arange(n), 0.36` is the standard grouped-bar geometry.

---

## C1 · Grouped bar — compare a measure across categories

**When** two or three series compared over ≤ 8 categories. The default
comparison chart.
**Data** tidy frame with `category`, `series`, `value`; pivoted to
`index=category, columns=series`.

```python
order = ["Cluster 0", "Cluster 1", "System"]
piv = df.pivot(index="scope", columns="run", values="fps").reindex(order)[RUN_ORDER]

x, width = np.arange(len(order)), 0.36
fig, ax = plt.subplots(figsize=(8.2, 4.8))

for i, run in enumerate(RUN_ORDER):
    off = (i - 0.5) * (width + 0.03)          # 0.03 = the surface gap
    bars = ax.bar(x + off, piv[run], width,
                  label=run, color=MODE_COLOR[run], **BAR_KW)
    label_bars(ax, bars, fmt="{:.2f}")

ax.set_xticks(x, order)
ax.set_ylabel("Throughput (FPS)")
ax.set_ylim(0, piv.to_numpy().max() * 1.18)
ax.set_title("Throughput by cluster — Dynamic vs Split")
ax.grid(axis="x", visible=False)
ax.legend(loc="upper left")

# One takeaway line under the plot, in muted ink — not a second title.
gain = (piv.loc["System", "Dynamic"] / piv.loc["System", "Split"] - 1) * 100
ax.annotate(f"Dynamic delivers {gain:+.1f}% system throughput",
            xy=(0.5, -0.16), xycoords="axes fraction",
            ha="center", fontsize=9.5, color=MUTED)

finish(fig, "01_fps_by_cluster.png")
```

**Pitfalls**
- `.reindex(order)` — without it pandas sorts categories alphabetically and the
  narrative order is lost.
- `[RUN_ORDER]` on the pivot — pins series order so colors stay stable.
- Offsets are `(i - (n-1)/2) * (width + gap)`; for n=2 that is `(i - 0.5)`.
- `loc="upper left"` collides with a tall leading bar more often than not.

---

## C2 · Small-multiple lines — same shape, several conditions

**When** you would otherwise put 4+ lines on one axis. Facet so each panel
carries ≤ 3 series.
**Data** long frame with `facet`, `series`, `x`, `y`.

```python
fig, axes = plt.subplots(1, 2, figsize=(13, 4.6), sharey=True)
ymax = df.window_fps.max() * 1.10

for ax, run in zip(axes, RUN_ORDER):
    sub_run = df[df.run == run]
    for cluster in ["Cluster 0", "Cluster 1"]:
        s = sub_run[sub_run.cluster == cluster].sort_values("done")
        ax.plot(s.done, s.window_fps, color=CLUSTER_COLOR[cluster],
                label=cluster, **LINE_KW)
        if len(s):                                  # selective label: endpoint only
            last = s.iloc[-1]
            ax.annotate(f"{last.window_fps:.1f}",
                        xy=(last.done, last.window_fps), xytext=(6, 0),
                        textcoords="offset points", va="center",
                        fontsize=9, color=CLUSTER_COLOR[cluster])
    ax.set_title(run)
    ax.set_xlabel("Completed batches (per cluster)")
    ax.set_ylim(0, ymax)
    ax.grid(axis="x", visible=False)

axes[0].set_ylabel("Rolling window FPS")
axes[0].legend(loc="upper left")                    # legend on the first panel only
fig.suptitle("Rolling window FPS per cluster", fontsize=14,
             fontweight="semibold", color=INK, y=1.02)
fig.tight_layout()
finish(fig, "02_window_fps_timeline_by_cluster.png")
```

**Pitfalls**
- `sharey=True` is mandatory for small multiples — independent y-scales make the
  panels non-comparable, which is the entire point of faceting.
- Compute `ymax` from the *whole* frame, not per panel.
- One legend, on the first panel. Repeating it in every panel is noise.
- `suptitle` needs `y=1.02` plus `tight_layout()` or it overlaps panel titles.

---

## C3 · Overlaid timeline with reference lines

**When** exactly 2–3 series over the same index and the comparison *is* the
point (so faceting would hide it).
**Data** long frame with `series`, `x`, `y`.

```python
fig, ax = plt.subplots(figsize=(12, 4.6))

for run in RUN_ORDER:
    s = df[df.run == run].sort_values("batch")
    m = s.window_fps.mean()
    ax.plot(s.batch, s.window_fps, color=MODE_COLOR[run],
            label=f"{run}  (mean {m:.2f})", alpha=0.9, **LINE_KW)
    ax.axhline(m, color=MODE_COLOR[run], linewidth=1.0, alpha=0.45)

ax.set_xlabel("Completed batch index (system-wide)")
ax.set_ylabel("Rolling window FPS")
ax.set_title("System-wide rolling window FPS over the run")
ax.set_ylim(0, df.window_fps.max() * 1.10)
ax.grid(axis="x", visible=False)
ax.legend(loc="upper left")
finish(fig, "03_system_window_fps_timeline.png")
```

**Pitfalls**
- Put the summary statistic **in the legend label**, not in a floating
  annotation — it stays attached to the series and never collides.
- Reference lines: same hue, thinner, `alpha≈0.45`. They must recede.
- `alpha=0.9` on noisy overlapping lines lets crossings read. Do not go below
  ~0.8 or the hue shifts toward the surface and breaks the contrast check.

---

## C4 · Grouped boxplot — distribution across categories

**When** spread and skew matter, not just the mean.
**Data** one array of raw values per (category, series).

```python
clusters = ["Cluster 0", "Cluster 1"]
fig, ax = plt.subplots(figsize=(8.6, 4.9))

positions, data, colors = [], [], []
for ci, cluster in enumerate(clusters):
    for ri, run in enumerate(RUN_ORDER):
        vals = df[(df.cluster == cluster) & (df.run == run)].window_fps.values
        positions.append(ci + (ri - 0.5) * 0.34)
        data.append(vals)
        colors.append(MODE_COLOR[run])

bp = ax.boxplot(data, positions=positions, widths=0.28, patch_artist=True,
                showfliers=False,
                medianprops=dict(color=SURFACE, linewidth=1.8),  # reads on the fill
                whiskerprops=dict(color=AXIS, linewidth=1.0),
                capprops=dict(color=AXIS, linewidth=1.0))
for patch, c in zip(bp["boxes"], colors):
    patch.set_facecolor(c); patch.set_edgecolor(SURFACE); patch.set_linewidth(1.2)

# Label the mean ABOVE THE WHISKER CAP — p75 sits inside the whisker.
for pos, vals in zip(positions, data):
    if not len(vals):
        continue
    q1, q3 = np.percentile(vals, [25, 75])
    whisker_top = vals[vals <= q3 + 1.5 * (q3 - q1)].max()
    ax.annotate(f"{vals.mean():.1f}", xy=(pos, whisker_top),
                xytext=(0, 7), textcoords="offset points",
                ha="center", fontsize=9, color=INK_2)

handles = [plt.Rectangle((0, 0), 1, 1, color=MODE_COLOR[r]) for r in RUN_ORDER]
ax.legend(handles, RUN_ORDER, loc="upper left")
ax.set_xticks(range(len(clusters)), clusters)
ax.set_ylabel("Rolling window FPS"); ax.set_xlabel("Cluster")
ax.set_title("Window FPS distribution by cluster  (box = IQR, label = mean)")
ax.grid(axis="x", visible=False)
finish(fig, "04_window_fps_distribution.png")
```

**Pitfalls**
- `ax.boxplot` produces no legend handles — build `plt.Rectangle` proxies.
- Median in **surface color**, not black: it must read against a saturated fill.
- Annotating at p75 puts the text on top of the whisker. Compute the cap.
- `showfliers=False` for dense series; otherwise outlier dots dominate. Say so
  in the title/subtitle when you suppress them.
- Say what the box *is* in the title — readers do not agree on box conventions.

---

## C5 · Two-panel bars — measures of different magnitude

**When** two subsets differ by ~10× and a shared axis would flatten one. **This
is the dual-axis replacement.**
**Data** same measure, split by a class column.

```python
roles, clusters = ["cloud", "edge"], ["Cluster 0", "Cluster 1"]
fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.8))   # NOT sharey — that's the point
x, width = np.arange(len(clusters)), 0.36

for ax, role in zip(axes, roles):
    piv = (svc[svc.role == role]
           .pivot(index="scope", columns="run", values="mean_ms")
           .reindex(clusters)[RUN_ORDER] / 1000.0)     # ms -> s at the edge
    for i, run in enumerate(RUN_ORDER):
        b = ax.bar(x + (i - 0.5)*(width + 0.03), piv[run], width,
                   label=run, color=MODE_COLOR[run], **BAR_KW)
        label_bars(ax, b, fmt="{:.2f}s")
    ax.set_xticks(x, clusters)
    ax.set_title(f"{role.capitalize()} devices")
    ax.set_ylim(0, piv.to_numpy().max() * 1.20)
    ax.grid(axis="x", visible=False)

axes[0].set_ylabel("Mean service latency (s)")
axes[0].legend(loc="upper left")
fig.suptitle("Mean service latency by device role", fontsize=14,
             fontweight="semibold", color=INK, y=1.02)
fig.tight_layout()
finish(fig, "05_service_latency_by_role.png")
```

**Pitfalls**
- Deliberately **omit** `sharey` here — that is what makes this legal where a
  dual axis is not. Each panel is honestly its own scale, and the panel titles
  say which is which.
- Convert units once, at the pivot, not inside the label formatter.
- Every panel needs its own `ylim` headroom.

---

## C6 · Multi-facet grouped bar — a statistic profile

**When** several summary statistics (mean/p50/p95/max) across several scopes.
**Data** one row per (scope, series) with one column per statistic.

```python
stats  = [("mean_ms", "Mean"), ("p50_ms", "p50"), ("p95_ms", "p95"), ("max_ms", "Max")]
scopes = ["Cluster 0", "Cluster 1", "System"]

fig, axes = plt.subplots(1, 3, figsize=(14.5, 4.8), sharey=True)
x, width = np.arange(len(stats)), 0.36
ymax = e2e[[c for c, _ in stats]].to_numpy().max() / 1000.0

for ax, scope in zip(axes, scopes):
    sub = e2e[e2e.scope == scope].set_index("run")
    for i, run in enumerate(RUN_ORDER):
        vals = [sub.loc[run, col] / 1000.0 for col, _ in stats]
        b = ax.bar(x + (i - 0.5)*(width + 0.03), vals, width,
                   label=run, color=MODE_COLOR[run], **BAR_KW)
        label_bars(ax, b, fmt="{:.0f}", fontsize=8.5)
    ax.set_xticks(x, [lbl for _, lbl in stats])
    ax.set_title(scope)
    ax.set_ylim(0, ymax * 1.16)
    ax.grid(axis="x", visible=False)

axes[0].set_ylabel("End-to-end latency (s)")
axes[0].legend(loc="upper left")
fig.suptitle("End-to-end latency profile  (lower is better)", fontsize=14,
             fontweight="semibold", color=INK, y=1.02)
fig.tight_layout()
finish(fig, "06_e2e_latency_profile.png")
```

**Pitfalls**
- Put "(lower is better)" in the suptitle. Latency charts are misread otherwise.
- `sharey=True` here — the scopes are the same measure and must be comparable.
- 24 bars means smaller labels (`fontsize=8.5`) and `{:.0f}`. If they still
  collide, drop to fewer statistics rather than shrinking further.

---

## C7 · Grouped bar over composite categories

**When** the x-axis is a cross-product (cluster × role) plus a total.
**Data** an explicit `(dim1, dim2)` row list — do not rely on groupby ordering.

```python
rows   = [("Cluster 0", "cloud"), ("Cluster 0", "edge"),
          ("Cluster 1", "cloud"), ("Cluster 1", "edge"),
          ("System",    "all")]
labels = ["C0\ncloud", "C0\nedge", "C1\ncloud", "C1\nedge", "System\nall"]

idx = df_utc.set_index(["run", "scope", "role"])
x, width = np.arange(len(rows)), 0.36
fig, ax = plt.subplots(figsize=(9.6, 4.9))

for i, run in enumerate(RUN_ORDER):
    vals = [float(np.ravel(idx.loc[(run, s, r), "utilization"])[0]) for s, r in rows]
    b = ax.bar(x + (i - 0.5)*(width + 0.03), vals, width,
               label=run, color=MODE_COLOR[run], **BAR_KW)
    label_bars(ax, b, fmt="{:.1f}%", fontsize=9)

ax.set_xticks(x, labels)
ax.set_ylabel("Utilization (%)")
ax.set_ylim(0, 118)                    # percentages: fix the ceiling, don't autoscale
ax.set_title("Device utilization by cluster and role")
ax.grid(axis="x", visible=False)
ax.legend(loc="upper right")           # upper-left is occupied by the tallest bar
finish(fig, "07_utilization_by_role.png")
```

**Pitfalls**
- Two-line tick labels (`"C0\ncloud"`) beat rotation. Rotated labels are slow to
  read and eat vertical space.
- `np.ravel(...)[0]` guards against `.loc` returning a Series when the MultiIndex
  has duplicate keys.
- For percentages, set `ylim(0, 118)` explicitly — autoscale to 99.8% leaves no
  room for the label.
- The legend moved to `upper right` **because the render showed it sitting on a
  93.4% bar**. Always check.

---

## C8 · Per-entity sorted bar, colored by class

**When** showing every device/host/endpoint individually — load balance,
outlier hunting.
**Data** one row per entity with a class column.

```python
fig, axes = plt.subplots(1, 2, figsize=(13, 4.9), sharey=True)

for ax, run in zip(axes, RUN_ORDER):
    sub = (df_utd[df_utd.run == run]
           .sort_values(["role", "utilization"], ascending=[True, False])
           .reset_index(drop=True))
    pos = np.arange(len(sub))
    b = ax.bar(pos, sub.utilization,
               color=[ROLE_COLOR[r] for r in sub.role], width=0.72, **BAR_KW)
    label_bars(ax, b, fmt="{:.0f}", fontsize=8, color=MUTED)

    ticks = sub.groupby("role").cumcount() + 1          # number WITHIN each role
    ax.set_xticks(pos, [f"{r[0].upper()}{n}" for r, n in zip(sub.role, ticks)],
                  fontsize=8.5)
    ax.set_title(f"{run}  —  mean {sub.utilization.mean():.1f}%")
    ax.set_xlabel("Device  (C = cloud, E = edge)")
    ax.set_ylim(0, 118)
    ax.grid(axis="x", visible=False)

handles = [plt.Rectangle((0, 0), 1, 1, color=ROLE_COLOR[r]) for r in ["cloud", "edge"]]
axes[0].set_ylabel("Utilization (%)")
axes[0].legend(handles, ["Cloud", "Edge"], loc="upper right")
fig.suptitle("Per-device utilization", fontsize=14,
             fontweight="semibold", color=INK, y=1.02)
fig.tight_layout()
finish(fig, "08_device_utilization.png")
```

**Pitfalls**
- Sort by `[class, value]` so classes stay blocked *and* rank within a class is
  visible.
- `groupby(...).cumcount()` numbers within class → `C1 C2 C3 E1 E2 …`. Using a
  running `enumerate` gives `C1 C2 C3 E4 E5 …`, which reads as missing devices.
- Colors come from `ROLE_COLOR[r]`, an entity dict — never `palette[i]`.
- Put the aggregate in the panel title; it is the reference the bars are read
  against.

---

## C9 · Paired metric lines — two related measures

**When** two measures of the same phenomenon (mAP@50 and mAP@50:95) over the
same index.
**Data** long frame with `series`, `x`, and one column per measure.

```python
fig, axes = plt.subplots(1, 2, figsize=(13, 4.7))

for ax, (col, title) in zip(axes, [("mAP50", "mAP@50"), ("mAP50_95", "mAP@50:95")]):
    for cluster in ["Cluster 0", "Cluster 1"]:
        s = src[src.cluster == cluster].sort_values("window")
        ax.plot(s.window, s[col], color=CLUSTER_COLOR[cluster], label=cluster,
                marker="o", **LINE_KW, **MARK_KW)
        last = s.iloc[-1]
        ax.annotate(f"{last[col]:.3f}", xy=(last.window, last[col]),
                    xytext=(6, 2), textcoords="offset points",
                    fontsize=9, color=CLUSTER_COLOR[cluster])
    ax.set_title(title); ax.set_xlabel("Window index"); ax.set_ylabel(title)
    ax.grid(axis="x", visible=False)

axes[0].legend(loc="upper left")
fig.suptitle(f"Detection accuracy by sliding window  ({note})", fontsize=14,
             fontweight="semibold", color=INK, y=1.02)
fig.tight_layout()
finish(fig, "09_map_by_window.png")
```

**Pitfalls**
- Two measures on different scales → two panels, **no** `sharey`.
- Markers need the 2px surface ring (`MARK_KW`) so overlapping points separate.
- Sparse series (≤ ~30 points) get markers; dense ones do not.
- `**LINE_KW, **MARK_KW` unpack together only if the key sets are disjoint —
  they are.

---

## C10 · Three-series grouped bar (relief rule applies)

**When** three entities compared across a couple of aggregations. Uses slot 3
(aqua), which is sub-3:1 — so **every bar must carry a visible label**.

```python
scopes      = ["Cluster 0", "Cluster 1", "Overall"]
scope_color = {"Cluster 0": S1, "Cluster 1": S2, "Overall": S3}
aggs        = ["WINDOW", "ALL"]
x, width    = np.arange(len(aggs)), 0.26

fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.7))
for ax, (col, title) in zip(axes, [("mAP50", "mAP@50"), ("mAP50_95", "mAP@50:95")]):
    piv = src.pivot_table(index="agg_kind", columns="scope", values=col).reindex(aggs)
    for i, scope in enumerate(scopes):
        b = ax.bar(x + (i - 1)*(width + 0.02), piv[scope], width,
                   label=scope, color=scope_color[scope], **BAR_KW)
        label_bars(ax, b, fmt="{:.4f}", fontsize=8.5)   # relief: mandatory here
    ax.set_xticks(x, ["Window mean", "All frames"])
    ax.set_title(title); ax.set_ylabel(title)
    ax.set_ylim(0, piv.to_numpy().max() * 1.25)
    ax.grid(axis="x", visible=False)

axes[0].legend(loc="upper right", ncol=3)
fig.suptitle("Overall detection accuracy", fontsize=14,
             fontweight="semibold", color=INK, y=1.02)
fig.tight_layout()
finish(fig, "10_map_summary.png")
```

**Pitfalls**
- Three series: offset is `(i - 1)`, i.e. `(i - (n-1)/2)`. Narrower `width=0.26`.
- Aqua is in play → labels are not optional.
- `ncol=3` keeps a 3-item legend on one line instead of stacking into the plot.
- `ylim * 1.25` (not 1.18) to clear both labels and the horizontal legend.

---

## C11 · Verdict bar — headline comparison

**When** summarizing "did A beat B" across mixed metrics where higher is better
for some and worse for others.
**Data** one row per metric with both values and a `goal` direction.

```python
# goal: +1 higher is better, -1 lower is better, 0 neither
metrics = [
    ("System throughput",   dyn_fps,  spl_fps,  "{:.2f} FPS", "higher is better", +1),
    ("Mean E2E latency",    dyn_lat,  spl_lat,  "{:.1f} s",   "lower is better",  -1),
    ("System utilization",  dyn_util, spl_util, "{:.1f} %",   "workload dependent", 0),
    ("mAP@50 (all frames)", dyn_map,  spl_map,  "{:.4f}",     "higher is better", +1),
]

pct, colors, verdicts = [], [], []
for _, dyn, spl, _, _, goal in metrics:
    p = (dyn / spl - 1) * 100
    score = goal * np.sign(p)
    pct.append(p)
    colors.append(GOOD if score > 0 else BAD if score < 0 else NEUTRAL)
    verdicts.append("better" if score > 0 else "worse" if score < 0
                    else ("no change" if p == 0 else "neutral"))

y = np.arange(len(metrics))[::-1]
fig, ax = plt.subplots(figsize=(11, 4.8))
ax.barh(y, pct, height=0.5, color=colors, **BAR_KW)
ax.axvline(0, color=AXIS, linewidth=1.0)

for yi, p, v in zip(y, pct, verdicts):
    ax.annotate(f"{p:+.1f}%  {v}", xy=(p, yi), xytext=(6 if p >= 0 else -6, 0),
                textcoords="offset points", va="center",
                ha="left" if p >= 0 else "right",
                fontsize=10, fontweight="semibold", color=INK)

# Absolute values live in the tick label — no floating text to collide.
ax.set_yticks(y, [f"{n}\nA {f.format(d)}  ·  B {f.format(s)}\n({h})"
                  for n, d, s, f, h, _ in metrics], fontsize=9.5)
ax.tick_params(axis="y", colors=INK_2)
lim = max(abs(p) for p in pct) * 1.9 + 4
ax.set_xlim(-lim, lim)
ax.set_xlabel("Change of A relative to B (%)")
ax.set_title("A vs B — headline metrics  (B = baseline)")
ax.annotate("Colour marks the verdict (green better / red worse), not the sign of the change",
            xy=(0.5, -0.22), xycoords="axes fraction", ha="center",
            fontsize=9, color=MUTED)
ax.grid(axis="y", visible=False)
finish(fig, "11_summary.png", hide_spines=("top", "right", "left"))
```

**Pitfalls**
- **Color keys to the verdict, never the sign.** A `+29%` latency move is a
  regression; painting it like a `+21%` throughput gain is a lie the reader
  cannot detect. This is the single most common defect in summary charts.
- Color now carries good/bad, i.e. status semantics — so the verdict **must**
  also be in text. Never color alone.
- Multi-line tick labels carry the absolute values; a separate annotation column
  collides with the y-axis.
- `hide_spines` includes `"left"` — a diverging bar chart's reference is the
  zero line, not the axis.
- `xlim` symmetric about zero, or the bar lengths misrepresent the ratio.

---

## C12 · Stat tile — when it should not be a chart

**When** the answer is one number, or a 2-value comparison with no distribution
behind it. A one-bar bar chart and a 2-slice pie are always wrong.

```python
fig, ax = plt.subplots(figsize=(4.2, 2.4))
ax.axis("off")
ax.text(0, 0.72, "System throughput", fontsize=11, color=INK_2)
ax.text(0, 0.30, "25.22", fontsize=40, color=INK, fontweight="semibold")
ax.text(0.56, 0.36, "FPS", fontsize=13, color=MUTED, transform=ax.transAxes)
ax.text(0, 0.06, "+21.2% vs Split baseline", fontsize=10, color=GOOD)
finish(fig, "00_hero_throughput.png", hide_spines=())
```

**Pitfalls**
- Same sans as everything else. No display or serif face on a hero figure.
- **No `tabular-nums`** on a large standalone number — equal-width digits make
  `121` look loose at display sizes. Reserve it for aligned table columns.
- The delta gets a status color *and* the words "vs baseline".

---

# Part III — Files

## §III.1 · Read the inputs before designing anything

```bash
head -6 path/to/file.log && tail -3 path/to/file.log && wc -l path/to/file.log
```

Do this for **every** input file. Then look for structure that changes the plan:

```bash
# Do two runs actually differ? (strip the volatile leading timestamp first)
diff <(cut -d' ' -f2- run_a/metric.log) <(cut -d' ' -f2- run_b/metric.log)
```

## §III.2 · The generic `key=value` parser

Most instrumentation logs are `<timestamp> [FLAG...] key=value key=value`. One
parser handles all of them — do not write a bespoke regex per file.

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

Then one small function per file shape, all returning `list[dict]`:

```python
def parse_fps_summary(run, path):
    rows = []
    for ln in read_lines(path):
        ts, flags, kv = parse_kv_line(ln)
        scope = CLUSTER_LABEL.get(kv.get("cluster"),
                                  "System" if "SYSTEM" in flags else None)
        if scope is None:
            continue                       # skip lines this parser doesn't own
        rows.append(dict(run=run, scope=scope,
                         fps=num(kv.get("fps")),
                         done=num(kv.get("done")),
                         share=num(kv.get("share"))))
    return rows
```

**Conventions that matter**
- Return `list[dict]`, concatenate across runs, build the DataFrame once. Never
  `pd.concat` in a loop.
- Every parser tags rows with its `run`/source so runs stack into one tidy frame.
- `continue` on lines the parser does not own, rather than raising — mixed-format
  logs are the norm.
- Map raw identifiers to display labels at parse time
  (`CLUSTER_LABEL = {"intermediate_queue_0": "Cluster 0"}`), so no chart code
  ever contains a raw ID.
- Normalize units at the *edge* (ms→s, `%`→float) in one place.

**Handle positional-value lines** (no `key=`) explicitly — e.g. a file that is
`<ts>` during warm-up and `<ts> <value>` afterwards:

```python
def parse_batch_done(run, path):
    rows, idx = [], 0
    for ln in read_lines(path):
        parts = ln.split()
        idx += 1
        if len(parts) == 2:            # warm-up rows have no value yet
            rows.append(dict(run=run, batch=idx,
                             ts=int(parts[0]), window_fps=float(parts[1])))
    return rows
```

## §III.3 · Map every input to the charts it feeds

Write this table into the notebook's opening markdown cell. It is how a reader
(and future-you) knows nothing was silently dropped.

| Input file | Shape | Feeds |
|---|---|---|
| `fps_cluster.log` | 1 row per cluster + `SYSTEM` summary | C1 |
| `fps_cluster_ns.log` | per-cluster rolling window samples | C2, C4 |
| `batch_done_ns.log` | system-wide rolling window samples | C3 |
| `latency_cluster.log` | service (per role) + e2e stats | C5, C6 |
| `utilization_cluster.log` | utilization by cluster × role | C7 |
| `utilization.log` | 1 row per device | C8 |
| `map_window.log` | accuracy per sliding window | C9 |
| `map.log` | accuracy summary | C10 |
| *(derived)* | headline metrics from several files | C11 |

If an input feeds nothing, either chart it or say in the notebook why not.

## §III.4 · Verify assumptions before charting them

Compute what you are about to assert visually, and branch on the result:

```python
piv = df_mw.pivot_table(index=["cluster", "window"], columns="run",
                        values=["mAP50", "mAP50_95"])
d50   = (piv[("mAP50", "A")]    - piv[("mAP50", "B")]).abs().max()
d5095 = (piv[("mAP50_95", "A")] - piv[("mAP50_95", "B")]).abs().max()
MAP_IDENTICAL = bool(d50 == 0 and d5095 == 0)
print("=> Accuracy is IDENTICAL across both modes." if MAP_IDENTICAL
      else "=> Accuracy differs; both are plotted separately.")
```

Then let the charts consume the flag:

```python
src  = df_mw[df_mw.run == "A"] if MAP_IDENTICAL else df_mw
note = "identical for A and B" if MAP_IDENTICAL else "per run"
```

**Why this matters.** Plotting two identical series draws two perfectly
overlapping lines and implies a comparison that does not exist — the reader sees
one line and cannot tell whether the other is hidden or missing. Keeping the
branch in the notebook means it self-corrects when a future run diverges.

Other assumptions worth an explicit check: identical record counts across runs,
no missing entities, monotonic timestamps, and units (is that field ms or ns?).

## §III.5 · Output files

```
results/<run-date>/imgs/
├── 01_fps_by_cluster.png
├── 02_window_fps_timeline_by_cluster.png
└── ...
```

- `NN_snake_case_topic.png` — the numeric prefix keeps narrative order in a
  directory listing.
- `dpi=300`, `bbox_inches="tight"`, `facecolor=SURFACE` (a transparent
  background renders as black in dark-mode viewers).
- The output directory is created in the setup cell, never assumed:
  `IMG_DIR.mkdir(parents=True, exist_ok=True)`.
- Numbering is stable across re-runs. If a chart is dropped, leave the gap
  rather than renumbering — links and references elsewhere stay valid.
- Final cell prints a manifest so the run is auditable:

```python
print(f"{len(SAVED)} charts written to {IMG_DIR}\n")
for name in SAVED:
    print(f"  {name:<42} {(IMG_DIR/name).stat().st_size/1024:8.1f} KB")
```

---

# Part IV — Build pipeline

## §IV.1 · Layout

```
project/
├── results/
│   ├── <run-date>/
│   │   ├── <run-a>/*.log        inputs
│   │   ├── <run-b>/*.log
│   │   └── imgs/                outputs  ← charts land here
│   └── visual/
│       └── <Name> Visualization.ipynb    deliverable
└── <scratch>/
    ├── build_nb.py              emits the notebook
    ├── run_nb.py                executes it, reports all errors
    └── validate_palette.py      §3
```

Keep `build_nb.py` / `run_nb.py` in a scratch directory unless the user wants
them committed — the notebook is the deliverable.

## §IV.2 · Generate the notebook from a builder script

**Do not hand-author `.ipynb` JSON, and do not edit cells one at a time.** A
builder script makes the whole notebook regenerable after any fix, keeps cell
order under version control, and means a style change is one edit rather than
twenty.

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
| **A** | `results/<date>/a` |
| **B** | `results/<date>/b` |

Both runs process the identical workload. Charts are written to
`results/<date>/imgs/`.
""")

md("## 0 · Setup — paths, palette, chart style")
code(r'''
<the §4 style block, plus ROOT / RESULTS / IMG_DIR>
''')

md("## 1 · Log parsers")
code(r'''
<the §III.2 parsers>
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
- Use `r'''...'''` for code cells so backslashes in Windows paths survive.
- One markdown heading + one code cell per chart. Never two charts in a cell —
  an error in the first hides the second.
- Cell 0 states what the runs are and where images go.
- Kernel metadata must be present or `nbclient` cannot pick an executor.

## §IV.3 · Execute headless and collect every error

```python
"""run_nb.py — execute the notebook in place, report all failures."""
import sys, nbformat
from nbclient import NotebookClient
from pathlib import Path

p  = Path(r"D:\...\results\visual\Result Visualization.ipynb")
nb = nbformat.read(str(p), as_version=4)

NotebookClient(nb, timeout=600, kernel_name="python3",
               resources={"metadata": {"path": str(p.parent)}},
               allow_errors=True).execute()      # <- collect, don't stop at #1
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
- `nbclient` + `ipykernel` are enough. `nbconvert` is **not** required and is
  often not installed.
- `allow_errors=True` is the important flag — you get every failing cell per
  run instead of one round-trip per bug.
- Executing writes outputs (including base64 PNGs) back into the `.ipynb`, so
  the delivered notebook renders without a re-run. Expect ~0.5–2 MB.
- A `zmq`/`Proactor event loop` RuntimeWarning on Windows is harmless.
- Re-running regenerates every PNG. `rm imgs/*.png` first if you want to be
  certain nothing stale survives a renumbering.

## §IV.4 · Look at the output — the step that finds the defects

```
Read d:\...\imgs\01_fps_by_cluster.png
```

Read them in batches of ~3 (they are large in context). Prioritize charts with
annotations, legends, or many bars — simple grouped bars rarely break.

Check each image for:

| Defect | Look for |
|---|---|
| Legend over a mark | default `upper left` on a chart with a tall leading bar |
| Label / whisker collision | annotations anchored to a percentile inside the whisker |
| Clipped labels | `ylim` too tight, or long tick labels cut at the figure edge |
| Overlapping tick labels | too many categories for the width |
| Misleading color | delta charts colored by sign instead of verdict |
| Series order flipped | pivot not re-indexed, so colors moved between entities |
| Invisible series | two identical series overlapping — §III.4 |
| Wrong units | axis says `s`, numbers are clearly `ms` |

Fix in `build_nb.py`, then re-run **both** scripts. Never patch the `.ipynb`
directly — the next build silently reverts it.

Expected: ~10 charts, 2 rounds of fixes, most of them layout.

---

# Part V — Gotchas & checklist

## §V.1 · pandas attribute shadowing — silent, not loud

`agg`, `max`, `min`, `sum`, `mean`, `count`, `size`, `mode`, `all`, `any`,
`filter`, `pop`, `name`, `index`, `values`, `shape`, `T`, `apply`, `where`,
`first`, `last`, `keys`, `items`, `plot`, `style`, `abs`, `add`, `rank`, `round`
are DataFrame attributes. A **column** with one of those names is shadowed:
attribute access returns the bound method, and comparing it yields an all-False
mask **without raising**.

```python
df[df.agg == "ALL"]        # -> empty. No error. Compares a method to a str.
df[df["agg"] == "ALL"]     # -> correct
```

Symptom: a chart silently loses a series, or `.loc["X"]` raises a bewildering
`KeyError: 'X'` on a frame you can see contains `X`.

**Fix:** rename the column at parse time (`agg` → `agg_kind`) so the trap cannot
be stepped on again downstream. Bracket access alone only fixes the call site
you happened to notice.

## §V.2 · matplotlib

- `ax.boxplot` returns no legend handles — build `plt.Rectangle` proxies.
- `fig.suptitle` needs `y=1.02` **and** `fig.tight_layout()`, or it overlaps
  panel titles.
- `bbox_inches="tight"` can still clip annotations placed outside the axes with
  `xycoords="axes fraction"`. Verify in the PNG.
- `sharey=True` is right for small multiples of the *same* measure and wrong for
  panels that exist precisely because the scales differ (C5).
- `ax.annotate(..., textcoords="offset points")` keeps labels put across dpi and
  figsize changes; data-coordinate offsets do not.
- Set `savefig.facecolor` — otherwise the PNG background is transparent and
  renders black in dark-mode viewers.

## §V.3 · Environment

- **`node` is usually absent on Windows** — port JS validators to Python (§3),
  do not skip the checks.
- `font.family` needs a fallback chain. `Segoe UI` exists on Windows; most named
  fonts do not. A missing font is a silent fallback plus console spam.
- Use absolute paths from a single `ROOT` constant so the notebook runs from any
  working directory. On Windows use raw strings: `Path(r"D:\...")`.
- Percent-formatted values (`utilization=37.82%`) need the `%` stripped before
  `float()` — do it in one shared `num()` helper.
- Nanosecond timestamps overflow float precision. Keep them as `int`, and
  subtract a baseline before converting to seconds.

## §V.4 · Pre-ship checklist

**Data**
- [ ] Every input file read, and its record count sanity-checked
- [ ] Assumptions about identical/differing runs verified in code, not assumed
- [ ] Units normalized at the parser, not at the label
- [ ] No column shadows a DataFrame attribute

**Encoding**
- [ ] No dual-axis plot anywhere
- [ ] Categorical hues taken in slot order; ≤ 3 series in any all-pairs form
- [ ] Colors bound to entities via dicts, not to positional index
- [ ] Palette validated by running the checks, not by eye
- [ ] Sub-3:1 fills (aqua / yellow / magenta) carry visible direct labels
- [ ] Delta charts colored by verdict, with the verdict in text

**Marks**
- [ ] Gridlines solid hairlines; top/right spines hidden
- [ ] Surface-colored gap between adjacent fills; no black borders
- [ ] Legend present for every ≥ 2-series chart, absent for single-series
- [ ] No legend covering a mark
- [ ] Direct labels selective, none clipped or overlapping
- [ ] Axis labels carry units; "lower is better" stated where relevant

**Delivery**
- [ ] Notebook executes top-to-bottom with **zero** cell errors
- [ ] **Every output image opened and visually inspected**
- [ ] Manifest cell lists every written file
- [ ] Builder script can regenerate the whole notebook from scratch
