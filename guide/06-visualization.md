# 06 · Visualization method

**Read this before writing a single line of chart code.** Recipes are in
[07-chart-catalogue.md](07-chart-catalogue.md); the notebook that runs them is in
[08-build-pipeline.md](08-build-pipeline.md).

> If a `dataviz` skill is available in your environment, load it first. This file is the
> matplotlib-specific operational version and does not replace the full reference.

---

## 1 · The order of operations

Color comes **last**. Most bad charts pick colors first.

| Step | Decide | Driven by |
|---|---|---|
| 1 | **Form** | the data's job: magnitude, identity, change over time, polarity, one headline |
| 2 | **Encoding** | which variable maps to position, which to color, which to facet |
| 3 | **Color role** | categorical / sequential / diverging / status |
| 4 | **Validate** | computed checks, never eyeballed (§4) |
| 5 | **Marks** | thin marks, hairline grid, selective labels (§5) |
| 6 | **Verify** | render it and *look* ([08 §5](08-build-pipeline.md)) |

### Picking the form

| The data's job | Form | Recipe |
|---|---|---|
| Compare magnitudes across categories | Bar | [C1](07-chart-catalogue.md) |
| Change over a continuous index | Line | [C2, C3](07-chart-catalogue.md) |
| Distribution / spread | Box | [C4](07-chart-catalogue.md) |
| **Two measures on different scales** | **Two panels — never a dual axis** | [C5](07-chart-catalogue.md) |
| A statistic profile (mean/p50/p95/max) | Faceted grouped bar | [C6](07-chart-catalogue.md) |
| Every entity individually | Sorted bar, colored by class | [C8](07-chart-catalogue.md) |
| Events against a timeline | Line + vertical rules | [C9](07-chart-catalogue.md) |
| Mixed-direction comparison | Verdict bar | [C10](07-chart-catalogue.md) |
| One number that *is* the story | **Stat tile — not a chart** | [C11](07-chart-catalogue.md) |

A one-bar bar chart and a two-slice pie are always wrong. The number is the chart.

---

## 2 · Non-negotiables

- **Never a dual-axis chart** (two y-scales on one plot). The alignment between the
  scales is arbitrary, so the chart invents a correlation that isn't in the data. Two
  measures of different scale → two panels, small multiples, or both indexed to a common
  base (=100 at t₀) on one axis. **This is the #1 chart mistake.**
- **Categorical hues in fixed slot order, never cycled.** A 9th series is never a
  generated hue — fold it into "Other" or facet.
- **Color follows the entity, not its rank.** Build `{entity: color}` dicts. Never
  `color=palette[i]` over a filtered list — filtering then repaints the survivors, and a
  reader who learned "edge is orange" is now misled.
- **Sequential = one hue, light→dark. Diverging = two hues + neutral gray midpoint.**
  Never a rainbow; never a hue at the diverging midpoint.
- **Gridlines and axes are solid hairlines**, one shade off the surface. Never dashed —
  dashing reads as "threshold" or "projection" when it is just a grid.
- **A legend whenever there are ≥ 2 series** (one series needs none — the title names
  it). Direct-label *selectively*: the endpoint, the extreme, the series that matters.
  Never a number on every point of a dense line.
- **Status colors are reserved** for good/warning/serious/critical and always ship with a
  text label, never color alone. Never reuse them as "series 4".
- **Text wears text tokens, never the series color.** (One exception: an endpoint label
  on a line may take the series color — it *is* the identity cue.)
- **No border drawn around marks to separate them.** Use a surface-colored gap.

---

## 3 · Palette

Take slots **in this order** — the ordering is the colorblind-safety mechanism, not
cosmetic.

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
| Scatter, bubble, small multiples | **all pairs** | **3 slots** |

Slot 4 (yellow) sits beside slot 2 (orange), and that pair fails the all-pairs floors —
which is why the all-pairs cap is 3, not 4.

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
`#cde2fb → #86b6ef → #3987e5 → #256abf → #104281`. For a *discrete ordered* ramp, start
no lighter than `#86b6ef` on the light surface.

**Diverging:** blue `#2a78d6` ↔ red `#e34948`, neutral gray midpoint `#f0efec`.

**Status** (never themed, never reused as series colors):
good `#0ca30c` · warning `#fab219` · serious `#ec835a` · critical `#d03b3b`.

**Relief rule.** Three light-mode slots are below 3:1 contrast on the light surface —
aqua (2.74), yellow (2.11), magenta (2.62). Using any of them **obligates** visible
direct labels or a table view. Not dismissable.

### Recommended fixed assignments for this result format

Bind these once in the setup cell so every chart in every project agrees:

```python
ROLE_COLOR  = {"cloud": S1, "edge": S2}                  # or your role names
GROUP_COLOR = {"Group 0": S1, "Group 1": S2, "System": S3}
RUN_COLOR   = {"<run-a>": S1, "<run-b>": S2}             # comparison runs
```

---

## 4 · Validate the palette — compute, don't eyeball

| Check | Threshold |
|---|---|
| Lightness band (OKLCH L) | light `0.43–0.77`, dark `0.48–0.67` |
| Chroma floor (OKLCH C) | `≥ 0.10` (below it a hue reads gray) |
| CVD separation (OKLab ΔE×100, protan/deutan, Machado 2009 @ severity 1.0) | `≥ 8` target; `6–8` legal **only** with secondary encoding |
| Normal-vision floor (unsimulated ΔE×100) | `≥ 15` — **hard gate**, secondary encoding does not excuse it |
| Contrast vs surface (WCAG) | `≥ 3:1`, else relief required |

Pairlist is **adjacent** for bars/lines/stacks, **all pairs** for
scatter/bubble/small-multiples.

`node` is frequently absent on Windows dev boxes, so a Python implementation ships as
`guide/validate_palette.py`. **Sanity-check it before trusting it:**

```bash
# 8 slots, adjacent  -> worst CVD 9.1 (protan), worst normal-vision 19.6
python guide/validate_palette.py "#2a78d6,#eb6834,#1baf7a,#eda100,#e87ba4,#008300,#4a3aa7,#e34948" light adjacent

# slots 1-3, all pairs -> worst CVD 9.2 (deutan), worst normal-vision 24.0
python guide/validate_palette.py "#2a78d6,#eb6834,#1baf7a" light all
```

If your numbers differ, the implementation is wrong — fix it before using the results.

---

## 5 · Style block and helpers

Paste verbatim into the notebook's setup cell.

```python
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
from pathlib import Path

# ---- tokens -------------------------------------------------------------
SURFACE = "#fcfcfb"; PAGE  = "#f9f9f7"
INK     = "#0b0b0b"; INK_2 = "#52514e"
MUTED   = "#898781"; GRID  = "#e1e0d9"; AXIS = "#c3c2b7"

S1, S2, S3 = "#2a78d6", "#eb6834", "#1baf7a"      # categorical slots 1-3
GOOD, BAD, NEUTRAL = "#0ca30c", "#d03b3b", MUTED   # status + neutral

# ---- rcParams -----------------------------------------------------------
mpl.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,          # else the PNG is transparent -> black in dark mode
    "font.family": ["Segoe UI", "DejaVu Sans", "sans-serif"],
    "font.size": 10,
    "axes.titlesize": 13, "axes.titleweight": "semibold",
    "axes.titlecolor": INK, "axes.titlepad": 12,
    "axes.labelsize": 10.5, "axes.labelcolor": INK_2,
    "axes.edgecolor": AXIS, "axes.linewidth": 0.8,
    "axes.grid": True, "axes.axisbelow": True,
    "grid.color": GRID, "grid.linestyle": "-", "grid.linewidth": 0.8,   # solid
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

Applied in every recipe:

```python
ax.grid(axis="x", visible=False)      # vertical bars: no grid on the category axis
ax.set_ylim(0, values.max() * 1.18)   # headroom so labels clear the top spine
```

---

## 6 · Anti-patterns — check every chart against this

| ❌ | ✅ |
|---|---|
| Dual-axis chart | two panels, or index to a common base |
| Color assigned by current rank | color follows the entity |
| A 9th generated hue | fold to "Other", or facet |
| Eyeballing colorblind safety | run the validator |
| A value-ramp on nominal categories | one color for all bars; ramps are for ordered data |
| Rainbow sequential | one hue, light→dark |
| Status color used for a plain series | categorical slots for identity, status for state |
| Eight hues when the story is one number | emphasis, or a stat tile |
| A one-bar bar chart / 2-slice pie | a stat tile |
| Thick saturated blocks, heavy grid | thin marks, hairline grid, generous padding |
| Dashed gridlines | solid hairlines |
| A number on every data point | selective direct labels + the axis |
| A border drawn around marks | a 2px surface gap |
| A label clipped by a too-small bar | move it outside the bar, or drop it to the manifest |
| Fixed container height that cuts the axis band | size to include the axis labels |
| `tabular-nums` on a large standalone number | proportional figures on hero values |
| Delta colored by sign instead of verdict | color the verdict, spell it out in text |
