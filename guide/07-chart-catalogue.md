# 07 · Chart catalogue

Eleven recipes covering every file in [01-result-format.md](01-result-format.md). Each
states **when to use**, the **data shape**, working **code**, and the **pitfalls** that
actually bite.

Read [06-visualization.md](06-visualization.md) first — the style block and helpers
(`BAR_KW`, `LINE_KW`, `MARK_KW`, `finish`, `label_bars`) are assumed to be in scope.

Throughout: `RUN_ORDER = ["<run-a>", "<run-b>"]` is the fixed series order, and
`x, width = np.arange(n), 0.36` is the standard grouped-bar geometry.

---

## The chart list

| # | File | Source log | Answers |
|---|---|---|---|
| C1 | `01_rate_by_group.png` | `group_rate.log` | How fast, per group and overall? |
| C2 | `02_window_rate_by_group.png` | `group_rate_ns.log` | How did each group behave over time? |
| C3 | `03_system_window_rate.png` | `batch_done_ns.log` | How did the system behave over time? |
| C4 | `04_window_rate_distribution.png` | `group_rate_ns.log` | How stable was the rate? |
| C5 | `05_service_latency_by_role.png` | `latency_group.log` | How fast is each device class? |
| C6 | `06_e2e_latency_profile.png` | `latency_group.log` | What does the latency tail look like? |
| C7 | `07_utilization_by_role.png` | `utilization_group.log` | Is the work split correctly? |
| C8 | `08_device_utilization.png` | `utilization.log` | Is any device a straggler? |
| C9 | `09_events_over_time.png` | `events_ns.log` + `batch_done_ns.log` | Did control decisions change throughput? |
| C10 | `10_run_comparison.png` | derived | Did run A beat run B? |
| C11 | `00_hero_<metric>.png` | derived | The one headline number |

Numbering is stable. If you drop a chart, leave the gap.

---

## C1 · Rate by group and system

**When** the headline throughput comparison. The default first chart.
**Data** `group_rate.log` → `scope`, `run`, `fps`.

```python
order = ["Group 0", "Group 1", "System"]
piv = df_rate.pivot(index="scope", columns="run", values="fps").reindex(order)[RUN_ORDER]

x, width = np.arange(len(order)), 0.36
fig, ax = plt.subplots(figsize=(8.2, 4.8))

for i, run in enumerate(RUN_ORDER):
    off = (i - 0.5) * (width + 0.03)          # 0.03 = the surface gap
    bars = ax.bar(x + off, piv[run], width,
                  label=run, color=RUN_COLOR[run], **BAR_KW)
    label_bars(ax, bars, fmt="{:.2f}")

ax.set_xticks(x, order)
ax.set_ylabel("Throughput (items/s)")
ax.set_ylim(0, piv.to_numpy().max() * 1.18)
ax.set_title("Throughput by group — <run-a> vs <run-b>")
ax.grid(axis="x", visible=False)
ax.legend(loc="upper left")

gain = (piv.loc["System", RUN_ORDER[0]] / piv.loc["System", RUN_ORDER[1]] - 1) * 100
ax.annotate(f"{RUN_ORDER[0]} delivers {gain:+.1f}% system throughput",
            xy=(0.5, -0.16), xycoords="axes fraction",
            ha="center", fontsize=9.5, color=MUTED)

finish(fig, "01_rate_by_group.png")
```

**Pitfalls**
- `.reindex(order)` — without it pandas sorts alphabetically and narrative order is lost.
- `[RUN_ORDER]` pins series order so colors stay bound to runs.
- Offsets are `(i - (n-1)/2) * (width + gap)`; for n=2 that is `(i - 0.5)`.
- **Group bars do not sum to the System bar** and should not be expected to — see
  [01 §3.3](01-result-format.md). Do not add a "total" annotation implying they do.
- `loc="upper left"` collides with a tall leading bar more often than not.

---

## C2 · Window rate per group, faceted by run

**When** four or more lines would otherwise share one axis.
**Data** `group_rate_ns.log` → `run`, `cluster`, `done`, `window_fps`.

```python
fig, axes = plt.subplots(1, 2, figsize=(13, 4.6), sharey=True)
ymax = df_tl.window_fps.max() * 1.10

for ax, run in zip(axes, RUN_ORDER):
    sub_run = df_tl[df_tl.run == run]
    for group in ["Group 0", "Group 1"]:
        s = sub_run[sub_run.cluster == group].sort_values("done")
        ax.plot(s.done, s.window_fps, color=GROUP_COLOR[group], label=group, **LINE_KW)
        if len(s):                                  # selective label: endpoint only
            last = s.iloc[-1]
            ax.annotate(f"{last.window_fps:.1f}",
                        xy=(last.done, last.window_fps), xytext=(6, 0),
                        textcoords="offset points", va="center",
                        fontsize=9, color=GROUP_COLOR[group])
    ax.set_title(run)
    ax.set_xlabel("Completed units (per group)")
    ax.set_ylim(0, ymax)
    ax.grid(axis="x", visible=False)

axes[0].set_ylabel("Rolling window rate (items/s)")
axes[0].legend(loc="upper left")                    # legend on the first panel only
fig.suptitle("Rolling window rate per group", fontsize=14,
             fontweight="semibold", color=INK, y=1.02)
fig.tight_layout()
finish(fig, "02_window_rate_by_group.png")
```

**Pitfalls**
- `sharey=True` is mandatory for small multiples — independent scales defeat the purpose.
- Compute `ymax` from the whole frame, not per panel.
- One legend, on the first panel. Repeating it in every panel is noise.
- `suptitle` needs `y=1.02` **plus** `tight_layout()` or it overlaps panel titles.
- The x-axis is *per-group* completion count, so groups end at different x. That is
  correct — do not force a shared x-max.

---

## C3 · System window rate with reference lines

**When** exactly 2–3 runs over the same index and the comparison *is* the point.
**Data** `batch_done_ns.log` → `run`, `batch`, `window_fps`.

```python
fig, ax = plt.subplots(figsize=(12, 4.6))

for run in RUN_ORDER:
    s = df_batch[df_batch.run == run].sort_values("batch")
    m = s.window_fps.mean()
    ax.plot(s.batch, s.window_fps, color=RUN_COLOR[run],
            label=f"{run}  (mean {m:.2f})", alpha=0.9, **LINE_KW)
    ax.axhline(m, color=RUN_COLOR[run], linewidth=1.0, alpha=0.45)

ax.set_xlabel("Completed unit index (system-wide)")
ax.set_ylabel("Rolling window rate (items/s)")
ax.set_title("System-wide rolling window rate over the run")
ax.set_ylim(0, df_batch.window_fps.max() * 1.10)
ax.grid(axis="x", visible=False)
ax.legend(loc="upper left")
finish(fig, "03_system_window_rate.png")
```

**Pitfalls**
- Put the summary statistic **in the legend label**, not in a floating annotation — it
  stays attached to the series and can never collide.
- Reference lines: same hue, thinner, `alpha≈0.45`. They must recede.
- `alpha=0.9` lets crossings read. Below ~0.8 the hue shifts toward the surface and
  breaks the contrast check.
- Remember the first `W-1` units have no `window_fps` — the line legitimately starts at
  x≈16, not x=1 ([01 §3.1](01-result-format.md)).

---

## C4 · Window rate distribution

**When** stability matters, not just the average. A high mean with a wide box is a worse
result than a slightly lower mean with a tight one.
**Data** `group_rate_ns.log` → raw `window_fps` arrays per (group, run).

```python
groups = ["Group 0", "Group 1"]
fig, ax = plt.subplots(figsize=(8.6, 4.9))

positions, data, colors = [], [], []
for ci, group in enumerate(groups):
    for ri, run in enumerate(RUN_ORDER):
        vals = df_tl[(df_tl.cluster == group) & (df_tl.run == run)].window_fps.values
        positions.append(ci + (ri - 0.5) * 0.34)
        data.append(vals)
        colors.append(RUN_COLOR[run])

bp = ax.boxplot(data, positions=positions, widths=0.28, patch_artist=True,
                showfliers=False,
                medianprops=dict(color=SURFACE, linewidth=1.8),   # reads on the fill
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

handles = [plt.Rectangle((0, 0), 1, 1, color=RUN_COLOR[r]) for r in RUN_ORDER]
ax.legend(handles, RUN_ORDER, loc="upper left")
ax.set_xticks(range(len(groups)), groups)
ax.set_ylabel("Rolling window rate (items/s)"); ax.set_xlabel("Group")
ax.set_title("Window rate distribution by group  (box = IQR, label = mean)")
ax.grid(axis="x", visible=False)
finish(fig, "04_window_rate_distribution.png")
```

**Pitfalls**
- `ax.boxplot` produces **no legend handles** — build `plt.Rectangle` proxies.
- Median in **surface color**, not black: it must read against a saturated fill.
- Annotating at p75 puts the text on top of the whisker. Compute the cap.
- Say what the box *is* in the title — readers do not agree on box conventions.
- `showfliers=False` on dense series; say so in the title when you suppress them.

---

## C5 · Service latency by role — two panels

**When** two device classes differ by ~10× and a shared axis would flatten one.
**This is the dual-axis replacement.**
**Data** `latency_group.log` where `kind=service` → `scope`, `role`, `run`, `mean_ms`.

```python
roles, groups = ["cloud", "edge"], ["Group 0", "Group 1"]
fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.8))   # NOT sharey — that is the point
x, width = np.arange(len(groups)), 0.36

for ax, role in zip(axes, roles):
    piv = (svc[svc.role == role]
           .pivot(index="scope", columns="run", values="mean_ms")
           .reindex(groups)[RUN_ORDER] / 1000.0)      # ms -> s once, at the pivot
    for i, run in enumerate(RUN_ORDER):
        b = ax.bar(x + (i - 0.5)*(width + 0.03), piv[run], width,
                   label=run, color=RUN_COLOR[run], **BAR_KW)
        label_bars(ax, b, fmt="{:.2f}s")
    ax.set_xticks(x, groups)
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
- Deliberately **omit** `sharey` here — that is what makes this legal where a dual axis is
  not. Each panel is honestly its own scale and the panel titles say which is which.
- Convert units once, at the pivot — never inside the label formatter.
- Chart `kind=service`, not `pipeline`. `pipeline` measures your queue depth, not your
  devices ([04 §2.2](04-latency.md)). If you want to show buffering, chart both **as
  separate panels** and say so in the title.

---

## C6 · End-to-end latency profile

**When** showing the tail, not just the average.
**Data** `latency_group.log` where `kind=e2e` → one row per (scope, run) with
`mean_ms`, `p50_ms`, `p95_ms`, `max_ms`.

```python
stats  = [("mean_ms", "Mean"), ("p50_ms", "p50"), ("p95_ms", "p95"), ("max_ms", "Max")]
scopes = ["Group 0", "Group 1", "System"]

fig, axes = plt.subplots(1, 3, figsize=(14.5, 4.8), sharey=True)
x, width = np.arange(len(stats)), 0.36
ymax = e2e[[c for c, _ in stats]].to_numpy().max() / 1000.0

for ax, scope in zip(axes, scopes):
    sub = e2e[e2e.scope == scope].set_index("run")
    for i, run in enumerate(RUN_ORDER):
        vals = [sub.loc[run, col] / 1000.0 for col, _ in stats]
        b = ax.bar(x + (i - 0.5)*(width + 0.03), vals, width,
                   label=run, color=RUN_COLOR[run], **BAR_KW)
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
- Put "(lower is better)" in the suptitle. Latency charts are misread without it.
- `sharey=True` here — same measure across scopes, so they must be comparable.
- 24 bars means `fontsize=8.5` and `{:.0f}`. If labels still collide, drop a statistic
  rather than shrinking further.
- `e2e` spans two machines ([04 §2.3](04-latency.md)). If the numbers will be quoted
  externally, add "indicative — spans two clocks" to the subtitle.

---

## C7 · Utilization by group and role

**When** answering "is the work split correctly?".
**Data** `utilization_group.log` → `(run, scope, role)` → `utilization`.

```python
rows   = [("Group 0", "cloud"), ("Group 0", "edge"),
          ("Group 1", "cloud"), ("Group 1", "edge"),
          ("System",  "all")]
labels = ["G0\ncloud", "G0\nedge", "G1\ncloud", "G1\nedge", "System\nall"]

idx = df_utg.set_index(["run", "scope", "role"])
x, width = np.arange(len(rows)), 0.36
fig, ax = plt.subplots(figsize=(9.6, 4.9))

for i, run in enumerate(RUN_ORDER):
    vals = [float(np.ravel(idx.loc[(run, s, r), "utilization"])[0]) for s, r in rows]
    b = ax.bar(x + (i - 0.5)*(width + 0.03), vals, width,
               label=run, color=RUN_COLOR[run], **BAR_KW)
    label_bars(ax, b, fmt="{:.1f}%", fontsize=9)

ax.set_xticks(x, labels)
ax.set_ylabel("Utilization (%)")
ax.set_ylim(0, 118)                    # percentages: fix the ceiling, never autoscale
ax.set_title("Device utilization by group and role")
ax.grid(axis="x", visible=False)
ax.legend(loc="upper right")           # upper-left is occupied by the tallest bar
finish(fig, "07_utilization_by_role.png")
```

**Pitfalls**
- Two-line tick labels (`"G0\ncloud"`) beat rotation. Rotated labels are slow to read.
- `np.ravel(...)[0]` guards against `.loc` returning a Series on duplicate MultiIndex keys.
- For percentages set `ylim(0, 118)` explicitly — autoscaling to 99.8% leaves no label room.
- This chart plots pooled `utilization`. If it diverges from `utilization_mean`, the group
  is imbalanced ([03 §6](03-utilization.md)) — consider annotating both.
- The legend moved to `upper right` **because the render showed it sitting on a 93.4%
  bar**. Always check.

---

## C8 · Per-device utilization

**When** hunting stragglers and checking load balance across every device.
**Data** `utilization.log` → one row per device with `role`, `utilization`.

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
- Sort by `[role, value]` so classes stay blocked *and* rank within a class is visible.
- `groupby(...).cumcount()` numbers within class → `C1 C2 C3 E1 E2 …`. A running
  `enumerate` gives `C1 C2 C3 E4 E5 …`, which reads as missing devices.
- Colors come from `ROLE_COLOR[r]`, an entity dict — never `palette[i]`.
- Put the aggregate in the panel title; it is the reference the bars are read against.
- A bar above 100% is a **measurement bug**, not a fast device
  ([03 §2.1](03-utilization.md)). Do not clip it — let it show, and fix the instrument.

---

## C9 · Control events over the timeline

**When** the run had control-plane decisions and you want to see whether they moved
throughput. This is what `events_ns.log` exists for.
**Data** `batch_done_ns.log` (series) + `events_ns.log` (rules), on a **shared ns-epoch
x-axis**.

```python
fig, axes = plt.subplots(len(RUN_ORDER), 1, figsize=(13, 3.4 * len(RUN_ORDER)),
                         sharex=False)
axes = np.atleast_1d(axes)

for ax, run in zip(axes, RUN_ORDER):
    s = df_batch[df_batch.run == run].sort_values("ts")
    if s.empty:
        continue
    t0 = s.ts.iloc[0]                                  # ns -> seconds since run start
    ax.plot((s.ts - t0) / 1e9, s.window_fps,
            color=RUN_COLOR[run], label=f"{run} window rate", **LINE_KW)

    ev = df_events[df_events.run == run]
    for j, (_, e) in enumerate(ev.iterrows()):
        ax.axvline((e.ts - t0) / 1e9, color=MUTED, linewidth=1.0)
        ax.annotate(e.description, xy=((e.ts - t0) / 1e9, ax.get_ylim()[1]),
                    xytext=(3, -10 - 12 * (j % 3)),    # stagger to avoid overlap
                    textcoords="offset points", rotation=0,
                    fontsize=8, color=MUTED, va="top")

    ax.set_title(f"{run}  —  {len(ev)} control event(s)")
    ax.set_ylabel("Window rate")
    ax.set_ylim(0, df_batch.window_fps.max() * 1.15)
    ax.grid(axis="x", visible=False)
    ax.legend(loc="upper left")

axes[-1].set_xlabel("Seconds since first completion")
fig.suptitle("Throughput response to control events", fontsize=14,
             fontweight="semibold", color=INK, y=1.01)
fig.tight_layout()
finish(fig, "09_events_over_time.png")
```

**Pitfalls**
- **Both files must be on the server clock** for the overlay to mean anything
  ([01 §1](01-result-format.md)). This is the reason for the one-clock rule.
- Convert ns to seconds-since-start **per panel**; absolute ns epochs make useless ticks.
- Event rules are chrome, not data: muted gray hairlines, never a series color.
- Stagger the labels (`12 * (j % 3)`). Events cluster in time and will overlap.
- No events? Skip this chart rather than shipping an empty one — and say so in the
  notebook.
- Do **not** `sharex` across runs; each run has its own duration.

---

## C10 · Run comparison — verdict bar

**When** summarizing "did A beat B" across metrics where higher is better for some and
worse for others.
**Data** derived — one row per metric with both values and a `goal` direction.

```python
# goal: +1 higher is better, -1 lower is better, 0 neither
metrics = [
    ("System throughput",  a_rate, b_rate, "{:.2f} it/s", "higher is better", +1),
    ("Mean E2E latency",   a_lat,  b_lat,  "{:.1f} s",    "lower is better",  -1),
    ("p95 E2E latency",    a_p95,  b_p95,  "{:.1f} s",    "lower is better",  -1),
    ("System utilization", a_util, b_util, "{:.1f} %",    "workload dependent", 0),
]

pct, colors, verdicts = [], [], []
for _, a, b, _, _, goal in metrics:
    p = (a / b - 1) * 100
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
ax.set_yticks(y, [f"{n}\nA {f.format(a)}  ·  B {f.format(b)}\n({h})"
                  for n, a, b, f, h, _ in metrics], fontsize=9.5)
ax.tick_params(axis="y", colors=INK_2)
lim = max(abs(p) for p in pct) * 1.9 + 4
ax.set_xlim(-lim, lim)
ax.set_xlabel("Change of A relative to B (%)")
ax.set_title("A vs B — headline metrics  (B = baseline)")
ax.annotate("Colour marks the verdict (green better / red worse), not the sign of the change",
            xy=(0.5, -0.22), xycoords="axes fraction", ha="center",
            fontsize=9, color=MUTED)
ax.grid(axis="y", visible=False)
finish(fig, "10_run_comparison.png", hide_spines=("top", "right", "left"))
```

**Pitfalls**
- **Color keys to the verdict, never the sign.** A `+29%` latency move is a regression;
  painting it like a `+21%` throughput gain is a lie the reader cannot detect. **This is
  the single most common defect in summary charts.**
- Color now carries good/bad, i.e. status semantics — so the verdict **must** also be in
  text. Never color alone.
- Multi-line tick labels carry the absolute values; a separate annotation column collides
  with the y-axis.
- `hide_spines` includes `"left"` — a diverging chart's reference is the zero line.
- `xlim` symmetric about zero, or bar lengths misrepresent the ratio.
- Utilization is genuinely direction-neutral: lower utilization at higher throughput is a
  *better* system. Mark it `goal=0` rather than guessing.

---

## C11 · Stat tile — when it should not be a chart

**When** the answer is one number. A one-bar bar chart is always wrong.

```python
fig, ax = plt.subplots(figsize=(4.2, 2.4))
ax.axis("off")
ax.text(0, 0.72, "System throughput", fontsize=11, color=INK_2)
ax.text(0, 0.30, "25.22", fontsize=40, color=INK, fontweight="semibold")
ax.text(0.56, 0.36, "items/s", fontsize=13, color=MUTED, transform=ax.transAxes)
ax.text(0, 0.06, "+21.2% vs baseline", fontsize=10, color=GOOD)
finish(fig, "00_hero_throughput.png", hide_spines=())
```

**Pitfalls**
- Same sans as everything else. No display or serif face on a hero figure.
- **No `tabular-nums`** on a large standalone number — equal-width digits make `121` look
  loose at display sizes.
- The delta gets a status color *and* the words "vs baseline".

---

## Coverage check

Before shipping, confirm every required file feeds at least one chart:

| Log | Chart |
|---|---|
| `batch_done_ns.log` | C3, C9 |
| `group_rate_ns.log` | C2, C4 |
| `group_rate.log` | C1, C10 |
| `utilization.log` | C8 |
| `utilization_group.log` | C7, C10 |
| `latency_group.log` | C5, C6, C10 |
| `events_ns.log` | C9 (skip if absent) |

If a required file feeds nothing, either chart it or state in the notebook why not.
