"""Free-time accounting for one device process.

FREE TIME is wall-clock time in which this device did **no pipeline work at
all**: no capture, no inference, no compress, no send, no receive, no
decompress, no postprocess, no metrics bookkeeping. It is the complement of
busy time over the device's own run span:

    free = (end - start) - |union of every lane's busy intervals|

Why a union and not a sum. A device runs several threads: the multithreading
pipeline has an inference thread and a transfer thread, and while one blocks the
other keeps working. The device is free only when EVERY lane is idle at the same
moment, so the busy intervals have to be merged before they are measured. Summing
the per-stage timers that Scheduler already keeps would answer a different
question ("how long did stage X take"), can exceed the wall clock outright once
two threads overlap, and silently misses the gaps between stages — which is
exactly where free time lives.

Relationship to utilization.log: utilization is busy/total over the
`get input -> output` window of ONE lane, so it charges the wait inside that
window to busy and ignores work done on the other lane. Free time counts every
lane and only counts real work, so `free% + utilization%` does not add to 100 and
is not meant to; see guide/10-free-time.md.

Timing uses perf_counter_ns (monotonic — an NTP step mid-run cannot create a
negative interval) and is converted to epoch ns only on export, so the server can
line up devices that share a machine.
"""

import socket
import threading
import time

# Work kinds. Every one of them is real work on this device; their union is what
# makes a device "not free". Keep this list short and stable — it is a wire
# format, and the server groups by it.
WORK_KINDS = (
    "capture",      # video read + decode + resize
    "tensor",       # stack / H2D / D2H / dtype conversion around the model
    "inference",    # model forward
    "compress",     # Encoder (quantize + pack) or the CPU move that replaces it
    "send",         # pickle.dumps + basic_publish
    "recv",         # basic_get + pickle.loads
    "decompress",   # Decoder + unpack
    "postprocess",  # NMS, mAP update, pred files, detection stream
    "metrics",      # RAM probe + CSV row
)

# Why the device was free, in the order the report attributes them. A moment can
# match several reasons (two lanes waiting for different things), so they are
# subtracted in this fixed priority and therefore always sum to exactly the free
# time — no double counting, no gaps.
WAIT_REASONS = (
    "input",         # blocked waiting for work to arrive (starved by the stage in front)
    "backpressure",  # stalled by the broker depth guard
    "downstream",    # blocked because the next stage is full
    "idle",          # poll loop with nothing to do (stream finished / nothing ready)
)

# Waits are recorded once per poll iteration, which for a 2 ms poll loop is
# ~500 intervals per idle second. Gaps under this are closed on the spot so a
# long idle stretch stays one interval instead of a hundred thousand. Work spans
# never use it (tolerance 0) — busy must stay exact.
_WAIT_COALESCE_NS = 1_000_000  # 1 ms

_PENDING_COMPACT = 4096


# ─── interval algebra ──────────────────────────────────────────────────────────

def merge_intervals(spans):
    """Sorted, disjoint union of (start, end) pairs."""
    if not spans:
        return []
    s = sorted(spans)
    out = [list(s[0])]
    for a, b in s[1:]:
        if a <= out[-1][1]:
            if b > out[-1][1]:
                out[-1][1] = b
        else:
            out.append([a, b])
    return [(a, b) for a, b in out]


def subtract_intervals(a, b):
    """a \\ b. Both must be sorted and disjoint (i.e. straight out of merge)."""
    if not b:
        return list(a)
    out = []
    j = 0
    for s, e in a:
        cur = s
        while j < len(b) and b[j][1] <= cur:
            j += 1
        k = j
        while k < len(b) and b[k][0] < e:
            bs, be = b[k]
            if bs > cur:
                out.append((cur, min(bs, e)))
            if be > cur:
                cur = be
            if cur >= e:
                break
            k += 1
        if cur < e:
            out.append((cur, e))
    return out


def clip_intervals(spans, lo, hi):
    """The part of `spans` inside [lo, hi]. Guards the coalescing tolerance from
    pushing an interval past the run's own end."""
    out = []
    for s, e in spans:
        s2, e2 = max(s, lo), min(e, hi)
        if e2 > s2:
            out.append((s2, e2))
    return out


def total_ns(spans):
    return sum(e - s for s, e in spans)


def coalesce_to(spans, max_n):
    """Reduce a disjoint interval list to at most max_n intervals by closing the
    SMALLEST gaps first. Returns (spans, slop_ns) where slop_ns is exactly how
    much non-busy time was swallowed, so the receiver can see the error bound
    instead of guessing. Only used for the wire: the local report is exact."""
    n = len(spans)
    if n <= max_n or max_n < 1:
        return list(spans), 0
    gaps = sorted((spans[i + 1][0] - spans[i][1], i) for i in range(n - 1))
    join = set()
    slop = 0
    for g, i in gaps[:n - max_n]:
        join.add(i)
        slop += g
    out = []
    cur_s, cur_e = spans[0]
    for i in range(n - 1):
        if i in join:
            cur_e = spans[i + 1][1]
        else:
            out.append((cur_s, cur_e))
            cur_s, cur_e = spans[i + 1]
    out.append((cur_s, cur_e))
    return out, slop


def host_cpu_times():
    """(idle_s, total_s) for the WHOLE machine, or None. This is the OS's own
    accounting across every process, so it answers 'is the machine free', which
    is a different (and coarser) question than 'is this pipeline free'."""
    try:
        import psutil
        t = psutil.cpu_times()
    except Exception:
        return None
    idle = float(getattr(t, "idle", 0.0))
    total = sum(float(v) for v in t)
    if total <= 0:
        return None
    return idle, total


class _Span:
    """Context manager for one work/wait interval. A tiny __slots__ class rather
    than @contextmanager: these are entered a few times per batch and once per
    captured frame, so the generator machinery is not worth paying for."""
    __slots__ = ("ft", "kind", "busy", "t0")

    def __init__(self, ft, kind, busy):
        self.ft = ft
        self.kind = kind
        self.busy = busy

    def __enter__(self):
        self.t0 = time.perf_counter_ns()
        return self

    def __exit__(self, *exc):
        self.ft._record(self.kind, self.t0, time.perf_counter_ns(), self.busy)
        return False


class _Noop:
    __slots__ = ()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


_NOOP = _Noop()


class FreeTimeTracker:
    """Records what every lane of this device was doing, then folds it into one
    free-time report. Thread-safe; a lane is simply a thread (each thread's
    intervals are tracked separately and merged at the end).

    Disabled trackers are free: every entry point returns a shared no-op.
    """

    def __init__(self, enable=True, bucket_s=1.0, max_buckets=3600, max_intervals=2000):
        self.enable = bool(enable)
        self.bucket_s = float(bucket_s) if bucket_s else 1.0
        self.max_buckets = int(max_buckets)
        self.max_intervals = int(max_intervals)
        self.hostname = socket.gethostname()

        self._lock = threading.Lock()
        self._t0_perf = None        # run start, monotonic
        self._t1_perf = None        # run end, monotonic
        self._t0_wall = None        # the same instant on the epoch clock
        self._cpu0 = None
        self._cpu1 = None

        self._busy = []             # merged disjoint busy intervals, all lanes
        self._waits = {}            # reason -> merged disjoint intervals
        self._pending = []          # busy spans awaiting a merge
        self._pending_wait = {}     # reason -> spans awaiting a merge
        self._open = {}             # (lane, kind, busy) -> [start, end], for coalescing
        self._kind_ns = {}          # raw summed duration per kind (overlaps included)
        self._kind_n = {}
        self._lane_ns = {}          # raw summed busy duration per lane

    # ─── recording ─────────────────────────────────────────────────────────────

    def work(self, kind):
        """`with ft.work("inference"): ...` — this lane is doing real work."""
        return _Span(self, kind, True) if self.enable else _NOOP

    def wait(self, reason):
        """`with ft.wait("input"): ...` — this lane is blocked, doing nothing."""
        return _Span(self, reason, False) if self.enable else _NOOP

    @staticmethod
    def now():
        """Monotonic ns, for the retroactive add_* calls below."""
        return time.perf_counter_ns()

    def add_work(self, kind, t0, t1=None):
        """Record a span whose classification is only known after the fact — e.g.
        a basic_get that is work when it returns a message and a wait when it
        doesn't."""
        if self.enable:
            self._record(kind, t0, t1 if t1 is not None else time.perf_counter_ns(), True)

    def add_wait(self, reason, t0, t1=None):
        if self.enable:
            self._record(reason, t0, t1 if t1 is not None else time.perf_counter_ns(), False)

    def _record(self, kind, t0, t1, busy):
        if t1 <= t0:
            t1 = t0  # a zero-length span still counts toward the call count
        lane = threading.current_thread().name
        tol = 0 if busy else _WAIT_COALESCE_NS
        with self._lock:
            self._kind_ns[kind] = self._kind_ns.get(kind, 0) + (t1 - t0)
            self._kind_n[kind] = self._kind_n.get(kind, 0) + 1
            if busy:
                self._lane_ns[lane] = self._lane_ns.get(lane, 0) + (t1 - t0)

            key = (lane, kind, busy)
            op = self._open.get(key)
            if op is not None and t0 - op[1] <= tol:
                if t1 > op[1]:
                    op[1] = t1
                return
            if op is not None:
                self._stash(kind, busy, op[0], op[1])
            self._open[key] = [t0, t1]

    def _stash(self, kind, busy, s, e):
        """Move a closed span into the pending list (caller holds the lock).

        Zero-length spans are counted (kind_n/kind_ns above) but not stored: they
        change no total and would only fragment the merged lists."""
        if e <= s:
            return
        if busy:
            self._pending.append((s, e))
            if len(self._pending) >= _PENDING_COMPACT:
                self._busy = merge_intervals(self._busy + self._pending)
                self._pending = []
        else:
            reason = kind if kind in WAIT_REASONS else "idle"
            p = self._pending_wait.setdefault(reason, [])
            p.append((s, e))
            if len(p) >= _PENDING_COMPACT:
                self._waits[reason] = merge_intervals(self._waits.get(reason, []) + p)
                self._pending_wait[reason] = []

    def _flush(self):
        """Close every open span and merge everything (caller holds the lock)."""
        for (lane, kind, busy), op in self._open.items():
            self._stash(kind, busy, op[0], op[1])
        self._open = {}
        if self._pending:
            self._busy = merge_intervals(self._busy + self._pending)
            self._pending = []
        for reason, p in self._pending_wait.items():
            if p:
                self._waits[reason] = merge_intervals(self._waits.get(reason, []) + p)
        self._pending_wait = {}

    # ─── run span ──────────────────────────────────────────────────────────────

    def start(self):
        if not self.enable:
            return
        self._cpu0 = host_cpu_times()
        with self._lock:
            self._t0_perf = time.perf_counter_ns()
            self._t0_wall = time.time_ns()

    def stop(self):
        if not self.enable:
            return
        with self._lock:
            self._t1_perf = time.perf_counter_ns()
        self._cpu1 = host_cpu_times()

    # ─── reporting ─────────────────────────────────────────────────────────────

    def _host_idle_pct(self):
        c0, c1 = self._cpu0, self._cpu1
        if not c0 or not c1:
            return None
        d_idle = c1[0] - c0[0]
        d_total = c1[1] - c0[1]
        if d_total <= 0:
            return None
        return max(0.0, min(100.0, 100.0 * d_idle / d_total))

    def _series(self, free_iv, t0, t1):
        """Free fraction per fixed-width bucket — the plottable 'when was this
        device free' series. The width widens automatically so a long run never
        ships more than max_buckets points."""
        span = t1 - t0
        if span <= 0:
            return 1.0, []
        width = int(self.bucket_s * 1e9)
        if width <= 0:
            width = int(1e9)
        n = max(1, -(-span // width))
        if n > self.max_buckets:
            width = -(-span // self.max_buckets)
            n = max(1, -(-span // width))
        acc = [0] * n
        for s, e in free_iv:
            i = max(0, (s - t0) // width)
            j = min(n - 1, (e - 1 - t0) // width)
            for b in range(int(i), int(j) + 1):
                bs = t0 + b * width
                be = min(bs + width, t1)
                ov = min(e, be) - max(s, bs)
                if ov > 0:
                    acc[b] += ov
        out = []
        for b in range(n):
            bs = t0 + b * width
            be = min(bs + width, t1)
            w = be - bs
            out.append(round(acc[b] / w, 4) if w > 0 else 0.0)
        return width / 1e9, out

    def report(self, **identity):
        """One whole-run free-time report for this device. `identity` carries the
        labels the server groups by (client_id, role, cluster_id, machine, ...).
        Returns None when the tracker is off or never saw a run."""
        if not self.enable:
            return None
        with self._lock:
            if self._t0_perf is None:
                return None
            t0 = self._t0_perf
            t1 = self._t1_perf if self._t1_perf is not None else time.perf_counter_ns()
            self._flush()
            busy = clip_intervals(self._busy, t0, t1)
            waits = {r: clip_intervals(v, t0, t1) for r, v in self._waits.items()}
            kind_ns = dict(self._kind_ns)
            kind_n = dict(self._kind_n)
            lane_ns = dict(self._lane_ns)

        span = max(0, t1 - t0)
        busy_ns = total_ns(busy)
        free_iv = subtract_intervals([(t0, t1)], busy) if span else []
        free_ns = total_ns(free_iv)

        # Attribute the free time to a reason, in priority order, each one
        # excluding what an earlier reason already claimed. Guarantees the parts
        # sum to free_ns exactly; whatever no wait span covers is 'unaccounted'.
        reasons = {}
        claimed = list(busy)
        for r in WAIT_REASONS:
            iv = waits.get(r)
            if not iv:
                continue
            own = subtract_intervals(iv, claimed)
            if own:
                reasons[r] = total_ns(own)
                claimed = merge_intervals(claimed + own)
        acc = sum(reasons.values())
        if free_ns - acc > 0:
            reasons["unaccounted"] = free_ns - acc

        gaps = [e - s for s, e in free_iv]
        bucket_s, series = self._series(free_iv, t0, t1) if span else (self.bucket_s, [])
        wire, slop = coalesce_to(busy, self.max_intervals)
        # Busy intervals on the epoch clock, so the server can union the devices
        # that share one machine (same clock) into a true machine free time.
        off = self._t0_wall - t0
        rep = {
            "action": "FREETIME",
            "hostname": self.hostname,
            "span_ns": span,
            "busy_ns": busy_ns,
            "free_ns": free_ns,
            "free_pct": (100.0 * free_ns / span) if span else 0.0,
            "kinds": {k: {"ns": kind_ns[k], "n": kind_n.get(k, 0)}
                      for k in kind_ns if k in WORK_KINDS},
            "free_reasons": reasons,
            "lanes": lane_ns,
            "free_gaps": len(gaps),
            "longest_free_ms": (max(gaps) / 1e6) if gaps else 0.0,
            "mean_free_ms": (sum(gaps) / len(gaps) / 1e6) if gaps else 0.0,
            "host_idle_pct": self._host_idle_pct(),
            "bucket_s": bucket_s,
            "free_series": series,
            "t_start_ns": self._t0_wall,
            "t_end_ns": self._t0_wall + span,
            "busy_intervals_ns": [(s + off, e + off) for s, e in wire],
            "busy_intervals_slop_ns": slop,
        }
        rep.update(identity)
        return rep


# ─── log file ──────────────────────────────────────────────────────────────────

def write_report_log(path, rep):
    """Write one device's free-time report to its own log file.

    Same line grammar as every other result file (guide/01-result-format.md):
    a 19-digit ns timestamp, an uppercase line-kind flag, then key=value. So the
    per-device file and the server's roll-up parse with the same reader."""
    if not rep:
        return None
    t_ns = time.time_ns()
    span_s = rep["span_ns"] / 1e9
    lines = [
        f"{t_ns} DEVICE client={rep.get('client_id')} role={rep.get('role')} "
        f"machine={rep.get('machine')} host={rep.get('hostname')} "
        f"cluster={rep.get('cluster_id')} device={rep.get('device')} "
        f"span_s={span_s:.3f} busy_s={rep['busy_ns'] / 1e9:.3f} "
        f"free_s={rep['free_ns'] / 1e9:.3f} free={rep['free_pct']:.2f}% "
        f"gaps={rep['free_gaps']} longest_free_ms={rep['longest_free_ms']:.3f} "
        f"mean_free_ms={rep['mean_free_ms']:.3f}"
        + (f" host_idle={rep['host_idle_pct']:.2f}%" if rep.get("host_idle_pct") is not None else "")
    ]
    for kind in WORK_KINDS:
        k = rep["kinds"].get(kind)
        if not k:
            continue
        lines.append(
            f"{t_ns} KIND kind={kind} n={k['n']} sum_s={k['ns'] / 1e9:.3f} "
            f"share={(100.0 * k['ns'] / rep['span_ns']) if rep['span_ns'] else 0.0:.2f}%")
    for reason, ns in sorted(rep["free_reasons"].items(), key=lambda kv: -kv[1]):
        lines.append(
            f"{t_ns} FREE reason={reason} free_s={ns / 1e9:.3f} "
            f"share={(100.0 * ns / rep['free_ns']) if rep['free_ns'] else 0.0:.2f}%")
    for lane, ns in sorted(rep["lanes"].items(), key=lambda kv: -kv[1]):
        lines.append(f"{t_ns} LANE lane={lane} busy_s={ns / 1e9:.3f}")
    for i, f in enumerate(rep["free_series"]):
        lines.append(f"{t_ns} BUCKET i={i} t_offset_s={i * rep['bucket_s']:.3f} "
                     f"free={100.0 * f:.2f}%")
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    return path
