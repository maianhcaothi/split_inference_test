"""Validate a results directory against guide/01-result-format.md.

usage: python validate_results.py <run-dir> [--names cluster|group]
"""
import re, sys
from pathlib import Path

TS   = re.compile(r"^\d{19}(\s|$)")
KV   = re.compile(r"(\w+)=([^\s]+)")
PCT  = re.compile(r"^\d+(\.\d+)?%$")

NAMES = {
    "group":   dict(rate_ns="group_rate_ns.log", rate="group_rate.log",
                    util_g="utilization_group.log", lat="latency_group.log"),
    "cluster": dict(rate_ns="fps_cluster_ns.log", rate="fps_cluster.log",
                    util_g="utilization_cluster.log", lat="latency_cluster.log"),
}

def lines(p):
    if not p.exists():
        return None
    return [l.rstrip("\n") for l in p.read_text(encoding="utf-8",
                                                errors="ignore").splitlines() if l.strip()]

def num(v):
    return float(str(v).rstrip("%"))

def main(run_dir, scheme):
    d, N = Path(run_dir), NAMES[scheme]
    errs, warns = [], []

    required = ["batch_done_ns.log", N["rate_ns"], N["rate"],
                "utilization.log", N["util_g"], N["lat"]]
    files = {}
    for name in required:
        ls = lines(d / name)
        if ls is None:
            errs.append(f"{name}: MISSING (required)")
        files[name] = ls or []

    # -- grammar: every line starts with a 19-digit ns timestamp -------------
    for name, ls in files.items():
        for i, ln in enumerate(ls, 1):
            if not TS.match(ln):
                errs.append(f"{name}:{i}: does not start with a 19-digit ns timestamp")
                break

    # -- batch_done_ns.log: 1 or 2 columns, col2 parses as float -------------
    bd = files["batch_done_ns.log"]
    for i, ln in enumerate(bd, 1):
        parts = ln.split()
        if len(parts) not in (1, 2):
            errs.append(f"batch_done_ns.log:{i}: expected 1 or 2 columns, got {len(parts)}")
        elif len(parts) == 2:
            try:
                float(parts[1])
            except ValueError:
                errs.append(f"batch_done_ns.log:{i}: column 2 is not a float: {parts[1]!r}")

    # -- cross-file: one group_rate_ns line per batch_done line -------------
    if bd and files[N["rate_ns"]] and len(bd) != len(files[N["rate_ns"]]):
        errs.append(f"line-count mismatch: batch_done_ns.log has {len(bd)}, "
                    f"{N['rate_ns']} has {len(files[N['rate_ns']])} "
                    f"(every completion must appear in both)")

    # -- timestamps monotonic in the live series ---------------------------
    for name in ("batch_done_ns.log", N["rate_ns"]):
        ts = [int(l.split()[0]) for l in files[name]]
        if any(b < a for a, b in zip(ts, ts[1:])):
            errs.append(f"{name}: timestamps are not monotonically non-decreasing")

    # -- group_rate.log: exactly one SYSTEM line; groups sum to it ----------
    rate = files[N["rate"]]
    sys_l = [l for l in rate if "SYSTEM" in l.split()]
    grp_l = [l for l in rate if "cluster=" in l or "group=" in l]
    if len(sys_l) != 1:
        errs.append(f"{N['rate']}: expected exactly 1 SYSTEM line, found {len(sys_l)}")
    else:
        sk = dict(KV.findall(sys_l[0]))
        if "steady_fps" in sk:
            errs.append(f"{N['rate']}: SYSTEM line must not carry steady_fps")
        gk = [dict(KV.findall(l)) for l in grp_l]
        if gk:
            # done/frames ARE additive.
            for key in ("done", "frames"):
                got, want = sum(num(k[key]) for k in gk), num(sk[key])
                if got != want:
                    errs.append(f"{N['rate']}: group {key} sums to {got:.0f}, "
                                f"SYSTEM says {want:.0f}")
            # fps is NOT additive (each scope divides by its own span). The exact
            # invariant is on the spans: SYSTEM span == max(group span).
            sys_span = num(sk["frames"]) / num(sk["fps"])
            spans    = [num(k["frames"]) / num(k["fps"]) for k in gk]
            if abs(max(spans) - sys_span) / sys_span > 0.01:
                errs.append(f"{N['rate']}: SYSTEM span {sys_span:.2f}s != max group span "
                            f"{max(spans):.2f}s (START is not shared, or SYSTEM does not "
                            f"end at the overall last completion)")
            if sum(num(k["fps"]) for k in gk) < num(sk["fps"]) * 0.99:
                errs.append(f"{N['rate']}: group fps sums BELOW SYSTEM fps, which is "
                            f"impossible with a shared START")
            share = sum(num(k["share"]) for k in gk if "share" in k)
            if any("share" in k for k in gk) and abs(share - 100.0) > 0.5:
                warns.append(f"{N['rate']}: share sums to {share:.1f}%, expected ~100%")

    # -- utilization: percent-formatted, and never above 100% --------------
    for name in ("utilization.log", N["util_g"]):
        for i, ln in enumerate(files[name], 1):
            kv = dict(KV.findall(ln))
            for key in ("utilization", "utilization_mean"):
                if key in kv:
                    if not PCT.match(kv[key]):
                        errs.append(f"{name}:{i}: {key} must be percent-formatted "
                                    f"with a trailing '%', got {kv[key]!r}")
                    elif num(kv[key]) > 100.0:
                        errs.append(f"{name}:{i}: {key}={kv[key]} exceeds 100% "
                                    f"(overlapping busy intervals were summed)")

    # -- utilization_group ALL/SYSTEM lines carry both ratios ---------------
    for i, ln in enumerate(files[N["util_g"]], 1):
        flags = [p for p in ln.split()[1:] if "=" not in p and p.isupper()]
        if ("ALL" in flags or "SYSTEM" in flags):
            kv = dict(KV.findall(ln))
            if "utilization_mean" not in kv:
                errs.append(f"{N['util_g']}:{i}: ALL/SYSTEM lines must carry "
                            f"utilization_mean beside utilization")

    # -- latency: required stat keys, ordering, e2e has no role -------------
    for i, ln in enumerate(files[N["lat"]], 1):
        kv = dict(KV.findall(ln))
        if "kind" not in kv:
            errs.append(f"{N['lat']}:{i}: missing kind=")
            continue
        missing = [k for k in ("n", "mean_ms", "p50_ms", "p95_ms", "max_ms") if k not in kv]
        if missing:
            errs.append(f"{N['lat']}:{i}: missing {', '.join(missing)}")
            continue
        if not (num(kv["p50_ms"]) <= num(kv["p95_ms"]) <= num(kv["max_ms"])):
            errs.append(f"{N['lat']}:{i}: percentiles out of order "
                        f"(p50 <= p95 <= max required)")
        if kv["kind"] == "e2e" and "role" in kv:
            errs.append(f"{N['lat']}:{i}: e2e lines must not carry role=")

    print(f"\nvalidating {d}  (naming scheme: {scheme})")
    for w in warns:
        print(f"  [WARN] {w}")
    for e in errs:
        print(f"  [FAIL] {e}")
    print(f"\n  -> {len(errs)} error(s), {len(warns)} warning(s): "
          f"{'CONFORMANT' if not errs else 'NOT CONFORMANT'}\n")
    return 1 if errs else 0

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("usage: python validate_results.py <run-dir> [--names cluster|group]")
    scheme = "cluster"
    if "--names" in sys.argv:
        scheme = sys.argv[sys.argv.index("--names") + 1]
    sys.exit(main(sys.argv[1], scheme))
