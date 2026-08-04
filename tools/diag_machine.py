"""
Chan doan MAY — ban gon, chi in nhung gi quyet dinh.

    python3 tools/diag_machine.py          # gon (mac dinh)
    python3 tools/diag_machine.py -v       # them chi tiet

Chay tren TUNG VM, gui toan bo output. Khong dung mang, khong dung broker,
khong dung code pipeline — nen no tach bach duoc "may cham di" khoi
"mang/code cham di".

Dong quan trong nhat la `forward`: so sanh forward pass HOM NAY voi file
profile_*.npy do tu lan chay TOT truoc day (anh chup toc do cua chinh may nay).
"""
import glob
import os
import platform
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
VERBOSE = "-v" in sys.argv


def sh(cmd, timeout=25):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                              timeout=timeout).stdout
    except Exception:
        return ""


# ── ten may in DAU TIEN ─────────────────────────────────────────────────────
print(f"=== {platform.node()} ===")

vcpu = os.cpu_count() or 0
omp = os.environ.get("OMP_NUM_THREADS", "unset")

# ── torch: so thread + ban build ────────────────────────────────────────────
tver = tthreads = mkl = dnn = "?"
torch = None
try:
    import torch
    tver = torch.__version__
    tthreads = torch.get_num_threads()
    cfg = torch.__config__.show().lower()
    # Substring checks against what torch actually prints in its build config.
    # A build without these is the classic "code unchanged but everything got
    # slower" cause: a generic wheel replacing an optimised one.
    mkl = "yes" if ("use_mkl=on" in cfg or "math kernel library" in cfg) else "no"
    dnn = "yes" if ("use_mkldnn=on" in cfg or "onednn" in cfg or "mkl-dnn" in cfg) else "no"
except Exception as e:
    tver = f"IMPORT-FAIL({type(e).__name__})"

print(f"vCPU={vcpu}  torch_threads={tthreads}  OMP_NUM_THREADS={omp}")
print(f"torch={tver}  MKL={mkl}  oneDNN={dnn}")

# ── CPU thuan, khong qua torch ──────────────────────────────────────────────
gflops = None
try:
    import numpy as np
    n = 1500
    a = np.random.rand(n, n).astype(np.float32)
    a @ a
    t = time.perf_counter()
    for _ in range(3):
        a @ a
    dt = (time.perf_counter() - t) / 3
    gflops = 2 * n ** 3 / dt / 1e9
    print(f"numpy_gflops={gflops:.1f}  ({gflops / max(vcpu,1):.1f}/vCPU)")
except Exception as e:
    print(f"numpy_gflops=FAIL({type(e).__name__})")

# ── PHEP THU QUYET DINH: forward pass hom nay vs profile cu ─────────────────
ratio = None
if torch is not None:
    try:
        import yaml
        import numpy as np
        c = yaml.safe_load(open("config.yaml", encoding="utf-8"))
        mname, bs = c["server"]["model"], int(c["server"]["batch-size"])
        ck = torch.load(f"{mname}.pt", map_location="cpu", weights_only=False)
        model = ck["model"].float().eval()
        x = torch.randn(bs, 3, 640, 640)
        with torch.no_grad():
            for _ in range(2):
                model(x)
            t = time.perf_counter()
            for _ in range(3):
                model(x)
            today = (time.perf_counter() - t) / 3
        olds = [(f, float(np.load(f).sum())) for f in sorted(glob.glob("profile_*bs*.npy"))]
        if olds:
            f, old = olds[0]
            ratio = today / old if old > 0 else None
            print(f"forward today={today:.3f}s  old={old:.3f}s  ratio={ratio:.2f}x  [{f}]")
            for f2, o2 in olds[1:]:
                print(f"  (them) old={o2:.3f}s  [{f2}]")
        else:
            print(f"forward today={today:.3f}s  old=KHONG-CO-FILE-CU")
    except Exception as e:
        print(f"forward=FAIL({type(e).__name__}: {e})")
else:
    print("forward=BO-QUA (khong co torch)")

# ── trieu chung hypervisor: chi lay so, khong dump bang ─────────────────────
if platform.system() == "Linux":
    try:
        lines = [l.split() for l in sh("vmstat 1 3").strip().splitlines()]
        row = [l for l in lines if len(l) >= 17 and l[0].isdigit()][-1]
        r, si, so, wa, st = row[0], row[6], row[7], row[15], row[16]
        print(f"steal={st}%  iowait={wa}%  swap_in={si} swap_out={so}  runnable={r}")
    except Exception:
        print("steal=? (can: apt install procps)")
    load = sh("cat /proc/loadavg").split()[:3]
    print(f"load={' '.join(load)}")
    mem = [l.split() for l in sh("free -m").splitlines() if l.startswith(("Mem:", "Swap:"))]
    for m in mem:
        print(f"{m[0]:<6} total={m[1]}MB used={m[2]}MB")
else:
    print(f"os={platform.system()} (phan hypervisor chi ho tro Linux)")

# ── ket luan mot dong ───────────────────────────────────────────────────────
verdict = []
if torch is None:
    verdict.append("TORCH-KHONG-IMPORT-DUOC")
if ratio is not None:
    verdict.append("MAY-CHAM-DI" if ratio > 1.5 else ("may-nhanh-hon" if ratio < 0.67 else "may-khong-doi"))
elif torch is not None:
    verdict.append("khong-co-profile-cu-de-so")
if isinstance(tthreads, int) and vcpu and tthreads < vcpu:
    verdict.append(f"CHI-{tthreads}/{vcpu}-THREAD")
if torch is not None and dnn == "no":
    verdict.append("TORCH-THIEU-oneDNN")
if gflops is not None and vcpu and gflops / vcpu < 5:
    verdict.append("CPU-YEU-BAT-THUONG")
print("VERDICT: " + (" | ".join(verdict) if verdict else "khong thay bat thuong"))

if VERBOSE:
    print("\n--- verbose ---")
    print(sh("lscpu | grep -E 'Model name|MHz|Hypervisor|Virtualization'"))
    print(sh("df -h . | tail -1"))
    if torch is not None:
        print(torch.__config__.show())
