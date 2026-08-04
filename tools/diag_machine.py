"""
Chan doan MAY (khong dung mang, khong dung broker, khong dung code pipeline).

Muc dich: tra loi "may co cham di khong, va cham vi cai gi" — tach bach duoc
4 nguyen nhan khac nhau:

  A. Ban torch bi doi (cai torchmetrics co the keo theo torch khac)
  B. Phan cung / host cham di (steal time, swap, throttle)
  C. Torch chay tren qua it thread
  D. Khong cham gi ca -> nguyen nhan nam o mang/broker/code

Cach doc o cuoi file.

    python3 tools/diag_machine.py

Chay tren TUNG VM roi so sanh voi nhau, va so voi file profile_*.npy do tu
lan chay tot truoc day (do la anh chup toc do cua chinh may nay hoi do).
"""
import glob
import os
import platform
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def sh(cmd):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                              timeout=20).stdout.strip()
    except Exception as e:
        return f"(loi: {e})"


def section(t):
    print("\n" + "=" * 62)
    print(f"  {t}")
    print("=" * 62)


section("0. MAY")
print(f"host      : {platform.node()}")
print(f"os        : {platform.platform()}")
print(f"python    : {sys.version.split()[0]}")
print(f"vCPU      : {os.cpu_count()}")
for var in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS"):
    print(f"{var:<10}: {os.environ.get(var, '(khong dat)')}")
if platform.system() == "Linux":
    print(sh("lscpu | grep -E 'Model name|MHz|Hypervisor|Virtualization'"))
    print(sh("uptime"))


# ─── B: phan cung thuan tuy, KHONG qua torch ────────────────────────────────
section("1. HIEU NANG CPU THUAN (numpy GEMM) — doc lap voi torch")
try:
    import numpy as np
    n = 1500
    a = np.random.rand(n, n).astype(np.float32)
    a @ a                                    # warmup
    t = time.perf_counter()
    reps = 3
    for _ in range(reps):
        a @ a
    dt = (time.perf_counter() - t) / reps
    gflops = 2 * n ** 3 / dt / 1e9
    print(f"numpy sgemm {n}x{n}: {dt*1000:8.1f} ms  ->  {gflops:6.1f} GFLOPS "
          f"(dung TAT CA thread ma BLAS thay)")
    print(f"  ~ {gflops / max(os.cpu_count() or 1, 1):.1f} GFLOPS / vCPU")
    print("  Con so TUYET DOI it y nghia — hay SO SANH GIUA CAC VM va so voi")
    print("  chinh may nay do lai sau. Duoi ~5 GFLOPS/vCPU la bat thuong.")
except Exception as e:
    print(f"(bo qua: {e})")


# ─── A + C: ban torch va so thread ──────────────────────────────────────────
section("2. BAN TORCH (nghi can chinh khi 'code khong doi ma cham di')")
try:
    import torch
    print(f"torch           : {torch.__version__}")
    print(f"get_num_threads : {torch.get_num_threads()}   <-- bang vCPU moi la dung")
    print(f"interop_threads : {torch.get_num_interop_threads()}")
    cfg = torch.__config__.show()
    for key in ("MKL", "MKL-DNN", "oneDNN", "OpenMP", "AVX", "NNPACK"):
        hit = [l for l in cfg.splitlines() if key.lower() in l.lower()]
        print(f"  {key:<8}: {'CO  ' + hit[0].strip()[:60] if hit else 'KHONG THAY'}")
except Exception as e:
    print(f"(khong import duoc torch: {e})")
    torch = None


# ─── Phep thu quyet dinh: forward pass that, so voi profile cu ──────────────
section("3. FORWARD PASS THAT — so voi profile do tu lan chay TOT truoc day")
if torch is not None:
    try:
        import yaml
        cfg = yaml.safe_load(open("config.yaml", encoding="utf-8"))
        model_name = cfg["server"]["model"]
        bs = int(cfg["server"]["batch-size"])
        ck = torch.load(f"{model_name}.pt", map_location="cpu", weights_only=False)
        model = ck["model"].float().eval()
        x = torch.randn(bs, 3, 640, 640)
        with torch.no_grad():
            for _ in range(2):
                model(x)                       # warmup
            t = time.perf_counter()
            reps = 3
            for _ in range(reps):
                model(x)
            dt = (time.perf_counter() - t) / reps
        print(f"HOM NAY : {dt:.3f} s/batch  (toan model, bs={bs}, {torch.get_num_threads()} threads)")

        import numpy as np
        found = False
        for f in sorted(glob.glob("profile_*bs*.npy")):
            old = float(np.load(f).sum())
            found = True
            print(f"HOI TRUOC: {old:.3f} s/batch   <- {f}")
            if old > 0:
                r = dt / old
                verdict = ("MAY CHAM DI ~%.1fx  <== DAY LA NGUYEN NHAN" % r) if r > 1.5 \
                    else ("nhanh hon %.1fx" % (1 / r) if r < 0.67 else "tuong duong (khong doi)")
                print(f"           ty le hom nay/hoi truoc = {r:.2f}x  ->  {verdict}")
        if not found:
            print("(khong tim thay profile_*.npy cu de so — chay lan dau thi chua co)")
    except Exception as e:
        print(f"(bo qua: {e})")
else:
    print("(khong co torch -> bo qua phep thu quyet dinh nay)")


# ─── B: trieu chung tu hypervisor ───────────────────────────────────────────
section("4. TRIEU CHUNG HYPERVISOR / HE DIEU HANH")
if platform.system() == "Linux":
    print("--- vmstat 1 5 (xem cot st / si / so / wa) ---")
    print(sh("vmstat 1 5"))
    print("\n--- bo nho ---")
    print(sh("free -h"))
    print("\n--- dia ---")
    print(sh("df -h . | tail -2"))
else:
    print("(chi ho tro Linux; tren Windows dung Task Manager / Resource Monitor)")


print("""
==============================================================
 CACH DOC
==============================================================
 Muc 3 la phep thu quyet dinh. Ty le hom_nay/hoi_truoc:
   ~1.0x  -> may KHONG cham di. Nguyen nhan nam o mang/broker/code.
   >1.5x  -> may that su cham di. Doc tiep muc 1, 2, 4 de biet vi sao.

 Roi phan biet tiep:
   numpy GEMM (muc 1) BINH THUONG + torch cham  -> BAN TORCH bi doi (muc 2).
        Kiem tra dong MKL/oneDNN. Neu "KHONG THAY" thi ban torch nay khong
        co thu vien toi uu -> cai lai dung ban cu.
   numpy GEMM CUNG CHAM                          -> PHAN CUNG/HOST (muc 4).
   torch.get_num_threads() = 1 ma vCPU = 4       -> chi chay 1 core.
        Sua: performance.torch_threads: <so vCPU>  hoac  --threads <n>

 Muc 4, cac cot cua vmstat:
   st > 5   -> host bi oversubscribe, hypervisor cuop CPU cua VM nay
   si/so > 0-> DANG SWAP, tham hoa cho hieu nang
   wa cao   -> nghen dia
   r > vCPU -> qua nhieu tien trinh cho CPU

 CHAY TREN TUNG VM roi so sanh. Neu tat ca deu cham nhu nhau -> host.
 Neu chi vai may cham -> rieng may do.
""")
