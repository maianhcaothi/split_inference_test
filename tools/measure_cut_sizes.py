"""
Measure CUT_DATA_SIZES_MB for any YOLO model.
Auto-saves result into src/Clustering.py.

Usage (run from split_inference_test/):
    python tools/measure_cut_sizes.py --model yolo26x --batch_size 4 --compress --num_bit 8
    python tools/measure_cut_sizes.py --model yolo11x --batch_size 4 --compress --num_bit 8
"""
import argparse
import pickle
import re
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

print("[1/5] Importing libraries...", flush=True)
try:
    import numpy as np
    import torch
    from src.Compress import Encoder
except Exception as e:
    print(f"[ERROR] Import failed: {e}", flush=True)
    sys.exit(1)

parser = argparse.ArgumentParser()
parser.add_argument("--model", required=True, help="Model name, e.g. yolo26x")
parser.add_argument("--batch_size", type=int, default=4)
parser.add_argument("--compress", action="store_true", help="Simulate compression")
parser.add_argument("--num_bit", type=int, default=8)
parser.add_argument("--device", default="cpu")
args = parser.parse_args()

print(f"[2/5] Loading {args.model}.pt ...", flush=True)
model_pt = f"{args.model}.pt"
if not os.path.exists(model_pt):
    print(f"[ERROR] {model_pt} not found. Run from split_inference_test/ directory.", flush=True)
    sys.exit(1)

try:
    ckpt = torch.load(model_pt, map_location=args.device, weights_only=False)
    model_obj = ckpt["model"].float().eval().to(args.device)
    layers = model_obj.model
except Exception as e:
    print(f"[ERROR] {e}", flush=True)
    sys.exit(1)

n = len(layers)
print(f"[3/5] Running ONE forward pass ({n} layers, batch={args.batch_size})...", flush=True)

# Build save_set from layer.f
save_set = set()
for layer in layers:
    f = getattr(layer, 'f', None)
    if f is None:
        print("[WARNING] layers missing .f — skip connections may be wrong", flush=True)
        break
    if isinstance(f, (list, tuple)):
        for fi in f:
            if fi != -1:
                save_set.add(fi)
    elif isinstance(f, int) and f != -1:
        save_set.add(f)

# Single forward pass — collect ALL layer outputs
dummy = torch.randn(args.batch_size, 3, 640, 640).to(args.device)
all_outputs = []  # all_outputs[i] = output tensor of layer i

with torch.no_grad():
    x = dummy
    y = []
    for i, layer in enumerate(layers):
        f = getattr(layer, 'f', -1)
        if isinstance(f, (list, tuple)):
            x = [x if fi == -1 else y[fi] for fi in f]
        elif isinstance(f, int) and f != -1:
            x = y[f]
        x = layer(x)
        if isinstance(x, tuple):
            x = x[0]
        all_outputs.append(x)
        y.append(x if i in save_set else None)

print(f"[4/5] Measuring message size at each cut point...", flush=True)

cut_sizes_mb = []
for cut in range(n - 1):
    # Build y list as edge would send it (up to layer `cut`)
    y_edge = []
    for i in range(cut + 1):
        if i in save_set or i == cut:
            y_edge.append(all_outputs[i])
        else:
            y_edge.append(None)

    try:
        if args.compress:
            tensors = [t.cpu().numpy() if isinstance(t, torch.Tensor) else None for t in y_edge]
            compressed, shape = Encoder(data_output=tensors, num_bits=args.num_bit)
            payload = {"action": "OUTPUT", "data": {
                "data": compressed, "shape": shape,
                "width": 640, "height": 640, "edge_start_time": 0.0
            }}
        else:
            tensors = [t.cpu() if isinstance(t, torch.Tensor) else None for t in y_edge]
            payload = {"action": "OUTPUT", "data": {
                "data": tensors, "width": 640, "height": 640, "edge_start_time": 0.0
            }}

        size_mb = len(pickle.dumps(payload)) / (1024 * 1024)
        cut_sizes_mb.append(round(size_mb, 2))
        print(f"  cut={cut:2d}  size={size_mb:.2f} MB", flush=True)
    except Exception as e:
        print(f"  cut={cut:2d}  ERROR: {e}", flush=True)
        cut_sizes_mb.append(0.0)

# Save to Clustering.py — key includes batch_size to avoid overwrite confusion
key = f"{args.model}_bs{args.batch_size}"
arr = np.array(cut_sizes_mb)
vals = ", ".join(str(v) for v in arr.tolist())
new_entry = f'    "{key}": np.array([\n        {vals}\n    ], dtype=float),  # batch_size={args.batch_size}'

clustering_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "src", "Clustering.py"
)

print(f"\n[5/5] Saving to Clustering.py (key='{key}') ...", flush=True)
with open(clustering_path, "r") as f:
    content = f.read()

marker = "CUT_DATA_SIZES_MB_BY_MODEL = {"
if f'"{key}": np.array' in content:
    pattern = rf'    "{re.escape(key)}": np\.array\(\[.*?\], dtype=float\)[^\n]*,'
    content = re.sub(pattern, new_entry, content, flags=re.DOTALL)
    action = "updated"
else:
    content = content.replace(marker, marker + "\n" + new_entry, 1)
    action = "added"

with open(clustering_path, "w") as f:
    f.write(content)

print(f"[OK] {action} '{args.model}' in Clustering.py", flush=True)
print(f"Values: {arr.tolist()}", flush=True)
