import os
import pickle
import time
import numpy as np
import torch
import src.Log as Log


def profile_or_load(model_name: str, model, device: str,
                    batch_size: int = 4, warmup: int = 10, runs: int = 100):
    """
    Profile per-layer inference time of model on device.
    Returns np.array of shape (n_layers,) — mean seconds per layer per batch.
    Cache saved as profile_{model_name}_{device}.npy next to client.py.
    """
    cache_path = f"profile_{model_name}_{device}_bs{batch_size}.npy"

    if os.path.exists(cache_path):
        times = np.load(cache_path)
        Log.print_with_color(
            f"[Profile] Loaded cache '{cache_path}'  "
            f"({len(times)} layers, total={times.sum()*1000:.1f} ms/batch)",
            "green"
        )
        return times

    Log.print_with_color(
        f"[Profile] Profiling {model_name} on {device} "
        f"({warmup} warmup + {runs} runs) ...",
        "yellow"
    )

    # Load via Ultralytics YOLO (nhất quán với measure_time_layer.py)
    from ultralytics import YOLO
    _yolo = YOLO(f"{model_name}.pt")
    _profile_model = _yolo.model.float().eval().to(device)

    layers = _profile_model.model
    n = len(layers)
    is_cuda = (device != "cpu" and torch.cuda.is_available())
    start_events = {}
    end_events   = {}
    layer_times  = [[] for _ in range(n)]
    hooks = []

    for i in range(n):
        def _pre(idx):
            def fn(m, inp):
                if is_cuda:
                    ev = torch.cuda.Event(enable_timing=True)
                    ev.record()
                    start_events[idx] = ev
                else:
                    start_events[idx] = time.perf_counter()
            return fn

        def _post(idx):
            def fn(m, inp, out):
                if is_cuda:
                    ev = torch.cuda.Event(enable_timing=True)
                    ev.record()
                    end_events[idx] = ev
                else:
                    layer_times[idx].append(time.perf_counter() - start_events[idx])
            return fn

        hooks.append(layers[i].register_forward_pre_hook(_pre(i)))
        hooks.append(layers[i].register_forward_hook(_post(i)))

    if is_cuda:
        dummy = torch.randn(batch_size, 3, 640, 640).to(device).half()
        _profile_model.half()
    else:
        dummy = torch.randn(batch_size, 3, 640, 640).to(device)

    # Warmup + Benchmark
    with torch.inference_mode():
        for _ in range(warmup + runs):
            _profile_model(dummy)
            if is_cuda:
                torch.cuda.synchronize()
                for i in range(n):
                    t_ms = start_events[i].elapsed_time(end_events[i])
                    layer_times[i].append(t_ms / 1000.0)

    for h in hooks:
        h.remove()

    # Average TẤT CẢ measurements (warmup + benchmark) — nhất quán với measure_time_layer.py
    avg = np.array([
        np.mean(layer_times[i]) if layer_times[i] else 0.0
        for i in range(n)
    ])

    np.save(cache_path, avg)
    Log.print_with_color(
        f"[Profile] Saved '{cache_path}'  "
        f"(total={avg.sum()*1000:.1f} ms/batch)",
        "green"
    )
    return avg


def measure_bandwidth(channel, client_id: str,
                      payload_size_mb: float = 1.0,
                      runs: int = 3) -> float:
    """
    Đo băng thông uplink (edge → server) qua RabbitMQ.
    Gửi payload kích thước biết trước, đợi server ACK, tính throughput.
    Returns: bandwidth ước tính (MB/s).
    """
    reply_queue = f"reply_{client_id}"
    channel.queue_declare(reply_queue, durable=False)

    payload = os.urandom(int(payload_size_mb * 1024 * 1024))

    timeout_s = 30.0
    samples = []
    for _ in range(runs):
        message = pickle.dumps(
            {
                "action": "BW_TEST",
                "client_id": client_id,
                "payload": payload,
            },
            protocol=pickle.HIGHEST_PROTOCOL,
        )
        t_start = time.perf_counter()
        channel.basic_publish(exchange='', routing_key='rpc_queue', body=message)

        while True:
            _, _, body = channel.basic_get(queue=reply_queue, auto_ack=True)
            if body:
                break
            if time.perf_counter() - t_start > timeout_s:
                raise TimeoutError(f"Bandwidth measurement timed out after {timeout_s}s (server not responding)")
            time.sleep(0.005)

        elapsed = time.perf_counter() - t_start
        samples.append(payload_size_mb / max(elapsed, 1e-6))

    bw = float(np.median(samples))
    Log.print_with_color(
        f"[Bandwidth] {bw:.1f} MB/s  "
        f"(samples: {[f'{s:.1f}' for s in samples]} MB/s, payload={payload_size_mb} MB x{runs})",
        "cyan"
    )
    return bw


def measure_compression_rate(role: str, batch_size: int = 32, num_bits: int = 8,
                             target_mb: float = 30.0, runs: int = 5) -> float:
    """
    Do toc do nen/giai nen Q-DeltaMask THAT tren chinh thiet bi nay (CPU rieng
    cua may, khac nhau giua tung loai may bien/cloud).

    role="edge"  -> do thoi gian Encoder() (nen)   -> tra encode_rate (ms/MB)
    role="cloud" -> do thoi gian Decoder() (giai nen) -> tra decode_rate (ms/MB)

    Dung tensor mo phong khung hinh video THAT (khung sau = khung truoc +
    nhieu nho, KHONG dung torch.randn doc lap tung khung) vi Q-DeltaMask delta-
    encode theo chieu batch - nhieu doc lap se pha vo gia dinh tuong quan thoi
    gian, cho ra so do sai (xem thao luan da chot trong du an).
    Da verify qua nhieu kich thuoc (12-89 MB): ti le ms/MB on dinh (tuyen tinh
    theo kich thuoc), nen chi can do 1 lan o target_mb roi dung truc tiep cho
    moi cut khac (nhan voi cut_data_sizes[cut]).
    """
    import time
    from src.Compress import Encoder, Decoder

    if role not in ("edge", "cloud"):
        raise ValueError(f"role phai la 'edge' hoac 'cloud', nhan duoc {role!r}")

    rng = np.random.default_rng(0)
    n_elements = int((target_mb * 1024 * 1024) / batch_size / 1.3)
    base = rng.standard_normal(n_elements).astype(np.float32)
    frames = [base]
    for _ in range(batch_size - 1):
        frames.append(frames[-1] + rng.standard_normal(n_elements).astype(np.float32) * 0.01)
    tensor = np.stack(frames).reshape(batch_size, n_elements)
    data_output = [tensor]

    samples = []
    for _ in range(runs):
        if role == "edge":
            t0 = time.perf_counter()
            encoded, shapes = Encoder(data_output, num_bits=num_bits)
            elapsed_s = time.perf_counter() - t0
        else:
            encoded, shapes = Encoder(data_output, num_bits=num_bits)
            t0 = time.perf_counter()
            _ = Decoder(encoded, shapes)
            elapsed_s = time.perf_counter() - t0
        actual_mb = sum(len(e) for e in encoded if e is not None) / (1024 * 1024)
        samples.append((elapsed_s * 1000.0) / actual_mb)

    rate = float(np.median(samples))
    label = "Encode" if role == "edge" else "Decode"
    Log.print_with_color(
        f"[Compress] {label} rate ({role}): {rate:.3f} ms/MB  "
        f"(samples: {[f'{s:.3f}' for s in samples]}, target={target_mb} MB x{runs})",
        "magenta"
    )
    return rate
