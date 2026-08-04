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
    Cache saved as profile_{model_name}_{device}_bs{batch_size}[_fp32].npy next to
    client.py.

    The _fp32 suffix appears ONLY for CUDA devices. That is where the measurement
    semantics actually changed: the old code profiled in half() while the pipeline
    runs float(), so a CUDA cache from before must not be reused. On CPU the old
    code never called half() (is_cuda was False), so an existing CPU cache is still
    valid and keeps its name — renaming it would have forced every CPU device to
    re-profile for no reason, and re-profiling changes the numbers the solver
    clusters on, which is exactly the confound you don't want mid-comparison.
    """
    _cuda_cache = (device != "cpu")
    suffix = "_fp32" if _cuda_cache else ""
    cache_path = f"profile_{model_name}_{device}_bs{batch_size}{suffix}.npy"

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

    # FP32, matching the runtime. The pipeline runs the model in float
    # (RpcClient loads it with .float()), so profiling in half() produced a cost
    # table for a precision that never executes — and it is not a uniform
    # rescale: FP16 speedup differs per layer type, which distorts exactly the
    # RELATIVE per-layer costs the Hungarian solver picks the cut from.
    dummy = torch.randn(batch_size, 3, 640, 640).to(device)

    # Warmup + Benchmark
    with torch.no_grad():
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
                      payload_size_mb: float = None,
                      runs: int = None,
                      mode: str = "new") -> float:
    """Dispatch to the legacy or the new probe (clustering.measure_mode).

    Two probes are kept side by side ON PURPOSE so a run can be reproduced with
    either, and the two compared. They measure different things and therefore
    return different numbers — that difference is the whole point of the A/B:

      legacy — 1MB through rpc_queue -> server -> reply_queue, waiting for an ACK
               with a 5ms polling loop. Includes the server's response time and
               quantises every sample to 5ms. This is what produced the cuts used
               by the July runs.
      new    — 4MB published straight into a throwaway queue on this device's own
               channel: the same path the run itself uses, no server in the middle,
               no polling quantisation.

    payload_size_mb / runs default per mode when left as None, and can be pinned
    from config to re-measure with different settings.
    """
    if str(mode).strip().lower() == "legacy":
        return _measure_bandwidth_legacy(
            channel, client_id,
            1.0 if payload_size_mb is None else payload_size_mb,
            3 if runs is None else runs)
    return _measure_bandwidth_direct(
        channel, client_id,
        4.0 if payload_size_mb is None else payload_size_mb,
        5 if runs is None else runs)


def _measure_bandwidth_legacy(channel, client_id: str,
                              payload_size_mb: float = 1.0,
                              runs: int = 3) -> float:
    """The July probe, kept verbatim so old results stay reproducible.

    Round trip: publish to 'rpc_queue' -> server replies BW_ACK on reply_<id> ->
    poll for it every 5ms. The measured time therefore includes the server's
    turnaround, and is quantised to the 5ms poll. Both effects push the estimate
    DOWN, which made the solver treat big feature maps as expensive and pick
    shallower cuts.
    """
    reply_queue = f"reply_{client_id}"
    channel.queue_declare(reply_queue, durable=False)
    payload = os.urandom(int(payload_size_mb * 1024 * 1024))

    timeout_s = 30.0
    samples = []
    for _ in range(runs):
        message = pickle.dumps({
            "action": "BW_TEST",
            "client_id": client_id,
            "payload": payload,
        })
        t_start = time.perf_counter()
        channel.basic_publish(exchange='', routing_key='rpc_queue', body=message)
        while True:
            _, _, body = channel.basic_get(queue=reply_queue, auto_ack=True)
            if body:
                break
            if time.perf_counter() - t_start > timeout_s:
                raise TimeoutError(
                    f"Bandwidth measurement timed out after {timeout_s}s (server not responding)")
            time.sleep(0.005)
        samples.append(payload_size_mb / max(time.perf_counter() - t_start, 1e-6))

    bw = float(np.median(samples))
    Log.print_with_color(
        f"[Bandwidth][legacy] {bw:.1f} MB/s  "
        f"(samples: {[f'{s:.1f}' for s in samples]} MB/s, payload={payload_size_mb} MB x{runs})",
        "cyan")
    return bw


def _measure_bandwidth_direct(channel, client_id: str,
                              payload_size_mb: float = 4.0,
                              runs: int = 5) -> float:
    """
    Đo băng thông uplink egress của device này (device → broker) qua RabbitMQ.
    Returns: bandwidth ước tính (MB/s).

    Measures the SAME path the run uses: a blocking basic_publish of a
    feature-map-sized body into a throwaway queue on this device's own channel.
    Deliberately NOT a round trip through the server, and deliberately no ACK
    polling — two things the previous version got wrong:

      * routing through 'rpc_queue' meant the number included the server's
        response time. That server is single-threaded pika at prefetch_count=1 and
        is busy running registration (Hungarian, torch.load) at exactly this
        moment, so the "bandwidth" was partly a measure of how distracted the
        server was;
      * `while ...: basic_get(); time.sleep(0.005)` quantised every sample to 5ms.
        A 1MB payload on a fast link takes ~10ms, so the quantisation alone was a
        ~50% error, which is why the estimate — and therefore the cut the solver
        picked from it — moved run to run.

    NO per-edge divisor is applied, on purpose. The solver models edges as
    independent parallel producers (Clustering.pair_metrics_for_cut sums 1/tau_i),
    so it needs each edge's OWN achievable share. When the co-located edges
    measure at the same time, contention is already in each measurement and
    dividing again would double-count it. That does mean the number is only right
    if the measurements overlap — the server logs the spread across clients so a
    staggered (and therefore too-optimistic) measurement is visible instead of
    silent. Pin clustering.network_rate_mb_s with measure_bandwidth: False when
    you need a reproducible cut.

    payload_size_mb defaults to 4MB rather than 1MB so the sample is dominated by
    transfer rather than by per-message overhead, and is closer to the size of a
    real intermediate feature map.
    """
    bw_queue = f"bwtest_{str(client_id).replace('-', '')}"
    channel.queue_declare(bw_queue, durable=False)
    channel.queue_purge(bw_queue)

    payload = os.urandom(int(payload_size_mb * 1024 * 1024))
    body = pickle.dumps({"action": "BW_TEST", "client_id": client_id, "payload": payload})
    on_wire_mb = len(body) / (1024 * 1024)

    samples = []
    try:
        for _ in range(runs):
            t_start = time.perf_counter()
            channel.basic_publish(exchange='', routing_key=bw_queue, body=body)
            elapsed = time.perf_counter() - t_start
            samples.append(on_wire_mb / max(elapsed, 1e-9))
            # Don't let the queue hold the payloads: several edges doing this at
            # once would otherwise park runs x payload_size_mb each in the broker.
            channel.queue_purge(bw_queue)
    finally:
        try:
            channel.queue_delete(queue=bw_queue)
        except Exception:
            pass

    if not samples:
        raise RuntimeError("Bandwidth measurement produced no samples")

    bw = float(np.median(samples))
    Log.print_with_color(
        f"[Bandwidth] {bw:.1f} MB/s egress  "
        f"(samples: {[f'{s:.1f}' for s in samples]} MB/s, body={on_wire_mb:.2f} MB x{runs})",
        "cyan"
    )
    return bw
