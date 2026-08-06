import os
import pika
import uuid
import argparse
import yaml

import torch

import src.Log
from src.RpcClient import RpcClient
from src.Scheduler import Scheduler

parser = argparse.ArgumentParser(description="Split learning framework")
parser.add_argument('--layer_id', type=int, required=True, help='ID of layer, start from 1')
parser.add_argument('--device', type=str, required=False, help='Device of client')
parser.add_argument('--name', type=str, required=False, default=None, help='Name of this machine (e.g. machine-2, device-1)')
parser.add_argument('--threads', type=int, required=False, default=None,
                    help='torch intra-op threads for THIS process (overrides performance.torch_threads)')

args = parser.parse_args()

with open('config.yaml', 'r', encoding='utf-8') as file:
    config = yaml.safe_load(file)


def _apply_torch_threads(cfg, layer_id, override):
    """Cap this process's torch intra-op threads.

    torch defaults to one thread per core PER PROCESS, which is right for one
    process per machine and badly wrong here: 9 edge processes on one 20-core box
    ask for 180 compute threads on 20 cores. The oversubscription slows every
    process down without raising aggregate throughput.

    'auto' divides the host's cores by the number of processes that share this
    role, which matches the deployment (all edges on one machine, all clouds on
    another). 0/None leaves torch untouched; an int sets it directly.
    """
    want = override if override is not None else cfg.get("performance", {}).get("torch_threads", 0)
    if want in (None, 0, "0", False):
        return
    if isinstance(want, str) and want.strip().lower() == "auto":
        clients = cfg["server"]["clients"]
        # layer_id 1 == edge (clients[0]), anything else == cloud (clients[1]).
        peers = clients[0] if layer_id == 1 else clients[1]
        want = max(1, (os.cpu_count() or 1) // max(1, int(peers)))
    try:
        n = max(1, int(want))
    except (TypeError, ValueError):
        print(f"[Threads] ignoring invalid performance.torch_threads={want!r}")
        return
    torch.set_num_threads(n)
    print(f"[Threads] torch intra-op threads = {n} (host has {os.cpu_count()} cores)")


_apply_torch_threads(config, args.layer_id, args.threads)

client_id = uuid.uuid4()
address = config["rabbit"]["address"]
username = config["rabbit"]["username"]
password = config["rabbit"]["password"]
virtual_host = config["rabbit"]["virtual-host"]

device = None

if args.device is None:
    if torch.cuda.is_available():
        device = "cuda"
        print(f"Using device: {torch.cuda.get_device_name(device)}")
    else:
        device = "cpu"
        print(f"Using device: CPU")
else:
    device = args.device
    print(f"Using device: {device}")

logger = src.Log.Logger(config['debug-mode'])
logger.log_info(f"Application start.")

credentials = pika.PlainCredentials(username, password)
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host=address,
        port=5672,
        virtual_host=f"{virtual_host}",
        credentials=credentials,
        heartbeat=3600,
        blocked_connection_timeout=600
    )
)
channel = connection.channel()

if __name__ == "__main__":
    src.Log.print_with_color("[>>>] Client sending registration message to server...", "red")

    layer_times = None
    model_name = config["server"]["model"]
    clustering_cfg = config.get("clustering", {})
    use_real_profile = clustering_cfg.get("profile_source", "auto") != "simulated"
    if clustering_cfg.get("enable", False) and use_real_profile and os.path.exists(f"{model_name}.pt"):
        try:
            from src.Profiler import profile_or_load
            ckpt = torch.load(f"{model_name}.pt", map_location=device, weights_only=False)
            model_obj = ckpt["model"].float().eval().to(device)
            layer_times = profile_or_load(
                model_name, model_obj, device,
                batch_size=config["server"]["batch-size"]
            ).tolist()
            del model_obj, ckpt
        except Exception as e:
            src.Log.print_with_color(f"[Profile] Warning: {e}", "yellow")
    elif not use_real_profile:
        src.Log.print_with_color("[Profile] Skipped (profile_source=simulated)", "yellow")

    bandwidth_mb_s = None
    if clustering_cfg.get("enable", False):
        if clustering_cfg.get("measure_bandwidth", True):
            try:
                from src.Profiler import measure_bandwidth
                bandwidth_mb_s = measure_bandwidth(channel, str(client_id))
            except Exception as e:
                src.Log.print_with_color(f"[Bandwidth] Warning: {e}", "yellow")
                if not channel.is_open:
                    channel = connection.channel()
        else:
            bandwidth_mb_s = float(clustering_cfg.get("network_rate_mb_s", 100.0))
            src.Log.print_with_color(f"[Bandwidth] Using fixed rate from config: {bandwidth_mb_s} MB/s", "cyan")

    data = {"action": "REGISTER", "client_id": client_id, "layer_id": args.layer_id,
            "message": "Hello from Client!", "layer_times": layer_times,
            "bandwidth_mb_s": bandwidth_mb_s, "client_name": args.name}
    scheduler = Scheduler(client_id, args.layer_id, channel, device)
    logger.log_debug(f"client_id : {client_id} , stage {args.layer_id} , "
                     f"channel {channel} , device {device}")
    client = RpcClient(client_id, args.layer_id, channel ,logger ,scheduler.inference_func, device)
    client.send_to_server(data)
    client.wait_response()
