import argparse
import sys
import signal
from src.Server import Server
from src.Utils import delete_old_queues
import src.Log
import yaml

parser = argparse.ArgumentParser(description="Split learning framework with controller.")
args = parser.parse_args()

with open('config.yaml', 'r', encoding='utf-8') as file:
    config = yaml.safe_load(file)

address = config["rabbit"]["address"]
username = config["rabbit"]["username"]
password = config["rabbit"]["password"]
virtual_host = config["rabbit"]["virtual-host"]


def signal_handler(sig, frame):
    print("\nCatch stop signal Ctrl+C. Stop the program.")
    delete_old_queues(address, username, password, virtual_host)
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    # NOTE: this clears QUEUES on the broker. It cannot and does not kill leftover
    # server PROCESSES — it only takes their queues away, which is what makes a
    # stale server stop consuming and exit. If it fails, nothing is cleaned and a
    # stale server keeps a share of every DONE, so say so loudly instead of
    # continuing silently (the return value used to be ignored).
    if not delete_old_queues(address, username, password, virtual_host):
        src.Log.print_with_color(
            "[FATAL] Could not reach the RabbitMQ management API at "
            f"http://{address}:15672 — old queues were NOT cleaned.\n"
            "        Without that cleanup a server left over from an earlier run "
            "keeps consuming fps_queue and every FPS number is divided between "
            "them. Check the management plugin, port 15672 and the credentials.",
            "red")
        sys.exit(1)
    server = Server(config)
    server.start()
