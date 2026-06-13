import pickle
import queue
import threading
import time
from dataclasses import dataclass

import pika
import torch

from src.Compress import Encoder


def transport_config(config=None):
    cfg = dict(config or {})
    max_messages = int(cfg.get("rabbit_max_queue_messages", cfg.get("max_queue_messages", 8)))
    default_high = max(1, max_messages - 2) if max_messages > 0 else 0
    high = int(cfg.get("backpressure_high_watermark", default_high))
    low = int(cfg.get("backpressure_low_watermark", max(0, high // 2)))

    return {
        "async_publish": bool(cfg.get("async_publish", True)),
        "local_queue_size": int(cfg.get("local_queue_size", 2)),
        "rabbit_max_queue_messages": max_messages,
        "rabbit_max_queue_bytes": int(cfg.get("rabbit_max_queue_bytes", 0)),
        "rabbit_overflow": cfg.get("rabbit_overflow", "reject-publish"),
        "backpressure_high_watermark": high,
        "backpressure_low_watermark": min(low, high),
        "backpressure_poll_sec": float(cfg.get("backpressure_poll_sec", 0.02)),
        "publish_confirm": bool(cfg.get("publish_confirm", True)),
        "consumer_prefetch": int(cfg.get("consumer_prefetch", 1)),
        "consumer_poll_sec": float(cfg.get("consumer_poll_sec", 0.02)),
    }


def queue_arguments(config=None):
    cfg = transport_config(config)
    args = {}
    if cfg["rabbit_max_queue_messages"] > 0:
        args["x-max-length"] = cfg["rabbit_max_queue_messages"]
        args["x-overflow"] = cfg["rabbit_overflow"]
    if cfg["rabbit_max_queue_bytes"] > 0:
        args["x-max-length-bytes"] = cfg["rabbit_max_queue_bytes"]
        args["x-overflow"] = cfg["rabbit_overflow"]
    return args or None


def rabbit_parameters(rabbit_config):
    credentials = pika.PlainCredentials(rabbit_config["username"], rabbit_config["password"])
    return pika.ConnectionParameters(
        host=rabbit_config["address"],
        port=5672,
        virtual_host=f"{rabbit_config['virtual-host']}",
        credentials=credentials,
        heartbeat=3600,
        blocked_connection_timeout=600,
    )


def declare_intermediate_queue(channel, queue_name, config=None):
    channel.queue_declare(queue=queue_name, durable=False, arguments=queue_arguments(config))


@dataclass
class PublishReceipt:
    size_bytes: int
    prepare_ms: float
    remote_wait_ms: float
    publish_ms: float
    local_wait_ms: float = 0.0
    completed_perf: float = 0.0


class PublishFuture:
    def __init__(self):
        self._event = threading.Event()
        self._receipt = None
        self._error = None
        self.local_wait_ms = 0.0

    def done(self):
        return self._event.is_set()

    def set_result(self, receipt):
        self._receipt = receipt
        self._event.set()

    def set_exception(self, error):
        self._error = error
        self._event.set()

    def result(self, timeout=None):
        if not self._event.wait(timeout):
            raise TimeoutError("publish did not finish before timeout")
        if self._error is not None:
            raise self._error
        return self._receipt


class CompletedPublishFuture(PublishFuture):
    def __init__(self, receipt):
        super().__init__()
        self.set_result(receipt)


class _StopPublisher:
    pass


def prepare_intermediate_message(data, compress):
    compress = compress or {"enable": False}
    start = time.perf_counter()
    payload = dict(data)
    outputs = payload.get("data", [])

    if compress.get("enable"):
        cpu_outputs = [
            t.detach().cpu().numpy() if isinstance(t, torch.Tensor) else None
            for t in outputs
        ]
        payload["data"], payload["shape"] = Encoder(
            data_output=cpu_outputs,
            num_bits=compress["num_bit"],
        )
    else:
        payload["data"] = [
            t.detach().cpu() if isinstance(t, torch.Tensor) else None
            for t in outputs
        ]

    body = pickle.dumps({"action": "OUTPUT", "data": payload}, protocol=pickle.HIGHEST_PROTOCOL)
    prepare_ms = (time.perf_counter() - start) * 1000
    return body, len(body), prepare_ms


class RabbitAsyncPublisher:
    def __init__(self, rabbit_config, transport, logger=None):
        self.rabbit_config = rabbit_config
        self.transport = transport_config(transport)
        self.logger = logger
        self._jobs = queue.Queue(maxsize=max(1, self.transport["local_queue_size"]))
        self._thread = threading.Thread(target=self._run, name="rabbit-publisher", daemon=True)
        self._thread.start()

    def submit(self, queue_name, data, compress):
        future = PublishFuture()
        start = time.perf_counter()
        self._jobs.put((queue_name, data, compress, future))
        future.local_wait_ms = (time.perf_counter() - start) * 1000
        return future

    def close(self):
        self._jobs.put(_StopPublisher())
        self._thread.join()

    def _connect(self):
        connection = pika.BlockingConnection(rabbit_parameters(self.rabbit_config))
        channel = connection.channel()
        if self.transport["publish_confirm"]:
            channel.confirm_delivery()
        return connection, channel

    def _wait_remote_backpressure(self, channel, queue_name):
        high = self.transport["backpressure_high_watermark"]
        if high <= 0:
            return 0.0

        low = self.transport["backpressure_low_watermark"]
        poll_sec = self.transport["backpressure_poll_sec"]
        start = time.perf_counter()

        while True:
            depth = channel.queue_declare(queue=queue_name, passive=True).method.message_count
            if depth < high:
                return (time.perf_counter() - start) * 1000
            while depth > low:
                time.sleep(poll_sec)
                depth = channel.queue_declare(queue=queue_name, passive=True).method.message_count

    def _publish_with_retry(self, channel, queue_name, body):
        while True:
            try:
                start = time.perf_counter()
                published = channel.basic_publish(exchange="", routing_key=queue_name, body=body)
                if published is False:
                    time.sleep(self.transport["backpressure_poll_sec"])
                    continue
                return (time.perf_counter() - start) * 1000
            except pika.exceptions.NackError:
                time.sleep(self.transport["backpressure_poll_sec"])

    def _run(self):
        connection = None
        channel = None

        try:
            try:
                connection, channel = self._connect()
            except Exception as exc:
                self._fail_pending_jobs(exc)
                return

            while True:
                job = self._jobs.get()
                if isinstance(job, _StopPublisher):
                    self._jobs.task_done()
                    break

                queue_name, data, compress, future = job
                try:
                    declare_intermediate_queue(channel, queue_name, self.transport)
                    body, size_bytes, prepare_ms = prepare_intermediate_message(data, compress)
                    remote_wait_ms = self._wait_remote_backpressure(channel, queue_name)
                    publish_ms = self._publish_with_retry(channel, queue_name, body)
                    completed_perf = time.perf_counter()
                    future.set_result(
                        PublishReceipt(
                            size_bytes=size_bytes,
                            prepare_ms=prepare_ms,
                            remote_wait_ms=remote_wait_ms,
                            publish_ms=publish_ms,
                            local_wait_ms=future.local_wait_ms,
                            completed_perf=completed_perf,
                        )
                    )
                except Exception as exc:
                    future.set_exception(exc)
                finally:
                    self._jobs.task_done()
        finally:
            if channel is not None and channel.is_open:
                channel.close()
            if connection is not None and connection.is_open:
                connection.close()

    def _fail_pending_jobs(self, exc):
        while True:
            job = self._jobs.get()
            if isinstance(job, _StopPublisher):
                self._jobs.task_done()
                break
            try:
                _, _, _, future = job
                future.set_exception(exc)
            finally:
                self._jobs.task_done()
