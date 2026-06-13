# Split Inference for YOLO

This project runs YOLO-style object detection as a distributed split-inference
pipeline. The model is divided into two parts:

- **Edge/head node**: reads video frames and runs the first part of the model.
- **Cloud/tail node**: receives intermediate activations and runs the rest of the model.

RabbitMQ is used to coordinate clients and transfer intermediate data. The
pipeline supports feature-map compression, automatic split selection, and
bounded queue back-pressure so the edge does not overload the cloud receiver.

## Why Split Inference?

Sending full video frames to a cloud server is simple, but it can waste network
bandwidth and increase latency. Split inference moves the early model layers to
the edge device and sends only the intermediate tensors needed by the remaining
layers.

Typical use cases:

- Jetson Nano or other embedded edge devices.
- Traffic camera or surveillance workloads.
- Experiments that compare edge-only, cloud-only, and split execution.
- Bandwidth and latency measurements for distributed inference.

## Architecture

```text
+------------------+        RabbitMQ         +------------------+
| Edge client      |  intermediate tensors   | Cloud client     |
| layer_id = 1     +------------------------>+ layer_id = 2     |
|                  |                         |                  |
| read video       |                         | receive tensors  |
| preprocess       |                         | decode tensors   |
| run head layers  |                         | run tail layers  |
| compress/send    |                         | postprocess      |
+--------+---------+                         +---------+--------+
         |                                             |
         | registration / start / stop                 |
         v                                             v
                  +--------------------------+
                  | Controller server        |
                  | server.py                |
                  | register clients         |
                  | choose split point       |
                  | send run configuration   |
                  +--------------------------+
```

### Components

| File | Responsibility |
| --- | --- |
| `server.py` | Starts the controller service and cleans old RabbitMQ queues. |
| `client.py` | Starts an edge or cloud inference client. |
| `src/Server.py` | Registers clients, selects split points, and sends run commands. |
| `src/Scheduler.py` | Runs edge/cloud inference loops and records metrics. |
| `src/Transport.py` | Handles RabbitMQ queue limits, async publishing, and back-pressure. |
| `src/Compress.py` | Quantizes and delta-encodes intermediate feature maps. |
| `src/Model.py` | Runs partial YOLO layers and postprocesses detections. |
| `src/Profiler.py` | Profiles per-layer model latency and measures RabbitMQ bandwidth. |
| `src/Clustering.py` | Selects split points and edge/cloud assignment with Hungarian matching. |

## Runtime Modes

The project can run in three modes:

| Mode | Description |
| --- | --- |
| `split` | Edge runs head layers, cloud runs tail layers. This is the default when `experiment.enable: False`. |
| `only_edge` | Edge runs the full model. Cloud only receives lightweight result messages. |
| `only_cloud` | Edge sends raw frames, cloud runs the full model. Useful as a baseline. |

To enable a baseline mode, set:

```yaml
experiment:
  enable: True
  mode: only_cloud   # split, only_edge, or only_cloud
```

If `experiment.enable` is `False`, the code uses `split` mode even if
`experiment.mode` is present in `config.yaml`.

## Data Flow

1. Start RabbitMQ.
2. Start `server.py`.
3. Start one or more clients with `client.py --layer_id 1` for edge nodes.
4. Start one or more clients with `client.py --layer_id 2` for cloud nodes.
5. Clients register with the controller through `rpc_queue`.
6. The controller waits until the expected number of edge and cloud clients is connected.
7. The controller sends model/configuration data to each client.
8. Edge clients process video batches and publish intermediate payloads.
9. Cloud clients consume payloads, finish inference, postprocess detections, and write metrics.
10. When edge clients finish, the controller sends stop messages and metrics are pivoted.

## Installation

Python 3.8 or newer is recommended.

```bash
pip install -r requirements.txt
```

The project expects a YOLO checkpoint named by `server.model`, for example:

```text
yolo26n.pt
```

If the checkpoint is missing, the server attempts to load/download it through
Ultralytics.

## RabbitMQ

RabbitMQ must be reachable by all machines.

Default local settings:

```text
AMQP:       localhost:5672
Dashboard: http://localhost:15672
Username:  guest
Password:  guest
```

For Docker:

```bash
docker run --rm -it \
  --name split-rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management
```

On multiple machines, set `rabbit.address` in `config.yaml` to the IP address
of the RabbitMQ host.

## Configuration

Main configuration lives in `config.yaml`.

```yaml
name: YOLO

server:
  model: yolo26n
  batch-size: 32
  cut-layer: a
  clients:
    - 1   # number of edge clients
    - 1   # number of cloud clients

experiment:
  enable: False
  mode: only_cloud

rabbit:
  address: localhost
  username: guest
  password: guest
  virtual-host: /

data: video.mp4
log-path: .
debug-mode: False

compress:
  enable: True
  num_bit: 8
```

### Split Selection

Use a fixed split by disabling clustering:

```yaml
clustering:
  enable: False
```

Then choose one of the predefined split labels:

```yaml
server:
  cut-layer: a   # a, b, c, or d
```

Use automatic split selection with profiling and Hungarian matching:

```yaml
clustering:
  enable: True
  max_clusters: 3
  measure_bandwidth: True
  profile_source: real   # real, auto, or simulated
```

When `profile_source: real`, every edge and cloud client must be able to load
the model checkpoint and profile its local layer times.

### Transport and Back-Pressure

Intermediate tensors can be large. The transport settings prevent the edge from
publishing faster than the cloud can consume.

```yaml
transport:
  async_publish: True
  local_queue_size: 2
  rabbit_max_queue_messages: 8
  rabbit_max_queue_bytes: 0
  rabbit_overflow: reject-publish
  backpressure_high_watermark: 6
  backpressure_low_watermark: 3
  backpressure_poll_sec: 0.02
  publish_confirm: True
  consumer_prefetch: 1
  consumer_poll_sec: 0.02
```

Recommended starting values:

| Setting | Effect |
| --- | --- |
| `async_publish` | Uses a publisher thread so edge inference is not blocked by RabbitMQ serialization and network I/O. |
| `local_queue_size` | Bounded in-process queue on the edge. Keep this small on Jetson devices. |
| `rabbit_max_queue_messages` | Hard limit for RabbitMQ intermediate queue depth. |
| `backpressure_high_watermark` | Edge begins waiting when queue depth reaches this value. |
| `backpressure_low_watermark` | Edge resumes after the cloud drains the queue to this value. |
| `publish_confirm` | Waits for broker confirmation. Safer, but slightly slower. |
| `consumer_prefetch` | Limits unacknowledged messages per cloud consumer. Use `1` for stable latency. |

If end-to-end latency grows every batch, the cloud is slower than the edge.
Lower `batch-size`, choose an earlier/later split point, add cloud clients, or
reduce `backpressure_high_watermark`.

## Running

Start the controller first:

```bash
python server.py
```

Start an edge client:

```bash
python client.py --layer_id 1
```

Start a cloud client:

```bash
python client.py --layer_id 2
```

Force CPU execution:

```bash
python client.py --layer_id 1 --device cpu
python client.py --layer_id 2 --device cpu
```

Give a client a stable name for profiling and clustering output:

```bash
python client.py --layer_id 1 --name jetson-edge-1
python client.py --layer_id 2 --name cloud-gpu-1
```

For multi-device experiments, make `server.clients` match the number of clients
you will start:

```yaml
server:
  clients:
    - 3   # edge clients
    - 2   # cloud clients
```

Then start three `layer_id 1` clients and two `layer_id 2` clients.

## Output Files

The system writes per-run metrics and detection output in the project folder.

| File | Description |
| --- | --- |
| `metrics_raw_<queue>_<client>.csv` | Temporary per-client metrics. |
| `metrics_pivoted_<queue>.csv` | Joined edge/cloud metrics, one row per processed batch. |
| `detections_stream.jsonl` | Streaming detection results by frame. |
| `detections.json` | Final detection results by frame. |
| `timing_edge_<client>.log` | Edge timing trace. |
| `timing_cloud_<client>.log` | Cloud timing trace. |
| `app.log` | Application log. |

## Metrics

Important columns in `metrics_pivoted_<queue>.csv`:

| Column | Meaning |
| --- | --- |
| `batch_id` | Batch index in arrival/order after pivoting. |
| `batch_size` | Number of frames in one model forward pass. |
| `best_cut` | Split layer chosen by fixed config or Hungarian matching. |
| `edge_latency_ms` | Edge processing time, including head inference, compression, and publish completion. |
| `cloud_latency_ms` | Cloud processing time, including receive, decode, tail inference, and postprocess. |
| `e2e_latency_ms` | End-to-end latency from edge batch start to cloud batch finish. |
| `edge_message_size_bytes` | Serialized payload size published by the edge. |
| `cloud_message_size_bytes` | Raw payload size received by the cloud. |
| `edge_fps`, `cloud_fps` | Per-device batch throughput. |
| `edge_ram_mb`, `cloud_ram_mb` | Resident memory usage for each process. |

End-to-end latency is the best signal for queue pressure:

```text
e2e_latency = edge_processing + queue_wait + cloud_processing
```

If `e2e_latency_ms` increases over time while `cloud_latency_ms` is stable,
messages are waiting in RabbitMQ and the selected split or batch size is not
balanced for the available devices.

## Project Layout

```text
split_inference_test/
|-- client.py
|-- server.py
|-- config.yaml
|-- requirements.txt
|-- cfg/
|-- imgs/
|-- src/
|   |-- Clustering.py
|   |-- Compress.py
|   |-- Log.py
|   |-- Model.py
|   |-- Profiler.py
|   |-- RpcClient.py
|   |-- Scheduler.py
|   |-- Server.py
|   |-- Transport.py
|   `-- Utils.py
`-- tools/
```

## Troubleshooting

### RabbitMQ queue grows continuously

The cloud cannot keep up. Try:

- Reduce `server.batch-size`.
- Lower `transport.backpressure_high_watermark`.
- Add more cloud clients.
- Use a split point that reduces cloud compute or network payload.
- Enable compression with `compress.enable: True`.

### Clients wait forever after registration

Check that `server.clients` matches the number of clients you started. The
controller starts inference only after all expected edge and cloud clients have
registered.

### Model profiling fails

If `clustering.profile_source: real`, each client must have the checkpoint file
and enough memory to profile the model. Use `profile_source: auto` or
`profile_source: simulated` for quick experiments.

### RabbitMQ declaration mismatch

If queue arguments were changed between runs, delete old queues or restart
RabbitMQ. The server also tries to clean old `reply_*`, `rpc_queue`, and
`intermediate_queue*` queues on startup.

## License

See [LICENSE](./LICENSE).
