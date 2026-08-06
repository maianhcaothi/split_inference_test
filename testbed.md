# Test bed — cấu hình chạy thực tế

Ghi lại **topology vật lý** đang dùng để chạy `server.py` + `client.py`. Mọi con số
trong `config.yaml` chỉ đúng khi khớp với bảng này — phần [Hệ quả lên config](#hệ-quả-lên-config)
nói rõ chỗ nào phụ thuộc.

## Topology

| Vai trò | Host | IP | vCPU | Tiến trình |
|---|---|---|---|---|
| **Server** (controller) | DAI | Tailscale `dai` / 100.68.127.89 | — | `python3 server.py` |
| **Broker** (RabbitMQ) | machine-1 | 192.168.101.91 | — | `rabbitmq-server` (port 5672) |
| Edge | machine-2 | 192.168.101.92 | **2** | `client.py --layer_id 1 --name machine-2` |
| Edge | machine-3 | 192.168.101.93 | **2** | `client.py --layer_id 1 --name machine-3` |
| Edge | machine-4 | 192.168.101.94 | **2** | `client.py --layer_id 1 --name machine-4` |
| Edge | machine-5 | 192.168.101.95 | **1** | `client.py --layer_id 1 --name machine-5` |
| Edge | machine-6 | 192.168.101.96 | **1** | `client.py --layer_id 1 --name machine-6` |
| Edge | machine-7 | 192.168.101.97 | **1** | `client.py --layer_id 1 --name machine-7` |
| Edge | machine-8 | 192.168.101.98 | **4** | `client.py --layer_id 1 --name machine-8` |
| Edge | machine-9 | 192.168.101.99 | **4** | `client.py --layer_id 1 --name machine-9` |
| Edge | machine-10 | 192.168.101.100 | **4** | `client.py --layer_id 1 --name machine-10` |
| Cloud | device-1 | 192.168.101.121 | **4** | `client.py --layer_id 2 --name device-1` |
| Cloud | device-2 | 192.168.101.122 | **4** | `client.py --layer_id 2 --name device-2` |
| Cloud | device-3 | 192.168.101.123 | **4** | `client.py --layer_id 2 --name device-3` |

Tóm tắt: **9 edge = 9 máy ảo riêng biệt, MỖI MÁY 1 tiến trình client**, chia 3 lớp phần cứng
2 / 1 / 4 vCPU (mỗi lớp 3 máy). **3 cloud = 3 máy ảo 4 vCPU.** Server và broker nằm trên
**hai máy khác nhau**, cả hai đều tách khỏi edge/cloud.

Tương ứng trong `config.yaml`: `server.clients: [9, 3]` (9 edge ở `layer_id=1`,
3 cloud ở `layer_id=2`), `rabbit.address: 192.168.101.91`.

Đường vào: workstation → Tailscale → DAI → SSH LAN. Edge/cloud không truy cập trực tiếp
từ workstation. Đường project trên mọi máy: `~/ntuanh/Optimizer/split_inference_test`
(trên DAI là symlink tới `/mnt/d/SplitInference/...`). Chạy script **từ thư mục gốc project**
(`src` là namespace package, không có `__init__.py`).

## Hệ quả lên config

Ba tham số dưới đây được viết cho một topology KHÁC (nhiều tiến trình edge dồn trên một
máy nhiều core). Trên test bed này chúng cần đọc lại:

### `performance.torch_threads`

`auto` = `max(1, os.cpu_count() // server.clients[0])` = `max(1, n_core // 9)`
([client.py:38-52](client.py#L38-L52)). Trên máy 4 core → `4 // 9 = 0` → **1 thread**.
Máy 2 core → 1 thread. Máy 1 core → 1 thread.

→ Với topology 1 tiến trình / 1 VM, `auto` **ép mọi edge về 1 thread và xoá sạch khác biệt
2 / 1 / 4 core**. Dùng `torch_threads: 0` (để torch tự lấy n_core) hoặc `--threads <n_core>`
cho từng máy. `auto` chỉ đúng khi cả 9 tiến trình edge nằm trên một máy.

Cloud: `n_core // clients[1]` = `4 // 3` = 1 thread — cũng cần xem lại vì mỗi cloud là VM riêng.

### `clustering.network_rate_mb_s`

Solver cộng `1/tau` của từng edge, nên nó cần **băng thông riêng của mỗi edge**, không
phải tổng đường truyền. 9 edge ở đây có 9 NIC riêng nên không chia cho 9 ở phía phát —
nhưng **chúng vẫn tranh nhau ingress của broker (machine-1)**, nên phần băng thông thực
mỗi edge vẫn ≈ (băng thông broker) / 9. Đây mới là con số cần điền.

### `clustering.measure_bandwidth`

Mỗi client tự đo egress lúc đăng ký. Chỉ đúng khi 9 edge đo ĐỒNG THỜI (contention đã nằm
trong số đo). Máy nào cache profile miss sẽ lệch pha → đo được đường truyền rỗng → báo cao.
Server in spread và cảnh báo khi > 3x. **Muốn cut/cluster tái lập được giữa các run thì đặt
`False`** và pin `network_rate_mb_s`.

## Profile cache

Mỗi máy tự profile model rồi cache ra `profile_{model}_{device}_bs{bs}_fp32.npy` **tại
thư mục chạy** ([Profiler.py:19](src/Profiler.py#L19)). File này:

- được tái dùng vĩnh viễn, không bao giờ tự hết hạn;
- **không** mã hoá số thread vào tên → đổi `torch_threads` mà không xoá cache thì clustering
  vẫn thấy profile cũ, trong khi runtime chạy ở số thread mới;
- là lý do nhiều run liên tiếp cho ra **cùng một** throughput/cut: input của solver đóng băng.

Sau mỗi lần đổi `torch_threads`, `batch-size`, hoặc model: xoá cache trên **cả 12 máy**
trước khi chạy lại.

## Đọc kết quả clustering

Hàng của ma trận feature được sắp theo `--name` (natural sort: machine-2 < machine-10),
không theo thứ tự REGISTER — `Server._ordered_clients`. Nhờ vậy **index i là cùng một máy
ở mọi run**. Trước đây nó theo thứ tự REGISTER, mà 9 client khởi động song song nên chỉ số
(và kéo theo id cụm, vì `agglomerative_cluster` đánh số cụm theo index nhỏ nhất) nhảy mỗi
run dù phân hoạch máy thật không hề đổi.

**Luôn truyền `--name`.** Không có nó thì thứ tự rơi về uuid — vẫn tất định nhưng vô nghĩa
khi đọc, và cột `types` in ra `edge_<uuid8>`.

Phần chi tiết giờ in kèm tên:

```
Edge cluster 0 <-> Cloud cluster 0
  Clients            : [0, 1, 2]  ['machine-2', 'machine-3', 'machine-4']
  Servers            : [0]  ['device-1']
```

Mỗi run cũng ghi `clustering_input.json` (cạnh các file log, `log-path`) chứa **toàn bộ
input** của solver: layer time từng máy, bandwidth đo được, rate thực dùng, bảng cut size.
Replay offline (không cần broker, không cần model):

```bash
python tools/replay_clustering.py clustering_input.json
python tools/replay_clustering.py clustering_input.json --rate 12
```

Khi kết quả đổi giữa hai run, so hai file này là cách duy nhất biết **input nào** đã đổi —
profile chậm đi, bandwidth đo lệch, hay không có gì đổi cả.
