"""
Clustering2.py — ban thuat toan moi (LP routing + local-cut) cua leader,
dong goi lai theo dung INTERFACE cua Clustering.py de chay duoc tren he thong
thuc (Server.py) ma KHONG can sua Server.py — chi can doi:

    from src.Clustering import (...)
    ->
    from src.Clustering2 import (...)

So khac biet chinh so voi Clustering.py (DeterministicSimilarityAssignmentSolver cu):
  1. Thong luong tung cap cum duoc tinh bang LP routing (scipy.linprog) co rang
     buoc bang thong tung lien ket ro rang, thay vi cong thuc dong
     min(producer_rate, service_rate) gia dinh san.
  2. Co utilization_target (mac dinh 0.90) - khong chay thiet bi/lien ket ở 100%
     ly thuyet, giu margin an toan ky thuat.
  3. CUT_DATA_SIZES_MB_BY_MODEL CHI giu "yolo26n_bs32" (theo yeu cau) - goi voi
     model/batch khac se bao loi ro rang, KHONG tu dong scale sai nhu bug cu.

Luu y an toan tich hop (QUAN TRONG):
  Thuat toan goc cua leader co them lua chon "local-only" (cum bien tu xu ly
  toan bo, khong gui cloud) thong qua cot dummy trong Hungarian. Server.py
  hien tai (_run_hungarian/notify_clients) CHUA duoc thiet ke de xu ly truong
  hop matching[k] == -1 (mot cum khong co cloud doi tac) - co the gay sai lech
  queue_name giua edge/cloud. De an toan khi chay tren may thuc, lop duoi day
  TAT lua chon local-only theo mac dinh (allow_local_only=False), Hungarian
  luon ghep K cum bien voi dung K cum may chu (giong hanh vi Clustering.py cu).
  Neu sau nay muon thu nghiem local-only, can sua them Server.py truoc.
"""
import numpy as np
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from scipy.optimize import linear_sum_assignment, linprog


# ---------------------------------------------------------------------------
# Du lieu mo phong cho nhanh fallback (profile_source != real) - giu nguyen
# nhu Clustering.py de tuong thich, khong dung trong duong real-profile.
# ---------------------------------------------------------------------------
DEVICE_A_4CORE = np.array([
    0.001433, 0.00342, 0.004968, 0.006701, 0.004795, 0.006678,
    0.003975, 0.003339, 0.003928, 0.001866, 0.00285, 0.000012,
    0.0, 0.005454, 0.000023, 0.0, 0.006286, 0.001675, 0.0,
    0.004344, 0.00167, 0.0, 0.005262, 0.017629
], dtype=float)

DEVICE_B_2CORE = np.array([
    0.002286, 0.005456, 0.007927, 0.010692, 0.00765, 0.010655,
    0.006341, 0.005327, 0.006268, 0.002977, 0.004546, 0.000018,
    0.0, 0.008701, 0.000037, 0.0, 0.010028, 0.002673, 0.0,
    0.006931, 0.002664, 0.0, 0.008394, 0.028126
], dtype=float)

DEVICE_C_1CORE = np.array([
    0.00327, 0.007807, 0.011341, 0.015297, 0.010946, 0.015245,
    0.009073, 0.007622, 0.008967, 0.00426, 0.006505, 0.000026,
    0.0, 0.012449, 0.000053, 0.0, 0.014348, 0.003824, 0.0,
    0.009917, 0.003811, 0.0, 0.01201, 0.040242
], dtype=float)

DEVICE_CLOUD = np.array([
    0.000534, 0.001272, 0.001851, 0.002496, 0.001785, 0.002487,
    0.001479, 0.001242, 0.001461, 0.000696, 0.001062, 0.000003,
    0.0, 0.002031, 0.000009, 0.0, 0.00234, 0.000624, 0.0,
    0.001617, 0.000621, 0.0, 0.001959, 0.006564
], dtype=float)

# Chi giu yolo26n_bs32 theo yeu cau - goi sai model/batch se loi ro, khong
# tu scale am tham nhu bug cu trong Clustering.py.
CUT_DATA_SIZES_MB_BY_MODEL = {
    "yolo26n_bs32": np.array([
        43.97, 22.76, 30.89, 12.04, 25.18, 31.6, 31.33, 34.18, 34.27,
        34.34, 33.93, 44.32, 49.65, 40.29, 65.74, 88.87, 52.59, 55.39,
        61.56, 58.23, 59.52, 62.02, 59.83
    ], dtype=float),
}

# Hang so RAW_INPUT_MB=13.0 trong Clustering.py ghi nham comment "batch_size=4",
# nhung do thuc te (Encoder 8-bit tren khung hinh co tuong quan giong video thuc,
# khung sau = khung truoc + nhieu nho - KHONG dung torch.randn() doc lap tung
# khung vi se pha vo co che delta) cho thay 13.0 MB khop voi batch=32, khong
# phai batch=4 (do o batch=4, gia tri thuc te chi ~2.35 MB). Vi vay cong thuc
# scale tuyen tinh "RAW_INPUT_MB * batch/4" trong Clustering.py la SAI - no
# nhan nham mot gia tri da-la-batch32 len 8 lan nua (-> 104.0, gap doi sai so).
# Giu nguyen 13.0, dung truc tiep cho batch=32, khong scale.
RAW_INPUT_MB_BS32 = 13.0


def get_raw_input_mb(batch_size: int = 4) -> float:
    if batch_size != 32:
        raise ValueError(
            f"[Clustering2] Chi ho tro batch_size=32 cho raw input (do thuc te). "
            f"Nhan duoc batch_size={batch_size}. Do lai bang Encoder 8-bit neu can batch khac."
        )
    return RAW_INPUT_MB_BS32


def get_cut_data_sizes(model_name: str, batch_size: int = 4) -> np.ndarray:
    """Clustering2 chi ho tro yolo26n_bs32. Goi model/batch khac se raise ngay,
    khong tu dong scale sai (day la dung bug cu da phat hien trong Clustering.py)."""
    key = f"{model_name}_bs{batch_size}"
    if key in CUT_DATA_SIZES_MB_BY_MODEL:
        return CUT_DATA_SIZES_MB_BY_MODEL[key].astype(float).copy()
    raise ValueError(
        f"[Clustering2] Chi ho tro 'yolo26n_bs32'. Khong tim thay cut-size cho "
        f"model_name={model_name!r}, batch_size={batch_size}. "
        f"Them entry vao CUT_DATA_SIZES_MB_BY_MODEL neu can model/batch khac."
    )


@dataclass
class ManualExperimentConfig:
    num_A: int = 3
    num_B: int = 3
    num_C: int = 3
    num_cloud: int = 3

    network_rate_mb_s: float = 100.0
    network_overhead_s: float = 0.0
    network_rates_matrix: Optional[np.ndarray] = None
    network_overheads_matrix: Optional[np.ndarray] = None

    client_uplink_caps_mb_s: Optional[np.ndarray] = None
    server_ingress_caps_mb_s: Optional[np.ndarray] = None

    input_data_mb: Optional[float] = None
    edge_time_jitter: float = 0.0
    cloud_time_jitter: float = 0.0

    max_clusters: Optional[int] = None
    cluster_penalty: float = 0.0
    exact_max_k: int = 7  # giu de tuong thich tham so Server.py truyen vao, khong dung (khong con method "exact")
    model_name: str = "yolo26n"
    batch_size: int = 32

    utilization_target: float = 0.90
    cluster_feature_mode: str = "raw_profile"  # giu giong hanh vi cu cua Clustering.py


@dataclass
class PairDetail:
    edge_cluster_id: int
    cloud_cluster_id: Optional[int]
    clients: List[int]
    servers: List[int]
    best_cut: int
    producer_rate: float
    network_rate: float
    service_rate: float
    throughput: float
    round_time: float
    routing_matrix: Optional[np.ndarray] = None
    is_local_only: bool = False


@dataclass
class OptimizationResult:
    method: str
    total_throughput: float
    score: float
    system_round_time_proxy: float
    fairness_std: float
    num_clusters: int
    edge_labels: np.ndarray
    cloud_labels: np.ndarray
    matching: np.ndarray          # luon trong [0, K-1] khi allow_local_only=False
    selected_columns: np.ndarray
    best_cuts: np.ndarray
    details: List[PairDetail]


def _jitter_profile(base: np.ndarray, jitter: float, rng: np.random.Generator) -> np.ndarray:
    if jitter <= 0:
        return base.astype(float).copy()
    noise = rng.uniform(1.0 - jitter, 1.0 + jitter, size=base.shape)
    out = base.astype(float) * noise
    out[base == 0.0] = 0.0
    return out


def _as_optional_vector(x, expected_len: int, name: str):
    if x is None:
        return None
    arr = np.asarray(x, dtype=float)
    if arr.shape != (expected_len,):
        raise ValueError(f"{name} must have shape ({expected_len},), got {arr.shape}")
    if np.any(arr <= 0):
        raise ValueError(f"{name} must contain positive values")
    return arr


class DeterministicSimilarityAssignmentSolver:
    """Cung ten class voi Clustering.py de Server.py dung duoc khong sua gi.
    Ben trong dung LP routing (leader) thay cong thuc dong cu."""

    def __init__(
        self,
        client_layer_times: np.ndarray,
        server_layer_times: np.ndarray,
        cut_data_sizes: np.ndarray,
        input_data_size: float,
        network_rates: np.ndarray,
        network_overheads: Optional[np.ndarray] = None,
        client_arrival_fps: Optional[np.ndarray] = None,
        client_uplink_caps_mb_s: Optional[np.ndarray] = None,
        server_ingress_caps_mb_s: Optional[np.ndarray] = None,
        utilization_target: float = 0.90,
        cluster_feature_mode: str = "raw_profile",
        allow_local_only: bool = False,
        encode_rate_ms_per_mb: Optional[np.ndarray] = None,
        decode_rate_ms_per_mb: Optional[np.ndarray] = None,
        eps: float = 1e-9,
    ):
        self.client_layer_times = np.asarray(client_layer_times, dtype=float)
        self.server_layer_times = np.asarray(server_layer_times, dtype=float)
        self.cut_data_sizes = np.asarray(cut_data_sizes, dtype=float)
        self.input_data_size = float(input_data_size)
        self.network_rates = np.asarray(network_rates, dtype=float)
        self.eps = float(eps)
        self.utilization_target = float(utilization_target)
        self.cluster_feature_mode = str(cluster_feature_mode)
        self.allow_local_only = bool(allow_local_only)

        if not (0.0 < self.utilization_target <= 1.0):
            raise ValueError("utilization_target must be in (0, 1].")

        self.N, self.L = self.client_layer_times.shape
        self.M, L2 = self.server_layer_times.shape
        if L2 != self.L:
            raise ValueError("client_layer_times and server_layer_times must have same number of layers")
        if self.cut_data_sizes.shape != (self.L - 1,):
            raise ValueError(f"cut_data_sizes must have shape ({self.L - 1},), got {self.cut_data_sizes.shape}")
        if self.network_rates.shape != (self.N, self.M):
            raise ValueError(f"network_rates must have shape ({self.N}, {self.M})")
        if np.any(self.network_rates <= 0):
            raise ValueError("All network_rates must be > 0")

        if network_overheads is None:
            self.network_overheads = np.zeros((self.N, self.M), dtype=float)
        else:
            self.network_overheads = np.asarray(network_overheads, dtype=float)
            if self.network_overheads.shape != (self.N, self.M):
                raise ValueError(f"network_overheads must have shape ({self.N}, {self.M})")

        self.client_arrival_fps = _as_optional_vector(client_arrival_fps, self.N, "client_arrival_fps")
        self.client_uplink_caps_mb_s = _as_optional_vector(client_uplink_caps_mb_s, self.N, "client_uplink_caps_mb_s")
        self.server_ingress_caps_mb_s = _as_optional_vector(server_ingress_caps_mb_s, self.M, "server_ingress_caps_mb_s")

        # Toc do nen (edge, ms/MB) / giai nen (cloud, ms/MB) Q-DeltaMask do thuc
        # tren CHINH thiet bi do (Profiler.measure_compression_rate). Mac dinh 0.0
        # (khong cong gi them) de tuong thich nguoc khi chua co so do.
        if encode_rate_ms_per_mb is None:
            self.encode_rate_ms_per_mb = np.zeros(self.N, dtype=float)
        else:
            self.encode_rate_ms_per_mb = np.asarray(encode_rate_ms_per_mb, dtype=float)
            if self.encode_rate_ms_per_mb.shape != (self.N,):
                raise ValueError(f"encode_rate_ms_per_mb must have shape ({self.N},), got {self.encode_rate_ms_per_mb.shape}")
        if decode_rate_ms_per_mb is None:
            self.decode_rate_ms_per_mb = np.zeros(self.M, dtype=float)
        else:
            self.decode_rate_ms_per_mb = np.asarray(decode_rate_ms_per_mb, dtype=float)
            if self.decode_rate_ms_per_mb.shape != (self.M,):
                raise ValueError(f"decode_rate_ms_per_mb must have shape ({self.M},), got {self.decode_rate_ms_per_mb.shape}")

        self.client_prefix = np.cumsum(self.client_layer_times, axis=1)
        self.server_suffix = np.flip(np.cumsum(np.flip(self.server_layer_times, axis=1), axis=1), axis=1)
        self.client_total = self.client_prefix[:, -1]
        self.server_total = np.sum(self.server_layer_times, axis=1)

        self.valid_cuts = list(range(-1, self.L))            # tuong thich Clustering.py: -1..L-1
        self.valid_cloud_cuts = list(range(-1, self.L - 1))  # khong tinh cut full-local
        self.local_cut = self.L - 1

        self.cluster_cache: Dict[int, Dict[str, object]] = {}
        self.pair_cache: Dict[int, Dict[Tuple[int, int], PairDetail]] = {}
        self.local_cache: Dict[int, Dict[int, PairDetail]] = {}

        self.client_type_names = ["?"] * self.N
        self.cloud_type_names = ["cloud"] * self.M

    # ------------------------------------------------------------------
    # Timing model (giong Clustering.py)
    # ------------------------------------------------------------------
    def edge_time(self, client_id: int, cut: int) -> float:
        # cut<0: gui anh tho, khong qua Q-DeltaMask -> khong cong encode.
        # cut>=L-1: full-local, khong gui gi -> khong cong encode.
        # 0<=cut<L-1: gui tensor trung gian, PHAI nen Q-DeltaMask -> cong encode.
        if cut < 0:
            return 0.0
        if cut >= self.L - 1:
            return float(self.client_total[client_id])
        encode_s = (self.encode_rate_ms_per_mb[client_id] * self.cut_data_sizes[cut]) / 1000.0
        return float(self.client_prefix[client_id, cut]) + encode_s

    def cloud_time(self, server_id: int, cut: int) -> float:
        # cut<0: nhan anh tho, khong qua Q-DeltaMask -> khong cong decode.
        # cut>=L-1: khong nhan gi -> khong cong decode.
        # 0<=cut<L-1: nhan tensor da nen, PHAI giai nen truoc khi tinh tiep -> cong decode.
        if cut < 0:
            return float(self.server_total[server_id])
        if cut >= self.L - 1:
            return 0.0
        decode_s = (self.decode_rate_ms_per_mb[server_id] * self.cut_data_sizes[cut]) / 1000.0
        return float(self.server_suffix[server_id, cut + 1]) + decode_s

    def payload_mb(self, cut: int) -> float:
        if cut < 0:
            return self.input_data_size
        if cut >= self.L - 1:
            return 0.0
        return float(self.cut_data_sizes[cut])

    def net_time(self, client_id: int, server_id: int, cut: int) -> float:
        data_mb = self.payload_mb(cut)
        if data_mb <= 0:
            return 0.0
        return float(data_mb / self.network_rates[client_id, server_id] + self.network_overheads[client_id, server_id])

    def _arrival_cap(self, client_id: int) -> float:
        if self.client_arrival_fps is None:
            return float("inf")
        return float(self.client_arrival_fps[client_id])

    def _client_prefix_cap(self, client_id: int, cut: int) -> float:
        arrival = self._arrival_cap(client_id)
        if cut < 0:
            compute_cap = float("inf")
        else:
            t = self.edge_time(client_id, cut)
            compute_cap = float("inf") if t <= self.eps else self.utilization_target / t
        return min(arrival, compute_cap)

    def _server_suffix_cap(self, server_id: int, cut: int) -> float:
        t = self.cloud_time(server_id, cut)
        if t <= self.eps:
            return float("inf")
        return self.utilization_target / t

    def _link_cap(self, client_id: int, server_id: int, cut: int) -> float:
        t = self.net_time(client_id, server_id, cut)
        if t <= self.eps:
            return float("inf")
        return self.utilization_target / t

    @staticmethod
    def _finite_sum(vals: List[float]) -> float:
        if any(np.isinf(v) for v in vals):
            return float("inf")
        return float(np.sum(vals))

    # ------------------------------------------------------------------
    # Clustering
    # ------------------------------------------------------------------
    def _normalize_features(self, x: np.ndarray) -> np.ndarray:
        x = np.asarray(x, dtype=float)
        mins = x.min(axis=0, keepdims=True)
        maxs = x.max(axis=0, keepdims=True)
        denom = np.maximum(maxs - mins, self.eps)
        return (x - mins) / denom

    def build_edge_features(self) -> np.ndarray:
        mean_rate = np.mean(self.network_rates, axis=1, keepdims=True)
        mean_ovh = np.mean(self.network_overheads, axis=1, keepdims=True)

        if self.cluster_feature_mode == "raw_profile":
            raw = np.concatenate([self.client_layer_times, mean_rate, mean_ovh], axis=1)
            return self._normalize_features(raw)

        if self.cluster_feature_mode != "cut_profile":
            raise ValueError("cluster_feature_mode must be 'cut_profile' or 'raw_profile'")

        rows = []
        for i in range(self.N):
            cut_latencies = []
            for cut in self.valid_cloud_cuts:
                edge_t = self.edge_time(i, cut)
                best_cloud_path = min(self.net_time(i, j, cut) + self.cloud_time(j, cut) for j in range(self.M))
                cut_latencies.append(edge_t + best_cloud_path)
            cut_latencies.append(self.edge_time(i, self.local_cut))
            rows.append(cut_latencies)
        raw = np.concatenate([np.asarray(rows, dtype=float), mean_rate, mean_ovh], axis=1)
        return self._normalize_features(raw)

    def build_cloud_features(self) -> np.ndarray:
        mean_rate = np.mean(self.network_rates, axis=0, keepdims=True).T
        mean_ovh = np.mean(self.network_overheads, axis=0, keepdims=True).T
        raw = np.concatenate([self.server_layer_times, mean_rate, mean_ovh], axis=1)
        return self._normalize_features(raw)

    def agglomerative_cluster(self, features: np.ndarray, K: int) -> np.ndarray:
        n = features.shape[0]
        if K <= 0 or K > n:
            raise ValueError(f"Invalid K={K} for n={n}")
        if K == n:
            return np.arange(n, dtype=int)
        if K == 1:
            return np.zeros(n, dtype=int)

        diff = features[:, None, :] - features[None, :, :]
        dist = np.sqrt(np.sum(diff * diff, axis=2))
        clusters = [[i] for i in range(n)]

        def avg_linkage(c1, c2):
            return float(np.mean([dist[a, b] for a in c1 for b in c2]))

        while len(clusters) > K:
            best_pair = None
            best_val = float("inf")
            for a in range(len(clusters)):
                for b in range(a + 1, len(clusters)):
                    val = avg_linkage(clusters[a], clusters[b])
                    if val < best_val:
                        best_val = val
                        best_pair = (a, b)
            a, b = best_pair
            merged = sorted(clusters[a] + clusters[b])
            clusters = [c for idx, c in enumerate(clusters) if idx not in (a, b)]
            clusters.append(merged)

        clusters = sorted(clusters, key=lambda c: min(c))
        labels = np.zeros(n, dtype=int)
        for k, members in enumerate(clusters):
            for idx in members:
                labels[idx] = k
        return labels

    @staticmethod
    def _invert_assignment(labels: np.ndarray, K: int) -> Dict[int, List[int]]:
        groups = {k: [] for k in range(K)}
        for idx, g in enumerate(labels):
            groups[int(g)].append(idx)
        return groups

    def build_similarity_clusters(self, K: int) -> Dict[str, object]:
        if K in self.cluster_cache:
            return self.cluster_cache[K]
        edge_labels = self.agglomerative_cluster(self.build_edge_features(), K)
        cloud_labels = self.agglomerative_cluster(self.build_cloud_features(), K)
        out = {
            "edge_labels": edge_labels,
            "cloud_labels": cloud_labels,
            "edge_groups": self._invert_assignment(edge_labels, K),
            "cloud_groups": self._invert_assignment(cloud_labels, K),
        }
        self.cluster_cache[K] = out
        return out

    # ------------------------------------------------------------------
    # LP routing (cai tien chinh cua leader)
    # ------------------------------------------------------------------
    def _solve_routing_lp(self, clients, servers, cut, include_server_constraints=True):
        C = len(clients)
        S = len(servers)
        if C == 0 or S == 0:
            return 0.0, np.zeros((C, S), dtype=float)
        nvar = C * S
        obj = -np.ones(nvar, dtype=float)
        A_ub, b_ub = [], []

        def var_idx(a, b):
            return a * S + b

        for a, i in enumerate(clients):
            cap = self._client_prefix_cap(i, cut)
            if np.isfinite(cap):
                row = np.zeros(nvar, dtype=float)
                for b in range(S):
                    row[var_idx(a, b)] = 1.0
                A_ub.append(row)
                b_ub.append(cap)

        if include_server_constraints:
            for b, j in enumerate(servers):
                cap = self._server_suffix_cap(j, cut)
                if np.isfinite(cap):
                    row = np.zeros(nvar, dtype=float)
                    for a in range(C):
                        row[var_idx(a, b)] = 1.0
                    A_ub.append(row)
                    b_ub.append(cap)

        data_mb = self.payload_mb(cut)
        if data_mb > self.eps:
            if self.client_uplink_caps_mb_s is not None:
                for a, i in enumerate(clients):
                    row = np.zeros(nvar, dtype=float)
                    for b in range(S):
                        row[var_idx(a, b)] = data_mb
                    A_ub.append(row)
                    b_ub.append(float(self.client_uplink_caps_mb_s[i]) * self.utilization_target)
            if include_server_constraints and self.server_ingress_caps_mb_s is not None:
                for b, j in enumerate(servers):
                    row = np.zeros(nvar, dtype=float)
                    for a in range(C):
                        row[var_idx(a, b)] = data_mb
                    A_ub.append(row)
                    b_ub.append(float(self.server_ingress_caps_mb_s[j]) * self.utilization_target)

        bounds = []
        for a, i in enumerate(clients):
            for b, j in enumerate(servers):
                link_cap = self._link_cap(i, j, cut)
                ub = None if np.isinf(link_cap) else link_cap
                bounds.append((0.0, ub))

        res = linprog(
            c=obj,
            A_ub=np.asarray(A_ub, dtype=float) if A_ub else None,
            b_ub=np.asarray(b_ub, dtype=float) if b_ub else None,
            bounds=bounds,
            method="highs",
        )

        if not res.success:
            return self._greedy_routing_fallback(clients, servers, cut)

        routing = np.maximum(res.x.reshape(C, S), 0.0)
        return float(np.sum(routing)), routing

    def _greedy_routing_fallback(self, clients, servers, cut):
        C, S = len(clients), len(servers)
        routing = np.zeros((C, S), dtype=float)
        client_remaining = np.array([self._client_prefix_cap(i, cut) for i in clients], dtype=float)
        server_remaining = np.array([self._server_suffix_cap(j, cut) for j in servers], dtype=float)
        client_remaining[~np.isfinite(client_remaining)] = 1e12
        server_remaining[~np.isfinite(server_remaining)] = 1e12

        candidates = []
        for a, i in enumerate(clients):
            for b, j in enumerate(servers):
                candidates.append((self.net_time(i, j, cut), a, b, self._link_cap(i, j, cut)))
        candidates.sort(key=lambda x: x[0])

        for _, a, b, link_cap in candidates:
            cap = min(client_remaining[a], server_remaining[b], link_cap if np.isfinite(link_cap) else 1e12)
            if cap <= self.eps:
                continue
            routing[a, b] += cap
            client_remaining[a] -= cap
            server_remaining[b] -= cap
        return float(np.sum(routing)), routing

    def pair_metrics_for_cut(self, edge_cluster_id, cloud_cluster_id, clients, servers, cut):
        """Giu ten ham nhu Clustering.py (cut full -1..L-1); cut=L-1 (local) duoc
        xu ly rieng o local_only_metrics, o day chi xet cut cloud hop le."""
        throughput, routing = self._solve_routing_lp(clients, servers, cut, include_server_constraints=True)
        producer_rate = self._finite_sum([self._client_prefix_cap(i, cut) for i in clients])
        service_rate = self._finite_sum([self._server_suffix_cap(j, cut) for j in servers])
        network_rate, _ = self._solve_routing_lp(clients, servers, cut, include_server_constraints=False)
        round_time = float(len(clients) / max(throughput, self.eps))
        return PairDetail(edge_cluster_id, cloud_cluster_id, list(clients), list(servers), cut,
                           producer_rate, network_rate, service_rate, throughput, round_time, routing, False)

    def local_only_metrics(self, edge_cluster_id, clients):
        throughput = self._finite_sum([self._client_prefix_cap(i, self.local_cut) for i in clients])
        if np.isinf(throughput):
            throughput = 1e12
        round_time = float(len(clients) / max(throughput, self.eps))
        return PairDetail(edge_cluster_id, None, list(clients), [], self.local_cut,
                           throughput, float("inf"), float("inf"), throughput, round_time, None, True)

    def best_cut_for_pair(self, edge_cluster_id, cloud_cluster_id, clients, servers):
        best_detail, best_throughput, best_round_time = None, -float("inf"), float("inf")
        for cut in self.valid_cloud_cuts:
            detail = self.pair_metrics_for_cut(edge_cluster_id, cloud_cluster_id, clients, servers, cut)
            if (detail.throughput > best_throughput) or (
                np.isclose(detail.throughput, best_throughput) and detail.round_time < best_round_time
            ):
                best_throughput, best_round_time, best_detail = detail.throughput, detail.round_time, detail
        return best_detail

    def build_pair_cache(self, K: int):
        if K in self.pair_cache and K in self.local_cache:
            return self.pair_cache[K], self.local_cache[K]
        cl = self.build_similarity_clusters(K)
        edge_groups, cloud_groups = cl["edge_groups"], cl["cloud_groups"]
        pair_out = {}
        for e in range(K):
            for c in range(K):
                pair_out[(e, c)] = self.best_cut_for_pair(e, c, edge_groups[e], cloud_groups[c])
        local_out = {e: self.local_only_metrics(e, edge_groups[e]) for e in range(K)}
        self.pair_cache[K] = pair_out
        self.local_cache[K] = local_out
        return pair_out, local_out

    def get_weight_matrix(self, K: int):
        cl = self.build_similarity_clusters(K)
        pair_cache, _ = self.build_pair_cache(K)
        W = np.zeros((K, K), dtype=float)
        for e in range(K):
            for c in range(K):
                W[e, c] = pair_cache[(e, c)].throughput
        return W, cl, pair_cache

    def _result_from_matching(self, K, matching, method):
        cl = self.build_similarity_clusters(K)
        pair_cache, local_cache = self.build_pair_cache(K)
        details, best_cuts = [], []
        for e in range(K):
            c = int(matching[e])
            detail = local_cache[e] if c < 0 else pair_cache[(e, c)]
            details.append(detail)
            best_cuts.append(detail.best_cut)
        cluster_fps = np.array([d.throughput for d in details], dtype=float)
        total_throughput = float(np.sum(cluster_fps))
        system_round_time_proxy = float(max(d.round_time for d in details)) if details else 0.0
        fairness_std = float(np.std(cluster_fps)) if len(cluster_fps) > 1 else 0.0
        return OptimizationResult(
            method=method, total_throughput=total_throughput, score=total_throughput,
            system_round_time_proxy=system_round_time_proxy, fairness_std=fairness_std,
            num_clusters=K, edge_labels=cl["edge_labels"].copy(), cloud_labels=cl["cloud_labels"].copy(),
            matching=np.asarray(matching, dtype=int).copy(), selected_columns=np.asarray(matching, dtype=int).copy(),
            best_cuts=np.array(best_cuts, dtype=int), details=details,
        )

    def solve_hungarian_for_k(self, K: int):
        pair_cache, local_cache = self.build_pair_cache(K)
        if self.allow_local_only:
            W = np.zeros((K, 2 * K), dtype=float)
            for e in range(K):
                for c in range(K):
                    W[e, c] = pair_cache[(e, c)].throughput
                for dummy in range(K):
                    W[e, K + dummy] = local_cache[e].throughput
            row_ind, col_ind = linear_sum_assignment(-W)
            selected = np.full(K, -1, dtype=int)
            selected[row_ind] = col_ind
            matching = np.where(selected >= K, -1, selected)
        else:
            # An toan tich hop voi Server.py: luon ghep K cum bien voi dung K
            # cum may chu, khong cho lua chon local-only.
            W, _, _ = self.get_weight_matrix(K)
            row_ind, col_ind = linear_sum_assignment(-W)
            matching = np.full(K, -1, dtype=int)
            matching[row_ind] = col_ind
        return self._result_from_matching(K, matching, "hungarian")

    def solve_best_over_k(self, method: str = "hungarian", max_clusters: Optional[int] = None,
                          cluster_penalty: float = 0.0, exact_max_k: int = 8):
        """Giu signature giong Clustering.py (method la tham so dau tien, vi
        Server.py goi solver.solve_best_over_k("hungarian", max_clusters=...)).
        Clustering2 chi ho tro Hungarian (khong con identity/greedy/exact)."""
        if method != "hungarian":
            raise ValueError(f"[Clustering2] Chi ho tro method='hungarian', nhan duoc {method!r}.")

        feasible_max = min(self.N, self.M)
        Kmax = feasible_max if max_clusters is None else min(max_clusters, feasible_max)

        best_score, best_result, best_k = -float("inf"), None, None
        all_results = {}
        for K in range(1, Kmax + 1):
            result = self.solve_hungarian_for_k(K)
            score = result.total_throughput - cluster_penalty * K
            all_results[K] = {"throughput": result.total_throughput, "score": score,
                              "system_round_time_proxy": result.system_round_time_proxy}
            if score > best_score:
                best_score, best_result, best_k = score, result, K

        return {"best_k": best_k, "best_result": best_result, "best_score": best_score, "all_results": all_results}


# ---------------------------------------------------------------------------
# Duong fallback mo phong (profile_source != real) - giu de import khong loi.
# ---------------------------------------------------------------------------
def build_manual_scenario(config: ManualExperimentConfig, seed: int = 0):
    rng = np.random.default_rng(seed)
    client_blocks, client_type_names = [], []
    for _ in range(config.num_A):
        client_blocks.append(_jitter_profile(DEVICE_A_4CORE, config.edge_time_jitter, rng))
        client_type_names.append("A")
    for _ in range(config.num_B):
        client_blocks.append(_jitter_profile(DEVICE_B_2CORE, config.edge_time_jitter, rng))
        client_type_names.append("B")
    for _ in range(config.num_C):
        client_blocks.append(_jitter_profile(DEVICE_C_1CORE, config.edge_time_jitter, rng))
        client_type_names.append("C")

    if len(client_blocks) == 0:
        raise ValueError("At least one edge device is required.")
    if config.num_cloud <= 0:
        raise ValueError("At least one cloud server is required.")

    client_layer_times = np.vstack(client_blocks)
    server_layer_times = np.vstack([
        _jitter_profile(DEVICE_CLOUD, config.cloud_time_jitter, rng) for _ in range(config.num_cloud)
    ])
    N, M = client_layer_times.shape[0], server_layer_times.shape[0]

    if config.network_rates_matrix is not None:
        network_rates = np.asarray(config.network_rates_matrix, dtype=float)
    else:
        network_rates = np.full((N, M), config.network_rate_mb_s, dtype=float)

    if config.network_overheads_matrix is not None:
        network_overheads = np.asarray(config.network_overheads_matrix, dtype=float)
    else:
        network_overheads = np.full((N, M), config.network_overhead_s, dtype=float)

    input_mb = get_raw_input_mb(config.batch_size) if config.input_data_mb is None else float(config.input_data_mb)

    solver = DeterministicSimilarityAssignmentSolver(
        client_layer_times=client_layer_times,
        server_layer_times=server_layer_times,
        cut_data_sizes=get_cut_data_sizes(config.model_name, config.batch_size),
        input_data_size=input_mb,
        network_rates=network_rates,
        network_overheads=network_overheads,
        utilization_target=config.utilization_target,
        cluster_feature_mode=config.cluster_feature_mode,
        allow_local_only=False,
    )
    solver.client_type_names = client_type_names
    solver.cloud_type_names = [f"cloud_{i}" for i in range(M)]
    return solver


def run_manual_hungarian_case(config: ManualExperimentConfig, seed: int = 0):
    """Tuong thich voi Server.py: results["solver"], results["hungarian"]."""
    solver = build_manual_scenario(config, seed=seed)
    max_clusters = config.max_clusters if config.max_clusters is not None else min(solver.N, solver.M)
    hungarian_result = solver.solve_best_over_k("hungarian", max_clusters=max_clusters,
                                                cluster_penalty=config.cluster_penalty)["best_result"]
    print_result(hungarian_result, solver, title="HUNGARIAN MATCHING RESULT (simulated, Clustering2)")
    return {"solver": solver, "hungarian": hungarian_result}


def print_result(result: OptimizationResult, solver: DeterministicSimilarityAssignmentSolver, title: str = "RESULT"):
    print("=" * 100)
    print(title)
    print(f"METHOD               : {result.method}")
    print(f"NUM CLUSTERS         : {result.num_clusters}")
    print(f"TOTAL THROUGHPUT     : {result.total_throughput:.6f}")
    print(f"SYSTEM ROUND TIME    : {result.system_round_time_proxy:.6f}")
    print("Edge labels          :", result.edge_labels.tolist())
    print("Cloud labels         :", result.cloud_labels.tolist())
    print("Matching             :", result.matching.tolist())
    print("Best cuts            :", result.best_cuts.tolist())
    print("-" * 100)

    client_types = getattr(solver, "client_type_names", ["?"] * solver.N)
    cloud_types = getattr(solver, "cloud_type_names", ["?"] * solver.M)
    edge_groups = solver._invert_assignment(result.edge_labels, result.num_clusters)
    cloud_groups = solver._invert_assignment(result.cloud_labels, result.num_clusters)

    for e in range(result.num_clusters):
        print(f"Edge cluster {e}: clients={edge_groups[e]} types={[client_types[i] for i in edge_groups[e]]}")
    for c in range(result.num_clusters):
        print(f"Cloud cluster {c}: servers={cloud_groups[c]} types={[cloud_types[i] for i in cloud_groups[c]]}")
    print("-" * 100)

    for d in result.details:
        mode = "LOCAL-ONLY" if d.is_local_only else "SPLIT/CLOUD"
        print(f"Edge cluster {d.edge_cluster_id} <-> Cloud cluster {d.cloud_cluster_id} [{mode}]")
        print(f"  Clients            : {d.clients}")
        print(f"  Servers            : {d.servers}")
        print(f"  Best cut           : {d.best_cut}")
        print(f"  Producer rate      : {d.producer_rate:.6f}")
        print(f"  Service rate       : {'inf' if np.isinf(d.service_rate) else f'{d.service_rate:.6f}'}")
        print(f"  Throughput         : {d.throughput:.6f}")
        print(f"  Round time         : {d.round_time:.6f}")
        print("-" * 100)
