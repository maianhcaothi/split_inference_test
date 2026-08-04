"""
Replay the Hungarian solver offline from a run's clustering_input.json.

The server prints only the solver's ANSWER (K, best_cuts, throughput), so when the
answer changes between runs there is no way to tell which input moved. This reruns
the exact same solver on the recorded input, and sweeps the one input we know is
uncertain — the per-edge egress rate — so you can see how sensitive K and the cut
point are to it.

Usage (from split_inference_test/):
    python tools/replay_clustering.py results/results_MMDD_HHMM_tag/clustering_input.json
    python tools/replay_clustering.py <json> --sweep 1,2,5,10,20,50,100
    python tools/replay_clustering.py <json> --rate 12          # single rate, verbose

Needs no model file and no broker: everything comes from the json.
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np

from src.Clustering import DeterministicSimilarityAssignmentSolver


def build_solver(data, rate_mb_s=None):
    """Rebuild the solver exactly as Server._run_hungarian did.

    rate_mb_s=None keeps the rates the run actually used (already divided by
    egress_share); a value overrides every edge with that per-edge rate.
    """
    edge_times = np.vstack([e["layer_times_s"] for e in data["edges"]])
    cloud_times = np.vstack([c["layer_times_s"] for c in data["clouds"]])
    n, m = len(data["edges"]), len(data["clouds"])
    if rate_mb_s is None:
        rates = np.array([[e["rate_used_mb_s"]] * m for e in data["edges"]])
    else:
        rates = np.full((n, m), float(rate_mb_s))
    return DeterministicSimilarityAssignmentSolver(
        client_layer_times=edge_times,
        server_layer_times=cloud_times,
        cut_data_sizes=np.array(data["cut_data_sizes_mb"], dtype=float),
        input_data_size=float(data["input_data_size_mb"]),
        network_rates=rates,
    )


def solve(data, rate_mb_s=None):
    solver = build_solver(data, rate_mb_s)
    out = solver.solve_best_over_k("hungarian", max_clusters=int(data["max_clusters"]))
    return solver, out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("json_path")
    ap.add_argument("--sweep", default="1,2,5,10,20,50,100,200",
                    help="comma-separated per-edge MB/s values to try")
    ap.add_argument("--rate", type=float, default=None,
                    help="single per-edge MB/s; prints the full per-K breakdown")
    args = ap.parse_args()

    with open(args.json_path, encoding="utf-8") as f:
        data = json.load(f)

    bs = data["batch_size"]
    print(f"model={data['model_name']} batch={bs} "
          f"edges={len(data['edges'])} clouds={len(data['clouds'])} "
          f"max_clusters={data['max_clusters']}")
    print(f"egress_share={data['egress_share']}  "
          f"network_rate_mb_s(config)={data['network_rate_mb_s']}")

    meas = [e["measured_mb_s"] for e in data["edges"] if e["measured_mb_s"] is not None]
    if meas:
        print(f"measured (solo/idle link): min={min(meas):.1f} "
              f"median={float(np.median(meas)):.1f} max={max(meas):.1f} MB/s")
    used = [e["rate_used_mb_s"] for e in data["edges"]]
    print(f"rate actually fed to solver: min={min(used):.2f} max={max(used):.2f} MB/s")

    # Per-edge total compute, the other half of the cut decision.
    tot = [sum(e["layer_times_s"]) for e in data["edges"]]
    print(f"edge full-model time: min={min(tot):.3f} median={float(np.median(tot)):.3f} "
          f"max={max(tot):.3f} s/batch  (spread {max(tot) / max(min(tot), 1e-9):.2f}x)")
    ctot = [sum(c["layer_times_s"]) for c in data["clouds"]]
    print(f"cloud full-model time: {[round(v, 3) for v in ctot]} s/batch")

    sizes = np.array(data["cut_data_sizes_mb"], dtype=float)
    nb = int((data.get("compress") or {}).get("num_bit", 0) or 0)
    wire = sizes * (nb / 32.0) if (data.get("compress") or {}).get("enable") else sizes

    def msg_mb(cut):
        """Wire size for a solver cut. Mirrors Clustering.net_time's three cases:
        cut < 0 ships the raw input, cut >= len(table) (== L-1) runs everything on
        the edge and sends NOTHING, otherwise it is the feature map."""
        if cut < 0:
            return float(data["input_data_size_mb"]) * (nb / 32.0 if nb else 1.0)
        if cut >= len(wire):
            return None            # nothing crosses the network
        return float(wire[cut])

    def msg_str(cut):
        v = msg_mb(cut)
        return "none" if v is None else f"{v:.1f}"

    if args.rate is not None:
        solver, out = solve(data, args.rate)
        print(f"\n--- per-edge rate = {args.rate} MB/s ---")
        for k, v in sorted(out["all_results"].items()):
            mark = " <== chosen" if k == out["best_k"] else ""
            print(f"  K={k}: throughput={v['throughput']:.4f} "
                  f"round_time_proxy={v['system_round_time_proxy']:.3f}{mark}")
        r = out["best_result"]
        cuts = [int(c) for c in r.best_cuts]
        print(f"  best_cuts={cuts}  splits={[c + 1 for c in cuts]}")
        for c in cuts:
            v = msg_mb(c)
            if v is None:
                print(f"    cut {c}: whole model on the edge, nothing sent")
            else:
                print(f"    cut {c}: message ~{v:.2f} MB on the wire")
        return

    print(f"\n{'per-edge MB/s':>14} | {'K':>2} | {'throughput':>10} | {'fps':>6} | "
          f"{'best_cuts':<16} | msg MB")
    print("-" * 76)
    for rate in [float(x) for x in args.sweep.split(",") if x.strip()]:
        _, out = solve(data, rate)
        r = out["best_result"]
        cuts = [int(c) for c in r.best_cuts]
        print(f"{rate:>14.1f} | {out['best_k']:>2} | {r.total_throughput:>10.4f} | "
              f"{r.total_throughput * bs:>6.1f} | {str(cuts):<16} | "
              f"{','.join(msg_str(c) for c in cuts)}")

    print("\nRead it like this: the cut the solver picks is a function of the rate you "
          "give it.\nA rate that is too high makes transfer look free and pushes the cut "
          "deep (big\nfeature maps); too low pushes it shallow. Find the row whose 'msg MB' "
          "the link\ncan actually carry at that fps, and pin clustering.network_rate_mb_s "
          "there with\nmeasure_bandwidth: False so the cut stops moving between runs.")


if __name__ == "__main__":
    main()
