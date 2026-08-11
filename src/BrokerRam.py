"""RAM usage of the QUEUE HOST (the machine running RabbitMQ), sampled by the
server for the whole run.

Why the server measures it and not the broker itself: nothing of ours runs on
machine-1 — it only hosts RabbitMQ — so there is no process there to instrument.
Every other meter in this project is reported by a device that runs our code;
this one has to be pulled from outside, and the server is the only component
that lives for the entire run and already owns the shutdown/archive step.

Why it matters. Every intermediate feature map crosses that box, and the depth
guard in `backpressure` exists precisely because it can be filled up: at
batch-size 32 an uncompressed split tensor is ~39 MB, so a queue that grows to
ten messages is ~400 MB sitting in the broker's memory. RabbitMQ answers a
high-water mark by blocking publishers, which shows up on the devices as a stall
with no obvious cause — free time attributed to `backpressure` or `downstream`
with nothing wrong on the device itself. The broker's RAM curve is what tells
those two stories apart, so it is sampled over the same window as everything
else and archived with the run.

Two sources, and the log always says which one produced a line:

  * `source=ssh` — the real thing: /proc/meminfo on the queue host, so it is
    HOST memory across all processes, plus the RSS of the Erlang VM so RabbitMQ's
    own share is separable from the rest of the box.
  * `source=rabbitmq_api` — fallback when SSH can't be established. The
    management API only knows about the broker, so `used_mb` is then the Erlang
    VM's memory and NOT host memory. Labelled, never silently substituted.

Sampling is one long-lived SSH session running a remote loop, not one SSH per
sample: a per-sample connection would pay a TCP + auth handshake every second on
the very machine whose load is being measured.

Everything here degrades to a warning. A RAM number is never worth failing a
finished run for.
"""

import base64
import os
import shutil
import subprocess
import tempfile
import threading
import time

_KB = 1024.0  # /proc/meminfo is in kB; MB = kB / 1024

# Bound the remote loop so a sampler orphaned by a hard kill of the server can't
# run on the broker forever. 24h at the default interval.
_MAX_REMOTE_SAMPLES = 86400

# How long to wait for the first sample before declaring SSH unusable and
# falling back to the management API. Covers connect + auth on a slow LAN.
_FIRST_SAMPLE_TIMEOUT_S = 20.0


def _remote_script(interval_s, max_samples):
    """The sampler that runs ON the queue host. POSIX sh, no bashisms.

    One line per sample, all fields on one line so a partial read can never
    interleave two samples:

        RAM <epoch_ns> <total> <free> <avail> <buffers> <cached> <swaptotal>
            <swapfree> <rabbit_rss>          (every field in kB except the ts)

    `ps` is run before awk so the RSS belongs to the same instant as the
    meminfo snapshot. The Erlang VM is matched on beam/rabbit/epmd, which is
    every process RabbitMQ starts.
    """
    return f"""
i=0
while [ $i -lt {max_samples} ]; do
  i=$((i+1))
  r=`ps -eo rss=,comm= 2>/dev/null | awk '$2 ~ /beam|rabbit|epmd/ {{s+=$1}} END{{print s+0}}'`
  awk -v ts="`date +%s%N`" -v r="$r" '
    /^MemTotal:/{{t=$2}} /^MemFree:/{{f=$2}} /^MemAvailable:/{{a=$2}}
    /^Buffers:/{{b=$2}} /^Cached:/{{c=$2}}
    /^SwapTotal:/{{st=$2}} /^SwapFree:/{{sf=$2}}
    END{{print "RAM", ts, t, f, a, b, c, st, sf, r}}' /proc/meminfo
  sleep {interval_s}
done
"""


def _fmt_kv(**kw):
    return " ".join(f"{k}={v}" for k, v in kw.items())


def _stats(vals):
    """n / min / mean / p50 / p95 / max, or None. Nearest-rank percentiles over
    the sorted samples — same convention as Server._stats_ms, so every number
    reported is a value that was actually observed."""
    if not vals:
        return None
    s = sorted(vals)
    n = len(s)

    def pct(q):
        return s[min(n - 1, max(0, int(round((n - 1) * q))))]

    return {"n": n, "min": s[0], "mean": sum(s) / n,
            "p50": pct(0.50), "p95": pct(0.95), "max": s[-1]}


class BrokerRamMonitor:
    """Samples the queue host's RAM in the background for the length of a run.

    start() -> the run happens -> stop() -> write_summary(). Between start and
    stop each sample is appended to `ns_log_path` as it arrives, so a run that
    dies before its shutdown step still leaves the series behind.
    """

    def __init__(self, host, user=None, password=None, port=22, interval_s=1.0,
                 ns_log_path=None, enable=True, api_port=15672,
                 api_user=None, api_password=None, api_vhost="/"):
        self.enable = bool(enable) and bool(host)
        self.host = host
        self.user = user
        self.password = password
        self.port = int(port or 22)
        self.interval_s = max(0.2, float(interval_s or 1.0))
        self.ns_log_path = ns_log_path
        self.api_port = int(api_port or 15672)
        self.api_user = api_user
        self.api_password = api_password
        self.api_vhost = api_vhost

        self.samples = []           # [{t_ns, total_kb, used_kb, ...}, ...]
        self.source = None          # "ssh" | "rabbitmq_api" | None
        self.error = None           # why there is no data, for the summary line
        # Phase boundaries on the SERVER's clock, set by mark(): "dispatch" when
        # work is first handed out, "finish" when the run (incl. drain) is over.
        # They split one continuous series into idle / run / tail, which is the
        # whole point of starting the sampler at server init: the same machine,
        # measured the same way, with and without the system running.
        self.marks = {}

        self._proc = None
        self._askpass_dir = None
        self._askpass_path = None
        self._threads = []
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._fh = None
        self._stderr_tail = []

    # ─── lifecycle ─────────────────────────────────────────────────────────────

    def start(self):
        if not self.enable:
            return False
        if self.ns_log_path:
            try:
                self._fh = open(self.ns_log_path, "a")
            except OSError as e:
                self._fh = None
                self._warn(f"sample log unavailable ({e}) — keeping samples in memory only")
        if self._start_ssh():
            return True
        # SSH refused/absent: keep measuring, but say plainly that the number
        # changed meaning (broker process, not host).
        return self._start_api_fallback()

    def mark(self, name):
        """Record a phase boundary on the server's clock.

        Two are used: 'dispatch' (work handed out — everything before it is the
        host at rest) and 'finish' (run over, drain included). Sampling itself
        never pauses, so the boundaries only ever partition an already-continuous
        series; a missing mark degrades to a coarser split, never to a gap."""
        if not self.enable:
            return
        with self._lock:
            self.marks[str(name)] = time.time_ns()

    def _phase(self, t_ns):
        """Which phase a sample belongs to. No dispatch mark means work never
        started, so every sample is idle."""
        d, f = self.marks.get("dispatch"), self.marks.get("finish")
        if f is not None and t_ns >= f:
            return "tail"
        if d is None or t_ns < d:
            return "idle"
        return "run"

    def stop(self, timeout_s=5.0):
        """Stop sampling. Safe to call twice, and safe to call when start()
        failed."""
        self._stop.set()
        self._teardown_ssh(timeout_s)
        for t in self._threads:
            t.join(timeout=timeout_s)
        self._threads = []
        if self._fh is not None:
            try:
                self._fh.close()
            except OSError:
                pass
            self._fh = None
    def _teardown_ssh(self, timeout_s=5.0):
        """Kill the SSH sampler and remove the askpass file. Deliberately does
        NOT touch `_fh` or the stop flag: the failed-start path calls this and
        then keeps sampling through the management API, into the same log."""
        p, self._proc = self._proc, None
        if p is not None:
            for kill in (p.terminate, p.kill):
                try:
                    kill()
                    p.wait(timeout=max(0.5, timeout_s / 2))
                    break
                except subprocess.TimeoutExpired:
                    continue
                except Exception:
                    break
        if self._askpass_dir:
            shutil.rmtree(self._askpass_dir, ignore_errors=True)
            self._askpass_dir = None
            self._askpass_path = None

    # ─── SSH sampler (the real host-RAM path) ──────────────────────────────────

    def _ssh_argv(self):
        """argv for the streaming sampler, or None when this machine can't do
        non-interactive password SSH.

        Three auth paths, in order of preference:
          1. no password configured -> key auth, BatchMode so it fails fast;
          2. sshpass on PATH        -> the simple case;
          3. SSH_ASKPASS + setsid   -> what actually works on the DAI server,
             which has no sshpass. ssh only consults SSH_ASKPASS when it has no
             controlling terminal, hence setsid.
        """
        remote = _remote_script(self.interval_s, _MAX_REMOTE_SAMPLES)
        b64 = base64.b64encode(remote.encode("utf-8")).decode("ascii")
        target = f"{self.user}@{self.host}" if self.user else self.host
        ssh = [
            "ssh", "-T",
            "-p", str(self.port),
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ConnectTimeout=10",
            "-o", "ServerAliveInterval=15",
            "-o", "ServerAliveCountMax=4",
            "-o", "LogLevel=ERROR",
            target,
            f"echo {b64} | base64 -d | sh",
        ]
        if not shutil.which("ssh"):
            self.error = "no ssh client on the server machine"
            return None
        if not self.password:
            return ssh[:1] + ["-o", "BatchMode=yes"] + ssh[1:]

        pw_opts = ["-o", "PubkeyAuthentication=no",
                   "-o", "PreferredAuthentications=password,keyboard-interactive",
                   "-o", "NumberOfPasswordPrompts=1"]
        ssh = ssh[:1] + pw_opts + ssh[1:]

        if shutil.which("sshpass"):
            return ["sshpass", "-p", self.password] + ssh
        if not shutil.which("setsid"):
            self.error = ("password SSH needs sshpass or setsid+SSH_ASKPASS; "
                          "neither is available on the server machine")
            return None
        self._askpass_dir = tempfile.mkdtemp(prefix="brokerram-")
        path = os.path.join(self._askpass_dir, "askpass.sh")
        quoted = self.password.replace("'", "'\\''")
        with open(path, "w") as fh:
            fh.write(f"#!/bin/sh\nprintf '%s\\n' '{quoted}'\n")
        os.chmod(path, 0o700)
        self._askpass_path = path
        return ["setsid", "-w"] + ssh

    def _start_ssh(self):
        argv = self._ssh_argv()
        if argv is None:
            return False
        env = dict(os.environ)
        if self._askpass_dir:
            env["SSH_ASKPASS"] = self._askpass_path
            env["SSH_ASKPASS_REQUIRE"] = "force"
            env.setdefault("DISPLAY", ":0")
        try:
            self._proc = subprocess.Popen(
                argv, stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                env=env, text=True, bufsize=1)
        except OSError as e:
            self.error = f"could not launch ssh: {e}"
            return False

        # Set before the readers start: the very first sample is logged by the
        # reader thread, and _sample_line stamps it with this.
        self.source = "ssh"
        first = threading.Event()
        proc = self._proc
        for target, args in ((self._read_stdout, (proc, first)),
                             (self._read_stderr, (proc,))):
            t = threading.Thread(target=target, args=args,
                                 name="broker-ram", daemon=True)
            t.start()
            self._threads.append(t)

        if first.wait(_FIRST_SAMPLE_TIMEOUT_S):
            return True
        tail = " | ".join(self._stderr_tail[-3:]) or "no output"
        self.error = (f"ssh to {self.host} produced no sample within "
                      f"{_FIRST_SAMPLE_TIMEOUT_S:.0f}s ({tail})")
        self.source = None
        self._teardown_ssh()
        for t in self._threads:
            t.join(timeout=2.0)
        self._threads = []
        return False

    def _read_stdout(self, proc, first):
        for line in proc.stdout:
            if self._stop.is_set():
                break
            parts = line.split()
            if len(parts) != 10 or parts[0] != "RAM":
                continue
            try:
                ts, total, free, avail, buf, cached, sw_t, sw_f, rss = (
                    int(float(v)) for v in parts[1:])
            except ValueError:
                continue
            if total <= 0:
                continue
            self._add({
                "t_ns": ts,
                "total_kb": total,
                # MemAvailable is the kernel's own estimate of what a new
                # allocation could get, so total-avail is memory that is really
                # committed. total-free would count reclaimable page cache as
                # used and read ~90% on any box that has touched a disk.
                "used_kb": total - avail,
                "avail_kb": avail,
                "free_kb": free,
                "cached_kb": buf + cached,
                "swap_used_kb": max(0, sw_t - sw_f),
                "rabbit_rss_kb": rss,
            })
            first.set()

    def _read_stderr(self, proc):
        for line in proc.stderr:
            line = line.strip()
            if line:
                self._stderr_tail.append(line)
                del self._stderr_tail[:-10]

    # ─── management-API fallback (broker process only) ─────────────────────────

    def _start_api_fallback(self):
        try:
            import requests  # noqa: F401
        except ImportError:
            return False
        t = threading.Thread(target=self._api_loop, name="broker-ram-api", daemon=True)
        t.start()
        self._threads.append(t)
        self.source = "rabbitmq_api"
        return True

    def _api_loop(self):
        import requests
        from requests.auth import HTTPBasicAuth
        url = f"http://{self.host}:{self.api_port}/api/nodes"
        auth = HTTPBasicAuth(self.api_user or "", self.api_password or "")
        while not self._stop.is_set():
            try:
                r = requests.get(url, auth=auth, timeout=2)
                if r.status_code == 200:
                    for node in r.json() or []:
                        used = int(node.get("mem_used") or 0)
                        limit = int(node.get("mem_limit") or 0)
                        if used <= 0:
                            continue
                        self._add({
                            "t_ns": time.time_ns(),
                            # mem_limit is the broker's high-water mark, not the
                            # machine's RAM. It is the only ceiling this source
                            # knows, so used_pct below is 'share of the broker's
                            # own budget' — which is what triggers publisher
                            # blocking, and is the useful number here.
                            "total_kb": (limit / _KB) if limit else 0,
                            "used_kb": used / _KB,
                            "avail_kb": max(0, limit - used) / _KB if limit else 0,
                            "free_kb": 0,
                            "cached_kb": 0,
                            "swap_used_kb": 0,
                            "rabbit_rss_kb": used / _KB,
                        })
                        break
            except Exception:
                pass
            self._stop.wait(self.interval_s)

    # ─── sample bookkeeping ────────────────────────────────────────────────────

    def _add(self, s):
        with self._lock:
            self.samples.append(s)
            if self._fh is None:
                return
            try:
                self._fh.write(self._sample_line(s) + "\n")
                self._fh.flush()
            except OSError:
                pass

    def _sample_line(self, s):
        total = s["total_kb"]
        pct = (100.0 * s["used_kb"] / total) if total else 0.0
        # phase is stamped at write time, which is also sample time: marks only
        # ever move forward, so a line written before dispatch is idle and stays
        # idle. Carrying it here makes the series self-describing — a reader can
        # shade the plot without cross-referencing the summary's mark timestamps.
        return f"{s['t_ns']} " + _fmt_kv(
            host=self.host,
            source=self.source or "ssh",
            phase=self._phase(s["t_ns"]),
            total_mb=f"{total / _KB:.1f}",
            used_mb=f"{s['used_kb'] / _KB:.1f}",
            used=f"{pct:.2f}%",
            avail_mb=f"{s['avail_kb'] / _KB:.1f}",
            free_mb=f"{s['free_kb'] / _KB:.1f}",
            cached_mb=f"{s['cached_kb'] / _KB:.1f}",
            swap_used_mb=f"{s['swap_used_kb'] / _KB:.1f}",
            rabbit_rss_mb=f"{s['rabbit_rss_kb'] / _KB:.1f}",
        )

    def _warn(self, msg):
        try:
            import src.Log
            src.Log.print_with_color(f"[BrokerRAM] {msg}", "yellow")
        except Exception:
            print(f"[BrokerRAM] {msg}")

    # ─── report ────────────────────────────────────────────────────────────────

    def _phase_summary(self, ss):
        """Roll up one phase's samples: how much RAM the host held during it."""
        if not ss:
            return None
        used = [s["used_kb"] / _KB for s in ss]
        pct = [(100.0 * s["used_kb"] / s["total_kb"]) if s["total_kb"] else 0.0
               for s in ss]
        return {
            "samples": len(ss),
            "span_s": (ss[-1]["t_ns"] - ss[0]["t_ns"]) / 1e9,
            "used": _stats(used),
            "used_pct": _stats(pct),
            "rss_mean": sum(s["rabbit_rss_kb"] / _KB for s in ss) / len(ss),
            "rss_max": max(s["rabbit_rss_kb"] / _KB for s in ss),
            "t_start_ns": ss[0]["t_ns"],
            "t_end_ns": ss[-1]["t_ns"],
        }

    def summary(self):
        """Whole-run roll-up, or None when nothing was sampled.

        Carries a per-phase breakdown beside the whole-window numbers. The
        phases are the reason the sampler starts at server init: 'idle' is this
        exact machine, measured by this exact method, with nothing of ours
        running on it — the only honest reference for what the run then added."""
        with self._lock:
            ss = list(self.samples)
        if not ss:
            return None
        by_phase = {}
        for s in ss:
            by_phase.setdefault(self._phase(s["t_ns"]), []).append(s)
        phases = {name: self._phase_summary(rows)
                  for name, rows in by_phase.items() if rows}
        used = [s["used_kb"] / _KB for s in ss]
        pct = [(100.0 * s["used_kb"] / s["total_kb"]) if s["total_kb"] else 0.0
               for s in ss]
        rss = [s["rabbit_rss_kb"] / _KB for s in ss]
        span_s = (ss[-1]["t_ns"] - ss[0]["t_ns"]) / 1e9
        return {
            "host": self.host,
            "source": self.source,
            "samples": len(ss),
            "interval_s": self.interval_s,
            "span_s": span_s,
            "total_mb": ss[0]["total_kb"] / _KB,
            "used": _stats(used),
            "used_pct": _stats(pct),
            "rss": _stats(rss),
            "start_mb": used[0],
            "end_mb": used[-1],
            # The two numbers the queue host is actually being watched for: how
            # much RAM the run added, and the worst moment it reached.
            "growth_mb": used[-1] - used[0],
            "peak_over_start_mb": max(used) - used[0],
            "swap_max_mb": max(s["swap_used_kb"] / _KB for s in ss),
            "t_start_ns": ss[0]["t_ns"],
            "t_end_ns": ss[-1]["t_ns"],
            "phases": phases,
            "marks": dict(self.marks),
        }

    def write_summary(self, path, t_ns=None):
        """Append this run's summary block to `path` (broker_ram.log). Returns
        the summary dict, or None when there was nothing to write."""
        s = self.summary()
        t_ns = t_ns or time.time_ns()
        if s is None:
            if not self.enable:
                return None
            line = (f"{t_ns} BROKER " + _fmt_kv(host=self.host, samples=0) +
                    f" (no samples: {self.error or 'unknown reason'})")
            try:
                with open(path, "a") as fh:
                    fh.write(line + "\n")
            except OSError:
                pass
            return None
        lines = [
            f"{t_ns} BROKER " + _fmt_kv(
                host=s["host"], source=s["source"], samples=s["samples"],
                interval_s=f"{s['interval_s']:.3f}", span_s=f"{s['span_s']:.3f}",
                total_mb=f"{s['total_mb']:.1f}",
                t_start_ns=s["t_start_ns"], t_end_ns=s["t_end_ns"]),
            f"{t_ns} USED " + _fmt_kv(
                min_mb=f"{s['used']['min']:.1f}", mean_mb=f"{s['used']['mean']:.1f}",
                p50_mb=f"{s['used']['p50']:.1f}", p95_mb=f"{s['used']['p95']:.1f}",
                max_mb=f"{s['used']['max']:.1f}",
                min=f"{s['used_pct']['min']:.2f}%", mean=f"{s['used_pct']['mean']:.2f}%",
                p95=f"{s['used_pct']['p95']:.2f}%", max=f"{s['used_pct']['max']:.2f}%"),
            f"{t_ns} DELTA " + _fmt_kv(
                start_mb=f"{s['start_mb']:.1f}", end_mb=f"{s['end_mb']:.1f}",
                growth_mb=f"{s['growth_mb']:.1f}",
                peak_over_start_mb=f"{s['peak_over_start_mb']:.1f}"),
            f"{t_ns} RABBIT " + _fmt_kv(
                mean_rss_mb=f"{s['rss']['mean']:.1f}", max_rss_mb=f"{s['rss']['max']:.1f}",
                swap_max_mb=f"{s['swap_max_mb']:.1f}"),
        ]

        # One line per phase, in the order they happen, then the comparison the
        # phases exist for. Phases with no samples are omitted rather than
        # written as zeros — a run with no idle window should look like one.
        ph = s.get("phases") or {}
        for name in ("idle", "run", "tail"):
            p = ph.get(name)
            if not p:
                continue
            lines.append(
                f"{t_ns} PHASE " + _fmt_kv(
                    phase=name, samples=p["samples"], span_s=f"{p['span_s']:.3f}",
                    min_mb=f"{p['used']['min']:.1f}", mean_mb=f"{p['used']['mean']:.1f}",
                    p50_mb=f"{p['used']['p50']:.1f}", p95_mb=f"{p['used']['p95']:.1f}",
                    max_mb=f"{p['used']['max']:.1f}",
                    mean=f"{p['used_pct']['mean']:.2f}%", max=f"{p['used_pct']['max']:.2f}%",
                    mean_rss_mb=f"{p['rss_mean']:.1f}", max_rss_mb=f"{p['rss_max']:.1f}",
                    t_start_ns=p["t_start_ns"], t_end_ns=p["t_end_ns"]))

        idle, run, tail = ph.get("idle"), ph.get("run"), ph.get("tail")
        if idle and run:
            base = idle["used"]["mean"]
            cmp_kv = dict(
                idle_mean_mb=f"{base:.1f}",
                run_mean_mb=f"{run['used']['mean']:.1f}",
                run_minus_idle_mb=f"{run['used']['mean'] - base:.1f}",
                run_peak_over_idle_mb=f"{run['used']['max'] - base:.1f}",
                idle_rss_mb=f"{idle['rss_mean']:.1f}",
                run_rss_mb=f"{run['rss_mean']:.1f}",
                run_rss_over_idle_mb=f"{run['rss_max'] - idle['rss_mean']:.1f}",
            )
            if tail:
                # Did the host come back to where it started? Measured a couple of
                # seconds after the run, so a positive number is 'not yet' — it is
                # only a leak if it stays positive on a later run's idle window.
                cmp_kv["tail_mean_mb"] = f"{tail['used']['mean']:.1f}"
                cmp_kv["tail_minus_idle_mb"] = f"{tail['used']['mean'] - base:.1f}"
                cmp_kv["tail_span_s"] = f"{tail['span_s']:.3f}"
            lines.append(f"{t_ns} COMPARE " + _fmt_kv(**cmp_kv))
        try:
            with open(path, "a") as fh:
                fh.write("\n".join(lines) + "\n")
        except OSError as e:
            self._warn(f"summary write failed ({path}): {e}")
        return s
