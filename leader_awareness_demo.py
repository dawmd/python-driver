#!/usr/bin/env python3
"""Live demo of leader-aware request routing for strongly-consistent tables.

This script brings up a small multi-node ScyllaDB cluster (via ccm), creates a
strongly-consistent keyspace with a tablets-based table (RF=3, many tablets),
and then drives continuous QUORUM writes and reads through the ScyllaDB Python
driver.  To keep the picture moving it also forces a Raft leader change on a
random subset of the tablets every few seconds (auto-balancing is disabled), so
each tablet's leader relocates.  Every few seconds it scrapes the ScyllaDB
Prometheus endpoint of each node for the
strong-consistency coordinator "bounce" counters and plots their cumulative
totals live with matplotlib.

The point of the demo is to visualise *leader awareness*:

  * With ``--routing-v2 on`` (default) the driver negotiates the
    ``TABLETS_ROUTING_V2_EXPERIMENTAL`` protocol extension, learns which
    replica is the tablet's Raft leader, and sends the request straight to it.
    The coordinator does not have to forward the request, so the bounce
    counters stay flat / near zero.

  * With ``--routing-v2 off`` the V2 extension is suppressed on the driver
    side, so it falls back to plain token awareness (V1).  It still picks a
    replica, but not necessarily the leader, so the strong-consistency
    coordinator has to *bounce* the request to the correct node/shard and the
    bounce counters climb steadily.

Run it once with each mode to contrast the two graphs:

    python leader_awareness_demo.py --routing-v2 on
    python leader_awareness_demo.py --routing-v2 off

In a graphical session this opens a live matplotlib window.  In a headless
shell (no ``DISPLAY``, e.g. an SSH / VS Code remote terminal) it instead writes
a live-updating PNG (``--out``) and drives load for ``--duration`` seconds --
open that PNG in VS Code to watch it refresh.

Requires: a local ScyllaDB build, ``ccmlib`` and ``matplotlib``.
"""

import argparse
import concurrent.futures
import os
import random
import shutil
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

METRIC_PREFIX = "scylla_strong_consistency_coordinator_"

# Short name -> full Prometheus metric name.  These counters use
# ``set_skip_when_empty()`` on the server, so they are simply absent from
# /metrics until they become non-zero -- a missing line therefore means 0.
BOUNCE_METRICS = {
    "write_node_bounces": METRIC_PREFIX + "write_node_bounces",
    "write_shard_bounces": METRIC_PREFIX + "write_shard_bounces",
    "read_node_bounces": METRIC_PREFIX + "read_node_bounces",
    "read_shard_bounces": METRIC_PREFIX + "read_shard_bounces",
}

# Stable plot order / colours.
METRIC_ORDER = [
    "write_node_bounces",
    "write_shard_bounces",
    "read_node_bounces",
    "read_shard_bounces",
]

PROMETHEUS_PORT = 9180

# All-zero Raft server id returned by /raft/leader_host when a group currently
# has no known leader (e.g. an election is in progress).
NULL_UUID = "00000000-0000-0000-0000-000000000000"


def parse_prometheus(text, wanted_full_names):
    """Sum every series whose base metric name is in ``wanted_full_names``.

    ScyllaDB exposes these counters per shard (e.g.
    ``scylla_..._write_node_bounces{shard="0"} 5``), so all series sharing a
    base name must be added together.  Absent metrics contribute 0.

    Args:
        text: Raw Prometheus exposition text.
        wanted_full_names: Iterable of full metric names to collect.

    Returns:
        Dict mapping each requested full name to its summed value (float).
    """
    sums = {name: 0.0 for name in wanted_full_names}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        # ``metric{labels} value`` -- the value is the last whitespace token.
        try:
            series, value = line.rsplit(" ", 1)
        except ValueError:
            continue
        base = series.split("{", 1)[0].strip()
        if base in sums:
            try:
                sums[base] += float(value)
            except ValueError:
                pass
    return sums


class MetricsPoller(threading.Thread):
    """Background thread scraping bounce counters from every node.

    It keeps the last known per-node values so a transient scrape failure
    never makes the cumulative totals dip.  Totals are exposed as deltas from
    the first successful reading so the graph starts at zero.
    """

    def __init__(self, node_addresses, interval, stop_event):
        super().__init__(name="metrics-poller", daemon=True)
        self._urls = {
            addr: "http://{}:{}/metrics".format(addr, PROMETHEUS_PORT)
            for addr in node_addresses
        }
        self._interval = interval
        self._stop_event = stop_event
        self._full_names = list(BOUNCE_METRICS.values())

        # Last known raw totals per node: {addr: {full_name: value}}.
        self._per_node = {addr: defaultdict(float) for addr in node_addresses}
        self._baseline = None  # {short_name: value} captured on first poll.

        self._lock = threading.Lock()
        # Plot history (guarded by _lock).
        self.times = []
        self.series = {short: [] for short in BOUNCE_METRICS}

    def _scrape_once(self):
        for addr, url in self._urls.items():
            try:
                with urllib.request.urlopen(url, timeout=self._interval) as resp:
                    text = resp.read().decode("utf-8", "replace")
            except (urllib.error.URLError, OSError):
                # Keep the previous values for this node.
                continue
            self._per_node[addr] = parse_prometheus(text, self._full_names)

    def _current_totals(self):
        totals = {short: 0.0 for short in BOUNCE_METRICS}
        for values in self._per_node.values():
            for short, full in BOUNCE_METRICS.items():
                totals[short] += values.get(full, 0.0)
        return totals

    def run(self):
        start = time.time()
        while not self._stop_event.is_set():
            self._scrape_once()
            totals = self._current_totals()
            if self._baseline is None:
                self._baseline = dict(totals)
            elapsed = time.time() - start
            with self._lock:
                self.times.append(elapsed)
                for short in BOUNCE_METRICS:
                    self.series[short].append(totals[short] - self._baseline[short])
            print(
                "[{:6.1f}s] ".format(elapsed)
                + "  ".join(
                    "{}={:.0f}".format(short, totals[short] - self._baseline[short])
                    for short in METRIC_ORDER
                ),
                flush=True,
            )
            self._stop_event.wait(self._interval)

    def snapshot(self):
        """Return a thread-safe copy of (times, {short: values})."""
        with self._lock:
            return list(self.times), {k: list(v) for k, v in self.series.items()}


# ---------------------------------------------------------------------------
# Workload
# ---------------------------------------------------------------------------

class Workload(threading.Thread):
    """Fires prepared statements in a tight loop until asked to stop.

    ``params_fn`` turns a random partition key into the positional bind
    parameters for the statement (e.g. ``(pk, pk)`` for the insert or
    ``(pk,)`` for the select).
    """

    def __init__(self, name, session, statement, key_space, stop_event, counters, params_fn):
        super().__init__(name=name, daemon=True)
        self._session = session
        self._statement = statement
        self._key_space = key_space
        self._stop_event = stop_event
        self._counters = counters  # {"ok": int, "err": int} shared per kind.
        self._params_fn = params_fn

    def run(self):
        rnd = random.Random()
        while not self._stop_event.is_set():
            pk = rnd.randrange(self._key_space)
            try:
                self._session.execute(self._statement, self._params_fn(pk))
                self._counters["ok"] += 1
            except Exception:  # noqa: BLE001 - demo: keep driving load
                self._counters["err"] += 1


# ---------------------------------------------------------------------------
# Leader churn
# ---------------------------------------------------------------------------

class LeaderChurner(threading.Thread):
    """Periodically forces a Raft leader change on a random subset of tablets.

    For a strongly-consistent table every tablet is its own Raft group, and the
    tablet's ``tablet_version`` -- which the driver caches for leader-aware
    routing -- is derived from the replica set *and* the current Raft leader.
    Every ``churn_interval`` seconds this reads the tablet layout from
    ``system.tablets``, picks a random ``churn_fraction`` of the tablets with a
    seeded RNG (so a run is reproducible), and asks each tablet's current leader
    to step down via ``/raft/trigger_stepdown``.  Leadership transfers to
    another replica, so the tablet's ``tablet_version`` changes and the driver's
    cached leader for that tablet goes stale: a coordinator that is not
    leader-aware then has to bounce subsequent requests until its routing
    information catches up.

    Forcing a leader change is far cheaper than migrating data -- no streaming
    and no replica-set reconfiguration -- so the stepdowns in a sweep run with
    bounded concurrency (``churn_concurrency``) and the next sweep only starts
    once the current one has completed.  Automatic tablet balancing is disabled
    first so leader placement is driven only by this loop.
    """

    def __init__(self, session, target_ip, host_addr, args, stop_event, counters):
        super().__init__(name="leader-churner", daemon=True)
        self._session = session
        self._target_ip = target_ip
        self._host_addr = dict(host_addr)  # {host_id: ip}
        self._args = args
        self._stop_event = stop_event
        self._counters = counters  # {"stepdowns": int, "err": int}
        self._table_id = None

    def _request(self, ip, path, method, params, timeout):
        url = "http://{}:{}{}".format(ip, self._args.api_port, path)
        if params:
            url += "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, method=method)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", "replace")

    def _resolve_table_id(self):
        row = self._session.execute(
            "SELECT id FROM system_schema.tables WHERE keyspace_name=%s AND table_name=%s",
            (self._args.keyspace, self._args.table)).one()
        return row.id if row else None

    def _read_tablets(self):
        # table_id is the partition key of system.tablets, so this is a cheap
        # single-partition read.  ``raft_group_id`` is the tablet's Raft group
        # (only set for strongly-consistent tablets) and every host in
        # ``replicas`` is a group member -- the leader is one of them.
        rows = self._session.execute(
            "SELECT raft_group_id, replicas FROM system.tablets WHERE table_id = {}".format(
                self._table_id))
        tablets = []
        for r in rows:
            if not r.raft_group_id:
                continue  # not strongly consistent / no Raft group yet
            hosts = [str(host) for (host, _shard) in (r.replicas or [])]
            if hosts:
                tablets.append((str(r.raft_group_id), hosts))
        return tablets

    def _find_leader(self, group_id, replica_hosts):
        # A tablet's Raft leader is one of its replicas, and every replica knows
        # who the leader is, so ask each replica node in turn until one answers.
        for host in replica_hosts:
            ip = self._host_addr.get(host)
            if ip is None:
                continue
            try:
                body = self._request(ip, "/raft/leader_host", "GET",
                                     {"group_id": group_id}, self._args.stepdown_timeout)
            except Exception:  # noqa: BLE001 - node busy/unreachable; try the next replica
                continue
            leader = body.strip().strip('"')
            if leader and leader != NULL_UUID:
                return leader
        return None

    def _churn_one(self, group_id, replica_hosts):
        leader = self._find_leader(group_id, replica_hosts)
        if leader is None:
            return False, "no current leader"
        leader_ip = self._host_addr.get(leader)
        if leader_ip is None:
            return False, "leader not in cluster"
        # Stepping down the leader hands leadership to another replica; the
        # request must go to the leader itself (a follower would reject it).
        try:
            self._request(leader_ip, "/raft/trigger_stepdown", "POST",
                          {"group_id": group_id, "timeout": str(int(self._args.stepdown_timeout))},
                          self._args.stepdown_timeout)
            return True, None
        except urllib.error.HTTPError as exc:  # leader changed under us, etc.
            try:
                body = exc.read().decode("utf-8", "replace").strip()
            except Exception:  # noqa: BLE001
                body = ""
            return False, "HTTP {}: {}".format(exc.code, body or exc.reason)
        except Exception as exc:  # noqa: BLE001 - timeout / connection reset / ...
            return False, "{}: {}".format(type(exc).__name__, exc)

    def _sweep_once(self, rnd, executor):
        tablets = self._read_tablets()
        if not tablets:
            return
        # Churn only a random subset each sweep so leaders keep moving steadily
        # without touching every tablet at once.
        count = max(1, round(len(tablets) * self._args.churn_fraction))
        subset = rnd.sample(tablets, min(count, len(tablets)))
        futures = [executor.submit(self._churn_one, gid, hosts) for gid, hosts in subset]
        changed = failed = 0
        reasons = defaultdict(int)
        for fut in concurrent.futures.as_completed(futures):
            ok, err = fut.result()
            if ok:
                changed += 1
                self._counters["stepdowns"] += 1
            else:
                failed += 1
                self._counters["err"] += 1
                if err:
                    reasons[err] += 1
        if self._stop_event.is_set():
            return  # interrupted by teardown; skip the noisy partial summary
        print("leader-churner: sweep changed {}/{} leaders ({} failed)".format(
            changed, len(subset), failed), flush=True)
        for reason, n in sorted(reasons.items(), key=lambda kv: -kv[1]):
            print("leader-churner:   {}x {}".format(n, reason), flush=True)

    def run(self):
        self._table_id = self._resolve_table_id()
        if self._table_id is None:
            print("leader-churner: could not resolve table id; leader churn disabled.", flush=True)
            return
        try:
            self._request(self._target_ip, "/storage_service/tablets/balancing", "POST",
                          {"enabled": "false"}, 10)
            print("leader-churner: automatic tablet balancing disabled.", flush=True)
        except Exception as exc:  # noqa: BLE001
            print("leader-churner: could not disable balancing: {}".format(exc), flush=True)

        rnd = random.Random(self._args.seed)
        # Let the workload warm up briefly before the first churn sweep.
        self._stop_event.wait(min(self._args.churn_interval, 3.0))
        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self._args.churn_concurrency, thread_name_prefix="churn")
        try:
            while not self._stop_event.is_set():
                started = time.time()
                try:
                    self._sweep_once(rnd, executor)
                except Exception as exc:  # noqa: BLE001 - keep churning despite transient errors
                    self._counters["err"] += 1
                    print("leader-churner: sweep failed: {}".format(exc), flush=True)
                # Pace sweeps by the interval; a sweep that runs longer just
                # starts the next one immediately so leaders keep churning.
                remaining = self._args.churn_interval - (time.time() - started)
                if remaining > 0:
                    self._stop_event.wait(remaining)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)


# ---------------------------------------------------------------------------
# Cluster / driver setup
# ---------------------------------------------------------------------------

def _rack_topology(nodes, racks):
    """Spread ``nodes`` as evenly as possible across ``racks`` racks in one DC."""
    counts = [nodes // racks + (1 if i < nodes % racks else 0) for i in range(racks)]
    return {"dc1": [c for c in counts if c > 0]}


def build_ccm_cluster(args, work_dir):
    """Create, populate and start a local multi-node ScyllaDB cluster."""
    from ccmlib.scylla_cluster import ScyllaCluster

    # ``--smp`` > 1 gives multiple shards so *shard* bounces are meaningful.
    os.environ.setdefault(
        "SCYLLA_EXT_OPTS",
        "--smp {} --memory {}".format(args.smp, args.memory),
    )

    print("Creating {}-node cluster ({} racks) from {} ...".format(
        args.nodes, args.racks, args.scylla_build_dir), flush=True)
    cluster = ScyllaCluster(work_dir, args.cluster_name, install_dir=args.scylla_build_dir)
    cluster.set_configuration_options({
        # Gates both strongly-consistent tables and the V2 routing extension.
        "experimental_features": ["strongly-consistent-tables"],
        "start_native_transport": True,
    })
    # A rack topology spreads nodes across racks (GossipingPropertyFileSnitch),
    # so RF=3 places one replica per rack.
    cluster.populate(_rack_topology(args.nodes, args.racks) if args.racks > 1 else args.nodes)
    print("Starting cluster (this can take a bit) ...", flush=True)
    cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
    return cluster


def connect_driver(contact_point):
    """Open a driver session with QUORUM as the default consistency level.

    The default execution profile's load balancing policy is already
    ``TokenAwarePolicy(DCAwareRoundRobinPolicy())``; leaving it unset keeps
    token/leader awareness while letting the driver infer the local DC.
    """
    from cassandra import ConsistencyLevel
    from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile

    profile = ExecutionProfile(consistency_level=ConsistencyLevel.QUORUM)
    cluster = Cluster(
        contact_points=[contact_point],
        port=9042,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
    )
    session = cluster.connect()
    return cluster, session


def create_schema(session, args):
    """Create the strongly-consistent keyspace + table, retrying briefly."""
    ks, table = args.keyspace, args.table
    create_ks = (
        "CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = "
        "{{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}} "
        "AND tablets = {{'initial': {tablets}}} AND consistency = 'global'"
    ).format(ks=ks, rf=args.rf, tablets=args.tablets)
    create_tbl = "CREATE TABLE IF NOT EXISTS {ks}.{tbl} (pk int PRIMARY KEY, v int)".format(ks=ks, tbl=table)

    last_exc = None
    for attempt in range(10):
        try:
            session.execute(create_ks)
            session.execute(create_tbl)
            return
        except Exception as exc:  # noqa: BLE001 - group0 may not be ready yet
            last_exc = exc
            time.sleep(1.0)
            print("Schema not ready yet (attempt {}): {}".format(attempt + 1, exc), flush=True)
    raise RuntimeError("Failed to create strongly-consistent schema: {}".format(last_exc))


def disable_routing_v2():
    """Suppress the V2 tablets-routing extension on the driver side.

    With V2 negotiation off the driver never echoes
    ``TABLETS_ROUTING_V2_EXPERIMENTAL`` in STARTUP, so the server falls back to
    V1 (token-aware, but not leader-aware) and the coordinator has to bounce
    strongly-consistent requests to the leader.
    """
    from cassandra.protocol_features import ProtocolFeatures

    ProtocolFeatures.parse_tablets_v2_info = staticmethod(lambda options: False)


# ---------------------------------------------------------------------------
# Live plot
# ---------------------------------------------------------------------------

def gui_display_available():
    """True only when both a GUI display and a usable toolkit are present.

    In a headless shell (e.g. an SSH / VS Code remote terminal with no
    ``DISPLAY``) matplotlib falls back to the non-interactive ``Agg`` backend,
    where ``plt.show()`` returns immediately instead of opening a window -- so
    we detect that up front and render to a PNG instead.
    """
    if not (os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY")):
        return False
    import importlib.util
    return any(importlib.util.find_spec(m) for m in
               ("PyQt6", "PyQt5", "PySide6", "PySide2", "tkinter"))


def _make_figure(args):
    """Build the figure, axes and one line per bounce counter."""
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, 6))
    mode = "ON" if args.routing_v2 == "on" else "OFF"
    fig.suptitle("Leader-aware routing demo - TABLETS_ROUTING_V2: {}".format(mode), fontsize=14)
    ax.set_xlabel("elapsed time (s)")
    ax.set_ylabel("cumulative bounces (summed over nodes)")
    ax.grid(True, alpha=0.3)

    lines = {}
    for short in METRIC_ORDER:
        (line,) = ax.plot([], [], label=short, linewidth=2)
        lines[short] = line
    ax.legend(loc="upper left")
    return fig, ax, lines


def _redraw(ax, lines, poller):
    """Push the latest poller snapshot into the line artists and rescale."""
    times, series = poller.snapshot()
    for short in METRIC_ORDER:
        lines[short].set_data(times, series[short])
    if times:
        ax.set_xlim(0, max(times[-1], 1))
        ymax = max((max(series[s]) for s in METRIC_ORDER if series[s]), default=1)
        ax.set_ylim(0, max(ymax * 1.1, 1))
    return list(lines.values())


def run_live_plot(poller, args, stop_event):
    """Block on an interactive matplotlib window that redraws live."""
    import matplotlib.pyplot as plt
    from matplotlib.animation import FuncAnimation

    fig, ax, lines = _make_figure(args)
    ani = FuncAnimation(fig, lambda _frame: _redraw(ax, lines, poller),
                        interval=1000, cache_frame_data=False)
    fig._leader_demo_ani = ani  # noqa: SLF001 - retain a strong ref

    fig.canvas.mpl_connect("close_event", lambda _event: stop_event.set())
    if args.duration > 0:
        timer = fig.canvas.new_timer(interval=int(args.duration * 1000))
        timer.add_callback(plt.close, fig)
        timer.start()
    try:
        plt.show()
    finally:
        stop_event.set()


def run_headless_plot(poller, args, stop_event):
    """No GUI display: repeatedly render the chart to a PNG the user can open.

    VS Code (and most image viewers) reload a PNG when it changes on disk, so
    saving after every update gives a "live" chart even in a headless shell.
    """
    import matplotlib
    matplotlib.use("Agg")

    fig, ax, lines = _make_figure(args)
    deadline = time.time() + args.duration if args.duration > 0 else None

    print("No GUI display detected -> saving a live-updating chart to {}".format(args.out), flush=True)
    print("   Open it in VS Code; it refreshes on every update ({}).".format(
        "running {:.0f}s".format(args.duration) if args.duration > 0 else "until Ctrl-C"), flush=True)
    try:
        while not stop_event.is_set() and (deadline is None or time.time() < deadline):
            _redraw(ax, lines, poller)
            fig.savefig(args.out, dpi=100)
            stop_event.wait(1.0)
    finally:
        _redraw(ax, lines, poller)
        fig.savefig(args.out, dpi=100)
        stop_event.set()
        print("Final chart saved to {}".format(args.out), flush=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="Live matplotlib demo of leader-aware routing for strongly-consistent tables.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--routing-v2", choices=["on", "off"], default="on",
        help="'on' negotiates TABLETS_ROUTING_V2 (leader-aware); 'off' forces V1 fallback.",
    )
    parser.add_argument(
        "--scylla-build-dir",
        help="Path to the ScyllaDB build/<mode> dir (must contain scylla under it).",
    )
    parser.add_argument("--nodes", type=int, default=6,
                        help="Number of nodes, spread across --racks (keep >= 2 per rack for node moves).")
    parser.add_argument("--rf", type=int, default=3, help="Replication factor of the keyspace.")
    parser.add_argument("--racks", type=int, default=3,
                        help="Number of racks to spread nodes across (set equal to --rf).")
    parser.add_argument("--tablets", type=int, default=100, help="Initial tablet count for the keyspace.")
    parser.add_argument("--keyspace", default="leader_demo", help="Keyspace name.")
    parser.add_argument("--table", default="t", help="Table name.")
    parser.add_argument("--poll-interval", type=float, default=5.0, help="Seconds between metric scrapes.")
    parser.add_argument("--writers", type=int, default=2, help="Number of concurrent writer threads.")
    parser.add_argument("--readers", type=int, default=2, help="Number of concurrent reader threads.")
    parser.add_argument("--key-space-size", type=int, default=1_000_000, help="Range of partition keys used.")
    parser.add_argument("--smp", type=int, default=2, help="Shards per node (SCYLLA_EXT_OPTS --smp).")
    parser.add_argument("--memory", default="1G", help="Memory per node (SCYLLA_EXT_OPTS --memory).")
    parser.add_argument("--cluster-name", default="leader_demo", help="ccm cluster name.")
    parser.add_argument("--keep-cluster", action="store_true", help="Do not stop/remove the cluster on exit.")
    parser.add_argument("--duration", type=float, default=60.0,
                        help="Seconds to drive load; 0 runs until the window is closed or Ctrl-C.")
    parser.add_argument("--out", default=None,
                        help="Headless PNG path (default: leader_awareness_routing_<mode>.png).")
    parser.add_argument("--headless", action="store_true",
                        help="Force PNG output even if a GUI display is available.")
    parser.add_argument("--churn-interval", type=float, default=2.0,
                        help="Seconds between leader-churn sweeps; 0 disables leader churn.")
    parser.add_argument("--churn-fraction", type=float, default=0.1,
                        help="Fraction of tablets whose leader is changed each sweep (0-1).")
    parser.add_argument("--churn-concurrency", type=int, default=16,
                        help="Max leader stepdowns issued concurrently per sweep.")
    parser.add_argument("--stepdown-timeout", type=float, default=10.0,
                        help="Per-request REST timeout in seconds for leader lookup / stepdown.")
    parser.add_argument("--seed", type=int, default=1,
                        help="RNG seed for choosing which tablets to churn (reproducible runs).")
    parser.add_argument("--api-port", type=int, default=10000,
                        help="ScyllaDB REST API port (ccm default 10000).")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.out is None:
        args.out = "leader_awareness_routing_{}.png".format(args.routing_v2)

    if not os.path.exists(os.path.join(args.scylla_build_dir, "scylla")):
        sys.exit("ScyllaDB binary not found under {} -- pass --scylla-build-dir".format(args.scylla_build_dir))

    if args.routing_v2 == "off":
        disable_routing_v2()
        print(">>> TABLETS_ROUTING_V2 DISABLED: driver will fall back to token-aware V1.", flush=True)
    else:
        print(">>> TABLETS_ROUTING_V2 ENABLED: driver routes to the tablet leader.", flush=True)

    work_dir = tempfile.mkdtemp(prefix="leader_demo_ccm_")
    stop_event = threading.Event()
    ccm_cluster = None
    driver_cluster = None
    write_counters = {"ok": 0, "err": 0}
    read_counters = {"ok": 0, "err": 0}
    churn_counters = {"stepdowns": 0, "err": 0}

    try:
        ccm_cluster = build_ccm_cluster(args, work_dir)
        node_addresses = [node.address() for node in ccm_cluster.nodelist()]
        contact_point = node_addresses[0]
        print("Cluster up. Nodes: {}".format(", ".join(node_addresses)), flush=True)

        driver_cluster, session = connect_driver(contact_point)
        create_schema(session, args)
        print("Keyspace '{}' (RF={}, {} tablets, consistency=global) ready.".format(
            args.keyspace, args.rf, args.tablets), flush=True)

        insert = session.prepare(
            "INSERT INTO {ks}.{tbl} (pk, v) VALUES (?, ?)".format(ks=args.keyspace, tbl=args.table))
        select = session.prepare(
            "SELECT v FROM {ks}.{tbl} WHERE pk = ?".format(ks=args.keyspace, tbl=args.table))

        threads = []
        for i in range(args.writers):
            threads.append(Workload("writer-{}".format(i), session, insert,
                                    args.key_space_size, stop_event, write_counters,
                                    params_fn=lambda pk: (pk, pk)))
        for i in range(args.readers):
            threads.append(Workload("reader-{}".format(i), session, select,
                                    args.key_space_size, stop_event, read_counters,
                                    params_fn=lambda pk: (pk,)))

        poller = MetricsPoller(node_addresses, args.poll_interval, stop_event)

        for t in threads:
            t.start()
        poller.start()

        if args.churn_interval > 0:
            host_addr = {str(h.host_id): h.address
                         for h in driver_cluster.metadata.all_hosts() if h.host_id}
            churner = LeaderChurner(session, contact_point, host_addr, args, stop_event, churn_counters)
            churner.start()
            print("Forcing a Raft leader change on a random {:.0%} of tablets every {:.0f}s "
                  "(auto-balancing disabled).".format(
                      args.churn_fraction, args.churn_interval), flush=True)

        if (not args.headless) and gui_display_available():
            print("Driving QUORUM writes+reads. Close the plot window (or Ctrl-C) to stop.", flush=True)
            run_live_plot(poller, args, stop_event)
        else:
            print("Driving QUORUM writes+reads for {:.0f}s ...".format(args.duration), flush=True)
            run_headless_plot(poller, args, stop_event)

    except KeyboardInterrupt:
        print("\nInterrupted.", flush=True)
    finally:
        stop_event.set()
        time.sleep(0.5)
        print("writes ok={} err={} | reads ok={} err={} | leader-stepdowns={} (err={})".format(
            write_counters["ok"], write_counters["err"],
            read_counters["ok"], read_counters["err"],
            churn_counters["stepdowns"], churn_counters["err"]), flush=True)
        if driver_cluster is not None:
            try:
                driver_cluster.shutdown()
            except Exception:  # noqa: BLE001
                pass
        if ccm_cluster is not None and not args.keep_cluster:
            print("Stopping and removing the ccm cluster ...", flush=True)
            try:
                ccm_cluster.stop(gently=False)
                ccm_cluster.remove()
            except Exception as exc:  # noqa: BLE001
                print("Cluster teardown error: {}".format(exc), flush=True)
        if not args.keep_cluster:
            shutil.rmtree(work_dir, ignore_errors=True)
        elif ccm_cluster is not None:
            print("Left cluster '{}' running (dir: {}).".format(args.cluster_name, work_dir), flush=True)


if __name__ == "__main__":
    main()
