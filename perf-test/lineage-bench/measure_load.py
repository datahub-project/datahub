#!/usr/bin/env python3
"""Measure how long the Lineage Graph Service takes to start from a snapshot, and its memory.

Starts the service (build/install/lineage-graph-server) as a local process --starts times against
an existing snapshot root (build one with bin/lineage-snapshot-builder, or with the core bench's
--publish from a dataset CSV). For each start it polls /ready every 50 ms and records the wall
time from process launch to READY, the service's own map and warm timings (/stats), the graph's
exact bytes per segment, JVM heap and process RSS.

Run `sudo purge` (macOS) before a start to measure a cold page cache; by default the snapshot's
pages are whatever the OS has cached.

Writes load.csv (one row per start) and stats_<n>.json to --out. Stdlib only.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import signal
import subprocess
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, List

HERE = Path(__file__).resolve().parent
DEFAULT_BIN = HERE.parents[1] / "metadata-jobs/lineage-graph-server/build/install/lineage-graph-server/bin/lineage-graph-server"


def get(url: str) -> Dict[str, Any]:
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read())


def wait_ready(server: str, proc: subprocess.Popen, timeout_s: float) -> float:
    start = time.perf_counter()
    while time.perf_counter() - start < timeout_s:
        if proc.poll() is not None:
            raise RuntimeError(f"service exited with {proc.returncode}")
        try:
            r = get(f"{server}/ready")
            if r["state"] == "READY":
                return time.perf_counter() - start
            if r["state"] == "FAILED":
                raise RuntimeError(f"service failed: {r.get('failure')}")
        except OSError:
            pass  # not listening yet, or 503 while waiting for a snapshot
        time.sleep(0.05)
    raise TimeoutError("service not READY in time")


def measure(i: int, binary: Path, env: Dict[str, str], server: str, out: Path, settle_s: float) -> Dict[str, Any]:
    proc = subprocess.Popen([str(binary)], env={**os.environ, **env},
                            stdout=(out / f"service_{i}.log").open("w"), stderr=subprocess.STDOUT)
    try:
        ready_s = wait_ready(server, proc, 600)
        time.sleep(settle_s)  # let the background warm finish
        stats = get(f"{server}/stats")
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
    reload = stats["reloads"][-1]
    mem = stats["memory"]
    graph = mem["graphBytes"]
    row = {
        "run": i,
        "ready_wall_ms": round(ready_s * 1000),
        "ready_since_process_start_ms": stats["startup"].get("readySinceProcessStartMs"),
        "map_ms": reload.get("mapMs"),
        "warm_ms": reload.get("warmMs"),
        "snapshot_mib": round(reload["snapshotBytes"] / 2**20, 1),
        "nodes": stats.get("nodes"),
        "edges": stats.get("edges"),
        "graph_mib": round(graph["total"] / 2**20, 1),
        "urn_dictionary_mib": round(graph["urnDictionary"] / 2**20, 1),
        "rss_before_map_mib": round(reload["rssBeforeBytes"] / 2**20, 1),
        "rss_after_warm_mib": round(reload.get("rssAfterWarmBytes", -1) / 2**20, 1),
        "heap_used_mib": round(mem["heapUsedBytes"] / 2**20, 1),
        "heap_committed_mib": round(mem["heapCommittedBytes"] / 2**20, 1),
    }
    (out / f"stats_{i}.json").write_text(json.dumps(stats, indent=2))
    print(json.dumps(row), flush=True)
    return row


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bin", type=Path, default=DEFAULT_BIN)
    ap.add_argument("--snapshot-root", type=Path, required=True)
    ap.add_argument("--server", default="http://localhost:8090")
    ap.add_argument("--java-opts", default="-Xmx1g -XX:+UseG1GC")
    ap.add_argument("--starts", type=int, default=3)
    ap.add_argument("--settle", type=float, default=2.0, help="seconds after READY before reading /stats")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    env = {"LINEAGE_GRAPH_SNAPSHOT_ROOT": str(args.snapshot_root.resolve()),
           "LINEAGE_GRAPH_SERVER_OPTS": args.java_opts,
           "LINEAGE_GRAPH_SERVER_PORT": args.server.rsplit(":", 1)[-1].strip("/")}
    rows: List[Dict[str, Any]] = [measure(i, args.bin, env, args.server, args.out, args.settle)
                                  for i in range(1, args.starts + 1)]
    with (args.out / "load.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"\nwrote {args.out / 'load.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
