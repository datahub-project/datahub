#!/usr/bin/env python3
"""Measure snapshot reloads in the Lineage Graph Service while walks run.

Client threads loop impact walks (POST /v1/impact, binary response read in full) over the bench
start nodes. Meanwhile the script publishes the current snapshot again under a new name every
--every seconds, exactly as the builder publishes (copy the directory under a temporary name,
rename it into place, then replace LATEST with a rename), so each publish is a real reload of a
just-written snapshot. After the run it reads the service's reload history from /stats.

Reports, per run:
  - every walk: start, duration, total, whether it matched expected_total, errors;
  - per reload: map, warm and drain times (from /stats), RSS before and after warming;
  - walk latency p50/p99/max for walks that overlapped a reload window vs the rest.

Stdlib only. Output: walks.csv, reloads.json, rss.csv, summary.md in --out.
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import statistics
import subprocess
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, List

RULES = [{"entityType": "dataset", "relationshipType": "DownstreamOf", "direction": "INCOMING",
          "opposingType": "dataset"}]


def get(url: str) -> Dict[str, Any]:
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.loads(r.read())


def walk(server: str, urn: str) -> Dict[str, Any]:
    body = json.dumps({"urn": urn, "rules": RULES, "maxHops": 1000, "maxRelations": 0,
                       "partialResults": True}).encode()
    req = urllib.request.Request(f"{server}/v1/impact", data=body,
                                 headers={"Content-Type": "application/json"}, method="POST")
    start = time.time()
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            size = len(r.read())
            total = int(r.headers["X-Total"])
            walk_ms = int(r.headers["X-Walk-Micros"]) / 1000
        error = ""
    except Exception as e:  # record and keep going
        size, total, walk_ms, error = 0, -1, 0.0, repr(e)[:200]
    return {"start": start, "ms": (time.perf_counter() - t0) * 1000, "walk_ms": walk_ms,
            "bytes": size, "total": total, "error": error}


def publish_copy(root: Path, source: Path) -> str:
    name = str(int(time.time() * 1000))
    tmp = root / f"{name}.tmp"
    shutil.copytree(source, tmp)
    tmp.rename(root / name)
    latest_tmp = root / "LATEST.tmp"
    latest_tmp.write_text(name + "\n")
    latest_tmp.rename(root / "LATEST")
    # Keep the newest two, as the builder does.
    dirs = sorted((d for d in root.iterdir() if d.is_dir() and d.name.isdigit()),
                  key=lambda d: int(d.name), reverse=True)
    for d in dirs[2:]:
        shutil.rmtree(d)
    return name


def pct(values: List[float], p: float) -> float:
    s = sorted(values)
    return s[min(len(s) - 1, int(round(p / 100 * (len(s) - 1))))] if s else float("nan")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--server", default="http://localhost:8090")
    ap.add_argument("--snapshot-root", type=Path, required=True)
    ap.add_argument("--start-nodes", type=Path, required=True)
    ap.add_argument("--clients", type=int, default=2)
    ap.add_argument("--reloads", type=int, default=10)
    ap.add_argument("--every", type=float, default=4.0, help="seconds between publishes")
    ap.add_argument("--window", type=float, default=1.0,
                    help="seconds after a reload's publish counted as 'during reload'")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    with args.start_nodes.open() as f:
        starts = [(r["urn"], int(r["expected_total"]), f"{r['family']}:{r['note']}")
                  for r in csv.DictReader(f)]
    latest = (args.snapshot_root / "LATEST").read_text().strip()
    source = args.snapshot_root / latest
    stats_before = get(f"{args.server}/stats")
    known = {r["snapshot"] for r in stats_before["reloads"]}

    stop = threading.Event()
    walks: List[Dict[str, Any]] = []
    lock = threading.Lock()
    port = args.server.rsplit(":", 1)[-1].strip("/")
    pid = subprocess.run(["lsof", "-nP", "-t", f"-iTCP:{port}", "-sTCP:LISTEN"],
                         capture_output=True, text=True).stdout.split()[0]
    rss_samples: List[tuple] = []

    def sample_rss() -> None:
        # Resident memory every 100 ms, to see whether two snapshots are ever resident together.
        while not stop.is_set():
            out = subprocess.run(["ps", "-o", "rss=", "-p", pid], capture_output=True, text=True).stdout
            if out.strip():
                rss_samples.append((time.time(), int(out) * 1024))
            time.sleep(0.1)

    def client(cid: int) -> None:
        i = cid
        while not stop.is_set():
            urn, expected, label = starts[i % len(starts)]
            i += 1
            w = walk(args.server, urn)
            w.update({"client": cid, "start_node": label, "expected": expected,
                      "ok": w["total"] == expected and not w["error"]})
            with lock:
                walks.append(w)

    threads = [threading.Thread(target=client, args=(c,), daemon=True) for c in range(args.clients)]
    threads.append(threading.Thread(target=sample_rss, daemon=True))
    for t in threads:
        t.start()
    time.sleep(args.every)
    published = []
    for _ in range(args.reloads):
        published.append(publish_copy(args.snapshot_root, source if source.exists()
                                      else args.snapshot_root / published[-1]))
        time.sleep(args.every)
    stop.set()
    for t in threads:
        t.join()
    time.sleep(2)  # let the last warm finish
    stats_after = get(f"{args.server}/stats")
    reloads = [r for r in stats_after["reloads"] if r["snapshot"] not in known]
    reloads.sort(key=lambda r: r["startedAt"])
    (args.out / "reloads.json").write_text(json.dumps(reloads, indent=1))

    with (args.out / "walks.csv").open("w", newline="") as f:
        cols = ["client", "start_node", "start", "ms", "walk_ms", "bytes", "total", "expected", "ok", "error"]
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for row in walks:
            w.writerow({k: row[k] for k in cols})

    windows = [(r["startedAt"] / 1000, r["startedAt"] / 1000 + args.window) for r in reloads]

    def during(w: Dict[str, Any]) -> bool:
        s, e = w["start"], w["start"] + w["ms"] / 1000
        return any(s < we and e > ws for ws, we in windows)

    def row(label: str, ws: List[Dict[str, Any]]) -> str:
        ms = [w["ms"] for w in ws]
        return (f"| {label} | {len(ws)} | {sum(not w['ok'] for w in ws)} | {pct(ms, 50):.1f} | "
                f"{pct(ms, 99):.1f} | {max(ms) if ms else float('nan'):.1f} |")

    def stat(key: str) -> str:
        v = [r[key] for r in reloads if key in r]
        return f"{statistics.median(v):.0f} (max {max(v):.0f})" if v else "n/a"

    big = [w for w in walks if w["expected"] >= 100_000]
    lines = [
        "# Reload under load",
        "",
        f"{args.clients} clients looping {len(starts)} start nodes; {len(reloads)} reloads of a "
        f"{reloads[0]['snapshotBytes'] / 2**20:.1f} MiB snapshot every {args.every:g} s "
        f"(of {args.reloads} published). Walk times are client wall time (walk + encode + transfer).",
        "",
        "| Walks | Count | Failed or wrong total | p50 ms | p99 ms | max ms |",
        "|---|---:|---:|---:|---:|---:|",
        row("All", walks),
        row(f"Overlapping a reload (first {args.window:g} s)", [w for w in walks if during(w)]),
        row("Not overlapping", [w for w in walks if not during(w)]),
        row("Large roots (>= 100k) overlapping", [w for w in big if during(w)]),
        row("Large roots (>= 100k) not overlapping", [w for w in big if not during(w)]),
        "",
        "| Reload step | Median ms (max) |",
        "|---|---:|",
        f"| Map and publish | {stat('publishMs')} |",
        f"| Warm (touch every page) | {stat('warmMs')} |",
        f"| Drain (until the old snapshot is unmapped) | {stat('drainMs')} |",
        "",
        f"Old snapshot unmapped after every reload: "
        f"{sum('drainMs' in r for r in reloads[:-1])} of {max(0, len(reloads) - 1)} "
        "(the last reload's snapshot is still current).",
        "",
        "RSS (MiB) per reload, before -> after the old snapshot is unmapped -> after warming: "
        + ", ".join(f"{r['rssBeforeBytes'] / 2**20:.0f}->{r.get('rssBeforeWarmBytes', 0) / 2**20:.0f}"
                    f"->{r.get('rssAfterWarmBytes', 0) / 2**20:.0f}" for r in reloads),
        "",
        f"RSS sampled every 100 ms (MiB): min {min(v for _, v in rss_samples) / 2**20:.0f}, "
        f"median {statistics.median(v for _, v in rss_samples) / 2**20:.0f}, "
        f"max {max(v for _, v in rss_samples) / 2**20:.0f}.",
    ]
    with (args.out / "rss.csv").open("w") as f:
        f.write("time,rss_bytes\n")
        f.writelines(f"{t:.3f},{v}\n" for t, v in rss_samples)
    (args.out / "summary.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    return 0 if all(w["ok"] for w in walks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
