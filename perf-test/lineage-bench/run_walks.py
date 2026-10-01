#!/usr/bin/env python3
"""Run serial Impact lineage walks against GMS and turn LINEAGE_BENCH log lines into CSVs.

See PLAN.md §3. Writes walks.csv (with the post-walk search/resolver timers), hops.csv,
slices.csv, esquery.csv and os_stats_{before,after}.json to --out. Stdlib only.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

QUERIES = {
    # Count only: GraphQL resolves no entity fields, so nothing is hydrated (run 1).
    "total": """
query benchWalk($input: SearchAcrossLineageInput!) {
  searchAcrossLineage(input: $input) { total }
}
""",
    # Roughly what the Impact tab asks for on its first page, so the 10 results are hydrated.
    "ui": """
query benchWalk($input: SearchAcrossLineageInput!) {
  searchAcrossLineage(input: $input) {
    total
    searchResults {
      degree
      entity {
        urn
        type
        ... on Dataset { name platform { name } properties { name description } }
      }
    }
  }
}
""",
}
MARKER = "LINEAGE_BENCH "


def walk_input(urn: str) -> Dict[str, Any]:
    return {
        "urn": urn,
        "direction": "DOWNSTREAM",
        "types": ["DATASET"],
        "query": "*",
        "start": 0,
        "count": 10,
        "orFilters": [{"and": [{"field": "degree", "values": ["1", "2", "3+"]}]}],
        "searchFlags": {"skipCache": True},
    }


def graphql(gms: str, auth: str, urn: str, timeout_s: int, shape: str) -> Dict[str, Any]:
    body = json.dumps({"query": QUERIES[shape], "variables": {"input": walk_input(urn)}}).encode()
    req = urllib.request.Request(
        f"{gms}/api/graphql",
        data=body,
        headers={"Content-Type": "application/json", "Authorization": auth},
    )
    start = time.perf_counter()
    status, total, error = 0, None, ""
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            status = resp.status
            payload = json.loads(resp.read())
        if payload.get("errors"):
            error = json.dumps(payload["errors"])[:500]
        data = (payload.get("data") or {}).get("searchAcrossLineage")
        total = data["total"] if data else None
    except urllib.error.HTTPError as e:
        status, error = e.code, e.read().decode(errors="replace")[:500]
    except Exception as e:  # timeouts, connection resets: record and keep going
        error = repr(e)[:500]
    return {"wall_ms": round((time.perf_counter() - start) * 1000), "http_status": status, "total": total, "error": error}


def os_stats(os_url: str) -> Any:
    """Node search/thread-pool stats plus the graph index's state. Latency depends a lot on index
    state (segments, caches), so every run records it."""

    def get(path: str) -> Any:
        with urllib.request.urlopen(f"{os_url}{path}", timeout=30) as r:
            return json.loads(r.read())

    return {
        "nodes": get("/_nodes/stats/indices,thread_pool?filter_path=nodes.*.indices.search,nodes.*.thread_pool.search"),
        "graph_index": get("/graph_service_v1/_stats/docs,segments,merge,refresh,query_cache,request_cache,search"
                           "?filter_path=_all.primaries"),
        "graph_segments": get("/_cat/segments/graph_service_v1?format=json&h=segment,docs.count,size"),
    }


def bench_lines(container: str, since: str) -> List[Dict[str, Any]]:
    out = subprocess.run(
        ["docker", "logs", "--since", since, container], capture_output=True, text=True, check=True
    )
    lines = []
    for line in (out.stdout + out.stderr).splitlines():
        i = line.find(MARKER + "{")
        if i >= 0:
            lines.append(json.loads(line[i + len(MARKER) :]))
    return lines


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return
    fields: List[str] = []
    for r in rows:
        fields += [k for k in r if k not in fields]
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--gms", default="http://localhost:8080")
    ap.add_argument("--opensearch", default="http://localhost:9200")
    ap.add_argument("--gms-container", default="datahub-datahub-gms-quickstart-1")
    ap.add_argument("--client-id", default="__datahub_system")
    ap.add_argument("--client-secret", default="JohnSnowKnowsNothing")
    ap.add_argument("--start-nodes", type=Path, required=True)
    ap.add_argument("--families", default="", help="comma-separated subset (default: all)")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--warmup", type=int, default=3)
    ap.add_argument("--pause", type=float, default=1.0, help="seconds between walks (PIT cleanup)")
    ap.add_argument("--timeout", type=int, default=180)
    ap.add_argument("--query-shape", choices=sorted(QUERIES), default="ui",
                    help="'total' asks only for the count (run 1); 'ui' also hydrates the page of 10")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args(argv)

    auth = f"Basic {args.client_id}:{args.client_secret}"
    with args.start_nodes.open() as f:
        nodes = list(csv.DictReader(f))
    if args.families:
        keep = set(args.families.split(","))
        nodes = [n for n in nodes if n["family"] in keep]
    args.out.mkdir(parents=True, exist_ok=True)

    run_start = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    (args.out / "os_stats_before.json").write_text(json.dumps(os_stats(args.opensearch)))

    warm_urn = next((n["urn"] for n in nodes if n["family"] == "mid"), nodes[0]["urn"])
    harness: List[Dict[str, Any]] = []
    for i in range(args.warmup):
        harness.append({"phase": "warmup", "urn": warm_urn, **graphql(args.gms, auth, warm_urn, args.timeout, args.query_shape)})
        time.sleep(args.pause)

    n_total = len(nodes) * args.repeats
    k = 0
    for rep in range(1, args.repeats + 1):
        for n in nodes:
            k += 1
            res = graphql(args.gms, auth, n["urn"], args.timeout, args.query_shape)
            row = {
                "phase": "measure",
                "repeat": rep,
                "family": n["family"],
                "note": n["note"],
                "urn": n["urn"],
                "expected_total": int(n["expected_total"]),
                "expected_hops": int(n["expected_hops"]),
                **res,
            }
            harness.append(row)
            ok = "ok" if res["total"] == row["expected_total"] else "MISMATCH"
            print(
                f"[{k}/{n_total}] {n['family']}:{n['note']} rep{rep} "
                f"{res['wall_ms']} ms total={res['total']} expected={row['expected_total']} {ok} {res['error'][:80]}",
                flush=True,
            )
            time.sleep(args.pause)

    time.sleep(3)  # let the last log lines flush
    (args.out / "os_stats_after.json").write_text(json.dumps(os_stats(args.opensearch)))

    lines = bench_lines(args.gms_container, run_start)
    gms_walks = [l for l in lines if l["t"] == "walk"]
    # Walks are serial, so the i-th GMS walk line belongs to the i-th harness call.
    if len(gms_walks) != len(harness):
        print(f"WARNING: {len(gms_walks)} GMS walk lines for {len(harness)} harness walks", file=sys.stderr)
    searches = {l["walk_id"]: l for l in lines if l["t"] == "search"}
    resolvers = {l["walk_id"]: l for l in lines if l["t"] == "resolver"}
    walk_meta: Dict[str, Dict[str, Any]] = {}
    walks_out = []
    for h, g in zip(harness, gms_walks):
        if g["start_urn"] != h["urn"]:
            print(f"WARNING: walk order mismatch at {h['urn']} vs {g['start_urn']}", file=sys.stderr)
        if h["phase"] != "measure":
            continue
        walk_meta[g["walk_id"]] = {"family": h["family"], "note": h["note"], "repeat": h["repeat"]}
        walks_out.append(
            {
                "walk_id": g["walk_id"],
                **{k2: h[k2] for k2 in ("family", "note", "repeat", "urn", "expected_total", "expected_hops")},
                "wall_ms": h["wall_ms"],
                "http_status": h["http_status"],
                "total": h["total"],
                "gms_total_rels": g["total_rels"],
                "gms_hops": g["hops"],
                "gms_walk_ms": g["total_ms"],
                "truncated": g["truncated"],
                "status": g["status"],
                "query_shape": args.query_shape,
                **{f"search_{k2}": v for k2, v in searches.get(g["walk_id"], {}).items() if k2 not in ("t", "walk_id")},
                **{f"resolver_{k2}": v for k2, v in resolvers.get(g["walk_id"], {}).items() if k2 not in ("t", "walk_id")},
                "error": h["error"],
            }
        )
    missing = [w["walk_id"] for w in walks_out if "search_total_ms" not in w or "resolver_total_ms" not in w]
    if missing:
        print(f"WARNING: {len(missing)} walks have no search/resolver line (older GMS image?)", file=sys.stderr)

    def tag(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [{**walk_meta[r["walk_id"]], **{k2: v for k2, v in r.items() if k2 != "t"}} for r in rows if r["walk_id"] in walk_meta]

    write_csv(args.out / "walks.csv", walks_out)
    write_csv(args.out / "hops.csv", tag([l for l in lines if l["t"] == "hop"]))
    write_csv(args.out / "slices.csv", tag([l for l in lines if l["t"] == "slice"]))
    write_csv(args.out / "esquery.csv", tag([l for l in lines if l["t"] == "esquery"]))

    bad = [w for w in walks_out if w["total"] != w["expected_total"]]
    trunc = [w for w in walks_out if w["truncated"] or w["status"] != "ok"]
    print(f"\n{len(walks_out)} walks, {len(bad)} count mismatches, {len(trunc)} truncated/failed -> {args.out}")
    for w in bad + [t for t in trunc if t not in bad]:
        print(f"  {w['family']}:{w['note']} rep{w['repeat']} total={w['total']} expected={w['expected_total']} "
              f"hops={w['gms_hops']}/{w['expected_hops']} truncated={w['truncated']} status={w['status']}")
    return 0 if not bad and not trunc else 1


if __name__ == "__main__":
    sys.exit(main())
