#!/usr/bin/env python3
"""Check that GMS lineage results match the in-memory lineage graph server, node by node.

For every start node whose walk fits in one page (expected_total <= --max-results), fetch:
  A. GMS searchAcrossLineage (whatever backend GMS currently uses), and
  B. the graph server's POST /v1/impact?format=json&full=true,
then compare per URN: presence, degree, degrees and explored. Paths are not compared for
equality: the OpenSearch walk processes a hop across parallel slices, so which parent reaches a
child first varies from run to run. Instead every path on both sides must be valid: start at the
root, end at the URN, have degree + 1 nodes, and follow real edges from dataset/<v>/edges.csv.
Larger walks are checked by total only (against expected_total).

`explored` is compared only for walks of at most --lightning-threshold results: above it, GMS's
LineageSearchService answers "*" queries in "lightning" mode, whose result builder does not copy
`explored` (or `degrees`) from the walk, so GMS reports explored=false for every result whichever
backend walked.

Writes parity.json to --out and exits 1 on any mismatch. Stdlib only.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

QUERY = """
query parity($input: SearchAcrossLineageInput!) {
  searchAcrossLineage(input: $input) {
    total
    searchResults {
      entity { urn }
      degree
      degrees
      explored
      paths { path { urn } }
    }
  }
}
"""
RULES = [{"entityType": "dataset", "relationshipType": "DownstreamOf", "direction": "INCOMING", "opposingType": "dataset"}]


def post(url: str, body: Dict[str, Any], headers: Dict[str, str], timeout: int = 300) -> Any:
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json", **headers})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def gms_walk(gms: str, auth: str, urn: str, count: int) -> Tuple[int, Dict[str, Dict[str, Any]]]:
    inp = {
        "urn": urn, "direction": "DOWNSTREAM", "types": ["DATASET"], "query": "*", "start": 0, "count": count,
        "orFilters": [{"and": [{"field": "degree", "values": ["1", "2", "3+"]}]}],
        "searchFlags": {"skipCache": True},
    }
    payload = post(f"{gms}/api/graphql", {"query": QUERY, "variables": {"input": inp}}, {"Authorization": auth})
    if payload.get("errors"):
        raise RuntimeError(json.dumps(payload["errors"])[:500])
    data = payload["data"]["searchAcrossLineage"]
    out = {}
    for r in data["searchResults"]:
        out[r["entity"]["urn"]] = {
            "degree": r["degree"],
            "degrees": sorted(r.get("degrees") or [r["degree"]]),
            "explored": bool(r.get("explored")),
            "paths": [[e["urn"] for e in p["path"] if e] for p in (r.get("paths") or []) if p],
        }
    return data["total"], out


def server_walk(server: str, urn: str, full: bool) -> Tuple[int, Dict[str, Dict[str, Any]]]:
    body = {"urn": urn, "rules": RULES, "maxHops": 1000, "maxRelations": 0, "partialResults": True}
    data = post(f"{server}/v1/impact?format=json" + ("&full=true" if full else ""), body, {})
    out = {}
    for r in data.get("relationships", []):
        out[r["urn"]] = {
            "degree": r["degree"],
            "degrees": sorted(r["degrees"]),
            "explored": bool(r["explored"]),
            "paths": r["paths"],
        }
    return data["total"], out


def bad_paths(root: str, urn: str, rel: Dict[str, Any], edges: Set[Tuple[str, str]]) -> List[str]:
    problems = []
    if not rel["paths"]:
        problems.append("no path")
    for p in rel["paths"]:
        if p[0] != root or p[-1] != urn:
            problems.append(f"endpoints {p[0]}..{p[-1]}")
        elif len(p) != rel["degree"] + 1 and len(p) - 1 not in rel["degrees"]:
            problems.append(f"length {len(p) - 1} vs degree {rel['degree']}")
        else:
            for parent, child in zip(p, p[1:]):
                if (child, parent) not in edges:
                    problems.append(f"no edge {child} -> {parent}")
                    break
    return problems


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--gms", default="http://localhost:8080")
    ap.add_argument("--graph-server", default="http://localhost:8090")
    ap.add_argument("--dataset", type=Path, default=Path(__file__).resolve().parent / "dataset/v1")
    ap.add_argument("--max-results", type=int, default=10000)
    ap.add_argument("--lightning-threshold", type=int, default=300)
    ap.add_argument("--auth", default="Basic __datahub_system:JohnSnowKnowsNothing")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    with (args.dataset / "start_nodes.csv").open() as f:
        nodes = list(csv.DictReader(f))
    print("loading edges...", flush=True)
    edges: Set[Tuple[str, str]] = set()
    with (args.dataset / "edges.csv").open() as f:
        r = csv.reader(f)
        next(r)
        for down, up in r:
            edges.add((down, up))

    report: List[Dict[str, Any]] = []
    failures = 0
    for n in nodes:
        urn, expected = n["urn"], int(n["expected_total"])
        label = f"{n['family']}:{n['note']}"
        s_total, s_rels = server_walk(args.graph_server, urn, expected <= args.max_results)
        row: Dict[str, Any] = {"node": label, "urn": urn, "expected": expected, "server_total": s_total}
        problems: List[str] = []
        if s_total != expected:
            problems.append(f"server total {s_total} != expected {expected}")
        if expected <= args.max_results:
            g_total, g_rels = gms_walk(args.gms, args.auth, urn, args.max_results)
            row["gms_total"] = g_total
            if set(g_rels) != set(s_rels):
                only_g, only_s = set(g_rels) - set(s_rels), set(s_rels) - set(g_rels)
                problems.append(f"urn sets differ: {len(only_g)} only in GMS, {len(only_s)} only in server")
            same_paths = 0
            for u in set(g_rels) & set(s_rels):
                g, s = g_rels[u], s_rels[u]
                fields = ["degree"]
                if expected <= args.lightning_threshold:
                    fields += ["degrees", "explored"]
                for field in fields:
                    if g[field] != s[field]:
                        problems.append(f"{u} {field}: gms={g[field]} server={s[field]}")
                for side, rel in (("gms", g), ("server", s)):
                    for p in bad_paths(urn, u, rel, edges):
                        problems.append(f"{side} path of {u}: {p}")
                same_paths += sorted(g["paths"]) == sorted(s["paths"])
            row["identical_paths"] = same_paths
            row["compared"] = len(set(g_rels) & set(s_rels))
        else:
            row["compared"] = "total only"
        row["problems"] = problems[:20]
        row["problem_count"] = len(problems)
        failures += bool(problems)
        report.append(row)
        print(f"{label:24} expected={expected:<7} server={s_total:<7} gms={row.get('gms_total', '-'):<7} "
              f"compared={row['compared']} identical_paths={row.get('identical_paths', '-')} "
              f"{'OK' if not problems else 'FAIL: ' + problems[0]}", flush=True)

    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "parity.json").write_text(json.dumps(report, indent=2))
    print(f"\n{len(report)} start nodes, {failures} with differences -> {args.out / 'parity.json'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
