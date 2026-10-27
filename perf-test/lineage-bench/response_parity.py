#!/usr/bin/env python3
"""Capture full searchAcrossLineage responses from GMS, and compare two captures.

Experiment 2 moves the lightning search (filters, page, facets) into the in-memory lineage graph
server and must not change a single byte of the GraphQL response. Capture once per GMS
configuration, then compare:

  response_parity.py capture --label pushdown_on  --out results/exp2/NN_parity/pushdown_on.jsonl
  (restart GMS with LINEAGE_GRAPH_SERVER_SEARCH_PUSHDOWN=false)
  response_parity.py capture --label pushdown_off --out results/exp2/NN_parity/pushdown_off.jsonl
  response_parity.py compare --mode exact results/.../pushdown_off.jsonl results/.../pushdown_on.jsonl

--mode exact: every response must be identical (canonical JSON of `data`, and `errors`).
--mode sets:  for comparing against the OpenSearch backend, whose walk returns results in a
different order: total, facet value counts, and the set of URNs when the page holds them all.

Every start node in --start-nodes runs every case in CASES (or --cases). Stdlib only.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

QUERY = """
query parity($input: SearchAcrossLineageInput!) {
  searchAcrossLineage(input: $input) {
    start
    count
    total
    lineageSearchPath
    isPartial
    freshness { cached }
    facets {
      field
      displayName
      entity { urn }
      aggregations { value count displayName entity { urn } }
    }
    searchResults {
      degree
      degrees
      explored
      truncatedChildren
      ignoredAsHop
      matchedFields { name value }
      paths { path { urn } }
      entity { urn type }
    }
  }
}
"""

ALL_DEGREES = {"field": "degree", "values": ["1", "2", "3+"]}
SNOWFLAKE = {"field": "platform", "values": ["urn:li:dataPlatform:snowflake"]}

# name -> (types, start, count, extra and-criteria, degree criterion)
CASES: Dict[str, Dict[str, Any]] = {
    "ui": {"types": ["DATASET"], "start": 0, "count": 10, "and": []},
    "filtered50": {"types": ["DATASET"], "start": 0, "count": 50, "and": [SNOWFLAKE]},
    "page2": {"types": ["DATASET"], "start": 10, "count": 10, "and": []},
    "deep_page": {"types": ["DATASET"], "start": 1000, "count": 20, "and": []},
    "origin": {"types": ["DATASET"], "start": 0, "count": 10, "and": [{"field": "origin", "values": ["PROD"]}]},
    "not_snowflake": {"types": ["DATASET"], "start": 0, "count": 10,
                      "and": [{**SNOWFLAKE, "negated": True}]},
    "contain": {"types": ["DATASET"], "start": 0, "count": 10,
                "and": [{"field": "platform", "values": ["snow"], "condition": "CONTAIN"}]},
    "degree_1_2": {"types": ["DATASET"], "start": 0, "count": 10, "and": [],
                   "degree": {"field": "degree", "values": ["1", "2"]}},
    "all_types": {"types": None, "start": 0, "count": 10, "and": []},
}


def walk_input(urn: str, case: Dict[str, Any]) -> Dict[str, Any]:
    inp: Dict[str, Any] = {
        "urn": urn,
        "direction": "DOWNSTREAM",
        "query": "*",
        "start": case["start"],
        "count": case["count"],
        "orFilters": [{"and": [case.get("degree", ALL_DEGREES)] + case["and"]}],
        "searchFlags": {"skipCache": True},
    }
    if case["types"] is not None:
        inp["types"] = case["types"]
    return inp


def call(gms: str, auth: str, urn: str, case: Dict[str, Any], timeout: int) -> Dict[str, Any]:
    body = json.dumps({"query": QUERY, "variables": {"input": walk_input(urn, case)}}).encode()
    req = urllib.request.Request(f"{gms}/api/graphql", data=body,
                                 headers={"Content-Type": "application/json", "Authorization": auth})
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            payload = json.loads(r.read())
            status = r.status
    except urllib.error.HTTPError as e:
        payload, status = {"http_error": e.read().decode(errors="replace")[:2000]}, e.code
    except Exception as e:  # timeouts, resets: record and keep going
        payload, status = {"exception": repr(e)[:2000]}, 0
    return {"status": status, "wall_ms": round((time.perf_counter() - t0) * 1000), "payload": payload}


def capture(args: argparse.Namespace) -> int:
    auth = f"Basic {args.client_id}:{args.client_secret}"
    with args.start_nodes.open() as f:
        nodes = list(csv.DictReader(f))
    cases = args.cases.split(",") if args.cases else list(CASES)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with args.out.open("w") as out:
        for node in nodes:
            for name in cases:
                res = call(args.gms, auth, node["urn"], CASES[name], args.timeout)
                data = (res["payload"].get("data") or {}).get("searchAcrossLineage") if isinstance(res["payload"], dict) else None
                out.write(json.dumps({"label": args.label, "case": name, "urn": node["urn"],
                                      "family": node["family"], "note": node["note"], **res}) + "\n")
                n += 1
                total = data["total"] if data else None
                print(f"[{n}] {node['family']}:{node['note']} {name} {res['wall_ms']} ms total={total} "
                      f"path={data.get('lineageSearchPath') if data else None} status={res['status']}", flush=True)
    print(f"{n} responses -> {args.out}")
    return 0


def load(path: Path) -> Dict[tuple, Dict[str, Any]]:
    rows = {}
    for line in path.read_text().splitlines():
        r = json.loads(line)
        rows[(r["urn"], r["case"])] = r
    return rows


def canonical(payload: Any) -> str:
    return json.dumps(payload, sort_keys=False, separators=(",", ":"))


def sets_view(payload: Dict[str, Any]) -> Any:
    data = (payload.get("data") or {}).get("searchAcrossLineage")
    if data is None:
        return {"errors": bool(payload.get("errors"))}
    facets = {f["field"]: sorted((a["value"], a["count"]) for a in f["aggregations"]) for f in data["facets"] or []}
    view: Dict[str, Any] = {"total": data["total"], "facets": facets}
    if data["total"] <= data["count"]:
        view["urns"] = sorted(r["entity"]["urn"] for r in data["searchResults"])
    return view


def compare(args: argparse.Namespace) -> int:
    a, b = load(args.a), load(args.b)
    keys = sorted(set(a) | set(b))
    report: List[Dict[str, Any]] = []
    for k in keys:
        ra, rb = a.get(k), b.get(k)
        if ra is None or rb is None:
            report.append({"urn": k[0], "case": k[1], "problem": "missing in " + ("a" if ra is None else "b")})
            continue
        if args.mode == "exact":
            same = canonical(ra["payload"]) == canonical(rb["payload"])
        else:
            same = sets_view(ra["payload"]) == sets_view(rb["payload"])
        if not same:
            report.append({"urn": k[0], "case": k[1], "problem": "differs",
                           "a": ra["payload"] if args.mode == "exact" else sets_view(ra["payload"]),
                           "b": rb["payload"] if args.mode == "exact" else sets_view(rb["payload"])})
    errors_a = sum(1 for r in a.values() if r["status"] != 200 or r["payload"].get("errors"))
    summary = {"mode": args.mode, "a": str(args.a), "b": str(args.b), "compared": len(keys),
               "mismatches": len(report), "responses_with_errors_in_a": errors_a}
    print(json.dumps(summary, indent=2))
    for r in report[:10]:
        print(f"  MISMATCH {r['case']} {r['urn']} {r['problem']}")
    if args.report:
        args.report.write_text(json.dumps({"summary": summary, "mismatches": report}, indent=1))
    return 1 if report else 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("capture")
    c.add_argument("--gms", default="http://localhost:8080")
    c.add_argument("--client-id", default="__datahub_system")
    c.add_argument("--client-secret", default="JohnSnowKnowsNothing")
    c.add_argument("--start-nodes", type=Path, default=Path(__file__).parent / "dataset/v1/start_nodes.csv")
    c.add_argument("--cases", default="", help=f"comma-separated subset of {','.join(CASES)}")
    c.add_argument("--timeout", type=int, default=300)
    c.add_argument("--label", required=True)
    c.add_argument("--out", type=Path, required=True)
    d = sub.add_parser("compare")
    d.add_argument("--mode", choices=["exact", "sets"], default="exact")
    d.add_argument("--report", type=Path)
    d.add_argument("a", type=Path)
    d.add_argument("b", type=Path)
    args = ap.parse_args(argv)
    return capture(args) if args.cmd == "capture" else compare(args)


if __name__ == "__main__":
    sys.exit(main())
