#!/usr/bin/env python3
"""Run serial Impact lineage walks and write CSVs.

--backend gms (default): searchAcrossLineage through GMS GraphQL; LINEAGE_BENCH log lines become
walks.csv (with the post-walk search/resolver timers), hops.csv, slices.csv, esquery.csv, plus
os_stats_{before,after}.json. See PLAN.md §3.

--backend direct: POST /v1/impact on the in-memory lineage graph server, reading the full binary
response; walks.csv gets the same columns (gms_walk_ms is the server's walk time) so
compare_report.py can put the two side by side. Stdlib only.
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
# "filtered50" is the vendor-style workload: the same walk, but a page of 50 with a platform filter.
QUERIES["filtered50"] = QUERIES["ui"]
MARKER = "LINEAGE_BENCH "
WALK_BASE_FIELDS = {"t", "walk_id", "start_urn", "hops", "total_rels", "truncated", "status", "total_ms"}
# Impact (downstream) rules for datasets, as LineageGraphFilters gives them for the bench graph.
DIRECT_RULES = [
    {"entityType": "dataset", "relationshipType": "DownstreamOf", "direction": "INCOMING", "opposingType": "dataset"}
]


def walk_input(urn: str, shape: str = "ui") -> Dict[str, Any]:
    filters = [{"field": "degree", "values": ["1", "2", "3+"]}]
    if shape == "filtered50":
        filters.append({"field": "platform", "values": ["urn:li:dataPlatform:snowflake"]})
    return {
        "urn": urn,
        "direction": "DOWNSTREAM",
        "types": ["DATASET"],
        "query": "*",
        "start": 0,
        "count": 50 if shape == "filtered50" else 10,
        "orFilters": [{"and": filters}],
        "searchFlags": {"skipCache": True},
    }


def search_spec(shape: str) -> Dict[str, Any]:
    """The lightning search GMS sends for a query shape (experiment 2, /v1/impact/search)."""
    filters = None
    if shape == "filtered50":
        filters = json.dumps({"or": [{"and": [{"field": "platform", "values": ["urn:li:dataPlatform:snowflake"],
                                               "condition": "EQUAL", "negated": False}]}]})
    return {"entityTypes": ["dataset"], "degreeValues": ["1", "2", "3+"], "filters": filters,
            "convertSchemaFields": True, "forceLightning": False, "lightningThreshold": 300,
            "from": 0, "size": 50 if shape == "filtered50" else 10}


def direct(server: str, urn: str, timeout_s: int, shape: Optional[str] = None) -> Dict[str, Any]:
    """One walk on the graph server, reading the whole binary response as GMS would. With a query
    shape, the walk plus the lightning search for that shape (/v1/impact/search)."""
    body_obj: Dict[str, Any] = {"urn": urn, "rules": DIRECT_RULES, "maxHops": 1000, "maxRelations": 0,
                                "partialResults": True}
    path = "/v1/impact"
    if shape is not None:
        body_obj["search"] = search_spec(shape)
        path = "/v1/impact/search"
    body = json.dumps(body_obj).encode()
    req = urllib.request.Request(f"{server}{path}", data=body, headers={"Content-Type": "application/json"})
    start = time.perf_counter()
    out: Dict[str, Any] = {"http_status": 0, "total": None, "error": "", "server_walk_ms": None,
                           "hops": None, "server_status": "", "partial": "", "bytes": 0}
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            out["http_status"] = resp.status
            out["bytes"] = len(resp.read())
            h = resp.headers
            out["total"] = int(h["X-Total"])
            out["hops"] = int(h["X-Hops"])
            out["server_status"] = h["X-Status"]
            out["partial"] = h["X-Partial"] == "true"
            out["server_walk_ms"] = round(int(h["X-Walk-Micros"]) / 1000, 2)
            if shape is not None:
                out["walked"] = int(h["X-Walked"])
                out["mode"] = h["X-Mode"]
                out["server_search_ms"] = round(int(h["X-Search-Micros"]) / 1000, 2)
                if out["mode"] == "FULL":  # below the lightning threshold: no matching done on the server
                    out["total"] = int(h["X-Prefiltered"])
    except urllib.error.HTTPError as e:
        out["http_status"], out["error"] = e.code, e.read().decode(errors="replace")[:500]
    except Exception as e:  # timeouts, connection resets: record and keep going
        out["error"] = repr(e)[:500]
    out["wall_ms"] = round((time.perf_counter() - start) * 1000)
    return out


def graphql(gms: str, auth: str, urn: str, timeout_s: int, shape: str) -> Dict[str, Any]:
    body = json.dumps({"query": QUERIES[shape], "variables": {"input": walk_input(urn, shape)}}).encode()
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


def run_meta(args: argparse.Namespace) -> Dict[str, Any]:
    """What this run ran against, so results can be traced back later: the code, the GMS image and
    its lineage settings, and the graph server's state."""

    def sh(*cmd: str) -> str:
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=30).stdout.strip()
        except Exception as e:  # missing docker/git: record why
            return f"<{e!r}>"

    meta: Dict[str, Any] = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "argv": sys.argv,
        "args": vars(args),
        "git_head": sh("git", "rev-parse", "HEAD"),
        "git_branch": sh("git", "rev-parse", "--abbrev-ref", "HEAD"),
        "git_dirty": sh("git", "status", "--short").splitlines(),
    }
    inspect = sh("docker", "inspect", args.gms_container)
    try:
        c = json.loads(inspect)[0]
        env = [e for e in c["Config"]["Env"]
               if e.split("=", 1)[0].startswith(("LINEAGE_", "ELASTICSEARCH_SEARCH_GRAPH", "ELASTICSEARCH_THREAD",
                                                 "CACHE_SEARCH_LINEAGE", "SEARCH_SERVICE", "JAVA_OPTS"))]
        meta["gms"] = {"container": args.gms_container, "image": c["Config"]["Image"], "image_id": c["Image"],
                       "started_at": c["State"]["StartedAt"], "env": env}
    except Exception:
        meta["gms"] = {"container": args.gms_container, "inspect": inspect[:500]}
    try:
        with urllib.request.urlopen(f"{args.graph_server}/stats", timeout=5) as r:
            meta["graph_server_stats"] = json.loads(r.read())
    except Exception as e:
        meta["graph_server_stats"] = f"<unreachable: {e!r}>"
    return meta


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
    ap.add_argument("--backend", choices=["gms", "direct", "direct-search"], default="gms",
                    help="direct-search: the server's walk plus lightning search for --query-shape")
    ap.add_argument("--graph-server", default="http://localhost:8090", help="for --backend direct")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args(argv)
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "run_meta.json").write_text(json.dumps(run_meta(args), indent=1, default=str))
    if args.backend in ("direct", "direct-search"):
        return run_direct(args)

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
                # In-memory walk lines carry their own timings (backend, server_walk_ms, http_ms, ...).
                **{f"walk_{k2}": v for k2, v in g.items() if k2 not in WALK_BASE_FIELDS},
                **{f"search_{k2}": v for k2, v in searches.get(g["walk_id"], {}).items() if k2 not in ("t", "walk_id")},
                **{f"resolver_{k2}": v for k2, v in resolvers.get(g["walk_id"], {}).items() if k2 not in ("t", "walk_id")},
                "error": h["error"],
            }
        )
    if not gms_walks:
        # GMS without the bench hooks (e.g. a plain branch build): keep the harness-side columns only.
        print("WARNING: no LINEAGE_BENCH lines; writing harness-only walks.csv", file=sys.stderr)
        for h in harness:
            if h["phase"] != "measure":
                continue
            walks_out.append(
                {
                    **{k2: h[k2] for k2 in ("family", "note", "repeat", "urn", "expected_total", "expected_hops")},
                    "wall_ms": h["wall_ms"],
                    "http_status": h["http_status"],
                    "total": h["total"],
                    "truncated": "",
                    "status": "ok" if h["http_status"] == 200 and not h["error"] else "error",
                    "query_shape": args.query_shape,
                    "error": h["error"],
                }
            )
    missing = [w.get("walk_id") for w in walks_out if "search_total_ms" not in w or "resolver_total_ms" not in w]
    if missing and gms_walks:
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
              f"hops={w.get('gms_hops', '')}/{w['expected_hops']} truncated={w['truncated']} status={w['status']}")
    return 0 if not bad and not trunc else 1


def run_direct(args: argparse.Namespace) -> int:
    with args.start_nodes.open() as f:
        nodes = list(csv.DictReader(f))
    if args.families:
        keep = set(args.families.split(","))
        nodes = [n for n in nodes if n["family"] in keep]
    args.out.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(f"{args.graph_server}/stats", timeout=30) as r:
        (args.out / "server_stats_before.json").write_text(r.read().decode())

    shape = args.query_shape if args.backend == "direct-search" else None
    for _ in range(args.warmup):  # JIT warm-up on every start node, not recorded
        for n in nodes:
            direct(args.graph_server, n["urn"], args.timeout, shape)

    walks_out: List[Dict[str, Any]] = []
    n_total = len(nodes) * args.repeats
    k = 0
    for rep in range(1, args.repeats + 1):
        for n in nodes:
            k += 1
            res = direct(args.graph_server, n["urn"], args.timeout, shape)
            row = {
                **{k2: n[k2] for k2 in ("family", "note", "urn")},
                "repeat": rep,
                "expected_total": int(n["expected_total"]),
                "expected_hops": int(n["expected_hops"]),
                "wall_ms": res["wall_ms"],
                "http_status": res["http_status"],
                "total": res["total"],
                "gms_total_rels": res["total"],
                "gms_hops": res["hops"],
                "gms_walk_ms": res["server_walk_ms"],
                "truncated": "true" if res["partial"] else "",
                "status": "ok" if res["http_status"] == 200 and res["server_status"] == "COMPLETE" else "error",
                "query_shape": shape or "direct",
                "backend": args.backend,
                "response_bytes": res["bytes"],
                **({"server_search_ms": res.get("server_search_ms"), "mode": res.get("mode"),
                    "walked": res.get("walked")} if shape else {}),
                "error": res["error"],
            }
            walks_out.append(row)
            ok = "ok" if res["total"] == row["expected_total"] else "MISMATCH"
            print(f"[{k}/{n_total}] {n['family']}:{n['note']} rep{rep} {res['wall_ms']} ms "
                  f"(walk {res['server_walk_ms']} ms, {res['bytes']} B) total={res['total']} {ok} {res['error'][:80]}",
                  flush=True)

    with urllib.request.urlopen(f"{args.graph_server}/stats", timeout=30) as r:
        (args.out / "server_stats_after.json").write_text(r.read().decode())
    write_csv(args.out / "walks.csv", walks_out)
    for name in ("hops.csv", "slices.csv", "esquery.csv"):
        (args.out / name).write_text("")
    bad = [w for w in walks_out if w["total"] != w["expected_total"]]
    failed = [w for w in walks_out if w["status"] != "ok"]
    print(f"\n{len(walks_out)} walks, {len(bad)} count mismatches, {len(failed)} failed -> {args.out}")
    for w in bad + [t for t in failed if t not in bad]:
        print(f"  {w['family']}:{w['note']} rep{w['repeat']} total={w['total']} expected={w['expected_total']} {w['error']}")
    return 0 if not bad and not failed else 1


if __name__ == "__main__":
    sys.exit(main())
