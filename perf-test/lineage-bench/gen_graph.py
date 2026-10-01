#!/usr/bin/env python3
"""Generate the lineage-bench dataset in a database-neutral format.

Writes nodes.csv, edges.csv, start_nodes.csv and manifest.json into --out. Ground truth
(expected per-hop sizes for every start node) comes from an in-memory BFS here, never from
a database, so every backend is checked against the same answers. See PLAN.md §1.3.
Stdlib only.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import sys
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

URN_PREFIX = "urn:li:dataset:(urn:li:dataPlatform:snowflake,bench."

# Layer sizes of the prod_like family (layer 0 is the single root). Sized from the largest
# prod tenant: ~56k direct downstreams, ~580k reachable, tapering over ~22 layers.
PROD_LIKE_LAYERS = [56_000, 120_000, 130_000, 100_000, 70_000, 45_000, 25_000, 15_000]
PROD_LIKE_LAYERS += [8_000, 5_000, 3_000, 2_000]
PROD_LIKE_LAYERS += [400, 300, 250, 200, 120, 80, 60, 40, 30, 20]


@dataclass
class Graph:
    urns: List[str] = field(default_factory=list)
    family: List[str] = field(default_factory=list)
    layer: List[int] = field(default_factory=list)
    # (downstream_id, upstream_id): "downstream DownstreamOf upstream"
    edges: List[Tuple[int, int]] = field(default_factory=list)

    def add_node(self, family: str, layer: int, name: str) -> int:
        self.urns.append(f"{URN_PREFIX}{family}.{name},PROD)")
        self.family.append(family)
        self.layer.append(layer)
        return len(self.urns) - 1

    def add_edge(self, downstream: int, upstream: int) -> None:
        self.edges.append((downstream, upstream))


@dataclass
class StartNode:
    node: int
    note: str


def scaled(n: int, scale: float, minimum: int = 1) -> int:
    return max(minimum, int(round(n * scale)))


def build_layered(
    g: Graph,
    rng: random.Random,
    family: str,
    layer_sizes: Sequence[int],
    parents_above: int,
    extra_parents_mean: float = 0.0,
    same_layer_share: float = 0.0,
) -> List[List[int]]:
    """Root plus layers. Each node gets `parents_above` distinct parents in the layer directly
    above, plus on average `extra_parents_mean` more, a `same_layer_share` fraction of which come
    from earlier nodes of its own layer. Parents only from the layer above or the same layer keep
    every node's BFS distance equal to its layer number."""
    layers: List[List[int]] = [[g.add_node(family, 0, "root")]]
    for depth, size in enumerate(layer_sizes, start=1):
        above = layers[-1]
        current: List[int] = []
        for i in range(size):
            node = g.add_node(family, depth, f"l{depth}_{i}")
            k = min(parents_above, len(above))
            parents = set(rng.sample(above, k)) if k < len(above) else set(above)
            extra = int(extra_parents_mean) + (
                1 if rng.random() < extra_parents_mean - int(extra_parents_mean) else 0
            )
            for _ in range(extra):
                if current and rng.random() < same_layer_share:
                    parents.add(rng.choice(current))
                else:
                    parents.add(rng.choice(above))
            for p in parents:
                g.add_edge(node, p)
            current.append(node)
        layers.append(current)
    return layers


def build_tree(g: Graph, family: str, fanout: int, depth: int, prefix: str = "") -> List[List[int]]:
    layers: List[List[int]] = [[g.add_node(family, 0, f"{prefix}root")]]
    for d in range(1, depth + 1):
        current: List[int] = []
        for parent_idx, parent in enumerate(layers[-1]):
            for c in range(fanout):
                node = g.add_node(family, d, f"{prefix}l{d}_{parent_idx * fanout + c}")
                g.add_edge(node, parent)
                current.append(node)
        layers.append(current)
    return layers


def pick_with_children(g: Graph, rng: random.Random, candidates: List[int]) -> int:
    """Random candidate that has at least one downstream, so its walk isn't empty."""
    cset = set(candidates)
    parents = [up for _, up in g.edges if up in cset]
    return rng.choice(sorted(set(parents))) if parents else rng.choice(candidates)


def fam_prod_like(g: Graph, rng: random.Random, scale: float) -> List[StartNode]:
    family = "prod_like"
    sizes = [scaled(n, scale) for n in PROD_LIKE_LAYERS]
    # 1 parent above + 1.2 extra (avg ~2.2 upstreams, like prod), 30% of extras from own layer.
    layers = build_layered(g, rng, family, sizes, 1, 1.2, 0.3)

    # ~1% of nodes in layers >= 4 get a back-edge: a node 1-3 layers up becomes downstream of it.
    # The shallower node keeps its original parent, so hop numbers don't change.
    for depth in range(4, len(layers)):
        for node in layers[depth]:
            if rng.random() < 0.01:
                target_layer = depth - rng.randint(1, 3)
                g.add_edge(rng.choice(layers[target_layer]), node)

    # One ~600-node strongly connected cluster across layers 5-8: chain them in layer order
    # (each step moves down at most one layer, so no BFS distance shrinks), then close the loop
    # with one back-edge.
    pool = [n for d in range(5, 9) for n in layers[d]]
    cluster = sorted(rng.sample(pool, min(scaled(600, scale, 3), len(pool))), key=lambda n: g.layer[n])
    for a, b in zip(cluster, cluster[1:]):
        g.add_edge(b, a)
    g.add_edge(cluster[0], cluster[-1])
    # A bare ring would make walks through the cluster take one hop per node. Add 2 shortcuts per
    # member, only where the downstream end is at most one layer deeper than the upstream end, so
    # hop numbers from the root stay equal to layers.
    for up in cluster:
        for down in rng.sample(cluster, 2):
            if down != up and g.layer[down] <= g.layer[up] + 1:
                g.add_edge(down, up)

    starts = [StartNode(layers[0][0], "root")]
    for d in (1, 2, 4):
        starts.append(StartNode(pick_with_children(g, rng, layers[d]), f"layer{d}"))
    return starts


def fam_hub(g: Graph, rng: random.Random, scale: float) -> List[StartNode]:
    layers = build_tree(g, "hub", scaled(56_000, scale), 1)
    return [StartNode(layers[0][0], "root")]


def fam_deep(g: Graph, rng: random.Random, scale: float) -> List[StartNode]:
    layers = build_layered(g, rng, "deep", [scaled(2_000, scale, 2)] * 26, 2)
    return [StartNode(layers[0][0], "root"), StartNode(pick_with_children(g, rng, layers[13]), "layer13")]


def make_deep100(width: int) -> Callable[[Graph, random.Random, float], List[StartNode]]:
    def fam(g: Graph, rng: random.Random, scale: float) -> List[StartNode]:
        family = f"deep100_w{width}" if width < 1000 else "deep100_w1k"
        w = width if width <= 10 else scaled(width, scale, 10)
        layers = build_layered(g, rng, family, [w] * 100, 1 if w == 1 else 2)
        return [
            StartNode(layers[0][0], "root"),
            StartNode(pick_with_children(g, rng, layers[50]), "layer50"),
            StartNode(pick_with_children(g, rng, layers[75]), "layer75"),
        ]

    return fam


def fam_wide(g: Graph, rng: random.Random, scale: float) -> List[StartNode]:
    layers = build_tree(g, "wide", scaled(50, scale ** (1 / 3), 2), 3)
    return [StartNode(layers[0][0], "root"), StartNode(pick_with_children(g, rng, layers[1]), "layer1")]


def fam_mid(g: Graph, rng: random.Random, scale: float) -> List[StartNode]:
    layers = build_tree(g, "mid", scaled(10, scale ** (1 / 5), 2), 5)
    return [StartNode(layers[0][0], "root"), StartNode(pick_with_children(g, rng, layers[2]), "layer2")]


def fam_small_islands(g: Graph, rng: random.Random, scale: float) -> List[StartNode]:
    roots = [build_tree(g, "small_islands", 3, 2, prefix=f"i{i}_")[0][0] for i in range(scaled(8_000, scale, 20))]
    return [StartNode(r, "root") for r in rng.sample(roots, 20)]


def fam_scc(g: Graph, rng: random.Random, scale: float) -> List[StartNode]:
    n = scaled(50_000, scale, 50)
    nodes = [g.add_node("scc", 0, f"n{i}") for i in range(n)]
    for i in range(n):
        g.add_edge(nodes[(i + 1) % n], nodes[i])  # ring: every node reaches every other
        for j in rng.sample(range(n), 2):
            if j != i:
                g.add_edge(nodes[j], nodes[i])
    return [StartNode(x, "member") for x in rng.sample(nodes, 3)]


FAMILIES: Dict[str, Callable[[Graph, random.Random, float], List[StartNode]]] = {
    "prod_like": fam_prod_like,
    "hub": fam_hub,
    "deep": fam_deep,
    "deep100_w1": make_deep100(1),
    "deep100_w10": make_deep100(10),
    "deep100_w100": make_deep100(100),
    "deep100_w1k": make_deep100(1000),
    "wide": fam_wide,
    "mid": fam_mid,
    "small_islands": fam_small_islands,
    "scc": fam_scc,
}


def dedupe_edges(g: Graph) -> None:
    seen = set()
    out = []
    for e in g.edges:
        if e[0] != e[1] and e not in seen:
            seen.add(e)
            out.append(e)
    g.edges = out


def downstream_adjacency(g: Graph) -> List[List[int]]:
    adj: List[List[int]] = [[] for _ in g.urns]
    for down, up in g.edges:
        adj[up].append(down)
    return adj


def bfs_hops(adj: List[List[int]], start: int) -> List[int]:
    """Number of newly reached nodes at each hop, walking downstream (Impact)."""
    seen = {start}
    frontier = [start]
    per_hop: List[int] = []
    while frontier:
        nxt = []
        for u in frontier:
            for v in adj[u]:
                if v not in seen:
                    seen.add(v)
                    nxt.append(v)
        if nxt:
            per_hop.append(len(nxt))
        frontier = nxt
    return per_hop


def largest_scc(adj: List[List[int]], members: List[int]) -> int:
    """Iterative Tarjan restricted to `members`; returns the largest SCC size."""
    member_set = set(members)
    index: Dict[int, int] = {}
    low: Dict[int, int] = {}
    on_stack = set()
    stack: List[int] = []
    best = 0
    counter = 0
    for root in members:
        if root in index:
            continue
        work = [(root, 0)]
        while work:
            v, i = work.pop()
            if i == 0:
                index[v] = low[v] = counter
                counter += 1
                stack.append(v)
                on_stack.add(v)
            recurse = False
            nbrs = adj[v]
            while i < len(nbrs):
                w = nbrs[i]
                i += 1
                if w not in member_set:
                    continue
                if w not in index:
                    work.append((v, i))
                    work.append((w, 0))
                    recurse = True
                    break
                if w in on_stack:
                    low[v] = min(low[v], index[w])
            if recurse:
                continue
            if low[v] == index[v]:
                size = 0
                while True:
                    w = stack.pop()
                    on_stack.discard(w)
                    size += 1
                    if w == v:
                        break
                best = max(best, size)
            if work:
                parent = work[-1][0]
                low[parent] = min(low[parent], low[v])
    return best


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--families", default=",".join(FAMILIES), help="comma-separated subset")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args(argv)

    families = [f.strip() for f in args.families.split(",") if f.strip()]
    unknown = [f for f in families if f not in FAMILIES]
    if unknown:
        ap.error(f"unknown families: {unknown}")

    g = Graph()
    starts: List[Tuple[str, StartNode]] = []
    for fam in families:
        # One RNG per family, so adding or removing a family never changes the others.
        rng = random.Random(f"{args.seed}:{fam}")
        for s in FAMILIES[fam](g, rng, args.scale):
            starts.append((fam, s))
        print(f"built {fam}: {sum(1 for f in g.family if f == fam)} nodes so far", file=sys.stderr)
    dedupe_edges(g)
    adj = downstream_adjacency(g)

    args.out.mkdir(parents=True, exist_ok=True)
    with (args.out / "nodes.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["urn", "family", "layer"])
        for i, urn in enumerate(g.urns):
            w.writerow([urn, g.family[i], g.layer[i]])
    with (args.out / "edges.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["downstream_urn", "upstream_urn"])
        for down, up in g.edges:
            w.writerow([g.urns[down], g.urns[up]])

    start_rows = []
    for fam, s in starts:
        per_hop = bfs_hops(adj, s.node)
        start_rows.append(
            {
                "urn": g.urns[s.node],
                "family": fam,
                "layer": g.layer[s.node],
                "note": s.note,
                "expected_per_hop": "|".join(map(str, per_hop)),
                "expected_total": sum(per_hop),
                "expected_hops": len(per_hop),
            }
        )
    with (args.out / "start_nodes.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(start_rows[0]))
        w.writeheader()
        w.writerows(start_rows)

    # Per-family report, to check against the prod targets in PLAN.md §1.3.
    report = {}
    upstreams: Dict[int, int] = {}
    for down, _ in g.edges:
        upstreams[down] = upstreams.get(down, 0) + 1
    for fam in families:
        members = [i for i, f in enumerate(g.family) if f == fam]
        mset = set(members)
        n_edges = sum(1 for d, _ in g.edges if d in mset)
        with_up = [upstreams[i] for i in members if i in upstreams]
        report[fam] = {
            "nodes": len(members),
            "edges": n_edges,
            "avg_upstreams": round(sum(with_up) / len(with_up), 2) if with_up else 0,
            "max_upstreams": max(with_up) if with_up else 0,
            "max_downstreams": max(len(adj[i]) for i in members),
            "largest_scc": largest_scc(adj, members),
        }

    manifest = {
        "seed": args.seed,
        "scale": args.scale,
        "families": families,
        "urn_prefix": URN_PREFIX,
        "nodes": len(g.urns),
        "edges": len(g.edges),
        "report": report,
        "sha256": {p: sha256(args.out / p) for p in ("nodes.csv", "edges.csv", "start_nodes.csv")},
    }
    (args.out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    print(f"\n{len(g.urns):,} nodes, {len(g.edges):,} edges -> {args.out}")
    print(f"{'family':<15}{'nodes':>10}{'edges':>11}{'avg_up':>8}{'max_up':>8}{'max_down':>10}{'scc':>8}")
    for fam, r in report.items():
        print(
            f"{fam:<15}{r['nodes']:>10,}{r['edges']:>11,}{r['avg_upstreams']:>8}"
            f"{r['max_upstreams']:>8}{r['max_downstreams']:>10,}{r['largest_scc']:>8,}"
        )
    print(f"\n{'start':<30}{'total':>10}{'hops':>6}  per-hop (first 8)")
    for row in start_rows:
        head = row["expected_per_hop"].split("|")[:8]
        print(f"{row['family'] + ':' + row['note']:<30}{row['expected_total']:>10,}{row['expected_hops']:>6}  {' '.join(head)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
