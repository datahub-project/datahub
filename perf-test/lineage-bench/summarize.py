"""Scratch summary of a lineage-bench run directory (walks/hops/esquery CSVs)."""

import csv
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path

d = Path(sys.argv[1])


def load(name):
    p = d / name
    if not p.exists() or p.stat().st_size == 0:
        return []
    with p.open() as f:
        return list(csv.DictReader(f))


walks, hops, esq = load("walks.csv"), load("hops.csv"), load("esquery.csv")
f = lambda x: float(x) if x not in ("", None) else 0.0

print("== walks (median over repeats) ==")
print(f"{'start':<28}{'total':>9}{'exp':>9}{'hops':>6}{'wall_ms':>9}{'gms_ms':>9}{'ms/hop':>8} trunc")
by = defaultdict(list)
for w in walks:
    by[(w["family"], w["note"], w["urn"])].append(w)
for (fam, note, _), ws in by.items():
    wall = st.median(f(w["wall_ms"]) for w in ws)
    gms = st.median(f(w["gms_walk_ms"]) for w in ws)
    h = st.median(f(w["gms_hops"]) for w in ws)
    tr = sum(w["truncated"] == "True" for w in ws)
    print(f"{fam + ':' + note:<28}{ws[0]['total']:>9}{ws[0]['expected_total']:>9}{h:>6.0f}{wall:>9.0f}{gms:>9.0f}"
          f"{(gms / h if h else 0):>8.0f} {tr}/{len(ws)}")

if hops:
    print("\n== where hop time goes (sum over all hops, all walks) ==")
    keys = ["pit_create_ms", "slices_ms", "pit_delete_ms", "postprocess_ms"]
    tot = sum(f(h["total_ms"]) for h in hops)
    parts = {k: sum(f(h[k]) for h in hops) for k in keys}
    other = tot - sum(parts.values())
    for k, v in parts.items():
        print(f"  {k:<16}{v / 1000:>10.1f}s  {100 * v / tot:5.1f}%")
    print(f"  {'other':<16}{other / 1000:>10.1f}s  {100 * other / tot:5.1f}%")
    small = [h for h in hops if f(h["urns_in"]) <= 10]
    if small:
        print(f"\n  hops with <=10 URNs in: n={len(small)}, median total {st.median(f(h['total_ms']) for h in small):.0f} ms, "
              f"pit_create {st.median(f(h['pit_create_ms']) for h in small):.0f}, slices {st.median(f(h['slices_ms']) for h in small):.0f}, "
              f"pit_delete {st.median(f(h['pit_delete_ms']) for h in small):.0f}")
    skew = [f(h["slice_max_ms"]) / max(1.0, f(h["slice_min_ms"])) for h in hops if f(h["slice_max_ms"]) > 50]
    if skew:
        print(f"  slice skew (max/min, hops with slice_max>50ms): median {st.median(skew):.1f}x, max {max(skew):.1f}x")

if esq:
    print("\n== esQuery: client ms by URNs in query x hits in page (median, n) ==")
    ub = lambda u: next(b for b in (1, 10, 100, 1000, 10000, 10**9) if u <= b)
    hb = lambda x: next(b for b in (0, 10, 100, 1000, 5000, 10**9) if x <= b)
    cells = defaultdict(list)
    for r in esq:
        cells[(ub(int(r["urns"])), hb(int(r["hits"])))].append((f(r["client_ms"]), f(r["took_ms"])))
    hcols = (0, 10, 100, 1000, 5000)
    print(f"{'urns<=':>8}" + "".join(f"{'hits<=' + str(h):>18}" for h in hcols))
    for u in (1, 10, 100, 1000, 10000, 10**9):
        row = f"{('>10k' if u == 10**9 else u):>8}"
        for h in hcols:
            v = cells.get((u, h))
            row += f"{(f'{st.median(c for c, _ in v):.0f}ms/{st.median(t for _, t in v):.0f}t n={len(v)}' if v else '-'):>18}"
        print(row)
    empty = [r for r in esq if int(r["hits"]) == 0]
    print(f"\n  esQuery calls: {len(esq)}, empty pages: {len(empty)} ({100 * len(empty) / len(esq):.0f}%), "
          f"time in empty pages: {sum(f(r['client_ms']) for r in empty) / 1000:.1f}s of {sum(f(r['client_ms']) for r in esq) / 1000:.1f}s")
    overhead = [f(r["client_ms"]) - f(r["took_ms"]) for r in esq if f(r["took_ms"]) >= 0]
    print(f"  client_ms - took_ms: median {st.median(overhead):.0f} ms, p90 {sorted(overhead)[int(0.9 * len(overhead))]:.0f} ms")
