#!/usr/bin/env python3
"""Render a lineage-bench run directory as a single self-contained HTML report.

Usage: python3 report.py results/<run_id> [--dataset dataset/v1] [--out report.html]

Reads walks.csv, hops.csv and esquery.csv (written by run_walks.py) and draws every chart as
inline SVG, so the page works offline and needs nothing but the standard library.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import math
import random
import statistics as st
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

Row = Dict[str, str]

FAMILY_ORDER = [
    "prod_like", "hub", "deep", "deep100_w1", "deep100_w10", "deep100_w100", "deep100_w1k",
    "wide", "mid", "small_islands", "scc",
]
# Categorical palette (colour-blind friendly), assigned by family order.
PALETTE = ["#4269d0", "#efb118", "#ff725c", "#6cc5b0", "#3ca951", "#ff8ab7", "#a463f2",
           "#97bbf5", "#9c6b4e", "#9498a0", "#e15759"]
HOP_PARTS = [("pit_create_ms", "PIT create", "#97bbf5"), ("slices_ms", "Slice queries", "#4269d0"),
             ("pit_delete_ms", "PIT delete", "#efb118"), ("postprocess_ms", "Post-process", "#3ca951"),
             ("other_ms", "Other", "#9498a0")]
POST_WALK = ("post_walk_ms", "After the walk (search + hydration + HTTP)", "#ff725c")
# Inside "Slice queries": activities of the slowest slice of a hop (it sets the hop's slice time).
SLICE_PARTS = [("q_first_hits", "esQuery: first page with hits", "#4269d0"),
               ("q_next_hits", "esQuery: search_after page with hits", "#a463f2"),
               ("q_empty_followup", "esQuery: empty follow-up", "#ff725c"),
               ("q_empty_first", "esQuery: empty first page", "#9498a0"),
               ("extract_ms", "Extract hits → relationships", "#3ca951"),
               ("build_ms", "Build request", "#6cc5b0"),
               ("queue_ms", "Queue wait (pit-worker pool)", "#efb118"),
               ("other_ms", "Other in slice", "#c7c7cc"),
               ("coord_ms", "Wait on slice futures", "#9c6b4e")]
# After the walk, from the search/resolver log lines (run 2 onwards).
POST_PARTS = [("post_search", "Search post-processing (filter + result page)", "#ff725c"),
              ("post_resolver", "Entity client + resolver mapping", "#efb118"),
              ("post_graphql", "GraphQL hydration + serialisation + HTTP", "#a463f2")]
PAGE_TYPES = [("first_hits", "First page with hits (page 0)", "#4269d0"),
              ("next_hits", "search_after page with hits (page ≥ 1)", "#a463f2"),
              ("empty_followup", "Empty follow-up (search_after, 0 hits)", "#ff725c"),
              ("empty_first", "Empty first page (slice had no matches)", "#9498a0")]
PAGE_SIZE = 5000  # graphService.limit.results.apiDefault (GRAPH_SERVICE_LIMIT_RESULTS_API_DEFAULT), unchanged
URN_BUCKETS = [(1, "1"), (10, "≤10"), (100, "≤100"), (1_000, "≤1k"), (10_000, "≤10k"), (10**12, ">10k")]
HIT_BUCKETS = [(0, "0"), (10, "≤10"), (100, "≤100"), (1_000, "≤1k"), (10**12, "≤5k")]
URN_PREFIX = "urn:li:dataset:(urn:li:dataPlatform:snowflake,bench."


def num(x: Optional[str]) -> float:
    try:
        return float(x) if x not in (None, "") else 0.0
    except ValueError:
        return 0.0


def load_csv(path: Path) -> List[Row]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def esc(s: object) -> str:
    return html.escape(str(s), quote=True)


def fmt_ms(ms: float) -> str:
    if ms < 1:
        return f"{ms:.2f} ms" if ms > 0 else "0 ms"
    if ms < 10:
        return f"{ms:.1f} ms"
    if ms >= 10_000:
        return f"{ms / 1000:.0f} s"
    if ms >= 1_000:
        return f"{ms / 1000:.1f} s"
    return f"{ms:.0f} ms"


def fmt_n(n: float) -> str:
    n = float(n)
    if n >= 1_000_000:
        return f"{n / 1e6:.2f}M"
    if n >= 10_000:
        return f"{n / 1e3:.0f}k"
    if n >= 1_000:
        return f"{n / 1e3:.1f}k"
    return f"{n:.0f}"


def family_color(fam: str) -> str:
    i = FAMILY_ORDER.index(fam) if fam in FAMILY_ORDER else len(FAMILY_ORDER) - 1
    return PALETTE[i % len(PALETTE)]


def bucket(value: float, buckets: Sequence[Tuple[float, str]]) -> str:
    return next(label for limit, label in buckets if value <= limit)


def page_type(r: Row) -> str:
    first = r["page"] == "0"
    if r["hits"] != "0":
        return "first_hits" if first else "next_hits"
    return "empty_first" if first else "empty_followup"


def node_name(urn: str) -> str:
    """bench.<family>.<name> -> <name>"""
    inner = urn[len(URN_PREFIX):-len(",PROD)")] if urn.startswith(URN_PREFIX) else urn
    return inner.split(".", 1)[1] if "." in inner else inner


def slice_activity(sl: Row, esq_rows: List[Row]) -> Dict[str, float]:
    """Split one slice's time into SLICE_PARTS (except coord_ms). esquery_ms is exact (nanosecond
    timer); per-page client_ms is whole milliseconds, so it only sets the split between page types."""
    esq_ms = num(sl["esquery_ms"])
    by: Dict[str, float] = defaultdict(float)
    for r in esq_rows:
        by[page_type(r)] += num(r["client_ms"])
    total = sum(by.values())
    out: Dict[str, float] = {}
    for k, _, _ in PAGE_TYPES:
        if total:
            share = by[k] / total
        else:
            share = sum(1 for r in esq_rows if page_type(r) == k) / len(esq_rows) if esq_rows else 0.0
        out["q_" + k] = esq_ms * share
    for k in ("extract_ms", "build_ms", "queue_ms", "other_ms"):
        out[k] = max(0.0, num(sl[k]))
    return out


def took_split(act: Dict[str, float], esq_rows: List[Row]) -> Dict[str, float]:
    """Split each page type's esQuery time (act["q_*"]) into OpenSearch took and time outside took, in the
    ratio of that slice's per-page took_ms / client_ms (both whole ms, so only the ratio is used)."""
    client: Dict[str, float] = defaultdict(float)
    took: Dict[str, float] = defaultdict(float)
    for r in esq_rows:
        c = num(r["client_ms"])
        client[page_type(r)] += c
        took[page_type(r)] += max(0.0, min(num(r["took_ms"]), c))
    out: Dict[str, float] = {}
    for k, _, _ in PAGE_TYPES:
        q = act.get("q_" + k, 0.0)
        share = took[k] / client[k] if client[k] else 0.0
        out["took_" + k] = q * share
        out["out_" + k] = q * (1 - share)
    return out


def split_bars(rows: List[Tuple[str, str, float, float]], normalize: bool, label: str) -> str:
    """One horizontal bar per row, split into took (solid) and outside took (faded): (label, colour, took, outside)."""
    row_h, left, right, top = 28, 280, 150, 26
    width = 900
    height = top + row_h * len(rows) + 12
    plot_w = width - left - right
    hi = 1.0 if normalize else (max(t + o for _, _, t, o in rows) or 1)
    xpos = lambda v: left + plot_w * v / hi
    fmt = (lambda v: f"{100 * v:.0f}%") if normalize else fmt_ms
    parts = []
    for f in (0, 0.25, 0.5, 0.75, 1.0):
        x = xpos(hi * f)
        parts.append(f'<line x1="{x:.1f}" y1="{top - 6}" x2="{x:.1f}" y2="{height - 10}" class="grid"/>')
        parts.append(text(x, top - 10, fmt(hi * f), "tick", "middle"))
    for i, (lab, color, t, o) in enumerate(rows):
        y = top + i * row_h
        total = (t + o) or 1
        tv, ov = (t / total, o / total) if normalize else (t, o)
        x0, x1, x2 = left, xpos(tv), xpos(tv + ov)
        tip = f"{lab}: {fmt_ms(t + o)} · took {fmt_ms(t)} ({100 * t / total:.0f}%) · outside took {fmt_ms(o)} ({100 * o / total:.0f}%)"
        parts.append(text(left - 8, y + 18, lab, "lab", "end"))
        parts.append(f'<rect x="{x0}" y="{y + 5}" width="{max(0.0, x1 - x0):.1f}" height="{row_h - 10}" '
                     f'fill="{color}"><title>{esc(tip)}</title></rect>')
        parts.append(f'<rect x="{x1:.1f}" y="{y + 5}" width="{max(0.0, x2 - x1):.1f}" height="{row_h - 10}" '
                     f'fill="{color}" fill-opacity="0.35"><title>{esc(tip)}</title></rect>')
        parts.append(text(x2 + 6, y + 18, f"took {100 * t / total:.0f}% · {fmt_ms(t + o)}", "val"))
    return svg(width, height, "".join(parts), label)


def took_section(took_totals: Dict[str, float], crit_pages: Dict[str, List[Row]]) -> str:
    """Inside esQuery: OpenSearch took vs the rest of the client call, per page type (slowest slices)."""
    rows = [(n.split(" (")[0], c, took_totals["took_" + k], took_totals["out_" + k])
            for k, n, c in PAGE_TYPES if took_totals["took_" + k] + took_totals["out_" + k] > 0]
    if not rows:
        return ""
    all_t, all_o = sum(t for _, _, t, _ in rows), sum(o for _, _, _, o in rows)
    rows = [("All esQuery calls", "#6b7280", all_t, all_o)] + rows
    views = [("took-abs", "Total time", split_bars(rows, False, "esQuery time split into took and outside took")),
             ("took-pct", "Share of each page type", split_bars(rows, True, "esQuery took share by page type"))]
    tabs = ("<div class='tabs' role='tablist'>" + "".join(
        f"<button role='tab' class='tab' aria-selected='{str(i == 0).lower()}' data-view='{k}' "
        f"onclick='showView(this)'>{esc(n)}</button>" for i, (k, n, _) in enumerate(views)) + "</div>"
        + "".join(f"<div class='view' id='{k}'{'' if i == 0 else ' hidden'}>{c}</div>" for i, (k, _, c) in enumerate(views)))
    trs = []
    for k, n, c in PAGE_TYPES:
        pages = crit_pages.get(k, [])
        if not pages:
            continue
        cl = [num(r["client_ms"]) for r in pages]
        tk = [max(0.0, min(num(r["took_ms"]), num(r["client_ms"]))) for r in pages]
        t, o = took_totals["took_" + k], took_totals["out_" + k]
        trs.append(f"<tr><td><i class='dot' style='background:{c}'></i>{esc(n)}</td><td class='n'>{fmt_n(len(pages))}</td>"
                   f"<td class='n'>{fmt_ms(t)}</td><td class='n'>{fmt_ms(o)}</td>"
                   f"<td class='n'>{100 * t / ((t + o) or 1):.0f}%</td>"
                   f"<td class='n'>{fmt_ms(st.median(cl))}</td><td class='n'>{fmt_ms(st.median(tk))}</td>"
                   f"<td class='n'>{fmt_ms(st.median(a - b for a, b in zip(cl, tk)))}</td></tr>")
    table = ("<div class='scroll'><table><thead><tr><th>Page type</th><th class='n'>Pages</th><th class='n'>Took</th>"
             "<th class='n'>Outside took</th><th class='n'>Took share</th><th class='n'>Median client</th>"
             "<th class='n'>Median took</th><th class='n'>Median outside</th></tr></thead>"
             f"<tbody>{''.join(trs)}</tbody></table></div>")
    return ("<h3>Inside esQuery: took vs outside took</h3><p class='muted'>The esQuery time from the chart above "
            "(slowest slice of each hop), split by page type into the time OpenSearch reports as <code>took</code> and "
            "the rest of the client call. Solid is took, faded is outside took.</p>"
            + legend([("took (OpenSearch search execution)", "#6b7280"), ("outside took", "rgba(107,114,128,.35)")])
            + tabs + table
            + "<dl class='defs'><dt>took</dt><dd>Reported by OpenSearch: the coordinating node's time for the search "
            "(PIT lookup, shard queue and query phase, fetch of the hits' <code>_source</code>, merge).</dd>"
            "<dt>Outside took</dt><dd><code>client_ms − took_ms</code>: serialising the request (every URN of the hop as a "
            "term), waiting for an HTTP connection, the network both ways, OpenSearch parsing the query and serialising "
            "the response, and GMS parsing it into <code>SearchHit</code>s. Per-page times are whole milliseconds, so "
            "each slice's exact esQuery time is split in the ratio of its pages' took and client time.</dd></dl>")


def post_parts(w: Row) -> Optional[Dict[str, float]]:
    """After-the-walk buckets; they sum exactly to wall time minus GMS walk time."""
    if not w.get("search_total_ms") or not w.get("resolver_total_ms"):
        return None
    wall, gms = num(w["wall_ms"]), num(w["gms_walk_ms"])
    search_total, resolver_total = num(w["search_total_ms"]), num(w["resolver_total_ms"])
    return {"post_search": max(0.0, search_total - gms),
            "post_resolver": max(0.0, resolver_total - search_total),
            "post_graphql": max(0.0, wall - resolver_total)}


# ---------------------------------------------------------------- aggregation


@dataclass
class Start:
    urn: str
    family: str
    note: str
    name: str
    walks: List[Row]
    expected: int
    expected_hops: int
    returned: int
    wall_ms: float
    gms_ms: float
    gms_hops: float
    truncated: int
    mismatched: int
    rep: Row = field(default_factory=dict)  # the repeat with the median wall time

    @property
    def label(self) -> str:
        return f"{self.family}:{self.note}"

    @property
    def status(self) -> str:
        if self.truncated:
            return "truncated"
        return "mismatch" if self.mismatched else "ok"


def build_starts(walks: List[Row]) -> List[Start]:
    groups: Dict[str, List[Row]] = defaultdict(list)
    for w in walks:
        groups[w["urn"]].append(w)
    out = []
    for urn, ws in groups.items():
        by_wall = sorted(ws, key=lambda w: num(w["wall_ms"]))
        out.append(
            Start(
                urn=urn,
                family=ws[0]["family"],
                note=ws[0]["note"],
                name=node_name(urn),
                walks=ws,
                expected=int(num(ws[0]["expected_total"])),
                expected_hops=int(num(ws[0]["expected_hops"])),
                returned=int(st.median(num(w["total"]) for w in ws)),
                wall_ms=st.median(num(w["wall_ms"]) for w in ws),
                gms_ms=st.median(num(w["gms_walk_ms"]) for w in ws),
                gms_hops=st.median(num(w["gms_hops"]) for w in ws),
                truncated=sum(w["truncated"] == "True" for w in ws),
                mismatched=sum(w["total"] != w["expected_total"] for w in ws),
                rep=by_wall[len(by_wall) // 2],
            )
        )
    order = {f: i for i, f in enumerate(FAMILY_ORDER)}
    out.sort(key=lambda s: (-s.expected, order.get(s.family, 99), s.note, s.name))
    return out


# ---------------------------------------------------------------- SVG helpers


def svg(width: int, height: int, body: str, label: str) -> str:
    return (f'<svg viewBox="0 0 {width} {height}" width="100%" role="img" aria-label="{esc(label)}" '
            f'preserveAspectRatio="xMinYMin meet">{body}</svg>')


def text(x: float, y: float, s: str, cls: str = "", anchor: str = "start", extra: str = "") -> str:
    return f'<text x="{x:.1f}" y="{y:.1f}" class="{cls}" text-anchor="{anchor}" {extra}>{esc(s)}</text>'


def log_ticks(lo: float, hi: float) -> List[float]:
    ticks, v = [], 10 ** math.floor(math.log10(max(lo, 1e-9)))
    while v <= hi * 1.0001:
        if v >= lo * 0.9999:
            ticks.append(v)
        v *= 10
    return ticks


def hbar_chart(items: List[Tuple[str, float, str, str]], unit_fmt: Callable[[float], str], label: str) -> str:
    """Horizontal bars on a linear axis: (label, value, colour, tooltip)."""
    if not items:
        return ""
    row_h, left, right, top = 24, 280, 80, 26
    width = 900
    height = top + row_h * len(items) + 12
    plot_w = width - left - right
    hi = max(v for _, v, _, _ in items) or 1
    xpos = lambda v: left + plot_w * v / (hi * 1.05)
    parts = []
    for f in (0, 0.25, 0.5, 0.75, 1.0):
        x = xpos(hi * f)
        parts.append(f'<line x1="{x:.1f}" y1="{top - 6}" x2="{x:.1f}" y2="{height - 10}" class="grid"/>')
        parts.append(text(x, top - 10, unit_fmt(hi * f), "tick", "middle"))
    for i, (lab, v, color, tip) in enumerate(items):
        y = top + i * row_h
        x1 = xpos(v)
        parts.append(text(left - 8, y + 16, lab, "lab", "end"))
        parts.append(f'<rect x="{left}" y="{y + 5}" width="{max(1.5, x1 - left):.1f}" height="{row_h - 10}" '
                     f'rx="2" fill="{color}"><title>{esc(tip)}</title></rect>')
        parts.append(text(x1 + 6, y + 16, unit_fmt(v), "val"))
    return svg(width, height, "".join(parts), label)


def stacked_columns(cols: List[Tuple[str, Dict[str, float], str]], parts: Sequence[Tuple[str, str, str]],
                    label: str, y_fmt: Callable[[float], str] = fmt_ms, height: int = 280) -> str:
    """Vertical stacked columns: (x label, {part key: value}, tooltip prefix)."""
    if not cols:
        return ""
    width, left, bottom, top = 900, 64, 40, 16
    plot_w, plot_h = width - left - 16, height - top - bottom
    totals = [sum(p.values()) for _, p, _ in cols]
    ymax = max(totals) * 1.08 or 1
    bw = plot_w / len(cols)
    every = max(1, math.ceil(len(cols) / 25))  # keep x labels readable on 100-hop walks
    out = []
    for f in (0, 0.25, 0.5, 0.75, 1.0):
        y = top + plot_h * (1 - f)
        out.append(f'<line x1="{left}" y1="{y:.1f}" x2="{width - 16}" y2="{y:.1f}" class="grid"/>')
        out.append(text(left - 6, y + 4, y_fmt(ymax * f), "tick", "end"))
    for i, (xl, pv, tip) in enumerate(cols):
        x = left + i * bw + bw * 0.12
        y = top + plot_h
        total = sum(pv.values())
        for key, name, color in parts:
            v = pv.get(key, 0.0)
            if v <= 0:
                continue
            h = plot_h * v / ymax
            y -= h
            out.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{max(0.8, bw * 0.76):.1f}" height="{h:.1f}" fill="{color}">'
                       f'<title>{esc(tip)} — {esc(name)}: {esc(y_fmt(v))} of {esc(y_fmt(total))}</title></rect>')
        if i % every == 0 or i == len(cols) - 1:
            out.append(text(x + bw * 0.38, top + plot_h + 16, xl, "tick", "middle"))
    out.append(text(left + plot_w / 2, height - 4, "hop", "axis", "middle"))
    return svg(width, height, "".join(out), label)


def timeline(segments: List[Tuple[str, float, str]], label: str) -> str:
    """One horizontal bar split into sequential segments: (tooltip, ms, colour)."""
    width, left, right, top, h = 900, 8, 8, 8, 30
    total = sum(v for _, v, _ in segments) or 1
    plot_w = width - left - right
    out, x = [], float(left)
    for tip, v, color in segments:
        w = plot_w * v / total
        out.append(f'<rect x="{x:.2f}" y="{top}" width="{max(0.6, w):.2f}" height="{h}" fill="{color}" '
                   f'stroke="var(--panel)" stroke-width="{0.6 if w > 2 else 0}"><title>{esc(tip)}</title></rect>')
        x += w
    for f in (0, 0.25, 0.5, 0.75, 1.0):
        anchor = "start" if f == 0 else ("end" if f == 1 else "middle")
        out.append(text(left + plot_w * f, top + h + 16, fmt_ms(total * f), "tick", anchor))
    return svg(width, top + h + 24, "".join(out), label)


def heatmap(cells: Dict[Tuple[str, str], List[Tuple[float, float]]], label: str) -> str:
    """Rows = URN buckets, columns = hit buckets; colour = median client ms (log scale)."""
    width, left, top, cw, ch = 900, 90, 40, 150, 46
    rows, cols = [b for _, b in URN_BUCKETS], [b for _, b in HIT_BUCKETS]
    height = top + ch * len(rows) + 30
    meds = [st.median(c for c, _ in v) for v in cells.values() if v]
    lo, hi = math.log10(max(1, min(meds))), math.log10(max(meds) + 1)
    out = [text(left + cw * len(cols) / 2, 14, "Hits returned in the page →", "axis", "middle"),
           text(14, top + ch * len(rows) / 2, "URNs in the query →", "axis", "middle",
                f'transform="rotate(-90 14 {top + ch * len(rows) / 2:.0f})"')]
    for j, c in enumerate(cols):
        out.append(text(left + cw * j + cw / 2, top - 8, c, "tick", "middle"))
    for i, r in enumerate(rows):
        out.append(text(left - 8, top + ch * i + ch / 2 + 4, r, "tick", "end"))
        for j, c in enumerate(cols):
            v = cells.get((r, c))
            x, y = left + cw * j, top + ch * i
            if not v:
                out.append(f'<rect x="{x + 2}" y="{y + 2}" width="{cw - 4}" height="{ch - 4}" rx="4" class="empty"/>')
                continue
            med = st.median(a for a, _ in v)
            took = st.median(b for _, b in v)
            t = 0 if hi == lo else (math.log10(max(med, 1)) - lo) / (hi - lo)
            ink = "#fff" if t > 0.55 else "#111"
            out.append(f'<rect x="{x + 2}" y="{y + 2}" width="{cw - 4}" height="{ch - 4}" rx="4" fill="{heat_color(t)}">'
                       f'<title>URNs {esc(r)}, hits {esc(c)}: median client {med:.0f} ms, took {took:.0f} ms, '
                       f'n={len(v)}</title></rect>')
            out.append(f'<text x="{x + cw / 2:.1f}" y="{y + 20}" text-anchor="middle" class="cellv" style="fill:{ink}">'
                       f'{esc(fmt_ms(med))}</text>')
            out.append(f'<text x="{x + cw / 2:.1f}" y="{y + 35}" text-anchor="middle" class="cells" style="fill:{ink}">'
                       f'took {esc(fmt_ms(took))} · n={esc(fmt_n(len(v)))}</text>')
    return svg(width, height, "".join(out), label)


def heat_color(t: float) -> str:
    stops = [(0.0, (255, 245, 204)), (0.5, (253, 141, 60)), (1.0, (165, 15, 21))]
    for (t0, c0), (t1, c1) in zip(stops, stops[1:]):
        if t <= t1:
            f = (t - t0) / (t1 - t0) if t1 > t0 else 0
            return "#%02x%02x%02x" % tuple(round(a + (b - a) * f) for a, b in zip(c0, c1))
    return "#a50f15"


def loglog_axes(xmax: float, ymin: float, ymax: float, x_name: str, y_name: str,
                width: int = 900, height: int = 380
                ) -> Tuple[List[str], Callable[[float], float], Callable[[float], float]]:
    left, bottom, top, right = 64, 44, 14, 16
    plot_w, plot_h = width - left - right, height - top - bottom
    xl, xh = 0.0, math.log10(max(xmax, 10)) + 0.1
    yl, yh = math.floor(math.log10(max(ymin, 1))), math.log10(max(ymax, 10)) + 0.1
    px = lambda v: left + plot_w * (math.log10(max(v, 1)) - xl) / (xh - xl)
    py = lambda v: top + plot_h * (1 - (math.log10(max(v, 1)) - yl) / (yh - yl))
    out = []
    for t in log_ticks(1, 10 ** xh):
        out.append(f'<line x1="{px(t):.1f}" y1="{top}" x2="{px(t):.1f}" y2="{top + plot_h}" class="grid"/>')
        out.append(text(px(t), top + plot_h + 16, fmt_n(t), "tick", "middle"))
    for t in log_ticks(10 ** yl, 10 ** yh):
        out.append(f'<line x1="{left}" y1="{py(t):.1f}" x2="{left + plot_w}" y2="{py(t):.1f}" class="grid"/>')
        out.append(text(left - 6, py(t) + 4, fmt_ms(t), "tick", "end"))
    out.append(text(left + plot_w / 2, height - 6, x_name, "axis", "middle"))
    out.append(text(14, top + plot_h / 2, y_name, "axis", "middle", f'transform="rotate(-90 14 {top + plot_h / 2:.0f})"'))
    return out, px, py


def scatter_loglog(points: List[Tuple[float, float, str, str]], label: str, x_name: str, y_name: str,
                   lines: Sequence[Tuple[str, str, List[Tuple[float, float]]]] = ()) -> str:
    """(x, y, colour, tooltip) on log-log axes, plus optional (name, colour, [(x, y)]) median lines."""
    out, px, py = loglog_axes(max(p[0] for p in points), 1, max(p[1] for p in points), x_name, y_name, height=360)
    rng = random.Random(7)
    for x, y, color, tip in points:
        jx = px(x) + rng.uniform(-3, 3)  # URN counts are discrete; jitter so points don't stack
        out.append(f'<circle cx="{jx:.1f}" cy="{py(y):.1f}" r="2.2" fill="{color}" fill-opacity="0.45">'
                   f'<title>{esc(tip)}</title></circle>')
    for name, color, pts in lines:
        path = " ".join(f"{px(x):.1f},{py(y):.1f}" for x, y in pts)
        out.append(f'<polyline points="{path}" fill="none" stroke="var(--panel)" stroke-width="5" stroke-linejoin="round"/>'
                   f'<polyline points="{path}" fill="none" stroke="{color}" stroke-width="2.5" stroke-linejoin="round"/>')
        for x, y in pts:
            out.append(f'<circle cx="{px(x):.1f}" cy="{py(y):.1f}" r="3.5" fill="{color}" stroke="var(--panel)" '
                       f'stroke-width="1.5"><title>{esc(f"{name}: median {fmt_ms(y)} at ~{fmt_n(x)} URNs")}</title></circle>')
    return svg(900, 360, "".join(out), label)


def log_bin_medians(pairs: List[Tuple[float, float]], per_decade: int = 2, min_n: int = 5) -> List[Tuple[float, float]]:
    """Median y per log-spaced x bin, placed at the bin's median x; bins with fewer than min_n points are dropped."""
    bins: Dict[int, List[Tuple[float, float]]] = defaultdict(list)
    for x, y in pairs:
        bins[math.floor(math.log10(max(x, 1)) * per_decade)].append((x, y))
    return [(st.median(x for x, _ in v), st.median(y for _, y in v)) for _, v in sorted(bins.items()) if len(v) >= min_n]


def start_dots(starts: List[Start], value: Callable[[Start], float], label: str, y_name: str,
               value_name: str) -> str:
    """One dot per start node: nodes returned (x) vs a time (y), log-log. Hollow = truncated walk."""
    pts = [(max(s.returned, 1), max(value(s), 1), s) for s in starts]
    out, px, py = loglog_axes(max(p[0] for p in pts), min(p[1] for p in pts), max(p[1] for p in pts),
                              "nodes returned by the walk", y_name)
    for x, y, s in sorted(pts, key=lambda p: p[2].status != "truncated"):
        tip = (f"{s.label} ({s.name}): {fmt_n(s.returned)} nodes returned of {fmt_n(s.expected)} expected, "
               f"{s.expected_hops} hops, {value_name} {fmt_ms(value(s))}")
        if s.status == "truncated":
            out.append(f'<circle cx="{px(x):.1f}" cy="{py(y):.1f}" r="6" fill="var(--panel)" stroke="var(--bad)" stroke-width="2.5">'
                       f'<title>{esc(tip)} (TRUNCATED)</title></circle>')
            out.append(text(px(x) - 4, py(y) - 12, f"{s.label} (truncated)", "val", "end"))
        else:
            out.append(f'<circle cx="{px(x):.1f}" cy="{py(y):.1f}" r="4.5" class="dotp"><title>{esc(tip)}</title></circle>')
    return svg(900, 380, "".join(out), label)


def start_dots_multi(starts: List[Start], series: Sequence[Tuple[str, str, Callable[[Start], Optional[float]]]],
                     label: str, y_name: str) -> str:
    """Several values per start node (one colour each) against nodes returned, log-log."""
    pts = []
    for s in starts:
        for name, color, fn in series:
            v = fn(s)
            if v is not None:
                pts.append((max(s.returned, 1), max(v, 1.0), color, f"{s.label} ({s.name}), {fmt_n(s.returned)} nodes: "
                                                                  f"{name} {fmt_ms(v)}"))
    if not pts:
        return ""
    out, px, py = loglog_axes(max(p[0] for p in pts), min(p[1] for p in pts), max(p[1] for p in pts),
                              "nodes returned by the walk", y_name)
    for x, y, color, tip in pts:
        out.append(f'<circle cx="{px(x):.1f}" cy="{py(y):.1f}" r="4" fill="{color}" fill-opacity=".85" '
                   f'stroke="var(--panel)" stroke-width="1"><title>{esc(tip)}</title></circle>')
    return svg(900, 380, "".join(out), label)


def scatter_hits(points: List[Tuple[float, float, str, str]], label: str, y_min: float = 1,
                 height: int = 360, y_name: str = "client time") -> str:
    """Latency vs hits in the page, log-log, with a separate band on the left for 0-hit pages.
    y_min sets the bottom of the time axis (smaller values are drawn on it)."""
    width, left, bottom, top, right, band = 900, 64, 44, 14, 16, 46
    plot_l = left + band
    plot_w, plot_h = width - plot_l - right, height - top - bottom
    xh = math.log10(max(max(p[0] for p in points), 10)) + 0.1
    yl = math.log10(y_min)
    yh = math.log10(max(max(p[1] for p in points), 10)) + 0.1
    px = lambda v: plot_l + plot_w * math.log10(max(v, 1)) / xh
    py = lambda v: top + plot_h * (1 - (math.log10(max(v, y_min)) - yl) / (yh - yl))
    out = [f'<rect x="{left + 4}" y="{top}" width="{band - 12}" height="{plot_h}" class="band"/>',
           text(left + band / 2 - 2, top + plot_h + 16, "0", "tick", "middle")]
    for t in log_ticks(1, 10 ** xh):
        out.append(f'<line x1="{px(t):.1f}" y1="{top}" x2="{px(t):.1f}" y2="{top + plot_h}" class="grid"/>')
        out.append(text(px(t), top + plot_h + 16, fmt_n(t), "tick", "middle"))
    for t in log_ticks(y_min, 10 ** yh):
        tick = fmt_ms(t) if t >= 0.01 else f"{t:g} ms"
        out.append(f'<line x1="{left}" y1="{py(t):.1f}" x2="{plot_l + plot_w}" y2="{py(t):.1f}" class="grid"/>')
        out.append(text(left - 6, py(t) + 4, tick, "tick", "end"))
    out.append(f'<line x1="{px(PAGE_SIZE):.1f}" y1="{top}" x2="{px(PAGE_SIZE):.1f}" y2="{top + plot_h}" class="limit"/>')
    out.append(text(px(PAGE_SIZE) - 4, top + 12, f"page size {fmt_n(PAGE_SIZE)}", "val", "end"))
    out.append(text(plot_l + plot_w / 2, height - 6, "hits returned in the page", "axis", "middle"))
    out.append(text(14, top + plot_h / 2, y_name, "axis", "middle", f'transform="rotate(-90 14 {top + plot_h / 2:.0f})"'))
    rng = random.Random(11)
    for x, y, color, tip in points:
        cx = (left + band / 2 - 2 + rng.uniform(-12, 12)) if x == 0 else px(x) + rng.uniform(-2, 2)
        out.append(f'<circle cx="{cx:.1f}" cy="{py(y):.1f}" r="2.2" fill="{color}" fill-opacity="0.45">'
                   f'<title>{esc(tip)}</title></circle>')
    return svg(width, height, "".join(out), label)


def line_chart(series: List[Tuple[str, str, List[Tuple[float, float]]]], label: str, x_name: str) -> str:
    width, height, left, bottom, top, right = 900, 300, 64, 44, 14, 140
    plot_w, plot_h = width - left - right, height - top - bottom
    allx = [x for _, _, pts in series for x, _ in pts]
    ally = [y for _, _, pts in series for _, y in pts]
    if not allx:
        return ""
    xmax, ymax = max(allx), max(ally) * 1.1
    px = lambda v: left + plot_w * v / xmax
    py = lambda v: top + plot_h * (1 - v / ymax)
    out = []
    for f in (0, 0.25, 0.5, 0.75, 1.0):
        out.append(f'<line x1="{left}" y1="{py(ymax * f):.1f}" x2="{left + plot_w}" y2="{py(ymax * f):.1f}" class="grid"/>')
        out.append(text(left - 6, py(ymax * f) + 4, fmt_ms(ymax * f), "tick", "end"))
        out.append(text(px(xmax * f), top + plot_h + 16, f"{xmax * f:.0f}", "tick", "middle"))
    out.append(text(left + plot_w / 2, height - 6, x_name, "axis", "middle"))
    for i, (name, color, pts) in enumerate(series):
        d = " ".join(f"{'M' if k == 0 else 'L'}{px(x):.1f},{py(y):.1f}" for k, (x, y) in enumerate(pts))
        out.append(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="2"><title>{esc(name)}</title></path>')
        ly = top + 14 + i * 20
        out.append(f'<rect x="{left + plot_w + 16}" y="{ly - 9}" width="12" height="12" rx="2" fill="{color}"/>')
        out.append(text(left + plot_w + 34, ly + 1, name, "lab"))
    return svg(width, height, "".join(out), label)


def legend(items: Sequence[Tuple[str, str]]) -> str:
    return '<div class="legend">' + "".join(
        f'<span><i style="background:{c}"></i>{esc(n)}</span>' for n, c in items) + "</div>"


# ---------------------------------------------------------------- per-walk detail


def walk_detail(s: Start, idx: int, hops_by_walk: Dict[str, List[Row]], esq_by_walk: Dict[str, List[Row]],
                slices_by_hop: Dict[Tuple[str, str], List[Row]]) -> str:
    w = s.rep
    hops = hops_by_walk.get(w.get("walk_id", ""), [])
    esq = esq_by_walk.get(w.get("walk_id", ""), [])
    wall, gms = num(w["wall_ms"]), num(w["gms_walk_ms"])
    post_walk = max(0.0, wall - gms)
    hop_sum = sum(num(h["total_ms"]) for h in hops)

    per_hop_esq: Dict[str, List[Row]] = defaultdict(list)
    for r in esq:
        per_hop_esq[r["hop"]].append(r)

    # The whole request as one timeline: each hop, the rest of the walk, then everything after it.
    blues = ["#4269d0", "#7f9be6"]
    segs = [(f"hop {h['hop']}: {fmt_n(num(h['urns_in']))} URNs in → {fmt_n(num(h['rels_out']))} new, "
             f"{fmt_ms(num(h['total_ms']))}", num(h["total_ms"]), blues[i % 2]) for i, h in enumerate(hops)]
    walk_rest = max(0.0, gms - hop_sum)
    if walk_rest >= 1:
        segs.append((f"walk time outside hops: {fmt_ms(walk_rest)}", walk_rest, "#9498a0"))
    pp = post_parts(w)
    if pp:
        for k, name, color in POST_PARTS:
            segs.append((f"{name}: {fmt_ms(pp[k])}", pp[k], color))
    else:
        segs.append((f"{POST_WALK[1]}: {fmt_ms(post_walk)}", post_walk, POST_WALK[2]))
    tl = timeline(segs, f"Request timeline for {s.label}")
    tl_legend = legend([("Hops (alternating shades)", "#4269d0"), ("Walk outside hops", "#9498a0")]
                       + ([(n, c) for _, n, c in POST_PARTS] if pp else [(POST_WALK[1], POST_WALK[2])]))
    has_slices = any("_slice_act" in h for h in hops)
    slice_chart = ""
    if has_slices:
        scols = [(h["hop"], h.get("_slice_act", {}),
                  f"hop {h['hop']} slowest slice (#{h.get('_crit_slice', '?')})") for h in hops]
        slice_chart = (f"<h3>Inside slice queries, hop by hop</h3><p class='muted small'>The slowest slice of each hop "
                       f"(it decides when the hop can continue), split by activity.</p>"
                       f"{legend([(n, c) for _, n, c in SLICE_PARTS])}"
                       f"{stacked_columns(scols, SLICE_PARTS, f'Slowest slice per hop for {s.label}', height=240)}")

    cols = [(h["hop"], {k: num(h[k]) for k, _, _ in HOP_PARTS},
             f"hop {h['hop']}: {fmt_n(num(h['urns_in']))} URNs in → {fmt_n(num(h['rels_out']))} new") for h in hops]
    chart = stacked_columns(cols, HOP_PARTS, f"Per-hop time for {s.label}", height=240)

    trs = []
    for h in hops:
        e = per_hop_esq.get(h["hop"], [])
        follow = [r for r in e if page_type(r) == "empty_followup"]
        nxt = [r for r in e if page_type(r) == "next_hits"]
        pages = max((int(r["page"]) + 1 for r in e), default=0)
        trs.append(
            f"<tr><td class='n'>{esc(h['hop'])}</td><td class='n'>{fmt_n(num(h['urns_in']))}</td>"
            f"<td class='n'>{fmt_n(num(h['rels_out']))}</td><td class='n'>{fmt_ms(num(h['pit_create_ms']))}</td>"
            f"<td class='n'>{fmt_ms(num(h['slices_ms']))}</td>"
            f"<td class='n'>{fmt_ms(num(h['slice_min_ms']))} – {fmt_ms(num(h['slice_max_ms']))}</td>"
            f"<td class='n'>{fmt_ms(num(h['pit_delete_ms']))}</td><td class='n'>{fmt_ms(num(h['postprocess_ms']))}</td>"
            f"<td class='n'>{fmt_ms(num(h['other_ms']))}</td><td class='n'><b>{fmt_ms(num(h['total_ms']))}</b></td>"
            f"<td class='n'>{len(e)}</td><td class='n'>{pages}</td><td class='n'>{len(nxt)}</td>"
            f"<td class='n'>{len(follow)} · {fmt_ms(sum(num(r['client_ms']) for r in follow))}</td>"
            + (f"<td><button class='more' aria-expanded='false' aria-controls='s{idx}_{h['hop']}' "
               f"onclick='toggle(this, \"Slices\")'>Slices</button></td>" if has_slices else "")
            + "</tr>")
        if has_slices:
            trs.append(f"<tr class='detail sub' id='s{idx}_{h['hop']}' hidden><td colspan='15'>"
                       f"{slice_table(w['walk_id'], h, slices_by_hop, per_hop_esq.get(h['hop'], []))}</td></tr>")
    table = ("<div class='scroll'><table class='compact'><thead><tr><th class='n'>Hop</th><th class='n'>URNs in</th>"
             "<th class='n'>New nodes</th><th class='n'>PIT create</th><th class='n'>Slices</th>"
             "<th class='n'>Fastest – slowest slice</th><th class='n'>PIT delete</th><th class='n'>Post-process</th>"
             "<th class='n'>Other</th><th class='n'>Hop total</th><th class='n'>esQuery calls</th>"
             "<th class='n'>Max pages / slice</th><th class='n'>search_after pages with hits</th><th class='n'>Empty follow-ups · summed time</th>"
             + ("<th></th>" if has_slices else "") + "</tr></thead><tbody>" + "".join(trs) + "</tbody></table></div>")

    reps = " · ".join(
        f"rep {r['repeat']}: {fmt_ms(num(r['wall_ms']))}{' (shown)' if r is w else ''}"
        for r in sorted(s.walks, key=lambda r: r["repeat"]))
    facts = [
        ("Start URN", f"<code>{esc(s.urn)}</code>"),
        ("Repeats", esc(reps)),
        ("Nodes", f"{fmt_n(num(w['total']))} returned · {fmt_n(s.expected)} expected"),
        ("Hops", (f"{esc(w['gms_hops'])} run by GMS before the time budget ran out · {s.expected_hops} needed"
                  if w["truncated"] == "True" else
                  f"{esc(w['gms_hops'])} run by GMS · {s.expected_hops} expected. GMS runs one extra hop, "
                  "querying the last layer to confirm nothing is below it.")),
        ("Wall time", f"<b>{fmt_ms(wall)}</b> = GMS walk {fmt_ms(gms)} + after the walk {fmt_ms(post_walk)}"),
    ]
    if pp:
        facts.append(("After the walk", " · ".join(f"{esc(n)} {fmt_ms(pp[k])}" for k, n, _ in POST_PARTS)
                      + f" (search path: {esc(w.get('search_code_path', '?'))}, filter {fmt_ms(num(w.get('search_filter_ms')))}, "
                        f"result page {fmt_ms(num(w.get('search_result_ms')))})"))
    facts_html = "<dl class='facts'>" + "".join(f"<dt>{k}</dt><dd>{v}</dd>" for k, v in facts) + "</dl>"
    return (f"<div class='detail-body'>{facts_html}"
            f"<h3>Whole request</h3>"
            f"{tl_legend}{tl}"
            + ("" if pp else "<p class='muted small'>The time after the walk isn't broken down for this run (no search/resolver "
               "log lines). It's measured as wall time minus GMS walk time.</p>")
            + f"<h3>Hop by hop</h3>{legend([(n, c) for _, n, c in HOP_PARTS])}{chart}{slice_chart}"
            + (f"<p class='muted small'>Use <b>Slices</b> on a hop to see all its slices.</p>" if has_slices else "")
            + f"{table}</div>")


def slice_table(walk_id: str, h: Row, slices_by_hop: Dict[Tuple[str, str], List[Row]], esq_rows: List[Row]) -> str:
    rows = sorted(slices_by_hop.get((walk_id, h["hop"]), []), key=lambda r: int(r["slice"]))
    by_slice: Dict[str, List[Row]] = defaultdict(list)
    for r in esq_rows:
        by_slice[r["slice"]].append(r)
    trs = []
    for sl in rows:
        act = slice_activity(sl, by_slice.get(sl["slice"], []))
        crit = " <span class='badge warn'>slowest</span>" if sl["slice"] == h.get("_crit_slice") else ""
        cells = "".join(f"<td class='n'>{fmt_ms(act[k])}</td>" for k, _, _ in SLICE_PARTS if k != "coord_ms")
        trs.append(f"<tr><td>{esc(sl['slice'])}{crit}</td><td class='n'>{esc(sl['pages'])}</td>{cells}"
                   f"<td class='n'><b>{fmt_ms(num(sl['queue_ms']) + num(sl['total_ms']))}</b></td>"
                   f"<td class='n'>{fmt_n(num(sl['rels_kept']))}</td><td>{esc(sl['end_reason'])}</td></tr>")
    heads = "".join(f"<th class='n'>{esc(n.replace('esQuery: ', ''))}</th>" for k, n, _ in SLICE_PARTS if k != "coord_ms")
    return (f"<div class='scroll'><table class='compact slices'><thead><tr><th>Slice</th><th class='n'>Pages</th>{heads}"
            f"<th class='n'>Queue + slice</th><th class='n'>Relationships kept</th><th>Ended by</th></tr></thead>"
            f"<tbody>{''.join(trs)}</tbody></table></div>"
            f"<p class='muted small'>Hop {esc(h['hop'])}: slice queries {fmt_ms(num(h['slices_ms']))}, of which "
            f"{fmt_ms(h.get('_slice_act', {}).get('coord_ms', 0))} is waiting on the slice futures after the slowest slice finished.</p>")


# ---------------------------------------------------------------- report


def build(run: Path, dataset: Optional[Path]) -> str:
    walks = load_csv(run / "walks.csv")
    hops = load_csv(run / "hops.csv")
    esq = load_csv(run / "esquery.csv")
    manifest = json.loads((dataset / "manifest.json").read_text()) if dataset and (dataset / "manifest.json").exists() else None
    if not walks:
        raise SystemExit(f"no walks.csv in {run}")

    for h in hops:
        known = sum(num(h[k]) for k, _, _ in HOP_PARTS if k != "other_ms")
        h["other_ms"] = str(max(0.0, num(h["total_ms"]) - known))
    hops_by_walk: Dict[str, List[Row]] = defaultdict(list)
    for h in hops:
        hops_by_walk[h["walk_id"]].append(h)
    for v in hops_by_walk.values():
        v.sort(key=lambda h: int(h["hop"]))
    esq_by_walk: Dict[str, List[Row]] = defaultdict(list)
    for r in esq:
        esq_by_walk[r["walk_id"]].append(r)

    slices = load_csv(run / "slices.csv")
    slices_by_hop: Dict[Tuple[str, str], List[Row]] = defaultdict(list)
    for sl in slices:
        slices_by_hop[(sl["walk_id"], sl["hop"])].append(sl)
    esq_by_slice: Dict[Tuple[str, str, str], List[Row]] = defaultdict(list)
    for r in esq:
        esq_by_slice[(r["walk_id"], r["hop"], r["slice"])].append(r)
    slice_totals: Dict[str, float] = defaultdict(float)
    took_totals: Dict[str, float] = defaultdict(float)
    crit_pages: Dict[str, List[Row]] = defaultdict(list)
    for h in hops:
        group = slices_by_hop.get((h["walk_id"], h["hop"]))
        if not group:
            continue
        crit = max(group, key=lambda sl: num(sl["queue_ms"]) + num(sl["total_ms"]))
        crit_rows = esq_by_slice.get((h["walk_id"], h["hop"], crit["slice"]), [])
        act = slice_activity(crit, crit_rows)
        for k, v in took_split(act, crit_rows).items():
            took_totals[k] += v
        for r in crit_rows:
            crit_pages[page_type(r)].append(r)
        act["coord_ms"] = max(0.0, num(h["slices_ms"]) - num(crit["queue_ms"]) - num(crit["total_ms"]))
        h["_slice_act"] = act  # type: ignore[assignment]
        h["_crit_slice"] = crit["slice"]
        for k, v in act.items():
            slice_totals[k] += v

    starts = build_starts(walks)
    n_ok = sum(1 for w in walks if w["total"] == w["expected_total"] and w["truncated"] != "True")
    n_trunc = sum(1 for w in walks if w["truncated"] == "True")
    n_mis = sum(1 for w in walks if w["total"] != w["expected_total"])
    repeats = max(len(s.walks) for s in starts)

    hop_total = sum(num(h["total_ms"]) for h in hops) or 1
    hop_parts = {k: sum(num(h[k]) for h in hops) for k, _, _ in HOP_PARTS}
    post_walk_total = sum(max(0.0, num(w["wall_ms"]) - num(w["gms_walk_ms"])) for w in walks)
    post_split = [post_parts(w) for w in walks]
    has_post = all(p is not None for p in post_split)
    post_totals = {k: sum(p[k] for p in post_split if p) for k, _, _ in POST_PARTS}
    wall_total = sum(num(w["wall_ms"]) for w in walks) or 1

    by_type: Dict[str, List[Row]] = defaultdict(list)
    for r in esq:
        by_type[page_type(r)].append(r)
    esq_total_ms = sum(num(r["client_ms"]) for r in esq) or 1

    # ---- headline cards
    worst = max(starts, key=lambda s: s.wall_ms)
    follow = by_type.get("empty_followup", [])
    follow_share = 100 * sum(num(r["client_ms"]) for r in follow) / esq_total_ms if esq else 0
    cards = [
        ("Walks", f"{len(walks)}", f"{len(starts)} start nodes × {repeats} repeats, all downstream"),
        ("Exact counts", f"{n_ok}/{len(walks)}", f"{n_trunc} truncated, {n_mis} count mismatches"),
        ("Slowest walk", fmt_ms(worst.wall_ms), f"{worst.label} ({fmt_n(worst.returned)}/{fmt_n(worst.expected)} nodes)"),
        ("Empty follow-up pages", f"{follow_share:.0f}%", f"of esQuery time · {100 * len(follow) / max(1, len(esq)):.0f}% of calls"),
    ]
    cards_html = '<div class="cards">' + "".join(
        f'<div class="card"><div class="k">{esc(k)}</div><div class="v">{esc(v)}</div><div class="s">{esc(s)}</div></div>'
        for k, v, s in cards) + "</div>"

    nvt = start_dots(starts, lambda s: s.wall_ms, "Nodes returned vs walk time", "median walk time (wall clock)",
                     "median wall")
    if has_post:
        def med_part(key: str) -> Callable[[Start], Optional[float]]:
            return lambda s: st.median(post_parts(w)[key] for w in s.walks)  # type: ignore[index]

        post_dots = (legend([(n, c) for _, n, c in POST_PARTS])
                     + start_dots_multi(starts, [(n, c, med_part(k)) for k, n, c in POST_PARTS],
                                        "Time after the walk vs nodes returned", "median time (per part)"))
    else:
        post_dots = start_dots(starts, lambda s: max(0.0, s.wall_ms - s.gms_ms), "Time after the walk vs nodes returned",
                               "median time after the walk", "after the walk")

    # ---- start-node table with expandable detail rows
    trs = []
    for i, s in enumerate(starts):
        badge = {"ok": ("ok", "exact"), "truncated": ("bad", "truncated"), "mismatch": ("warn", "mismatch")}[s.status]
        per_hop = s.gms_ms / s.gms_hops if s.gms_hops else 0
        did = f"d{i}"
        trs.append(
            f"<tr class='main'><td><i class='dot' style='background:{family_color(s.family)}'></i>{esc(s.label)}"
            f"<div class='sub'>{esc(s.name)}</div></td>"
            f"<td class='n'>{fmt_n(s.expected)}</td><td class='n'>{fmt_n(s.returned)}</td>"
            f"<td class='n'>{s.expected_hops}</td><td class='n'>{fmt_ms(s.wall_ms)}</td><td class='n'>{fmt_ms(per_hop)}</td>"
            f"<td><span class='badge {badge[0]}'>{badge[1]}</span></td>"
            f"<td><button class='more' aria-expanded='false' aria-controls='{did}' onclick='toggle(this)'>View details</button></td></tr>"
            f"<tr class='detail' id='{did}' hidden><td colspan='8'>{walk_detail(s, i, hops_by_walk, esq_by_walk, slices_by_hop)}</td></tr>")
    walks_table = ("<div class='scroll'><table class='walks'><thead><tr><th>Start node</th><th class='n'>Expected nodes</th>"
                   "<th class='n'>Returned</th><th class='n'>Expected hops</th><th class='n'>Median wall</th>"
                   "<th class='n'>Per hop</th><th>Result</th><th></th></tr></thead><tbody>" + "".join(trs) + "</tbody></table></div>")

    # ---- where the time goes
    request_items = [("Hops (all walks)", hop_total, "#4269d0", f"Hops: {fmt_ms(hop_total)}")]
    if has_post:
        request_items += [(n, post_totals[k], c, f"{n}: {fmt_ms(post_totals[k])}") for k, n, c in POST_PARTS]
    else:
        request_items.append((POST_WALK[1], post_walk_total, POST_WALK[2], f"{POST_WALK[1]}: {fmt_ms(post_walk_total)}"))
    request_chart = hbar_chart(request_items, fmt_ms, "Request time: walk vs after the walk")
    slice_total = sum(slice_totals.values()) or 1
    slice_section = ""
    if slice_totals:
        slice_section = (
            "<h3>Inside slice queries</h3><p class='muted'>For each hop, the slowest slice (it decides when the hop can "
            "continue), split by activity and summed over every hop of every walk.</p>"
            + hbar_chart([(n, slice_totals[k], c, f"{n}: {fmt_ms(slice_totals[k])} ({100 * slice_totals[k] / slice_total:.1f}%)")
                          for k, n, c in SLICE_PARTS], fmt_ms, "Inside slice queries")
            + "<dl class='defs'>"
            "<dt>esQuery (4 page types)</dt><dd>The <code>search()</code> call for each page, same as \"client\" time below, "
            "split by page type.</dd>"
            "<dt>Extract hits → relationships</dt><dd>After each page: <code>GraphQueryUtils.extractRelationships</code> "
            "(parse hits, check the visited set, build paths), the shared relationship budget, and reading the next "
            "<code>search_after</code> key.</dd>"
            "<dt>Build request</dt><dd>Building the <code>SearchRequest</code> for each page, including the terms query "
            "with every URN of the hop.</dd>"
            "<dt>Queue wait</dt><dd>From handing the slice to the pit-worker pool until a thread starts it.</dd>"
            "<dt>Other in slice</dt><dd>Loop bookkeeping between those steps.</dd>"
            "<dt>Wait on slice futures</dt><dd>Hop slice time minus the slowest slice: collecting the futures in "
            "<code>processSliceFutures</code>.</dd></dl>")
        slice_section += took_section(took_totals, crit_pages)
    breakdown = hbar_chart(
        [(name, hop_parts[k], color, f"{name}: {fmt_ms(hop_parts[k])} ({100 * hop_parts[k] / hop_total:.1f}% of hop time)")
         for k, name, color in HOP_PARTS], fmt_ms, "Where hop time goes")

    # ---- page types
    page_items = []
    for key, name, color in PAGE_TYPES:
        rows = by_type.get(key, [])
        if rows:
            t = sum(num(r["client_ms"]) for r in rows)
            page_items.append((name, t, color, f"{name}: {len(rows)} calls, median "
                               f"{fmt_ms(st.median(num(r['client_ms']) for r in rows))}, total {fmt_ms(t)}"))
    page_chart = hbar_chart(page_items, fmt_ms, "Total esQuery time by page type")
    page_rows = "".join(
        f"<tr><td><i class='dot' style='background:{c}'></i>{esc(n)}</td><td class='n'>{fmt_n(len(by_type.get(k, [])))}</td>"
        f"<td class='n'>{100 * len(by_type.get(k, [])) / max(1, len(esq)):.0f}%</td>"
        f"<td class='n'>{fmt_ms(st.median(num(r['client_ms']) for r in by_type[k])) if by_type.get(k) else '–'}</td>"
        f"<td class='n'>{fmt_ms(st.median(num(r['took_ms']) for r in by_type[k])) if by_type.get(k) else '–'}</td>"
        f"<td class='n'>{100 * sum(num(r['client_ms']) for r in by_type.get(k, [])) / esq_total_ms:.0f}%</td></tr>"
        for k, n, c in PAGE_TYPES)
    page_table = ("<table><thead><tr><th>Page type</th><th class='n'>Calls</th><th class='n'>Share</th>"
                  "<th class='n'>Median client</th><th class='n'>Median took</th><th class='n'>Share of time</th></tr></thead>"
                  f"<tbody>{page_rows}</tbody></table>")

    heat_views = [("all", "All pages", lambda r: True), ("first", "First page (page 0)", lambda r: r["page"] == "0"),
                  ("next", "search_after pages (page ≥ 1)", lambda r: r["page"] != "0")]
    heat_parts = []
    for key, name, keep in heat_views:
        cells: Dict[Tuple[str, str], List[Tuple[float, float]]] = defaultdict(list)
        for r in esq:
            if keep(r):
                cells[(bucket(num(r["urns"]), URN_BUCKETS), bucket(num(r["hits"]), HIT_BUCKETS))].append(
                    (num(r["client_ms"]), num(r["took_ms"])))
        if cells:
            heat_parts.append((key, name, heatmap(cells, f"esQuery median latency ({name})")))
    heat = ("<div class='tabs' role='tablist'>" + "".join(
        f"<button role='tab' class='tab' aria-selected='{str(i == 0).lower()}' data-view='heat-{k}' "
        f"onclick='showView(this)'>{esc(n)}</button>" for i, (k, n, _) in enumerate(heat_parts)) + "</div>"
        + "".join(f"<div class='view' id='heat-{k}'{'' if i == 0 else ' hidden'}>{h}</div>"
                  for i, (k, _, h) in enumerate(heat_parts)))

    rng = random.Random(42)
    sample = esq if len(esq) <= 6000 else rng.sample(esq, 6000)
    colors = {k: c for k, _, c in PAGE_TYPES}
    scatter = scatter_loglog(
        [(num(r["urns"]), num(r["client_ms"]), colors[page_type(r)],
          f"{r['family']}:{r['note']} hop {r['hop']} slice {r['slice']} page {r['page']}: "
          f"{r['urns']} URNs, {r['hits']} hits, client {r['client_ms']} ms, took {r['took_ms']} ms") for r in sample],
        "esQuery client latency vs URNs in query", "URNs in the query", "client time") if esq else ""
    # The same calls split into OpenSearch took and the rest of client time (GMS request build/serialise,
    # transfer, OpenSearch body parse, response parse, connection wait).
    split_colors = {"took": "#4269d0", "outside": "#efb118"}
    split_pts = []
    for r in sample:
        if num(r["took_ms"]) < 0:
            continue
        where = (f"{r['family']}:{r['note']} hop {r['hop']} slice {r['slice']} page {r['page']}: "
                 f"{r['urns']} URNs, {r['hits']} hits, client {r['client_ms']} ms")
        outside = max(num(r["client_ms"]) - num(r["took_ms"]), 0)
        split_pts.append((num(r["urns"]), num(r["took_ms"]), split_colors["took"], f"{where}, took {r['took_ms']} ms"))
        split_pts.append((num(r["urns"]), outside, split_colors["outside"], f"{where}, outside took {fmt_ms(outside)}"))
    timed = [r for r in esq if num(r["took_ms"]) >= 0]
    split_lines = [
        ("OpenSearch took", split_colors["took"], log_bin_medians([(num(r["urns"]), num(r["took_ms"])) for r in timed])),
        ("Outside took", split_colors["outside"],
         log_bin_medians([(num(r["urns"]), max(num(r["client_ms"]) - num(r["took_ms"]), 0)) for r in timed]))]
    split_scatter = scatter_loglog(split_pts, "esQuery took and time outside took vs URNs in query",
                                   "URNs in the query", "time per call", lines=split_lines) if split_pts else ""
    split_legend = legend([("OpenSearch took", split_colors["took"]),
                           ("Outside took (client − took)", split_colors["outside"])])
    hits_scatter = scatter_hits(
        [(num(r["hits"]), num(r["client_ms"]), colors[page_type(r)],
          f"{r['family']}:{r['note']} hop {r['hop']} slice {r['slice']} page {r['page']}: "
          f"{r['hits']} hits, {r['urns']} URNs, client {r['client_ms']} ms, took {r['took_ms']} ms") for r in sample],
        "esQuery client latency vs hits in the page") if esq else ""
    # Same pages, with the per-page slice activities GMS times around each call (run 2 onwards).
    slice_colors = {k: c for k, _, c in SLICE_PARTS}
    with_parts = [r for r in sample if r.get("extract_ms", "") != ""]
    part_pts = []
    for r in with_parts:
        where = (f"{r['family']}:{r['note']} hop {r['hop']} slice {r['slice']} page {r['page']}: "
                 f"{r['hits']} hits, {r['urns']} URNs")
        part_pts.append((num(r["hits"]), num(r["client_ms"]), colors[page_type(r)],
                         f"{where}, esQuery client {r['client_ms']} ms"))
        for k, name in (("extract_ms", "extract hits → relationships"), ("build_ms", "build request")):
            if num(r[k]) > 0:
                part_pts.append((num(r["hits"]), num(r[k]), slice_colors[k], f"{where}, {name} {fmt_ms(num(r[k]))}"))
    hits_parts_scatter = scatter_hits(
        part_pts, "Slice time per page vs hits in the page", y_min=0.001, height=440,
        y_name="time per page") if with_parts else ""
    parts_legend = legend([(n, c) for _, n, c in PAGE_TYPES]
                          + [(n, c) for k, n, c in SLICE_PARTS if k in ("extract_ms", "build_ms")])
    gap = [num(r["client_ms"]) - num(r["took_ms"]) for r in esq if num(r["took_ms"]) >= 0]
    big = [r for r in esq if num(r["urns"]) > 10_000]
    big_gap = (st.median(num(r["client_ms"]) for r in big), st.median(num(r["took_ms"]) for r in big)) if big else None

    series = []
    for fam in ("deep100_w1", "deep100_w10", "deep100_w100", "deep100_w1k"):
        per: Dict[int, List[float]] = defaultdict(list)
        for h in hops:
            if h["family"] == fam and h["note"] == "root":
                per[int(h["hop"])].append(num(h["total_ms"]))
        if per:
            pts = [(k, st.median(v)) for k, v in sorted(per.items()) if k <= 100]
            smooth = [(x, st.median(y for _, y in pts[max(0, i - 2): i + 3])) for i, (x, _) in enumerate(pts)]
            series.append((fam, f"width {fam.split('_w')[1]}", family_color(fam), smooth))
    depth_chart = line_chart([(n, c, p) for _, n, c, p in series], "Per-hop latency along 100-hop walks",
                             "hop number") if series else ""
    root_wall = {s.family: s.wall_ms for s in starts if s.note == "root"}
    depth_rows = "".join(
        f"<tr><td><i class='dot' style='background:{c}'></i>{esc(n)}</td>"
        f"<td class='n'>{fmt_ms(st.median(y for _, y in pts))}</td><td class='n'>{fmt_ms(root_wall.get(fam, 0))}</td></tr>"
        for fam, n, c, pts in series)
    depth_table = ("<table><thead><tr><th>Width</th><th class='n'>Median per hop</th><th class='n'>100-hop walk</th></tr></thead>"
                   f"<tbody>{depth_rows}</tbody></table>") if series else ""
    skew = [num(h["slice_max_ms"]) / max(1.0, num(h["slice_min_ms"])) for h in hops if num(h["slice_max_ms"]) > 50]

    ds_html = ""
    if manifest:
        rows = "".join(
            f"<tr><td><i class='dot' style='background:{family_color(f)}'></i>{esc(f)}</td>"
            f"<td class='n'>{fmt_n(r['nodes'])}</td><td class='n'>{fmt_n(r['edges'])}</td>"
            f"<td class='n'>{r['avg_upstreams']}</td><td class='n'>{fmt_n(r['max_downstreams'])}</td>"
            f"<td class='n'>{fmt_n(r['largest_scc'])}</td></tr>" for f, r in manifest["report"].items())
        ds_html = (f"<section id='dataset'><h2>Dataset</h2><p class='muted'>{esc(dataset.name)} · seed {manifest['seed']} · "
                   f"scale {manifest['scale']} · {fmt_n(manifest['nodes'])} datasets · {fmt_n(manifest['edges'])} edges</p>"
                   "<div class='scroll'><table><thead><tr><th>Family</th><th class='n'>Nodes</th><th class='n'>Edges</th>"
                   "<th class='n'>Avg upstreams</th><th class='n'>Max downstreams</th><th class='n'>Largest SCC</th></tr></thead>"
                   f"<tbody>{rows}</tbody></table></div></section>")

    notes = []
    if worst.status != "ok":
        notes.append(f"<b>{esc(worst.label)}</b> returned {fmt_n(worst.returned)} of {fmt_n(worst.expected)} nodes: "
                     f"the walk hit its time budget after {worst.gms_hops:.0f} hops.")
    if esq:
        notes.append(f"Empty follow-up pages (the extra call each slice makes to learn it is finished) are "
                     f"<b>{100 * len(follow) / len(esq):.0f}%</b> of esQuery calls and <b>{follow_share:.0f}%</b> of esQuery time.")
    if big_gap:
        notes.append(f"Queries with more than 10k URNs: median client time {fmt_ms(big_gap[0])} vs OpenSearch took "
                     f"{fmt_ms(big_gap[1])}. Most of the time is spent outside the search itself.")
    if hops:
        notes.append(f"Slice queries are {100 * hop_parts['slices_ms'] / hop_total:.0f}% of hop time. PIT create and delete are "
                     f"{100 * (hop_parts['pit_create_ms'] + hop_parts['pit_delete_ms']) / hop_total:.1f}%.")
    notes.append(f"After the walk: {fmt_ms(post_walk_total)} in total, "
                 f"{100 * post_walk_total / wall_total:.0f}% of wall time across all walks"
                 + ((": " + ", ".join(f"{n.split(' (')[0].lower()} {100 * post_totals[k] / max(post_walk_total, 1):.0f}%"
                                      for k, n, _ in POST_PARTS) + ".") if has_post else "."))
    if slice_totals:
        q = sum(slice_totals[k] for k, _, _ in SLICE_PARTS if k.startswith("q_"))
        notes.append(f"Inside the slowest slice of each hop, esQuery calls are {100 * q / slice_total:.0f}% of the time. "
                     f"Extracting relationships is {100 * slice_totals['extract_ms'] / slice_total:.1f}%, building requests "
                     f"{100 * slice_totals['build_ms'] / slice_total:.1f}% and queue wait {100 * slice_totals['queue_ms'] / slice_total:.1f}%.")
    if took_totals:
        tk = sum(v for k, v in took_totals.items() if k.startswith("took_"))
        ot = sum(v for k, v in took_totals.items() if k.startswith("out_"))
        shares = ", ".join(f"{n.split(' (')[0].lower()} {100 * took_totals['took_' + k] / tot:.0f}%"
                           for k, n, _ in PAGE_TYPES
                           if (tot := took_totals["took_" + k] + took_totals["out_" + k]) > 0)
        notes.append(f"Only <b>{100 * tk / ((tk + ot) or 1):.0f}%</b> of esQuery time in the slowest slices is OpenSearch "
                     f"<code>took</code>; the rest is outside the search execution. Took share by page type: {shares}.")
    shapes = sorted({w.get("query_shape", "total") or "total" for w in walks})
    query_shape = ", ".join(shapes)
    notes_html = "<ul class='notes'>" + "".join(f"<li>{n}</li>" for n in notes) + "</ul>"

    return TEMPLATE.format(
        title=esc(f"Lineage bench · {run.name}"),
        run=esc(run.name),
        cards=cards_html,
        notes=notes_html,
        nvt=nvt,
        n_starts=len(starts),
        walks_table=walks_table,
        breakdown=breakdown,
        request_chart=request_chart,
        slice_section=slice_section,
        query_shape=esc(query_shape),
        page_chart=page_chart,
        page_table=page_table,
        heat=heat,
        scatter=scatter,
        hits_scatter=hits_scatter,
        urn_split=(f"<h3>Took vs time outside took</h3><p class='muted'>The same calls, each drawn twice: OpenSearch's "
                   "own <code>took</code> (blue) and the rest of the client time (amber), which covers GMS building and "
                   "serialising the request, sending it, OpenSearch parsing the body, the response coming back and GMS "
                   "parsing it, plus any wait for one of the 2 connections. The lines are medians over all calls in "
                   "half-decade URN bins. Values under 1 ms sit on the bottom edge.</p>"
                   f"{split_legend}{split_scatter}") if split_scatter else "",
        hits_parts=(f"<h3>Including the other slice times</h3><p class='muted'>The same pages, now also showing "
                    "the two slice activities that GMS times around each call: extracting hits into relationships "
                    "(green) and building the request (teal). The time axis goes down to 0.001 ms because both are "
                    "often far under a millisecond; values below that sit on the bottom edge and exact zeros are left "
                    "out. Queue wait and other slice time are recorded once per slice, not per page, so they "
                    f"aren't shown here.</p>{parts_legend}{hits_parts_scatter}") if hits_parts_scatter else "",
        post_dots=post_dots,
        page_size=f"{PAGE_SIZE:,}",
        page_legend=legend([(n, c) for _, n, c in PAGE_TYPES]),
        gap=esc(f"client − took: median {fmt_ms(st.median(gap))}, p90 {fmt_ms(sorted(gap)[int(0.9 * len(gap))])}." if gap else ""),
        depth_chart=depth_chart,
        depth_table=depth_table,
        skew=esc(f"Slice skew (slowest ÷ fastest slice, hops where the slowest slice took >50 ms): "
                 f"median {st.median(skew):.1f}×, max {max(skew):.0f}×" if skew else ""),
        dataset=ds_html,
        n_esq=fmt_n(len(esq)),
        n_hops=fmt_n(len(hops)),
    )


TEMPLATE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
:root {{ --bg:#f7f7f5; --panel:#fff; --ink:#1d1d1f; --muted:#6b6b70; --line:#e4e4e0; --grid:#ececea; --hl:#f1f3fa;
        --ok:#1f7a3a; --okbg:#e3f3e7; --bad:#a3261b; --badbg:#fbe4e1; --warn:#8a5a00; --warnbg:#fdf0d5; --accent:#3257b8; }}
@media (prefers-color-scheme: dark) {{
  :root {{ --bg:#141416; --panel:#1d1d20; --ink:#ececee; --muted:#9a9aa2; --line:#2e2e33; --grid:#2a2a2f; --hl:#23252c;
          --ok:#7ad69a; --okbg:#1c3325; --bad:#ff9a8f; --badbg:#3a1f1c; --warn:#f2c46b; --warnbg:#3a2f16; --accent:#8fa9f0; }}
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Inter,sans-serif; }}
html {{ scroll-behavior:smooth; }}
.layout {{ display:grid; grid-template-columns:200px minmax(0,1040px); gap:28px; max-width:1300px; margin:0 auto; padding:0 16px; }}
main {{ min-width:0; padding:28px 0 64px; }}
main [id] {{ scroll-margin-top:16px; }}
.toc {{ position:sticky; top:0; align-self:start; max-height:100vh; overflow-y:auto; padding:32px 0 24px; }}
.toc-title {{ color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.06em; margin:0 0 8px 12px; }}
.toc ol {{ list-style:none; margin:0; padding:0; }}
.toc a {{ display:block; padding:5px 10px; margin:1px 0; border-left:2px solid transparent; border-radius:0 6px 6px 0;
          color:var(--muted); text-decoration:none; font-size:13px; line-height:1.35; }}
.toc a:hover {{ color:var(--ink); background:var(--hl); }}
.toc a.active {{ color:var(--accent); border-left-color:var(--accent); background:var(--hl); font-weight:600; }}
@media (max-width:960px) {{ .layout {{ grid-template-columns:minmax(0,1fr); }} .toc {{ display:none; }} }}
h1 {{ font-size:22px; margin:0 0 4px; }} h2 {{ font-size:16px; margin:0 0 4px; }} h3 {{ font-size:14px; margin:18px 0 4px; }}
.muted {{ color:var(--muted); margin:0 0 12px; }} .small {{ font-size:12px; }}
section {{ background:var(--panel); border:1px solid var(--line); border-radius:12px; padding:18px 20px; margin:16px 0; }}
.scroll {{ overflow-x:auto; }}
.cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(210px,1fr)); gap:12px; margin:16px 0; }}
.card {{ background:var(--panel); border:1px solid var(--line); border-radius:12px; padding:14px 16px; }}
.card .k {{ color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.04em; }}
.card .v {{ font-size:26px; font-weight:650; margin:2px 0; font-variant-numeric:tabular-nums; }}
.card .s {{ color:var(--muted); font-size:12px; }}
table {{ width:100%; border-collapse:collapse; font-variant-numeric:tabular-nums; }}
th,td {{ padding:6px 8px; border-bottom:1px solid var(--line); text-align:left; white-space:nowrap; vertical-align:top; }}
th {{ color:var(--muted); font-weight:550; font-size:12px; }} td.n,th.n {{ text-align:right; }}
table.compact th, table.compact td {{ padding:4px 6px; font-size:12px; }}
table.compact th {{ white-space:normal; vertical-align:bottom; }}
tr.detail > td {{ max-width:0; }}
.sub {{ color:var(--muted); font-size:11px; margin-left:17px; }}
tr.main.open td {{ background:var(--hl); border-bottom-color:transparent; }}
tr.detail > td {{ background:var(--hl); white-space:normal; padding:4px 16px 16px; }}
tr.detail.sub > td {{ background:var(--panel); padding:6px 10px 10px; }}
table.slices th, table.slices td {{ font-size:11.5px; }}
.detail-body {{ max-width:960px; }}
button.more {{ font:inherit; font-size:12px; color:var(--accent); background:none; border:1px solid var(--line);
               border-radius:6px; padding:2px 10px; cursor:pointer; }}
button.more:hover, button.more:focus-visible {{ border-color:var(--accent); outline:none; }}
dl.facts {{ display:grid; grid-template-columns:110px 1fr; gap:4px 12px; margin:10px 0 0; font-size:13px; }}
dl.facts dt {{ color:var(--muted); }} dl.facts dd {{ margin:0; overflow-wrap:anywhere; }}
code {{ font-size:12px; }}
.dot {{ display:inline-block; width:9px; height:9px; border-radius:2px; margin-right:8px; vertical-align:0; }}
.badge {{ padding:1px 8px; border-radius:99px; font-size:12px; }}
.badge.ok {{ color:var(--ok); background:var(--okbg); }} .badge.bad {{ color:var(--bad); background:var(--badbg); }}
.badge.warn {{ color:var(--warn); background:var(--warnbg); }}
.legend {{ display:flex; flex-wrap:wrap; gap:6px 16px; margin:4px 0 10px; color:var(--muted); font-size:12px; }}
.legend i {{ display:inline-block; width:10px; height:10px; border-radius:2px; margin-right:6px; vertical-align:-1px; }}
.notes {{ margin:0; padding-left:18px; }} .notes li {{ margin:4px 0; }}
dl.defs {{ display:grid; grid-template-columns:150px 1fr; gap:8px 14px; margin:14px 0 0; font-size:13px; }}
dl.defs dt {{ font-weight:600; }} dl.defs dd {{ margin:0; color:var(--muted); }}
@media (max-width:640px) {{ dl.defs, dl.facts {{ grid-template-columns:1fr; }} }}
svg text {{ fill:var(--ink); font-size:12px; }} svg .tick {{ fill:var(--muted); font-size:11px; }}
svg .axis {{ fill:var(--muted); font-size:12px; }} svg .val {{ fill:var(--muted); font-size:11px; }}
svg .grid {{ stroke:var(--grid); stroke-width:1; }} svg .empty {{ fill:var(--grid); }}
svg .dotp {{ fill:var(--accent); fill-opacity:.8; stroke:var(--panel); stroke-width:1; }}
svg .band {{ fill:var(--grid); opacity:.6; }}
svg .limit {{ stroke:var(--muted); stroke-width:1.2; stroke-dasharray:4 3; }}
.tabs {{ display:flex; gap:6px; margin:4px 0 10px; flex-wrap:wrap; }}
.tab {{ font:inherit; font-size:12px; color:var(--muted); background:none; border:1px solid var(--line); border-radius:99px;
        padding:3px 12px; cursor:pointer; }}
.tab[aria-selected="true"] {{ color:var(--panel); background:var(--accent); border-color:var(--accent); }}
svg .cellv {{ font-size:14px; font-weight:600; }} svg .cells {{ font-size:10.5px; }}
footer {{ color:var(--muted); font-size:12px; text-align:center; margin-top:24px; }}
</style>
<script>
function toggle(btn, label) {{
  var row = document.getElementById(btn.getAttribute('aria-controls'));
  var open = row.hidden;
  row.hidden = !open;
  btn.setAttribute('aria-expanded', String(open));
  label = label || 'details';
  btn.textContent = label === 'details' ? (open ? 'Hide details' : 'View details') : (open ? 'Hide ' + label.toLowerCase() : label);
  btn.closest('tr').classList.toggle('open', open);
}}
document.addEventListener('DOMContentLoaded', function () {{
  var links = Array.prototype.slice.call(document.querySelectorAll('.toc a'));
  var targets = links.map(function (a) {{ return document.getElementById(a.dataset.target); }});
  function update() {{
    // The active section is the last one whose top has passed a line 30% down the viewport.
    var line = window.innerHeight * 0.3, current = 0;
    targets.forEach(function (t, i) {{ if (t && t.getBoundingClientRect().top <= line) current = i; }});
    if (window.innerHeight + window.scrollY >= document.body.scrollHeight - 2) current = targets.length - 1;
    links.forEach(function (a, i) {{
      a.classList.toggle('active', i === current);
      if (i === current) a.setAttribute('aria-current', 'true'); else a.removeAttribute('aria-current');
    }});
  }}
  window.addEventListener('scroll', update, {{ passive: true }});
  window.addEventListener('resize', update);
  update();
}});
function showView(tab) {{
  var group = tab.parentElement;
  group.querySelectorAll('.tab').forEach(function (t) {{
    var on = t === tab;
    t.setAttribute('aria-selected', String(on));
    document.getElementById(t.dataset.view).hidden = !on;
  }});
}}
</script></head>
<body><div class="layout">
<nav class="toc" aria-label="Contents"><div class="toc-title">Contents</div><ol><li><a href="#summary" data-target="summary">Summary</a></li><li><a href="#time" data-target="time">Where the time goes</a></li><li><a href="#starts" data-target="starts">Start nodes</a></li><li><a href="#size" data-target="size">Walk size vs time</a></li><li><a href="#pages" data-target="pages">esQuery pages</a></li><li><a href="#heat" data-target="heat">Latency by query size</a></li><li><a href="#urns" data-target="urns">Latency vs URNs</a></li><li><a href="#hits" data-target="hits">Latency vs hits</a></li><li><a href="#depth" data-target="depth">Depth × width</a></li><li><a href="#dataset" data-target="dataset">Dataset</a></li><li><a href="#notes" data-target="notes">Key observations</a></li></ol></nav>
<main>
<h1>Lineage walk benchmark</h1>
<p class="muted">Run <b>{run}</b> · serial Impact (downstream) walks, degree 1, 2 and 3+, skipCache · query shape <b>{query_shape}</b> · {n_hops} hops · {n_esq} esQuery calls</p>
<div id="summary">{cards}</div>

<section id="time"><h2>Where the time goes</h2>
<p class="muted">Summed over every walk. The first chart splits each request into the walk (all hops) and the time after it.</p>
{request_chart}
<h3>Inside a hop</h3>
{breakdown}
<dl class="defs">
<dt>PIT create</dt><dd>Opening the point-in-time snapshot on the graph index at the start of the hop (<code>ESUtils.computePointInTime</code>).</dd>
<dt>Slice queries</dt><dd>From the PIT being ready until every slice has finished. This is wall time: the 6 slices run in parallel. Each slice loops over pages: the esQuery call, then turning the hits into lineage relationships and checking them against the visited set (<code>GraphQueryUtils.extractRelationships</code>). So the processing right after each esQuery is <b>inside</b> this number, not in Post-process.</dd>
<dt>PIT delete</dt><dd>Closing the PIT at the end of the hop.</dd>
<dt>Post-process</dt><dd>After all slices return: merging the hop's relationships into the walk's result map (deduplicating nodes reached twice) and building the next hop's URN list.</dd>
<dt>Other</dt><dd>The rest of the hop: building the lineage query, cancelling and draining the slice futures, and partial-result bookkeeping.</dd>
<dt>After the walk</dt><dd>Wall time minus GMS walk time, in three parts (run 2 onwards). <b>Search post-processing</b>: in <code>LineageSearchService</code> after the walk, filtering the relationships and building the result page (lightning path in memory, tortoise path via the entity index). <b>Entity client + resolver mapping</b>: the rest of the resolver's call, plus mapping the result. <b>GraphQL hydration + serialisation + HTTP</b>: everything after the resolver returns, including loading the requested fields of the page of results.</dd>
</dl>
{slice_section}</section>

<section id="starts"><h2>Start nodes</h2>
<p class="muted">All {n_starts} start nodes, largest expected walk first. "Expected hops" is the generator's ground truth. <b>View details</b> shows the whole request and every hop of the median repeat.</p>
{walks_table}</section>

<section id="size"><h2>Walk size vs time</h2>
<div class="tabs" role="tablist"><button role="tab" class="tab" aria-selected="true" data-view="size-walk" onclick="showView(this)">Nodes reached vs walk time</button><button role="tab" class="tab" aria-selected="false" data-view="size-post" onclick="showView(this)">Nodes reached vs time after the walk</button></div>
<div class="view" id="size-walk"><p class="muted">One dot per start node (median of repeats), log–log. Hover a dot to see which start node it is. A hollow red dot means the walk was truncated, so it's plotted at the nodes it actually returned.</p>
{nvt}</div>
<div class="view" id="size-post" hidden><p class="muted">Wall time minus GMS walk time, one dot per start node and part (median of repeats), log–log. Values under 1 ms sit on the bottom line.</p>
{post_dots}</div></section>

<section id="pages"><h2>esQuery pages</h2><p class="muted">Each slice sends a first page (page 0), then keeps sending <code>search_after</code> pages until one comes back empty. That last empty page is the "empty follow-up". Page size is {page_size} hits (<code>graphService.limit.results.apiDefault</code>, default).</p>
{page_chart}{page_table}
<dl class="defs">
<dt>Median client</dt><dd>Time GMS measures around the <code>search()</code> call: serialising the request (which lists every URN of the hop), sending it, OpenSearch queueing and running it, sending the response back, and parsing it.</dd>
<dt>Median took</dt><dd>OpenSearch's own <code>took</code>: time spent executing the search inside the cluster. It leaves out the network, request and response serialisation, and client-side parsing.</dd>
<dt>client − took</dt><dd>Overhead outside the search itself. It's small for small queries, and large once a query carries tens of thousands of URNs.</dd>
</dl></section>

<section id="heat"><h2>esQuery latency by query size</h2><p class="muted">Median client time per call (colour and large number), with OpenSearch <code>took</code> and call count. Switch between all pages, first pages only and <code>search_after</code> pages only.</p>
{heat}</section>

<section id="urns"><h2>esQuery latency vs URNs in the query</h2><p class="muted">Sample of calls, log–log. {gap}</p>
{page_legend}{scatter}{urn_split}</section>

<section id="hits"><h2>esQuery latency vs hits in the page</h2><p class="muted">Same sample, log–log. Pages with 0 hits sit in the band on the left. The dashed line is the page size: a full page means the slice has more to fetch.</p>
{page_legend}{hits_scatter}{hits_parts}</section>

<section id="depth"><h2>Depth × width</h2><p class="muted">The four 100-hop families differ only in layer width (5-hop rolling median).</p>
{depth_chart}{depth_table}<p class="muted">{skew}</p></section>

{dataset}

<section id="notes"><h2>Key observations</h2>{notes}</section>
<footer>Generated by perf-test/lineage-bench/report.py</footer>
</main></div></body></html>
"""


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("run", type=Path, help="results/<run_id> directory")
    ap.add_argument("--dataset", type=Path, default=Path(__file__).parent / "dataset" / "v1")
    ap.add_argument("--out", type=Path, default=None, help="default: <run>/report.html")
    args = ap.parse_args(argv)
    out = args.out or args.run / "report.html"
    out.write_text(build(args.run, args.dataset))
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
