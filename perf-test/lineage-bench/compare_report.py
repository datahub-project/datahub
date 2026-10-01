#!/usr/bin/env python3
"""Render two lineage-bench runs side by side as one self-contained HTML report.

Usage: python3 compare_report.py results/<run_a> results/<run_b> [--names "Run 3" "Run 4"] [--out compare.html]

Compares walk times per start node, and esQuery client time, OpenSearch took and time outside took
against URNs in the query and hits in the page. Reuses report.py's styles and helpers.
"""

from __future__ import annotations

import argparse
import math
import random
import statistics as st
from collections import Counter, defaultdict
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from report import (PAGE_SIZE, TEMPLATE, Row, Start, build_starts, esc, fmt_ms, fmt_n, legend, load_csv,
                    log_bin_medians, log_ticks, num, svg, text)

COLORS = ("#ff725c", "#4269d0")  # first run, second run
SAMPLE = 3000  # calls drawn per run and chart; medians use every call
Y_MIN = 0.5  # bottom of time axes; took and client time are whole milliseconds, so 0 sits on the bottom line
STYLE = TEMPLATE.split("<style>")[1].split("</style>")[0].replace("{{", "{").replace("}}", "}")
SCRIPT = TEMPLATE.split("<script>")[1].split("</script>")[0].replace("{{", "{").replace("}}", "}")
ROOT = ("prod_like", "root")

METRICS: List[Tuple[str, str, Callable[[Row], float]]] = [
    ("client", "Client time", lambda r: num(r["client_ms"])),
    ("took", "OpenSearch took", lambda r: max(num(r["took_ms"]), 0.0)),
    ("outside", "Outside took", lambda r: max(num(r["client_ms"]) - num(r["took_ms"]), 0.0)),
]


class Run:
    def __init__(self, path: Path, name: str, color: str) -> None:
        self.path, self.name, self.color = path, name, color
        self.walks = load_csv(path / "walks.csv")
        self.esq = load_csv(path / "esquery.csv")
        self.slices = load_csv(path / "slices.csv")
        self.starts = {s.urn: s for s in build_starts(self.walks)}
        self.sample = random.Random(5).sample(self.esq, min(SAMPLE, len(self.esq)))

    def total(self, key: str, rows: Sequence[Row], fn: Callable[[Row], float]) -> float:
        return sum(fn(r) for r in rows) / 1e3

    def totals(self) -> Dict[str, float]:
        not_root = lambda r: (r["family"], r["note"]) != ROOT
        esq_nr = [r for r in self.esq if not_root(r)]
        walks_nr = [w for w in self.walks if not_root(w)]
        return {
            "correct": sum(w["total"] == w["expected_total"] for w in self.walks),
            "walks": len(self.walks),
            "wall": sum(num(w["wall_ms"]) for w in self.walks) / 1e3,
            "wall_nr": sum(num(w["wall_ms"]) for w in walks_nr) / 1e3,
            "gms": sum(num(w["gms_walk_ms"]) for w in self.walks) / 1e3,
            "calls": len(self.esq),
            "empty": sum(r["hits"] == "0" for r in self.esq),
            "empty_client": sum(num(r["client_ms"]) for r in self.esq if r["hits"] == "0") / 1e3,
            "client_nr": sum(num(r["client_ms"]) for r in esq_nr) / 1e3,
            "took_nr": sum(max(num(r["took_ms"]), 0) for r in esq_nr) / 1e3,
            "outside_nr": sum(max(num(r["client_ms"]) - num(r["took_ms"]), 0) for r in esq_nr) / 1e3,
            "extract_nr": sum(num(r["extract_ms"]) for r in esq_nr) / 1e3,
        }


# ---------------------------------------------------------------- charts


def cmp_scatter(runs: Sequence[Run], metric: Callable[[Row], float], x_of: Callable[[Row], float],
                x_name: str, y_name: str, label: str, zero_band: bool) -> str:
    """Sampled calls of each run plus a median line per run, log-log.
    With zero_band, calls with x == 0 go in a band on the left, with their median as a bar."""
    width, height, left, bottom, top, right = 900, 380, 64, 44, 14, 16
    band = 46 if zero_band else 0
    plot_l = left + band
    plot_w, plot_h = width - plot_l - right, height - top - bottom
    xmax = max(x_of(r) for run in runs for r in run.esq)
    ymax = max(metric(r) for run in runs for r in run.esq)
    xh = math.log10(max(xmax, 10)) + 0.1
    yl, yh = math.log10(Y_MIN), math.log10(max(ymax, 10)) + 0.1
    px = lambda v: plot_l + plot_w * math.log10(max(v, 1)) / xh
    py = lambda v: top + plot_h * (1 - (math.log10(max(v, Y_MIN)) - yl) / (yh - yl))
    out = []
    if zero_band:
        out.append(f'<rect x="{left + 4}" y="{top}" width="{band - 12}" height="{plot_h}" class="band"/>')
        out.append(text(left + band / 2 - 2, top + plot_h + 16, "0", "tick", "middle"))
    for t in log_ticks(1, 10 ** xh):
        out.append(f'<line x1="{px(t):.1f}" y1="{top}" x2="{px(t):.1f}" y2="{top + plot_h}" class="grid"/>')
        out.append(text(px(t), top + plot_h + 16, fmt_n(t), "tick", "middle"))
    for t in log_ticks(1, 10 ** yh):
        out.append(f'<line x1="{left}" y1="{py(t):.1f}" x2="{plot_l + plot_w}" y2="{py(t):.1f}" class="grid"/>')
        out.append(text(left - 6, py(t) + 4, fmt_ms(t), "tick", "end"))
    out.append(text(left - 6, py(Y_MIN) + 4, "<1 ms", "tick", "end"))
    if zero_band:
        out.append(f'<line x1="{px(PAGE_SIZE):.1f}" y1="{top}" x2="{px(PAGE_SIZE):.1f}" y2="{top + plot_h}" class="limit"/>')
        out.append(text(px(PAGE_SIZE) - 4, top + 12, f"page size {fmt_n(PAGE_SIZE)}", "val", "end"))
    out.append(text(plot_l + plot_w / 2, height - 6, x_name, "axis", "middle"))
    out.append(text(14, top + plot_h / 2, y_name, "axis", "middle", f'transform="rotate(-90 14 {top + plot_h / 2:.0f})"'))

    rng = random.Random(3)
    for run in runs:
        for r in run.sample:
            x, y = x_of(r), metric(r)
            cx = (left + band / 2 - 2 + rng.uniform(-12, 12)) if (zero_band and x == 0) else px(x) + rng.uniform(-2.5, 2.5)
            tip = f"{run.name}: {r['family']}:{r['note']} hop {r['hop']}, {fmt_n(num(r['urns']))} URNs, {fmt_n(num(r['hits']))} hits, {fmt_ms(y)}"
            out.append(f'<circle cx="{cx:.1f}" cy="{py(y):.1f}" r="2.1" fill="{run.color}" fill-opacity="0.35">'
                       f'<title>{esc(tip)}</title></circle>')
    for run in runs:
        pairs = [(x_of(r), metric(r)) for r in run.esq if not (zero_band and x_of(r) == 0)]
        pts = log_bin_medians(pairs, per_decade=3)
        path = " ".join(f"{px(x):.1f},{py(y):.1f}" for x, y in pts)
        out.append(f'<polyline points="{path}" fill="none" stroke="var(--panel)" stroke-width="5.5" stroke-linejoin="round"/>'
                   f'<polyline points="{path}" fill="none" stroke="{run.color}" stroke-width="2.6" stroke-linejoin="round"/>')
        for x, y in pts:
            out.append(f'<circle cx="{px(x):.1f}" cy="{py(y):.1f}" r="3.6" fill="{run.color}" stroke="var(--panel)" stroke-width="1.5">'
                       f'<title>{esc(f"{run.name}: median {fmt_ms(y)} at ~{fmt_n(x)}")}</title></circle>')
        if zero_band:
            zeros = [metric(r) for r in run.esq if x_of(r) == 0]
            if zeros:
                m = st.median(zeros)
                out.append(f'<line x1="{left + 6}" x2="{left + band - 10}" y1="{py(m):.1f}" y2="{py(m):.1f}" '
                           f'stroke="var(--panel)" stroke-width="6"/>'
                           f'<line x1="{left + 6}" x2="{left + band - 10}" y1="{py(m):.1f}" y2="{py(m):.1f}" '
                           f'stroke="{run.color}" stroke-width="3"><title>{esc(f"{run.name}: {len(zeros):,} empty pages, median {fmt_ms(m)}")}</title></line>')
    return svg(width, height, "".join(out), label)


def walk_dumbbell(runs: Sequence[Run], starts: List[Tuple[str, Start, Optional[Start]]]) -> str:
    """One row per start node: median wall time in each run on a log axis, joined by a line."""
    a, b = runs
    row_h, top, bottom, left, right = 19, 10, 40, 250, 70
    width = 900
    height = top + bottom + row_h * len(starts)
    plot_w = width - left - right
    vals = [s.wall_ms for _, s, _ in starts] + [t.wall_ms for _, _, t in starts if t]
    lo, hi = math.log10(min(vals) * 0.8), math.log10(max(vals) * 1.25)
    px = lambda v: left + plot_w * (math.log10(v) - lo) / (hi - lo)
    out = []
    for t in log_ticks(10 ** lo, 10 ** hi):
        out.append(f'<line x1="{px(t):.1f}" y1="{top}" x2="{px(t):.1f}" y2="{height - bottom}" class="grid"/>')
        out.append(text(px(t), height - bottom + 16, fmt_ms(t), "tick", "middle"))
    out.append(text(left + plot_w / 2, height - 6, "median wall time per walk (log scale)", "axis", "middle"))
    for i, (label, s, t) in enumerate(starts):
        y = top + row_h * i + row_h / 2
        out.append(text(left - 10, y + 4, label, "lab", "end"))
        if t is None:
            continue
        x1, x2 = px(s.wall_ms), px(t.wall_ms)
        out.append(f'<line x1="{x1:.1f}" x2="{x2:.1f}" y1="{y:.1f}" y2="{y:.1f}" stroke="var(--muted)" stroke-width="1.5" stroke-opacity=".6"/>')
        for run, st_, x in ((a, s, x1), (b, t, x2)):
            flag = " (truncated in some repeats)" if st_.truncated else ""
            tip = f"{label} · {run.name}: median {fmt_ms(st_.wall_ms)}, {fmt_n(st_.returned)} of {fmt_n(st_.expected)} returned{flag}"
            if st_.truncated:
                out.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="var(--panel)" stroke="{run.color}" stroke-width="2.2"><title>{esc(tip)}</title></circle>')
            else:
                out.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="{run.color}"><title>{esc(tip)}</title></circle>')
        ch = (t.wall_ms - s.wall_ms) / s.wall_ms * 100
        out.append(text(max(x1, x2) + 10, y + 4, f"{ch:+.0f}%", "val"))
    return svg(width, height, "".join(out), "Median wall time per start node in both runs")


# ---------------------------------------------------------------- tables


def pct(a: float, b: float) -> str:
    return f"{(b - a) / a * 100:+.0f}%" if a else "–"


def totals_table(runs: Sequence[Run]) -> str:
    ta, tb = (r.totals() for r in runs)
    rows = [
        ("Walks with the expected count", f"{ta['correct']} of {ta['walks']}", f"{tb['correct']} of {tb['walks']}", ""),
        ("Harness wall time, all walks", f"{ta['wall']:.0f} s", f"{tb['wall']:.0f} s", pct(ta["wall"], tb["wall"])),
        ("Wall time, excluding prod_like:root", f"{ta['wall_nr']:.0f} s", f"{tb['wall_nr']:.0f} s", pct(ta["wall_nr"], tb["wall_nr"])),
        ("GMS walk time", f"{ta['gms']:.0f} s", f"{tb['gms']:.0f} s", pct(ta["gms"], tb["gms"])),
        ("esQuery calls", f"{ta['calls']:,}", f"{tb['calls']:,}", pct(ta["calls"], tb["calls"])),
        ("Empty pages", f"{ta['empty']:,}", f"{tb['empty']:,}", pct(ta["empty"], tb["empty"])),
        ("Empty-page client time", f"{ta['empty_client']:.0f} s", f"{tb['empty_client']:.0f} s", pct(ta["empty_client"], tb["empty_client"])),
        ("esQuery client time, excluding root", f"{ta['client_nr']:.0f} s", f"{tb['client_nr']:.0f} s", pct(ta["client_nr"], tb["client_nr"])),
        ("– took", f"{ta['took_nr']:.0f} s", f"{tb['took_nr']:.0f} s", pct(ta["took_nr"], tb["took_nr"])),
        ("– outside took", f"{ta['outside_nr']:.0f} s", f"{tb['outside_nr']:.0f} s", pct(ta["outside_nr"], tb["outside_nr"])),
        ("Extract, excluding root", f"{ta['extract_nr']:.0f} s", f"{tb['extract_nr']:.0f} s", pct(ta["extract_nr"], tb["extract_nr"])),
    ]
    a, b = runs
    head = f"<tr><th>Measure</th><th class='n'>{esc(a.name)}</th><th class='n'>{esc(b.name)}</th><th class='n'>Change</th></tr>"
    body = "".join(f"<tr><td>{esc(m)}</td><td class='n'>{x}</td><td class='n'>{y}</td><td class='n'>{c}</td></tr>"
                   for m, x, y, c in rows)
    return f'<div class="scroll"><table>{head}{body}</table></div>'


def walk_table(runs: Sequence[Run], starts: List[Tuple[str, Start, Optional[Start]]]) -> str:
    a, b = runs

    def status(s: Optional[Start]) -> str:
        if s is None:
            return "–"
        if s.truncated:
            return f'<span class="badge bad">{s.truncated} of {len(s.walks)} truncated</span>'
        return '<span class="badge bad">mismatch</span>' if s.mismatched else '<span class="badge ok">ok</span>'

    head = (f"<tr><th>Start node</th><th class='n'>Expected nodes</th><th class='n'>Hops</th>"
            f"<th class='n'>{esc(a.name)} median</th><th class='n'>{esc(b.name)} median</th><th class='n'>Change</th>"
            f"<th>{esc(a.name)}</th><th>{esc(b.name)}</th></tr>")
    body = []
    for label, s, t in starts:
        body.append(f"<tr><td>{esc(label)}</td><td class='n'>{fmt_n(s.expected)}</td><td class='n'>{s.expected_hops}</td>"
                    f"<td class='n'>{fmt_ms(s.wall_ms)}</td><td class='n'>{fmt_ms(t.wall_ms) if t else '–'}</td>"
                    f"<td class='n'>{pct(s.wall_ms, t.wall_ms) if t else '–'}</td><td>{status(s)}</td><td>{status(t)}</td></tr>")
    return f'<div class="scroll"><table class="compact">{head}{"".join(body)}</table></div>'


def ms_cell(ms: float) -> str:
    """Times are whole milliseconds, so a median of 0 means under 1 ms."""
    return "&lt;1 ms" if ms < 1 else fmt_ms(ms)


def bin_table(runs: Sequence[Run], x_of: Callable[[Row], float], bins: Sequence[Tuple[float, float, str]],
              x_label: str) -> str:
    a, b = runs
    head = (f"<tr><th>{esc(x_label)}</th>"
            + "".join(f"<th class='n'>{esc(r.name)} calls</th>" for r in runs)
            + "".join(f"<th class='n'>{esc(m)} {esc(a.name)}</th><th class='n'>{esc(m)} {esc(b.name)}</th><th class='n'>Change</th>"
                      for _, m, _ in METRICS) + "</tr>")
    body = []
    for lo, hi, name in bins:
        per_run = [[r for r in run.esq if lo <= x_of(r) <= hi] for run in runs]
        if not all(per_run):
            continue
        cells = "".join(f"<td class='n'>{len(rows):,}</td>" for rows in per_run)
        for _, _, fn in METRICS:
            ma, mb = (st.median(fn(r) for r in rows) for rows in per_run)
            cells += (f"<td class='n'>{ms_cell(ma)}</td><td class='n'>{ms_cell(mb)}</td>"
                      f"<td class='n'>{pct(ma, mb) if ma >= 1 and mb >= 1 else '–'}</td>")
        body.append(f"<tr><td>{esc(name)}</td>{cells}</tr>")
    return f'<div class="scroll"><table class="compact">{head}{"".join(body)}</table></div>'


URN_BINS = [(1, 1, "1"), (2, 10, "2–10"), (11, 100, "11–100"), (101, 1_000, "101–1k"), (1_001, 5_000, "1k–5k"),
            (5_001, 20_000, "5k–20k"), (20_001, 80_000, "20k–80k"), (80_001, 10**9, ">80k")]
HIT_BINS = [(0, 0, "0 (empty page)"), (1, 10, "1–10"), (11, 100, "11–100"), (101, 1_000, "101–1k"),
            (1_001, 4_999, "1k–5k (short page)"), (5_000, 5_000, "5,000 (full page)")]


def tabbed(prefix: str, charts: Sequence[Tuple[str, str, str]]) -> str:
    tabs = "".join(f'<button role="tab" class="tab" aria-selected="{str(i == 0).lower()}" data-view="{prefix}-{key}" '
                   f'onclick="showView(this)">{esc(name)}</button>' for i, (key, name, _) in enumerate(charts))
    views = "".join(f'<div class="view" id="{prefix}-{key}"{"" if i == 0 else " hidden"}>{body}</div>'
                    for i, (key, name, body) in enumerate(charts))
    return f'<div class="tabs" role="tablist">{tabs}</div>{views}'


# ---------------------------------------------------------------- page


def build(runs: Sequence[Run], title: str, note: str) -> str:
    a, b = runs
    counts = Counter(s.label for s in a.starts.values())
    starts = [(s.label if counts[s.label] == 1 else f"{s.label} · {s.name}", s, b.starts.get(urn))
              for urn, s in a.starts.items()]
    starts.sort(key=lambda x: -x[1].wall_ms)
    run_legend = legend([(r.name, r.color) for r in runs])
    ta, tb = (r.totals() for r in runs)
    root_a, root_b = (next((s for s in r.starts.values() if (s.family, s.note) == ROOT), None) for r in runs)

    def card(k: str, v: str, s: str) -> str:
        return f'<div class="card"><div class="k">{esc(k)}</div><div class="v">{v}</div><div class="s">{s}</div></div>'

    cards = '<div class="cards">' + "".join([
        card("Wall time excl. root walk", f"{pct(ta['wall_nr'], tb['wall_nr'])}", f"{ta['wall_nr']:.0f} s → {tb['wall_nr']:.0f} s"),
        card("esQuery calls", f"{pct(ta['calls'], tb['calls'])}", f"{ta['calls']:,} → {tb['calls']:,}"),
        card("esQuery client time excl. root", f"{pct(ta['client_nr'], tb['client_nr'])}",
             f"{ta['client_nr']:.0f} s → {tb['client_nr']:.0f} s"),
        card("Correct walks", f"{ta['correct']} → {tb['correct']}", f"of {tb['walks']}"),
    ] + ([card("prod_like:root", f"{fmt_ms(root_a.wall_ms)} → {fmt_ms(root_b.wall_ms)}",
               f"returned {fmt_n(root_a.returned)} → {fmt_n(root_b.returned)} of {fmt_n(root_b.expected)} (median of repeats)")]
         if root_a and root_b else [])) + "</div>"

    x_urns = lambda r: num(r["urns"])
    x_hits = lambda r: num(r["hits"])
    urn_charts = [(key, name, cmp_scatter(runs, fn, x_urns, "URNs in the query", name.lower(), f"{name} vs URNs in the query", False))
                  for key, name, fn in METRICS]
    hit_charts = [(key, name, cmp_scatter(runs, fn, x_hits, "hits returned in the page", name.lower(), f"{name} vs hits in the page", True))
                  for key, name, fn in METRICS]

    toc = [("summary", "Summary"), ("walks", "Walk times"), ("urns", "esQuery vs URNs"), ("hits", "esQuery vs hits")]
    toc_html = "".join(f'<li><a href="#{k}" data-target="{k}">{esc(n)}</a></li>' for k, n in toc)
    sample_note = (f"Dots are a random sample of {SAMPLE:,} calls per run; lines are medians over every call "
                   f"({a.name}: {len(a.esq):,}, {b.name}: {len(b.esq):,}), in bins of a third of a decade. "
                   f"Times are whole milliseconds, so calls under 1 ms sit on the bottom line.")
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(title)}</title><style>{STYLE}</style><script>{SCRIPT}</script></head>
<body><div class="layout">
<nav class="toc" aria-label="Contents"><div class="toc-title">Contents</div><ol>{toc_html}</ol></nav>
<main>
<h1>{esc(title)}</h1>
<p class="muted"><b>{esc(a.name)}</b> ({esc(a.path.name)}) vs <b>{esc(b.name)}</b> ({esc(b.path.name)}) · {esc(note)}</p>
<div id="summary">{cards}
<section><h2>Totals</h2><p class="muted">prod_like:root returns more relationships when it completes, so the rows that exclude it are the fair comparison for esQuery time.</p>
{totals_table(runs)}</section></div>

<section id="walks"><h2>Walk times</h2><p class="muted">Median wall time of the repeats for each start node, slowest in {esc(a.name)} first. A hollow dot means the walk was truncated in at least one repeat. Hover a dot for details.</p>
{run_legend}{walk_dumbbell(runs, starts)}
<h3>Per start node</h3>{walk_table(runs, starts)}</section>

<section id="urns"><h2>esQuery latency vs URNs in the query</h2><p class="muted">Every esQuery call, whatever its page type. {sample_note} <b>Outside took</b> is client time minus OpenSearch took: serialising the request, sending it, OpenSearch parsing it, the response, and GMS parsing it.</p>
{run_legend}{tabbed("urn", urn_charts)}
<h3>Medians by URNs in the query</h3>{bin_table(runs, x_urns, URN_BINS, "URNs in the query")}
<p class="muted small">These medians cover every call, so they also reflect which calls a run sends. Where the first run sent many empty follow-up pages (each costing a full query in OpenSearch), part of the drop between runs comes from those calls no longer being made, not from each call getting cheaper. The "hits in the page" table below keeps pages with and without hits apart.</p></section>

<section id="hits"><h2>esQuery latency vs hits in the page</h2><p class="muted">Same calls, against hits returned. Empty pages sit in the band on the left, with a bar at each run's median. The dashed line is the page size. {sample_note}</p>
{run_legend}{tabbed("hit", hit_charts)}
<h3>Medians by hits in the page</h3>{bin_table(runs, x_hits, HIT_BINS, "Hits in the page")}</section>
<footer>Generated by perf-test/lineage-bench/compare_report.py</footer>
</main></div></body></html>
"""


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("run_a", type=Path)
    ap.add_argument("run_b", type=Path)
    ap.add_argument("--names", nargs=2, default=None, metavar=("A", "B"))
    ap.add_argument("--note", default="same data, start nodes and repeats", help="subtitle: what differs between the runs")
    ap.add_argument("--out", type=Path, default=None, help="default: <run_b>/compare_<run_a>.html")
    args = ap.parse_args(argv)
    names = args.names or [args.run_a.name, args.run_b.name]
    runs = [Run(args.run_a, names[0], COLORS[0]), Run(args.run_b, names[1], COLORS[1])]
    out = args.out or args.run_b / f"compare_{args.run_a.name}.html"
    out.write_text(build(runs, f"{names[0]} vs {names[1]}", args.note))
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
