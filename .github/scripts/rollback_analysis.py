#!/usr/bin/env python3
"""
Rollback compatibility report between two DataHub releases (N → N-1).

Analyzes PDL schema changes, aspect migration mutators, upgrade steps, and
schema version gaps to produce a per-change risk classification and a
top-level feasibility verdict for rolling back from N to N-1.

Usage:
    python3 .github/scripts/rollback_analysis.py --current v2.3.0rc7-cloud --target v2.2.3-cloud
    python3 .github/scripts/rollback_analysis.py --current HEAD --target v2.2.3-cloud --output report.md --json
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import report_aspect_changes as rac

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SAFE = "safe"
REQUIRES_ATTENTION = "requires_attention"
BLOCKS_ROLLBACK = "blocks_rollback"

DIM_PDL_SCHEMA = "pdl_schema"
DIM_MUTATOR = "mutator"
DIM_UPGRADE_STEP = "upgrade_step"
DIM_REINDEX = "reindex"
DIM_SCHEMA_VERSION = "schema_version"

VERDICT_FEASIBLE = "feasible_as_is"
VERDICT_MANUAL = "feasible_with_manual_intervention"
VERDICT_NOT_RECOMMENDED = "not_recommended"

VERDICT_LABELS = {
    VERDICT_FEASIBLE: "✅ Feasible as-is",
    VERDICT_MANUAL: "⚠️ Feasible with manual intervention",
    VERDICT_NOT_RECOMMENDED: "\U0001f6d1 Not recommended",
}

VERDICT_DESCRIPTIONS = {
    VERDICT_FEASIBLE: (
        "All changes are safe; rollback N → N-1 requires no manual steps."
    ),
    VERDICT_MANUAL: (
        "Some changes need review or manual action, but none categorically "
        "block rollback."
    ),
    VERDICT_NOT_RECOMMENDED: (
        "One or more changes block rollback without prior remediation."
    ),
}

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class RollbackFinding:
    dimension: str
    risk: str
    path: str
    aspect_name: Optional[str]
    summary: str
    detail: Optional[str] = None
    pr_number: Optional[str] = None
    author: Optional[str] = None
    reindex_required: bool = False


# ---------------------------------------------------------------------------
# Dimension 1 + 4: PDL schema changes (includes reindex triggers)
# ---------------------------------------------------------------------------


def classify_pdl_for_rollback(
    path: str, current: str, target: str
) -> list[RollbackFinding]:
    """Classify field/enum/rename changes for rollback risk.

    Direction is reversed compared to forward-compatibility: a field *added*
    in N means N-1 doesn't know about it.
    """
    findings: list[RollbackFinding] = []
    current_content = rac.file_at(current, path)
    target_content = rac.file_at(target, path)

    cur_meta = rac.aspect_meta(current_content) if current_content else None
    tgt_meta = rac.aspect_meta(target_content) if target_content else None
    aspect_name = (cur_meta or {}).get("name") or (tgt_meta or {}).get("name")

    # Skip non-aspect PDL files (enums, shared records) — they're not stored
    # in metadata_aspect_v2 and don't directly affect rollback. Changes
    # propagate via the aspects that include them (captured by schema version
    # gaps on those aspects).
    if not aspect_name:
        return findings

    pr = _first_pr(current, path, target)
    author = _file_author(current, path, target)

    if current_content and not target_content:
        findings.append(RollbackFinding(
            dimension=DIM_PDL_SCHEMA, risk=SAFE, path=path,
            aspect_name=aspect_name,
            summary="New file in N — absent in N-1 (ignored on rollback)",
            pr_number=pr, author=author,
        ))
        return findings

    if not current_content and target_content:
        findings.append(RollbackFinding(
            dimension=DIM_PDL_SCHEMA, risk=REQUIRES_ATTENTION, path=path,
            aspect_name=aspect_name,
            summary="File deleted in N — N-1 expects it",
            pr_number=pr, author=author,
        ))
        return findings

    if not current_content and not target_content:
        return findings

    # --- Field diff ---
    cur_fields = rac.fields(current_content)
    tgt_fields = rac.fields(target_content)

    for name in sorted(set(cur_fields) - set(tgt_fields)):
        findings.append(RollbackFinding(
            dimension=DIM_PDL_SCHEMA, risk=SAFE, path=path,
            aspect_name=aspect_name,
            summary=f"Added field `{name}` — N-1 ignores unknown fields",
            pr_number=pr, author=author,
        ))

    for name in sorted(set(tgt_fields) - set(cur_fields)):
        findings.append(RollbackFinding(
            dimension=DIM_PDL_SCHEMA, risk=REQUIRES_ATTENTION, path=path,
            aspect_name=aspect_name,
            summary=f"Removed field `{name}` — N-1 expects it",
            detail="Field deletion requires reindex after rollback",
            pr_number=pr, author=author, reindex_required=True,
        ))

    for name in sorted(set(cur_fields) & set(tgt_fields)):
        cur, tgt = cur_fields[name], tgt_fields[name]

        if cur["type"] != tgt["type"]:
            findings.append(RollbackFinding(
                dimension=DIM_PDL_SCHEMA, risk=REQUIRES_ATTENTION, path=path,
                aspect_name=aspect_name,
                summary=f"Type change on `{name}`: `{tgt['type']}`→`{cur['type']}`",
                detail="Type change requires reindex after rollback",
                pr_number=pr, author=author, reindex_required=True,
            ))

        if tgt["optional"] and not cur["optional"]:
            findings.append(RollbackFinding(
                dimension=DIM_PDL_SCHEMA, risk=REQUIRES_ATTENTION, path=path,
                aspect_name=aspect_name,
                summary=f"Optional→required flip on `{name}`",
                detail="N-1 treats field as optional; verify null handling if rolling forward again",
                pr_number=pr, author=author,
            ))

        if not tgt["optional"] and cur["optional"]:
            findings.append(RollbackFinding(
                dimension=DIM_PDL_SCHEMA, risk=SAFE, path=path,
                aspect_name=aspect_name,
                summary=f"Required→optional flip on `{name}` — safe for rollback",
                pr_number=pr, author=author,
            ))

    # --- Enum diff ---
    cur_enums = rac.enums(current_content)
    tgt_enums = rac.enums(target_content)
    for ename in sorted(set(cur_enums) & set(tgt_enums)):
        for v in sorted(set(cur_enums[ename]) - set(tgt_enums[ename])):
            findings.append(RollbackFinding(
                dimension=DIM_PDL_SCHEMA, risk=SAFE, path=path,
                aspect_name=aspect_name,
                summary=f"Enum `{ename}`: added value `{v}` — N-1 ignores it",
                pr_number=pr, author=author,
            ))
        for v in sorted(set(tgt_enums[ename]) - set(cur_enums[ename])):
            findings.append(RollbackFinding(
                dimension=DIM_PDL_SCHEMA, risk=REQUIRES_ATTENTION, path=path,
                aspect_name=aspect_name,
                summary=f"Enum `{ename}`: removed value `{v}` — N-1 may write it",
                pr_number=pr, author=author,
            ))

    # --- Record rename ---
    cur_name = rac.record_name(current_content)
    tgt_name = rac.record_name(target_content)
    if cur_name and tgt_name and cur_name != tgt_name:
        rf = rac.renamed_from(current_content)
        if rf == tgt_name:
            findings.append(RollbackFinding(
                dimension=DIM_PDL_SCHEMA, risk=REQUIRES_ATTENTION, path=path,
                aspect_name=aspect_name,
                summary=(
                    f"Record renamed `{tgt_name}`→`{cur_name}` "
                    f"(renamedFrom set)"
                ),
                pr_number=pr, author=author,
            ))
        else:
            findings.append(RollbackFinding(
                dimension=DIM_PDL_SCHEMA, risk=BLOCKS_ROLLBACK, path=path,
                aspect_name=aspect_name,
                summary=(
                    f"Record renamed `{tgt_name}`→`{cur_name}` "
                    f"without @renamedFrom"
                ),
                pr_number=pr, author=author,
            ))

    return findings


# ---------------------------------------------------------------------------
# Dimension 2: Mutator migrations
# ---------------------------------------------------------------------------

_METHOD_RETURN_INT_RE_CACHE: dict[str, re.Pattern[str]] = {}


def _extract_method_return_int(
    java_content: str, method_name: str
) -> Optional[int]:
    if method_name not in _METHOD_RETURN_INT_RE_CACHE:
        _METHOD_RETURN_INT_RE_CACHE[method_name] = re.compile(
            rf"{method_name}\s*\(\s*\)\s*\{{\s*return\s+(\d+)[Ll]?\s*;",
            re.DOTALL,
        )
    m = _METHOD_RETURN_INT_RE_CACHE[method_name].search(java_content)
    return int(m.group(1)) if m else None


def classify_mutators_for_rollback(
    current: str, target: str
) -> list[RollbackFinding]:
    """Classify new mutators for rollback risk.

    Rollback uses retention + Kafka replay (not reverse transforms). All new
    mutators are flagged as requires_attention — the operator must verify that
    the retention window and Kafka replay cover the mutated data.
    """
    hierarchy = rac.discover_mutator_hierarchy()
    mutators = rac.find_mutators_added_in_window(target, current, hierarchy)

    # Deduplicate by (path, class_name) — same mutator touched by multiple PRs
    # should be a single finding with merged PR numbers.
    seen: dict[tuple[str, str], dict] = {}
    for m in mutators:
        key = (m["path"], m["class_name"])
        if key not in seen:
            seen[key] = {**m, "_prs": []}
        pr = m.get("pr")
        if pr and pr not in seen[key]["_prs"]:
            seen[key]["_prs"].append(pr)

    findings: list[RollbackFinding] = []
    for (path, _cls), m in seen.items():
        content = rac.file_at(current, path)
        if not content:
            continue

        src_v = _extract_method_return_int(content, "getSourceVersion")
        tgt_v = _extract_method_return_int(content, "getTargetVersion")
        hop = f"v{src_v}→v{tgt_v}" if src_v is not None and tgt_v is not None else ""

        aspect = m.get("target_aspect") or "?"
        hop_label = f" ({hop})" if hop else ""
        summary = (
            f"New mutator `{m['class_name']}`{hop_label} — "
            f"verify retention/replay coverage"
        )
        pr_str = ", ".join(m["_prs"]) if m["_prs"] else None

        findings.append(RollbackFinding(
            dimension=DIM_MUTATOR, risk=REQUIRES_ATTENTION, path=path,
            aspect_name=m.get("target_aspect"),
            summary=summary,
            detail=(
                f"Aspect: {aspect}, {hop}. "
                f"Rollback recovers pre-mutation data via retention + Kafka "
                f"replay. Verify the retention window covers all data written "
                f"through this mutator during the N deployment."
            ),
            pr_number=pr_str, author=m.get("author"),
        ))

    return findings


# ---------------------------------------------------------------------------
# Dimension 3: Upgrade steps
# ---------------------------------------------------------------------------

_BLOCKING_STEP = "BlockingSystemUpgrade"
_NON_BLOCKING_STEP = "NonBlockingSystemUpgrade"
_IMPLEMENTS_STEP_RE = re.compile(
    rf"implements\s+(?:{_BLOCKING_STEP}|{_NON_BLOCKING_STEP})\b"
)


def discover_upgrade_step_hierarchy() -> dict[str, str]:
    """Class names that implement BlockingSystemUpgrade or NonBlockingSystemUpgrade.

    Returns {class_name: step_type} mapping. Uses git grep at HEAD.
    """
    hierarchy: dict[str, str] = {}
    for iface in (_BLOCKING_STEP, _NON_BLOCKING_STEP):
        _collect_implementors(iface, iface, hierarchy)
    return hierarchy


def _collect_implementors(
    parent: str, root_iface: str, hierarchy: dict[str, str]
) -> None:
    for pattern in (f"implements {parent}", f"extends {parent}"):
        try:
            out = rac._git("grep", "-l", pattern, "--", "*.java")
        except subprocess.CalledProcessError:
            continue
        for path in out.strip().splitlines():
            if "/test/" in path:
                continue
            try:
                content = Path(rac.REPO_ROOT / path).read_text(encoding="utf-8")
            except OSError:
                continue
            cp = rac._extract_class_and_parent(content)
            if cp and cp[0] not in hierarchy:
                hierarchy[cp[0]] = root_iface
                _collect_implementors(cp[0], root_iface, hierarchy)


def find_upgrade_steps_added_in_window(
    base: str, head: str
) -> list[dict]:
    hierarchy = discover_upgrade_step_hierarchy()
    results: list[dict] = []
    try:
        out = rac._git(
            "log", "--diff-filter=A", "--name-only",
            "--format=COMMIT %H %s", f"{base}..{head}", "--", "*.java",
        )
    except subprocess.CalledProcessError:
        return results

    current_sha: Optional[str] = None
    current_subject: str = ""
    for line in out.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("COMMIT "):
            parts = line.split(" ", 2)
            current_sha = parts[1] if len(parts) > 1 else None
            current_subject = parts[2] if len(parts) > 2 else ""
            continue
        if not line.endswith(".java") or "/test/" in line or current_sha is None:
            continue
        try:
            content = rac._git("show", f"{current_sha}:{line}")
        except subprocess.CalledProcessError:
            continue

        cp = rac._extract_class_and_parent(content)
        if not cp:
            continue
        class_name, parent = cp

        step_type: Optional[str] = None
        m = _IMPLEMENTS_STEP_RE.search(content)
        if m:
            step_type = m.group(0).split()[-1]
        elif parent in hierarchy:
            step_type = hierarchy[parent]

        if not step_type:
            continue

        try:
            author = rac._git(
                "log", "-1", "--format=%an", current_sha
            ).strip() or None
        except subprocess.CalledProcessError:
            author = None

        results.append({
            "sha": current_sha[:10],
            "pr": rac._extract_pr_number(current_subject),
            "path": line,
            "class_name": class_name,
            "step_type": step_type,
            "author": author,
            "subject": current_subject,
        })
    return results


def classify_upgrade_steps_for_rollback(
    current: str, target: str
) -> list[RollbackFinding]:
    steps = find_upgrade_steps_added_in_window(target, current)
    findings: list[RollbackFinding] = []
    for s in steps:
        findings.append(RollbackFinding(
            dimension=DIM_UPGRADE_STEP,
            risk=REQUIRES_ATTENTION,
            path=s["path"],
            aspect_name=None,
            summary=f"New {s['step_type']}: `{s['class_name']}`",
            detail="Verify idempotency and rollback safety",
            pr_number=s.get("pr"),
            author=s.get("author"),
        ))
    return findings


# ---------------------------------------------------------------------------
# Dimension 5: Schema version gap analysis
# ---------------------------------------------------------------------------


def analyze_schema_version_gaps(
    current: str, target: str, pdl_paths: list[str]
) -> list[RollbackFinding]:
    findings: list[RollbackFinding] = []
    for path in pdl_paths:
        cur_content = rac.file_at(current, path)
        tgt_content = rac.file_at(target, path)
        cur_meta = rac.aspect_meta(cur_content) if cur_content else None
        tgt_meta = rac.aspect_meta(tgt_content) if tgt_content else None
        if not cur_meta or not tgt_meta:
            continue
        cur_v = cur_meta.get("schemaVersion") or 1
        tgt_v = tgt_meta.get("schemaVersion") or 1
        if cur_v > tgt_v:
            gap = cur_v - tgt_v
            findings.append(RollbackFinding(
                dimension=DIM_SCHEMA_VERSION,
                risk=REQUIRES_ATTENTION,
                path=path,
                aspect_name=cur_meta.get("name"),
                summary=(
                    f"Schema version gap: v{tgt_v}→v{cur_v} "
                    f"({gap} hop{'s' if gap > 1 else ''})"
                ),
                detail=(
                    f"N-1 expects version {tgt_v}; N writes version {cur_v}. "
                    f"Retention + Kafka replay must cover this version gap."
                ),
                pr_number=_first_pr(current, path, target),
                author=_file_author(current, path, target),
            ))
    return findings


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _first_pr(head: str, path: str, base: str) -> Optional[str]:
    prs = rac.pr_numbers_for_file(head, path, base)
    return prs[0] if prs else None


def _file_author(head: str, path: str, base: str) -> Optional[str]:
    return rac.last_author_for_file(head, path, base)


def _resolve_sha(ref: str) -> str:
    try:
        return rac._git("rev-parse", ref).strip()
    except subprocess.CalledProcessError:
        print(f"Error: could not resolve ref '{ref}'", file=sys.stderr)
        raise SystemExit(2)


# ---------------------------------------------------------------------------
# Verdict
# ---------------------------------------------------------------------------


def compute_verdict(findings: list[RollbackFinding]) -> str:
    risks = {f.risk for f in findings}
    if BLOCKS_ROLLBACK in risks:
        return VERDICT_NOT_RECOMMENDED
    if REQUIRES_ATTENTION in risks:
        return VERDICT_MANUAL
    return VERDICT_FEASIBLE


# ---------------------------------------------------------------------------
# Markdown report
# ---------------------------------------------------------------------------


def _format_pr(pr_number: Optional[str]) -> str:
    if not pr_number:
        return "—"
    return ", ".join(f"#{p.strip()}" for p in pr_number.split(","))


def render_rollback_report(
    findings: list[RollbackFinding],
    current: str,
    target: str,
    current_sha: str,
    target_sha: str,
) -> str:
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    verdict = compute_verdict(findings)

    blockers = [f for f in findings if f.risk == BLOCKS_ROLLBACK]
    attention = [f for f in findings if f.risk == REQUIRES_ATTENTION]
    safe = [f for f in findings if f.risk == SAFE]

    lines = [
        f"# Rollback Compatibility Report: {current} → {target}",
        "",
        f"**Current (N):** `{current}` (sha: `{current_sha[:10]}`)  ",
        f"**Target (N-1):** `{target}` (sha: `{target_sha[:10]}`)  ",
        f"**Generated:** {generated}",
        "",
        f"## Verdict: {VERDICT_LABELS[verdict]}",
        "",
        f"> {VERDICT_DESCRIPTIONS[verdict]}",
        "",
        (
            f"**Summary:** {len(findings)} changes analyzed · "
            f"{len(safe)} safe · {len(attention)} require attention · "
            f"{len(blockers)} block{'s' if len(blockers) == 1 else ''} rollback"
        ),
        "",
        "---",
        "",
    ]

    if not findings:
        lines.append(
            f"No schema, mutator, or upgrade-step changes between "
            f"`{target}` and `{current}`."
        )
        return "\n".join(lines) + "\n"

    if blockers:
        lines.extend(_render_finding_table("Blockers", blockers))

    if attention:
        lines.extend(_render_finding_table("Requires Attention", attention))

    if safe:
        lines.extend([
            "<details>",
            f"<summary>Safe Changes ({len(safe)})</summary>",
            "",
        ])
        lines.extend(
            _render_finding_table("Safe Changes", safe, heading=False)
        )
        lines.extend(["</details>", ""])

    mutator_findings = [f for f in findings if f.dimension == DIM_MUTATOR]
    if mutator_findings:
        lines.extend(_render_mutator_section(mutator_findings))

    step_findings = [f for f in findings if f.dimension == DIM_UPGRADE_STEP]
    if step_findings:
        lines.extend(_render_step_section(step_findings))

    reindex_findings = [f for f in findings if f.reindex_required]
    if reindex_findings:
        lines.extend(_render_reindex_section(reindex_findings))

    version_findings = [f for f in findings if f.dimension == DIM_SCHEMA_VERSION]
    if version_findings:
        lines.extend(_render_version_section(version_findings))

    return "\n".join(lines) + "\n"


def _render_finding_table(
    title: str,
    findings: list[RollbackFinding],
    heading: bool = True,
) -> list[str]:
    lines: list[str] = []
    if heading:
        lines.extend([f"## {title}", ""])
    lines.extend([
        "| # | Dimension | Aspect | Risk | Summary | PR |",
        "| --- | --- | --- | --- | --- | --- |",
    ])
    for i, f in enumerate(findings, 1):
        pr = _format_pr(f.pr_number)
        aspect = f"`{f.aspect_name}`" if f.aspect_name else "—"
        lines.append(
            f"| {i} | {f.dimension} | {aspect} | {f.risk} "
            f"| {f.summary} | {pr} |"
        )
    lines.append("")
    return lines


def _render_mutator_section(findings: list[RollbackFinding]) -> list[str]:
    lines = [
        "## Mutators in Window",
        "",
        "| Mutator | Aspect | Version Hop | Risk | PR |",
        "| --- | --- | --- | --- | --- |",
    ]
    for f in findings:
        pr = _format_pr(f.pr_number)
        aspect = f.aspect_name or "—"
        cls_m = re.search(r"`([^`]+)`", f.summary)
        cls = cls_m.group(1) if cls_m else "?"
        hop_m = re.search(r"\(v\d+→v\d+\)", f.summary)
        hop = hop_m.group(0).strip("()") if hop_m else "—"
        lines.append(f"| `{cls}` | {aspect} | {hop} | {f.risk} | {pr} |")
    lines.append("")
    return lines


def _render_step_section(findings: list[RollbackFinding]) -> list[str]:
    lines = [
        "## Upgrade Steps in Window",
        "",
        "| Step | Type | Risk | PR |",
        "| --- | --- | --- | --- |",
    ]
    for f in findings:
        pr = _format_pr(f.pr_number)
        cls_m = re.search(r"`([^`]+)`", f.summary)
        cls = cls_m.group(1) if cls_m else "?"
        step_type = "Non-blocking" if "NonBlocking" in f.summary else "Blocking"
        lines.append(f"| `{cls}` | {step_type} | {f.risk} | {pr} |")
    lines.append("")
    return lines


def _render_reindex_section(findings: list[RollbackFinding]) -> list[str]:
    lines = [
        "## Reindex Triggers",
        "",
        "| Aspect | Field | Reason | PR |",
        "| --- | --- | --- | --- |",
    ]
    for f in findings:
        pr = _format_pr(f.pr_number)
        aspect = f.aspect_name or "—"
        field_m = re.search(r"`([^`]+)`", f.summary)
        field = field_m.group(1) if field_m else "—"
        reason = f.detail or f.summary
        lines.append(f"| {aspect} | `{field}` | {reason} | {pr} |")
    lines.append("")
    return lines


def _render_version_section(findings: list[RollbackFinding]) -> list[str]:
    lines = [
        "## Schema Version Gaps",
        "",
        "| Aspect | Gap | Detail | PR |",
        "| --- | --- | --- | --- |",
    ]
    for f in findings:
        pr = _format_pr(f.pr_number)
        aspect = f.aspect_name or "—"
        lines.append(f"| {aspect} | {f.summary} | {f.detail or ''} | {pr} |")
    lines.append("")
    return lines


# ---------------------------------------------------------------------------
# JSON report
# ---------------------------------------------------------------------------


def render_json_report(
    findings: list[RollbackFinding],
    current: str,
    target: str,
    current_sha: str,
    target_sha: str,
) -> str:
    verdict = compute_verdict(findings)
    data = {
        "current": current,
        "current_sha": current_sha[:10],
        "target": target,
        "target_sha": target_sha[:10],
        "generated": datetime.now(timezone.utc).isoformat(),
        "verdict": verdict,
        "verdict_label": VERDICT_LABELS[verdict],
        "summary": {
            "total": len(findings),
            "safe": sum(1 for f in findings if f.risk == SAFE),
            "requires_attention": sum(
                1 for f in findings if f.risk == REQUIRES_ATTENTION
            ),
            "blocks_rollback": sum(
                1 for f in findings if f.risk == BLOCKS_ROLLBACK
            ),
        },
        "findings": [asdict(f) for f in findings],
    }
    return json.dumps(data, indent=2)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run(
    current: str, target: str
) -> tuple[list[RollbackFinding], str, str]:
    """Run all analysis dimensions. Returns (findings, current_sha, target_sha)."""
    current_sha = _resolve_sha(current)
    target_sha = _resolve_sha(target)

    findings: list[RollbackFinding] = []

    pdl_paths = rac.changed_pdls(target, current)
    for path in pdl_paths:
        findings.extend(classify_pdl_for_rollback(path, current, target))

    findings.extend(classify_mutators_for_rollback(current, target))
    findings.extend(classify_upgrade_steps_for_rollback(current, target))
    findings.extend(
        analyze_schema_version_gaps(current, target, pdl_paths)
    )

    return findings, current_sha, target_sha


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Rollback compatibility report between two DataHub releases."
        ),
    )
    parser.add_argument(
        "--current", required=True,
        help="Current build (N) — tag, branch, or SHA",
    )
    parser.add_argument(
        "--target", default=None,
        help=(
            "Target build (N-1) — tag, branch, or SHA (default: latest stable "
            "release tag — v*-cloud in acryl-fork repos, v* in OSS DataHub)"
        ),
    )
    parser.add_argument(
        "--output", default=None,
        help="Write markdown report to this file (default: stdout)",
    )
    parser.add_argument(
        "--json", dest="emit_json", action="store_true",
        help="Also emit a JSON report alongside markdown",
    )
    args = parser.parse_args(argv)

    # Same baseline rule as the PDL change report, so both tools agree on N-1.
    if args.target is None:
        args.target = rac.resolve_base()
        print(f"Resolved target (N-1): {args.target}", file=sys.stderr)

    findings, current_sha, target_sha = run(args.current, args.target)

    md = render_rollback_report(
        findings, args.current, args.target, current_sha, target_sha
    )

    if args.output:
        Path(args.output).write_text(md, encoding="utf-8")
        print(f"Report written to {args.output}", file=sys.stderr)
    else:
        print(md)

    if args.emit_json:
        json_path = (args.output or "rollback-report.md").replace(
            ".md", ".json"
        )
        json_out = render_json_report(
            findings, args.current, args.target, current_sha, target_sha
        )
        Path(json_path).write_text(json_out, encoding="utf-8")
        print(f"JSON report written to {json_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
