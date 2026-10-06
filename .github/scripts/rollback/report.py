"""Markdown and JSON rendering of rollback findings."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

from rollback import model


def _format_pr(pr_number: Optional[str]) -> str:
    if not pr_number:
        return "—"
    return ", ".join(f"#{p.strip()}" for p in pr_number.split(","))


def render_rollback_report(
    findings: list[model.RollbackFinding],
    current: str,
    target: str,
    current_sha: str,
    target_sha: str,
    warning: Optional[str] = None,
) -> str:
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    verdict = model.compute_verdict(findings)

    blockers = [f for f in findings if f.risk == model.BLOCKS_ROLLBACK]
    attention = [f for f in findings if f.risk == model.REQUIRES_ATTENTION]
    safe = [f for f in findings if f.risk == model.SAFE]

    lines = [
        f"# Rollback Compatibility Report: {current} → {target}",
        "",
        f"**Current (N):** `{current}` (sha: `{current_sha[:10]}`)  ",
        f"**Target (N-1):** `{target}` (sha: `{target_sha[:10]}`)  ",
        f"**Generated:** {generated}",
        "",
    ]
    if warning:
        lines += [f"> ⚠️ **Warning:** {warning}", ""]
    lines += [
        f"## Verdict: {model.VERDICT_LABELS[verdict]}",
        "",
        f"> {model.VERDICT_DESCRIPTIONS[verdict]}",
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

    lines.extend(
        [
            "_N-1 read / N-1 write / N-1 data loss: what happens on N-1 to records "
            "N wrote, after rolling back._",
            "",
        ]
    )

    if blockers:
        lines.extend(_render_finding_table("Blockers", blockers))

    if attention:
        lines.extend(_render_finding_table("Requires Attention", attention))

    if safe:
        lines.extend(
            [
                "<details>",
                f"<summary>Safe Changes ({len(safe)})</summary>",
                "",
            ]
        )
        lines.extend(_render_finding_table("Safe Changes", safe, heading=False))
        lines.extend(["</details>", ""])

    mutator_findings = [f for f in findings if f.dimension == model.DIM_MUTATOR]
    if mutator_findings:
        lines.extend(_render_mutator_section(mutator_findings))

    step_findings = [f for f in findings if f.dimension == model.DIM_UPGRADE_STEP]
    if step_findings:
        lines.extend(_render_step_section(step_findings))

    reindex_findings = [f for f in findings if f.reindex_required]
    if reindex_findings:
        lines.extend(_render_reindex_section(reindex_findings))

    version_findings = [f for f in findings if f.dimension == model.DIM_SCHEMA_VERSION]
    if version_findings:
        lines.extend(_render_version_section(version_findings))

    return "\n".join(lines) + "\n"


def _table_cell(text: Optional[str]) -> str:
    """Make free text safe for a single markdown table cell."""
    if not text:
        return ""
    return " ".join(text.split()).replace("|", "\\|")


TABLE_DESCRIPTIONS = {
    "Blockers": (
        "Don't roll back until these are handled: N-1 can't read or write data N wrote."
    ),
    "Requires Attention": (
        "Rollback can go ahead, but check each item first: N-1 may fail on, "
        "drop, or misread some data N wrote. Why / action says what to check."
    ),
    "Safe Changes": "N-1 handles these on its own; no action needed.",
}


def _render_finding_table(
    title: str,
    findings: list[model.RollbackFinding],
    heading: bool = True,
) -> list[str]:
    lines: list[str] = []
    if heading:
        lines.extend([f"## {title}", ""])
    if title in TABLE_DESCRIPTIONS:
        lines.extend([f"_{TABLE_DESCRIPTIONS[title]}_", ""])
    columns = [
        "#",
        "Dimension",
        "Aspect",
        "Risk",
        "Summary",
        "N-1 read",
        "N-1 write",
        "N-1 data loss",
        "Why / action",
        "PR",
    ]
    lines.extend(
        [
            "| " + " | ".join(columns) + " |",
            "| " + " | ".join("---" for _ in columns) + " |",
        ]
    )
    for i, f in enumerate(findings, 1):
        aspect = f"`{f.aspect_name}`" if f.aspect_name else "—"
        cells = [
            str(i),
            f.dimension,
            aspect,
            f.risk,
            f.summary,
            f.read_impact or "",
            f.write_impact or "",
            f.data_loss or "",
        ]
        cells += [_table_cell(f.detail), _format_pr(f.pr_number)]
        lines.append("| " + " | ".join(cells) + " |")
    lines.append("")
    return lines


def _render_mutator_section(findings: list[model.RollbackFinding]) -> list[str]:
    lines = [
        "## Mutators in Window",
        "",
        "| Mutator | Aspect | Version Hop | Risk | PR |",
        "| --- | --- | --- | --- | --- |",
    ]
    for f in findings:
        pr = _format_pr(f.pr_number)
        aspect = f.aspect_name or "—"
        cls = f.subject or "?"
        hop = f.hop or "—"
        lines.append(f"| `{cls}` | {aspect} | {hop} | {f.risk} | {pr} |")
    lines.append("")
    return lines


def _render_step_section(findings: list[model.RollbackFinding]) -> list[str]:
    lines = [
        "## Upgrade Steps in Window",
        "",
        "| Step | Type | Risk | PR |",
        "| --- | --- | --- | --- |",
    ]
    for f in findings:
        pr = _format_pr(f.pr_number)
        cls = f.subject or "?"
        step_type = "Non-blocking" if "NonBlocking" in f.summary else "Blocking"
        lines.append(f"| `{cls}` | {step_type} | {f.risk} | {pr} |")
    lines.append("")
    return lines


def _render_reindex_section(findings: list[model.RollbackFinding]) -> list[str]:
    lines = [
        "## Reindex Triggers",
        "",
        "| Aspect | Field | Reason | PR |",
        "| --- | --- | --- | --- |",
    ]
    for f in findings:
        pr = _format_pr(f.pr_number)
        aspect = _table_cell(f.aspect_name) or "—"
        field = f"{f.record}.{f.subject}" if f.record else f.subject or "—"
        reason = _table_cell(f.detail or f.summary)
        lines.append(f"| {aspect} | `{field}` | {reason} | {pr} |")
    lines.append("")
    return lines


def _render_version_section(findings: list[model.RollbackFinding]) -> list[str]:
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


def render_json_report(
    findings: list[model.RollbackFinding],
    current: str,
    target: str,
    current_sha: str,
    target_sha: str,
    warning: Optional[str] = None,
) -> str:
    verdict = model.compute_verdict(findings)
    data = {
        "current": current,
        "current_sha": current_sha[:10],
        "target": target,
        "target_sha": target_sha[:10],
        "generated": datetime.now(timezone.utc).isoformat(),
        "warning": warning,
        "verdict": verdict,
        "verdict_label": model.VERDICT_LABELS[verdict],
        "summary": {
            "total": len(findings),
            "safe": sum(1 for f in findings if f.risk == model.SAFE),
            "requires_attention": sum(
                1 for f in findings if f.risk == model.REQUIRES_ATTENTION
            ),
            "blocks_rollback": sum(
                1 for f in findings if f.risk == model.BLOCKS_ROLLBACK
            ),
        },
        "findings": [model.public_dict(f) for f in findings],
    }
    return json.dumps(data, indent=2)
