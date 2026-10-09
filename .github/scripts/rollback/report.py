"""Markdown and JSON rendering of rollback findings."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Optional

from rollback import model


def _format_pr(pr_number: Optional[str]) -> str:
    if not pr_number:
        return "—"
    return ", ".join(f"#{p.strip()}" for p in pr_number.split(","))


def _format_changes(f: model.RollbackFinding, limit: int = 3) -> str:
    """Links to the PRs (or commits, when a commit has no PR) that changed the
    finding's file on N's first-parent history."""
    if not f.commits:
        return _format_pr(f.pr_number)
    refs: list[str] = []
    seen: set[str] = set()
    for c in f.commits:
        if c.get("pr"):
            if c["pr"] in seen:
                continue
            seen.add(c["pr"])
            text, url = f"#{c['pr']}", c.get("pr_url")
        else:
            text, url = f"`{c['sha'][:10]}`", c.get("url")
        refs.append(f"[{text}]({url})" if url else text)
    more = len(refs) - limit
    return ", ".join(refs[:limit]) + (f" +{more} more" if more > 0 else "")


def _verdict_details(findings: list[model.RollbackFinding]) -> list[str]:
    """One line per risk level present, saying what it covers and what to do."""

    def count(risk: str) -> list[model.RollbackFinding]:
        return [f for f in findings if f.risk == risk]

    lines = []
    blockers = count(model.BLOCKS_ROLLBACK)
    if blockers:
        lines.append(
            f"- **{len(blockers)} block rollback:** N-1 can't read or write data N "
            "wrote. Fix the records on N before rolling back (see Blockers)."
        )
    attention = count(model.REQUIRES_ATTENTION)
    if attention:
        kinds = {
            model.DIM_PDL_SCHEMA: "schema change",
            model.DIM_MUTATOR: "mutator",
            model.DIM_UPGRADE_STEP: "upgrade step",
            model.DIM_SCHEMA_VERSION: "unexplained version bump",
            model.DIM_EVENT_SCHEMA: "Kafka event schema change",
        }
        parts = []
        for dim, label in kinds.items():
            n = sum(1 for f in attention if f.dimension == dim)
            if n:
                parts.append(f"{n} {label}{'s' if n > 1 else ''}")
        lines.append(
            f"- **{len(attention)} need a decision:** {', '.join(parts)}. Check each "
            "item's Why / action before rolling back (see Requires Attention)."
        )
    loss = count(model.EXPECTED_LOSS)
    if loss:
        lines.append(
            f"- **{len(loss)} expected loss:** data of N's new features that N-1 "
            "drops or can't use. No decision needed; see Expected Loss for what "
            "is removed."
        )
    safe = count(model.SAFE)
    if safe:
        lines.append(f"- **{len(safe)} safe:** N-1 handles these on its own.")
    return lines


def _names(items: list[str], limit: int = 6) -> str:
    unique = list(dict.fromkeys(items))
    shown = ", ".join(f"`{x}`" for x in unique[:limit])
    return shown + (f" +{len(unique) - limit} more" if len(unique) > limit else "")


def _n1_summary(findings: list[model.RollbackFinding]) -> list[str]:
    """What N-1 can read, write and lose after the rollback, built from the
    rows' impact columns so it always matches them."""
    schema = [f for f in findings if f.dimension == model.DIM_PDL_SCHEMA]
    if not schema:
        return []

    def where(f: model.RollbackFinding) -> list[str]:
        # A nested record's row shows a shortened list of the aspects using it;
        # an aspect's own row names it, plus any aspects that embed it.
        own = (
            f.aspect_name
            if f.aspect_name
            and ", " not in f.aspect_name
            and "more" not in f.aspect_name
            else None
        )
        return ([own] if own else []) + list(f.affected_aspects)

    def new_entity(f: model.RollbackFinding) -> bool:
        return "entity type new in N" in f.summary

    entities = [f.subject for f in schema if new_entity(f) and f.subject]
    read_fails = [
        a
        for f in schema
        if f.read_impact in (model.API_FAILS, model.UI_API_FAILS) and not new_entity(f)
        for a in where(f)
    ]
    write_fails = [
        a
        for f in schema
        if f.write_impact == model.FAILS and not f.summary.startswith("New file in N")
        for a in where(f)
    ]

    read = "works"
    if read_fails or entities:
        parts = []
        if read_fails:
            parts.append(
                f"entities whose records use N's new values or lack a field N-1 requires ({_names(read_fails)})"
            )
        if entities:
            kind = "types" if len(set(entities)) > 1 else "type"
            parts.append(f"the new entity {kind} {_names(entities)}")
        read += ", except " + " and ".join(parts)
    write = "works for data valid under N-1's schema"
    if write_fails:
        write += (
            f"; fails for records holding N-only values or targets in {_names(write_fails)}, "
            "until those values are removed (or the aspect is deleted and re-emitted)"
        )

    losses = []
    dropped = [f for f in schema if f.write_impact == model.DROPS_NEW_FIELD]
    if dropped:
        losses.append(
            f"N's {len(dropped)} new fields are dropped when N-1 saves those records"
            if len(dropped) > 1
            else "N's new field is dropped when N-1 saves those records"
        )
    numbers = [
        a
        for f in schema
        if f.data_loss
        in (model.LOSS_IF_OUT_OF_RANGE, model.LOSS_FRACTIONS, model.LOSS_PRECISION)
        for a in where(f)
    ]
    if numbers:
        losses.append(f"numbers may be truncated or rounded in {_names(numbers)}")
    removed = [a for f in schema if f.data_loss == model.LOSS_YES for a in where(f)]
    if removed:
        losses.append(f"values of fields N removed are gone in {_names(removed)}")
    graph = [
        a for f in schema if f.data_loss == model.LOSS_GRAPH_ONLY for a in where(f)
    ]
    if graph:
        losses.append(
            f"some graph edges from {_names(graph)} are missing until N-1 saves those records"
        )
    loss = "; ".join(losses) if losses else "none"
    if any(f.summary.startswith("New file in N") for f in schema):
        loss += ". N's new aspects and entity types stay in the database"

    lines = [
        "**On N-1 after the rollback:**",
        "",
        f"- **Read:** {read}.",
        f"- **Write:** {write}.",
        f"- **Data loss:** {loss}.",
    ]
    unknown = [
        f
        for f in findings
        if model.NOT_ANALYSED in (f.read_impact, f.write_impact, f.data_loss)
        or model.UNKNOWN in (f.read_impact, f.write_impact, f.data_loss)
    ]
    if unknown:
        lines.append(
            f"- **Not analysed:** {len(unknown)} item{'s' if len(unknown) > 1 else ''} "
            "whose effect the tool can't determine; see Requires Attention."
        )
    return lines + [""]


def _removed_item(f: model.RollbackFinding) -> str:
    """Short name for what an expected-loss finding removes, e.g.
    "field `Rec.f`", "enum value `E.V`", "the whole aspect"."""
    body = f.change.split(": ", 1)[1] if f.record else f.change
    qualified = f"{f.record}.{f.subject}" if f.record else f.subject
    if body.startswith("New file in N"):
        if "entity type new in N" in f.summary:
            return f"the whole `{f.subject}` entity (new in N)"
        return "the whole aspect"
    if body.startswith("Added field"):
        return f"field `{qualified}`"
    value = re.search(r"added (?:value|member) `([^`]+)`", body)
    if value and body.startswith("Enum"):
        return f"enum value `{f.subject}.{value.group(1)}`"
    if value and body.startswith("Union"):
        return f"union member `{f.subject}.{value.group(1)}`"
    types = body.partition("gained target types ")[2]
    if types:
        return f"{types} targets on `{qualified}`"
    return body[0].lower() + body[1:]


def _removed_by_rollback(findings: list[model.RollbackFinding]) -> list[str]:
    """Expected-loss changes grouped by aspect: what N added that rollback removes."""
    by_aspect: dict[str, list[str]] = {}
    for f in findings:
        by_aspect.setdefault(f.aspect_name or "(shared types)", []).append(
            _removed_item(f)
        )
    return [f"- `{aspect}`: {', '.join(items)}" for aspect, items in by_aspect.items()]


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
    loss = [f for f in findings if f.risk == model.EXPECTED_LOSS]
    safe = [f for f in findings if f.risk == model.SAFE]

    lines = [
        f"# Rollback Compatibility Report: {current} → {target}",
        "",
        f"**Current (N):** `{current}` (sha: `{current_sha[:10]}`)  ",
        f"**Target (N-1):** `{target}` (sha: `{target_sha[:10]}`)  ",
        f"**Generated:** {generated}",
        "",
        (
            "_Assumes the rollback runs N-1's system-update with a new "
            "`DATAHUB_REVISION`, so its blocking step re-applies N-1's index mappings, "
            "and then restore-indices._"
        ),
        "",
    ]
    if warning:
        lines += [f"> ⚠️ **Warning:** {warning}", ""]
    lines += [
        f"## Verdict: {model.VERDICT_LABELS[verdict]}",
        "",
        f"> {model.VERDICT_DESCRIPTIONS[verdict]}",
        "",
        *_verdict_details(findings),
        "",
        *_n1_summary(findings),
        (
            f"**Summary:** {len(findings)} changes analyzed · "
            f"{len(safe)} safe · {len(loss)} expected loss · "
            f"{len(attention)} require attention · "
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

    if loss:
        lines.extend(
            [
                "## Expected Loss",
                "",
                f"_{TABLE_DESCRIPTIONS['Expected Loss']}_",
                "",
                "**Removed by rollback:**",
                "",
                *_removed_by_rollback(loss),
                "",
                "<details>",
                f"<summary>Expected loss details ({len(loss)})</summary>",
                "",
            ]
        )
        lines.extend(
            _render_finding_table("Expected Loss", loss, heading=False, describe=False)
        )
        lines.extend(["</details>", ""])

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
    "Expected Loss": (
        "Rolling back removes N's new features: N-1 drops or can't use the data "
        "below. This is the normal effect of a rollback and needs no decision, "
        "but records that hold new values or targets fail on N-1 until they're "
        "rewritten without them."
    ),
    "Safe Changes": "N-1 handles these on its own; no action needed.",
}


def _render_finding_table(
    title: str,
    findings: list[model.RollbackFinding],
    heading: bool = True,
    describe: bool = True,
) -> list[str]:
    lines: list[str] = []
    if heading:
        lines.extend([f"## {title}", ""])
    if describe and title in TABLE_DESCRIPTIONS:
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
        "Change",
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
        cells += [_table_cell(f.detail), _format_changes(f)]
        lines.append("| " + " | ".join(cells) + " |")
    lines.append("")
    return lines


def _render_mutator_section(findings: list[model.RollbackFinding]) -> list[str]:
    lines = [
        "## Mutators in Window",
        "",
        "| Mutator | Aspect | Version Hop | Risk | Change |",
        "| --- | --- | --- | --- | --- |",
    ]
    for f in findings:
        pr = _format_changes(f)
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
        "| Step | Type | Risk | Change |",
        "| --- | --- | --- | --- |",
    ]
    for f in findings:
        pr = _format_changes(f)
        cls = f.subject or "?"
        step_type = "Non-blocking" if "NonBlocking" in f.summary else "Blocking"
        lines.append(f"| `{cls}` | {step_type} | {f.risk} | {pr} |")
    lines.append("")
    return lines


def _render_reindex_section(findings: list[model.RollbackFinding]) -> list[str]:
    lines = [
        "## Reindex Triggers",
        "",
        "| Aspect | Field | Reason | Change |",
        "| --- | --- | --- | --- |",
    ]
    for f in findings:
        pr = _format_changes(f)
        aspect = _table_cell(f.aspect_name) or "—"
        field = f"{f.record}.{f.subject}" if f.record else f.subject or "—"
        reason = _table_cell(f.detail or f.summary)
        lines.append(f"| {aspect} | `{field}` | {reason} | {pr} |")
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
            "expected_loss": sum(1 for f in findings if f.risk == model.EXPECTED_LOSS),
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
