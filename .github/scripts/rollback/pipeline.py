"""Run all analysis dimensions and the stages that combine their results."""

from __future__ import annotations

from typing import Optional

import report_aspect_changes as rac

from rollback import java_scan, model, pdl_rules, repo


def set_mutator_impact(findings: list[model.RollbackFinding]) -> None:
    """A mutator only reshapes records into N's schema, so N-1 sees its output
    as that aspect's field changes. Use the worst of those."""
    for m in findings:
        if m.dimension != model.DIM_MUTATOR:
            continue
        fields = [
            f
            for f in findings
            if f.dimension == model.DIM_PDL_SCHEMA
            and (f.aspect_name == m.aspect_name or m.aspect_name in f.affected_aspects)
        ]
        if fields:
            m.read_impact = model.worst(
                [f.read_impact for f in fields], model.READ_SEVERITY
            )
            m.write_impact = model.worst(
                [f.write_impact for f in fields], model.WRITE_SEVERITY
            )
            m.data_loss = model.worst(
                [f.data_loss for f in fields], model.LOSS_SEVERITY
            )
        else:
            # A mutator can rewrite values without changing the schema.
            m.read_impact = m.write_impact = m.data_loss = "unknown"
        m.detail = _mutator_detail(fields, m)


def _mutator_detail(
    fields: list[model.RollbackFinding], m: model.RollbackFinding
) -> str:
    gate = "Only runs when ASPECT_MIGRATION_MUTATOR_ENABLED is on (off by default)."
    if not fields:
        return (
            "No schema change found for this aspect, so the impact depends on "
            "what the mutator rewrites; check its transform. If you roll back "
            "with the Option F restore, check the aspect's version history "
            f"covers records this mutator changed. {gate}"
        )
    changes = "; ".join(
        f.summary.split(" — ")[0][0].lower() + f.summary.split(" — ")[0][1:]
        for f in fields
    )
    if {m.read_impact, m.write_impact, m.data_loss} & {"not analysed", "unknown"}:
        effect = "Some of these changes couldn't be analysed; check the PR."
    elif "fails" in (m.read_impact or "") or m.write_impact == "fails":
        effect = "N-1 can't read or write the records it converts."
    elif m.write_impact == model.DROPS_NEW_FIELD:
        effect = "N-1 drops the new field when it saves a record."
    else:
        effect = "N-1 reads and writes them fine."
    # Why it needs attention: the ZDU rollback plan (Option F) restores
    # pre-mutation versions from aspect history, which must still hold them.
    restore = (
        "If you roll back with the Option F restore, check the aspect's "
        "version history covers records this mutator changed."
    )
    return f"Converts records to N's shape: {changes}. {effect} {restore} {gate}"


def set_upgrade_step_impact(
    findings: list[model.RollbackFinding], current: str, target: str
) -> None:
    """Derive a step's impact from the aspects it touches: unknown to N-1
    breaks N-1's restore-indices; otherwise the worst of that aspect's field
    changes, or ok if it didn't change."""
    steps = [f for f in findings if f.dimension == model.DIM_UPGRADE_STEP]
    if not steps:
        return
    constants = repo.aspect_name_constants()
    n1_aspects = repo.aspect_names_at(target)
    for step in steps:
        # Steps record their own runs in these; not a data change.
        aspects = sorted(
            java_scan.step_aspects(step.path, current, constants)
            - {"dataHubUpgradeRequest", "dataHubUpgradeResult"}
        )
        if not aspects:
            step.detail = (
                "Couldn't tell which aspects this step writes. Verify it is "
                "safe to leave applied after rollback."
            )
            continue
        reads, writes, losses, notes = [], [], [], []
        for a in aspects:
            if a not in n1_aspects:
                reads.append("restore-indices fails")
                writes.append("fails")
                notes.append(f"`{a}` (not in N-1)")
                continue
            related = [
                f
                for f in findings
                if f.dimension == model.DIM_PDL_SCHEMA
                and (f.aspect_name == a or a in f.affected_aspects)
            ]
            reads += [f.read_impact for f in related] or ["ok"]
            writes += [f.write_impact for f in related]
            losses += [f.data_loss for f in related]
            notes.append(f"`{a}`" + (" (changed in N)" if related else ""))
        step.read_impact = model.worst(reads, model.READ_SEVERITY)
        step.write_impact = model.worst(writes, model.WRITE_SEVERITY)
        step.data_loss = model.worst(losses, model.LOSS_SEVERITY)
        step.detail = (
            f"Touches {', '.join(notes)}. Impact is for these aspects as a "
            f"whole (including changes the step itself doesn't make); still "
            f"verify the step's logic is safe to leave applied after rollback."
        )


def flag_unexplained_version_gaps(findings: list[model.RollbackFinding]) -> None:
    """A version bump with no top-level field change means the change is in a
    nested or shared record, which this tool doesn't analyse."""
    explained: set[Optional[str]] = set()
    for f in findings:
        if f.dimension == model.DIM_PDL_SCHEMA:
            explained.add(f.aspect_name)
            explained.update(f.affected_aspects)
    for g in findings:
        if g.dimension != model.DIM_SCHEMA_VERSION or g.aspect_name in explained:
            continue
        g.risk = model.REQUIRES_ATTENTION
        g.read_impact = g.write_impact = g.data_loss = "not analysed"
        g.detail = (
            "Version bumped but no change found in this aspect or the records "
            "it uses (the tool doesn't parse typerefs or unions). Check the PR "
            "for what changed."
        )


def run(current: str, target: str) -> tuple[list[model.RollbackFinding], str, str]:
    """Run all analysis dimensions. Returns (findings, current_sha, target_sha)."""
    current_sha = repo.resolve_sha(current)
    target_sha = repo.resolve_sha(target)

    findings: list[model.RollbackFinding] = []

    pdl_paths = rac.changed_pdls(target, current)
    read = repo.cached_reader()
    for path in pdl_paths:
        findings.extend(
            pdl_rules.classify_pdl_for_rollback(path, current, target, read)
        )
    findings.extend(pdl_rules.analyze_nested_changes(current, target, pdl_paths, read))
    pdl_rules.attribute_embedded_aspect_changes(findings, current, target, pdl_paths)

    findings.extend(java_scan.classify_mutators_for_rollback(current, target))
    findings.extend(java_scan.classify_upgrade_steps_for_rollback(current, target))
    findings.extend(pdl_rules.analyze_schema_version_gaps(current, target, pdl_paths))
    set_mutator_impact(findings)
    set_upgrade_step_impact(findings, current, target)
    flag_unexplained_version_gaps(findings)

    return findings, current_sha, target_sha
