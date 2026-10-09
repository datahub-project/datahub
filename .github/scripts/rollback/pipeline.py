"""Run all analysis dimensions and the stages that combine their results."""

from __future__ import annotations

from typing import Optional

import report_aspect_changes as rac

from rollback import java_scan, model, pdl_rules, repo


def _aspect_changes(
    findings: list[model.RollbackFinding], aspect: Optional[str]
) -> list[model.RollbackFinding]:
    """Schema changes that reach `aspect`, plus its version bump when no
    change explains it (that bump's impact is "not analysed")."""
    return [
        f
        for f in findings
        if (f.aspect_name == aspect or aspect in f.affected_aspects)
        and (
            f.dimension == model.DIM_PDL_SCHEMA
            or (
                f.dimension == model.DIM_SCHEMA_VERSION
                and f.read_impact == model.NOT_ANALYSED
            )
        )
    ]


def set_mutator_impact(findings: list[model.RollbackFinding]) -> None:
    """A mutator only reshapes records into N's schema, so N-1 sees its output
    as that aspect's field changes. Use the worst of those."""
    for m in findings:
        if m.dimension != model.DIM_MUTATOR:
            continue
        fields = _aspect_changes(findings, m.aspect_name)
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
            m.read_impact = m.write_impact = m.data_loss = model.UNKNOWN
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
    changes = "; ".join(f.change[0].lower() + f.change[1:] for f in fields)
    if {m.read_impact, m.write_impact, m.data_loss} & {
        model.NOT_ANALYSED,
        model.UNKNOWN,
    }:
        effect = "Some of these changes couldn't be analysed; check the PR."
    # Every failing read impact ends in "fails" ("API fails", ...).
    elif model.FAILS in (m.read_impact or "") or m.write_impact == model.FAILS:
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
                # N-1 reads entities without an aspect it doesn't know; only an
                # entity type that is new in N can't be read at all.
                new_entities = pdl_rules.new_entity_types(
                    a, current, target, rac.file_at
                )
                reads.append(model.API_FAILS if new_entities else model.OK)
                writes.append(model.FAILS)
                notes.append(
                    f"`{a}` (part of the new entity type {', '.join(new_entities)})"
                    if new_entities
                    else f"`{a}` (not in N-1, which reads its entities without it)"
                )
                continue
            related = _aspect_changes(findings, a)
            reads += [f.read_impact for f in related] or [model.OK]
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
        g.read_impact = g.write_impact = g.data_loss = model.NOT_ANALYSED
        g.detail = (
            "Version bumped but no change found in this aspect or the records "
            "it uses (the tool doesn't parse typerefs or unions). Check the PR "
            "for what changed."
        )


def run(
    current: str, target: str, repo_url: Optional[str] = None
) -> tuple[list[model.RollbackFinding], str, str]:
    """Run all analysis dimensions. Returns (findings, current_sha, target_sha)."""
    current_sha = repo.resolve_sha(current)
    target_sha = repo.resolve_sha(target)
    pdl_paths = rac.changed_pdls(target, current)
    findings = collect_findings(current, target, pdl_paths)
    combine_findings(findings, current, target, pdl_paths)
    attach_commits(findings, current, target, repo_url)
    return findings, current_sha, target_sha


def attach_commits(
    findings: list[model.RollbackFinding],
    current: str,
    target: str,
    repo_url: Optional[str],
) -> None:
    """Link each finding to the commits on N's first-parent history that changed
    its file, and take its PR numbers from them, so every PR belongs to the
    repository being analysed (a fork's merge PR rather than an upstream one)."""
    cache: dict[str, list[tuple[str, Optional[str]]]] = {}
    for f in findings:
        if f.path not in cache:
            cache[f.path] = repo.first_parent_changes(current, f.path, target)
        changes = cache[f.path]
        if not changes:
            continue
        f.commits = [
            {
                "sha": sha,
                "pr": pr,
                "url": f"{repo_url}/commit/{sha}" if repo_url else None,
                "pr_url": f"{repo_url}/pull/{pr}" if repo_url and pr else None,
            }
            for sha, pr in changes
        ]
        prs = list(dict.fromkeys(pr for _, pr in changes if pr))
        f.pr_number = ", ".join(prs) or None


def collect_findings(
    current: str, target: str, pdl_paths: list[str]
) -> list[model.RollbackFinding]:
    """Each dimension's own findings. None of these reads another's output."""
    read = repo.cached_reader()
    findings: list[model.RollbackFinding] = []
    for path in pdl_paths:
        findings.extend(
            pdl_rules.classify_pdl_for_rollback(path, current, target, read)
        )
    findings.extend(pdl_rules.analyze_nested_changes(current, target, pdl_paths, read))
    findings.extend(java_scan.classify_mutators_for_rollback(current, target))
    findings.extend(java_scan.classify_upgrade_steps_for_rollback(current, target))
    findings.extend(pdl_rules.analyze_schema_version_gaps(current, target, pdl_paths))
    return findings


def combine_findings(
    findings: list[model.RollbackFinding],
    current: str,
    target: str,
    pdl_paths: list[str],
) -> None:
    """Stages that update findings from other dimensions' results, in place.
    Order matters: attribution sets the aspects each schema change reaches,
    which the relationship stage needs to find other aspects of the same entity
    and the version-gap stage needs to find unexplained bumps; the mutator and
    upgrade-step stages then read all of them."""
    pdl_rules.attribute_embedded_aspect_changes(findings, current, target, pdl_paths)
    pdl_rules.refine_relationship_findings(findings, target)
    flag_unexplained_version_gaps(findings)
    set_mutator_impact(findings)
    set_upgrade_step_impact(findings, current, target)
