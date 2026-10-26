"""`probe filter` verdicts for the Dataplex source that one pattern cannot state.

DataplexConfig's probe hooks delegate here, imported lazily, so ingestion never
loads this module. Project precedence is gcp_project_filter's
project_rejected_by, which ingestion's is_project_allowed also calls.
"""

import re
from typing import Optional, Sequence

from datahub.ingestion.agent.verdicts import (
    Verdict,
    VerdictContext,
    ancestors_in,
    pattern_verdict,
)
from datahub.ingestion.source.common.gcp_project_filter import project_rejected_by
from datahub.ingestion.source.dataplex.dataplex_config import (
    DATAPLEX_ASPECT_TYPE_KIND,
    DATAPLEX_ENTRY_FQN_KIND,
    DATAPLEX_ENTRY_GROUP_KIND,
    DATAPLEX_ENTRY_KIND,
    DATAPLEX_PROJECT_KIND,
    DataplexConfig,
)

SPANNER_ENTRY_GROUP = "@spanner"
# The attribute a `probe run entries` record carries the FQN in.
FQN_ATTRIBUTE = "fully_qualified_name"
FQN_PATTERN_FIELD = "filter_config.entries.fqn_pattern"
# Ingestion skips an entry with no FQN, whatever the patterns say.
MISSING_FQN = "missing_fully_qualified_name"
# The project segment of a Dataplex resource name; it may be a project number.
_PROJECT_SEGMENT = re.compile(r"(?:^|/)projects/([^/]+)(?:/|$)")


def ancestor_kinds(config: DataplexConfig, kind: str) -> Optional[Sequence[str]]:
    """What contains each kind, outermost first.

    API mode lists entries per project and entry group, dropping a group
    before listing its entries, so an entry is judged under both. `export`
    exports only the resolved projects, so a group is judged under its
    project there; `read_export` reads whatever export it is pointed at, so
    nothing scopes a group. In both export modes the group above an entry
    decides nothing (the project is judged by the entry verdict instead). An
    aspect type is matched wherever an aspect appears, so nothing contains it.
    """
    if kind == DATAPLEX_ASPECT_TYPE_KIND:
        return ()
    if kind == DATAPLEX_ENTRY_GROUP_KIND and config.extraction_method == "read_export":
        return ()
    leaves = (DATAPLEX_ENTRY_KIND, DATAPLEX_ENTRY_FQN_KIND)
    if kind in leaves and config.reads_from_export:
        return ()
    return ancestors_in(
        (DATAPLEX_PROJECT_KIND, DATAPLEX_ENTRY_GROUP_KIND), kind, leaves
    )


def verdict(config: DataplexConfig, ctx: VerdictContext) -> Optional[Verdict]:
    if ctx.kind == DATAPLEX_PROJECT_KIND:
        # A `probe run projects` record was listed with project_labels
        # applied; a bare name was not.
        return _project_verdict(
            config, ctx.name, ctx, labels_checked=bool(ctx.attributes)
        )
    if ctx.kind == DATAPLEX_ENTRY_GROUP_KIND:
        return _entry_group_verdict(config, ctx)
    if ctx.kind in (DATAPLEX_ENTRY_KIND, DATAPLEX_ENTRY_FQN_KIND):
        return _entry_verdict(config, ctx)
    return None


def _looks_like_spanner(ctx: VerdictContext) -> bool:
    """Whether a judged entry is one ingestion reads via search_entries: by
    its FQN (spanner:...), the entry type a `probe run entries` record
    carries, or the @spanner system entry group."""
    entry_type = ctx.attributes.get("entry_type", "")
    return (
        ctx.name.startswith("spanner:")
        or "spanner" in entry_type
        or any(
            f"/entryGroups/{SPANNER_ENTRY_GROUP}" in p
            for p in (ctx.name, *ctx.parent_path)
        )
    )


def _entry_group_verdict(
    config: DataplexConfig, ctx: VerdictContext
) -> Optional[Verdict]:
    """Included wherever ingestion reads a group's entries without the group
    pattern, so an entry below it is not dropped by parent exclusion (which
    the framework applies after this override)."""
    if config.reads_from_export:
        ctx.warn(
            f"extraction_method is '{config.extraction_method}', which reads "
            f"entries from a metadata export and does not apply "
            f"filter_config.entry_groups.pattern; filter entries with "
            f"filter_config.entries instead"
        )
        return Verdict.include()
    if not ctx.name.endswith(f"/entryGroups/{SPANNER_ENTRY_GROUP}"):
        return None
    if pattern_verdict(config, ctx.pattern_field, ctx.target).included:
        return None
    ctx.warn(
        f"filter_config.entry_groups.pattern denies {SPANNER_ENTRY_GROUP}, but "
        f"Spanner entries are read through search_entries, which does not "
        f"apply it, so they are ingested anyway; filter them with "
        f"filter_config.entries instead"
    )
    return Verdict.include()


def _project_verdict(
    config: DataplexConfig, project: str, ctx: VerdictContext, *, labels_checked: bool
) -> Verdict:
    if config.project_ids and not config.project_id_pattern.is_allow_all():
        ctx.warn(
            "project_ids is set, so ingestion reads exactly those projects and "
            "project_id_pattern is not consulted"
        )
    rejected = project_rejected_by(config, project)
    if rejected is not None:
        return Verdict.exclude(rejected)
    if config.project_labels and not config.project_ids and not labels_checked:
        # Labels narrow the Resource Manager search before the pattern runs,
        # and neither a bare name nor an entry record carries them.
        ctx.warn(
            f"project_labels is set, and '{project}' was not listed with them "
            f"applied, so it was judged by project_id_pattern alone; list "
            f"projects with `probe run projects` (which applies the labels) and "
            f"judge that output with --from-run"
        )
    return Verdict.include()


def _entry_verdict(config: DataplexConfig, ctx: VerdictContext) -> Verdict:
    """A missing FQN, then both entry patterns, in the order ingestion's
    _report_and_should_process_entry reports them, then what the extraction
    method adds."""
    if ctx.kind == DATAPLEX_ENTRY_KIND:
        fqn = ctx.attributes.get(FQN_ATTRIBUTE)
        if fqn == "":
            return Verdict.exclude(MISSING_FQN)
        by_name = pattern_verdict(config, ctx.pattern_field, ctx.target)
        if not by_name.included:
            return by_name
        if fqn is None:
            ctx.warn(
                "these entries were judged without their fully_qualified_name, "
                "so filter_config.entries.fqn_pattern, which ingestion also "
                "applies, was not; judge a `probe run entries` listing with "
                "--from-run, or check the FQNs with --kind EntryFqn"
            )
        else:
            by_fqn = pattern_verdict(config, FQN_PATTERN_FIELD, fqn)
            if not by_fqn.included:
                return Verdict.exclude(FQN_PATTERN_FIELD, matched_target=fqn)
    else:
        by_fqn = pattern_verdict(config, ctx.pattern_field, ctx.target)
        if not by_fqn.included:
            return by_fqn
    if not config.reads_from_export and _looks_like_spanner(ctx):
        ctx.warn(
            "Spanner entries are read through search_entries, which does not "
            "apply filter_config.entry_groups.pattern; only the entry and "
            "project filters decide them"
        )
    if config.extraction_method == "read_export":
        ctx.warn(
            "extraction_method is 'read_export', so the export you point at "
            "decides which projects are present; project_ids, project_labels "
            "and project_id_pattern are not applied to entries"
        )
        return Verdict.include()
    if config.extraction_method != "export":
        return Verdict.include()
    return _exported_entry_project_verdict(config, ctx)


def _exported_entry_project_verdict(
    config: DataplexConfig, ctx: VerdictContext
) -> Verdict:
    """`export` exports only the resolved projects (run_exports), so the
    entry's project decides too. The project comes from a --parent given as a
    project id, else from the project segment of the entry's or a parent's
    resource name."""
    project = _entry_project(ctx)
    if project is None:
        ctx.warn(
            "no project could be found for these entries (no --parent project "
            "id, and no projects/<id> in their names), so project_ids and "
            "project_id_pattern, which decide what `export` exports, were not "
            "judged; pass the project id as the first --parent"
        )
        return Verdict.include()
    if project.isdigit():
        ctx.warn(
            f"these entries name project number {project}, not a project id, "
            f"so project_ids and project_id_pattern (matched on ids) were not "
            f"judged; pass the project id as the first --parent"
        )
        return Verdict.include()
    by_project = _project_verdict(config, project, ctx, labels_checked=False)
    if not by_project.included:
        ctx.warn(
            f"the containing {DATAPLEX_PROJECT_KIND} '{project}' is excluded by "
            f"{by_project.excluded_by}, so the metadata export never includes it"
        )
    return by_project


def _entry_project(ctx: VerdictContext) -> Optional[str]:
    for parent in ctx.parent_path:
        if not parent.startswith("projects/"):
            # A project id as the recipe names it, as `entries` lists it first.
            return parent
    for name in (ctx.name, *ctx.parent_path):
        match = _PROJECT_SEGMENT.search(name)
        if match:
            return match.group(1)
    return None
