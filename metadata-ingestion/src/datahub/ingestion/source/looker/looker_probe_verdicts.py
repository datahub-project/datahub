"""How Looker ingestion decides what it emits, judged for `probe filter`.

The provider (looker_probe.py) writes these attribute keys into its listings,
and LookerDashboardSourceConfig.probe_verdict_override reads them here. One
module, so the two cannot drift apart on a key name. The rules themselves are
looker_selection's, which ingestion calls too; this module turns a listing's
attributes into that module's facts, and warns when a fact is missing.
"""

from typing import Mapping, Optional

from datahub.ingestion.agent.verdicts import Verdict, VerdictContext
from datahub.ingestion.source.looker.looker_config import LookerDashboardSourceConfig
from datahub.ingestion.source.looker.looker_selection import (
    DASHBOARD_KIND,
    EXPLORE_KIND,
    LOOK_KIND,
    MODEL_KIND,
    UNKNOWN,
    DashboardFacts,
    ElementFacts,
    LookFacts,
    MaybeBool,
    dashboard_id_verdict,
    dashboard_verdict,
    element_verdict,
    standalone_look_verdict,
)

ATTR_DELETED = "deleted"
ATTR_FOLDER_PATH = "folder_path"
ATTR_FOLDER_PERSONAL = "folder_personal"
ATTR_FOLDER_PATH_ALLOWED = "folder_path_allowed"
ATTR_TYPE = "type"
ATTR_HAS_QUERY = "has_query"
ATTR_EXPLORE_COUNT = "explore_count"
# A chart's own dashboard, stamped on each `charts` record: `probe filter`
# judges a --parent with no facts of its own, so a dashboard ingestion drops
# would otherwise leave its charts reading included.
ATTR_DASHBOARD_DELETED = "dashboard_deleted"
ATTR_DASHBOARD_FOLDER_PATH = "dashboard_folder_path"
ATTR_DASHBOARD_FOLDER_PERSONAL = "dashboard_folder_personal"
ATTR_DASHBOARD_FOLDER_PATH_ALLOWED = "dashboard_folder_path_allowed"
# Written only by a `--trace-charts` listing, and only when the trace could
# settle the question; absent means undetermined.
ATTR_USED = "used"
ATTR_ON_KEPT_DASHBOARD = "on_kept_dashboard"
# Written on every row of a `looks --trace-charts` listing, so a missing
# on_kept_dashboard can be told apart: not traced, or traced and unsettled.
ATTR_TRACED = "traced"

# excluded_by values for rules only the probe applies; ingestion's own are in
# looker_selection.
MODEL_HAS_NO_EXPLORES = "model_has_no_explores"
LOOK_QUERY_UNREADABLE = "look_query_unreadable"

_NO_DASHBOARD_FACTS = (
    "dashboards named without their listing were judged on dashboard_pattern "
    "alone; ingestion also drops deleted dashboards, personal-folder ones under "
    "skip_personal_folders, and ones folder_path_pattern denies. Save `probe run "
    "dashboards --report-to` and pass it with `probe filter --from-run` to judge those"
)
_NO_ELEMENT_FACTS = (
    "charts named without their listing were judged on dashboard_pattern (for "
    "the --parent dashboard) and chart_pattern alone; ingestion also skips "
    "every chart of a deleted, personal-folder or folder_path_pattern-denied "
    "dashboard, and elements that are not `vis` or have no query. Save `probe "
    "run charts --report-to` and pass it with `probe filter --from-run`"
)
_NO_LOOK_FACTS = (
    "standalone looks named without their listing were judged on "
    "extract_independent_looks alone; save `probe run looks --report-to` and "
    "pass it with `probe filter --from-run` to judge deleted, personal-folder "
    "and query-less looks"
)
_LOOK_QUERY_UNREAD = (
    "some looks' queries could not be read when they were listed (see that "
    "listing's warnings); ingestion skips a look whose query read fails, so "
    "they are reported excluded. Re-run `probe run looks --report-to` once the "
    "read succeeds to judge them"
)
_CHART_PATTERN_NOT_APPLIED = (
    "these are standalone looks (no --parent dashboard): ingestion's "
    "extract_independent_looks never consults chart_pattern, so it was not "
    "applied, whatever pattern_field says"
)
_ON_A_DASHBOARD_NOT_JUDGED = (
    "a look that is also on a dashboard ingestion reads is emitted as that "
    "dashboard's chart instead of as a standalone look; that was not judged "
    "for looks listed without `--trace-charts`, so some reported included may "
    "be emitted only as dashboard charts. Save `probe run looks --trace-charts "
    "--report-to` and pass it with `probe filter --from-run` to judge it"
)
_ON_A_DASHBOARD_UNDETERMINED = (
    "a look that is also on a dashboard ingestion reads is emitted as that "
    "dashboard's chart instead of as a standalone look; the chart trace could "
    "not settle that for some looks (it hit its read bound, or a read failed: "
    "see that listing's warnings), so some reported included may be emitted "
    "only as dashboard charts"
)
_FOLDER_PATH_UNDETERMINED = (
    "some dashboards' folder paths could not be read when they were listed "
    "(see that listing's warnings), so folder_path_pattern was not judged for "
    "them; re-run the listing once the read succeeds"
)
_USED_EXPLORES_UNDETERMINED = (
    "emit_used_explores_only is true (the default), so ingestion emits an "
    "explore, and its LookML model, only when a dashboard chart or standalone "
    "look it keeps queries it. For names without that fact -- named bare, "
    "listed without `--trace-charts`, or left undetermined by an interrupted "
    "trace -- only that switch was judged, not whether anything queries them: "
    "they are reported excluded, which is right only for unused ones. Save "
    "`probe run explores --model <name> --trace-charts --report-to` (or "
    "`models --trace-charts`) and pass it with `probe filter --from-run` for "
    "the verdict ingestion makes"
)


def _flag(attributes: Mapping[str, str], key: str) -> bool:
    # listing_from_run renders a JSON bool as "true"/"false".
    return attributes.get(key) == "true"


def _fact(attributes: Mapping[str, str], key: str) -> MaybeBool:
    """A listed bool, or UNKNOWN when the listing did not write it."""
    value = attributes.get(key)
    if value is None:
        return UNKNOWN
    return value == "true"


def _dashboard_facts(
    dashboard_id: str,
    attributes: Mapping[str, str],
    deleted_key: str = ATTR_DELETED,
    path_key: str = ATTR_FOLDER_PATH,
    personal_key: str = ATTR_FOLDER_PERSONAL,
    allowed_key: str = ATTR_FOLDER_PATH_ALLOWED,
) -> DashboardFacts:
    """A dashboard's facts as a listing wrote them.

    A personal folder's path is withheld (it is named after its user), so the
    flag the listing computed at run time stands in for re-matching it.
    """
    return DashboardFacts(
        dashboard_id=dashboard_id,
        deleted=_flag(attributes, deleted_key),
        personal=_flag(attributes, personal_key),
        folder_path=attributes.get(path_key),
        folder_path_allowed=_fact(attributes, allowed_key),
    )


def _warn_if_folder_undetermined(
    ctx: VerdictContext, path_key: str, allowed_key: str
) -> None:
    # A dashboard with no folder is written allowed; only an unreadable
    # folder path leaves both out.
    if path_key not in ctx.attributes and allowed_key not in ctx.attributes:
        ctx.warn(_FOLDER_PATH_UNDETERMINED)


def _dashboard(
    config: LookerDashboardSourceConfig, ctx: VerdictContext
) -> Optional[Verdict]:
    attributes = ctx.attributes
    if ATTR_DELETED not in attributes:
        ctx.warn(_NO_DASHBOARD_FACTS)
        return None
    _warn_if_folder_undetermined(ctx, ATTR_FOLDER_PATH, ATTR_FOLDER_PATH_ALLOWED)
    return dashboard_verdict(config, _dashboard_facts(ctx.target, attributes))


def _parent_dashboard(
    config: LookerDashboardSourceConfig, ctx: VerdictContext
) -> Optional[Verdict]:
    """dashboard_verdict for the chart's own dashboard, from the facts `charts`
    stamped on the chart. None: it is kept."""
    attributes = ctx.attributes
    dashboard_id = ctx.parent_path[-1]
    if ATTR_DASHBOARD_DELETED not in attributes:
        by_id = dashboard_id_verdict(config, dashboard_id)
        if not by_id.included:
            return by_id
        ctx.warn(_NO_ELEMENT_FACTS)
        return None
    _warn_if_folder_undetermined(
        ctx, ATTR_DASHBOARD_FOLDER_PATH, ATTR_DASHBOARD_FOLDER_PATH_ALLOWED
    )
    verdict = dashboard_verdict(
        config,
        _dashboard_facts(
            dashboard_id,
            attributes,
            ATTR_DASHBOARD_DELETED,
            ATTR_DASHBOARD_FOLDER_PATH,
            ATTR_DASHBOARD_FOLDER_PERSONAL,
            ATTR_DASHBOARD_FOLDER_PATH_ALLOWED,
        ),
    )
    return None if verdict.included else verdict


def _dashboard_element(
    config: LookerDashboardSourceConfig, ctx: VerdictContext
) -> Optional[Verdict]:
    dropped_with_dashboard = _parent_dashboard(config, ctx)
    if dropped_with_dashboard is not None:
        return dropped_with_dashboard
    attributes = ctx.attributes
    # `charts` always writes has_query, so on a record that carries it a
    # missing type is a null one (a saved run drops nulls), which the vis
    # rule excludes; on a bare name it is unknown.
    listed = ATTR_HAS_QUERY in attributes
    verdict = element_verdict(
        config,
        ElementFacts(
            element_id=ctx.target,
            element_type=attributes.get(ATTR_TYPE) if listed else UNKNOWN,
            has_query=_fact(attributes, ATTR_HAS_QUERY),
        ),
    )
    if verdict.included and not listed:
        ctx.warn(_NO_ELEMENT_FACTS)
        return None
    return verdict


def _standalone_look(
    config: LookerDashboardSourceConfig, ctx: VerdictContext
) -> Verdict:
    attributes = ctx.attributes
    verdict = standalone_look_verdict(
        config,
        LookFacts(
            personal=_flag(attributes, ATTR_FOLDER_PERSONAL),
            deleted=_fact(attributes, ATTR_DELETED),
            on_kept_dashboard=_fact(attributes, ATTR_ON_KEPT_DASHBOARD),
            has_query=_fact(attributes, ATTR_HAS_QUERY),
        ),
    )
    if not config.extract_independent_looks:
        return verdict
    ctx.warn(_CHART_PATTERN_NOT_APPLIED)
    if ATTR_ON_KEPT_DASHBOARD not in attributes:
        ctx.warn(
            _ON_A_DASHBOARD_UNDETERMINED
            if _flag(attributes, ATTR_TRACED)
            else _ON_A_DASHBOARD_NOT_JUDGED
        )
    # Before the deleted check: an exclusion decided by a fact the listing
    # did write (no query, on a kept dashboard, personal folder) stands
    # whether or not `deleted` was written.
    if not verdict.included:
        return verdict
    if ATTR_DELETED not in attributes:
        ctx.warn(_NO_LOOK_FACTS)
        return Verdict.include()
    # `looks` writes has_query null (dropped from the attributes) when its
    # get_look read-back failed; ingestion's own get_look then `continue`s.
    if ATTR_HAS_QUERY not in attributes:
        ctx.warn(_LOOK_QUERY_UNREAD)
        return Verdict.exclude(LOOK_QUERY_UNREADABLE)
    return verdict


def _used_explores_rule(
    config: LookerDashboardSourceConfig, ctx: VerdictContext
) -> Verdict:
    if config.emit_used_explores_only:
        used = ctx.attributes.get(ATTR_USED)
        if used == "true":
            return Verdict.include()
        if used != "false":
            ctx.warn(_USED_EXPLORES_UNDETERMINED)
        return Verdict.exclude("emit_used_explores_only")
    # list_all_explores yields nothing for a model without explores, so
    # _make_explore_containers never emits its container.
    if ctx.kind == MODEL_KIND and ctx.attributes.get(ATTR_EXPLORE_COUNT) == "0":
        return Verdict.exclude(MODEL_HAS_NO_EXPLORES)
    return Verdict.include()


def looker_verdict(
    config: LookerDashboardSourceConfig, ctx: VerdictContext
) -> Optional[Verdict]:
    """LookerDashboardSourceConfig.probe_verdict_override's body.

    A `Look` with a --parent is a dashboard chart; without one it is a
    standalone look. The two follow different ingestion paths.
    """
    if ctx.structural is not None:
        # No built-in rule applies to these kinds today; keep one if it does.
        return ctx.structural
    if ctx.kind == DASHBOARD_KIND:
        return _dashboard(config, ctx)
    if ctx.kind == LOOK_KIND:
        if ctx.parent_path:
            return _dashboard_element(config, ctx)
        return _standalone_look(config, ctx)
    if ctx.kind in (MODEL_KIND, EXPLORE_KIND):
        return _used_explores_rule(config, ctx)
    return None
