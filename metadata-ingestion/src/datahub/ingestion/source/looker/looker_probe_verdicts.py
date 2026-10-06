"""How Looker ingestion decides what it emits, judged for `probe filter`.

The provider (looker_probe.py) writes these attribute keys into its listings,
and LookerDashboardSourceConfig.probe_verdict_override reads them here. One
module, so the two cannot drift apart on a key name. The rules themselves are
looker_selection's, which ingestion calls too; this module turns a listing's
attributes into that module's facts, and warns when a fact is missing.
"""

from typing import TYPE_CHECKING, Mapping, Optional

from datahub.ingestion.agent.verdicts import Verdict, VerdictContext
from datahub.ingestion.source.common.subtypes import (
    BIAssetSubTypes,
    BIContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.looker.looker_selection import (
    DashboardFacts,
    ElementFacts,
    LookFacts,
    dashboard_id_verdict,
    dashboard_verdict,
    element_verdict,
    standalone_look_verdict,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.looker.looker_config import (
        LookerDashboardSourceConfig,
    )

DASHBOARD_KIND = str(BIAssetSubTypes.DASHBOARD)
# Every Looker chart is emitted with this subtype, a dashboard element and a
# standalone look alike (_make_chart_entities), so both listings share it.
LOOK_KIND = str(BIAssetSubTypes.LOOKER_LOOK)
MODEL_KIND = str(BIContainerSubTypes.LOOKML_MODEL)
EXPLORE_KIND = str(DatasetSubTypes.LOOKER_EXPLORE)

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


def _bool_or_none(attributes: Mapping[str, str], key: str) -> Optional[bool]:
    """A listed bool, or None when the listing did not write it."""
    value = attributes.get(key)
    if value is None:
        return None
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
        folder_path_allowed=_bool_or_none(attributes, allowed_key),
    )


def _dashboard(
    config: "LookerDashboardSourceConfig", ctx: VerdictContext
) -> Optional[Verdict]:
    attributes = ctx.attributes
    if ATTR_DELETED not in attributes:
        ctx.warn(_NO_DASHBOARD_FACTS)
        return None
    return dashboard_verdict(config, _dashboard_facts(ctx.target, attributes))


def _parent_dashboard(
    config: "LookerDashboardSourceConfig", ctx: VerdictContext
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
    config: "LookerDashboardSourceConfig", ctx: VerdictContext
) -> Optional[Verdict]:
    dropped_with_dashboard = _parent_dashboard(config, ctx)
    if dropped_with_dashboard is not None:
        return dropped_with_dashboard
    attributes = ctx.attributes
    verdict = element_verdict(
        config,
        ElementFacts(
            element_id=ctx.target,
            element_type=attributes.get(ATTR_TYPE),
            has_query=_bool_or_none(attributes, ATTR_HAS_QUERY),
        ),
    )
    if verdict.included and ATTR_TYPE not in attributes:
        ctx.warn(_NO_ELEMENT_FACTS)
        return None
    return verdict


def _standalone_look(
    config: "LookerDashboardSourceConfig", ctx: VerdictContext
) -> Verdict:
    attributes = ctx.attributes
    verdict = standalone_look_verdict(
        config,
        LookFacts(
            deleted=_bool_or_none(attributes, ATTR_DELETED),
            on_kept_dashboard=_bool_or_none(attributes, ATTR_ON_KEPT_DASHBOARD),
            has_query=_bool_or_none(attributes, ATTR_HAS_QUERY),
            personal=_flag(attributes, ATTR_FOLDER_PERSONAL),
        ),
    )
    if not config.extract_independent_looks:
        return verdict
    ctx.warn(_CHART_PATTERN_NOT_APPLIED)
    if ATTR_ON_KEPT_DASHBOARD not in attributes:
        ctx.warn(_ON_A_DASHBOARD_NOT_JUDGED)
    if ATTR_DELETED not in attributes:
        ctx.warn(_NO_LOOK_FACTS)
        return Verdict.include()
    if not verdict.included:
        return verdict
    # `looks` writes has_query null (dropped from the attributes) when its
    # get_look read-back failed; ingestion's own get_look then `continue`s.
    if ATTR_HAS_QUERY not in attributes:
        ctx.warn(_LOOK_QUERY_UNREAD)
        return Verdict.exclude(LOOK_QUERY_UNREADABLE)
    return verdict


def _used_explores_rule(
    config: "LookerDashboardSourceConfig", ctx: VerdictContext
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
    config: "LookerDashboardSourceConfig", ctx: VerdictContext
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
