"""How Looker ingestion decides what it emits, restated for `probe filter`.

The provider (looker_probe.py) writes these attribute keys into its listings,
and LookerDashboardSourceConfig.probe_verdict_override reads them here. One
module, so the two cannot drift apart on a key name. Every rule cites the
looker_source.py code it mirrors; change them together.
"""

from typing import TYPE_CHECKING, Mapping, Optional

from datahub.ingestion.agent.verdicts import Verdict, VerdictContext, pattern_verdict
from datahub.ingestion.source.common.subtypes import (
    BIAssetSubTypes,
    BIContainerSubTypes,
    DatasetSubTypes,
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
# Written only by a `--trace-charts` listing, and only when the trace could
# settle the question; absent means undetermined.
ATTR_USED = "used"
ATTR_ON_KEPT_DASHBOARD = "on_kept_dashboard"

# excluded_by values that name a rule rather than a config field.
NOT_A_VIS_ELEMENT = "element_type"
ELEMENT_HAS_NO_QUERY = "element_has_no_query"
LOOK_HAS_NO_QUERY = "look_has_no_query"
MODEL_HAS_NO_EXPLORES = "model_has_no_explores"
ON_A_KEPT_DASHBOARD = "on_a_kept_dashboard"

_NO_DASHBOARD_FACTS = (
    "dashboards named without their listing were judged on dashboard_pattern "
    "alone; ingestion also drops deleted dashboards, personal-folder ones under "
    "skip_personal_folders, and ones folder_path_pattern denies. Save `probe run "
    "dashboards --report-to` and pass it with `probe filter --from-run` to judge those"
)
_NO_ELEMENT_FACTS = (
    "charts named without their listing were judged on chart_pattern alone; "
    "ingestion also skips elements that are not `vis` or have no query. Save "
    "`probe run charts --report-to` and pass it with `probe filter --from-run`"
)
_NO_LOOK_FACTS = (
    "standalone looks named without their listing were judged on "
    "extract_independent_looks alone; save `probe run looks --report-to` and "
    "pass it with `probe filter --from-run` to judge deleted, personal-folder "
    "and query-less looks"
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


def _folder_path_allowed(
    config: "LookerDashboardSourceConfig", attributes: Mapping[str, str]
) -> bool:
    """_should_skip_dashboard_by_folder_path, from a listing's facts.

    A listed path is re-matched against the recipe being checked. A personal
    folder's path is withheld (it is named after its user), so the flag the
    listing computed at run time stands in. No folder at all means ingestion
    applies no folder rule.
    """
    path = attributes.get(ATTR_FOLDER_PATH)
    if path is not None:
        return config.folder_path_pattern.allowed(path)
    return attributes.get(ATTR_FOLDER_PATH_ALLOWED) != "false"


def _dashboard(
    config: "LookerDashboardSourceConfig", ctx: VerdictContext
) -> Optional[Verdict]:
    attributes = ctx.attributes
    if ATTR_DELETED not in attributes:
        ctx.warn(_NO_DASHBOARD_FACTS)
        return None
    # Listed by ingestion only under include_deleted (get_workunits_internal),
    # so this comes before dashboard_pattern.
    if _flag(attributes, ATTR_DELETED) and not config.include_deleted:
        return Verdict(False, "include_deleted")
    by_id = pattern_verdict(config, ctx.pattern_field, ctx.target)
    if not by_id.included:
        return by_id
    if config.skip_personal_folders and _flag(attributes, ATTR_FOLDER_PERSONAL):
        return Verdict(False, "skip_personal_folders")
    if not _folder_path_allowed(config, attributes):
        return Verdict(False, "folder_path_pattern")
    return Verdict.include()


def _dashboard_element(
    config: "LookerDashboardSourceConfig", ctx: VerdictContext
) -> Optional[Verdict]:
    by_id = pattern_verdict(config, ctx.pattern_field, ctx.target)
    if not by_id.included:
        return by_id
    attributes = ctx.attributes
    if ATTR_TYPE not in attributes:
        ctx.warn(_NO_ELEMENT_FACTS)
        return None
    # _get_looker_dashboard_element returns None, then
    # _make_dashboard_and_chart_entities keeps only type == "vis".
    if attributes.get(ATTR_HAS_QUERY) == "false":
        return Verdict(False, ELEMENT_HAS_NO_QUERY)
    if attributes[ATTR_TYPE] != "vis":
        return Verdict(False, NOT_A_VIS_ELEMENT)
    return Verdict.include()


def _standalone_look(
    config: "LookerDashboardSourceConfig", ctx: VerdictContext
) -> Verdict:
    if not config.extract_independent_looks:
        return Verdict(False, "extract_independent_looks")
    ctx.warn(_CHART_PATTERN_NOT_APPLIED)
    attributes = ctx.attributes
    if ATTR_ON_KEPT_DASHBOARD not in attributes:
        ctx.warn(_ON_A_DASHBOARD_NOT_JUDGED)
    if ATTR_DELETED not in attributes:
        ctx.warn(_NO_LOOK_FACTS)
        return Verdict.include()
    # extract_independent_looks: all_looks(soft_deleted=include_deleted),
    # then reachable_look_registry, then query_id is None, then the
    # personal-folder skip.
    if _flag(attributes, ATTR_DELETED) and not config.include_deleted:
        return Verdict(False, "include_deleted")
    if _flag(attributes, ATTR_ON_KEPT_DASHBOARD):
        return Verdict(False, ON_A_KEPT_DASHBOARD)
    if attributes.get(ATTR_HAS_QUERY) == "false":
        return Verdict(False, LOOK_HAS_NO_QUERY)
    if config.skip_personal_folders and _flag(attributes, ATTR_FOLDER_PERSONAL):
        return Verdict(False, "skip_personal_folders")
    return Verdict.include()


def _used_explores_rule(
    config: "LookerDashboardSourceConfig", ctx: VerdictContext
) -> Verdict:
    if config.emit_used_explores_only:
        used = ctx.attributes.get(ATTR_USED)
        if used == "true":
            return Verdict.include()
        if used != "false":
            ctx.warn(_USED_EXPLORES_UNDETERMINED)
        return Verdict(False, "emit_used_explores_only")
    # list_all_explores yields nothing for a model without explores, so
    # _make_explore_containers never emits its container.
    if ctx.kind == MODEL_KIND and ctx.attributes.get(ATTR_EXPLORE_COUNT) == "0":
        return Verdict(False, MODEL_HAS_NO_EXPLORES)
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
