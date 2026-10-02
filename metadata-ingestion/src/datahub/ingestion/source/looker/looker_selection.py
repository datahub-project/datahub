"""Which Looker dashboards, charts and standalone looks ingestion keeps.

Pure functions of the recipe and of facts about one object, called by
LookerDashboardSource at each stage where it decides, and composed in the
same order for `probe filter` by looker_probe_verdicts.
"""

from dataclasses import dataclass
from typing import Optional, Protocol

from looker_sdk.sdk.api40.models import DashboardElement, FolderBase

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.verdicts import Verdict

# excluded_by values that name a rule rather than a config field.
NOT_A_VIS_ELEMENT = "element_type"
ELEMENT_HAS_NO_QUERY = "element_has_no_query"
LOOK_HAS_NO_QUERY = "look_has_no_query"
ON_A_KEPT_DASHBOARD = "on_a_kept_dashboard"


class LookerSelectionConfig(Protocol):
    # Properties, not attributes: the config module imports this one, so it
    # cannot be named here, and a property lets the pydantic fields satisfy
    # the Protocol under mypy.
    @property
    def dashboard_pattern(self) -> AllowDenyPattern: ...

    @property
    def chart_pattern(self) -> AllowDenyPattern: ...

    @property
    def folder_path_pattern(self) -> AllowDenyPattern: ...

    @property
    def include_deleted(self) -> bool: ...

    @property
    def skip_personal_folders(self) -> bool: ...

    @property
    def extract_independent_looks(self) -> bool: ...


def is_personal_folder(folder: Optional[FolderBase]) -> bool:
    return folder is not None and bool(
        folder.is_personal or folder.is_personal_descendant
    )


def element_has_query(element: DashboardElement) -> bool:
    """Whether _get_looker_dashboard_element returns an element, not None.

    It tries element.query, then element.look -- returning None when the look
    has no query, without trying result_maker -- then element.result_maker.
    """
    if element.query is not None:
        return True
    if element.look is not None:
        return element.look.query is not None
    return element.result_maker is not None


def deleted_verdict(config: LookerSelectionConfig, deleted: bool) -> Verdict:
    """Ingestion lists deleted dashboards and looks only under include_deleted.

    That is a listing choice, so ingestion never calls this; the parity test
    pins it.
    """
    if deleted and not config.include_deleted:
        return Verdict.exclude("include_deleted")
    return Verdict.include()


def dashboard_id_verdict(config: LookerSelectionConfig, dashboard_id: str) -> Verdict:
    if config.dashboard_pattern.allowed(dashboard_id):
        return Verdict.include()
    return Verdict.exclude("dashboard_pattern")


def personal_folder_verdict(config: LookerSelectionConfig, personal: bool) -> Verdict:
    if config.skip_personal_folders and personal:
        return Verdict.exclude("skip_personal_folders")
    return Verdict.include()


def folder_path_verdict(
    config: LookerSelectionConfig,
    folder_path: Optional[str],
    *,
    path_allowed: Optional[bool] = None,
) -> Verdict:
    """folder_path_pattern on the joined path. No folder means no rule.

    The probe withholds a path that may name a user and passes the flag it
    computed at listing time instead.
    """
    if folder_path is not None:
        allowed = config.folder_path_pattern.allowed(folder_path)
    else:
        allowed = path_allowed is not False
    return Verdict.include() if allowed else Verdict.exclude("folder_path_pattern")


def chart_id_verdict(config: LookerSelectionConfig, element_id: str) -> Verdict:
    if config.chart_pattern.allowed(element_id):
        return Verdict.include()
    return Verdict.exclude("chart_pattern")


def element_type_verdict(element_type: Optional[str]) -> Verdict:
    if element_type == "vis":
        return Verdict.include()
    return Verdict.exclude(NOT_A_VIS_ELEMENT)


@dataclass(frozen=True)
class DashboardFacts:
    dashboard_id: str
    deleted: bool
    personal: bool
    folder_path: Optional[str]
    folder_path_allowed: Optional[bool] = None


def dashboard_verdict(config: LookerSelectionConfig, facts: DashboardFacts) -> Verdict:
    """get_workunits_internal (listing, then dashboard_pattern), then
    process_dashboard (personal folder, then folder path)."""
    for verdict in (
        deleted_verdict(config, facts.deleted),
        dashboard_id_verdict(config, facts.dashboard_id),
        personal_folder_verdict(config, facts.personal),
        folder_path_verdict(
            config, facts.folder_path, path_allowed=facts.folder_path_allowed
        ),
    ):
        if not verdict.included:
            return verdict
    return Verdict.include()


@dataclass(frozen=True)
class ElementFacts:
    element_id: str
    # None: the listing did not say; that rule is skipped.
    element_type: Optional[str]
    has_query: Optional[bool]


def element_verdict(config: LookerSelectionConfig, facts: ElementFacts) -> Verdict:
    """The elements loop (chart_pattern), then _get_looker_dashboard_element
    (no query), then _make_dashboard_and_chart_entities (vis only)."""
    by_id = chart_id_verdict(config, facts.element_id)
    if not by_id.included:
        return by_id
    if facts.has_query is False:
        return Verdict.exclude(ELEMENT_HAS_NO_QUERY)
    if facts.element_type is not None:
        return element_type_verdict(facts.element_type)
    return Verdict.include()


@dataclass(frozen=True)
class LookFacts:
    # None: not judged here (ingestion's all_looks already chose by
    # include_deleted, or the probe was not told).
    deleted: Optional[bool]
    on_kept_dashboard: Optional[bool]
    has_query: Optional[bool]
    personal: bool


def standalone_look_verdict(config: LookerSelectionConfig, facts: LookFacts) -> Verdict:
    """extract_independent_looks: the switch, all_looks(soft_deleted=...),
    reachable_look_registry, query_id is None, then the personal folder."""
    if not config.extract_independent_looks:
        return Verdict.exclude("extract_independent_looks")
    if facts.deleted is not None:
        by_deleted = deleted_verdict(config, facts.deleted)
        if not by_deleted.included:
            return by_deleted
    if facts.on_kept_dashboard:
        return Verdict.exclude(ON_A_KEPT_DASHBOARD)
    if facts.has_query is False:
        return Verdict.exclude(LOOK_HAS_NO_QUERY)
    return personal_folder_verdict(config, facts.personal)
