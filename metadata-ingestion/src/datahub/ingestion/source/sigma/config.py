import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pydantic
from pydantic import BaseModel, Field

from datahub.configuration.common import AllowDenyPattern, TransparentSecretStr
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.api.report import EntityFilterReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)

logger = logging.getLogger(__name__)


class Constant:
    """
    keys used in sigma plugin
    """

    # Rest API response key constants
    REFRESH_TOKEN = "refresh_token"
    ACCESS_TOKEN = "access_token"
    EMAIL = "email"
    ENTRIES = "entries"
    MEMBERID = "memberId"
    EDGES = "edges"
    DEPENDENCIES = "dependencies"
    SOURCE = "source"
    TARGET = "target"
    WORKSPACEID = "workspaceId"
    PATH = "path"
    NAME = "name"
    URL = "url"
    ELEMENTID = "elementId"
    ID = "id"
    PARENTID = "parentId"
    TYPE = "type"
    DATASET = "dataset"
    WORKBOOK = "workbook"
    BADGE = "badge"
    NEXTPAGE = "nextPage"
    NEXTPAGETOKEN = "nextPageToken"
    DATA_MODEL = "data-model"
    DATA_MODEL_ID = "dataModelId"

    # Source Config constants
    DEFAULT_API_URL = "https://aws-api.sigmacomputing.com/v2"


class WorkspaceCounts(BaseModel):
    workbooks_count: int = 0
    datasets_count: int = 0
    elements_count: int = 0
    pages_count: int = 0
    data_models_count: int = 0
    data_model_elements_count: int = 0

    def is_empty(self) -> bool:
        return (
            self.workbooks_count == 0
            and self.datasets_count == 0
            and self.elements_count == 0
            and self.pages_count == 0
            and self.data_models_count == 0
            and self.data_model_elements_count == 0
        )

    def as_obj(self) -> dict:
        return {
            "workbooks_count": self.workbooks_count,
            "datasets_count": self.datasets_count,
            "elements_count": self.elements_count,
            "pages_count": self.pages_count,
            "data_models_count": self.data_models_count,
            "data_model_elements_count": self.data_model_elements_count,
        }


class SigmaWorkspaceEntityFilterReport(EntityFilterReport):
    type: str = "workspace"

    workspace_counts: Dict[str, WorkspaceCounts] = Field(
        default_factory=dict,
        description="Counts of workbooks, datasets, elements and pages in each workspace.",
    )

    def increment_workbooks_count(self, workspace_id: str) -> None:
        if workspace_id not in self.workspace_counts:
            self.workspace_counts[workspace_id] = WorkspaceCounts()
        self.workspace_counts[workspace_id].workbooks_count += 1

    def increment_datasets_count(self, workspace_id: str) -> None:
        if workspace_id not in self.workspace_counts:
            self.workspace_counts[workspace_id] = WorkspaceCounts()
        self.workspace_counts[workspace_id].datasets_count += 1

    def increment_elements_count(self, workspace_id: str) -> None:
        if workspace_id not in self.workspace_counts:
            self.workspace_counts[workspace_id] = WorkspaceCounts()
        self.workspace_counts[workspace_id].elements_count += 1

    def increment_pages_count(self, workspace_id: str) -> None:
        if workspace_id not in self.workspace_counts:
            self.workspace_counts[workspace_id] = WorkspaceCounts()
        self.workspace_counts[workspace_id].pages_count += 1

    def increment_data_models_count(self, workspace_id: str) -> None:
        if workspace_id not in self.workspace_counts:
            self.workspace_counts[workspace_id] = WorkspaceCounts()
        self.workspace_counts[workspace_id].data_models_count += 1

    def increment_data_model_elements_count(self, workspace_id: str) -> None:
        if workspace_id not in self.workspace_counts:
            self.workspace_counts[workspace_id] = WorkspaceCounts()
        self.workspace_counts[workspace_id].data_model_elements_count += 1

    def as_obj(self) -> dict:
        return {
            "filtered": self.dropped_entities.as_obj(),
            "processed": self.processed_entities.as_obj(),
            "workspace_counts": {
                key: item.as_obj() for key, item in self.workspace_counts.items()
            },
        }


@dataclass
class SigmaSourceReport(StaleEntityRemovalSourceReport):
    workspaces: SigmaWorkspaceEntityFilterReport = field(
        default_factory=SigmaWorkspaceEntityFilterReport
    )
    non_accessible_workspaces_count: int = 0

    datasets: EntityFilterReport = EntityFilterReport.field(type="dataset")
    datasets_without_workspace: int = 0

    workbooks: EntityFilterReport = EntityFilterReport.field(type="workbook")
    workbooks_without_workspace: int = 0

    data_models: EntityFilterReport = EntityFilterReport.field(type="data_model")
    data_models_without_workspace: int = 0

    number_of_files_metadata: Dict[str, int] = field(default_factory=dict)
    empty_workspaces: List[str] = field(default_factory=list)

    # Sheet upstreams skipped because the upstream element was filtered out of
    # the chart map (e.g. a pivot-table blocked by get_page_elements's allowlist).
    num_filtered_sheet_upstreams: int = 0

    # DM element emission / upstream resolution counters.
    data_model_elements_emitted: int = 0
    data_model_element_intra_upstreams: int = 0
    data_model_element_external_upstreams: int = 0
    data_model_element_upstreams_unresolved: int = 0

    # Cross-DM element references (DM-A element pulls a table from DM-B).
    # Wire shape confirmed 2026-04-22 against a live tenant: the consuming
    # DM's ``/dataModels/{id}/lineage`` returns ``sourceIds`` of shape
    # ``<otherDmUrlId>/<suffix>`` — identical to the workbook→DM shape. The
    # suffix is opaque (Sigma's suffix↔elementId map is not exposed), so
    # resolution falls back to the name-bridge using the consuming element's
    # own ``name`` (Sigma's default: element name mirrors the source element).
    # ``data_model_element_cross_dm_upstreams_resolved`` : edge emitted to
    #   the referenced DM element Dataset URN.
    # ``data_model_element_cross_dm_upstreams_ambiguous`` : multiple DM
    #   elements share the name; deterministic pick-lowest-elementId used.
    # ``data_model_element_cross_dm_upstreams_name_unmatched_but_dm_known``:
    #   referenced DM was ingested but its element names do not include the
    #   consuming element's name (user likely renamed the consuming element).
    # ``data_model_element_cross_dm_upstreams_dm_unknown`` : referenced DM
    #   is not part of this ingestion run (filtered or missing permission).
    # On any unresolved outcome the existing ``data_model_element_upstreams_unresolved``
    # also increments, so total unresolved counts stay correct.
    data_model_element_cross_dm_upstreams_resolved: int = 0
    data_model_element_cross_dm_upstreams_ambiguous: int = 0
    data_model_element_cross_dm_upstreams_name_unmatched_but_dm_known: int = 0
    data_model_element_cross_dm_upstreams_dm_unknown: int = 0

    # Personal-space / unlisted DMs discovered on demand via /v2/dataModels/{urlId}.
    # These DMs live outside the workspace-scoped /v2/dataModels listing (path
    # "My Documents", workspaceId null) but are reachable by urlId. The discovery
    # loop in get_workunits_internal fetches them so their Container + element
    # Datasets are emitted and cross-DM lineage edges resolve end-to-end.
    #
    # ``data_model_external_references_discovered`` : an unlisted DM was
    #   successfully fetched by urlId and added to the ingestion. Each such DM
    #   emits one Container + N Dataset entities as if it had been listed.
    # ``data_model_external_reference_unresolved`` : /v2/dataModels/{urlId}
    #   returned non-200 (403 most common — the service-account client has no
    #   visibility into that user's personal space). Edge suppressed; existing
    #   ``data_model_element_cross_dm_upstreams_dm_unknown`` also increments.
    # ``data_model_element_cross_dm_upstreams_single_element_fallback`` :
    #   referenced DM has exactly one element and name-bridge miss was resolved
    #   unambiguously by picking that single element. Common for CSV-upload DMs
    #   where the consumer renames its own element ("Test Data") but the
    #   producer retains the file name ("data.csv").
    data_model_external_references_discovered: int = 0
    data_model_external_reference_unresolved: int = 0
    data_model_element_cross_dm_upstreams_single_element_fallback: int = 0

    # /columns entries with ``elementId = None`` (DM-global calculations).
    # Dropped because there is no element Dataset to attach them to, but
    # counted so a user reporting "missing field" can distinguish "column
    # never existed" from "column silently dropped here".
    data_model_columns_without_element_dropped: int = 0

    # Workbook → DM element bridge counters. See sigma.py for the resolution
    # algorithm; the DM element is matched by name (Sigma coalesces same-named
    # DM element references at the API contract level, so name is sufficient).
    # ``element_dm_edges_resolved``   : workbook→DM edge successfully resolved to
    #                                   a DM element Dataset URN and emitted as
    #                                   a unique input on the chart.
    # ``element_dm_edges_deduped``    : resolved URN was already present on the
    #                                   same chart's input set (e.g. diamond
    #                                   lineage reference the same DM element
    #                                   under different sourceIds); not emitted
    #                                   a second time.
    # ``element_dm_edge_ambiguous``   : multiple DM elements share the same
    #                                   name; deterministic pick-first was used.
    # ``element_dm_edge_name_unmatched_but_dm_known`` : DM itself was ingested
    #                                   but the specific element name could not
    #                                   be matched. No lineage edge is emitted
    #                                   (``ChartInfo.inputs`` accepts only
    #                                   Dataset URNs, so a Container URN cannot
    #                                   substitute); counter surfaces partial
    #                                   visibility for report triage.
    # ``element_dm_edge_unresolved``  : DM is not known to this ingestion run
    #                                   (filtered by pattern or never fetched);
    #                                   no lineage edge is emitted.
    element_dm_edges_resolved: int = 0
    element_dm_edges_deduped: int = 0
    element_dm_edge_ambiguous: int = 0
    element_dm_edge_name_unmatched_but_dm_known: int = 0
    element_dm_edge_unresolved: int = 0
    # Sigma's live API shape (probed 2026-04-22 on a real tenant) places the
    # DM-reference node ``<dmUrlId>/<suffix>`` ONLY in ``edges[].source`` and
    # NOT as a key in the ``dependencies`` dict. We synthesize the upstream
    # from the edge alone, using the workbook element's own ``name`` as the
    # DM element name (Sigma's default is to name the workbook ref after the
    # referenced DM element; user rename degrades to ``element_dm_edge_*``
    # counters above). This counter surfaces how many workbook→DM refs
    # travelled the synthesized path vs. the legacy "DM in dependencies"
    # path (which remains as defence against a future API shape change).
    element_dm_edge_synthesized_from_edge_only: int = 0


class PlatformDetail(PlatformInstanceConfigMixin, EnvConfigMixin):
    data_source_platform: str = pydantic.Field(
        description="A chart's data sources platform name.",
    )
    default_db: Optional[str] = pydantic.Field(
        default=None,
        description="Default database name to use when parsing SQL queries. "
        "Used to generate fully qualified table URNs (e.g., 'prod' for 'prod.public.table').",
    )
    default_schema: Optional[str] = pydantic.Field(
        default=None,
        description="Default schema name to use when parsing SQL queries. "
        "Used to generate fully qualified table URNs (e.g., 'public' for 'prod.public.table').",
    )


class SigmaSourceConfig(
    StatefulIngestionConfigBase, PlatformInstanceConfigMixin, EnvConfigMixin
):
    api_url: str = pydantic.Field(
        default=Constant.DEFAULT_API_URL, description="Sigma API hosted URL."
    )
    client_id: str = pydantic.Field(description="Sigma Client ID")
    client_secret: TransparentSecretStr = pydantic.Field(
        description="Sigma Client Secret"
    )
    # Sigma workspace identifier
    workspace_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter Sigma workspaces in ingestion."
        "Mention 'My documents' if personal entities also need to ingest.",
    )
    ingest_owner: Optional[bool] = pydantic.Field(
        default=True,
        description="Ingest Owner from source. This will override Owner info entered from UI.",
    )
    ingest_shared_entities: Optional[bool] = pydantic.Field(
        default=False,
        description="Whether to ingest the shared entities or not.",
    )
    extract_lineage: Optional[bool] = pydantic.Field(
        default=True,
        description="Whether to extract lineage of workbook's elements and datasets or not.",
    )
    workbook_lineage_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter workbook's elements and datasets lineage in ingestion."
        "Requires extract_lineage to be enabled.",
    )
    chart_sources_platform_mapping: Dict[str, PlatformDetail] = pydantic.Field(
        default={},
        description="A mapping of the sigma workspace/workbook/chart folder path to all chart's data sources platform details present inside that folder path.",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = pydantic.Field(
        default=None, description="Sigma Stateful Ingestion Config."
    )
    workbook_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter Sigma workbook names in ingestion.",
    )
    ingest_data_models: bool = pydantic.Field(
        default=False,
        description="Whether to ingest Sigma Data Models. Each Data Model is emitted "
        "as a Container with one Dataset per element inside it (plus per-element "
        "``SchemaMetadata`` and ``UpstreamLineage``). Default is ``False`` because "
        "enabling this introduces a new entity class to the graph — existing tenants "
        "will see new Containers and Datasets appear on first ingest and will need "
        "to factor those into any soft-delete policy if they later disable this flag.",
    )
    data_model_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter Sigma Data Model names in ingestion. "
        "Requires ingest_data_models to be enabled.",
    )
