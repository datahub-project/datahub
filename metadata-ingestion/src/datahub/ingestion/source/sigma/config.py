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

    # Sheet upstream skipped because the upstream element was filtered out
    # of the chart map (e.g. pivot-table blocked by page-element allowlist).
    num_filtered_sheet_upstreams: int = 0

    # DM element emission / upstream resolution.
    data_model_elements_emitted: int = 0
    data_model_element_intra_upstreams: int = 0
    data_model_element_external_upstreams: int = 0
    # Split intentionally so operators can triage "upstream dataset
    # exists but wasn't ingested in this run" (typically a pattern
    # filter or missing read perm) vs "source_id shape we do not yet
    # parse" (cross-DM refs ahead of the follow-up PR, or a future
    # Sigma shape). ``data_model_element_upstreams_unresolved`` is
    # kept as an aggregate for dashboards that already read it.
    data_model_element_upstreams_unresolved_external: int = 0
    data_model_element_upstreams_unknown_shape: int = 0
    data_model_element_upstreams_unresolved: int = 0

    # /columns entries with ``elementId = None`` (DM-global calculations),
    # dropped because there is no element Dataset to attach them to.
    data_model_columns_without_element_dropped: int = 0

    # Duplicate ``column.name`` on a single DM element, dropped to avoid
    # ``SchemaMetadata`` with duplicate ``fieldPath`` values.
    data_model_element_columns_duplicate_fieldpath_dropped: int = 0

    # Entries dropped as duplicates by the pagination-level natural-key
    # dedup in ``_paginated_entries`` / lineage raw dedup. Normally 0;
    # non-zero indicates an echoed pagination cursor or server-side
    # overlap between pages -- correctness is preserved (no double
    # emission) but the signal is surfaced here so operators can spot it.
    pagination_duplicate_entries_dropped: int = 0
    # Entries dropped by per-endpoint ``ValidationError`` handling. Only
    # the first ``_MAX_MALFORMED_WARNINGS_PER_ENDPOINT`` rows per endpoint
    # emit a user-visible warning to prevent report flooding on a
    # vendor-wide regression; this counter captures the rest.
    pagination_malformed_entries_dropped: int = 0


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
        "``SchemaMetadata`` and, when ``extract_lineage`` is also enabled, "
        "``UpstreamLineage``). Default is ``False`` because "
        "enabling this introduces a new entity class to the graph — existing tenants "
        "will see new Containers and Datasets appear on first ingest and will need "
        "to factor those into any soft-delete policy if they later disable this flag. "
        "Enabling this issues ``/dataModels/{id}/elements`` and ``/columns`` calls "
        "per Data Model unconditionally; the ``/lineage`` call is only issued when "
        "``extract_lineage`` is also ``True`` (so users who opt out of lineage at "
        "the workbook surface don't get a lineage endpoint hit under a different flag).",
    )
    data_model_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter Sigma Data Model names in ingestion. "
        "Requires ingest_data_models to be enabled.",
    )
