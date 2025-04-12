import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pydantic
from pydantic import BaseModel, Field

from datahub.configuration.common import AllowDenyPattern
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
    ENTRIES = "entries"
    MEMBERID = "memberId"
    FIRSTNAME = "firstName"
    LASTNAME = "lastName"
    EDGES = "edges"
    DEPENDENCIES = "dependencies"
    SOURCE = "source"
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

    # Source Config constants
    DEFAULT_API_URL = "https://aws-api.sigmacomputing.com/v2"


class WorkspaceCounts(BaseModel):
    workbooks_count: int = 0
    datasets_count: int = 0
    elements_count: int = 0
    pages_count: int = 0

    def is_empty(self) -> bool:
        return (
            self.workbooks_count == 0
            and self.datasets_count == 0
            and self.elements_count == 0
            and self.pages_count == 0
        )

    def as_obj(self) -> dict:
        return {
            "workbooks_count": self.workbooks_count,
            "datasets_count": self.datasets_count,
            "elements_count": self.elements_count,
            "pages_count": self.pages_count,
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

    number_of_files_metadata: Dict[str, int] = field(default_factory=dict)
    empty_workspaces: List[str] = field(default_factory=list)


class PlatformDetail(PlatformInstanceConfigMixin, EnvConfigMixin):
    data_source_platform: str = pydantic.Field(
        description="A chart's data sources platform name.",
    )


class SigmaSourceConfig(
    StatefulIngestionConfigBase, PlatformInstanceConfigMixin, EnvConfigMixin
):
    api_url: str = pydantic.Field(
        default=Constant.DEFAULT_API_URL, description="Sigma API hosted URL."
    )
    client_id: str = pydantic.Field(description="Sigma Client ID")
    client_secret: str = pydantic.Field(description="Sigma Client Secret")
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
