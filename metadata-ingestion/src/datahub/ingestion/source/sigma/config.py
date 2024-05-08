import logging
from dataclasses import dataclass
from typing import Dict, Optional

import pydantic

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
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
    ENTRIES = "entries"
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


@dataclass
class SigmaSourceReport(StaleEntityRemovalSourceReport):
    number_of_workspaces: int = 0

    def report_number_of_workspaces(self, number_of_workspaces: int) -> None:
        self.number_of_workspaces = number_of_workspaces


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
        "Mention 'User Folder' if entities of 'My documents' need to ingest.",
    )
    ingest_owner: Optional[bool] = pydantic.Field(
        default=True,
        description="Ingest Owner from source. This will override Owner info entered from UI",
    )
    chart_sources_platform_mapping: Dict[str, PlatformDetail] = pydantic.Field(
        default={},
        description="A mapping of the sigma workspace/workbook/chart folder path to all chart's data sources platform details present inside that folder path.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None
