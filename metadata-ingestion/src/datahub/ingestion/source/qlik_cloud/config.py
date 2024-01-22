import logging
from dataclasses import dataclass
from typing import Optional

import pydantic

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)

logger = logging.getLogger(__name__)

QLIK_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


class Constant:
    """
    keys used in qlik plugin
    """

    PLATFORM_NAME = "qlikcloud"
    DATA = "data"
    ID = "id"
    NAME = "name"
    DESCRIPTION = "description"
    TYPE = "type"
    OWNERID = "ownerId"
    CREATEDAT = "createdAt"
    UPDATEDAT = "updatedAt"
    SECUREQRI = "secureQri"
    QRI = "qri"
    SPACEID = "spaceId"
    CREATEDTIME = "createdTime"
    LASTMODIFIEDTIME = "lastModifiedTime"
    OPERATIONAL = "operational"
    SIZE = "size"
    ROWCOUNT = "rowCount"
    DATATYPE = "dataType"
    PRIMARYKEY = "primaryKey"
    NULLABLE = "nullable"
    SCHEMA = "schema"
    DATAFIELDS = "dataFields"
    RESOURCETYPE = "resourceType"
    RESOURCEATTRIBUTES = "resourceAttributes"
    USAGE = "usage"
    CREATEDDATE = "createdDate"
    MODIFIEDDATE = "modifiedDate"
    RESOURCEID = "resourceId"
    # Item type
    APP = "app"
    DATASET = "dataset"
    # Personal entity constants
    PERSONAL_SPACE_ID = "personal-space-id"
    PERSONAL_SPACE_NAME = "personal_space"


@dataclass
class QlikSourceReport(StaleEntityRemovalSourceReport):
    number_of_spaces: int = 0

    def report_number_of_spaces(self, number_of_spaces: int) -> None:
        self.number_of_spaces = number_of_spaces


class QlikSourceConfig(StatefulIngestionConfigBase, DatasetSourceConfigMixin):
    tenant_hostname: str = pydantic.Field(description="Qlik Tenant hostname")
    api_key: str = pydantic.Field(description="Qlik API Key")
    # Qlik space identifier
    space_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter Qlik spaces in ingestion",
    )
    extract_personal_entity: Optional[bool] = pydantic.Field(
        default=False,
        description="Whether personal space and apps and datasets should be ingested.",
    )
    ingest_owner: Optional[bool] = pydantic.Field(
        default=True,
        description="Ingest Owner from source. This will override Owner info entered from UI",
    )
    # Configuration for stateful ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = pydantic.Field(
        default=None, description="Qlik Stateful Ingestion Config."
    )
