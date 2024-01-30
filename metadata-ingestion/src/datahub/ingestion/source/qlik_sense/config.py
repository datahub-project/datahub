import logging
from dataclasses import dataclass
from typing import Optional

import pydantic

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)

logger = logging.getLogger(__name__)

QLIK_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


class WebSocketRequest:
    """
    Web socket send request dict
    """

    OPEN_DOC = {
        "jsonrpc": "2.0",
        "id": 1,
        "handle": -1,
        "method": "OpenDoc",
        "params": {"qDocName": "f0714ca7-7093-49e4-8b58-47bb38563647"},
    }


class Constant:
    """
    keys used in qlik plugin
    """

    # Rest API response key constants
    DATA = "data"
    NAME = "name"
    TYPE = "type"
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
    DATASETSCHEMA = "datasetSchema"
    # Websocket response key constants
    QID = "qId"
    RESULT = "result"
    QRETURN = "qReturn"
    QHANDLE = "qHandle"
    QLAYOUT = "qLayout"
    QMETA = "qMeta"
    QCHILDLIST = "qChildList"
    QITEMS = "qItems"
    QINFO = "qInfo"
    QLIST = "qList"
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


class QlikSourceConfig(
    StatefulIngestionConfigBase, PlatformInstanceConfigMixin, EnvConfigMixin
):
    tenant_hostname: str = pydantic.Field(description="Qlik Tenant hostname")
    api_key: str = pydantic.Field(description="Qlik API Key")
    # Qlik space identifier
    space_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter Qlik spaces in ingestion",
    )
    extract_personal_entity: Optional[bool] = pydantic.Field(
        default=False,
        description="Whether personal space, apps and datasets should be ingested.",
    )
    ingest_owner: Optional[bool] = pydantic.Field(
        default=True,
        description="Ingest Owner from source. This will override Owner info entered from UI",
    )
