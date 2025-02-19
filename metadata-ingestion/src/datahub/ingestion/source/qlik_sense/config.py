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

    # Rest API response key constants
    DATA = "data"
    ID = "id"
    NAME = "name"
    TYPE = "type"
    ITEMID = "itemId"
    NEXT = "next"
    LINKS = "links"
    HREF = "href"
    DATASETTYPE = "datasetType"
    CREATEDAT = "createdAt"
    UPDATEDAT = "updatedAt"
    SECUREQRI = "secureQri"
    QRI = "qri"
    SPACEID = "spaceId"
    SPACE = "space"
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
    USAGE = "usage"
    CREATEDDATE = "createdDate"
    MODIFIEDDATE = "modifiedDate"
    RESOURCEID = "resourceId"
    DATASETSCHEMA = "datasetSchema"
    GRAPH = "graph"
    NODES = "nodes"
    RESOURCES = "resources"
    LINEAGE = "lineage"
    TABLELABEL = "tableLabel"
    TABLEQRI = "tableQRI"
    OWNERID = "ownerId"
    # Websocket response key constants
    QID = "qId"
    RESULT = "result"
    QRETURN = "qReturn"
    QTYPE = "qType"
    QHANDLE = "qHandle"
    QLAYOUT = "qLayout"
    QMETA = "qMeta"
    QCHILDLIST = "qChildList"
    QITEMS = "qItems"
    QINFO = "qInfo"
    QLIST = "qList"
    CONNECTORPROPERTIES = "connectorProperties"
    TABLEQUALIFIERS = "tableQualifiers"
    CONNECTIONINFO = "connectionInfo"
    SOURCECONNECTORID = "sourceConnectorID"
    DATABASENAME = "databaseName"
    SCHEMANAME = "schemaName"
    TABLES = "tables"
    DATACONNECTORID = "dataconnectorid"
    DATACONNECTORNAME = "dataconnectorName"
    DATACONNECTORPLATFORM = "dataconnectorPlatform"
    # Item type
    APP = "app"
    DATASET = "dataset"
    # Personal entity constants
    PERSONAL_SPACE_ID = "personal-space-id"
    PERSONAL_SPACE_NAME = "personal_space"
    # Hypercube
    HYPERCUBE = "qHyperCube"


@dataclass
class QlikSourceReport(StaleEntityRemovalSourceReport):
    number_of_spaces: int = 0

    def report_number_of_spaces(self, number_of_spaces: int) -> None:
        self.number_of_spaces = number_of_spaces


class PlatformDetail(PlatformInstanceConfigMixin, EnvConfigMixin):
    pass


class QlikSourceConfig(
    StatefulIngestionConfigBase, PlatformInstanceConfigMixin, EnvConfigMixin
):
    tenant_hostname: str = pydantic.Field(description="Qlik Tenant hostname")
    api_key: str = pydantic.Field(description="Qlik API Key")
    # Qlik space identifier
    space_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter Qlik spaces in ingestion."
        "Mention 'personal_space' if entities of personal space need to ingest",
    )
    ingest_owner: Optional[bool] = pydantic.Field(
        default=True,
        description="Ingest Owner from source. This will override Owner info entered from UI",
    )
    # Qlik app dataset upstream tables from data connection to platform instance mapping
    data_connection_to_platform_instance: Dict[str, PlatformDetail] = pydantic.Field(
        default={},
        description="A mapping of the Qlik app dataset upstream tables from data connection to platform instance."
        "Use 'data_connection_name' as key.",
    )
