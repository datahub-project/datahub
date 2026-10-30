import logging
from dataclasses import dataclass
from typing import Dict, Optional

from pydantic import Field, model_validator

from datahub.configuration.common import AllowDenyPattern, TransparentSecretStr
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
from datahub.utilities.str_enum import StrEnum

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


class QlikDeployment(StrEnum):
    CLOUD = "cloud"
    ON_PREM = "on_prem"


class QlikSourceConfig(
    StatefulIngestionConfigBase, PlatformInstanceConfigMixin, EnvConfigMixin
):
    deployment: QlikDeployment = Field(
        default=QlikDeployment.CLOUD,
        description="Qlik Cloud (`cloud`) or Qlik Sense Enterprise on Windows (`on_prem`).",
    )
    tenant_hostname: Optional[str] = Field(
        default=None,
        description="Qlik Cloud tenant hostname. Required when deployment is cloud.",
    )
    api_key: Optional[TransparentSecretStr] = Field(
        default=None,
        description="Qlik Cloud API key. Required when deployment is cloud.",
    )
    server: Optional[str] = Field(
        default=None,
        description="Qlik Sense Enterprise hostname. Required when deployment is on_prem. "
        "Must match the certificate name. Repository Service and Engine ports are reached on this host.",
    )
    qrs_port: int = Field(
        default=4242,
        description="Qlik Repository Service port used for on-prem inventory.",
    )
    engine_port: int = Field(
        default=4747,
        description="Qlik Engine port used for on-prem app, sheet, and chart reads.",
    )
    client_cert: Optional[str] = Field(
        default=None,
        description="Path to the PEM client certificate exported from the QMC. Required on-prem.",
    )
    client_key: Optional[str] = Field(
        default=None,
        description="Path to the PEM client key exported from the QMC. Required on-prem.",
    )
    root_cert: Optional[str] = Field(
        default=None,
        description="Path to the PEM root certificate exported from the QMC. Required on-prem.",
    )
    user_directory: str = Field(
        default="INTERNAL",
        description="User directory for the on-prem X-Qlik-User header.",
    )
    qrs_user_id: str = Field(
        default="sa_repository",
        description="User id for Repository Service calls. Certificate auth defaults to the repository service account.",
    )
    engine_user_id: str = Field(
        default="sa_engine",
        description="User id for Engine websocket calls. Certificate auth defaults to the engine service account.",
    )
    ingest_unpublished_apps: bool = Field(
        default=False,
        description="On-prem only. Also ingest apps that are not published to a stream.",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Configuration for stateful ingestion and stale metadata removal.",
    )
    # Qlik space identifier
    space_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns to filter Qlik spaces in ingestion."
        "Mention 'personal_space' if entities of personal space need to ingest",
    )
    ingest_owner: Optional[bool] = Field(
        default=True,
        description="Ingest Owner from source. This will override Owner info entered from UI",
    )
    # Qlik app dataset upstream tables from data connection to platform instance mapping
    data_connection_to_platform_instance: Dict[str, PlatformDetail] = Field(
        default={},
        description="A mapping of the Qlik app dataset upstream tables from data connection to platform instance."
        "Use 'data_connection_name' as key.",
    )

    @model_validator(mode="after")
    def validate_deployment_credentials(self) -> "QlikSourceConfig":
        if self.deployment == QlikDeployment.ON_PREM:
            missing = [
                name
                for name, value in (
                    ("server", self.server),
                    ("client_cert", self.client_cert),
                    ("client_key", self.client_key),
                    ("root_cert", self.root_cert),
                )
                if not value
            ]
            if missing:
                raise ValueError("On-prem Qlik Sense requires " + ", ".join(missing))
            return self
        if not self.tenant_hostname or self.api_key is None:
            raise ValueError("Qlik Cloud requires tenant_hostname and api_key")
        return self

    def ui_hostname(self) -> str:
        if self.deployment == QlikDeployment.ON_PREM:
            assert self.server is not None
            return self.server
        assert self.tenant_hostname is not None
        return self.tenant_hostname

    def qlik_user_header(self, user_id: str) -> str:
        return f"UserDirectory={self.user_directory}; UserId={user_id}"
