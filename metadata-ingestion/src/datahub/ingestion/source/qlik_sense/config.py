import logging
from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional, Union

from pydantic import Field, field_validator, model_validator

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    TransparentSecretStr,
)
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.qlik_sense.file_lineage import file_storage_of_uri
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.utilities.lossy_collections import LossyList

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
    FINGERPRINT = "ingestionFingerprint"
    CREATEDDATE = "createdDate"
    MODIFIEDDATE = "modifiedDate"
    RESOURCEID = "resourceId"
    DATASETSCHEMA = "datasetSchema"
    GRAPH = "graph"
    NODES = "nodes"
    EDGES = "edges"
    SOURCE = "source"
    TARGET = "target"
    RESOURCEATTRIBUTES = "resourceAttributes"
    RESOURCES = "resources"
    LINEAGE = "lineage"
    TABLELABEL = "tableLabel"
    TABLEQRI = "tableQRI"
    OWNERID = "ownerId"
    PUBLISHED = "published"
    USERID = "userId"
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
    QSCRIPT = "qScript"
    # Data connection keys (Qlik Cloud REST and on-prem QRS)
    QNAME = "qName"
    DATASOURCEID = "datasourceID"
    QCONNECTSTATEMENT = "qConnectStatement"
    CONNECTIONSTRING = "connectionstring"
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
    # Container objects hold charts as tabs
    CONTAINER = "container"
    QDATA = "qData"
    CONTAINERCHILDID = "containerChildId"
    CHILDREN = "children"
    REFID = "refId"
    LABEL = "label"
    # Chart field references
    QGROUPFIELDDEFS = "qGroupFieldDefs"
    QPROPERTY = "qProp"
    QPROPENTRY = "qPropEntry"
    QPROPERTYTREEITEM = "qProperty"
    QCHILDREN = "qChildren"
    QHYPERCUBEDEF = "qHyperCubeDef"
    QEXTENDSID = "qExtendsId"
    QMEASURES = "qMeasures"
    QDEF = "qDef"
    QLIBRARYID = "qLibraryId"
    QMEASURE = "qMeasure"


@dataclass
class QlikSourceReport(StaleEntityRemovalSourceReport):
    number_of_spaces: int = 0
    script_tables_found: int = 0
    script_tables_with_lineage: int = 0
    apps_without_load_model: LossyList[str] = field(default_factory=LossyList)
    qvds_found: int = 0
    # QVD paths a load script names that resolve to no single file, such as a
    # wildcard, a variable, a relative path or an unknown data connection.
    unresolved_qvd_paths: LossyList[str] = field(default_factory=LossyList)
    # QVDs loaded without a table label, which Qlik names itself.
    unlabelled_qvd_loads: LossyList[str] = field(default_factory=LossyList)
    # Apps Qlik Cloud has no lineage graph for, read as having no tables.
    apps_without_lineage_graph: LossyList[str] = field(default_factory=LossyList)
    unresolved_data_connections: LossyList[str] = field(default_factory=LossyList)
    file_sources_resolved: int = 0
    # File data connections whose files an app reads but whose storage is not
    # known: add them to file_connection_to_storage.
    unresolved_file_connections: LossyList[str] = field(default_factory=LossyList)
    apps_read: int = 0
    apps_skipped_unchanged: int = 0
    # Apps read again because checking whether they changed failed.
    apps_change_check_failed: LossyList[str] = field(default_factory=LossyList)

    def report_number_of_spaces(self, number_of_spaces: int) -> None:
        self.number_of_spaces = number_of_spaces


class PlatformDetail(PlatformInstanceConfigMixin, EnvConfigMixin):
    platform: Optional[str] = Field(
        default=None,
        description="DataHub platform of the tables read through this connection, "
        "e.g. `mssql` or `databricks`. Inferred from the data connection when not set.",
    )
    database: Optional[str] = Field(
        default=None,
        description="Database (catalog for Databricks) used for tables the load script "
        "does not qualify. Inferred from the connection string when not set.",
    )
    default_schema: Optional[str] = Field(
        default=None,
        description="Schema used for tables the load script does not qualify.",
    )


class FileStorageDetail(PlatformInstanceConfigMixin, EnvConfigMixin):
    storage_uri: Optional[str] = Field(
        default=None,
        description="Where the connection's files are, as an `abfss://<container>@<account>"
        ".dfs.core.windows.net/<folder>`, `s3://<bucket>/<folder>` or "
        "`gs://<bucket>/<folder>` URI, or Azure HTTPS. An Azure URI must name the "
        "container; `wasb://` and `wasbs://` are not accepted. Read from the data "
        "connection when not set, which needs the API key's user to see the "
        "connection.",
    )

    convert_urns_to_lowercase: bool = Field(
        default=False,
        description="Lowercase the file dataset names, to match a storage source "
        "run with `convert_urns_to_lowercase`.",
    )

    @field_validator("storage_uri")
    @classmethod
    def storage_uri_is_supported(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and file_storage_of_uri(value) is None:
            raise ValueError(
                f"Unsupported storage_uri {value!r}: use an abfss:// or Azure HTTPS "
                "URI that names the container, or an s3:// or gs:// URI"
            )
        return value


class QlikOnPremConfig(ConfigModel):
    server: str = Field(
        min_length=1,
        description="Qlik Sense Enterprise hostname. Must match the certificate name. "
        "Repository Service and Engine ports are reached on this host.",
    )
    client_cert: str = Field(
        min_length=1,
        description="Path to the PEM client certificate exported from the QMC.",
    )
    client_key: str = Field(
        min_length=1,
        description="Path to the PEM client key exported from the QMC.",
    )
    root_cert: str = Field(
        min_length=1,
        description="Path to the PEM root certificate exported from the QMC.",
    )
    client_key_password: Optional[TransparentSecretStr] = Field(
        default=None,
        description="Password of the client key, when it was exported from the QMC "
        "with one.",
    )
    qrs_port: int = Field(
        default=4242,
        ge=1,
        le=65535,
        description="Qlik Repository Service port used for inventory.",
    )
    engine_port: int = Field(
        default=4747,
        ge=1,
        le=65535,
        description="Qlik Engine port used for app, sheet, and chart reads.",
    )
    server_cert_hostname: Optional[str] = Field(
        default=None,
        description="Hostname the Qlik server certificate was issued to, when it "
        "differs from `server` (for example when connecting by IP address). The certificate is "
        "still verified against `root_cert`, but its name is checked against this value.",
    )
    verify_server_hostname: bool = Field(
        default=True,
        description="Set to false to skip the certificate name check when no "
        "hostname matches it. The certificate chain is still verified against `root_cert`.",
    )
    user_directory: str = Field(
        default="INTERNAL",
        description="User directory for the X-Qlik-User header.",
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
        description="Also ingest apps that are not published to a stream.",
    )
    skip_unchanged_apps: bool = Field(
        default=False,
        description="Do not re-read an app from the Engine when the app, its "
        "objects (sheets, stories, master items), its reload time, the data connections, this "
        "recipe and the connector version are all unchanged since the last run, and DataHub "
        "still holds that run's metadata for it. Its entities are kept, not soft-deleted. "
        "Requires stateful ingestion. Set `stateful_ingestion.ignore_old_state` for one run "
        "to force a full read, for example after ingesting upstream schemas that column-level "
        "lineage should resolve against.",
    )

    read_data_model: bool = Field(
        default=False,
        description="Open each app with its data to read its data model, which "
        "gives every table its fields and types, including tables loaded with "
        "`LOAD *` and from QVDs, and lets charts link to exactly the tables and "
        "fields they use. Off by default: apps are then opened without their "
        "data, which is much faster, and table fields come only from the load "
        "script. Opening an app with its data loads it into Engine memory until "
        "the app cache time set in the QMC has passed since its last session, "
        "reads one app at a time unless `max_workers` is set, and fails for apps "
        "whose section access denies the service user their data.",
    )
    qvd_folder_aliases: Dict[str, str] = Field(
        default={},
        description="Folder paths to treat as the same folder when matching QVD "
        "files across apps, as the path one app names -> the path to use. For "
        "example `{'//qlik-sense/qvd': '//qlik-sense.corp.local/qvd'}`, when apps "
        "reach one share by different host names or drive letters. A QVD is "
        "identified by its file path, so files under these paths join into one "
        "dataset.",
    )
    qvd_ignored_connections: List[str] = Field(
        default=["ServerLogFolder", "ArchivedLogsFolder"],
        description="Folder data connections whose QVDs are not ingested. By "
        "default the connections the monitoring apps read their logs through.",
    )

    @model_validator(mode="after")
    def warn_unused_server_cert_hostname(self) -> "QlikOnPremConfig":
        if self.server_cert_hostname and not self.verify_server_hostname:
            logger.warning(
                "server_cert_hostname is ignored for TLS hostname verification because "
                "verify_server_hostname is false. It is still used for Qlik UI URLs."
            )
        return self

    def ui_hostname(self) -> str:
        return self.server_cert_hostname or self.server

    def tls_assert_hostname(self) -> Union[str, Literal[False], None]:
        if not self.verify_server_hostname:
            return False
        return self.server_cert_hostname

    def qlik_user_header(self, user_id: str) -> str:
        return f"UserDirectory={self.user_directory}; UserId={user_id}"


class QlikSourceConfig(
    StatefulIngestionConfigBase, PlatformInstanceConfigMixin, EnvConfigMixin
):
    tenant_hostname: Optional[str] = Field(
        default=None,
        description="Qlik Cloud tenant hostname. Required unless `on_prem` is set.",
    )
    api_key: Optional[TransparentSecretStr] = Field(
        default=None,
        description="Qlik Cloud API key. Required unless `on_prem` is set.",
    )
    on_prem: Optional[QlikOnPremConfig] = Field(
        default=None,
        description="Connection to Qlik Sense Enterprise on Windows. Set this instead of "
        "`tenant_hostname` and `api_key` to read an on-prem site.",
    )
    max_partial_reads_kept: int = Field(
        default=3,
        ge=0,
        description="With stateful ingestion and stale metadata removal, how many "
        "consecutive runs an app that is only partly read keeps the entities of its "
        "last complete read. After that, its partial read is ingested and reported "
        "as a failure, so its current state appears and stale metadata removal is "
        "skipped for that run.",
    )
    max_workers: Optional[int] = Field(
        default=None,
        ge=1,
        description="Number of apps and datasets read at the same time, across "
        "all spaces and streams. Each app read holds one Engine session open. "
        "Defaults to 1, as every app opened with its data is loaded into Engine "
        "memory, and to 4 on-prem when `on_prem.read_data_model` is false and apps "
        "are opened without their data.",
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
    timeout_seconds: int = Field(
        default=300,
        ge=1,
        description="Seconds to wait for the Qlik Engine, and on-prem for the "
        "Repository Service, to connect or reply before the request fails. An app "
        "whose read times out is ingested as partly read.",
    )
    include_sheets_and_charts: bool = Field(
        default=True,
        description="Ingest each app's sheets as dashboards and their charts. When "
        "false, apps are read without their sheets, which is faster, and each app "
        "dashboard links only to its tables. With stale metadata removal, sheets "
        "and charts ingested earlier are soft-deleted; raise "
        "`stateful_ingestion.fail_safe_threshold` for that first run, as they are "
        "usually most of the entities and their removal otherwise trips it.",
    )
    include_column_lineage: bool = Field(
        default=True,
        description="Emit column-level lineage, from load scripts and app data "
        "models and, on Qlik Cloud, from the field level of Qlik's lineage graph, "
        "which is read for each app table and each dataset an app stores into and "
        "also gives the fields of tables a load script drops. The field-level "
        "reads cost one request per table.",
    )
    # Qlik app dataset upstream tables from data connection to platform instance mapping
    data_connection_to_platform_instance: Dict[str, PlatformDetail] = Field(
        default={},
        description="A mapping of the Qlik app dataset upstream tables from data connection to platform instance."
        "Use 'data_connection_name' as key. Set `platform`, `database` and `default_schema` when they "
        "cannot be inferred from the data connection.",
    )
    script_variables: Dict[str, str] = Field(
        default={},
        description="Values for load script variables the connector can't read, "
        "such as ones computed with LET, set in an included script, or deleted "
        "after the reload, keyed by variable name without `$()`. Used in QVD and "
        "file paths, connection names, SQL and chart expressions. An app's own "
        "variables, as its last reload left them, take precedence, and a "
        "variable the visible script sets overrides it from where it is set.",
    )
    file_connection_to_storage: Dict[str, FileStorageDetail] = Field(
        default={},
        description="Qlik Cloud only. Where the files of Azure Storage, Amazon S3 "
        "and Google Cloud Storage data connections are in DataHub, for lineage from "
        "the files apps load. Use the data connection name as key, as load scripts "
        "write it (`<space>:<connection>` or `<connection>`). Set `storage_uri` "
        "when the API key's user cannot see the connection, and `platform_instance` "
        "and `env` to match the source that ingests the storage.",
    )

    @field_validator("script_variables")
    @classmethod
    def script_variable_names_are_bare(cls, value: Dict[str, str]) -> Dict[str, str]:
        # A name such as `$(vSchema)` or ` vSchema` never matches a reference
        # in the script, so it would be ignored without notice.
        for name in value:
            if not name or name != name.strip() or any(c in name for c in "$()="):
                raise ValueError(
                    f"Invalid script_variables name {name!r}: use the variable "
                    "name alone, such as vSchema, without $() or spaces around it"
                )
        return value

    @model_validator(mode="after")
    def validate_connection(self) -> "QlikSourceConfig":
        if self.on_prem is not None:
            if self.tenant_hostname or self.api_key is not None:
                raise ValueError(
                    "Set either the on_prem block or tenant_hostname and api_key, not both"
                )
            return self
        if not self.tenant_hostname or self.api_key is None:
            raise ValueError(
                "Qlik Cloud requires tenant_hostname and api_key "
                "(set the on_prem block for Qlik Sense Enterprise on Windows)"
            )
        return self

    def resolved_max_workers(self) -> int:
        if self.max_workers is not None:
            return self.max_workers
        return 4 if self.on_prem is not None and not self.on_prem.read_data_model else 1

    def skips_unchanged_apps(self) -> bool:
        return self.on_prem is not None and self.on_prem.skip_unchanged_apps

    def ui_hostname(self) -> str:
        if self.on_prem is not None:
            return self.on_prem.ui_hostname()
        assert self.tenant_hostname is not None
        return self.tenant_hostname
