import logging
from typing import Dict, List, Optional

from pydantic import Field, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern, TransparentSecretStr
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulIngestionConfigBase,
    StatefulStaleMetadataRemovalConfig,
)

logger = logging.getLogger(__name__)

# IDMC connParams["Connection Type"] → DataHub platform. Informatica doesn't
# publish the canonical list, so entries cover observed native connectors
# (CamelCase/Snake_Case), legacy CDK connectors (``TOOLKIT_CCI_*``), and
# versioned variants (``_v2`` / ``V2``). Unknown types surface as unresolved
# — users can extend via ``connection_type_platform_map`` in the recipe.
CONNECTION_TYPE_MAP: Dict[str, str] = {
    # --- Relational databases ---
    "Oracle": "oracle",
    "Oracle_CDC": "oracle",
    "MySql": "mysql",
    "MySql_CDC": "mysql",
    "PostgreSql": "postgres",
    "PostgreSql_CDC": "postgres",
    "SqlServer": "mssql",
    "SqlServer_ODBC": "mssql",
    "SqlServer_CDC": "mssql",
    "TOOLKIT_CCI_DB2": "db2",
    "DB2": "db2",
    "IBM_DB2": "db2",
    "TOOLKIT_CCI_SAP_HANA": "hana",
    "SAP_HANA": "hana",
    "TOOLKIT_CCI_TERADATA": "teradata",
    "Teradata": "teradata",
    "Teradata_PT": "teradata",
    "Greenplum": "greenplum",
    "Vertica": "vertica",
    "Sybase": "sybase",
    # --- Cloud data warehouses ---
    "Snowflake_Cloud_Data_Warehouse": "snowflake",
    "Snowflake_Cloud_Data_Warehouse_v2": "snowflake",
    "Snowflake_Data_Cloud": "snowflake",
    "Redshift": "redshift",
    "Redshift_v2": "redshift",
    "Amazon_Redshift_v2": "redshift",
    "TOOLKIT_CCI_GOOGLE_BIG_QUERY": "bigquery",
    "Google_BigQuery": "bigquery",
    "Google_BigQuery_v2": "bigquery",
    "GoogleBigQuery": "bigquery",
    "GoogleBigQuery_v2": "bigquery",
    "TOOLKIT_CCI_AZURE_SQL": "mssql",
    "Azure_SQL_Database": "mssql",
    "Azure_SQL": "mssql",
    "TOOLKIT_CCI_AZURE_SYNAPSE": "mssql",
    "Azure_Synapse_SQL": "mssql",
    "Azure_Synapse_Analytics": "mssql",
    "TOOLKIT_CCI_DATABRICKS": "databricks",
    "Databricks": "databricks",
    "Databricks_Delta": "databricks",
    "Databricks_Lakehouse": "databricks",
    # --- Cloud object storage ---
    "S3": "s3",
    "Amazon_S3": "s3",
    "Amazon_S3_v2": "s3",
    "AmazonS3": "s3",
    "AmazonS3_v2": "s3",
    "Google_Cloud_Storage": "gcs",
    "Google_Cloud_Storage_v2": "gcs",
    "GoogleCloudStorage": "gcs",
    "GoogleCloudStorage_v2": "gcs",
    "Azure_Blob_Storage": "abs",
    "Azure_Blob_Storage_v2": "abs",
    "Azure_Data_Lake_Store": "abs",
    "Azure_Data_Lake_Store_Gen2": "abs",
    "Azure_Data_Lake_Store_V2": "abs",
    "ADLS_Gen2": "abs",
    # --- NoSQL and big data ---
    "MongoDB": "mongodb",
    "MongoDB_v2": "mongodb",
    "Cassandra": "cassandra",
    "Hive": "hive",
    "Apache_Hive": "hive",
    "HDFS": "hdfs",
    "Hadoop_HDFS": "hdfs",
    "ElasticSearch": "elasticsearch",
    "Elasticsearch": "elasticsearch",
    "Kafka": "kafka",
    "Apache_Kafka": "kafka",
    # --- Flat / semi-structured ---
    "FlatFile": "file",
    "CSVFile": "file",
    "FTP": "file",
    "SFTP": "file",
    # --- SaaS apps (lineage emitted but dataset side is usually external) ---
    "Salesforce": "salesforce",
    "NetSuite": "netsuite",
    "Workday": "workday",
    "Workday_v2": "workday",
    # --- Short-name aliases for name-substring inference ---
    # Catches connections where conn_type is empty and only the human name
    # hints at the platform (e.g. "Sample Snowflake Connection"). Longer keys
    # above still win via longest-match.
    "Snowflake": "snowflake",
    "BigQuery": "bigquery",
    "PostgreSQL": "postgres",
    "Postgres": "postgres",
    "SQL_Server": "mssql",
    "SQLServer": "mssql",
}

DEFAULT_PAGE_SIZE = 200
DEFAULT_EXPORT_BATCH_SIZE = 1000
DEFAULT_EXPORT_POLL_TIMEOUT_SECS = 300
DEFAULT_EXPORT_POLL_INTERVAL_SECS = 5
DEFAULT_LOGIN_TIMEOUT_SECS = 30
DEFAULT_SESSION_VALIDATION_TIMEOUT_SECS = 10
DEFAULT_REQUEST_TIMEOUT_SECS = 60


class InformaticaSourceConfig(
    PlatformInstanceConfigMixin, EnvConfigMixin, StatefulIngestionConfigBase
):
    """Configuration for Informatica Cloud (IDMC) ingestion source."""

    # Authentication
    login_url: str = Field(
        default="https://dm-us.informaticacloud.com",
        description=(
            "Informatica Cloud login URL. This is the regional pod URL, not the "
            "runtime serverUrl. After login, the connector discovers the actual "
            "API base URL from the login response. "
            "Common values: https://dm-us.informaticacloud.com (US), "
            "https://dm2-us.informaticacloud.com (US2), "
            "https://dm-em.informaticacloud.com (EMEA), "
            "https://dm-ap.informaticacloud.com (APAC)."
        ),
    )

    username: str = Field(
        description="Informatica Cloud username (email or service account name).",
    )

    password: TransparentSecretStr = Field(
        description="Informatica Cloud password.",
    )

    # Filtering — Layer 1: Scope by org structure
    project_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter IDMC projects by name. "
            "Only projects matching these patterns will be ingested. "
            "Example: allow: ['Production.*'], deny: ['.*_sandbox']"
        ),
    )

    folder_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter IDMC folders by name. "
            "Only folders matching these patterns will be ingested."
        ),
    )

    # Filtering — Layer 2: Fine-tune by name
    mapping_task_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter Mapping Tasks. "
            "Matched against '<folder_path>/<name>' so same-named tasks in "
            "different folders can be targeted independently "
            "(e.g. allow: ['.*/ProjectA/MyTask'])."
        ),
    )

    taskflow_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns to filter Taskflows. "
            "Matched against '<folder_path>/<name>' so same-named taskflows in "
            "different folders can be targeted independently "
            "(e.g. allow: ['.*/ProjectA/MyFlow'])."
        ),
    )

    # Filtering — Layer 3: Tag-based (recommended for large orgs)
    tag_filter_names: List[str] = Field(
        default=[],
        description=(
            "List of literal IDMC tag names. When set, only objects tagged with "
            "at least one of these tags will be ingested. Tags are matched exactly "
            "(not regex). This is the recommended filtering approach for large orgs — "
            "IDMC admins tag objects in the UI and the connector picks them up."
        ),
    )

    @field_validator("tag_filter_names")
    @classmethod
    def _strip_blank_tag_names(cls, v: List[str]) -> List[str]:
        # IDMC rejects blank tag names server-side with an opaque 400;
        # catch the mistake at config parse time with a clear error.
        cleaned = [t.strip() for t in v if t and t.strip()]
        if len(cleaned) != len(v):
            raise ValueError(
                "tag_filter_names contained empty or whitespace-only entries; "
                "remove them or populate with IDMC tag names."
            )
        return cleaned

    # Features
    extract_lineage: bool = Field(
        default=True,
        description=(
            "Whether to extract table-level lineage from mapping definitions. "
            "Requires the 'Asset - export' privilege on the service account. "
            "When enabled, uses the v3 Export API to fetch full mapping definitions."
        ),
    )

    extract_ownership: bool = Field(
        default=True,
        description=(
            "Whether to extract ownership from IDMC object createdBy/updatedBy fields."
        ),
    )

    strip_user_email_domain: bool = Field(
        default=False,
        description=(
            "Strip the domain from IDMC user identifiers before forming the "
            "CorpUser URN (e.g. ``alice@acme.com`` → ``urn:li:corpuser:alice``). "
            "Enable when your Okta/AzureAD source ingests users without the "
            "email domain so ownership edges align with existing CorpUser URNs."
        ),
    )

    extract_tags: bool = Field(
        default=True,
        description=(
            "Emit IDMC object tags as DataHub GlobalTags on Projects, Folders, "
            "Taskflows, and Mapping Tasks. Set to False to skip tag extraction."
        ),
    )

    connection_type_overrides: Dict[str, str] = Field(
        default={},
        description=(
            "Per-connection-ID override mapping IDMC connection id → DataHub "
            "platform name. Use when a single connection can't be auto-resolved "
            "(e.g., a one-off custom connector). "
            "Example: {'01DM180B000000000008': 'snowflake'}. "
            "Takes priority over `connection_type_platform_map` and the built-in "
            "CONNECTION_TYPE_MAP."
        ),
    )

    connection_type_platform_map: Dict[str, str] = Field(
        default={},
        description=(
            "Extend the built-in connection-type → DataHub-platform map with "
            'custom entries. Keys are the IDMC `connParams["Connection Type"]` '
            "string (or the connection's top-level `type` as a fallback), values "
            "are DataHub platform names. Useful for new IDMC marketplace "
            "connectors that aren't in the built-in map yet. "
            "Example: {'MyCustomConnector_v3': 'snowflake', 'CompanyDW': 'postgres'}. "
            "Entries here are merged with (and override) CONNECTION_TYPE_MAP."
        ),
    )

    convert_urns_to_lowercase: bool = Field(
        default=True,
        description=(
            "Lowercase the dataset qualifier in emitted upstream URNs to match "
            "the default behavior of the Snowflake, Postgres, and BigQuery sources "
            "(which lowercase by default). Set to False only if you've disabled "
            "lowercasing on every source this connector produces lineage to."
        ),
    )

    connection_to_platform_instance: Dict[str, str] = Field(
        default={},
        description=(
            "Map IDMC connection ID → DataHub `platform_instance` to use when "
            "building upstream/downstream dataset URNs. Required whenever the "
            "target source was ingested with a non-default platform_instance; "
            "otherwise lineage edges will point at URNs that don't exist in "
            "DataHub. Example: {'01DM180B000000000008': 'prod_sf'}."
        ),
    )

    # Performance tuning
    page_size: int = Field(
        default=DEFAULT_PAGE_SIZE,
        description="Number of objects to fetch per API page (max 200 for v3 objects).",
        ge=1,
        le=200,
    )

    export_batch_size: int = Field(
        default=DEFAULT_EXPORT_BATCH_SIZE,
        description="Number of mappings per v3 export batch job (max 1000).",
        ge=1,
        le=1000,
    )

    export_poll_timeout_secs: int = Field(
        default=DEFAULT_EXPORT_POLL_TIMEOUT_SECS,
        description="Timeout in seconds for polling export job completion.",
        ge=30,
        le=3600,
    )

    export_poll_interval_secs: int = Field(
        default=DEFAULT_EXPORT_POLL_INTERVAL_SECS,
        description="Interval in seconds between export job status polls.",
        ge=1,
        le=600,
    )

    request_timeout_secs: int = Field(
        default=DEFAULT_REQUEST_TIMEOUT_SECS,
        description=(
            "HTTP timeout in seconds for IDMC API requests. Raise this for large "
            "deployments where /api/v2/mapping or /api/v2/connection returns many "
            "records and the default 60s is insufficient."
        ),
        ge=5,
        le=600,
    )

    max_concurrent_export_jobs: int = Field(
        default=4,
        description=(
            "Maximum number of v3 export jobs to run concurrently. Each job covers "
            "one batch of mappings. Increase to reduce lineage wall-clock time on "
            "large orgs; decrease if hitting IDMC rate limits."
        ),
        ge=1,
        le=8,
    )

    # Stateful ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Configuration for stateful ingestion and stale entity removal.",
    )

    @field_validator("login_url")
    @classmethod
    def _validate_login_url(cls, v: str) -> str:
        # IDMC has no http endpoints; reject at parse time rather than
        # silently sending credentials in clear text.
        if not v:
            raise ValueError("login_url must not be empty")
        url = v.rstrip("/")
        if not url.startswith("https://"):
            raise ValueError(f"login_url must start with https://, got: {v}")
        return url

    @model_validator(mode="after")
    def validate_poll_bounds(self) -> "InformaticaSourceConfig":
        if self.export_poll_interval_secs >= self.export_poll_timeout_secs:
            raise ValueError(
                "export_poll_interval_secs must be less than export_poll_timeout_secs "
                f"(interval={self.export_poll_interval_secs}, "
                f"timeout={self.export_poll_timeout_secs})"
            )
        return self

    @field_validator("connection_type_overrides")
    @classmethod
    def validate_connection_type_overrides(
        cls, overrides: Dict[str, str]
    ) -> Dict[str, str]:
        known_platforms = set(CONNECTION_TYPE_MAP.values())
        for conn_id, platform in overrides.items():
            if not platform:
                raise ValueError(
                    f"connection_type_overrides[{conn_id}] has empty platform name"
                )
            if platform not in known_platforms:
                logger.warning(
                    "connection_type_overrides[%s] uses platform %r which is "
                    "not in the built-in platform map; double-check spelling.",
                    conn_id,
                    platform,
                )
        return overrides

    @field_validator("connection_type_platform_map")
    @classmethod
    def validate_connection_type_platform_map(
        cls, type_map: Dict[str, str]
    ) -> Dict[str, str]:
        for conn_type, platform in type_map.items():
            if not conn_type:
                raise ValueError(
                    "connection_type_platform_map has an empty connection-type key; "
                    "keys must match the IDMC `connParams['Connection Type']` string."
                )
            if not platform:
                raise ValueError(
                    f"connection_type_platform_map[{conn_type!r}] has empty platform name"
                )
        return type_map
