import base64
import dataclasses
import json
import logging
from collections import namedtuple
from typing import Any, Dict, Iterable, List, Optional, Tuple, TypedDict, Union

from pydantic import Field, model_validator
from sqlalchemy import create_engine, text
from sqlalchemy.engine.reflection import Inspector

from datahub.configuration.common import AllowDenyPattern, HiddenFromDocs
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
    SourceCapabilityModifier,
)
from datahub.ingestion.source.sql.hive.exceptions import (
    InvalidDatasetIdentifierError,
)
from datahub.ingestion.source.sql.hive.storage_lineage import (
    HiveStorageLineage,
    HiveStorageLineageConfigMixin,
)
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    SqlWorkUnit,
    get_schema_metadata,
)
from datahub.ingestion.source.sql.sql_config import (
    BasicSQLAlchemyConfig,
    SQLCommonConfig,
)
from datahub.ingestion.source.sql.sql_utils import (
    add_table_to_schema_container,
    gen_database_container,
    gen_database_key,
    gen_schema_container,
    gen_schema_key,
    get_domain_wu,
)
from datahub.ingestion.source.sql.sqlalchemy_uri import make_sqlalchemy_uri
from datahub.ingestion.source.state.stateful_ingestion_base import JobId
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    SubTypesClass,
    ViewPropertiesClass,
)
from datahub.utilities.groupby import groupby_unsorted
from datahub.utilities.hive_schema_to_avro import get_schema_fields_for_hive_column
from datahub.utilities.str_enum import StrEnum

logger: logging.Logger = logging.getLogger(__name__)

TableKey = namedtuple("TableKey", ["schema", "table"])


# =============================================================================
# Row Type Definitions for Data-Fetching Methods
#
# These TypedDicts define the expected row format for each _fetch_* method.
# Subclasses (e.g., HiveMetastoreThriftSource) must return rows matching these
# formats to ensure compatibility with the WorkUnit generation logic.
#
# STABILITY NOTE: These are extension points. Changes to these types or the
# _fetch_* method signatures should be considered breaking changes for
# subclasses. If you modify these, also update the Thrift connector.
# =============================================================================


class TableRow(TypedDict):
    """Row format for _fetch_table_rows(). One row per column."""

    schema_name: str  # Database/schema name
    table_name: str  # Table name
    table_type: str  # EXTERNAL_TABLE, MANAGED_TABLE
    create_date: str  # YYYY-MM-DD format
    col_name: str  # Column name
    col_sort_order: int  # Column ordinal position
    col_description: str  # Column comment (may be empty)
    col_type: str  # Hive type string (e.g., "string", "struct<...>")
    is_partition_col: int  # 0 = regular column, 1 = partition column
    table_location: str  # Storage location (e.g., s3://..., hdfs://...)


class ViewRow(TypedDict):
    """Row format for _fetch_hive_view_rows(). One row per column."""

    schema_name: str  # Database/schema name
    table_name: str  # View name
    table_type: str  # VIRTUAL_VIEW
    view_expanded_text: str  # Expanded SQL definition
    description: str  # View description (may be empty)
    create_date: str  # YYYY-MM-DD format
    col_name: str  # Column name
    col_sort_order: int  # Column ordinal position
    col_description: str  # Column comment (may be empty)
    col_type: str  # Hive type string


class SchemaRow(TypedDict):
    """Row format for _fetch_schema_rows()."""

    schema: str  # Database/schema name


class TablePropertiesRow(TypedDict):
    """Row format for _fetch_table_properties_rows(). One row per property."""

    schema_name: str  # Database/schema name
    table_name: str  # Table name
    PARAM_KEY: str  # Property key
    PARAM_VALUE: str  # Property value


class HiveMetastoreConfigMode(StrEnum):
    hive = "hive"
    presto = "presto"
    presto_on_hive = "presto-on-hive"
    trino = "trino"


class HiveMetastoreConnectionType(StrEnum):
    """Connection type for HiveMetastoreSource."""

    sql = "sql"
    thrift = "thrift"


@dataclasses.dataclass
class ViewDataset:
    dataset_name: str
    schema_name: str
    columns: List[dict]
    view_definition: Optional[str] = None


class HiveMetastore(BasicSQLAlchemyConfig, HiveStorageLineageConfigMixin):
    views_where_clause_suffix: str = Field(
        default="",
        description="Where clause to specify what Presto views should be ingested.",
    )
    tables_where_clause_suffix: str = Field(
        default="",
        description="Where clause to specify what Hive tables should be ingested.",
    )
    schemas_where_clause_suffix: str = Field(
        default="",
        description="Where clause to specify what Hive schemas should be ingested.",
    )
    ingestion_job_id: str = ""
    host_port: str = Field(
        default="localhost:3306",
        description="Host URL and port to connect to. Example: localhost:3306",
    )
    scheme: HiddenFromDocs[str] = Field(default="mysql+pymysql")

    database_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for hive/presto database to filter in ingestion. Specify regex to only match the database name. e.g. to match all tables in database analytics, use the regex 'analytics'",
    )

    metastore_db_name: Optional[str] = Field(
        default=None,
        description="Name of the Hive metastore's database (usually: metastore). For backward compatibility, if this field is not provided, the database field will be used. If both the 'database' and 'metastore_db_name' fields are set then the 'database' field will be used to filter the hive/presto/trino database",
    )
    mode: HiveMetastoreConfigMode = Field(
        default=HiveMetastoreConfigMode.hive,
        description=f"The ingested data will be stored under this platform. Valid options: {[e.value for e in HiveMetastoreConfigMode]}",
    )
    use_catalog_subtype: bool = Field(
        default=True,
        description="Container Subtype name to be 'Database' or 'Catalog' Valid options: ['True', 'False']",
    )
    use_dataset_pascalcase_subtype: bool = Field(
        default=False,
        description="Dataset Subtype name to be 'Table' or 'View' Valid options: ['True', 'False']",
    )

    include_view_lineage: bool = Field(
        default=True,
        description="Whether to extract lineage from Hive views. Requires parsing the view definition SQL.",
    )

    include_catalog_name_in_ids: bool = Field(
        default=False,
        description="Add the Presto catalog name (e.g. hive) to the generated dataset urns. `urn:li:dataset:(urn:li:dataPlatform:hive,hive.user.logging_events,PROD)` versus `urn:li:dataset:(urn:li:dataPlatform:hive,user.logging_events,PROD)`",
    )

    enable_properties_merge: bool = Field(
        default=True,
        description="By default, the connector enables merging of properties with what exists on the server. Set this to False to enable the default connector behavior of overwriting properties on each ingestion.",
    )

    simplify_nested_field_paths: bool = Field(
        default=False,
        description="Simplify v2 field paths to v1 by default. If the schema has Union or Array types, still falls back to v2",
    )

    # -------------------------------------------------------------------------
    # Connection type and Thrift-specific settings
    # -------------------------------------------------------------------------

    connection_type: HiveMetastoreConnectionType = Field(
        default=HiveMetastoreConnectionType.sql,
        description="Connection method: 'sql' for direct database access (MySQL/PostgreSQL), "
        "'thrift' for HMS Thrift API with optional Kerberos support.",
    )

    # Thrift-specific settings (only used when connection_type="thrift")
    use_kerberos: bool = Field(
        default=False,
        description="Whether to use Kerberos/SASL authentication. Only used when connection_type='thrift'.",
    )
    kerberos_service_name: str = Field(
        default="hive",
        description="Kerberos service name for the HMS principal. Only used when connection_type='thrift'.",
    )
    kerberos_hostname_override: Optional[str] = Field(
        default=None,
        description="Override the hostname used for Kerberos principal construction. "
        "Use this when connecting through a load balancer where the connection "
        "hostname differs from the Kerberos principal hostname. "
        "Example: If you connect to 'hms-lb.company.com:9083' but the Kerberos principal is "
        "'hive/hms-internal.company.com@REALM', set this to 'hms-internal.company.com'. "
        "Only used when connection_type='thrift'.",
    )
    timeout_seconds: int = Field(
        default=60,
        description="Connection timeout in seconds. Only used when connection_type='thrift'.",
    )
    catalog_name: Optional[str] = Field(
        default=None,
        description="Catalog name for HMS 3.x multi-catalog deployments. Only used when connection_type='thrift'.",
    )

    @model_validator(mode="after")
    def validate_thrift_settings(self) -> "HiveMetastore":
        """Validate settings compatibility with Thrift connection."""
        if self.connection_type == HiveMetastoreConnectionType.thrift:
            # Validate mode - Thrift only supports 'hive' mode
            # presto/trino modes require SQLAlchemy for view queries
            if self.mode != HiveMetastoreConfigMode.hive:
                raise ValueError(
                    f"'mode: {self.mode.value}' is not supported with 'connection_type: thrift'.\n\n"
                    "Thrift mode only supports 'mode: hive' because presto/trino modes require "
                    "direct database queries to extract view definitions.\n\n"
                    "If you need presto/trino view extraction, use 'connection_type: sql' with "
                    "a SQLAlchemy connection to the metastore database instead."
                )

            # Validate WHERE clauses - not supported in Thrift mode
            where_clause_error = (
                "SQL WHERE clause filtering is not supported in Thrift mode.\n\n"
                "Your config uses 'connection_type: thrift', which connects via the "
                "Thrift API instead of direct database queries. WHERE clauses only work "
                "with database queries.\n\n"
                "Please use the standard DataHub pattern-based filtering instead:\n"
                "  - database_pattern: Filter databases by regex (e.g., allow: ['^prod_.*'])\n"
                "  - table_pattern: Filter tables by regex (e.g., deny: ['.*_temp$'])\n"
                "  - view_pattern: Filter views by regex\n\n"
                "Example:\n"
                "  source:\n"
                "    type: hive-metastore\n"
                "    config:\n"
                "      connection_type: thrift\n"
                "      host_port: hms.company.com:9083\n"
                "      database_pattern:\n"
                "        allow:\n"
                "          - '^prod_.*'\n"
                "        deny:\n"
                "          - '^test_.*'"
            )
            if self.schemas_where_clause_suffix:
                raise ValueError(
                    f"'schemas_where_clause_suffix' cannot be used with 'connection_type: thrift'.\n\n{where_clause_error}"
                )
            if self.tables_where_clause_suffix:
                raise ValueError(
                    f"'tables_where_clause_suffix' cannot be used with 'connection_type: thrift'.\n\n{where_clause_error}"
                )
            if self.views_where_clause_suffix:
                raise ValueError(
                    f"'views_where_clause_suffix' cannot be used with 'connection_type: thrift'.\n\n{where_clause_error}"
                )
        return self

    def get_sql_alchemy_url(
        self, uri_opts: Optional[Dict[str, Any]] = None, database: Optional[str] = None
    ) -> str:
        if not ((self.host_port and self.scheme) or self.sqlalchemy_uri):
            raise ValueError("host_port and schema or connect_uri required.")

        return self.sqlalchemy_uri or make_sqlalchemy_uri(
            self.scheme,
            self.username,
            self.password.get_secret_value() if self.password is not None else None,
            self.host_port,
            self.metastore_db_name if self.metastore_db_name else self.database,
            uri_opts=uri_opts,
        )


@platform_name("Hive Metastore")
@config_class(HiveMetastore)
@support_status(SupportStatus.CERTIFIED)
@capability(
    SourceCapability.DELETION_DETECTION, "Enabled by default via stateful ingestion"
)
@capability(SourceCapability.DATA_PROFILING, "Not Supported", False)
@capability(SourceCapability.CLASSIFICATION, "Not Supported", False)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default for views via `include_view_lineage`, and to upstream/downstream storage via `emit_storage_lineage`",
    subtype_modifier=[
        SourceCapabilityModifier.TABLE,
        SourceCapabilityModifier.VIEW,
    ],
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default for views via `include_view_lineage`, and to storage via `include_column_lineage` when storage lineage is enabled",
    subtype_modifier=[
        SourceCapabilityModifier.TABLE,
        SourceCapabilityModifier.VIEW,
    ],
)
@capability(
    SourceCapability.CONTAINERS,
    "Enabled by default",
    subtype_modifier=[
        SourceCapabilityModifier.CATALOG,
        SourceCapabilityModifier.SCHEMA,
    ],
)
class HiveMetastoreSource(SQLAlchemySource):
    """
    This plugin extracts the following:

    - Metadata for Presto views and Hive tables (external / managed)
    - Column types associated with each table / view
    - Detailed table / view property info

    Subclasses can override the data-fetching methods to use different data sources
    (e.g., Thrift API instead of direct database access).
    """

    _TABLES_SQL_STATEMENT = """
    SELECT source.* FROM
    (SELECT t.TBL_ID, d.NAME as schema_name, t.TBL_NAME as table_name, t.TBL_TYPE as table_type,
           FROM_UNIXTIME(t.CREATE_TIME, '%Y-%m-%d') as create_date, p.PKEY_NAME as col_name, p.INTEGER_IDX as col_sort_order,
           p.PKEY_COMMENT as col_description, p.PKEY_TYPE as col_type, 1 as is_partition_col, s.LOCATION as table_location
    FROM TBLS t
    JOIN DBS d ON t.DB_ID = d.DB_ID
    JOIN SDS s ON t.SD_ID = s.SD_ID
    JOIN PARTITION_KEYS p ON t.TBL_ID = p.TBL_ID
    WHERE t.TBL_TYPE IN ('EXTERNAL_TABLE', 'MANAGED_TABLE')
    {where_clause_suffix}
    UNION
    SELECT t.TBL_ID, d.NAME as schema_name, t.TBL_NAME as table_name, t.TBL_TYPE as table_type,
           FROM_UNIXTIME(t.CREATE_TIME, '%Y-%m-%d') as create_date, c.COLUMN_NAME as col_name, c.INTEGER_IDX as col_sort_order,
            c.COMMENT as col_description, c.TYPE_NAME as col_type, 0 as is_partition_col, s.LOCATION as table_location
    FROM TBLS t
    JOIN DBS d ON t.DB_ID = d.DB_ID
    JOIN SDS s ON t.SD_ID = s.SD_ID
    JOIN COLUMNS_V2 c ON s.CD_ID = c.CD_ID
    WHERE t.TBL_TYPE IN ('EXTERNAL_TABLE', 'MANAGED_TABLE')
    {where_clause_suffix}
    ) source
    ORDER by tbl_id desc, col_sort_order asc;
    """

    _TABLES_POSTGRES_SQL_STATEMENT = """
    SELECT source.* FROM
    (SELECT t."TBL_ID" as tbl_id, d."NAME" as schema_name, t."TBL_NAME" as table_name, t."TBL_TYPE" as table_type,
            to_char(to_timestamp(t."CREATE_TIME"), 'YYYY-MM-DD') as create_date, p."PKEY_NAME" as col_name, p."INTEGER_IDX" as col_sort_order,
            p."PKEY_COMMENT" as col_description, p."PKEY_TYPE" as col_type, 1 as is_partition_col, s."LOCATION" as table_location
    FROM "TBLS" t
    JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
    JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
    JOIN "PARTITION_KEYS" p ON t."TBL_ID" = p."TBL_ID"
    WHERE t."TBL_TYPE" IN ('EXTERNAL_TABLE', 'MANAGED_TABLE')
    {where_clause_suffix}
    UNION
    SELECT t."TBL_ID" as tbl_id, d."NAME" as schema_name, t."TBL_NAME" as table_name, t."TBL_TYPE" as table_type,
           to_char(to_timestamp(t."CREATE_TIME"), 'YYYY-MM-DD') as create_date, c."COLUMN_NAME" as col_name,
           c."INTEGER_IDX" as col_sort_order, c."COMMENT" as col_description, c."TYPE_NAME" as col_type, 0 as is_partition_col, s."LOCATION" as table_location
    FROM "TBLS" t
    JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
    JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
    JOIN "COLUMNS_V2" c ON s."CD_ID" = c."CD_ID"
    WHERE t."TBL_TYPE" IN ('EXTERNAL_TABLE', 'MANAGED_TABLE')
    {where_clause_suffix}
    ) source
    ORDER by tbl_id desc, col_sort_order asc;
    """

    _VIEWS_POSTGRES_SQL_STATEMENT = """
    SELECT t."TBL_ID", d."NAME" as "schema", t."TBL_NAME" "name", t."TBL_TYPE", t."VIEW_ORIGINAL_TEXT" as "view_original_text"
    FROM "TBLS" t
    JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
    WHERE t."VIEW_EXPANDED_TEXT" = '/* Presto View */'
    {where_clause_suffix}
    ORDER BY t."TBL_ID" desc;
    """

    _VIEWS_SQL_STATEMENT = """
    SELECT t.TBL_ID, d.NAME as `schema`, t.TBL_NAME name, t.TBL_TYPE, t.VIEW_ORIGINAL_TEXT as view_original_text
    FROM TBLS t
    JOIN DBS d ON t.DB_ID = d.DB_ID
    WHERE t.VIEW_EXPANDED_TEXT = '/* Presto View */'
    {where_clause_suffix}
    ORDER BY t.TBL_ID desc;
    """

    _HIVE_VIEWS_SQL_STATEMENT = """
    SELECT source.* FROM
    (SELECT t.TBL_ID, d.NAME as schema_name, t.TBL_NAME as table_name, t.TBL_TYPE as table_type, t.VIEW_EXPANDED_TEXT as view_expanded_text, tp.PARAM_VALUE as description,
           FROM_UNIXTIME(t.CREATE_TIME, '%Y-%m-%d') as create_date, c.COLUMN_NAME as col_name, c.INTEGER_IDX as col_sort_order,
            c.COMMENT as col_description, c.TYPE_NAME as col_type
    FROM TBLS t
    JOIN DBS d ON t.DB_ID = d.DB_ID
    JOIN SDS s ON t.SD_ID = s.SD_ID
    JOIN COLUMNS_V2 c ON s.CD_ID = c.CD_ID
    LEFT JOIN TABLE_PARAMS tp ON (t.TBL_ID = tp.TBL_ID AND tp.PARAM_KEY='comment')
    WHERE t.TBL_TYPE IN ('VIRTUAL_VIEW')
    {where_clause_suffix}
    ) source
    ORDER by tbl_id desc, col_sort_order asc;
    """

    _HIVE_VIEWS_POSTGRES_SQL_STATEMENT = """
    SELECT source.* FROM
    (SELECT t."TBL_ID" as tbl_id, d."NAME" as schema_name, t."TBL_NAME" as table_name, t."TBL_TYPE" as table_type, t."VIEW_EXPANDED_TEXT" as view_expanded_text, tp."PARAM_VALUE" as description,
           to_char(to_timestamp(t."CREATE_TIME"), 'YYYY-MM-DD') as create_date, c."COLUMN_NAME" as col_name,
           c."INTEGER_IDX" as col_sort_order, c."TYPE_NAME" as col_type
    FROM "TBLS" t
    JOIN "DBS" d ON t."DB_ID" = d."DB_ID"
    JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
    JOIN "COLUMNS_V2" c ON s."CD_ID" = c."CD_ID"
    LEFT JOIN "TABLE_PARAMS" tp ON (t."TBL_ID" = tp."TBL_ID" AND tp."PARAM_KEY"='comment')
    WHERE t."TBL_TYPE" IN ('VIRTUAL_VIEW')
    {where_clause_suffix}
    ) source
    ORDER by tbl_id desc, col_sort_order asc;
    """

    _HIVE_PROPERTIES_SQL_STATEMENT = """
    SELECT d.NAME as schema_name, t.TBL_NAME as table_name, tp.PARAM_KEY, tp.PARAM_VALUE
    FROM TABLE_PARAMS tp
    JOIN TBLS t on t.TBL_ID = tp.TBL_ID
    JOIN DBS d on d.DB_ID = t.DB_ID
    WHERE 1
    {where_clause_suffix}
    ORDER BY tp.TBL_ID desc;
    """

    _HIVE_PROPERTIES_POSTGRES_SQL_STATEMENT = """
    SELECT d."NAME" as schema_name, t."TBL_NAME" as table_name, tp."PARAM_KEY", tp."PARAM_VALUE"
    FROM "TABLE_PARAMS" tp
    JOIN "TBLS" t on t."TBL_ID" = tp."TBL_ID"
    JOIN "DBS" d on d."DB_ID" = t."DB_ID"
    WHERE 1 = 1
    {where_clause_suffix}
    ORDER BY tp."TBL_ID" desc;
    """

    _PRESTO_VIEW_PREFIX = "/* Presto View: "
    _PRESTO_VIEW_SUFFIX = " */"

    _SCHEMAS_SQL_STATEMENT = """
    SELECT d.NAME as `schema`
    FROM DBS d
    WHERE 1
    {where_clause_suffix}
    ORDER BY d.NAME desc;
    """

    _SCHEMAS_POSTGRES_SQL_STATEMENT = """
    SELECT d."NAME" as "schema"
    FROM "DBS" d
    WHERE 1 = 1
    {where_clause_suffix}
    ORDER BY d."NAME" desc;
    """

    def __init__(self, config: HiveMetastore, ctx: PipelineContext) -> None:
        super().__init__(config, ctx, config.mode.value)
        self.config: HiveMetastore = config
        self._alchemy_client = SQLAlchemyClient(config)
        self.database_container_subtype = (
            DatasetContainerSubTypes.CATALOG
            if config.use_catalog_subtype
            else DatasetContainerSubTypes.DATABASE
        )
        self.view_subtype = (
            DatasetSubTypes.VIEW.title()
            if config.use_dataset_pascalcase_subtype
            else DatasetSubTypes.VIEW.lower()
        )
        self.table_subtype = (
            DatasetSubTypes.TABLE.title()
            if config.use_dataset_pascalcase_subtype
            else DatasetSubTypes.TABLE.lower()
        )
        self.storage_lineage = HiveStorageLineage(
            config=config,
            env=config.env,
        )

    def get_db_name(self, inspector: Inspector) -> str:
        if self.config.database:
            return f"{self.config.database}"
        else:
            return super().get_db_name(inspector)

    def get_db_schema(self, dataset_identifier: str) -> Tuple[Optional[str], str]:
        if dataset_identifier is None:
            raise InvalidDatasetIdentifierError("dataset_identifier cannot be None")
        elif not dataset_identifier.strip():
            raise InvalidDatasetIdentifierError("dataset_identifier cannot be empty")

        parts = dataset_identifier.split(".")

        # Filter out empty parts (e.g., from ".." in identifier)
        parts = [p for p in parts if p]
        if not parts:
            raise InvalidDatasetIdentifierError(
                f"Invalid dataset identifier: {dataset_identifier}"
            )

        if self.config.include_catalog_name_in_ids:
            if len(parts) >= 3:
                return parts[0], parts[1]
            elif len(parts) == 2:
                return None, parts[0]
            else:
                return None, parts[0]
        else:
            if len(parts) >= 2:
                return None, parts[0]
            else:
                return None, parts[0]

    @classmethod
    def create(cls, config_dict, ctx):
        config = HiveMetastore.model_validate(config_dict)

        # Route to Thrift source if connection_type is thrift
        if config.connection_type == HiveMetastoreConnectionType.thrift:
            from datahub.ingestion.source.sql.hive.hive_metastore_thrift_source import (
                HiveMetastoreThriftSource,
            )

            return HiveMetastoreThriftSource.create(config_dict, ctx)

        return cls(config, ctx)

    # =========================================================================
    # EXTENSION POINTS: Overridable Data-Fetching Methods
    # =========================================================================
    #
    # These methods define the contract between HiveMetastoreSource and its
    # subclasses (e.g., HiveMetastoreThriftSource). Subclasses override these
    # to fetch data from different sources while reusing all WorkUnit logic.
    #
    # STABILITY CONTRACT:
    # - Return types must conform to TableRow, ViewRow, SchemaRow, TablePropertiesRow
    # - Method signatures should not change without updating subclasses
    # - Changes here may break HiveMetastoreThriftSource - test both paths!
    #
    # See: hive_metastore_thrift_source.py for the Thrift implementation
    # =========================================================================

    def _get_tables_sql_statement(self, where_clause_suffix: str) -> str:
        """Get the SQL statement for fetching tables. Override in subclasses if needed."""
        return (
            HiveMetastoreSource._TABLES_POSTGRES_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
            if "postgresql" in self.config.scheme
            else HiveMetastoreSource._TABLES_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
        )

    def _fetch_table_rows(self, where_clause_suffix: str) -> Iterable[Dict[str, Any]]:
        """
        Fetch table/column rows from the data source.

        This is an EXTENSION POINT - subclasses (e.g., Thrift connector) override
        this to use different data sources while reusing WorkUnit generation.

        Args:
            where_clause_suffix: SQL WHERE clause suffix (ignored by Thrift connector)

        Returns:
            Iterable of dicts matching TableRow format. See TableRow TypedDict for field definitions.

        Note:
            Each row represents one column. Tables with N columns produce N rows.
            Partition columns have is_partition_col=1 and appear after regular columns.
        """
        statement = self._get_tables_sql_statement(where_clause_suffix)
        return self._alchemy_client.execute_query(statement)

    def _fetch_hive_view_rows(
        self, where_clause_suffix: str
    ) -> Iterable[Dict[str, Any]]:
        """
        Fetch Hive view rows from the data source.

        This is an EXTENSION POINT - subclasses override to use different data sources.

        Args:
            where_clause_suffix: SQL WHERE clause suffix (ignored by Thrift connector)

        Returns:
            Iterable of dicts matching ViewRow format. See ViewRow TypedDict for field definitions.

        Note:
            Each row represents one column. Views with N columns produce N rows.
        """
        statement = (
            HiveMetastoreSource._HIVE_VIEWS_POSTGRES_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
            if "postgresql" in self.config.scheme
            else HiveMetastoreSource._HIVE_VIEWS_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
        )
        return self._alchemy_client.execute_query(statement)

    def _fetch_schema_rows(self, where_clause_suffix: str) -> Iterable[Dict[str, Any]]:
        """
        Fetch schema/database rows from the data source.

        This is an EXTENSION POINT - subclasses override to use different data sources.

        Args:
            where_clause_suffix: SQL WHERE clause suffix (ignored by Thrift connector)

        Returns:
            Iterable of SchemaRow dicts. See SchemaRow TypedDict for field definitions.
        """
        statement = (
            HiveMetastoreSource._SCHEMAS_POSTGRES_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
            if "postgresql" in self.config.scheme
            else HiveMetastoreSource._SCHEMAS_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
        )
        return self._alchemy_client.execute_query(statement)

    def _fetch_table_properties_rows(
        self, where_clause_suffix: str
    ) -> Iterable[Dict[str, Any]]:
        """
        Fetch table properties rows from the data source.

        This is an EXTENSION POINT - subclasses override to use different data sources.

        Args:
            where_clause_suffix: SQL WHERE clause suffix (ignored by Thrift connector)

        Returns:
            Iterable of dicts matching TablePropertiesRow format. See TablePropertiesRow TypedDict.

        Note:
            Each row represents one property. Tables with N properties produce N rows.
        """
        statement = (
            HiveMetastoreSource._HIVE_PROPERTIES_POSTGRES_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
            if "postgresql" in self.config.scheme
            else HiveMetastoreSource._HIVE_PROPERTIES_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
        )
        return self._alchemy_client.execute_query(statement)

    def gen_database_containers(
        self,
        database: str,
        extra_properties: Optional[Dict[str, Any]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        database_container_key = gen_database_key(
            database,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        yield from gen_database_container(
            database=database,
            database_container_key=database_container_key,
            sub_types=[self.database_container_subtype],
            domain_registry=self.domain_registry,
            domain_config=self.config.domain,
            extra_properties=extra_properties,
        )

    def gen_schema_containers(
        self,
        schema: str,
        database: str,
        extra_properties: Optional[Dict[str, Any]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        if not isinstance(self.config, HiveMetastore):
            raise TypeError(f"Expected HiveMetastore, got {type(self.config).__name__}")
        where_clause_suffix: str = ""
        if (
            self.config.schemas_where_clause_suffix
            or self._get_db_filter_where_clause()
        ):
            where_clause_suffix = f"{self.config.schemas_where_clause_suffix} {self._get_db_filter_where_clause()}"

        iter_res = self._fetch_schema_rows(where_clause_suffix)
        for row in iter_res:
            schema = row["schema"]
            if not self.config.database_pattern.allowed(schema):
                continue
            database_container_key = gen_database_key(
                database,
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            schema_container_key = gen_schema_key(
                db_name=database,
                schema=schema,
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            yield from gen_schema_container(
                database=database,
                schema=schema,
                sub_types=[DatasetContainerSubTypes.SCHEMA],
                database_container_key=database_container_key,
                schema_container_key=schema_container_key,
                domain_registry=self.domain_registry,
                domain_config=self.config.domain,
                extra_properties=extra_properties,
            )

    def get_default_ingestion_job_id(self) -> JobId:
        """
        Default ingestion job name that sql_common provides.
        Subclasses can override as needed.
        """
        return JobId(self.config.ingestion_job_id)

    def _get_table_properties(
        self, db_name: str, scheme: str, where_clause_suffix: str
    ) -> Dict[str, Dict[str, str]]:
        iter_res = self._fetch_table_properties_rows(where_clause_suffix)
        table_properties: Dict[str, Dict[str, str]] = {}
        for row in iter_res:
            dataset_name = f"{row['schema_name']}.{row['table_name']}"
            if self.config.include_catalog_name_in_ids:
                dataset_name = f"{db_name}.{dataset_name}"
            if row["PARAM_KEY"] and row["PARAM_VALUE"]:
                table_properties.setdefault(dataset_name, {})[row["PARAM_KEY"]] = row[
                    "PARAM_VALUE"
                ]

        return table_properties

    def loop_tables(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        if (
            "mysql" in self.config.scheme
            and self.config.metastore_db_name
            and self.config.metastore_db_name != schema
        ):
            return

        if not isinstance(sql_config, HiveMetastore):
            raise TypeError(f"Expected HiveMetastore, got {type(sql_config).__name__}")
        where_clause_suffix = f"{sql_config.tables_where_clause_suffix} {self._get_db_filter_where_clause()}"

        db_name = self.get_db_name(inspector)

        properties_cache = self._get_table_properties(
            db_name=db_name,
            scheme=sql_config.scheme,
            where_clause_suffix=where_clause_suffix,
        )

        iter_res = self._fetch_table_rows(where_clause_suffix)

        for key, group in groupby_unsorted(iter_res, self._get_table_key):
            schema_name = (
                f"{db_name}.{key.schema}"
                if self.config.include_catalog_name_in_ids
                else key.schema
            )

            dataset_name = self.get_identifier(
                schema=schema_name, entity=key.table, inspector=inspector
            )

            self.report.report_entity_scanned(dataset_name, ent_type="table")

            if not self.config.database_pattern.allowed(key.schema):
                self.report.report_dropped(f"{dataset_name}")
                continue

            if not sql_config.table_pattern.allowed(dataset_name):
                self.report.report_dropped(dataset_name)
                continue

            columns = list(group)
            if len(columns) == 0:
                self.report.report_warning(dataset_name, "missing column information")

            dataset_urn: str = make_dataset_urn_with_platform_instance(
                self.platform,
                dataset_name,
                self.config.platform_instance,
                self.config.env,
            )
            dataset_snapshot = DatasetSnapshot(
                urn=dataset_urn,
                aspects=[StatusClass(removed=False)],
            )

            schema_fields = self.get_schema_fields(dataset_name, columns, inspector)
            self._set_partition_key(columns, schema_fields)

            schema_metadata = get_schema_metadata(
                self.report,
                dataset_name,
                self.platform,
                columns,
                None,
                None,
                schema_fields,
                self.config.simplify_nested_field_paths,
            )
            dataset_snapshot.aspects.append(schema_metadata)

            properties: Dict[str, str] = properties_cache.get(dataset_name, {})
            properties["table_type"] = str(columns[-1]["table_type"] or "")
            properties["table_location"] = str(columns[-1]["table_location"] or "")
            properties["create_date"] = str(columns[-1]["create_date"] or "")

            par_columns: str = ", ".join(
                [c["col_name"] for c in columns if c["is_partition_col"]]
            )
            if par_columns != "":
                properties["partitioned_columns"] = par_columns

            table_description = properties.get("comment")
            yield from self.add_hive_dataset_to_container(
                dataset_urn=dataset_urn, inspector=inspector, schema=key.schema
            )

            if self.config.enable_properties_merge:
                from datahub.specific.dataset import DatasetPatchBuilder

                patch_builder: DatasetPatchBuilder = DatasetPatchBuilder(
                    urn=dataset_snapshot.urn
                )
                patch_builder.set_display_name(key.table)

                if table_description:
                    patch_builder.set_description(description=table_description)

                for prop, value in properties.items():
                    patch_builder.add_custom_property(key=prop, value=value)
                yield from [
                    MetadataWorkUnit(
                        id=f"{mcp_raw.entityUrn}-{DatasetPropertiesClass.ASPECT_NAME}",
                        mcp_raw=mcp_raw,
                    )
                    for mcp_raw in patch_builder.build()
                ]
            else:
                dataset_properties = DatasetPropertiesClass(
                    name=key.table,
                    description=table_description,
                    customProperties=properties,
                )
                dataset_snapshot.aspects.append(dataset_properties)

            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            yield SqlWorkUnit(id=dataset_name, mce=mce)

            dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
            if dpi_aspect:
                yield dpi_aspect

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypesClass(typeNames=[self.table_subtype]),
            ).as_workunit()

            if self.config.domain:
                assert self.domain_registry
                yield from get_domain_wu(
                    dataset_name=dataset_name,
                    entity_urn=dataset_urn,
                    domain_config=self.config.domain,
                    domain_registry=self.domain_registry,
                )

            if self.config.emit_storage_lineage and properties.get("table_location"):
                table_dict = {
                    "StorageDescriptor": {"Location": properties["table_location"]}
                }
                yield from self.storage_lineage.get_lineage_mcp(
                    dataset_urn=dataset_urn,
                    table=table_dict,
                    dataset_schema=schema_metadata,
                )

    def add_hive_dataset_to_container(
        self, dataset_urn: str, inspector: Inspector, schema: str
    ) -> Iterable[MetadataWorkUnit]:
        db_name = self.get_db_name(inspector)
        schema_container_key = gen_schema_key(
            db_name=db_name,
            schema=schema,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )
        yield from add_table_to_schema_container(
            dataset_urn=dataset_urn,
            parent_container_key=schema_container_key,
        )

    def get_hive_view_columns(self, inspector: Inspector) -> Iterable[ViewDataset]:
        where_clause_suffix = ""
        if self.config.views_where_clause_suffix or self._get_db_filter_where_clause():
            where_clause_suffix = f"{self.config.views_where_clause_suffix} {self._get_db_filter_where_clause()}"

        iter_res = self._fetch_hive_view_rows(where_clause_suffix)
        for key, group in groupby_unsorted(iter_res, self._get_table_key):
            db_name = self.get_db_name(inspector)

            schema_name = (
                f"{db_name}.{key.schema}"
                if self.config.include_catalog_name_in_ids
                else key.schema
            )

            dataset_name = self.get_identifier(
                schema=schema_name, entity=key.table, inspector=inspector
            )

            if not self.config.database_pattern.allowed(key.schema):
                self.report.report_dropped(f"{dataset_name}")
                continue

            columns = list(group)

            if len(columns) == 0:
                self.report.report_warning(dataset_name, "missing column information")

            yield ViewDataset(
                dataset_name=dataset_name,
                schema_name=key.schema,
                columns=columns,
                view_definition=columns[-1]["view_expanded_text"],
            )

    def get_presto_view_columns(self, inspector: Inspector) -> Iterable[ViewDataset]:
        where_clause_suffix = ""
        if self.config.views_where_clause_suffix or self._get_db_filter_where_clause():
            where_clause_suffix = f"{self.config.views_where_clause_suffix} {self._get_db_filter_where_clause()}"

        statement: str = (
            HiveMetastoreSource._VIEWS_POSTGRES_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
            if "postgresql" in self.config.scheme
            else HiveMetastoreSource._VIEWS_SQL_STATEMENT.format(
                where_clause_suffix=where_clause_suffix
            )
        )

        iter_res = self._alchemy_client.execute_query(statement)
        for row in iter_res:
            db_name = self.get_db_name(inspector)
            schema_name = (
                f"{db_name}.{row['schema']}"
                if self.config.include_catalog_name_in_ids
                else row["schema"]
            )
            dataset_name = self.get_identifier(
                schema=schema_name,
                entity=row["name"],
                inspector=inspector,
            )

            columns, view_definition = self._get_presto_view_column_metadata(
                row["view_original_text"]
            )

            if len(columns) == 0:
                self.report.report_warning(dataset_name, "missing column information")

            yield ViewDataset(
                dataset_name=dataset_name,
                schema_name=row["schema"],
                columns=columns,
                view_definition=view_definition,
            )

    def loop_views(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        if not isinstance(sql_config, HiveMetastore):
            raise TypeError(f"Expected HiveMetastore, got {type(sql_config).__name__}")

        if (
            "mysql" in self.config.scheme
            and self.config.metastore_db_name
            and self.config.metastore_db_name != schema
        ):
            return

        iter: Iterable[ViewDataset]
        if self.config.mode in [HiveMetastoreConfigMode.hive]:
            iter = self.get_hive_view_columns(inspector=inspector)
        else:
            iter = self.get_presto_view_columns(inspector=inspector)
        for dataset in iter:
            self.report.report_entity_scanned(dataset.dataset_name, ent_type="view")

            if not sql_config.view_pattern.allowed(dataset.dataset_name):
                self.report.report_dropped(dataset.dataset_name)
                continue

            dataset_urn = make_dataset_urn_with_platform_instance(
                self.platform,
                dataset.dataset_name,
                self.config.platform_instance,
                self.config.env,
            )
            dataset_snapshot = DatasetSnapshot(
                urn=dataset_urn,
                aspects=[StatusClass(removed=False)],
            )

            schema_fields = self.get_schema_fields(
                dataset.dataset_name,
                dataset.columns,
                inspector,
            )

            schema_metadata = get_schema_metadata(
                self.report,
                dataset.dataset_name,
                self.platform,
                dataset.columns,
                canonical_schema=schema_fields,
                simplify_nested_field_paths=self.config.simplify_nested_field_paths,
            )
            dataset_snapshot.aspects.append(schema_metadata)

            properties: Dict[str, str] = {"is_view": "True"}
            dataset_properties = DatasetPropertiesClass(
                name=dataset.dataset_name.split(".")[-1],
                description=None,
                customProperties=properties,
            )
            dataset_snapshot.aspects.append(dataset_properties)

            view_properties = ViewPropertiesClass(
                materialized=False,
                viewLogic=dataset.view_definition if dataset.view_definition else "",
                viewLanguage="SQL",
            )
            dataset_snapshot.aspects.append(view_properties)

            yield from self.add_hive_dataset_to_container(
                dataset_urn=dataset_urn, inspector=inspector, schema=dataset.schema_name
            )

            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            yield SqlWorkUnit(id=dataset.dataset_name, mce=mce)

            dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
            if dpi_aspect:
                yield dpi_aspect

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypesClass(typeNames=[self.view_subtype]),
            ).as_workunit()

            view_properties_aspect = ViewPropertiesClass(
                materialized=False,
                viewLanguage="SQL",
                viewLogic=dataset.view_definition if dataset.view_definition else "",
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=view_properties_aspect,
            ).as_workunit()

            if self.config.domain:
                assert self.domain_registry
                yield from get_domain_wu(
                    dataset_name=dataset.dataset_name,
                    entity_urn=dataset_urn,
                    domain_registry=self.domain_registry,
                    domain_config=self.config.domain,
                )

            if dataset.view_definition and self.config.include_view_lineage:
                default_db = None
                default_schema = None
                try:
                    default_db, default_schema = self.get_db_schema(
                        dataset.dataset_name
                    )
                except InvalidDatasetIdentifierError as e:
                    logger.warning(
                        f"Invalid view identifier '{dataset.dataset_name}': {e}"
                    )
                    continue

                self.aggregator.add_view_definition(
                    view_urn=dataset_urn,
                    view_definition=dataset.view_definition,
                    default_db=default_db,
                    default_schema=default_schema,
                )

    def _get_db_filter_where_clause(self) -> str:
        if self.config.metastore_db_name is None:
            return ""
        if self.config.database:
            if "postgresql" in self.config.scheme:
                return f"AND d.\"NAME\" = '{self.config.database}'"
            else:
                return f"AND d.NAME = '{self.config.database}'"

        return ""

    def _get_table_key(self, row: Dict[str, Any]) -> TableKey:
        return TableKey(schema=row["schema_name"], table=row["table_name"])

    def _get_presto_view_column_metadata(
        self, view_original_text: str
    ) -> Tuple[List[Dict], str]:
        """Extract column metadata from base64-encoded Presto view definition."""
        encoded_view_info = view_original_text.split(
            HiveMetastoreSource._PRESTO_VIEW_PREFIX, 1
        )[-1].rsplit(HiveMetastoreSource._PRESTO_VIEW_SUFFIX, 1)[0]

        decoded_view_info = base64.b64decode(encoded_view_info)
        view_definition = json.loads(decoded_view_info).get("originalSql")

        columns = json.loads(decoded_view_info).get("columns")
        for col in columns:
            col["col_name"], col["col_type"] = col["name"], col["type"]

        return list(columns), view_definition

    def close(self) -> None:
        if self._alchemy_client.connection is not None:
            self._alchemy_client.connection.close()
        super().close()

    def get_schema_fields_for_column(
        self,
        dataset_name: str,
        column: Dict[Any, Any],
        inspector: Inspector,
        pk_constraints: Optional[Dict[Any, Any]] = None,
        partition_keys: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
    ) -> List[SchemaField]:
        return get_schema_fields_for_hive_column(
            column["col_name"],
            column["col_type"],
            description=(
                column["col_description"] if "col_description" in column else ""  # noqa: SIM401
            ),
            default_nullable=True,
        )

    def _set_partition_key(self, columns, schema_fields):
        if len(columns) > 0:
            partition_key_names = set()
            for column in columns:
                if column["is_partition_col"]:
                    partition_key_names.add(column["col_name"])

            for schema_field in schema_fields:
                name = schema_field.fieldPath.split(".")[-1]
                if name in partition_key_names:
                    schema_field.isPartitioningKey = True


class SQLAlchemyClient:
    def __init__(self, config: SQLCommonConfig):
        self.config = config
        self.connection = self._get_connection()

    def _get_connection(self) -> Any:
        url = self.config.get_sql_alchemy_url()
        engine = create_engine(url, **self.config.options)
        conn = engine.connect()
        return conn

    def execute_query(self, query: str) -> Iterable:
        """
        Create an iterator to execute sql.
        """
        results = self.connection.execute(text(query))
        return iter(results)
