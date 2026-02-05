"""
Configuration classes for Hive Metastore source.

This module contains all configuration-related classes for the Hive Metastore
connector, including:
- HiveMetastoreConfigMode: Enum for metadata extraction modes
- HiveMetastoreConnectionType: Enum for connection types (SQL/Thrift)
- HiveMetastore: Main configuration class
- Row type definitions (TypedDicts) for data fetcher interfaces
"""

from typing import Any, Dict, Optional, TypedDict

from pydantic import Field, model_validator

from datahub.configuration.common import AllowDenyPattern, HiddenFromDocs
from datahub.ingestion.source.sql.hive.storage_lineage import (
    HiveStorageLineageConfigMixin,
)
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig
from datahub.ingestion.source.sql.sqlalchemy_uri import make_sqlalchemy_uri
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.utilities.str_enum import StrEnum

# =============================================================================
# Row Type Definitions
#
# These TypedDicts define the expected row format for data fetching methods.
# Both SQL and Thrift fetchers must return rows matching these formats.
# =============================================================================


class TableRow(TypedDict):
    """Row format for table/column data. One row per column."""

    schema_name: str
    table_name: str
    table_type: str
    create_date: str
    col_name: str
    col_sort_order: int
    col_description: str
    col_type: str
    is_partition_col: int
    table_location: str


class ViewRow(TypedDict):
    """Row format for Hive view data. One row per column."""

    schema_name: str
    table_name: str
    table_type: str
    view_expanded_text: str
    description: str
    create_date: str
    col_name: str
    col_sort_order: int
    col_description: str
    col_type: str


class SchemaRow(TypedDict):
    """Row format for schema/database data."""

    schema: str


class TablePropertiesRow(TypedDict):
    """Row format for table properties. One row per property."""

    schema_name: str
    table_name: str
    PARAM_KEY: str
    PARAM_VALUE: str


# =============================================================================
# Configuration Enums
# =============================================================================


class HiveMetastoreConfigMode(StrEnum):
    """Mode for metadata extraction."""

    hive = "hive"
    presto = "presto"
    presto_on_hive = "presto-on-hive"
    trino = "trino"


class HiveMetastoreConnectionType(StrEnum):
    """Connection type for HiveMetastoreSource."""

    sql = "sql"
    thrift = "thrift"


# =============================================================================
# Main Configuration Class
# =============================================================================


class HiveMetastore(
    BasicSQLAlchemyConfig,
    HiveStorageLineageConfigMixin,
    StatefulIngestionConfigBase,
):
    """
    Configuration for Hive Metastore source.

    Supports two connection types:
    - sql: Direct database access (MySQL/PostgreSQL) to HMS backend
    - thrift: HMS Thrift API with Kerberos support
    """

    # -------------------------------------------------------------------------
    # Connection settings
    # -------------------------------------------------------------------------

    connection_type: HiveMetastoreConnectionType = Field(
        default=HiveMetastoreConnectionType.sql,
        description="Connection method: 'sql' for direct database access (MySQL/PostgreSQL), "
        "'thrift' for HMS Thrift API with optional Kerberos support.",
    )

    host_port: str = Field(
        default="localhost:3306",
        description="Host and port. For SQL: database port (3306/5432). "
        "For Thrift: HMS Thrift port (9083).",
    )

    scheme: HiddenFromDocs[str] = Field(default="mysql+pymysql")

    # -------------------------------------------------------------------------
    # SQL-specific settings (only used when connection_type="sql")
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # DEPRECATED SQL-specific settings - will be removed in a future release
    # -------------------------------------------------------------------------

    views_where_clause_suffix: str = Field(
        default="",
        description="DEPRECATED: This option has been deprecated for security reasons "
        "and will be removed in a future release. Use 'database_pattern' instead.",
    )
    tables_where_clause_suffix: str = Field(
        default="",
        description="DEPRECATED: This option has been deprecated for security reasons "
        "and will be removed in a future release. Use 'database_pattern' instead.",
    )
    schemas_where_clause_suffix: str = Field(
        default="",
        description="DEPRECATED: This option has been deprecated for security reasons "
        "and will be removed in a future release. Use 'database_pattern' instead.",
    )

    metastore_db_name: Optional[str] = Field(
        default=None,
        description="Name of the Hive metastore's database (usually: metastore). "
        "For backward compatibility, if not provided, the database field will be used. "
        "If both 'database' and 'metastore_db_name' are set, 'database' is used for filtering.",
    )

    # -------------------------------------------------------------------------
    # Thrift-specific settings (only used when connection_type="thrift")
    # -------------------------------------------------------------------------

    use_kerberos: bool = Field(
        default=False,
        description="Whether to use Kerberos/SASL authentication. Only for connection_type='thrift'.",
    )
    kerberos_service_name: str = Field(
        default="hive",
        description="Kerberos service name for the HMS principal. Only for connection_type='thrift'.",
    )
    kerberos_hostname_override: Optional[str] = Field(
        default=None,
        description="Override hostname for Kerberos principal construction. "
        "Use when connecting through a load balancer. Only for connection_type='thrift'.",
    )
    timeout_seconds: int = Field(
        default=60,
        description="Connection timeout in seconds. Only for connection_type='thrift'.",
    )
    catalog_name: Optional[str] = Field(
        default=None,
        description="Catalog name for HMS 3.x multi-catalog deployments. Only for connection_type='thrift'.",
    )

    # -------------------------------------------------------------------------
    # Shared settings
    # -------------------------------------------------------------------------

    database_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for databases to filter.",
    )

    mode: HiveMetastoreConfigMode = Field(
        default=HiveMetastoreConfigMode.hive,
        description=f"Platform mode for metadata. Valid options: {[e.value for e in HiveMetastoreConfigMode]}",
    )

    use_catalog_subtype: bool = Field(
        default=True,
        description="Use 'Catalog' (True) or 'Database' (False) as container subtype.",
    )

    use_dataset_pascalcase_subtype: bool = Field(
        default=False,
        description="Use 'Table'/'View' (True) or 'table'/'view' (False) as dataset subtype.",
    )

    include_view_lineage: bool = Field(
        default=True,
        description="Extract lineage from Hive views by parsing view definitions.",
    )

    include_catalog_name_in_ids: bool = Field(
        default=False,
        description="Add catalog name to dataset URNs. "
        "Example: urn:li:dataset:(urn:li:dataPlatform:hive,catalog.db.table,PROD)",
    )

    enable_properties_merge: bool = Field(
        default=True,
        description="Merge properties with existing server data instead of overwriting.",
    )

    simplify_nested_field_paths: bool = Field(
        default=False,
        description="Simplify v2 field paths to v1. Falls back to v2 for Union/Array types.",
    )

    ingestion_job_id: str = ""

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Configuration for stateful ingestion and stale entity removal.",
    )

    @model_validator(mode="after")
    def validate_deprecated_where_clause_options(self) -> "HiveMetastore":
        """
        Check for deprecated WHERE clause suffix options and fail with error.

        These options have been deprecated for security reasons (SQL injection risk)
        and will be removed in a future release.
        """
        deprecated_fields = []
        if self.tables_where_clause_suffix:
            deprecated_fields.append("tables_where_clause_suffix")
        if self.views_where_clause_suffix:
            deprecated_fields.append("views_where_clause_suffix")
        if self.schemas_where_clause_suffix:
            deprecated_fields.append("schemas_where_clause_suffix")

        if deprecated_fields:
            raise ValueError(
                f"DEPRECATED: {', '.join(deprecated_fields)} - removed for security reasons. "
                f"Use 'database_pattern' instead."
            )
        return self

    @model_validator(mode="after")
    def validate_thrift_settings(self) -> "HiveMetastore":
        """Validate settings compatibility with Thrift connection."""
        if self.connection_type == HiveMetastoreConnectionType.thrift:
            # Validate mode - Thrift only supports 'hive' mode
            if self.mode != HiveMetastoreConfigMode.hive:
                raise ValueError(
                    f"'mode: {self.mode.value}' is not supported with 'connection_type: thrift'.\n\n"
                    "Thrift mode only supports 'mode: hive' because presto/trino modes require "
                    "direct database queries to extract view definitions.\n\n"
                    "Use 'connection_type: sql' for presto/trino view extraction."
                )
        return self

    def get_sql_alchemy_url(
        self, uri_opts: Optional[Dict[str, Any]] = None, database: Optional[str] = None
    ) -> str:
        """Generate SQLAlchemy connection URL."""
        if not ((self.host_port and self.scheme) or self.sqlalchemy_uri):
            raise ValueError("host_port and scheme or sqlalchemy_uri required.")

        return self.sqlalchemy_uri or make_sqlalchemy_uri(
            self.scheme,
            self.username,
            self.password.get_secret_value() if self.password is not None else None,
            self.host_port,
            self.metastore_db_name if self.metastore_db_name else self.database,
            uri_opts=uri_opts,
        )
