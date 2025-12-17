"""
Hive Metastore Source - Unified connector for SQL and Thrift connections.

This module provides the HiveMetastoreSource connector that extracts metadata
from Hive Metastore via either:
- Direct database access (MySQL/PostgreSQL) - connection_type: sql
- Thrift API with Kerberos support - connection_type: thrift

Architecture:
- Uses composition instead of inheritance for clean separation
- HiveDataFetcher Protocol abstracts data access (SQL vs Thrift)
- HiveMetadataProcessor handles all WorkUnit generation
"""

import dataclasses
import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple, TypedDict

if TYPE_CHECKING:
    from datahub.ingestion.source.sql.hive.hive_data_fetcher import HiveDataFetcher
    from datahub.utilities.registries.domain_registry import DomainRegistry

from pydantic import Field, model_validator

from datahub.configuration.common import AllowDenyPattern, HiddenFromDocs
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import SourceCapabilityModifier
from datahub.ingestion.source.sql.hive.exceptions import InvalidDatasetIdentifierError
from datahub.ingestion.source.sql.hive.storage_lineage import (
    HiveStorageLineageConfigMixin,
)
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig
from datahub.ingestion.source.sql.sqlalchemy_uri import make_sqlalchemy_uri
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.utilities.str_enum import StrEnum

logger = logging.getLogger(__name__)


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
# Configuration
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

    views_where_clause_suffix: str = Field(
        default="",
        description="WHERE clause suffix for Presto views query. Only for connection_type='sql'.",
    )
    tables_where_clause_suffix: str = Field(
        default="",
        description="WHERE clause suffix for tables query. Only for connection_type='sql'.",
    )
    schemas_where_clause_suffix: str = Field(
        default="",
        description="WHERE clause suffix for schemas query. Only for connection_type='sql'.",
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

            # Validate WHERE clauses - not supported in Thrift mode
            where_clause_error = (
                "SQL WHERE clause filtering is not supported in Thrift mode.\n\n"
                "Use pattern-based filtering instead:\n"
                "  - database_pattern: Filter databases by regex\n"
                "  - table_pattern: Filter tables by regex\n"
                "  - view_pattern: Filter views by regex"
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


# =============================================================================
# Source Report
# =============================================================================


@dataclasses.dataclass
class HiveMetastoreSourceReport(StaleEntityRemovalSourceReport):
    """Report for HiveMetastoreSource."""

    tables_scanned: int = 0
    views_scanned: int = 0
    filtered: List[str] = dataclasses.field(default_factory=list)

    def report_entity_scanned(self, name: str, ent_type: str = "table") -> None:
        """Report that an entity was scanned."""
        if ent_type == "table":
            self.tables_scanned += 1
        elif ent_type == "view":
            self.views_scanned += 1

    def report_dropped(self, name: str) -> None:
        """Report that an entity was filtered/dropped."""
        self.filtered.append(name)


# =============================================================================
# Source
# =============================================================================


@platform_name("Hive Metastore")
@config_class(HiveMetastore)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
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
class HiveMetastoreSource(StatefulIngestionSourceBase, TestableSource):
    """
    Extracts metadata from Hive Metastore.

    Supports two connection methods selected via `connection_type`:
    - sql: Direct connection to HMS backend database (MySQL/PostgreSQL)
    - thrift: Connection to HMS Thrift API with Kerberos support

    Features:
    - Table and view metadata extraction
    - Schema field types including complex types (struct, map, array)
    - Storage lineage to S3, HDFS, Azure, GCS
    - View lineage via SQL parsing
    - Stateful ingestion for stale entity removal
    """

    def __init__(self, config: HiveMetastore, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.config: HiveMetastore = config
        self.report: HiveMetastoreSourceReport = HiveMetastoreSourceReport()
        self.platform = config.mode.value

        # Initialize subtypes based on config (exposed for backward compatibility)
        from datahub.ingestion.source.common.subtypes import DatasetSubTypes

        self.table_subtype = (
            DatasetSubTypes.TABLE.title()
            if config.use_dataset_pascalcase_subtype
            else DatasetSubTypes.TABLE
        )
        self.view_subtype = (
            DatasetSubTypes.VIEW.title()
            if config.use_dataset_pascalcase_subtype
            else DatasetSubTypes.VIEW
        )

        # Initialize domain registry
        self.domain_registry: Optional["DomainRegistry"] = None
        if config.domain:
            from datahub.utilities.registries.domain_registry import DomainRegistry

            self.domain_registry = DomainRegistry(
                cached_domains=[k for k in config.domain], graph=ctx.graph
            )

        # Create data fetcher based on connection type
        self._fetcher: "HiveDataFetcher"
        if config.connection_type == HiveMetastoreConnectionType.sql:
            from datahub.ingestion.source.sql.hive.hive_data_fetcher import (
                SQLAlchemyDataFetcher,
            )

            self._fetcher = SQLAlchemyDataFetcher(config)
        else:
            from datahub.ingestion.source.sql.hive.hive_thrift_fetcher import (
                ThriftDataFetcher,
            )

            self._fetcher = ThriftDataFetcher(config)

        # Initialize SQL parsing aggregator for view lineage
        self._aggregator = None
        if config.include_view_lineage:
            from datahub.sql_parsing.sql_parsing_aggregator import (
                SqlParsingAggregator,
            )

            self._aggregator = SqlParsingAggregator(
                platform=self.platform,
                platform_instance=config.platform_instance,
                env=config.env,
                graph=self.ctx.graph,
            )

        # Initialize stale entity removal
        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, config, ctx
        )

    @classmethod
    def create(
        cls, config_dict: Dict[str, Any], ctx: PipelineContext
    ) -> "HiveMetastoreSource":
        """Create source instance from config dict."""
        config = HiveMetastore.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        """Test connectivity to Hive Metastore."""
        test_report = TestConnectionReport()

        try:
            config = HiveMetastore.model_validate(config_dict)

            if config.connection_type == HiveMetastoreConnectionType.thrift:
                # Test Thrift connection
                from datahub.ingestion.source.sql.hive.hive_thrift_client import (
                    HiveMetastoreThriftClient,
                    ThriftConnectionConfig,
                )

                host_port_parts = config.host_port.split(":")
                host = host_port_parts[0]
                port = int(host_port_parts[1]) if len(host_port_parts) > 1 else 9083

                thrift_config = ThriftConnectionConfig(
                    host=host,
                    port=port,
                    use_kerberos=config.use_kerberos,
                    kerberos_service_name=config.kerberos_service_name,
                    kerberos_hostname_override=config.kerberos_hostname_override,
                    timeout_seconds=config.timeout_seconds,
                    max_retries=1,
                )

                client = HiveMetastoreThriftClient(thrift_config)
                client.connect()
                try:
                    databases = client.get_all_databases()
                    test_report.basic_connectivity = CapabilityReport(
                        capable=True,
                        failure_reason=None,
                    )
                    test_report.capability_report = {
                        "list_databases": CapabilityReport(
                            capable=True,
                            mitigation_message=f"Found {len(databases)} databases",
                        )
                    }
                finally:
                    client.close()
            else:
                # Test SQL connection
                from datahub.ingestion.source.sql.hive.hive_data_fetcher import (
                    SQLAlchemyDataFetcher,
                )

                fetcher = SQLAlchemyDataFetcher(config)
                try:
                    schemas = list(fetcher.fetch_schema_rows())
                    test_report.basic_connectivity = CapabilityReport(
                        capable=True,
                        failure_reason=None,
                    )
                    test_report.capability_report = {
                        "list_schemas": CapabilityReport(
                            capable=True,
                            mitigation_message=f"Found {len(schemas)} schemas",
                        )
                    }
                finally:
                    fetcher.close()

        except Exception as e:
            error_msg = str(e)
            mitigation = None

            if "GSSAPI" in error_msg or "Kerberos" in error_msg.lower():
                mitigation = (
                    "Kerberos authentication failed. Ensure you have a valid ticket "
                    "(run 'kinit') and check kerberos_service_name."
                )
            elif "Connection refused" in error_msg:
                mitigation = (
                    "Could not connect. Verify host_port and service is running."
                )
            elif "timed out" in error_msg.lower():
                mitigation = "Connection timed out. Check network and firewall rules."

            test_report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=error_msg,
                mitigation_message=mitigation,
            )

        return test_report

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        """Return WorkUnit processors for stale entity removal."""
        return [
            *super().get_workunit_processors(),
            self.stale_entity_removal_handler.workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Generate WorkUnits via the metadata processor."""
        from datahub.ingestion.source.sql.hive.hive_metadata_processor import (
            HiveMetadataProcessor,
        )

        processor = HiveMetadataProcessor(
            config=self.config,
            fetcher=self._fetcher,
            report=self.report,
            platform=self.platform,
            aggregator=self._aggregator,
            domain_registry=self.domain_registry,
        )

        yield from processor.get_workunits()

        # Emit aggregator workunits for view lineage
        if self._aggregator:
            for mcp in self._aggregator.gen_metadata():
                yield mcp.as_workunit()

    def get_report(self) -> HiveMetastoreSourceReport:
        """Return the source report."""
        return self.report

    def close(self) -> None:
        """Clean up resources."""
        # Report failures from Thrift fetcher
        from datahub.ingestion.source.sql.hive.hive_thrift_fetcher import (
            ThriftDataFetcher,
        )

        if isinstance(self._fetcher, ThriftDataFetcher):
            for db_name, error_msg in self._fetcher.get_database_failures():
                self.report.report_warning(
                    f"database-{db_name}",
                    f"Failed to process database: {error_msg}",
                )
            for db_name, table_name, error_msg in self._fetcher.get_table_failures():
                self.report.report_warning(
                    f"table-{db_name}.{table_name}",
                    f"Failed to process table: {error_msg}",
                )

        # Close fetcher
        self._fetcher.close()

        super().close()

    # =========================================================================
    # Backward Compatibility Methods
    # =========================================================================

    def get_db_schema(self, dataset_identifier: str) -> Tuple[Optional[str], str]:
        """
        Extract database and schema from dataset identifier.

        Kept for backward compatibility with code that may reference this method.
        """
        if dataset_identifier is None:
            raise InvalidDatasetIdentifierError("dataset_identifier cannot be None")
        elif not dataset_identifier.strip():
            raise InvalidDatasetIdentifierError("dataset_identifier cannot be empty")

        parts = dataset_identifier.split(".")
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
