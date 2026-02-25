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

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple

if TYPE_CHECKING:
    from datahub.ingestion.source.sql.hive.hive_data_fetcher import HiveDataFetcher
    from datahub.utilities.registries.domain_registry import DomainRegistry

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
from datahub.ingestion.source.sql.hive.hive_metastore_config import (
    HiveMetastore,
    HiveMetastoreConnectionType,
)
from datahub.ingestion.source.sql.hive.hive_metastore_report import (
    HiveMetastoreSourceReport,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)

logger = logging.getLogger(__name__)


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
            from datahub.ingestion.source.sql.hive.hive_sql_fetcher import (
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
                from datahub.ingestion.source.sql.hive.hive_sql_fetcher import (
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
