"""
Hive Metastore Thrift Source for DataHub.

This connector extracts metadata from Hive Metastore via the Thrift protocol,
supporting Kerberos/SASL authentication. It extends HiveMetastoreSource and
overrides only the data-fetching layer to use Thrift instead of direct database access.

Use this connector when:
- HiveServer2 is not available (can't use the `hive` connector)
- Direct database access is not available (can't use the `hive-metastore` connector)
- Only the HMS Thrift API (port 9083) is accessible
"""

import logging
from typing import Any, Dict, Iterable, List, Optional

from pydantic import Field
from sqlalchemy.engine.reflection import Inspector

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
    SourceCapabilityModifier,
)
from datahub.ingestion.source.sql.hive.hive_metastore_source import (
    HiveMetastore,
    HiveMetastoreSource,
)
from datahub.ingestion.source.sql.hive.hive_thrift_client import (
    HiveMetastoreThriftClient,
    ThriftConnectionConfig,
    ThriftInspectorAdapter,
)
from datahub.ingestion.source.sql.hive.storage_lineage import HiveStorageLineage
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource

logger = logging.getLogger(__name__)


class HiveMetastoreThriftConfig(HiveMetastore):
    """
    Configuration for Hive Metastore Thrift source.

    Extends HiveMetastore config but replaces database connection settings
    with Thrift/Kerberos connection settings.
    """

    # Override host_port with Thrift-specific description
    host_port: str = Field(
        default="localhost:9083",
        description="Host and port of the Hive Metastore Thrift service. "
        "Example: 'hms.company.com:9083'",
    )

    # Kerberos settings
    use_kerberos: bool = Field(
        default=False,
        description="Whether to use Kerberos/SASL authentication. "
        "Set to true for Kerberized HMS environments.",
    )
    kerberos_service_name: str = Field(
        default="hive",
        description="Kerberos service name for the HMS principal. "
        "Default is 'hive', but some environments use custom names.",
    )
    kerberos_hostname_override: Optional[str] = Field(
        default=None,
        description="Override the hostname used for Kerberos principal construction. "
        "Use this when connecting through a load balancer where the connection "
        "hostname differs from the Kerberos principal hostname.",
    )

    # Connection timeout
    timeout_seconds: int = Field(
        default=60,
        description="Connection timeout in seconds.",
    )

    # HMS 3.x Catalog Support
    catalog_name: Optional[str] = Field(
        default=None,
        description="Catalog name for HMS 3.x. If not set, uses the default 'hive' catalog. "
        "Set this to ingest from a specific catalog in multi-catalog HMS deployments.",
    )
    include_catalog_name_in_ids: bool = Field(
        default=False,
        description="Add the catalog name to generated dataset URNs. "
        "Example: urn:li:dataset:(urn:li:dataPlatform:hive,mycatalog.mydb.mytable,PROD) "
        "vs urn:li:dataset:(urn:li:dataPlatform:hive,mydb.mytable,PROD)",
    )

    # Note: Retry logic is handled internally by the Thrift client with sensible defaults
    # (3 retries, exponential backoff, 30s max wait). These are not exposed as config options.
    #
    # Note: WHERE clause validation is handled by the parent class HiveMetastore.validate_thrift_settings()
    # which validates that WHERE clauses are not used when connection_type='thrift'.

    # Override database_pattern with clearer description for Thrift
    database_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for databases to filter in ingestion.",
    )

    def get_sql_alchemy_url(
        self, uri_opts: Optional[Dict[str, Any]] = None, database: Optional[str] = None
    ) -> str:
        """
        Not used for Thrift connector, but required by parent class.
        Returns a dummy URL since we don't use SQLAlchemy.
        """
        return "hive://localhost:10000/default"


@config_class(HiveMetastoreThriftConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled via stateful ingestion",
    supported=True,
)
@capability(SourceCapability.DATA_PROFILING, "Not supported", supported=False)
@capability(SourceCapability.TEST_CONNECTION, "Enabled", supported=True)
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
class HiveMetastoreThriftSource(HiveMetastoreSource, TestableSource):
    """
    Extracts metadata from Hive Metastore via Thrift protocol.

    This connector extends HiveMetastoreSource and overrides the data-fetching
    methods to use the Thrift API instead of direct database queries. All the
    metadata processing and WorkUnit generation logic is inherited from the
    parent class.

    Features:
    - Direct connection to HMS via Thrift (no HiveServer2 or database required)
    - Kerberos/SASL authentication with hostname override support
    - Reuses all existing HiveMetastoreSource logic for metadata processing
    - Automatic retry logic for transient network failures
    - Storage lineage support (S3, HDFS, Azure, GCS, etc.)
    """

    def __init__(self, config: HiveMetastoreThriftConfig, ctx: PipelineContext) -> None:
        # Skip HiveMetastoreSource.__init__ which creates SQLAlchemyClient
        # Instead, call SQLAlchemySource.__init__ directly
        SQLAlchemySource.__init__(self, config, ctx, config.mode.value)
        self.config: HiveMetastoreThriftConfig = config

        # Set up subtypes (same as HiveMetastoreSource)
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

        # We don't use SQLAlchemyClient - set to None to avoid errors
        self._alchemy_client = None  # type: ignore

        # Initialize storage lineage support (same as HiveMetastoreSource)
        self.storage_lineage = HiveStorageLineage(
            config=config,
            env=config.env,
        )

        # Create Thrift client config
        self._thrift_config = self._create_thrift_config(config)

        # Client will be initialized when needed
        self._thrift_client: Optional[HiveMetastoreThriftClient] = None

        # Create fake inspector for parent class methods
        self._thrift_inspector = ThriftInspectorAdapter(
            database=config.database or "hive"
        )

        # Cache for databases to process
        self._databases: Optional[List[str]] = None

    @classmethod
    def create(
        cls, config_dict: Dict[str, Any], ctx: PipelineContext
    ) -> "HiveMetastoreThriftSource":
        # Pydantic handles all defaults from HiveMetastoreThriftConfig field definitions
        config = HiveMetastoreThriftConfig.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def _create_thrift_config(
        config: HiveMetastoreThriftConfig, max_retries: Optional[int] = None
    ) -> ThriftConnectionConfig:
        """Create ThriftConnectionConfig from source config."""
        host_port_parts = config.host_port.split(":")
        host = host_port_parts[0]
        port = int(host_port_parts[1]) if len(host_port_parts) > 1 else 9083

        kwargs: Dict[str, Any] = {
            "host": host,
            "port": port,
            "use_kerberos": config.use_kerberos,
            "kerberos_service_name": config.kerberos_service_name,
            "kerberos_hostname_override": config.kerberos_hostname_override,
            "timeout_seconds": config.timeout_seconds,
        }
        if max_retries is not None:
            kwargs["max_retries"] = max_retries

        return ThriftConnectionConfig(**kwargs)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        """
        Test connectivity to Hive Metastore via Thrift.

        This method validates:
        1. Basic connectivity - Can connect to HMS
        2. Authentication - Kerberos/SASL handshake succeeds
        3. API access - Can list databases
        """
        test_report = TestConnectionReport()

        try:
            config = HiveMetastoreThriftConfig.model_validate(config_dict)

            # Create client config (single attempt for test connection)
            thrift_config = HiveMetastoreThriftSource._create_thrift_config(
                config, max_retries=1
            )

            # Attempt connection and basic API call
            client = HiveMetastoreThriftClient(thrift_config)
            client.connect()

            try:
                databases = client.get_all_databases()
                test_report.basic_connectivity = CapabilityReport(
                    capable=True,
                    failure_reason=None,
                )
                # Report number of databases found
                test_report.capability_report = {
                    "list_databases": CapabilityReport(
                        capable=True,
                        failure_reason=None,
                        mitigation_message=f"Found {len(databases)} databases",
                    )
                }
            finally:
                client.close()

        except ImportError as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=f"Missing dependencies: {e}",
                mitigation_message="Install with: pip install pymetastore thrift-sasl acryl-pyhive[hive-pure-sasl]",
            )
        except Exception as e:
            error_msg = str(e)

            # Provide helpful mitigation messages for common errors
            mitigation = None
            if "GSSAPI" in error_msg or "Kerberos" in error_msg.lower():
                mitigation = (
                    "Kerberos authentication failed. Ensure you have a valid ticket "
                    "(run 'kinit') and check kerberos_service_name and kerberos_hostname_override settings."
                )
            elif "Connection refused" in error_msg:
                mitigation = (
                    "Could not connect to HMS. Verify host_port is correct and "
                    "the Hive Metastore service is running on port 9083."
                )
            elif "timed out" in error_msg.lower():
                mitigation = (
                    "Connection timed out. Check network connectivity and firewall rules. "
                    "Consider increasing timeout_seconds."
                )

            test_report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=error_msg,
                mitigation_message=mitigation,
            )

        return test_report

    def _ensure_connected(self) -> None:
        """Ensure Thrift client is connected."""
        if self._thrift_client is None:
            # Create and connect in separate steps to avoid inconsistent state
            # if connect() fails (client would be set but not connected)
            client = HiveMetastoreThriftClient(self._thrift_config)
            client.connect()
            # Only assign after successful connection
            self._thrift_client = client

    def _get_catalog_name(self) -> Optional[str]:
        """Get catalog name from config, or None for default catalog."""
        return self.config.catalog_name

    def _get_databases_to_process(self) -> List[str]:
        """Get list of databases, applying pattern filter."""
        if self._databases is None:
            self._ensure_connected()
            assert self._thrift_client is not None

            catalog_name = self._get_catalog_name()
            all_databases = self._thrift_client.get_all_databases(catalog_name)
            self._databases = [
                db for db in all_databases if self.config.database_pattern.allowed(db)
            ]
            catalog_info = f" in catalog '{catalog_name}'" if catalog_name else ""
            logger.info(
                f"Found {len(all_databases)} databases{catalog_info}, "
                f"{len(self._databases)} after filtering"
            )

        return self._databases

    # -------------------------------------------------------------------------
    # Override data-fetching methods to use Thrift instead of SQL
    # Note: where_clause_suffix is accepted for API compatibility with parent class
    # but is ignored since we validate that these options are not set in config.
    # -------------------------------------------------------------------------

    def _fetch_table_rows(self, where_clause_suffix: str) -> Iterable[Dict[str, Any]]:
        """Fetch table/column rows via Thrift API."""
        self._ensure_connected()
        assert self._thrift_client is not None
        databases = self._get_databases_to_process()
        catalog_name = self._get_catalog_name()
        return self._thrift_client.iter_table_rows(databases, catalog_name)

    def _fetch_hive_view_rows(
        self, where_clause_suffix: str
    ) -> Iterable[Dict[str, Any]]:
        """Fetch Hive view rows via Thrift API."""
        self._ensure_connected()
        assert self._thrift_client is not None
        databases = self._get_databases_to_process()
        catalog_name = self._get_catalog_name()
        return self._thrift_client.iter_view_rows(databases, catalog_name)

    def _fetch_schema_rows(self, where_clause_suffix: str) -> Iterable[Dict[str, Any]]:
        """Fetch schema/database rows via Thrift API."""
        self._ensure_connected()
        assert self._thrift_client is not None
        databases = self._get_databases_to_process()
        catalog_name = self._get_catalog_name()
        return self._thrift_client.iter_schema_rows(databases, catalog_name)

    def _fetch_table_properties_rows(
        self, where_clause_suffix: str
    ) -> Iterable[Dict[str, Any]]:
        """Fetch table properties rows via Thrift API."""
        self._ensure_connected()
        assert self._thrift_client is not None
        databases = self._get_databases_to_process()
        catalog_name = self._get_catalog_name()
        return self._thrift_client.iter_table_properties_rows(databases, catalog_name)

    # -------------------------------------------------------------------------
    # Override inspector-related methods
    # -------------------------------------------------------------------------

    def get_inspectors(self) -> Iterable[Inspector]:
        """Return our fake inspector adapter."""
        yield self._thrift_inspector  # type: ignore

    def get_db_name(self, inspector: Inspector) -> str:
        """
        Get database/catalog name from config or inspector.

        This is used for URN generation when include_catalog_name_in_ids is True.
        Returns the catalog_name if set, otherwise falls back to config.database
        or the default "hive" catalog name.
        """
        if self.config.catalog_name:
            return self.config.catalog_name
        if self.config.database:
            return self.config.database
        # For Thrift, we use "hive" as the default catalog name
        return "hive"

    def get_schema_names(self, inspector: Inspector) -> List[str]:
        """
        Override to return schema names from Thrift API.

        In Hive, schemas are equivalent to databases, so we return
        the filtered list of databases.
        """
        return self._get_databases_to_process()

    # -------------------------------------------------------------------------
    # Override close to clean up Thrift connection
    # -------------------------------------------------------------------------

    def close(self) -> None:
        """Close Thrift connection and call parent close."""
        if self._thrift_client is not None:
            # Report any database/table failures that were tracked
            for db_name, error_msg in self._thrift_client.get_database_failures():
                self.report.report_warning(
                    f"Failed to process database {db_name}: {error_msg}"
                )
            for (
                db_name,
                table_name,
                error_msg,
            ) in self._thrift_client.get_table_failures():
                self.report.report_warning(
                    f"Failed to process table {db_name}.{table_name}: {error_msg}"
                )

            self._thrift_client.close()
            self._thrift_client = None
        # Don't call super().close() as it tries to close SQLAlchemy connection
        # which we don't have. Just close the stateful ingestion handler if present.
        if hasattr(self, "stale_entity_removal_handler"):
            self.stale_entity_removal_handler.close()
