"""
HBase Source for DataHub Metadata Ingestion
"""

import logging
from typing import Dict, Iterable, List, Optional, Union

from pydantic import Field, field_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    NumberTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
)
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity

logger = logging.getLogger(__name__)

PLATFORM_NAME = "hbase"


class HBaseSourceConfig(StatefulIngestionConfigBase):
    """
    Configuration for HBase source
    """

    host: str = Field(
        description="HBase Thrift server hostname or IP address",
    )
    port: int = Field(
        default=9090,
        description="HBase Thrift server port (default: 9090)",
    )
    use_ssl: bool = Field(
        default=False,
        description="Whether to use SSL/TLS for connection",
    )
    timeout: Optional[int] = Field(
        default=30000,
        description="Connection timeout in milliseconds (default: 30000)",
    )
    namespace_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for namespaces to filter in ingestion.",
    )
    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables to filter in ingestion.",
    )
    include_column_families: bool = Field(
        default=True,
        description="Include column families as schema metadata",
    )
    env: str = Field(
        default="PROD",
        description="Environment to use in namespace when constructing URNs",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="Platform instance to use in namespace when constructing URNs",
    )

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        if not 1 <= v <= 65535:
            raise ValueError("Port must be between 1 and 65535")
        return v

    @field_validator("timeout")
    @classmethod
    def validate_timeout(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v <= 0:
            raise ValueError("Timeout must be positive")
        return v


class HBaseSourceReport(ConfigModel):
    """
    Report for HBase source
    """

    num_namespaces_scanned: int = 0
    num_tables_scanned: int = 0
    num_tables_failed: int = 0
    dropped_namespaces: List[str] = []
    dropped_tables: List[str] = []
    failures: List[Dict[str, str]] = []
    warnings: List[Dict[str, str]] = []

    def close(self) -> None:
        """Optional close method for report cleanup"""
        pass

    def report_entity_scanned(self, name: str, ent_type: str = "table") -> None:
        if ent_type == "namespace":
            self.num_namespaces_scanned += 1
        else:
            self.num_tables_scanned += 1

    def report_dropped(self, name: str) -> None:
        if "." in name:
            self.dropped_tables.append(name)
        else:
            self.dropped_namespaces.append(name)

    def failure(
        self,
        message: str,
        context: Optional[str] = None,
        exc: Optional[Exception] = None,
    ) -> None:
        failure_entry = {"message": message}
        if context:
            failure_entry["context"] = context
        if exc:
            failure_entry["exception"] = str(exc)
        self.failures.append(failure_entry)
        logger.error(f"Failure: {message} - Context: {context} - Exception: {exc}")

    def warning(self, message: str, context: Optional[str] = None) -> None:
        warning_entry = {"message": message}
        if context:
            warning_entry["context"] = context
        self.warnings.append(warning_entry)
        logger.warning(f"Warning: {message} - Context: {context}")


class NamespaceKey(ContainerKey):
    """Container key for HBase namespace"""

    namespace: str


@platform_name("HBase", id="hbase")
@config_class(HBaseSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default via stateful ingestion",
    supported=True,
)
class HBaseSource(StatefulIngestionSourceBase):
    """
    This plugin extracts the following metadata from Apache HBase:

    - Namespaces (as containers)
    - Tables with their metadata
    - Column families and column qualifiers
    - Table properties and configuration

    HBase is a distributed, scalable, big data store built on top of Hadoop.
    This connector uses the HBase Thrift API via the happybase library to extract metadata.

    Requirements:
    - HBase Thrift server must be running and accessible
    - Install the happybase library: pip install happybase
    """

    config: HBaseSourceConfig
    report: HBaseSourceReport
    platform: str

    def __init__(self, ctx: PipelineContext, config: HBaseSourceConfig):
        super().__init__(config, ctx)
        self.ctx = ctx
        self.platform = PLATFORM_NAME
        self.config = config
        self.report = HBaseSourceReport()
        self.connection = None

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "HBaseSource":
        config = HBaseSourceConfig.model_validate(config_dict)
        return cls(ctx, config)

    def get_platform(self) -> str:
        return PLATFORM_NAME

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def _connect(self) -> bool:
        """
        Establish connection to HBase via happybase (Thrift)
        """
        try:
            # Import happybase library
            import happybase

            logger.info(f"Connecting to HBase at {self.config.host}:{self.config.port}")

            # Create connection using happybase
            self.connection = happybase.Connection(
                host=self.config.host,
                port=self.config.port,
                timeout=self.config.timeout,
                transport="framed" if not self.config.use_ssl else "framed",
                protocol="binary",
            )

            # Test connection by listing tables
            _ = self.connection.tables()

            logger.info(
                f"Successfully connected to HBase at {self.config.host}:{self.config.port}"
            )
            return True

        except ImportError:
            self.report.failure(
                message="Failed to import happybase library. Please install it with: pip install happybase",
                context="connection",
            )
            logger.error(
                "happybase library not found. Install with: pip install happybase"
            )
            return False
        except Exception as e:
            # Clean up connection on failure
            if self.connection:
                try:
                    self.connection.close()
                    logger.debug("Closed failed connection attempt")
                except Exception as close_error:
                    logger.debug(f"Error closing failed connection: {close_error}")
                finally:
                    self.connection = None

            self.report.failure(
                message="Failed to connect to HBase",
                context=f"{self.config.host}:{self.config.port}",
                exc=e,
            )
            return False

    def _close_connection(self) -> None:
        """
        Internal method to safely close the HBase connection
        """
        if self.connection:
            try:
                self.connection.close()
                logger.info("HBase connection closed successfully")
            except Exception as e:
                logger.warning(f"Error closing HBase connection: {e}")
            finally:
                self.connection = None

    def _get_namespaces(self) -> List[str]:
        """
        Get list of namespaces from HBase
        """
        try:
            # Get all tables including their namespaces
            tables = self.connection.tables()

            namespaces = set()
            for table in tables:
                table_str = (
                    table.decode("utf-8") if isinstance(table, bytes) else str(table)
                )
                if b":" in table or ":" in table_str:
                    namespace = table_str.split(":", 1)[0]
                    namespaces.add(namespace)
                else:
                    namespaces.add("default")

            return sorted(list(namespaces))

        except Exception as e:
            self.report.failure(message="Failed to get namespaces from HBase", exc=e)
            return []

    def _get_tables_in_namespace(self, namespace: str) -> List[str]:
        """
        Get all tables in a given namespace
        """
        try:
            all_tables = self.connection.tables()
            namespace_tables = []

            for table in all_tables:
                table_str = (
                    table.decode("utf-8") if isinstance(table, bytes) else str(table)
                )

                if namespace == "default":
                    # Default namespace tables don't have namespace prefix
                    if ":" not in table_str:
                        namespace_tables.append(table_str)
                else:
                    # Check if table belongs to this namespace
                    if table_str.startswith(f"{namespace}:"):
                        # Remove namespace prefix for table name
                        table_name = table_str.split(":", 1)[1]
                        namespace_tables.append(table_name)

            return namespace_tables

        except Exception as e:
            self.report.failure(
                message=f"Failed to get tables for namespace {namespace}", exc=e
            )
            return []

    def _get_table_descriptor(self, full_table_name: str) -> Optional[Dict]:
        """
        Get table descriptor including column families using happybase
        """
        try:
            # Convert to bytes if string (happybase expects bytes for table names)
            table_name_bytes = (
                full_table_name.encode("utf-8")
                if isinstance(full_table_name, str)
                else full_table_name
            )

            # Get table object
            table = self.connection.table(table_name_bytes)

            # Get column families from table
            families = table.families()

            # Convert to dict structure
            result = {"column_families": {}}

            for cf_name, cf_descriptor in families.items():
                cf_name_str = (
                    cf_name.decode("utf-8")
                    if isinstance(cf_name, bytes)
                    else str(cf_name)
                )
                # Remove trailing colon if present
                cf_name_str = cf_name_str.rstrip(":")

                result["column_families"][cf_name_str] = {
                    "name": cf_name_str,
                    "maxVersions": cf_descriptor.get(b"VERSIONS", b"1").decode("utf-8")
                    if isinstance(cf_descriptor.get(b"VERSIONS"), bytes)
                    else str(cf_descriptor.get("VERSIONS", "1")),
                    "compression": cf_descriptor.get(b"COMPRESSION", b"NONE").decode(
                        "utf-8"
                    )
                    if isinstance(cf_descriptor.get(b"COMPRESSION"), bytes)
                    else str(cf_descriptor.get("COMPRESSION", "NONE")),
                    "inMemory": cf_descriptor.get(b"IN_MEMORY", b"false").decode(
                        "utf-8"
                    )
                    == "true"
                    if isinstance(cf_descriptor.get(b"IN_MEMORY"), bytes)
                    else cf_descriptor.get("IN_MEMORY", "false") == "true",
                    "blockCacheEnabled": cf_descriptor.get(
                        b"BLOCKCACHE", b"true"
                    ).decode("utf-8")
                    == "true"
                    if isinstance(cf_descriptor.get(b"BLOCKCACHE"), bytes)
                    else cf_descriptor.get("BLOCKCACHE", "true") == "true",
                    "timeToLive": cf_descriptor.get(b"TTL", b"FOREVER").decode("utf-8")
                    if isinstance(cf_descriptor.get(b"TTL"), bytes)
                    else str(cf_descriptor.get("TTL", "FOREVER")),
                }

            return result

        except Exception as e:
            self.report.failure(
                message=f"Failed to get descriptor for table {full_table_name}", exc=e
            )
            return None

    def _convert_hbase_type_to_schema_field_type(
        self, hbase_type: str = "bytes"
    ) -> SchemaFieldDataTypeClass:
        """
        Convert HBase data types to DataHub schema field types
        HBase stores everything as bytes, but we provide common type mappings
        """
        type_mapping = {
            "string": StringTypeClass(),
            "int": NumberTypeClass(),
            "long": NumberTypeClass(),
            "float": NumberTypeClass(),
            "double": NumberTypeClass(),
            "boolean": BooleanTypeClass(),
            "bytes": BytesTypeClass(),
            "array": ArrayTypeClass(nestedType=["bytes"]),
        }

        return SchemaFieldDataTypeClass(
            type=type_mapping.get(hbase_type.lower(), BytesTypeClass())
        )

    def _generate_schema_fields(self, table_descriptor: Dict) -> List[SchemaField]:
        """
        Generate schema fields from table descriptor
        """
        schema_fields = []

        # Add row key field (always present in HBase)
        schema_fields.append(
            SchemaField(
                fieldPath="rowkey",
                nativeDataType="bytes",
                type=self._convert_hbase_type_to_schema_field_type("bytes"),
                description="HBase row key",
                nullable=False,
                isPartOfKey=True,
            )
        )

        # Add column family fields
        for cf_name, _cf_props in table_descriptor.get("column_families", {}).items():
            schema_fields.append(
                SchemaField(
                    fieldPath=cf_name,
                    nativeDataType="column_family",
                    type=SchemaFieldDataTypeClass(type=BytesTypeClass()),
                    description=f"Column family: {cf_name}",
                    nullable=True,
                    isPartOfKey=False,
                )
            )

        return schema_fields

    def _generate_namespace_container(self, namespace: str) -> Container:
        """
        Generate container for HBase namespace
        """
        namespace_container_key = self._generate_namespace_container_key(namespace)

        return Container(
            namespace_container_key,
            display_name=namespace,
            qualified_name=namespace,
            subtype=DatasetContainerSubTypes.SCHEMA,
            description=f"HBase namespace: {namespace}",
        )

    def _generate_namespace_container_key(self, namespace: str) -> ContainerKey:
        """
        Generate container key for namespace
        """
        return NamespaceKey(
            namespace=namespace,
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

    def _generate_table_dataset(
        self, namespace: str, table_name: str, table_descriptor: Dict
    ) -> Optional[Dataset]:
        """
        Generate dataset for HBase table
        """
        # Full table name with namespace
        if namespace == "default":
            full_table_name = table_name
            dataset_name = table_name
        else:
            full_table_name = f"{namespace}:{table_name}"
            dataset_name = f"{namespace}.{table_name}"

        self.report.report_entity_scanned(dataset_name, ent_type="table")

        if not self.config.table_pattern.allowed(dataset_name):
            self.report.report_dropped(dataset_name)
            return None

        # Generate schema fields
        schema_fields = None
        if self.config.include_column_families and table_descriptor:
            try:
                schema_fields = self._generate_schema_fields(table_descriptor)
            except Exception:
                self.report.warning(
                    message="Failed to generate schema fields", context=dataset_name
                )

        # Generate custom properties
        custom_properties = {}
        if table_descriptor and "column_families" in table_descriptor:
            custom_properties["column_families"] = str(
                len(table_descriptor["column_families"])
            )
            for cf_name, cf_props in table_descriptor["column_families"].items():
                custom_properties[f"cf.{cf_name}.maxVersions"] = str(
                    cf_props.get("maxVersions", "1")
                )
                custom_properties[f"cf.{cf_name}.compression"] = str(
                    cf_props.get("compression", "NONE")
                )

        return Dataset(
            platform=self.platform,
            name=dataset_name,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            subtype=DatasetSubTypes.TABLE,
            parent_container=self._generate_namespace_container_key(namespace),
            schema=schema_fields,
            display_name=table_name,
            qualified_name=full_table_name,
            description=f"HBase table in namespace '{namespace}'",
            custom_properties=custom_properties,
        )

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        """
        Main method to generate work units
        """
        # Connect to HBase
        if not self._connect():
            return

        try:
            # Get all namespaces
            namespaces = self._get_namespaces()

            for namespace in namespaces:
                # Check if namespace matches pattern
                if not self.config.namespace_pattern.allowed(namespace):
                    self.report.report_dropped(namespace)
                    continue

                self.report.report_entity_scanned(namespace, ent_type="namespace")

                # Generate namespace container
                yield self._generate_namespace_container(namespace)

                # Get tables in namespace
                tables = self._get_tables_in_namespace(namespace)

                for table_name in tables:
                    try:
                        # Get full table name for HBase API
                        if namespace == "default":
                            full_table_name = table_name
                        else:
                            full_table_name = f"{namespace}:{table_name}"

                        # Get table descriptor
                        table_descriptor = self._get_table_descriptor(full_table_name)

                        # Generate table dataset
                        dataset = self._generate_table_dataset(
                            namespace, table_name, table_descriptor
                        )

                        if dataset:
                            yield dataset

                    except Exception as e:
                        self.report.num_tables_failed += 1
                        self.report.failure(
                            message="Failed to process table",
                            context=f"{namespace}:{table_name}",
                            exc=e,
                        )
        finally:
            # Always close connection after processing, even if errors occurred
            self._close_connection()

    def get_report(self) -> HBaseSourceReport:
        """
        Return the ingestion report
        """
        return self.report

    def close(self) -> None:
        """
        Clean up resources and close HBase connection
        """
        self._close_connection()
        super().close()
