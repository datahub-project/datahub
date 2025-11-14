import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union

import pyarrow as pa
import pydantic
from pydantic import Field
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType, TimestampType
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import RecordEnvelope
from datahub.ingestion.api.sink import Sink, SinkReport, WriteCallback
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)

logger = logging.getLogger(__name__)

DEFAULT_REST_TIMEOUT = 120
DEFAULT_REST_RETRY_POLICY = {"total": 3, "backoff_factor": 0.1}


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, *args, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None and hasattr(self, "timeout"):
            kwargs["timeout"] = self.timeout
        return super().send(request, *args, **kwargs)


class IcebergRestSinkConfig(ConfigModel):
    """Configuration for Iceberg REST Catalog sink"""

    uri: str = Field(
        description="URI of the Iceberg REST Catalog endpoint (e.g., 'http://localhost:8080/iceberg' or 'https://your-instance.acryl.io/gms/iceberg/')"
    )

    warehouse: str = Field(
        description="Warehouse name in the catalog (e.g., 'arctic_warehouse')"
    )

    namespace: str = Field(
        default="datahub_metadata",
        description="Namespace to store DataHub metadata tables",
    )

    table_name: str = Field(
        default="metadata_aspects_v2",
        description="Name of the table to store metadata aspects",
    )

    token: Optional[str] = Field(
        default=None,
        description="Authentication token for the REST catalog (e.g., DataHub Personal Access Token)",
    )

    aws_role_arn: Optional[str] = Field(
        default=None,
        description="AWS role ARN for assuming role authentication (used with vended credentials)",
    )

    create_table_if_not_exists: bool = Field(
        default=True,
        description="Create the table and namespace if they don't exist",
    )

    verify_warehouse: bool = Field(
        default=True,
        description=(
            "Verify that the warehouse exists and is accessible before writing data. "
            "For DataHub catalogs, the warehouse must be pre-created using 'datahub iceberg create'. "
            "For other catalogs, this validates the warehouse configuration."
        ),
    )

    batch_size: int = Field(
        default=10000,
        description=(
            "Number of records to batch before writing to Iceberg. "
            "Larger batches (10000-50000) create fewer, larger Parquet files which improves query performance. "
            "Smaller batches use less memory but create more files."
        ),
    )

    connection: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Connection resiliency settings with 'timeout' (seconds) and 'retry' (urllib3 Retry params)",
    )

    # S3 configuration
    s3_region: Optional[str] = Field(
        default=None,
        alias="s3.region",
        description="AWS S3 region",
    )

    s3_access_key_id: Optional[str] = Field(
        default=None,
        alias="s3.access-key-id",
        description="AWS S3 access key ID",
    )

    s3_secret_access_key: Optional[str] = Field(
        default=None,
        alias="s3.secret-access-key",
        description="AWS S3 secret access key",
    )

    s3_endpoint: Optional[str] = Field(
        default=None,
        alias="s3.endpoint",
        description="S3 endpoint URL (for MinIO or custom S3-compatible storage)",
    )

    # Allow additional catalog configuration
    class Config:
        extra = "allow"

    @pydantic.model_validator(mode="after")
    def _build_catalog_config(self):
        """Build catalog configuration dict for pyiceberg"""
        catalog_config: Dict[str, Any] = {
            "type": "rest",
            "uri": self.uri,
        }

        if self.token:
            catalog_config["token"] = self.token

        if self.s3_region:
            catalog_config["s3.region"] = self.s3_region

        if self.s3_access_key_id:
            catalog_config["s3.access-key-id"] = self.s3_access_key_id

        if self.s3_secret_access_key:
            catalog_config["s3.secret-access-key"] = self.s3_secret_access_key

        if self.s3_endpoint:
            catalog_config["s3.endpoint"] = self.s3_endpoint

        if self.aws_role_arn:
            catalog_config["header.X-Iceberg-Access-Delegation"] = "vended-credentials"

        if self.connection:
            catalog_config["connection"] = self.connection

        self._catalog_config = catalog_config
        return self


class IcebergRestSinkReport(SinkReport):
    """Report for Iceberg REST Catalog sink"""

    catalog_uri: Optional[str] = None
    warehouse_location: Optional[str] = None
    namespace_created: bool = False
    table_created: bool = False
    write_errors: int = 0

    def report_write_error(self) -> None:
        self.write_errors += 1


class IcebergRestSink(Sink[IcebergRestSinkConfig, IcebergRestSinkReport]):
    """Sink that writes DataHub metadata to an Iceberg REST Catalog"""

    def __post_init__(self) -> None:
        logger.info(f"Initializing Iceberg REST sink with URI: {self.config.uri}")

        # Initialize catalog
        self.catalog: Catalog = load_catalog(
            name="datahub_sink",
            warehouse=self.config.warehouse,
            **self.config._catalog_config,
        )

        # Apply retry/timeout configuration for REST catalogs
        if isinstance(self.catalog, RestCatalog):
            logger.debug("Configuring HTTP adapter for REST catalog")
            retry_policy: Dict[str, Any] = DEFAULT_REST_RETRY_POLICY.copy()
            if self.config.connection and self.config.connection.get("retry"):
                retry_policy.update(self.config.connection["retry"])
            retries = Retry(**retry_policy)
            logger.debug(f"Retry policy: {retry_policy}")

            timeout = DEFAULT_REST_TIMEOUT
            if self.config.connection and self.config.connection.get("timeout"):
                timeout = self.config.connection["timeout"]
            logger.debug(f"Timeout: {timeout}")

            self.catalog._session.mount(
                "http://", TimeoutHTTPAdapter(timeout=timeout, max_retries=retries)
            )
            self.catalog._session.mount(
                "https://", TimeoutHTTPAdapter(timeout=timeout, max_retries=retries)
            )

        # Store report metadata
        self.report.catalog_uri = self.config.uri
        self.report.warehouse_location = self.config.warehouse

        # Verify warehouse exists and is accessible
        if self.config.verify_warehouse:
            self._verify_warehouse()

        # Create namespace and table if needed
        if self.config.create_table_if_not_exists:
            self._ensure_table_exists()

        # Load the table
        self.table = self.catalog.load_table(
            f"{self.config.namespace}.{self.config.table_name}"
        )

        # Initialize batch buffer
        self.batch_buffer: list = []

        logger.info(
            f"Iceberg REST sink initialized successfully. Table: {self.config.namespace}.{self.config.table_name}"
        )

    def _verify_warehouse(self) -> None:
        """Verify that the warehouse exists and is accessible"""
        try:
            # Try to list namespaces to verify warehouse is accessible
            namespaces = self.catalog.list_namespaces()
            logger.info(
                f"Warehouse '{self.config.warehouse}' is accessible. "
                f"Found {len(namespaces)} existing namespace(s)."
            )
        except Exception as e:
            error_msg = (
                f"Failed to verify warehouse '{self.config.warehouse}'. Error: {e}. "
            )

            # Add DataHub-specific guidance
            if "datahub" in self.config.uri.lower() or "/iceberg/" in self.config.uri:
                error_msg += (
                    "For DataHub catalogs, ensure the warehouse was created using: "
                    f"datahub iceberg create -w {self.config.warehouse} -d <data_root> "
                    "-i <client_id> --client_secret <client_secret> --region <region> --role <role_arn>"
                )
            else:
                error_msg += (
                    "Verify that the warehouse parameter is correct and the catalog is running. "
                    "Set verify_warehouse=false to skip this check."
                )

            logger.error(error_msg)
            raise ValueError(error_msg) from e

    def _ensure_table_exists(self) -> None:
        """Create namespace and table if they don't exist"""
        # Check if namespace exists, create if not
        try:
            existing_namespaces = self.catalog.list_namespaces()
            namespace_tuple = (self.config.namespace,)

            if namespace_tuple not in existing_namespaces:
                try:
                    self.catalog.create_namespace(self.config.namespace)
                    self.report.namespace_created = True
                    logger.info(f"Created namespace: {self.config.namespace}")
                except NamespaceAlreadyExistsError:
                    # Race condition: namespace was created between check and create
                    logger.debug(f"Namespace already exists: {self.config.namespace}")
                    self.report.namespace_created = False
                except Exception as e:
                    # Handle other errors (like DataHub validation errors)
                    error_msg = str(e).lower()
                    if "already exists" in error_msg or "already exist" in error_msg:
                        logger.info(
                            f"Namespace {self.config.namespace} already exists (detected from error message)"
                        )
                        self.report.namespace_created = False
                    else:
                        logger.error(
                            f"Failed to create namespace {self.config.namespace}: {e}"
                        )
                        raise
            else:
                logger.info(f"Namespace {self.config.namespace} already exists")
                self.report.namespace_created = False
        except Exception as e:
            logger.error(
                f"Failed to check/create namespace {self.config.namespace}: {e}"
            )
            raise

        # Check if table exists, create if not
        try:
            table_identifier = f"{self.config.namespace}.{self.config.table_name}"

            # Try to load the table to see if it exists
            try:
                self.catalog.load_table(table_identifier)
                logger.info(f"Table {table_identifier} already exists")
                self.report.table_created = False
                return
            except Exception:
                # Table doesn't exist, create it
                pass

            schema = Schema(
                NestedField(1, "urn", StringType(), required=True),
                NestedField(2, "aspect", StringType(), required=True),
                NestedField(3, "metadata", StringType(), required=True),
                NestedField(4, "systemmetadata", StringType(), required=False),
                NestedField(5, "version", LongType(), required=True),
                NestedField(6, "createdon", TimestampType(), required=True),
            )

            # Partition by aspect for query performance
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=2,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="aspect",
                )
            )

            self.catalog.create_table(
                table_identifier, schema, partition_spec=partition_spec
            )
            self.report.table_created = True
            logger.info(f"Created table: {table_identifier}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info(
                    f"Table already exists: {self.config.namespace}.{self.config.table_name}"
                )
                self.report.table_created = False
            else:
                logger.error(f"Failed to create table {table_identifier}: {e}")
                raise

    def _process_record(
        self, record_envelope: RecordEnvelope
    ) -> Optional[Dict[str, Any]]:
        """Process a single record and return its data as a dict"""
        record = record_envelope.record

        # Convert MCP/MCE to MCP wrapper for consistent handling
        if isinstance(record, MetadataChangeProposalWrapper):
            mcp = record
        elif isinstance(record, MetadataChangeProposal):
            mcp = MetadataChangeProposalWrapper.try_from_obj(record)
        elif isinstance(record, MetadataChangeEvent):
            logger.warning("MetadataChangeEvent not directly supported, skipping")
            return None
        else:
            logger.warning(f"Unknown record type: {type(record)}, skipping")
            return None

        # Extract fields matching DataHub database columns
        urn = str(mcp.entityUrn) if mcp.entityUrn else ""
        aspect = mcp.aspectName or ""

        # Serialize aspect to JSON (this is the "metadata" column in database)
        metadata = ""
        if mcp.aspect:
            try:
                aspect_obj = mcp.aspect.to_obj()
                metadata = json.dumps(aspect_obj)
            except Exception as e:
                logger.warning(f"Failed to serialize aspect: {e}")
                metadata = str(mcp.aspect)

        # Serialize system metadata
        systemmetadata = ""
        if mcp.systemMetadata:
            try:
                systemmetadata = json.dumps(mcp.systemMetadata.to_obj())
            except Exception as e:
                logger.warning(f"Failed to serialize system metadata: {e}")
                systemmetadata = str(mcp.systemMetadata)

        # Version 0 represents the latest version in DataHub
        version = 0

        # Use timezone-naive timestamp to match Iceberg schema
        createdon = datetime.now(tz=timezone.utc).replace(tzinfo=None)

        return {
            "urn": urn,
            "aspect": aspect,
            "metadata": metadata,
            "systemmetadata": systemmetadata,
            "version": version,
            "createdon": createdon,
            "envelope": record_envelope,
        }

    def _flush_batch(self) -> None:
        """Flush the current batch of records to Iceberg"""
        if not self.batch_buffer:
            return

        try:
            # Separate envelopes from data
            envelopes = [record.pop("envelope") for record in self.batch_buffer]

            # Create PyArrow table from batch
            pa_table = pa.Table.from_pydict(
                {
                    "urn": [r["urn"] for r in self.batch_buffer],
                    "aspect": [r["aspect"] for r in self.batch_buffer],
                    "metadata": [r["metadata"] for r in self.batch_buffer],
                    "systemmetadata": [r["systemmetadata"] for r in self.batch_buffer],
                    "version": [r["version"] for r in self.batch_buffer],
                    "createdon": [r["createdon"] for r in self.batch_buffer],
                },
                schema=pa.schema(
                    [
                        ("urn", pa.string(), False),
                        ("aspect", pa.string(), False),
                        ("metadata", pa.string(), False),
                        ("systemmetadata", pa.string(), True),
                        ("version", pa.int64(), False),
                        ("createdon", pa.timestamp("us"), False),
                    ]
                ),
            )

            # Write batch to Iceberg table
            self.table.append(pa_table)

            # Reload table to get latest metadata (prevents optimistic concurrency conflicts)
            self.table = self.catalog.load_table(
                f"{self.config.namespace}.{self.config.table_name}"
            )

            # Report success for all records in batch
            for envelope in envelopes:
                self.report.report_record_written(envelope)

            logger.info(f"Flushed batch of {len(self.batch_buffer)} records to Iceberg")

        except Exception as e:
            logger.error(
                f"Failed to flush batch of {len(self.batch_buffer)} records: {e}",
                exc_info=True,
            )
            # Report errors for all records in batch
            for _ in self.batch_buffer:
                self.report.report_write_error()
            raise
        finally:
            # Clear the buffer
            self.batch_buffer.clear()

    def write_record_async(
        self,
        record_envelope: RecordEnvelope[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
            ]
        ],
        write_callback: WriteCallback,
    ) -> None:
        """Write a DataHub metadata record to the Iceberg table (batched)"""
        try:
            # Process the record
            record_data = self._process_record(record_envelope)

            if record_data is None:
                # Unsupported record type, skip it
                write_callback.on_success(record_envelope, {})
                return

            # Add to batch buffer
            self.batch_buffer.append(record_data)

            # Flush if batch is full
            if len(self.batch_buffer) >= self.config.batch_size:
                self._flush_batch()

            # Always report success immediately (actual write happens in flush)
            write_callback.on_success(record_envelope, {})

        except Exception as e:
            logger.error(f"Failed to process record for batching: {e}", exc_info=True)
            self.report.report_write_error()
            write_callback.on_failure(record_envelope, e, {})

    def close(self) -> None:
        """Cleanup resources and flush remaining records"""
        # Flush any remaining records in the batch
        if self.batch_buffer:
            logger.info(
                f"Flushing {len(self.batch_buffer)} remaining records before close"
            )
            try:
                self._flush_batch()
            except Exception as e:
                logger.error(f"Failed to flush remaining records on close: {e}")

        super().close()
        logger.info(
            f"Iceberg REST sink closed. Total records written: {self.report.total_records_written}, "
            f"Write errors: {self.report.write_errors}"
        )

    def configured(self) -> str:
        """Return human-readable configuration summary"""
        return f"Iceberg REST sink writing to {self.config.uri}/{self.config.warehouse}/{self.config.namespace}.{self.config.table_name}"
