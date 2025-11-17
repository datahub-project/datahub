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

    upsert: bool = Field(
        default=True,
        description=(
            "If True, uses PyIceberg's native upsert operation (requires PyIceberg 0.9.0+) "
            "to update existing records based on (urn, aspect, version) primary key, preventing duplicates. "
            "Falls back to append mode if upsert is not available. "
            "If False, uses append-only mode (creates duplicates on updates)."
        ),
    )

    truncate: bool = Field(
        default=False,
        description=(
            "If True, truncate the table before writing data. "
            "This removes all existing data from the table. Use with caution as it will delete all data."
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
        description=(
            "AWS S3 region. Required when the catalog delegates S3 I/O to the client. "
            "Not needed when using DataHub catalog with vended credentials (aws_role_arn)."
        ),
    )

    s3_access_key_id: Optional[str] = Field(
        default=None,
        alias="s3.access-key-id",
        description=(
            "AWS S3 access key ID. Required when the catalog delegates S3 I/O to the client. "
            "Not needed when using DataHub catalog with vended credentials or AWS default credential chain."
        ),
    )

    s3_secret_access_key: Optional[str] = Field(
        default=None,
        alias="s3.secret-access-key",
        description=(
            "AWS S3 secret access key. Required when the catalog delegates S3 I/O to the client. "
            "Not needed when using DataHub catalog with vended credentials or AWS default credential chain."
        ),
    )

    s3_endpoint: Optional[str] = Field(
        default=None,
        alias="s3.endpoint",
        description=(
            "S3 endpoint URL (for MinIO or custom S3-compatible storage). "
            "Only needed when using S3-compatible storage instead of AWS S3."
        ),
    )

    # Allow additional catalog configuration
    class Config:
        extra = "allow"

    @pydantic.model_validator(mode="after")
    def _build_catalog_config(self):
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
        # Validate that this sink is only used with the datahub source
        if (
            self.ctx.pipeline_config
            and hasattr(self.ctx.pipeline_config, "source")
            and self.ctx.pipeline_config.source is not None
        ):
            source_type = getattr(self.ctx.pipeline_config.source, "type", None)
            if source_type is not None and source_type != "datahub":
                raise ValueError(
                    f"The iceberg-rest sink is only compatible with the 'datahub' source. "
                    f"Current source type: '{source_type}'. "
                    f"This sink is designed to replicate DataHub's internal metadata structure "
                    f"and requires metadata in the format produced by the datahub source."
                )

        logger.info(f"Initializing Iceberg REST sink with URI: {self.config.uri}")

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

        self.report.catalog_uri = self.config.uri
        self.report.warehouse_location = self.config.warehouse

        if self.config.verify_warehouse:
            self._verify_warehouse()

        if self.config.create_table_if_not_exists:
            self._ensure_table_exists()

        self.table = self.catalog.load_table(
            f"{self.config.namespace}.{self.config.table_name}"
        )

        if self.config.truncate:
            logger.warning(
                f"Truncating table {self.config.namespace}.{self.config.table_name} - all existing data will be deleted"
            )
            try:
                # Use overwrite with empty PyArrow table to truncate
                empty_table = pa.Table.from_pydict(
                    {
                        "urn": [],
                        "entity_type": [],
                        "aspect": [],
                        "metadata": [],
                        "systemmetadata": [],
                        "version": [],
                        "createdon": [],
                    },
                    schema=pa.schema(
                        [
                            ("urn", pa.string(), False),
                            ("entity_type", pa.string(), False),
                            ("aspect", pa.string(), False),
                            ("metadata", pa.string(), False),
                            ("systemmetadata", pa.string(), True),
                            ("version", pa.int64(), False),
                            ("createdon", pa.timestamp("us"), False),
                        ]
                    ),
                )
                self.table.overwrite(empty_table)
                logger.info("Table truncated successfully")
            except Exception as e:
                logger.error(f"Failed to truncate table: {e}")
                raise

        # Use deferred upsert for incremental runs (stateful ingestion enabled)
        # to avoid reading the table multiple times
        self._use_deferred_upsert = False
        if self.config.upsert:
            if (
                self.ctx.pipeline_config
                and hasattr(self.ctx.pipeline_config, "source")
                and self.ctx.pipeline_config.source is not None
            ):
                source_config = self.ctx.pipeline_config.source.config
                si_config = None
                if isinstance(source_config, dict):
                    si_config = source_config.get("stateful_ingestion")
                elif hasattr(source_config, "stateful_ingestion"):
                    si_config = source_config.stateful_ingestion

                si_enabled = False
                if isinstance(si_config, dict):
                    si_enabled = si_config.get("enabled", False)
                elif hasattr(si_config, "enabled"):
                    si_enabled = si_config.enabled

                if si_enabled:
                    self._use_deferred_upsert = True
                    logger.info(
                        "Stateful ingestion enabled - using deferred upsert strategy "
                        "(collect all records, then upsert once at the end). "
                        "This is more efficient for incremental updates."
                    )

        if self._use_deferred_upsert:
            self.deferred_buffer: list = []
            logger.debug("Initialized deferred buffer for single upsert operation")

        self.batch_buffer: list = []

        logger.info(
            f"Iceberg REST sink initialized successfully. Table: {self.config.namespace}.{self.config.table_name}, "
            f"upsert={self.config.upsert}, truncate={self.config.truncate}, "
            f"deferred_upsert={self._use_deferred_upsert}"
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

        try:
            table_identifier = f"{self.config.namespace}.{self.config.table_name}"

            try:
                existing_table = self.catalog.load_table(table_identifier)
                logger.info(f"Table {table_identifier} already exists")
                if self.config.upsert:
                    props = existing_table.properties()
                    if "identifier-field-ids" not in props:
                        logger.warning(
                            f"Table {table_identifier} exists but doesn't have identifier-field-ids property. "
                            f"Upsert operations may fail. Consider recreating the table or updating properties."
                        )
                self.report.table_created = False
                return
            except Exception:
                pass

            schema = Schema(
                NestedField(1, "urn", StringType(), required=True),
                NestedField(2, "entity_type", StringType(), required=True),
                NestedField(3, "aspect", StringType(), required=True),
                NestedField(4, "metadata", StringType(), required=True),
                NestedField(5, "systemmetadata", StringType(), required=False),
                NestedField(6, "version", LongType(), required=True),
                NestedField(7, "createdon", TimestampType(), required=True),
            )

            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=2,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name="entity_type",
                )
            )

            # identifier-field-ids specifies which fields are used for upsert matching
            table_properties = {
                "identifier-field-ids": "1,3,6",  # urn (1), aspect (3), version (6)
            }

            self.catalog.create_table(
                table_identifier,
                schema,
                partition_spec=partition_spec,
                properties=table_properties,
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

        urn = str(mcp.entityUrn) if mcp.entityUrn else ""

        entity_type = "unknown"
        if urn:
            urn_parts = urn.split(":")
            if len(urn_parts) >= 3 and urn_parts[0] == "urn" and urn_parts[1] == "li":
                entity_type = urn_parts[2]

        aspect = mcp.aspectName or ""

        metadata = ""
        if mcp.aspect:
            try:
                aspect_obj = mcp.aspect.to_obj()
                metadata = json.dumps(aspect_obj)
            except Exception as e:
                logger.warning(f"Failed to serialize aspect: {e}")
                metadata = str(mcp.aspect)

        systemmetadata = ""
        if mcp.systemMetadata:
            try:
                systemmetadata = json.dumps(mcp.systemMetadata.to_obj())
            except Exception as e:
                logger.warning(f"Failed to serialize system metadata: {e}")
                systemmetadata = str(mcp.systemMetadata)

        version = 0

        createdon = datetime.now(tz=timezone.utc).replace(tzinfo=None)

        return {
            "urn": urn,
            "entity_type": entity_type,
            "aspect": aspect,
            "metadata": metadata,
            "systemmetadata": systemmetadata,
            "version": version,
            "createdon": createdon,
            "envelope": record_envelope,
        }

    def _deduplicate_batch(self) -> None:
        """Deduplicate batch by (urn, aspect, version), keeping latest record"""
        if not self.batch_buffer:
            return

        unique_records = {}
        for record in self.batch_buffer:
            key = (record["urn"], record["aspect"], record["version"])
            if key not in unique_records:
                unique_records[key] = record
            else:
                # Safety checks to prevent bus errors from invalid datetime comparisons
                current_createdon = record.get("createdon")
                existing_createdon = unique_records[key].get("createdon")

                if current_createdon is not None and existing_createdon is not None:
                    try:
                        if current_createdon > existing_createdon:
                            unique_records[key] = record
                    except (TypeError, ValueError) as e:
                        logger.warning(
                            f"Error comparing timestamps: {e}. Keeping existing record."
                        )
                elif current_createdon is not None:
                    unique_records[key] = record

        if len(unique_records) < len(self.batch_buffer):
            logger.debug(
                f"Deduplicated batch: {len(self.batch_buffer)} -> {len(unique_records)} records"
            )
            self.batch_buffer = list(unique_records.values())

    def _create_pyarrow_table(self) -> pa.Table:
        """Create PyArrow table from batch buffer"""
        try:
            return pa.Table.from_pydict(
                {
                    "urn": [r["urn"] for r in self.batch_buffer],
                    "entity_type": [r["entity_type"] for r in self.batch_buffer],
                    "aspect": [r["aspect"] for r in self.batch_buffer],
                    "metadata": [r["metadata"] for r in self.batch_buffer],
                    "systemmetadata": [r["systemmetadata"] for r in self.batch_buffer],
                    "version": [r["version"] for r in self.batch_buffer],
                    "createdon": [r["createdon"] for r in self.batch_buffer],
                },
                schema=pa.schema(
                    [
                        ("urn", pa.string(), False),
                        ("entity_type", pa.string(), False),
                        ("aspect", pa.string(), False),
                        ("metadata", pa.string(), False),
                        ("systemmetadata", pa.string(), True),
                        ("version", pa.int64(), False),
                        ("createdon", pa.timestamp("us"), False),
                    ]
                ),
            )
        except Exception as e:
            logger.error(
                f"Failed to create PyArrow table: {e}. "
                f"Batch size: {len(self.batch_buffer)}. "
                f"Sample record keys: {list(self.batch_buffer[0].keys()) if self.batch_buffer else 'empty'}"
            )
            raise

    def _perform_upsert(self, pa_table: pa.Table) -> None:
        """Perform upsert operation on Iceberg table"""
        # Reload table before upsert to avoid optimistic concurrency conflicts
        try:
            self.table = self.catalog.load_table(
                f"{self.config.namespace}.{self.config.table_name}"
            )
        except Exception as e:
            logger.warning(f"Failed to reload table before upsert: {e}")

        logger.debug(
            f"Performing upsert operation for {len(self.batch_buffer)} records "
            f"matching on primary key (urn, aspect, version)"
        )
        try:
            if hasattr(self.table, "upsert"):
                # Try with join columns first, fall back to identifier-field-ids if that fails
                try:
                    result = self.table.upsert(
                        pa_table,
                        ["urn", "aspect", "version"],  # Join columns
                    )
                except (TypeError, ValueError) as e:
                    # If that fails, try without join columns (using identifier-field-ids from table)
                    logger.debug(
                        f"Upsert with join columns failed: {e}, trying without"
                    )
                    try:
                        result = self.table.upsert(pa_table)
                    except Exception as e2:
                        # If all attempts fail, log and fall back to append
                        logger.warning(
                            f"Upsert failed: {e2}. Falling back to append mode. "
                            f"Ensure table has identifier-field-ids property set."
                        )
                        raise
                if hasattr(result, "rows_updated") and hasattr(result, "rows_inserted"):
                    logger.info(
                        f"Upserted {len(self.batch_buffer)} records: "
                        f"{result.rows_updated} updated, {result.rows_inserted} inserted"
                    )
                else:
                    logger.info(
                        f"Upserted {len(self.batch_buffer)} records (result: {result})"
                    )
            else:
                logger.warning(
                    "PyIceberg upsert not available (requires 0.9.0+). "
                    "Using append mode. Upgrade PyIceberg for native upsert support."
                )
                self.table.append(pa_table)
        except AttributeError:
            logger.warning(
                "PyIceberg upsert not available. Using append mode. "
                "Upgrade to PyIceberg 0.9.0+ for native upsert support."
            )
            self.table.append(pa_table)
        except Exception as e:
            error_type = type(e).__name__
            if "CommitFailedException" in error_type or "409" in str(e):
                logger.warning(
                    f"Optimistic concurrency conflict during upsert: {e}. "
                    f"Reloading table and retrying with append mode."
                )
                self.table = self.catalog.load_table(
                    f"{self.config.namespace}.{self.config.table_name}"
                )
                self.table.append(pa_table)
            else:
                logger.error(
                    f"Upsert operation failed: {e}. Falling back to append mode.",
                    exc_info=True,
                )
                self.table.append(pa_table)

    def _perform_append(self, pa_table: pa.Table) -> None:
        """Perform append operation on Iceberg table"""
        logger.debug(
            f"Performing append (INSERT) operation for {len(self.batch_buffer)} records. "
            f"This is a first run or stateful_ingestion is disabled, so using insert instead of upsert."
        )
        self.table.append(pa_table)

    def _reload_table(self) -> None:
        """Reload table to get latest metadata (prevents optimistic concurrency conflicts)"""
        try:
            self.table = self.catalog.load_table(
                f"{self.config.namespace}.{self.config.table_name}"
            )
        except Exception as e:
            logger.warning(f"Failed to reload table after batch write: {e}")
            # Continue anyway - table will be reloaded on next access

    def _flush_batch(self) -> None:
        """Flush the current batch of records to Iceberg"""
        if not self.batch_buffer:
            return

        try:
            envelopes = [record.pop("envelope") for record in self.batch_buffer]

            # Deduplicate before creating PyArrow table to avoid memory issues
            # Only needed for upsert (incremental runs), not for append (first runs)
            should_upsert = self.config.upsert and self._use_deferred_upsert
            if should_upsert:
                self._deduplicate_batch()

            pa_table = self._create_pyarrow_table()

            if should_upsert:
                self._perform_upsert(pa_table)
            else:
                self._perform_append(pa_table)

            self._reload_table()

            for envelope in envelopes:
                self.report.report_record_written(envelope)

            logger.info(f"Flushed batch of {len(self.batch_buffer)} records to Iceberg")

        except Exception as e:
            logger.error(
                f"Failed to flush batch of {len(self.batch_buffer)} records: {e}",
                exc_info=True,
            )
            for _ in self.batch_buffer:
                self.report.report_write_error()
            raise
        finally:
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
        """Write a DataHub metadata record to the Iceberg table (batched or deferred)"""
        try:
            record_data = self._process_record(record_envelope)

            if record_data is None:
                write_callback.on_success(record_envelope, {})
                return

            if self._use_deferred_upsert:
                self.deferred_buffer.append(record_data)
            else:
                self.batch_buffer.append(record_data)
                if len(self.batch_buffer) >= self.config.batch_size:
                    self._flush_batch()

            write_callback.on_success(record_envelope, {})

        except Exception as e:
            logger.error(f"Failed to process record for batching: {e}", exc_info=True)
            self.report.report_write_error()
            write_callback.on_failure(record_envelope, e, {})

    def _flush_deferred_upsert(self) -> None:
        """Flush all deferred records in a single upsert operation"""
        if not self.deferred_buffer:
            return

        logger.info(
            f"Flushing {len(self.deferred_buffer)} deferred records in single upsert operation"
        )

        try:
            envelopes = [record.pop("envelope") for record in self.deferred_buffer]

            unique_records = {}
            for record in self.deferred_buffer:
                key = (record["urn"], record["aspect"], record["version"])
                if key not in unique_records:
                    unique_records[key] = record
                else:
                    current_createdon = record.get("createdon")
                    existing_createdon = unique_records[key].get("createdon")

                    if current_createdon is not None and existing_createdon is not None:
                        try:
                            if current_createdon > existing_createdon:
                                unique_records[key] = record
                        except (TypeError, ValueError) as e:
                            logger.warning(
                                f"Error comparing timestamps: {e}. Keeping existing record."
                            )
                    elif current_createdon is not None:
                        unique_records[key] = record

            if len(unique_records) < len(self.deferred_buffer):
                logger.debug(
                    f"Deduplicated deferred records: {len(self.deferred_buffer)} -> {len(unique_records)}"
                )

            pa_table = pa.Table.from_pydict(
                {
                    "urn": [r["urn"] for r in unique_records.values()],
                    "entity_type": [r["entity_type"] for r in unique_records.values()],
                    "aspect": [r["aspect"] for r in unique_records.values()],
                    "metadata": [r["metadata"] for r in unique_records.values()],
                    "systemmetadata": [
                        r["systemmetadata"] for r in unique_records.values()
                    ],
                    "version": [r["version"] for r in unique_records.values()],
                    "createdon": [r["createdon"] for r in unique_records.values()],
                },
                schema=pa.schema(
                    [
                        ("urn", pa.string(), False),
                        ("entity_type", pa.string(), False),
                        ("aspect", pa.string(), False),
                        ("metadata", pa.string(), False),
                        ("systemmetadata", pa.string(), True),
                        ("version", pa.int64(), False),
                        ("createdon", pa.timestamp("us"), False),
                    ]
                ),
            )

            # Reload table before upsert to avoid optimistic concurrency conflicts
            try:
                self.table = self.catalog.load_table(
                    f"{self.config.namespace}.{self.config.table_name}"
                )
            except Exception as e:
                logger.warning(f"Failed to reload table before deferred upsert: {e}")

            if hasattr(self.table, "upsert"):
                try:
                    result = self.table.upsert(
                        pa_table,
                        ["urn", "aspect", "version"],  # Join columns
                    )
                except (TypeError, ValueError) as e:
                    # If that fails, try without join columns (using identifier-field-ids from table)
                    logger.debug(
                        f"Upsert with join columns failed: {e}, trying without"
                    )
                    try:
                        result = self.table.upsert(pa_table)
                    except Exception as e2:
                        # If all attempts fail, log and fall back to append
                        logger.warning(
                            f"Upsert failed: {e2}. Falling back to append mode. "
                            f"Ensure table has identifier-field-ids property set."
                        )
                        self.table.append(pa_table)
                        result = None

                if (
                    result
                    and hasattr(result, "rows_updated")
                    and hasattr(result, "rows_inserted")
                ):
                    logger.info(
                        f"Upserted {len(unique_records)} records: "
                        f"{result.rows_updated} updated, {result.rows_inserted} inserted"
                    )
                else:
                    logger.info(
                        f"Upserted {len(unique_records)} records (result: {result})"
                    )
            else:
                logger.warning(
                    "PyIceberg upsert not available. Using append mode. "
                    "Upgrade to PyIceberg 0.9.0+ for native upsert support."
                )
                self.table.append(pa_table)

            for envelope in envelopes:
                self.report.report_record_written(envelope)

            logger.info(
                f"Successfully flushed {len(unique_records)} deferred records in single upsert operation"
            )

        except Exception as e:
            logger.error(
                f"Failed to flush deferred upsert: {e}",
                exc_info=True,
            )
            for _ in self.deferred_buffer:
                self.report.report_write_error()
            raise
        finally:
            self.deferred_buffer.clear()

    def close(self) -> None:
        """Cleanup resources and flush remaining records"""
        if self._use_deferred_upsert and self.deferred_buffer:
            try:
                self._flush_deferred_upsert()
            except Exception as e:
                logger.error(f"Failed to flush deferred records on close: {e}")

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
