import hashlib
import json
import logging
import os
import shutil
import tempfile
import time
import zipfile
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from pydantic import field_validator

if TYPE_CHECKING:
    from mypy_boto3_s3.service_resource import ObjectSummary

from acryl_datahub_cloud.datahub_reporting.datahub_dataset import (
    DATAHUB_ACTOR_URN,
    DataHubBasedS3Dataset,
    DatasetMetadata,
    DatasetRegistrationSpec,
    FileStoreBackedDatasetConfig,
)
from acryl_datahub_cloud.elasticsearch.graph_service import BaseModelRow
from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetPropertiesClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)

logger = logging.getLogger(__name__)


@dataclass
class ExtractionStats:
    """Statistics about the extraction process."""

    source_path: str
    file_count: int
    total_size_bytes: int
    source_files_md5: str  # Combined MD5 of all source file checksums
    zip_md5: str  # MD5 of final ZIP file


class S3ClientConfig(ConfigModel):
    bucket: str = os.getenv("DATA_BUCKET", "")
    path: str = os.getenv("RDS_DATA_PATH", "rds_backup/metadata_aspect_v2")


class DataHubReportingExtractSQLSourceConfig(ConfigModel):
    enabled: bool = True
    server: Optional[DatahubClientConfig] = None
    sql_backup_config: S3ClientConfig
    extract_sql_store: FileStoreBackedDatasetConfig
    # Maximum size (in bytes) of files to stream from S3 per batch using chunked streaming.
    # Files are streamed in 8MB chunks directly from S3 to ZIP without writing to disk, processing
    # files in batches to limit peak memory usage. This prevents both disk pressure and excessive
    # memory consumption during batch processing.
    # Default: 5GB (5 * 1024 * 1024 * 1024 bytes)
    batch_size_bytes: int = 5 * 1024 * 1024 * 1024
    # Total number of days to check for a valid RDS backup, starting from yesterday.
    # For example, 7 means check yesterday through 7 days ago (7 dates total).
    # Only used as a fallback when the primary backup is incomplete.
    fallback_lookback_days: int = 7

    @field_validator("extract_sql_store", mode="before")
    @classmethod
    def set_default_extract_soft_delete_flag(cls, v):
        if v is None:
            return v

        # If v is already a FileStoreBackedDatasetConfig object, skip dict-based modifications
        if isinstance(v, FileStoreBackedDatasetConfig):
            return v

        # v is a dictionary - apply default values
        if "dataset_registration_spec" not in v:
            v["dataset_registration_spec"] = DatasetRegistrationSpec(soft_deleted=False)
        elif "soft_deleted" not in v["dataset_registration_spec"]:
            v["dataset_registration_spec"]["soft_deleted"] = False

        if "file" not in v:
            default_config = FileStoreBackedDatasetConfig.dummy()
            v["file"] = f"{default_config.file_name}.{default_config.file_extension}"
        else:
            file_path = Path(v["file"])
            v["file_name"] = file_path.stem
            v["file_extension"] = file_path.suffix.lstrip(".")

        return v


class SQLRow(BaseModelRow):
    urn: str
    aspect: str
    version: int
    metadata: str
    systemmetadata: Optional[str]
    createdon: float
    createdby: str
    createdfor: str

    @classmethod
    def generate_schema_metadata(cls) -> SchemaMetadataClass:
        """Generate SchemaMetadata from SQLRow definition.

        This allows us to define the schema statically without needing to download
        sample parquet files for schema inference.
        """

        def get_type_from_pyarrow_type(pyarrow_type: str) -> SchemaFieldDataTypeClass:
            """Convert PyArrow type string to SchemaFieldDataTypeClass."""
            numeric_types = (
                "int8",
                "int16",
                "int32",
                "int64",
                "uint8",
                "uint16",
                "uint32",
                "uint64",
                "float16",
                "float32",
                "float64",
                "double",
            )
            if pyarrow_type.lower() in numeric_types:
                return SchemaFieldDataTypeClass(type=NumberTypeClass())
            return SchemaFieldDataTypeClass(type=StringTypeClass())

        audit_stamp = AuditStampClass(
            time=int(time.time()) * 1000, actor=DATAHUB_ACTOR_URN
        )

        fields = []
        for field in cls.datahub_schema():
            fields.append(
                SchemaFieldClass(
                    fieldPath=field.name,
                    type=get_type_from_pyarrow_type(field.type.lower()),
                    nativeDataType=field.type,
                )
            )

        return SchemaMetadataClass(
            created=audit_stamp,
            lastModified=audit_stamp,
            hash="",
            platform="urn:li:dataPlatform:s3",
            version=0,
            schemaName="SQLRow",
            fields=fields,
            platformSchema=OtherSchemaClass(rawSchema=""),
        )


# Minimum valid ZIP file size in bytes.
# A ZIP file needs at least an End of Central Directory Record (EOCD), which is 22 bytes.
# Any file smaller than this cannot be a valid ZIP archive.
MIN_ZIP_FILE_SIZE = 22


@platform_name(id="datahub", platform_name="DataHub")
@config_class(DataHubReportingExtractSQLSourceConfig)
@support_status(SupportStatus.INCUBATING)
class DataHubReportingExtractSQLSource(Source):
    platform = "datahub"

    def __init__(
        self, config: DataHubReportingExtractSQLSourceConfig, ctx: PipelineContext
    ):
        super().__init__(ctx)
        self.config: DataHubReportingExtractSQLSourceConfig = config
        self.report = SourceReport()
        self.s3_client = boto3.client("s3")
        self.s3_resource = boto3.resource("s3")
        self.datahub_based_s3_dataset = DataHubBasedS3Dataset(
            self.config.extract_sql_store,
            dataset_metadata=DatasetMetadata(
                displayName="SQL Extract",
                description="This is an automated SQL Extract from DataHub's backend",
                schemaFields=SQLRow.datahub_schema(),
            ),
        )

    @staticmethod
    def _parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
        """Parse S3 URI into bucket and key components."""
        if not s3_uri.startswith("s3://"):
            raise ValueError(f"Invalid S3 URI (must start with 's3://'): {s3_uri}")
        path = s3_uri[5:]  # Strip "s3://"
        if "/" not in path:
            raise ValueError(f"Invalid S3 URI (missing key): {s3_uri}")
        bucket, key = path.split("/", 1)
        return bucket, key

    def _validate_zip_at_uri(self, s3_uri: str) -> Optional[int]:
        """Validate ZIP file exists and is valid at the given S3 URI.

        Checks:
        1. File exists in S3
        2. File has valid ZIP magic bytes (PK header: 0x504B0304)
        3. File is at least MIN_ZIP_FILE_SIZE bytes

        Uses a single S3 Range request to read the header and parse file size
        from Content-Range header.

        Returns:
            File size in bytes if valid, None if missing or invalid.
        """
        bucket, key = self._parse_s3_uri(s3_uri)

        try:
            response = self.s3_client.get_object(
                Bucket=bucket, Key=key, Range="bytes=0-3"
            )
            header_bytes = response["Body"].read()

            # Parse total file size from Content-Range header: "bytes 0-3/TOTAL"
            # Total may be "*" (unknown) per HTTP spec, e.g. "bytes 0-3/*"
            content_range = response.get("ContentRange", "")
            if "/" not in content_range:
                logger.warning(
                    f"No Content-Range header for {s3_uri}, cannot verify file size"
                )
                return None
            total_size_str = content_range.split("/")[1]
            try:
                total_size = int(total_size_str)
            except ValueError:
                logger.warning(
                    f"Cannot parse Content-Range total for {s3_uri}: {total_size_str!r}"
                )
                return None

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("404", "NoSuchKey"):
                return None
            raise

        # ZIP local file header signature: 0x504B0304
        zip_magic = b"PK\x03\x04"
        if header_bytes != zip_magic:
            logger.warning(
                f"ZIP at {s3_uri} has invalid header: {header_bytes.hex()} "
                f"(expected: {zip_magic.hex()})"
            )
            return None

        if total_size < MIN_ZIP_FILE_SIZE:
            logger.warning(
                f"ZIP at {s3_uri} is too small: {total_size} bytes "
                f"(minimum: {MIN_ZIP_FILE_SIZE})"
            )
            return None

        return total_size

    def _restore_from_s3_metadata(
        self, extraction_stats: ExtractionStats
    ) -> List[MetadataChangeProposalWrapper]:
        """Restore dataset metadata from S3 metadata.json using static schema.

        Uses SQLRow.generate_schema_metadata() to define schema statically, eliminating
        the need to download sample parquet files.

        Args:
            extraction_stats: Already-fetched extraction stats from S3 metadata.json.
        """
        schema_metadata = SQLRow.generate_schema_metadata()

        mcps = list(
            self.datahub_based_s3_dataset.register_dataset(
                dataset_urn=self.datahub_based_s3_dataset.get_dataset_urn(),
                physical_uri=self.datahub_based_s3_dataset.get_file_uri(),
                custom_properties=self._stats_to_custom_properties(extraction_stats),
                schema_metadata=schema_metadata,
            )
        )

        logger.info(
            f"Dataset metadata restored from S3 for {self.datahub_based_s3_dataset.get_dataset_urn()}"
        )
        return mcps

    def _get_metadata_s3_key(
        self, output_date: Optional[date] = None
    ) -> Tuple[str, str]:
        """Get S3 bucket and key for the metadata.json file.

        Args:
            output_date: Optional date for the output file. If None, uses today's date.
        """
        if output_date is not None:
            file_uri = self.datahub_based_s3_dataset.get_remote_file_uri(
                self.datahub_based_s3_dataset.config.bucket_prefix,
                date=output_date,
            )
        else:
            file_uri = self.datahub_based_s3_dataset.get_file_uri()
        bucket, key = self._parse_s3_uri(file_uri)
        # Replace last extension with _metadata.json (e.g., "data.zip" -> "data_metadata.json")
        # For files without extension, appends _metadata.json to the full key
        metadata_key = key.rsplit(".", 1)[0] + "_metadata.json"
        return bucket, metadata_key

    def _upload_extraction_metadata_to_s3(
        self, stats: ExtractionStats, output_date: date
    ) -> None:
        """Upload extraction metadata as JSON to S3 alongside the output file.

        Args:
            stats: Extraction statistics to upload.
            output_date: The date for the output file path.
        """
        bucket, metadata_key = self._get_metadata_s3_key(output_date)
        metadata = asdict(stats)
        logger.info(f"Uploading extraction metadata to s3://{bucket}/{metadata_key}")
        self.s3_client.put_object(
            Bucket=bucket,
            Key=metadata_key,
            Body=json.dumps(metadata).encode("utf-8"),
            ContentType="application/json",
        )

    def _read_extraction_metadata_from_s3(self) -> Optional[ExtractionStats]:
        """Read extraction metadata from S3 if it exists."""
        bucket, metadata_key = self._get_metadata_s3_key()
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=metadata_key)
            metadata = json.loads(response["Body"].read().decode("utf-8"))
            return ExtractionStats(**metadata)
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("404", "NoSuchKey"):
                logger.info(f"No metadata file found at s3://{bucket}/{metadata_key}")
                return None
            raise
        except (json.JSONDecodeError, TypeError, KeyError) as e:
            logger.warning(
                f"Failed to parse metadata file at s3://{bucket}/{metadata_key}: {e}"
            )
            return None

    def _validate_datahub_metadata(
        self,
        dataset_properties: Optional[DatasetPropertiesClass],
        s3_metadata: ExtractionStats,
    ) -> bool:
        """Validate DataHub metadata matches S3 metadata.json.

        Uses metadata.json as the source of truth to detect drift between
        DataHub and S3 state.
        """
        if dataset_properties is None:
            return False
        custom_props = dataset_properties.customProperties or {}
        expected = self._stats_to_custom_properties(s3_metadata)
        return all(custom_props.get(k) == v for k, v in expected.items())

    @staticmethod
    def _stats_to_custom_properties(stats: ExtractionStats) -> Dict[str, str]:
        """Convert ExtractionStats to custom properties dict."""
        return {
            "source_path": stats.source_path,
            "source_file_count": str(stats.file_count),
            "source_total_size_bytes": str(stats.total_size_bytes),
            "source_files_md5": stats.source_files_md5,
            "zip_md5": stats.zip_md5,
        }

    def _check_success_marker_valid(self, bucket: str, prefix: str) -> bool:
        """
        Check if _SUCCESS marker file exists and has the latest timestamp in the S3 prefix.

        The _SUCCESS marker should be written after all data files, so its LastModified
        timestamp must be >= all other files in the prefix to confirm the backup is complete.

        Streams objects without materializing the full list to keep memory constant.
        """
        success_file_key = f"{prefix}/_SUCCESS"

        success_obj = None
        latest_data_file = None
        object_count = 0

        for obj in self.s3_resource.Bucket(bucket).objects.filter(Prefix=prefix):
            object_count += 1
            if obj.key == success_file_key:
                success_obj = obj
            elif (
                latest_data_file is None
                or obj.last_modified > latest_data_file.last_modified
            ):
                latest_data_file = obj

        if object_count == 0:
            logger.warning(f"No files found at s3://{bucket}/{prefix}")
            return False

        logger.debug(
            f"Scanned {object_count} objects in s3://{bucket}/{prefix} for _SUCCESS marker"
        )

        if success_obj is None:
            logger.warning(
                f"_SUCCESS marker not found at s3://{bucket}/{success_file_key}"
            )
            return False

        # If there are no other files besides _SUCCESS, that's valid
        if latest_data_file is None:
            logger.info(
                f"Found _SUCCESS marker at s3://{bucket}/{success_file_key} (no other files in prefix)"
            )
            return True

        # Verify _SUCCESS has the latest timestamp
        if success_obj.last_modified >= latest_data_file.last_modified:
            logger.info(
                f"Found valid _SUCCESS marker at s3://{bucket}/{success_file_key} "
                f"(timestamp {success_obj.last_modified} >= latest data file {latest_data_file.last_modified})"
            )
            return True

        logger.warning(
            f"_SUCCESS marker at s3://{bucket}/{success_file_key} has timestamp {success_obj.last_modified}, "
            f"but data file {latest_data_file.key} has later timestamp {latest_data_file.last_modified}. "
            "Backup may be incomplete or corrupted."
        )
        return False

    def _check_output_zip_valid(self, target_date: date) -> bool:
        """Check if output ZIP file exists and is valid for the given date."""
        file_uri = self.datahub_based_s3_dataset.get_remote_file_uri(
            self.datahub_based_s3_dataset.config.bucket_prefix,
            date=target_date,
        )
        file_size = self._validate_zip_at_uri(file_uri)
        if file_size is None:
            return False
        logger.debug(f"Output ZIP at {file_uri} is valid ({file_size} bytes)")
        return True

    def _update_presigned_url_for_date(
        self,
        target_date: date,
        dataset_properties: Optional[DatasetPropertiesClass],
    ) -> List[MetadataChangeProposalWrapper]:
        """Update presigned URL for a specific date's output file.

        Returns empty list if generate_presigned_url is disabled in config.
        """
        if not self.datahub_based_s3_dataset.config.generate_presigned_url:
            return []

        previous_file_uri = self.datahub_based_s3_dataset.get_remote_file_uri(
            self.datahub_based_s3_dataset.config.bucket_prefix,
            date=target_date,
        )
        return self.datahub_based_s3_dataset._update_presigned_url(
            self.datahub_based_s3_dataset.get_dataset_urn(),
            previous_file_uri,
            dataset_properties,
        )

    def get_workunits(self) -> Iterator[MetadataWorkUnit]:
        if not self.config.enabled:
            logger.info("Source is disabled, stopping")
            return

        self.graph = (
            self.ctx.require_graph("Loading default graph coordinates.")
            if self.config.server is None
            else DataHubGraph(config=self.config.server)
        )

        dataset_properties: Optional[DatasetPropertiesClass] = self.graph.get_aspect(
            self.datahub_based_s3_dataset.get_dataset_urn(), DatasetPropertiesClass
        )

        # Generate a static path used for temporary files,
        # so we consistently delete the data (possibly based on pipeline name)
        tmp_dir_aux = (
            self.ctx.pipeline_name if self.ctx.pipeline_name else "sql_default_dir"
        )
        tmp_dir = os.path.join(tempfile.gettempdir(), tmp_dir_aux.replace(":", "_"))

        output_file = (
            self.datahub_based_s3_dataset.config.file
            if self.datahub_based_s3_dataset.config.file
            else f"{self.datahub_based_s3_dataset.config.file_name}.zip"
        )

        # Check today's output file and metadata once, then decide what to do
        today_file_uri = self.datahub_based_s3_dataset.get_file_uri()
        today_zip_valid = self._validate_zip_at_uri(today_file_uri) is not None
        s3_metadata = (
            self._read_extraction_metadata_from_s3() if today_zip_valid else None
        )

        if today_zip_valid and s3_metadata is not None:
            if self._validate_datahub_metadata(dataset_properties, s3_metadata):
                # Everything is in sync — just refresh presigned URL if needed
                logger.info(
                    f"Skipping sql extract - S3 output file at {today_file_uri} "
                    "is valid and DataHub metadata is in sync with metadata.json"
                )
                if self.datahub_based_s3_dataset.config.generate_presigned_url:
                    mcps = self.datahub_based_s3_dataset.update_presigned_url(
                        dataset_properties=dataset_properties
                    )
                    for mcp in mcps:
                        yield mcp.as_workunit()
                return

            # ZIP + metadata exist but DataHub is out of sync — restore from S3
            logger.info(
                "Restoring dataset metadata from S3 metadata.json using static schema"
            )
            for mcp in self._restore_from_s3_metadata(s3_metadata):
                yield mcp.as_workunit()
            return

        # Run full extraction
        yield from self._run_full_extraction(
            tmp_dir=tmp_dir,
            output_file=output_file,
            dataset_properties=dataset_properties,
        )

    def _run_full_extraction(
        self,
        tmp_dir: str,
        output_file: str,
        dataset_properties: Optional[DatasetPropertiesClass],
    ) -> Iterator[MetadataWorkUnit]:
        """Run full extraction with fallback to previous backups.

        Loops through dates starting from yesterday, looking for:
        1. A valid RDS backup with _SUCCESS marker (preferred - fresh data)
        2. If no valid RDS backup, falls back to existing output ZIP

        Checks exactly `fallback_lookback_days` dates (yesterday through N days ago).

        Output date convention: output_date = source_date + 1 day.
        For example, if the source backup is from Jan 31, the output is written to Feb 1's path.
        This is because the RDS backup for a given date contains data up to that date,
        so the output represents the state as of the next day.
        """
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()

        for days_back in range(self.config.fallback_lookback_days):
            current_date = yesterday - timedelta(days=days_back)

            # 1. Check if RDS backup for this date has valid _SUCCESS marker (preferred)
            time_partition_path = (
                f"year={current_date.year}/month={current_date.month:02d}"
                f"/day={current_date.day:02d}"
            )
            bucket_prefix = (
                f"{self.config.sql_backup_config.path}/{time_partition_path}"
            )

            if self._check_success_marker_valid(
                bucket=self.config.sql_backup_config.bucket,
                prefix=bucket_prefix,
            ):
                logger.info(
                    f"Found valid RDS backup for {current_date}, running extraction"
                )
                # Output date = source_date + 1 (source from Jan 31 -> output to Feb 1)
                output_date = current_date + timedelta(days=1)
                yield from self._perform_extraction(
                    tmp_dir=tmp_dir,
                    output_file=output_file,
                    bucket_prefix=bucket_prefix,
                    output_date=output_date,
                )
                return

            # 2. Fallback: check if valid output ZIP already exists for this source date
            # The output ZIP for source_date is stored at source_date + 1
            expected_output_date = current_date + timedelta(days=1)
            if self._check_output_zip_valid(expected_output_date):
                logger.info(
                    f"No valid RDS backup for {current_date}, but found valid output ZIP "
                    f"at {expected_output_date}. Refreshing presigned URL."
                )
                for mcp in self._update_presigned_url_for_date(
                    target_date=expected_output_date,
                    dataset_properties=dataset_properties,
                ):
                    yield mcp.as_workunit()
                return

            # Neither worked, try previous day
            logger.warning(
                f"No valid backup found for {current_date} "
                f"(RDS backup incomplete and output ZIP missing/invalid), "
                "trying previous day"
            )

        # No valid backup found within the lookback window
        min_date = yesterday - timedelta(days=self.config.fallback_lookback_days - 1)
        raise RuntimeError(
            f"No valid backup found in the last {self.config.fallback_lookback_days} days. "
            f"Both RDS backups and output ZIP files are missing or invalid for all "
            f"dates from {min_date} to {yesterday}. Manual intervention required."
        )

    def _upload_zip_to_s3(self, local_path: str, s3_uri: str) -> None:
        """Upload file to explicit S3 URI.

        Args:
            local_path: Local file path to upload.
            s3_uri: Full S3 URI (e.g., s3://bucket/path/file.zip).
        """
        bucket, key = self._parse_s3_uri(s3_uri)
        logger.info(f"Uploading {local_path} to {s3_uri}")
        self.s3_client.upload_file(local_path, bucket, key)

    def _perform_extraction(
        self,
        tmp_dir: str,
        output_file: str,
        bucket_prefix: str,
        output_date: date,
    ) -> Iterator[MetadataWorkUnit]:
        """Perform the actual extraction: download, ZIP, upload, and register dataset.

        Args:
            tmp_dir: Temporary directory for intermediate files.
            output_file: Name of the output ZIP file.
            bucket_prefix: S3 prefix for the source RDS backup.
            output_date: The date to use for the output file path (source_date + 1).
        """
        self._clean_up_old_state(state_directory=tmp_dir)

        try:
            extraction_stats = self._download_and_zip_in_batches(
                bucket=self.config.sql_backup_config.bucket,
                prefix=bucket_prefix,
                batch_dir=os.path.join(tmp_dir, "download"),
                output_zip=os.path.join(tmp_dir, output_file),
                batch_size_bytes=self.config.batch_size_bytes,
            )
            if extraction_stats is None:
                # This shouldn't happen if _SUCCESS marker was valid
                raise RuntimeError(
                    f"No files found at s3://{self.config.sql_backup_config.bucket}/{bucket_prefix} "
                    "despite valid _SUCCESS marker. The backup may be corrupted or incomplete."
                )

            # Force update the DataHubBasedS3Dataset local file path to match where the zip file was created.
            self.datahub_based_s3_dataset.local_file_path = os.path.join(
                tmp_dir, output_file
            )

            # Upload zip file to S3 using explicit output_date path
            file_uri = self.datahub_based_s3_dataset.get_remote_file_uri(
                self.datahub_based_s3_dataset.config.bucket_prefix,
                date=output_date,
            )
            self._upload_zip_to_s3(
                self.datahub_based_s3_dataset.local_file_path, file_uri
            )

            # Upload extraction metadata to S3 for future restoration without re-downloading
            self._upload_extraction_metadata_to_s3(extraction_stats, output_date)

            # Compute profile & schema information based on the parquet files that were downloaded.
            for mcp in self.datahub_based_s3_dataset.register_dataset(
                dataset_urn=self.datahub_based_s3_dataset.get_dataset_urn(),
                physical_uri=file_uri,
                local_file=os.path.join(tmp_dir, "download", "*.parquet"),
                custom_properties=self._stats_to_custom_properties(extraction_stats),
            ):
                yield mcp.as_workunit()

            logger.info(
                f"Reporting dataset registered at {self.datahub_based_s3_dataset.get_dataset_urn()}"
            )
        finally:
            self._clean_up_old_state(state_directory=tmp_dir)

    @staticmethod
    def _clean_up_old_state(state_directory: str) -> None:
        shutil.rmtree(state_directory, ignore_errors=True)
        download_dir = os.path.join(state_directory, "download")
        os.makedirs(download_dir, exist_ok=True)

    @staticmethod
    def _stream_file_to_zip_from_local(
        local_file_path: str,
        zipf: zipfile.ZipFile,
        file_name: str,
        chunk_size: int,
    ) -> str:
        """Stream file from local disk to ZIP, returning MD5 checksum."""
        # MD5 used for integrity checking, not cryptographic security
        file_md5 = hashlib.md5()
        with (
            open(local_file_path, "rb") as local_file,
            zipf.open(file_name, "w") as zip_entry,
        ):
            while True:
                chunk = local_file.read(chunk_size)
                if not chunk:
                    break
                file_md5.update(chunk)
                zip_entry.write(chunk)
        return file_md5.hexdigest()

    def _stream_file_to_zip_from_s3(
        self,
        bucket: str,
        file_key: str,
        zipf: zipfile.ZipFile,
        file_name: str,
        chunk_size: int,
    ) -> str:
        """Stream file from S3 to ZIP, returning MD5 checksum."""
        # MD5 used for integrity checking, not cryptographic security
        file_md5 = hashlib.md5()
        s3_response = self.s3_client.get_object(Bucket=bucket, Key=file_key)
        body_stream = s3_response["Body"]

        with zipf.open(file_name, "w") as zip_entry:
            while True:
                chunk = body_stream.read(chunk_size)
                if not chunk:
                    break
                file_md5.update(chunk)
                zip_entry.write(chunk)
        return file_md5.hexdigest()

    @staticmethod
    def _group_objects_into_batches(
        objects: List["ObjectSummary"], batch_size_bytes: int
    ) -> List[List["ObjectSummary"]]:
        """
        Group S3 objects into batches based on cumulative size.

        Files larger than batch_size_bytes get their own batch.
        """
        batches: List[List["ObjectSummary"]] = []
        current_batch: List["ObjectSummary"] = []
        current_batch_size = 0

        for obj in objects:
            obj_size = obj.size

            # If file is larger than batch size, give it its own batch
            if obj_size > batch_size_bytes:
                if current_batch:
                    batches.append(current_batch)
                    current_batch = []
                    current_batch_size = 0

                batches.append([obj])  # Solo batch for large file
                logger.warning(
                    f"File {obj.key} ({obj_size / (1024**2):.2f} MB) exceeds batch size "
                    f"({batch_size_bytes / (1024**2):.2f} MB), processing in separate batch"
                )
                continue

            # If adding this file would exceed batch size, start a new batch
            if (
                current_batch_size > 0
                and current_batch_size + obj_size > batch_size_bytes
            ):
                batches.append(current_batch)
                current_batch = []
                current_batch_size = 0

            current_batch.append(obj)
            current_batch_size += obj_size

        # Add the last batch if it has files
        if current_batch:
            batches.append(current_batch)

        return batches

    @staticmethod
    def _verify_zip_file(
        output_zip: str, expected_count: int, files_written: int
    ) -> int:
        """
        Verify ZIP file integrity after writing.

        Returns the ZIP file size in bytes on success.
        Raises RuntimeError with diagnostic info on failure.
        """
        if not os.path.exists(output_zip):
            raise RuntimeError(
                f"ZIP file {output_zip} does not exist after writing {files_written} files"
            )

        zip_file_size = os.path.getsize(output_zip)
        logger.info(
            f"ZIP file size: {zip_file_size / (1024 * 1024 * 1024):.2f} GB ({zip_file_size:,} bytes)"
        )

        if zip_file_size == 0:
            raise RuntimeError(
                f"ZIP file {output_zip} is empty (0 bytes) after writing {files_written} files"
            )

        try:
            with zipfile.ZipFile(output_zip, "r") as verify_zipf:
                actual_count = len(verify_zipf.namelist())
                if actual_count != expected_count:
                    raise RuntimeError(
                        f"ZIP verification failed: expected {expected_count} files, got {actual_count}. "
                        f"ZIP file may be corrupted."
                    )
        except zipfile.BadZipFile as e:
            # Provide diagnostic info to help debug production issues
            first_bytes = b""
            try:
                with open(output_zip, "rb") as f:
                    first_bytes = f.read(4)
            except Exception:
                pass
            raise RuntimeError(
                f"ZIP verification failed: {e}. "
                f"File size: {zip_file_size:,} bytes, "
                f"First 4 bytes: {first_bytes.hex() if first_bytes else 'unreadable'} "
                f"(expected: 504b0304 for PK ZIP header)"
            ) from e

        return zip_file_size

    @staticmethod
    def _compute_file_md5(file_path: str, chunk_size: int = 8 * 1024 * 1024) -> str:
        """Compute MD5 checksum of a file for integrity verification (not cryptographic use)."""
        md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                md5.update(chunk)
        return md5.hexdigest()

    def _download_and_zip_in_batches(
        self,
        bucket: str,
        prefix: str,
        batch_dir: str,
        output_zip: str,
        batch_size_bytes: int,
    ) -> Optional[ExtractionStats]:
        """
        Stream files from S3 directly into ZIP using chunked streaming, processing in batches to limit memory usage.

        Downloads the first file to batch_dir for schema/profile computation, then streams all files to ZIP
        using 8MB chunks to ensure constant memory usage regardless of individual file sizes.

        Args:
            bucket: S3 bucket name
            prefix: S3 prefix to filter objects
            batch_dir: Local directory for temporary sample file download (for schema computation)
            output_zip: Output ZIP file path
            batch_size_bytes: Maximum total size of files to stream in each batch before flushing

        Returns:
            ExtractionStats with file count, total size, and source path if files were processed, None otherwise
        """
        objects = list(self.s3_resource.Bucket(bucket).objects.filter(Prefix=prefix))

        if not objects:
            return None

        logger.info(
            f"Found {len(objects)} files in s3://{bucket}/{prefix}, streaming in batches of up to {batch_size_bytes / (1024**2):.2f} MB"
        )

        # Download first file to batch_dir for schema/profile computation
        # This is required by register_dataset() which needs a local parquet file to generate schema
        os.makedirs(batch_dir, exist_ok=True)
        first_obj = objects[0]
        sample_file_path = os.path.join(batch_dir, os.path.basename(first_obj.key))

        try:
            logger.info(
                f"Downloading first file s3://{bucket}/{first_obj.key} ({first_obj.size / (1024**2):.2f} MB) "
                f"to {sample_file_path} for schema computation"
            )
            self.s3_client.download_file(bucket, first_obj.key, sample_file_path)
        except ClientError as e:
            logger.error(f"Failed to download first file for schema computation: {e}")
            raise RuntimeError(
                f"Cannot compute schema without at least one sample file: {e}"
            ) from e

        # Group objects into batches based on cumulative size
        batches = self._group_objects_into_batches(objects, batch_size_bytes)
        logger.info(f"Split {len(objects)} files into {len(batches)} batches")

        # Track whether we've processed the first file to avoid downloading it twice
        first_obj_processed = False

        # Process all batches with a single ZIP file session to avoid append mode issues.
        # ZIP append mode ("a") can corrupt the central directory at scale, causing files
        # from later batches to be silently lost. By keeping the ZIP open for the entire
        # operation, the central directory is written only once at the end.
        chunk_size = 8 * 1024 * 1024  # 8MB chunks for constant memory usage
        files_written = 0
        file_checksums: List[str] = []

        with zipfile.ZipFile(
            output_zip, "w", zipfile.ZIP_STORED, allowZip64=True
        ) as zipf:
            for batch_idx, batch in enumerate(batches):
                batch_size_mb = sum(obj.size for obj in batch) / (1024 * 1024)
                logger.info(
                    f"Processing batch {batch_idx + 1}/{len(batches)} with {len(batch)} files ({batch_size_mb:.2f} MB)"
                )

                for obj in batch:
                    file_key = obj.key

                    # Preserve S3 path structure in ZIP to avoid filename collisions
                    # Strip only the common prefix, keep subdirectories
                    relative_path = file_key[len(prefix) :].lstrip("/")
                    file_name = (
                        relative_path if relative_path else os.path.basename(file_key)
                    )

                    try:
                        # If this is the first file and we already downloaded it, reuse local copy
                        if not first_obj_processed and file_key == first_obj.key:
                            logger.debug(
                                f"Adding {file_name} ({obj.size / (1024**2):.2f} MB) to ZIP from local file"
                            )
                            file_md5 = self._stream_file_to_zip_from_local(
                                sample_file_path, zipf, file_name, chunk_size
                            )
                            first_obj_processed = True
                        else:
                            # Stream from S3 using chunked reads for constant memory usage
                            logger.debug(
                                f"Streaming {file_name} ({obj.size / (1024**2):.2f} MB) from S3"
                            )
                            file_md5 = self._stream_file_to_zip_from_s3(
                                bucket, file_key, zipf, file_name, chunk_size
                            )

                        file_checksums.append(f"{file_name}:{file_md5}")
                        files_written += 1
                        logger.debug(f"Added {file_name} to ZIP (md5: {file_md5})")

                    except ClientError as e:
                        logger.error(f"Failed to stream s3://{bucket}/{file_key}: {e}")
                        raise RuntimeError(
                            f"Failed to stream file {file_key} from S3: {e}"
                        ) from e
                    except Exception as e:
                        logger.error(
                            f"Unexpected error processing s3://{bucket}/{file_key}: {e}"
                        )
                        raise RuntimeError(
                            f"Failed to process file {file_key}: {e}"
                        ) from e

                logger.info(
                    f"Batch {batch_idx + 1}/{len(batches)} complete, {files_written} files written so far"
                )

        # ZIP context manager has exited - central directory is now written
        logger.info(
            f"ZIP file closed after writing {files_written} files, verifying integrity..."
        )

        zip_file_size = self._verify_zip_file(output_zip, len(objects), files_written)

        # Compute combined source checksum from individual file checksums
        # Sort for deterministic ordering, then hash the concatenated checksums
        file_checksums.sort()
        combined_md5 = hashlib.md5("\n".join(file_checksums).encode()).hexdigest()

        # Compute ZIP file checksum
        zip_md5 = self._compute_file_md5(output_zip)

        total_size_bytes = sum(obj.size for obj in objects)
        logger.info(
            f"ZIP verification passed: {len(objects)} files ({total_size_bytes / (1024 * 1024):.2f} MB source) "
            f"compressed to {zip_file_size / (1024 * 1024):.2f} MB across {len(batches)} batches"
        )
        logger.info(f"Source files combined MD5: {combined_md5}, ZIP MD5: {zip_md5}")
        return ExtractionStats(
            source_path=f"s3://{bucket}/{prefix}",
            file_count=len(objects),
            total_size_bytes=total_size_bytes,
            source_files_md5=combined_md5,
            zip_md5=zip_md5,
        )

    def get_report(self) -> SourceReport:
        return self.report
