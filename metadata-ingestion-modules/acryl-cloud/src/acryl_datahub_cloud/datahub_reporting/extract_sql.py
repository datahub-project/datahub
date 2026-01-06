import logging
import os
import shutil
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, List, Literal, Optional

import boto3
from botocore.exceptions import ClientError
from pydantic import field_validator

if TYPE_CHECKING:
    from mypy_boto3_s3.service_resource import ObjectSummary

from acryl_datahub_cloud.datahub_reporting.datahub_dataset import (
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
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import DatasetPropertiesClass

logger = logging.getLogger(__name__)


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
            v["file_name"] = v["file"].split(".")[0]
            v["file_extension"] = v["file"].split(".")[-1]

        return v


class SQLGraphRow(BaseModelRow):
    urn: str
    aspect: str
    version: int
    metadata: str
    systemmetadata: Optional[str]
    createdon: float
    createdby: str
    createdfor: str


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
        self.opened_files: List[str] = []
        self.s3_client = boto3.client("s3")
        self.datahub_based_s3_dataset = DataHubBasedS3Dataset(
            self.config.extract_sql_store,
            dataset_metadata=DatasetMetadata(
                displayName="SQL Extract",
                description="This is an automated SQL Extract from DataHub's backend",
                schemaFields=SQLGraphRow.datahub_schema(),
            ),
        )

    @staticmethod
    def should_skip_extract(
        dataset_properties: Optional[DatasetPropertiesClass],
    ) -> bool:
        """Skip graph extraction if the dataset has already been pushed to DataHub today"""

        skip_extract = False
        # If present check is the updated timestamp for the current day
        if dataset_properties and dataset_properties.lastModified:
            # datetime function requires timestamp in seconds
            ts = datetime.fromtimestamp(dataset_properties.lastModified.time / 1000)
            skip_extract = ts.day is datetime.today().day

            if skip_extract:
                logger.info(
                    f"Skipping sql extract as dataset has been updated today {ts}"
                )

        return skip_extract

    def get_workunits(self):
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
        tmp_dir = f"/tmp/{tmp_dir_aux.replace(':', '_')}"

        output_file = (
            self.datahub_based_s3_dataset.config.file
            if self.datahub_based_s3_dataset.config.file
            else f"{self.datahub_based_s3_dataset.config.file_name}.zip"
        )

        mcps: Iterable[MetadataChangeProposalWrapper] = []
        if (
            self.should_skip_extract(dataset_properties)
            and self.datahub_based_s3_dataset.config.generate_presigned_url
        ):
            mcps = self.datahub_based_s3_dataset.update_presigned_url(
                dataset_properties=dataset_properties
            )
        else:
            # Get yesterday's RDS data dump.
            previous_date = datetime.now() - timedelta(days=1)
            time_partition_path = "year={}/month={:02d}/day={:02d}".format(
                previous_date.year, previous_date.month, previous_date.day
            )
            bucket_prefix = (
                f"{self.config.sql_backup_config.path}/{time_partition_path}"
            )

            self._clean_up_old_state(state_directory=tmp_dir)

            files_downloaded: bool = self._download_and_zip_in_batches(
                bucket=self.config.sql_backup_config.bucket,
                prefix=bucket_prefix,
                batch_dir=f"{tmp_dir}/download/",
                output_zip=f"{tmp_dir}/{output_file}",
                batch_size_bytes=self.config.batch_size_bytes,
            )
            if not files_downloaded:
                logger.warning(f"Skipping as no files were found in {bucket_prefix}")
                return

            # Compute profile & schema information, this is based on the parquet files that were downloaded and not the zip file.
            # We must hard-code the local file from which the dataset will be created, otherwise the upload to s3 will be in
            # unexpected path.
            mcps = self.datahub_based_s3_dataset.register_dataset(
                dataset_urn=self.datahub_based_s3_dataset.get_dataset_urn(),
                physical_uri=self.datahub_based_s3_dataset.get_file_uri(),
                local_file=f"{tmp_dir}/download/*.parquet",
            )

            # Force update the DataHubBasedS3Dataset local file path to match where the zip file was created.
            self.datahub_based_s3_dataset.local_file_path = f"{tmp_dir}/{output_file}"
            # Upload zip file to s3
            self.datahub_based_s3_dataset.upload_file_to_s3()

        for mcp in mcps:
            # logger.warning(json.dumps(str(mcp)))
            yield mcp.as_workunit()

        logger.info(
            f"Reporting dataset registered at {self.datahub_based_s3_dataset.get_dataset_urn()}"
        )

        self._clean_up_old_state(state_directory=tmp_dir)

    @staticmethod
    def _clean_up_old_state(state_directory: str) -> None:
        shutil.rmtree(state_directory, ignore_errors=True)
        path = Path(f"{state_directory}/download/")
        path.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _stream_file_to_zip_from_local(
        local_file_path: str,
        zipf: zipfile.ZipFile,
        file_name: str,
        chunk_size: int,
    ) -> None:
        """Stream file from local disk to ZIP using chunked reads."""
        with (
            open(local_file_path, "rb") as local_file,
            zipf.open(file_name, "w") as zip_entry,
        ):
            while True:
                chunk = local_file.read(chunk_size)
                if not chunk:
                    break
                zip_entry.write(chunk)

    def _stream_file_to_zip_from_s3(
        self,
        bucket: str,
        file_key: str,
        zipf: zipfile.ZipFile,
        file_name: str,
        chunk_size: int,
    ) -> None:
        """Stream file from S3 to ZIP using chunked reads."""
        s3_response = self.s3_client.get_object(Bucket=bucket, Key=file_key)
        body_stream = s3_response["Body"]

        with zipf.open(file_name, "w") as zip_entry:
            while True:
                chunk = body_stream.read(chunk_size)
                if not chunk:
                    break
                zip_entry.write(chunk)

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

    def _download_and_zip_in_batches(
        self,
        bucket: str,
        prefix: str,
        batch_dir: str,
        output_zip: str,
        batch_size_bytes: int,
    ) -> bool:
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
            True if any files were processed, False otherwise
        """
        s3_resource = boto3.resource("s3")
        objects = list(s3_resource.Bucket(bucket).objects.filter(Prefix=prefix))

        if not objects:
            return False

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

        # Process each batch: stream from S3 directly to ZIP using chunked reads
        zip_mode: Literal["x", "a"] = "x"  # Create new file for first batch
        chunk_size = 8 * 1024 * 1024  # 8MB chunks for constant memory usage

        for batch_idx, batch in enumerate(batches):
            batch_size_mb = sum(obj.size for obj in batch) / (1024 * 1024)
            logger.info(
                f"Processing batch {batch_idx + 1}/{len(batches)} with {len(batch)} files ({batch_size_mb:.2f} MB)"
            )

            # Stream files from S3 directly into ZIP using chunked reads
            with zipfile.ZipFile(output_zip, zip_mode, zipfile.ZIP_DEFLATED) as zipf:
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
                            logger.info(
                                f"Adding {file_name} ({obj.size / (1024**2):.2f} MB) to ZIP from local file "
                                f"(already downloaded for schema computation)"
                            )
                            self._stream_file_to_zip_from_local(
                                sample_file_path, zipf, file_name, chunk_size
                            )
                            first_obj_processed = True
                        else:
                            # Stream from S3 using chunked reads for constant memory usage
                            logger.info(
                                f"Streaming {file_name} ({obj.size / (1024**2):.2f} MB) from S3 using chunked reads"
                            )
                            self._stream_file_to_zip_from_s3(
                                bucket, file_key, zipf, file_name, chunk_size
                            )

                        logger.info(f"Added {file_name} to ZIP file")

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

            # After first batch, switch to append mode for subsequent batches
            zip_mode = "a"

            logger.info(
                f"Batch {batch_idx + 1}/{len(batches)} complete, streamed {len(batch)} files"
            )

        total_size_mb = sum(obj.size for obj in objects) / (1024 * 1024)
        logger.info(
            f"Successfully streamed all {len(objects)} files ({total_size_mb:.2f} MB) across {len(batches)} batches"
        )
        return True

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        for file in self.opened_files:
            os.remove(file)
        return super().close()
