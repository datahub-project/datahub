import logging
import os
import shutil
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Optional

import boto3
from pydantic import validator

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
    server: Optional[DatahubClientConfig] = None
    sql_backup_config: S3ClientConfig
    extract_sql_store: FileStoreBackedDatasetConfig

    @validator("extract_sql_store", pre=True, always=True)
    def set_default_extract_soft_delete_flag(cls, v):
        if v is not None:
            if "dataset_registration_spec" not in v:
                v["dataset_registration_spec"] = DatasetRegistrationSpec(
                    soft_deleted=False
                )
            elif "soft_deleted" not in v["dataset_registration_spec"]:
                v["dataset_registration_spec"]["soft_deleted"] = False

            if "file" not in v:
                default_config = FileStoreBackedDatasetConfig.dummy()
                v["file"] = (
                    f"{default_config.file_name}.{default_config.file_extension}"
                )
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
                    f"Skipping graph extract as dataset has been updated today {ts}"
                )

        return skip_extract

    def get_workunits(self):
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

            files_downloaded: bool = self._download_files(
                bucket=self.config.sql_backup_config.bucket,
                prefix=bucket_prefix,
                target_dir=f"{tmp_dir}/download/",
            )
            if not files_downloaded:
                logger.warning(f"Skipping as no files were found in {bucket_prefix}")
                return

            self._zip_folder(
                folder_path=f"{tmp_dir}/download",
                output_file=f"{tmp_dir}/{output_file}",
            )

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

    def _download_files(self, bucket: str, prefix: str, target_dir: str) -> bool:
        objects = boto3.resource("s3").Bucket(bucket).objects.filter(Prefix=prefix)

        files_downloaded = False

        # Iterate over objects in the time partition path
        for obj in objects:
            # Extract file key
            file_key = obj.key

            # Generate local file path
            local_file_path = os.path.join(
                os.getcwd(), target_dir, os.path.basename(file_key)
            )

            logger.info(f"Downloading s3://{bucket}/{file_key} to {local_file_path}")

            # Download file from S3
            self.s3_client.download_file(bucket, file_key, local_file_path)

            files_downloaded = True

        return files_downloaded

    @staticmethod
    def _zip_folder(folder_path: str, output_file: str) -> None:
        logger.info(f"Zipping {folder_path} to {output_file}")
        with zipfile.ZipFile(output_file, "x", zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(folder_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    logger.info(f"Adding {file_path} to ZIP file")
                    # Add file to zip archive with relative path
                    zipf.write(file_path, os.path.relpath(file_path, folder_path))

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        for file in self.opened_files:
            os.remove(file)
        return super().close()
