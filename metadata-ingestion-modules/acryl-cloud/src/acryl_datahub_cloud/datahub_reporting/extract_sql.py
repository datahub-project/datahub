import logging
import os
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import boto3
from pydantic import validator

from acryl_datahub_cloud.datahub_reporting.datahub_dataset import (
    BaseModelRow,
    DataHubBasedS3Dataset,
    DatasetMetadata,
    DatasetRegistrationSpec,
    FileStoreBackedDatasetConfig,
)
from datahub.configuration.common import ConfigModel
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
                v[
                    "file"
                ] = f"{default_config.file_name}.{default_config.file_extension}"
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

    @classmethod
    def from_sql_doc(cls, doc):
        return cls(
            urn=doc["urn"],
            aspect=doc["aspect"],
            version=doc["version"],
            metadata=doc["metadata"],
            systemmetadata=doc["systemmetadata"],
            createdon=doc["createdon"],
            createdby=doc["createdby"],
            createdfor=doc["createdfor"],
        )


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

        if (
            self.should_skip_extract(dataset_properties)
            and self.datahub_based_s3_dataset.config.generate_presigned_url
        ):
            mcps = self.datahub_based_s3_dataset.update_presigned_url(
                dataset_properties=dataset_properties
            )
            for mcp in mcps:
                yield mcp.as_workunit()
        else:
            # Get yesterday's RDS data dump.
            previous_date = datetime.now() - timedelta(days=1)
            # Must be a static path, so we consistently delete the data (possibly based on pipeline name)
            tmp_dir = self.ctx.pipeline_name if self.ctx.pipeline_name else "default"

            time_partition_path = "year={}/month={:02d}/day={:02d}".format(
                previous_date.year, previous_date.month, previous_date.day
            )
            output_file = self.datahub_based_s3_dataset.config.file
            self._clean_up_old_state(
                state_directory=tmp_dir, result_file_path=output_file
            )
            self._download_files(
                bucket=self.config.sql_backup_config.bucket,
                prefix=f"{self.config.sql_backup_config.path}/{time_partition_path}",
                target_dir=tmp_dir,
            )
            self._zip_folder(folder_path=tmp_dir, output_file=output_file)
            logger.warning(self.datahub_based_s3_dataset.dataset_metadata)
            mcps = self.datahub_based_s3_dataset.commit()
            self._clean_up_old_state(
                state_directory=tmp_dir, result_file_path=output_file
            )

            for mcp in mcps:
                logger.info(
                    f"Reporting dataset registered at {self.datahub_based_s3_dataset.get_dataset_urn()}"
                )
                yield mcp.as_workunit()

    @staticmethod
    def _clean_up_old_state(
        state_directory: str, result_file_path: Optional[str] = None
    ) -> None:
        path = Path(state_directory)
        path.mkdir(parents=True, exist_ok=True)
        files = os.listdir(state_directory)
        if files:
            logger.info("Some files found in the state directory, cleaning it")
            for file in files:
                file_path = os.path.join(state_directory, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
        if result_file_path and os.path.isfile(result_file_path):
            os.remove(result_file_path)

    def _download_files(self, bucket: str, prefix: str, target_dir: str) -> None:
        objects = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if "Contents" in objects:
            # Iterate over objects in the time partition path
            for obj in objects["Contents"]:
                # Extract file key
                file_key = obj["Key"]

                # Generate local file path
                local_file_path = os.path.join(
                    os.getcwd(), target_dir, os.path.basename(file_key)
                )

                # Download file from S3
                self.s3_client.download_file(bucket, file_key, local_file_path)
                # print(f"Downloaded {file_key} to {local_file_path}")
        else:
            logger.warning(f"No objects found in {prefix}")

    @staticmethod
    def _zip_folder(folder_path: str, output_file: str) -> None:
        with zipfile.ZipFile(output_file, "x", zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(folder_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    # Add file to zip archive with relative path
                    zipf.write(file_path, os.path.relpath(file_path, folder_path))

    def _upload_file(self, bucket: str, prefix: str, file: str) -> None:
        self.s3_client.upload_file(file, bucket, prefix)

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        for file in self.opened_files:
            os.remove(file)
        return super().close()
