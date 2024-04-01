import logging
import os
from datetime import datetime
from dataclasses import dataclass
from typing import List, Optional

import boto3
from pydantic import validator

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
from datahub.ingestion.source.datahub_reporting.datahub_dataset import (
    DataHubBasedS3Dataset,
    DatasetMetadata,
    DatasetRegistrationSpec,
    FileStoreBackedDatasetConfig,
)
from datahub.metadata.schema_classes import DatasetPropertiesClass

logger = logging.getLogger(__name__)

class S3ClientConfig(ConfigModel):
    bucket: str = os.getenv("DATA_BUCKET", "localhost")
    path: str = os.getenv("ELASTICSEARCH_HOST", "localhost")

class DataHubReportingExtractSqlSourceConfig(ConfigModel):
    server: Optional[DatahubClientConfig] = None
    sql_backup_config: S3ClientConfig
    extract_sql_store: FileStoreBackedDatasetConfig

    @validator("extract_sql_store", pre=True, always=True)
    def set_default_extract_soft_delete_flag(cls, v, values):
        if v is not None:
            if "dataset_registration_spec" not in v:
                v["dataset_registration_spec"] = DatasetRegistrationSpec(
                    soft_deleted=False
                )
            elif "soft_deleted" not in v["dataset_registration_spec"]:
                v["dataset_registration_spec"]["soft_deleted"] = False
        return v

"""
@dataclass
class DataHubReportingExtractSQLSourceReport(SourceReport):
    edges_scanned: int = 0
    edges_skipped: int = 0
    file_store_connection_status: str = "Not connected"
    file_store_uri: str = ""

    def increment_edges_scanned(self):
        self.edges_scanned += 1

    def increment_edges_skipped(self):
        self.edges_skipped += 1

    def as_string(self) -> str:
        return super().as_string()
"""


@platform_name(id="datahub", platform_name="DataHub")
@config_class(DataHubReportingExtractSqlSourceConfig)
@support_status(SupportStatus.INCUBATING)
class DataHubReportingExtractSQLSource(Source):
    platform = "datahub"

    def __init__(
            self, config: DataHubReportingExtractSqlSourceConfig, ctx: PipelineContext
    ):
        super().__init__(ctx)
        self.config: DataHubReportingExtractSqlSourceConfig = config
        self.report = SourceReport()
        self.opened_files: List[str] = []
        self.s3_client = boto3.client("s3")
        self.datahub_based_s3_dataset = DataHubBasedS3Dataset(
            self.config.extract_sql_store,
            dataset_metadata=DatasetMetadata(
                displayName="SQL Extract",
                description="This is an automated SQL Extract from DataHub's backend",
            ),
        )

    @staticmethod
    def should_skip_extract(
            self, dataset_properties: Optional[DatasetPropertiesClass]
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
            # TODO: IMPLEMENT


        return None

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        for file in self.opened_files:
            os.remove(file)
        return super().close()