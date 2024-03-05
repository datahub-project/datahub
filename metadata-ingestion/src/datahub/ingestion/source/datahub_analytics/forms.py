import datetime
import logging
import os
import pathlib
import tempfile
import time
from typing import Iterable, List, Optional

import boto3
import pandas as pd
from pydantic import BaseModel

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.datahub_analytics.datahub_form_reporting import (
    DataHubFormReportingData,
)
from datahub.ingestion.source.datahub_analytics.forms_config import (
    DataHubAnalyticsFormSourceConfig,
    DataHubAnalyticsFormSourceReport,
    PartitioningStrategy,
)
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    OperationClass,
    OperationTypeClass,
    StatusClass,
)

logger = logging.getLogger(__name__)


class FormAnalyticsConfig(BaseModel):
    enabled: bool
    dataset_urn: Optional[str]
    dataset_uri: Optional[str]


@platform_name(id="datahub", platform_name="DataHub")
@config_class(DataHubAnalyticsFormSourceConfig)
@support_status(SupportStatus.INCUBATING)
class DataHubAnalyticsFormsSource(Source):
    platform = "datahub"

    def __init__(self, config: DataHubAnalyticsFormSourceConfig, ctx: PipelineContext):
        self.config: DataHubAnalyticsFormSourceConfig = config
        self.report = DataHubAnalyticsFormSourceReport()
        self.opened_files: List[str] = []

    def get_reporting_config(self) -> Optional[FormAnalyticsConfig]:
        form_config_query = """
            query {
                formAnalyticsConfig {
                    enabled
                    datasetUrn
                    datasetUri
                 }
            }
            """
        query_result = self.graph.execute_graphql(query=form_config_query)
        if query_result:
            if query_result.get("formAnalyticsConfig", {}).get("enabled") is False:
                return FormAnalyticsConfig(enabled=False)
            return FormAnalyticsConfig(
                enabled=True,
                dataset_urn=query_result.get("formAnalyticsConfig", {}).get(
                    "datasetUrn"
                ),
                dataset_uri=query_result.get("formAnalyticsConfig", {}).get(
                    "datasetUri"
                ),
            )
        else:
            return None

    def create_s3_parquet_file(
        self,
        force_create: bool,
        s3_uri: str,
        df: pd.DataFrame,
        report: DataHubAnalyticsFormSourceReport,
    ) -> str:
        # first check if the file exists, if not skip
        # use boto3 to check if the file exists
        bucket, key = s3_uri.replace("s3://", "").split("/", 1)
        s3 = boto3.client("s3")

        if not force_create:
            # check if the file exists and return early if it does
            try:
                # first split the s3_uri to get the bucket and key
                s3.head_object(Bucket=bucket, Key=key)
                print("File already exists")
                report.files_created = 0
                return s3_uri
            except Exception as e:
                logger.error(f"Failed to check if file exists due to {e}")
                report.reporting_store_connection_status = (
                    f"Failed to connect to S3. due to {e}"
                )
                pass

        # create a temp dir and a file in the temp dir
        temp_path = tempfile.mkdtemp()
        file_path = f"{temp_path}/{bucket}/{key}"
        pathlib.Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        # write to a local parquet file
        df.to_parquet(file_path, compression=self.config.reporting_file_compression)
        self.report.rows_created = df.shape[0]
        self.opened_files.append(file_path)
        print(f"Created file {file_path}")
        s3.upload_file(file_path, bucket, key)
        print(f"Created file {s3_uri}")
        report.files_created = 1
        report.reporting_store_connection_status = "OK"
        return s3_uri

    def _register_reporting_dataset(
        self, dataset_urn: str, physical_uri: str, num_rows: int
    ) -> Iterable[MetadataChangeProposalWrapper]:
        dataset_properties = DatasetPropertiesClass(
            name="Forms Reporting Dataset",
            description="A dataset for storing forms reporting data for analytics.",
            externalUrl=physical_uri,
            uri=None,  # Unfortunately, there is a bug in URI validation that requires this to be None
        )
        current_time_millis = int(time.time()) * 1000
        mcps = MetadataChangeProposalWrapper.construct_many(
            entityUrn=dataset_urn,
            aspects=[
                dataset_properties,
                StatusClass(
                    removed=True  # Registering this is as a soft deleted asset to ensure it doesn't show up in search
                ),
                OperationClass(
                    timestampMillis=current_time_millis,
                    operationType=OperationTypeClass.UPDATE,
                    lastUpdatedTimestamp=current_time_millis,
                    numAffectedRows=num_rows if num_rows else None,
                ),
            ],
        )
        yield from mcps

    def get_reporting_file_uri(
        self, reporting_dataset_uri_prefix: str, date: Optional[datetime.date] = None
    ) -> str:
        if (
            self.config.reporting_snapshot_partitioning_strategy
            == PartitioningStrategy.DATE
        ):
            assert date is not None
            return f"{reporting_dataset_uri_prefix.rstrip('/')}/{date.strftime('%Y-%m-%d')}/{self.config.reporting_file_name}.{self.config.reporting_file_extension}"
        elif (
            self.config.reporting_snapshot_partitioning_strategy
            == PartitioningStrategy.SNAPSHOT
        ):
            return f"{reporting_dataset_uri_prefix.rstrip('/')}/{self.config.reporting_file_name}.{self.config.reporting_file_extension}"
        else:
            raise ValueError(
                f"Unsupported partitioning strategy: {self.config.reporting_snapshot_partitioning_strategy}"
            )

    def get_workunits(self):
        self.graph = DataHubGraph(config=self.config.server)
        form_analytics_config = self.get_reporting_config()

        if form_analytics_config and not form_analytics_config.enabled:
            logger.error("Form analytics is not enabled. Skipping reporting.")
            self.report.feature_enabled = False
            return

        form_data = DataHubFormReportingData(self.graph, self.config.allowed_forms)
        # If form analytics config is not present, use the default reporting bucket prefix
        dataset_uri_prefix = (
            form_analytics_config.dataset_uri
            if form_analytics_config and form_analytics_config.dataset_uri
            else self.config.reporting_bucket_prefix
        )

        dataset_uri = self.get_reporting_file_uri(
            dataset_uri_prefix, date=datetime.date.today()
        )
        assert dataset_uri.startswith("s3://"), "Reporting URI must be an S3 URI"

        reporting_df = form_data.get_dataframe(
            lambda x: self.report.increment_assets_scanned(),
            lambda x: self.report.increment_forms_scanned(),
        )

        s3_uri = self.create_s3_parquet_file(
            self.config.reporting_file_overwrite_existing,
            dataset_uri,
            reporting_df,
            self.report,
        )

        # If form analytics config is not present, use the default reporting dataset name
        dataset_urn = (
            form_analytics_config.dataset_urn
            if form_analytics_config and form_analytics_config.dataset_urn
            else make_dataset_urn("datahub", self.config.reporting_dataset_name)
        )
        for mcp in self._register_reporting_dataset(
            dataset_urn, s3_uri, reporting_df.shape[0]
        ):
            self.report.reporting_store_file_uri = s3_uri
            logger.info(f"Reporting file created at {s3_uri}")
            logger.info(f"Reporting dataset registered at {dataset_urn}")
            yield mcp.as_workunit()

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        for file in self.opened_files:
            os.remove(file)
        return super().close()
