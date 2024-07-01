import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

import boto3
from opensearchpy import OpenSearch
from pydantic import validator

from acryl_datahub_cloud.datahub_reporting.datahub_dataset import (
    DataHubBasedS3Dataset,
    DatasetMetadata,
    DatasetRegistrationSpec,
    FileStoreBackedDatasetConfig,
)
from acryl_datahub_cloud.elasticsearch.config import ElasticSearchClientConfig
from acryl_datahub_cloud.elasticsearch.graph_service import ElasticGraphRow
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


class DataHubReportingExtractGraphSourceConfig(ConfigModel):
    server: Optional[DatahubClientConfig] = None
    search_index: ElasticSearchClientConfig = ElasticSearchClientConfig()
    extract_graph_store: FileStoreBackedDatasetConfig
    relationships_include: Optional[List[str]] = None
    relationships_exclude: Optional[List[str]] = None
    entity_types_include: Optional[List[str]] = None
    entity_types_exclude: Optional[List[str]] = None
    query_timeout: int = 30
    extract_batch_size: int = 2000

    @validator("extract_graph_store", pre=True, always=True)
    def set_default_extract_soft_delete_flag(cls, v, values):
        if v is not None:
            if "dataset_registration_spec" not in v:
                v["dataset_registration_spec"] = DatasetRegistrationSpec(
                    soft_deleted=False
                )
            elif "soft_deleted" not in v["dataset_registration_spec"]:
                v["dataset_registration_spec"]["soft_deleted"] = False
        return v


@dataclass
class DataHubReportingExtractGraphSourceReport(SourceReport):
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


@platform_name(id="datahub", platform_name="DataHub")
@config_class(DataHubReportingExtractGraphSourceConfig)
@support_status(SupportStatus.INCUBATING)
class DataHubReportingExtractGraphSource(Source):
    platform = "datahub"

    def __init__(
        self, config: DataHubReportingExtractGraphSourceConfig, ctx: PipelineContext
    ):
        super().__init__(ctx)
        self.config: DataHubReportingExtractGraphSourceConfig = config
        self.report = DataHubReportingExtractGraphSourceReport()
        self.opened_files: List[str] = []
        self.s3_client = boto3.client("s3")
        self.datahub_based_s3_dataset = DataHubBasedS3Dataset(
            self.config.extract_graph_store,
            dataset_metadata=DatasetMetadata(
                displayName="Graph Extract",
                description="This is an automated Graph Extract from DataHub's backend",
            ),
        )

    def process_batch(self, results):
        for doc in results:
            row = ElasticGraphRow.from_elastic_doc(doc["_source"])
            self.report.increment_edges_scanned()
            self.datahub_based_s3_dataset.append(row)

    def should_skip_graph_extract(
        self, dataset_properties: Optional[DatasetPropertiesClass]
    ) -> bool:
        """Skip graph extraction if the dataset has already been pushed to DataHub today"""

        skip_extract = False
        # If present check if the updated timestamp is for the current day
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
            self.should_skip_graph_extract(dataset_properties)
            and self.datahub_based_s3_dataset.config.generate_presigned_url
        ):
            mcps = self.datahub_based_s3_dataset.update_presigned_url(
                dataset_properties=dataset_properties
            )
            for mcp in mcps:
                yield mcp.as_workunit()
        else:
            endpoint = self.config.search_index.endpoint
            index_prefix = self.config.search_index.index_prefix
            user = self.config.search_index.username
            password = self.config.search_index.password
            batch_size = self.config.extract_batch_size
            server = OpenSearch(
                [endpoint],
                http_auth=(user, password),
                use_ssl=self.config.search_index and self.config.search_index.use_ssl,
            )

            query = {
                "query": {"match_all": {}},
                "sort": [
                    {"source.urn": {"order": "desc"}},
                    {"destination.urn": {"order": "desc"}},
                    {"relationshipType": {"order": "desc"}},
                    {"lifecycleOwner": {"order": "desc"}},
                ],
            }

            index = f"{index_prefix}_graph_service_v1"
            response = server.create_pit(index, keep_alive="10m")

            # TODO: Save PIT, we can resume processing based on <pit, search_after> tuple
            pit = response.get("pit_id")
            query.update({"pit": {"id": pit, "keep_alive": "10m"}})

            # TODO: Using slicing we can parallelize the ES calls below:
            # https://opensearch.org/docs/latest/search-plugins/searching-data/point-in-time/#search-slicing
            while True:
                results = server.search(
                    body=query,
                    size=batch_size,
                    params={"timeout": self.config.query_timeout},
                )
                self.process_batch(results["hits"]["hits"])
                if len(results["hits"]["hits"]) < batch_size:
                    break
                query.update({"search_after": results["hits"]["hits"][-1]["sort"]})

            response = server.delete_pit(body={"pit_id": pit})

            self.datahub_based_s3_dataset.commit()

            for mcp in self.datahub_based_s3_dataset.commit():
                self.report.file_store_uri = (
                    self.datahub_based_s3_dataset.get_file_uri()
                )
                logger.info(f"Reporting file created at {self.report.file_store_uri}")
                logger.info(
                    f"Reporting dataset registered at {self.datahub_based_s3_dataset.get_dataset_urn()}"
                )
                yield mcp.as_workunit()

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        for file in self.opened_files:
            os.remove(file)
        return super().close()
