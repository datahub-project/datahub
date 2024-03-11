import logging
import os
from dataclasses import dataclass
from typing import List, Optional

import boto3
from opensearchpy import OpenSearch

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.graph.client import DatahubClientConfig
from datahub.ingestion.source.datahub_reporting.datahub_dataset import (
    BaseModelRow,
    DataHubBasedS3Dataset,
    DatasetMetadata,
    FileStoreBackedDatasetConfig,
)

logger = logging.getLogger(__name__)


class ElasticSearchClientConfig(ConfigModel):
    host: str
    port: Optional[int] = None
    use_ssl: bool = False
    verify_certs: bool = False
    ca_certs: Optional[str] = None
    client_cert: Optional[str] = None
    client_key: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    cluster_name: Optional[str] = None


class DataHubReportingExtractGraphSourceConfig(ConfigModel):
    server: Optional[DatahubClientConfig] = None
    search_index: Optional[ElasticSearchClientConfig] = None
    extract_graph_store: FileStoreBackedDatasetConfig
    relationships_include: Optional[List[str]] = None
    relationships_exclude: Optional[List[str]] = None
    entity_types_include: Optional[List[str]] = None
    entity_types_exclude: Optional[List[str]] = None


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


class ElasticGraphRow(BaseModelRow):
    source_urn: str
    source_entity_type: str
    destination_urn: str
    destination_entity_type: str
    relationship_type: str
    created_on: int
    created_by: str
    updated_on: int
    updated_by: str

    @classmethod
    def from_elastic_doc(cls, doc):
        return cls(
            source_urn=doc["source"]["urn"],
            source_entity_type=doc["source"]["entityType"],
            destination_urn=doc["destination"]["urn"],
            destination_entity_type=doc["destination"]["entityType"],
            relationship_type=doc["relationshipType"],
            created_on=doc["createdOn"],
            created_by=doc["createdActor"],
            updated_on=doc["updatedOn"],
            updated_by=doc["updatedActor"],
        )


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

    def get_workunits(self):

        endpoint = ""
        if self.config.search_index:
            if self.config.search_index.host and not self.config.search_index.port:
                endpoint = f"{self.config.search_index.host}"
            elif self.config.search_index.host and self.config.search_index.port:
                endpoint = (
                    f"{self.config.search_index.host}:{self.config.search_index.port}"
                )

        deployment = (
            self.config.search_index.cluster_name if self.config.search_index else ""
        )
        user = ""
        password = ""
        server = OpenSearch([endpoint], http_auth=(user, password))
        # [
        #     "https://vpc-use1-saas-prod-etsy-es-01-2oxpbdaulqke65nu24urq3lgoy.us-east-1.es.amazonaws.com"
        # ],
        # http_auth=("<user>", "<password>"),

        query = {"query": {"match_all": {}}, "sort": [{"createdOn": {"order": "asc"}}]}

        index_prefix = f"{deployment}_" if deployment else ""

        index = f"{index_prefix}graph_service_v1"
        response = server.create_pit(index, keep_alive="10m")

        # TODO: Save PIT, we can resume processing based on <pit, search_after> tuple
        pit = response.get("pit_id")
        query.update({"pit": {"id": pit, "keep_alive": "10m"}})

        # TODO: Using slicing we can parallelize the ES calls below:
        # https://opensearch.org/docs/latest/search-plugins/searching-data/point-in-time/#search-slicing
        while True:
            results = server.search(body=query, size=10000)  # batch of data
            self.process_batch(results["hits"]["hits"])
            if len(results["hits"]["hits"]) < 10000:
                break
            query.update({"search_after": results["hits"]["hits"][-1]["sort"]})

        response = server.delete_pit(body={"pit_id": pit})

        self.datahub_based_s3_dataset.commit()

        # If form analytics config is not present, use the default reporting dataset name
        # dataset_urn = make_dataset_urn("acryl", self.config)

        for mcp in self.datahub_based_s3_dataset.commit():
            self.report.file_store_uri = self.datahub_based_s3_dataset.get_file_uri()
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
