import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Set

from opensearchpy import OpenSearch

from acryl_datahub_cloud.elasticsearch.config import ElasticSearchClientConfig
from acryl_datahub_cloud.elasticsearch.graph_service import ElasticGraphRow
from datahub.configuration import ConfigModel
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
from datahub.metadata.schema_classes import AuditStampClass, LineageFeaturesClass

logger = logging.getLogger(__name__)

SYSTEM_ACTOR = "urn:li:corpuser:__datahub_system"


class LineageFeaturesSourceConfig(ConfigModel):
    search_index: ElasticSearchClientConfig = ElasticSearchClientConfig()
    query_timeout: int = 30
    extract_batch_size: int = 2000


@dataclass
class LineageExtractGraphSourceReport(SourceReport):
    edges_scanned: int = 0


@platform_name(id="datahub", platform_name="DataHub")
@config_class(LineageFeaturesSourceConfig)
@support_status(SupportStatus.INCUBATING)
class DataHubLineageFeaturesSource(Source):
    platform = "datahub"

    def __init__(self, config: LineageFeaturesSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config: LineageFeaturesSourceConfig = config
        self.report = LineageExtractGraphSourceReport()
        self.opened_files: List[str] = []

        self.valid_urns: Set[str] = set()
        self.upstream_counts: Dict[str, int] = defaultdict(int)
        self.downstream_counts: Dict[str, int] = defaultdict(int)

    def process_batch(self, results: Iterable[dict]) -> None:
        for doc in results:
            row = ElasticGraphRow.from_elastic_doc(doc["_source"])
            self.report.edges_scanned += 1
            if (
                row.source_urn in self.valid_urns
                and row.destination_urn in self.valid_urns
            ):
                self.upstream_counts[row.source_urn] += 1
                self.downstream_counts[row.destination_urn] += 1

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        graph = self.ctx.require_graph("Load non soft-deleted urns")
        for urn in graph.get_urns_by_filter(batch_size=self.config.extract_batch_size):
            self.valid_urns.add(urn)

        timestamp = datetime.now(tz=timezone.utc)
        server = OpenSearch(
            [self.config.search_index.endpoint],
            http_auth=(
                self.config.search_index.username,
                self.config.search_index.password,
            ),
            use_ssl=self.config.search_index.use_ssl,
        )

        query = {
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"source.entityType": "schemaField"}},
                        {"term": {"destination.entityType": "schemaField"}},
                        {"term": {"relationshipType": "DownstreamOf"}},
                    ],
                }
            },
            "sort": [
                {"source.urn": {"order": "desc"}},
                {"destination.urn": {"order": "desc"}},
                {"lifecycleOwner": {"order": "desc"}},
            ],
        }

        index = f"{self.config.search_index.index_prefix}graph_service_v1"
        response = server.create_pit(index, keep_alive="10m")

        # TODO: Save PIT, we can resume processing based on <pit, search_after> tuple
        pit = response.get("pit_id")
        query.update({"pit": {"id": pit, "keep_alive": "10m"}})

        # TODO: Using slicing we can parallelize the ES calls below:
        # https://opensearch.org/docs/latest/search-plugins/searching-data/point-in-time/#search-slicing
        batch_size = self.config.extract_batch_size
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

        server.delete_pit(body={"pit_id": pit})

        # In Python 3.9, can be replaced by `self.self.upstream_counts.keys() | self.downstream_counts.keys()`
        for urn in set(self.upstream_counts.keys()).union(
            self.downstream_counts.keys()
        ):
            print(urn, self.upstream_counts[urn], self.downstream_counts[urn])
            yield MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=LineageFeaturesClass(
                    upstreamCount=self.upstream_counts[urn],
                    downstreamCount=self.downstream_counts[urn],
                    computedAt=AuditStampClass(
                        time=int(timestamp.timestamp() * 1000),
                        actor=SYSTEM_ACTOR,
                    ),
                ),
            ).as_workunit()

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        for file in self.opened_files:
            os.remove(file)
        return super().close()
