import logging
from concurrent import futures
from typing import Dict, Iterable, List

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import RemovedStatusFilter
from datahub.ingestion.source.datahub.config import DataHubSourceConfig
from datahub.ingestion.source.datahub.report import DataHubSourceReport
from datahub.metadata.schema_classes import _Aspect

logger = logging.getLogger(__name__)

# Should work for at least mysql, mariadb, postgres
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


class DataHubApiReader:
    def __init__(
        self,
        config: DataHubSourceConfig,
        report: DataHubSourceReport,
        graph: DataHubGraph,
    ):
        self.config = config
        self.report = report
        self.graph = graph

    def get_aspects(self) -> Iterable[MetadataChangeProposalWrapper]:
        urns = self.graph.get_urns_by_filter(
            status=RemovedStatusFilter.ALL,
            batch_size=self.config.database_query_batch_size,
        )
        tasks: List[futures.Future[Iterable[MetadataChangeProposalWrapper]]] = []
        with futures.ThreadPoolExecutor(
            max_workers=self.config.max_workers
        ) as executor:
            for urn in urns:
                tasks.append(executor.submit(self._get_aspects_for_urn, urn))
            for task in futures.as_completed(tasks):
                yield from task.result()

    def _get_aspects_for_urn(self, urn: str) -> Iterable[MetadataChangeProposalWrapper]:
        aspects: Dict[str, _Aspect] = self.graph.get_entity_semityped(urn)  # type: ignore
        for aspect in aspects.values():
            yield MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=aspect,
            )
