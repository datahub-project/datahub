import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from typing import Iterable, List, Optional, Set, Dict

from datahub.configuration import ConfigModel
from pydantic import Field

from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_user_urn,
)
from datahub.emitter.sql_parsing_builder import SqlParsingBuilder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    MetadataWorkUnitProcessor,
    Source,
    SourceCapability,
    SourceReport,
)
from datahub.ingestion.api.source_helpers import auto_workunit_reporter
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage

logger = logging.getLogger(__name__)

DATAJOB_QUERY = """
query listDataJobs($query:String!, $scrollId: String) {
  scrollAcrossEntities(input: { types: [DATA_JOB], query: $query, count: 10, scrollId: $scrollId}) {
    nextScrollId
    count
    searchResults {
      entity {
        type
        ... on DataJob {
          urn
          type
        }
      }
    }
  }
}
"""

DATA_PROCESS_INSTANCES_QUERY = """
query getDataJobRuns($dataJobUrn: String!, $start: Int!, $count: Int!) {
  dataJob(urn: $dataJobUrn) {
    runs(start: $start, count: $count) {
      runs {
        created {
          time
          actor
        }
        urn
      }
      #...runResults
      
      __typename
    }
    __typename
  }
}
"""

class MetadataCleanupConfig(ConfigModel):
    retention_days: Optional[int] = Field(
        10,
        description="Number of days to retain metadata in DataHub",
    )

    aspects_to_clean: List[str] = Field(
        ["DataprocessInstance"],
        description="List of aspect names to clean up",
    )

    keep_last_n: Optional[int] = Field(
        5,
        description="Number of latest aspects to keep",
    )


class MetadataCleanupSourceReport(SourceReport):
    num_aspects_removed: int = 0
    num_aspect_removed_by_type: Dict[str, int] = {}

@platform_name("Metadata Cleanup")
@config_class(MetadataCleanupConfig)
@support_status(SupportStatus.INCUBATING)
class MetadataCleanupSource(Source):
    """
    This source clean up aspects.

    """

    def __init__(self, ctx: PipelineContext, config: MetadataCleanupConfig):
        if not ctx.graph:
            raise ValueError(
                "MetadataCleanupSource needs a datahub_api"
            )

        self.graph: DataHubGraph = ctx.graph
        self.ctx = ctx
        self.config = config
        self.report = MetadataCleanupSourceReport()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "MetadataCleanupSource":
        config = MetadataCleanupConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_report(self) -> MetadataCleanupSourceReport:
        return self.report

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [partial(auto_workunit_reporter, self.get_report())]

    def delete_dpi_from_datajobs(self, job_urn: str):
        dpis:List = []
        start:int = 0
        count:int = 1000
        while True:
            job_query_result = self.ctx.graph.execute_graphql(DATA_PROCESS_INSTANCES_QUERY, {"dataJobUrn": job_urn, "start": start, "count": count})
            runs = job_query_result.get("dataJob").get("runs").get("runs")
            for dpi in runs:
                dpis.append(dpi)

            start += count
            logger.info(f"Collected {len(dpis)} DPIs from {job_urn}")
            if len(runs) < count:
                break


        dpis.sort(key=lambda x: x['created']['time'], reverse=True)
        if len(dpis) <= self.config.keep_last_n:
            return

        deleted_count_last_n = 0
        if self.config.keep_last_n:
            for dpi in dpis[self.config.keep_last_n:]:
                deleted_count_last_n += 1
                self.report.num_aspects_removed += 1
                self.report.num_aspect_removed_by_type["dataProcessInstance"] = self.report.num_aspect_removed_by_type.get("dataProcessInstance", 0) + 1
                self.ctx.graph.delete_entity(dpi['urn'], True)
                if deleted_count_last_n % count == 0:
                    logger.info(f"Deleted {deleted_count_last_n} DPIs from {job_urn}")

        deleted_count_retention = 0
        if self.config.retention_days:
            retention_time = datetime.now(timezone.utc).timestamp() - self.config.retention_days * 24 * 60 * 60
            for dpi in dpis:
                if dpi['created']['time'] < retention_time * 1000:
                    deleted_count_retention += 1
                    self.report.num_aspects_removed += 1
                    self.report.num_aspect_removed_by_type["dataProcessInstance"] = self.report.num_aspect_removed_by_type.get("dataProcessInstance", 0) + 1
                    self.ctx.graph.delete_entity(dpi['urn'], True)
                    if deleted_count_retention % count == 0:
                        logger.info(f"Deleted {deleted_count_retention} DPIs from {job_urn} due to retention")

        logger.info(f"Deleted {deleted_count_retention} DPIs from {job_urn} due to retention")

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:

        scroll_id:Optional[str] = None

        while True:
            result = self.ctx.graph.execute_graphql(DATAJOB_QUERY, {"query": "*", scroll_id: scroll_id if scroll_id else "null"})
            first = False
            scroll_id = result.get("scrollAcrossEntities").get("nextScrollId")
            for job in result.get("scrollAcrossEntities").get("searchResults"):
                job_urn = job.get("entity").get("urn")
                self.delete_dpi_from_datajobs(job_urn)
            if not scroll_id:
                break

        return []
