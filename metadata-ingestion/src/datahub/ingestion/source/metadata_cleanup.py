import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import partial
from typing import Dict, Iterable, List, Optional

from pydantic import Field

from datahub.configuration import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, Source, SourceReport
from datahub.ingestion.api.source_helpers import auto_workunit_reporter
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.stats_collections import TopKDict

logger = logging.getLogger(__name__)

DATAJOB_QUERY = """
query listDataJobs($query:String!, $scrollId: String) {
  scrollAcrossEntities(input: { types: [DATA_JOB
    ], query: $query, count: 10, scrollId: $scrollId
  }) {
    nextScrollId
    count
    searchResults {
      entity {
        type
        ... on DataJob {
          urn
          dataFlow {
            urn
          }
          type
          lastIngested
          subTypes {
            typeNames
          }
          jobId
          dataPlatformInstance {
            urn
          }
         runs {
            total
          }
        }
      }
    }
  }
}
"""

DATAFLOW_QUERY = """
query listDataFlows($query:String!, $scrollId: String) {
  scrollAcrossEntities(input: { types: [DATA_FLOW], query: $query, count: 10, scrollId: $scrollId}) {
    nextScrollId
    count
    searchResults {
      entity {
        type
        ... on DataFlow {
            urn
            type
            orchestrator
            cluster
            lastIngested
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
      #...runResult

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

    delete_empty_data_jobs: bool = Field(
        True, description="Wether to delete Data Jobs without runs"
    )

    delete_empty_data_flows: bool = Field(
        True, description="Wether to delete Data Flows without runs"
    )

    hard_delete_entities: bool = Field(
        False,
        description="Whether to hard delete entities",
    )

    batch_size: int = Field(
        500,
        description="The number of entities to get in a batch from GraphQL",
    )


@dataclass
class DataFlowEntity:
    urn: str
    orchestrator: Optional[str]
    cluster: Optional[str]
    last_ingested: Optional[int]


@dataclass
class DataJobEntity:
    urn: str
    flow_urn: str
    lastIngested: Optional[int]
    jobId: Optional[str]
    dataPlatformInstance: Optional[str]
    total_runs: int = 0


@dataclass
class MetadataCleanupSourceReport(SourceReport):
    num_aspects_removed: int = 0
    num_aspect_removed_by_type: TopKDict[str, int] = field(default_factory=TopKDict)
    sample_removed_aspects_by_type: TopKDict[str, LossyList[str]] = field(
        default_factory=TopKDict
    )


@platform_name("Metadata Cleanup")
@config_class(MetadataCleanupConfig)
@support_status(SupportStatus.INCUBATING)
class MetadataCleanupSource(Source):
    """
    This source clean up aspects.

    """

    def __init__(self, ctx: PipelineContext, config: MetadataCleanupConfig):
        if not ctx.graph:
            raise ValueError("MetadataCleanupSource needs a datahub_api")

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

    def fetch_dpis(self, job_urn: str, batch_size: int) -> List[dict]:
        assert self.ctx.graph
        dpis = []
        start = 0
        while True:
            job_query_result = self.ctx.graph.execute_graphql(
                DATA_PROCESS_INSTANCES_QUERY,
                {"dataJobUrn": job_urn, "start": start, "count": batch_size},
            )
            job_data = job_query_result.get("dataJob")
            if not job_data:
                raise ValueError(f"Error getting job {job_urn}")

            runs_data = job_data.get("runs")
            if not runs_data:
                raise ValueError(f"Error getting runs for {job_urn}")

            runs = runs_data.get("runs")
            dpis.extend(runs)
            start += batch_size
            if len(runs) < batch_size:
                break
        return dpis

    def keep_last_n_dpi(self, dpis: List[Dict], job: DataJobEntity) -> None:
        if not self.config.keep_last_n:
            return

        deleted_count_last_n = 0
        if len(dpis) >= self.config.keep_last_n:
            for dpi in dpis[self.config.keep_last_n :]:
                deleted_count_last_n += 1
                self.delete_entity(dpi["urn"], "dataprocessInstance")
                dpi["deleted"] = True
                if deleted_count_last_n % self.config.batch_size == 0:
                    logger.info(f"Deleted {deleted_count_last_n} DPIs from {job.urn}")
        logger.info(f"Deleted {deleted_count_last_n} DPIs from {job.urn}")

    def delete_entity(self, urn: str, type: str) -> None:
        assert self.ctx.graph

        self.report.num_aspects_removed += 1
        self.report.num_aspect_removed_by_type[type] = (
            self.report.num_aspect_removed_by_type.get(type, 0) + 1
        )
        if type not in self.report.sample_removed_aspects_by_type:
            self.report.sample_removed_aspects_by_type[type] = LossyList()
        self.report.sample_removed_aspects_by_type[type].append(urn)

        self.ctx.graph.delete_entity(urn, self.config.hard_delete_entities)

    def delete_dpi_from_datajobs(self, job: DataJobEntity) -> None:
        assert self.ctx.graph

        dpis = self.fetch_dpis(job.urn, self.config.batch_size)
        dpis.sort(key=lambda x: x["created"]["time"], reverse=True)

        if self.config.keep_last_n:
            self.keep_last_n_dpi(dpis, job)

        if self.config.retention_days is not None:
            self.remove_old_dpis(dpis, job)

        job.total_runs = len(
            list(
                filter(lambda dpi: "deleted" not in dpi or not dpi.get("deleted"), dpis)
            )
        )

    def remove_old_dpis(self, dpis: List[Dict], job: DataJobEntity) -> None:
        if self.config.retention_days is None:
            return

        deleted_count_retention = 0
        retention_time = (
            int(datetime.now(timezone.utc).timestamp())
            - self.config.retention_days * 24 * 60 * 60
        )
        for dpi in dpis:
            if dpi.get("deleted"):
                continue

            if dpi["created"]["time"] < retention_time * 1000:
                deleted_count_retention += 1
                self.delete_entity(dpi["urn"], "dataprocessInstance")
                dpi["deleted"] = True
                if deleted_count_retention % self.config.batch_size == 0:
                    logger.info(
                        f"Deleted {deleted_count_retention} DPIs from {job.urn} due to retention"
                    )

            logger.info(
                f"Deleted {deleted_count_retention} DPIs from {job.urn} due to retention"
            )

    def get_data_flows(self) -> Iterable[DataFlowEntity]:
        assert self.ctx.graph

        scroll_id: Optional[str] = None

        while True:
            result = self.ctx.graph.execute_graphql(
                DATAFLOW_QUERY,
                {"query": "*", scroll_id: scroll_id if scroll_id else "null"},
            )
            scrollAcrossEntities = result.get("scrollAcrossEntities")
            if not scrollAcrossEntities:
                raise ValueError("Missing scrollAcrossEntities in response")

            scroll_id = scrollAcrossEntities.get("nextScrollId")
            for flow in scrollAcrossEntities.get("searchResults"):
                yield DataFlowEntity(
                    urn=flow.get("entity").get("urn"),
                    orchestrator=flow.get("entity").get("orchestrator"),
                    cluster=flow.get("entity").get("cluster"),
                    last_ingested=flow.get("entity").get("lastIngested"),
                )

            if not scroll_id:
                break

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        assert self.ctx.graph

        dataFlows: Dict[str, DataFlowEntity] = {}
        for flow in self.get_data_flows():
            dataFlows[flow.urn] = flow

        scroll_id: Optional[str] = None
        dataJobs: Dict[str, List[DataJobEntity]] = defaultdict(list)

        while True:
            result = self.ctx.graph.execute_graphql(
                DATAJOB_QUERY,
                {"query": "*", scroll_id: scroll_id if scroll_id else "null"},
            )
            scrollAcrossEntities = result.get("scrollAcrossEntities")
            if not scrollAcrossEntities:
                raise ValueError("Missing scrollAcrossEntities in response")

            scroll_id = scrollAcrossEntities.get("nextScrollId")
            for job in scrollAcrossEntities.get("searchResults"):
                datajob_entity = DataJobEntity(
                    urn=job.get("entity").get("urn"),
                    flow_urn=job.get("entity").get("dataFlow").get("urn"),
                    lastIngested=job.get("entity").get("lastIngested"),
                    jobId=job.get("entity").get("jobId"),
                    dataPlatformInstance=job.get("entity").get("dataPlatformInstance"),
                    total_runs=job.get("entity").get("runs").get("total"),
                )
                if datajob_entity.total_runs > 0:
                    self.delete_dpi_from_datajobs(datajob_entity)
                if (
                    datajob_entity.total_runs == 0
                    and self.config.delete_empty_data_jobs
                ):
                    logger.info(
                        f"Deleting datajob {datajob_entity.urn} because there are no runs"
                    )
                    self.delete_entity(datajob_entity.urn, "dataJob")
                else:
                    dataJobs[datajob_entity.flow_urn].append(datajob_entity)

            for key in dataFlows.keys():
                if (
                    not dataJobs.get(key) or len(dataJobs[key]) == 0
                ) and self.config.delete_empty_data_flows:
                    logger.info(
                        f"Deleting dataflow {key} because there are not datajobs"
                    )
                    self.delete_entity(key, "dataFlow")

            if not scroll_id:
                break

        return []
