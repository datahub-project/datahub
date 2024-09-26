import logging
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import partial
from typing import Dict, Iterable, List, Optional

from pydantic import Field

from datahub.configuration import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceReport
from datahub.ingestion.api.source_helpers import auto_workunit_reporter
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.stats_collections import TopKDict

logger = logging.getLogger(__name__)

DATAJOB_QUERY = """
query listDataJobs($query:String!, $scrollId: String, $batchSize: Int) {
  scrollAcrossEntities(input: { types: [DATA_JOB
    ], query: $query, count: $batchSize, scrollId: $scrollId
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
query listDataFlows($query:String!, $scrollId: String, $batchSize: Int) {
  scrollAcrossEntities(input: { types: [DATA_FLOW], query: $query, count: $batchSize, scrollId: $scrollId}) {
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


class DataProcessCleanupConfig(ConfigModel):
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

    max_workers: int = Field(
        10,
        description="The number of workers to use for deletion",
    )

    delay: Optional[float] = Field(
        0.25,
        description="Delay between each batch",
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
class DataProcessCleanupReport(SourceReport):
    num_aspects_removed: int = 0
    num_aspect_removed_by_type: TopKDict[str, int] = field(default_factory=TopKDict)
    sample_removed_aspects_by_type: TopKDict[str, LossyList[str]] = field(
        default_factory=TopKDict
    )


class DataProcessCleanup:
    """
    This source is a maintenance source which cleans up old/unused aspects.

    Currently it only supports:.
        - DataFlow
        -DataJob
        - DataProcessInstance

    """

    def __init__(
        self,
        ctx: PipelineContext,
        config: DataProcessCleanupConfig,
        report: DataProcessCleanupReport,
        dry_run: bool = False,
    ):
        if not ctx.graph:
            raise ValueError("MetadataCleanupSource needs a datahub_api")

        self.graph: DataHubGraph = ctx.graph
        self.ctx = ctx
        self.config = config
        self.report = report
        self.dry_run = dry_run

    def get_report(self) -> DataProcessCleanupReport:
        return self.report

    # auto_work_unit_report is overriden to disable a couple of automation like auto status aspect, etc.. which is not needed her.
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

    def keep_last_n_dpi(
        self, dpis: List[Dict], job: DataJobEntity, executor: ThreadPoolExecutor
    ) -> None:
        if not self.config.keep_last_n:
            return

        deleted_count_last_n = 0
        if len(dpis) >= self.config.keep_last_n:
            futures = {}
            for dpi in dpis[self.config.keep_last_n :]:
                future = executor.submit(
                    self.delete_entity, dpi["urn"], "dataprocessInstance"
                )
                futures[future] = dpi

            for future in as_completed(futures):
                deleted_count_last_n += 1
                futures[future]["deleted"] = True

            if deleted_count_last_n % self.config.batch_size == 0:
                logger.info(f"Deleted {deleted_count_last_n} DPIs from {job.urn}")
                if self.config.delay:
                    logger.info(f"Sleeping for {self.config.delay} seconds")
                    time.sleep(self.config.delay)

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

        if self.dry_run:
            logger.info(
                f"Dry run is on otherwise it would have deleted {urn} with hard deletion is{self.config.hard_delete_entities}"
            )
            return

        self.ctx.graph.delete_entity(urn, self.config.hard_delete_entities)

    def delete_dpi_from_datajobs(self, job: DataJobEntity) -> None:
        assert self.ctx.graph

        dpis = self.fetch_dpis(job.urn, self.config.batch_size)
        dpis.sort(key=lambda x: x["created"]["time"], reverse=True)

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            if self.config.keep_last_n:
                self.keep_last_n_dpi(dpis, job, executor)

            if self.config.retention_days is not None:
                self.remove_old_dpis(dpis, job, executor)

        job.total_runs = len(
            list(
                filter(lambda dpi: "deleted" not in dpi or not dpi.get("deleted"), dpis)
            )
        )

    def remove_old_dpis(
        self, dpis: List[Dict], job: DataJobEntity, executor: ThreadPoolExecutor
    ) -> None:
        if self.config.retention_days is None:
            return

        deleted_count_retention = 0
        retention_time = (
            int(datetime.now(timezone.utc).timestamp())
            - self.config.retention_days * 24 * 60 * 60
        )

        futures = {}
        for dpi in dpis:
            if dpi.get("deleted"):
                continue

            if dpi["created"]["time"] < retention_time * 1000:
                future = executor.submit(
                    self.delete_entity, dpi["urn"], "dataprocessInstance"
                )
                futures[future] = dpi

        for future in as_completed(futures):
            deleted_count_retention += 1
            futures[future]["deleted"] = True

            if deleted_count_retention % self.config.batch_size == 0:
                logger.info(
                    f"Deleted {deleted_count_retention} DPIs from {job.urn} due to retention"
                )

                if self.config.delay:
                    logger.info(f"Sleeping for {self.config.delay} seconds")
                    time.sleep(self.config.delay)

        logger.info(
            f"Deleted {deleted_count_retention} DPIs from {job.urn} due to retention"
        )

    def get_data_flows(self) -> Iterable[DataFlowEntity]:
        assert self.ctx.graph

        scroll_id: Optional[str] = None
        previous_scroll_id: Optional[str] = None

        while True:
            result = self.ctx.graph.execute_graphql(
                DATAFLOW_QUERY,
                {
                    "query": "*",
                    "scrollId": scroll_id if scroll_id else None,
                    "batchSize": self.config.batch_size,
                },
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

            if not scroll_id or previous_scroll_id == scroll_id:
                break

            previous_scroll_id = scroll_id

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        assert self.ctx.graph

        dataFlows: Dict[str, DataFlowEntity] = {}
        for flow in self.get_data_flows():
            dataFlows[flow.urn] = flow

        scroll_id: Optional[str] = None
        dataJobs: Dict[str, List[DataJobEntity]] = defaultdict(list)
        deleted_jobs: int = 0
        while True:
            result = self.ctx.graph.execute_graphql(
                DATAJOB_QUERY,
                {
                    "query": "*",
                    "scrollId": scroll_id if scroll_id else None,
                    "batchSize": self.config.batch_size,
                },
            )
            scrollAcrossEntities = result.get("scrollAcrossEntities")
            if not scrollAcrossEntities:
                raise ValueError("Missing scrollAcrossEntities in response")

            logger.info(f"Got {scrollAcrossEntities.get('count')} DataJob entities")

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
                    deleted_jobs += 1
                    if deleted_jobs % self.config.batch_size == 0:
                        logger.info(f"Deleted {deleted_jobs} DataJobs")
                else:
                    dataJobs[datajob_entity.flow_urn].append(datajob_entity)

            if not scroll_id:
                break

        logger.info(f"Deleted {deleted_jobs} DataJobs")
        # Delete empty dataflows if needed
        if self.config.delete_empty_data_flows:
            deleted_data_flows: int = 0
            for key in dataFlows.keys():
                if not dataJobs.get(key) or len(dataJobs[key]) == 0:
                    logger.info(
                        f"Deleting dataflow {key} because there are not datajobs"
                    )
                    self.delete_entity(key, "dataFlow")
                    deleted_data_flows += 1
                    if deleted_jobs % self.config.batch_size == 0:
                        logger.info(f"Deleted {deleted_data_flows} DataFlows")
            logger.info(f"Deleted {deleted_data_flows} DataFlows")
        return []
