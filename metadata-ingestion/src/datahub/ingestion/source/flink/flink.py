import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Union

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
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.flink.flink_client import (
    FlinkClusterConfig,
    FlinkJobSummary,
    FlinkRestClient,
)
from datahub.ingestion.source.flink.flink_config import FlinkSourceConfig
from datahub.ingestion.source.flink.flink_entities import FlinkEntityBuilder
from datahub.ingestion.source.flink.flink_lineage import (
    FlinkLineageOrchestrator,
    LineageResult,
)
from datahub.ingestion.source.flink.flink_report import FlinkSourceReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.sdk.entity import Entity

logger = logging.getLogger(__name__)


@dataclass
class _JobProcessingResult:
    """Thread-safe container for per-job results collected in worker threads."""

    job_name: str
    job_id: str
    workunits: List[Union[MetadataWorkUnit, Entity]] = field(default_factory=list)
    lineage_sources: int = 0
    lineage_sinks: int = 0
    lineage_unclassified: List[str] = field(default_factory=list)
    lineage_failed: bool = False
    dpis_emitted: int = 0


@platform_name("Flink", id="flink")
@config_class(FlinkSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Table-level lineage from Kafka sources/sinks",
)
@capability(SourceCapability.DELETION_DETECTION, "Via stateful ingestion")
class FlinkSource(StatefulIngestionSourceBase, TestableSource):
    """
    Apache Flink connector for extracting metadata and lineage from Flink clusters.

    Polls the Flink JobManager REST API to discover jobs, extract source-to-sink
    lineage, and track job run history. Zero-instrumentation -- no code changes
    required to Flink applications.
    """

    config: FlinkSourceConfig
    report: FlinkSourceReport
    platform: str = "flink"

    def __init__(self, config: FlinkSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.config = config
        self.report = FlinkSourceReport()
        self.client = FlinkRestClient(config)
        self.lineage_orchestrator = FlinkLineageOrchestrator()
        self.entity_builder = FlinkEntityBuilder(config)
        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, config, ctx
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "FlinkSource":
        config = FlinkSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_report(self) -> FlinkSourceReport:
        return self.report

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            self.stale_entity_removal_handler.workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        """Main entry point: discover jobs, extract metadata, emit workunits."""

        # 1. Fetch cluster config (main thread -- safe to use report.api_latency)
        try:
            with self.report.api_latency:
                cluster_config = self.client.get_cluster_config()
            self.report.flink_version = cluster_config.flink_version
            logger.info(
                "Connected to Flink cluster version %s",
                cluster_config.flink_version,
            )
        except Exception as e:
            self.report.failure(
                title="Failed to connect to Flink cluster",
                message="Could not reach the Flink REST API. Check rest_endpoint and authentication.",
                context=self.config.rest_endpoint,
                exc=e,
            )
            return

        # 2. Fetch job overview (main thread)
        try:
            with self.report.api_latency:
                jobs = self.client.get_jobs_overview()
        except Exception as e:
            self.report.failure(
                title="Failed to fetch job overview",
                message="Could not list jobs from the Flink cluster.",
                context=self.config.rest_endpoint,
                exc=e,
            )
            return

        logger.info("Discovered %d jobs on Flink cluster", len(jobs))

        # 3. Filter and process jobs
        filtered_jobs = self._filter_jobs(jobs)

        # 4. Deduplicate jobs with same name
        deduplicated = self._deduplicate_jobs(filtered_jobs)

        # 5. Process jobs in parallel -- worker threads return results,
        #    all report mutations happen in the main thread below.
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {
                executor.submit(self._process_job, summary, cluster_config): summary
                for summary in deduplicated.values()
            }
            for future in as_completed(futures):
                summary = futures[future]
                try:
                    result = future.result()
                    # Merge report stats in main thread (thread-safe)
                    self.report.report_job_processed()
                    self.report.report_lineage(
                        sources=result.lineage_sources,
                        sinks=result.lineage_sinks,
                    )
                    for desc in result.lineage_unclassified:
                        self.report.lineage_unclassified_nodes.append(desc[:100])
                    if result.lineage_failed:
                        self.report.lineage_failed += 1
                    self.report.dpis_emitted += result.dpis_emitted

                    yield from result.workunits
                except Exception as e:
                    self.report.report_job_failed(summary.name)
                    self.report.warning(
                        title="Failed to process Flink job",
                        message="Job metadata could not be extracted. Skipping.",
                        context=f"{summary.name} (jid={summary.jid})",
                        exc=e,
                    )

    def _filter_jobs(self, jobs: List[FlinkJobSummary]) -> List[FlinkJobSummary]:
        """Apply name pattern and state filters."""
        filtered = []
        for job in jobs:
            self.report.report_job_discovered()

            if not self.config.job_name_pattern.allowed(job.name):
                self.report.report_job_filtered_by_name(job.name)
                logger.debug("Filtered job %s by name pattern", job.name)
                continue

            if job.state.upper() not in self.config.include_job_states:
                self.report.report_job_filtered_by_state(job.name, job.state)
                logger.debug(
                    "Filtered job %s (state=%s) by state filter",
                    job.name,
                    job.state,
                )
                continue

            filtered.append(job)

        logger.info(
            "After filtering: %d jobs (from %d discovered)",
            len(filtered),
            len(jobs),
        )
        return filtered

    def _deduplicate_jobs(
        self, jobs: List[FlinkJobSummary]
    ) -> Dict[str, FlinkJobSummary]:
        """Deduplicate jobs with the same name -- keep most recently started."""
        by_name: Dict[str, FlinkJobSummary] = {}
        for job in jobs:
            existing = by_name.get(job.name)
            if existing is None:
                by_name[job.name] = job
            elif job.start_time > existing.start_time:
                logger.warning(
                    "Duplicate job name '%s': keeping jid=%s (start=%d) over jid=%s (start=%d)",
                    job.name,
                    job.jid,
                    job.start_time,
                    existing.jid,
                    existing.start_time,
                )
                by_name[job.name] = job
        return by_name

    def _process_job(
        self,
        summary: FlinkJobSummary,
        cluster_config: FlinkClusterConfig,
    ) -> _JobProcessingResult:
        """Process a single Flink job in a worker thread.

        Returns a result object -- no shared report state is mutated here.
        """
        result = _JobProcessingResult(
            job_name=summary.name,
            job_id=summary.jid,
        )

        job_detail = self.client.get_job_details(summary.jid)
        checkpoint_config = self.client.get_checkpoint_config(summary.jid)

        lineage_result = LineageResult()
        if self.config.include_lineage and job_detail.plan_nodes:
            try:
                lineage_result = self.lineage_orchestrator.extract(
                    job_detail.plan_nodes
                )
                result.lineage_sources = len(lineage_result.sources)
                result.lineage_sinks = len(lineage_result.sinks)
                result.lineage_unclassified = [
                    desc for desc in lineage_result.unclassified
                ]
            except Exception as e:
                result.lineage_failed = True
                logger.warning(
                    "Lineage extraction failed for job %s: %s",
                    job_detail.name,
                    e,
                )

        # Build DataFlow entity (SDK V2)
        dataflow = self.entity_builder.build_dataflow(
            job_detail, checkpoint_config, cluster_config.flink_version
        )
        result.workunits.append(dataflow)

        # Build DataJob entity (SDK V2)
        datajob = self.entity_builder.build_datajob(
            dataflow, job_detail, lineage_result
        )
        result.workunits.append(datajob)

        # Build DataProcessInstance workunits
        if self.config.include_run_history:
            result.workunits.extend(
                self.entity_builder.build_dpi_workunits(
                    job_detail.name,
                    job_detail,
                    datajob,
                    lineage_result,
                )
            )
            result.dpis_emitted = 1

        return result

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        from datahub.ingestion.api.source import CapabilityReport

        test_report = TestConnectionReport()

        try:
            config = FlinkSourceConfig.model_validate(config_dict)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=f"Invalid configuration: {e}",
            )
            return test_report

        client = FlinkRestClient(config)

        try:
            client.test_connectivity()
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=f"Cannot reach Flink REST API: {e}",
            )
            return test_report

        try:
            client.get_jobs_overview()
            test_report.capability_report = {
                SourceCapability.LINEAGE_COARSE: CapabilityReport(
                    capable=True,
                    failure_reason=None,
                ),
            }
        except Exception as e:
            test_report.capability_report = {
                SourceCapability.LINEAGE_COARSE: CapabilityReport(
                    capable=False,
                    failure_reason=f"Cannot list jobs: {e}",
                ),
            }

        return test_report
