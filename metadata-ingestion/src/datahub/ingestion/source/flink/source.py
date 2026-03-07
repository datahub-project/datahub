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
    CapabilityReport,
    MetadataWorkUnitProcessor,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.flink.catalog import FlinkCatalogExtractor
from datahub.ingestion.source.flink.client import (
    FlinkCheckpointConfig,
    FlinkClusterConfig,
    FlinkJobDetail,
    FlinkJobSummary,
    FlinkRestClient,
)
from datahub.ingestion.source.flink.config import FlinkSourceConfig
from datahub.ingestion.source.flink.entities import (
    FlinkEntityBuilder,
    _compute_dataset_urns,
    _materialize_dataset_workunits,
)
from datahub.ingestion.source.flink.lineage import (
    FlinkLineageOrchestrator,
    LineageResult,
)
from datahub.ingestion.source.flink.report import FlinkSourceReport
from datahub.ingestion.source.flink.sql_gateway_client import FlinkSQLGatewayClient
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.sdk.datajob import DataJob
from datahub.sdk.entity import Entity

logger = logging.getLogger(__name__)


@dataclass
class _JobProcessingResult:
    """Per-job results collected in worker threads. Each worker gets its own instance."""

    job_name: str
    job_id: str
    workunits: List[Union[MetadataWorkUnit, Entity]] = field(default_factory=list)
    lineage_sources: int = 0
    lineage_sinks: int = 0
    lineage_unclassified: List[str] = field(default_factory=list)
    lineage_error: Optional[str] = None
    lineage_exc: Optional[BaseException] = None
    dpis_emitted: int = 0
    dpi_exc: Optional[BaseException] = None


@platform_name("Flink", id="flink")
@config_class(FlinkSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Table-level lineage from Kafka sources/sinks",
)
@capability(SourceCapability.DELETION_DETECTION, "Via stateful ingestion")
@capability(
    SourceCapability.SCHEMA_METADATA,
    "Catalog table schemas via SQL Gateway (requires include_catalog_metadata)",
)
@capability(
    SourceCapability.CONTAINERS,
    "Catalog databases as containers (requires SQL Gateway)",
)
class FlinkSource(StatefulIngestionSourceBase, TestableSource):
    """Apache Flink connector. Extracts jobs, lineage, and run history
    from the Flink JobManager REST API."""

    config: FlinkSourceConfig
    report: FlinkSourceReport
    platform: str = "flink"

    def __init__(self, config: FlinkSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.config = config
        self.report = FlinkSourceReport()
        self.client = FlinkRestClient(config.connection)
        self.sql_gateway_client: Optional[FlinkSQLGatewayClient] = None
        if config.include_catalog_metadata and config.connection.sql_gateway_url:
            self.sql_gateway_client = FlinkSQLGatewayClient(config.connection)
        self.lineage_orchestrator = FlinkLineageOrchestrator(report=self.report)
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

    def get_workunits_internal(
        self,
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        # 1. Connect and get cluster info
        try:
            cluster_config = self.client.get_cluster_config()
            self.report.flink_version = cluster_config.flink_version
            logger.info("Connected to Flink %s", cluster_config.flink_version)
        except Exception as e:
            self.report.failure(
                title="Failed to connect to Flink cluster",
                message="Could not reach the Flink REST API.",
                context=self.config.connection.rest_api_url,
                exc=e,
            )
            return

        # 2. Fetch and filter jobs
        try:
            jobs = self.client.get_jobs_overview()
        except Exception as e:
            self.report.failure(
                title="Failed to fetch job overview",
                message="Could not list jobs from the Flink cluster.",
                context=self.config.connection.rest_api_url,
                exc=e,
            )
            return

        filtered = self._filter_jobs(jobs)
        deduplicated = self._deduplicate_jobs(filtered)

        # 3. Process jobs in parallel
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {
                executor.submit(self._process_job, summary, cluster_config): summary
                for summary in deduplicated.values()
            }
            for future in as_completed(futures):
                summary = futures[future]
                try:
                    result = future.result()
                    self.report.report_job_processed()
                    self.report.report_lineage_extracted(
                        sources=result.lineage_sources,
                        sinks=result.lineage_sinks,
                    )
                    for desc in result.lineage_unclassified:
                        self.report.report_lineage_unclassified(desc[:100])
                    if result.lineage_error:
                        self.report.report_lineage_failed(
                            result.job_name,
                            result.lineage_error,
                            exc=result.lineage_exc,
                        )
                    if result.dpi_exc:
                        self.report.warning(
                            title="Failed to build run history",
                            message="DPI construction failed. Job metadata was still emitted.",
                            context=f"job={result.job_name}",
                            exc=result.dpi_exc,
                        )
                    if result.dpis_emitted:
                        self.report.report_dpi_emitted()
                    yield from result.workunits
                except Exception as e:
                    logger.exception("Failed to process job %s", summary.name)
                    self.report.report_job_failed(summary.name, str(e), exc=e)

        # 4. Catalog metadata (optional)
        if self.sql_gateway_client:
            catalog_extractor = FlinkCatalogExtractor(
                config=self.config,
                sql_client=self.sql_gateway_client,
                report=self.report,
            )
            yield from catalog_extractor.extract()

    def _filter_jobs(self, jobs: List[FlinkJobSummary]) -> List[FlinkJobSummary]:
        filtered = []
        for job in jobs:
            self.report.report_job_scanned()
            if not self.config.job_name_pattern.allowed(job.name):
                self.report.report_job_filtered_by_name()
                continue
            if job.state.upper() not in self.config.include_job_states:
                self.report.report_job_filtered_by_state()
                continue
            filtered.append(job)
        return filtered

    def _deduplicate_jobs(
        self, jobs: List[FlinkJobSummary]
    ) -> Dict[str, FlinkJobSummary]:
        by_name: Dict[str, FlinkJobSummary] = {}
        for job in jobs:
            existing = by_name.get(job.name)
            if existing is None:
                by_name[job.name] = job
            elif job.start_time > existing.start_time:
                logger.warning(
                    "Duplicate job '%s': keeping jid=%s over jid=%s",
                    job.name,
                    job.jid,
                    existing.jid,
                )
                by_name[job.name] = job
        return by_name

    def _process_job(
        self,
        summary: FlinkJobSummary,
        cluster_config: FlinkClusterConfig,
    ) -> _JobProcessingResult:
        result = _JobProcessingResult(job_name=summary.name, job_id=summary.jid)
        job_detail = self.client.get_job_details(summary.jid)
        checkpoint_config = self._fetch_checkpoint_config(summary)
        lineage_result = self._extract_lineage(result, job_detail)
        datajob_for_dpi = self._build_flow_and_jobs(
            result, job_detail, checkpoint_config, cluster_config, lineage_result
        )
        self._build_dpi(result, job_detail, datajob_for_dpi, lineage_result)
        return result

    def _fetch_checkpoint_config(
        self, summary: FlinkJobSummary
    ) -> Optional[FlinkCheckpointConfig]:
        try:
            return self.client.get_checkpoint_config(summary.jid)
        except Exception as e:
            logger.warning(
                "Failed to fetch checkpoint config for job %s: %s",
                summary.name,
                e,
                exc_info=True,
            )
            self.report.warning(
                title="Failed to fetch checkpoint config",
                message="Checkpoint configuration could not be retrieved. "
                "Job metadata will be emitted without checkpoint properties.",
                context=f"job={summary.name}",
                exc=e,
            )
            return None

    def _extract_lineage(
        self,
        result: _JobProcessingResult,
        job_detail: FlinkJobDetail,
    ) -> LineageResult:
        lineage_result = LineageResult()
        if self.config.include_lineage and job_detail.plan_nodes:
            try:
                lineage_result = self.lineage_orchestrator.extract(
                    job_detail.plan_nodes
                )
                result.lineage_sources = len(lineage_result.sources)
                result.lineage_sinks = len(lineage_result.sinks)
                result.lineage_unclassified = list(lineage_result.unclassified)
            except Exception as e:
                result.lineage_error = str(e)
                result.lineage_exc = e
                logger.warning(
                    "Lineage extraction failed for %s: %s",
                    job_detail.name,
                    e,
                    exc_info=True,
                )
        return lineage_result

    def _build_flow_and_jobs(
        self,
        result: _JobProcessingResult,
        job_detail: FlinkJobDetail,
        checkpoint_config: Optional[FlinkCheckpointConfig],
        cluster_config: FlinkClusterConfig,
        lineage_result: LineageResult,
    ) -> Optional[DataJob]:
        dataflow = self.entity_builder.build_dataflow(
            job_detail, checkpoint_config, cluster_config.flink_version
        )
        result.workunits.append(dataflow)

        if self.config.operator_granularity == "vertex":
            datajobs = self.entity_builder.build_datajobs_per_vertex(
                dataflow, job_detail, lineage_result
            )
            result.workunits.extend(datajobs)
            datajob_for_dpi = datajobs[0] if datajobs else None
        else:
            datajob = self.entity_builder.build_datajob(
                dataflow, job_detail, lineage_result
            )
            result.workunits.append(datajob)
            datajob_for_dpi = datajob

        # Materialize lineage dataset entities (new SDK doesn't auto-create them)
        if self.config.include_lineage:
            all_urns = _compute_dataset_urns(
                lineage_result.sources, self.config
            ) + _compute_dataset_urns(lineage_result.sinks, self.config)
            result.workunits.extend(_materialize_dataset_workunits(all_urns))

        return datajob_for_dpi

    def _build_dpi(
        self,
        result: _JobProcessingResult,
        job_detail: FlinkJobDetail,
        datajob_for_dpi: Optional[DataJob],
        lineage_result: LineageResult,
    ) -> None:
        if not self.config.include_run_history or not datajob_for_dpi:
            return
        try:
            dpi_wus = list(
                self.entity_builder.build_dpi_workunits(
                    job_detail=job_detail,
                    datajob=datajob_for_dpi,
                    lineage_result=lineage_result,
                )
            )
            result.workunits.extend(dpi_wus)
            result.dpis_emitted = 1
        except Exception as e:
            result.dpi_exc = e
            logger.warning(
                "DPI construction failed for %s: %s",
                job_detail.name,
                e,
                exc_info=True,
            )

    def close(self) -> None:
        try:
            self.client.close()
        except Exception as e:
            self.report.warning(
                title="Failed to close Flink REST client",
                message="HTTP session may not have been properly closed.",
                context=self.config.connection.rest_api_url,
                exc=e,
            )
        try:
            if self.sql_gateway_client:
                self.sql_gateway_client.close()
        except Exception as e:
            self.report.warning(
                title="Failed to close SQL Gateway client",
                message="SQL Gateway session may not have been properly closed.",
                context=str(self.config.connection.sql_gateway_url),
                exc=e,
            )
        super().close()

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            config = FlinkSourceConfig.model_validate(config_dict)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=f"Invalid configuration: {e}",
            )
            return test_report

        client = FlinkRestClient(config.connection)
        try:
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
                    SourceCapability.LINEAGE_COARSE: CapabilityReport(capable=True),
                }
            except Exception as e:
                test_report.capability_report = {
                    SourceCapability.LINEAGE_COARSE: CapabilityReport(
                        capable=False,
                        failure_reason=f"Cannot list jobs: {e}",
                    ),
                }

            if config.connection.sql_gateway_url:
                test_report.capability_report.update(
                    FlinkSource._test_sql_gateway(config)
                )

            return test_report
        finally:
            client.close()

    @staticmethod
    def _test_sql_gateway(
        config: FlinkSourceConfig,
    ) -> Dict[Union[SourceCapability, str], CapabilityReport]:
        sql_client = FlinkSQLGatewayClient(config.connection)
        try:
            err = sql_client.test_connection()
            if err is None:
                report = CapabilityReport(capable=True)
            else:
                report = CapabilityReport(
                    capable=False,
                    failure_reason=f"Cannot connect to SQL Gateway: {err}",
                )
            return {
                SourceCapability.SCHEMA_METADATA: report,
                SourceCapability.CONTAINERS: report,
            }
        finally:
            sql_client.close()
