import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from datahub.configuration.common import ConfigurationError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.ingestion_job_reporting_provider_base import (
    IngestionReportingProviderBase,
    IngestionReportingProviderConfig,
    JobId,
    JobStateFilterType,
    JobStateKey,
    ReportingJobStatesMap,
)
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatahubIngestionRunSummaryClass,
)

logger = logging.getLogger(__name__)


class DatahubIngestionReportingProviderConfig(IngestionReportingProviderConfig):
    datahub_api: Optional[DataHubGraphConfig] = DataHubGraphConfig()


class DatahubIngestionReportingProvider(IngestionReportingProviderBase):
    orchestrator_name: str = "datahub"

    def __init__(self, graph: DataHubGraph, name: str):
        super().__init__(name)
        self.graph = graph
        if not self._is_server_stateful_ingestion_capable():
            raise ConfigurationError(
                "Datahub server is not capable of supporting stateful ingestion."
                " Please consider upgrading to the latest server version to use this feature."
            )

    @classmethod
    def create(
        cls, config_dict: Dict[str, Any], ctx: PipelineContext, name: str
    ) -> IngestionReportingProviderBase:
        if ctx.graph:
            return cls(ctx.graph, name)
        elif config_dict is None:
            raise ConfigurationError("Missing provider configuration.")
        else:
            provider_config = DatahubIngestionReportingProviderConfig.parse_obj(
                config_dict
            )
            if provider_config.datahub_api:
                graph = DataHubGraph(provider_config.datahub_api)
                ctx.graph = graph
                return cls(graph, name)
            else:
                raise ConfigurationError(
                    "Missing datahub_api. Provide either a global one or under the state_provider."
                )

    def _is_server_stateful_ingestion_capable(self) -> bool:
        server_config = self.graph.get_config() if self.graph else None
        if server_config and server_config.get("statefulIngestionCapable"):
            return True
        return False

    def get_latest_run_summary(
        self,
        pipeline_name: str,
        platform_instance_id: str,
        job_name: JobId,
    ) -> Optional[DatahubIngestionRunSummaryClass]:

        logger.info(
            f"Querying for the latest ingestion run summary for pipelineName:'{pipeline_name}',"
            f" platformInstanceId:'{platform_instance_id}', job_name:'{job_name}'"
        )

        data_job_urn = self.get_data_job_urn(
            self.orchestrator_name, pipeline_name, job_name, platform_instance_id
        )
        latest_run_summary: Optional[
            DatahubIngestionRunSummaryClass
        ] = self.graph.get_latest_timeseries_value(
            entity_urn=data_job_urn,
            aspect_name="datahubIngestionRunSummary",
            filter_criteria_map={
                "pipelineName": pipeline_name,
                "platformInstanceId": platform_instance_id,
            },
            aspect_type=DatahubIngestionRunSummaryClass,
        )
        if latest_run_summary:
            logger.info(
                f"The latest saved run summary for pipelineName:'{pipeline_name}',"
                f" platformInstanceId:'{platform_instance_id}', job_name:'{job_name}' found with start_time:"
                f" {datetime.fromtimestamp(latest_run_summary.timestampMillis/1000, tz=timezone.utc)} and a"
                f" bucket duration of {latest_run_summary.eventGranularity}."
            )
            return latest_run_summary
        else:
            logger.info(
                f"No committed ingestion run summary for pipelineName:'{pipeline_name}',"
                f" platformInstanceId:'{platform_instance_id}', job_name:'{job_name}' found"
            )

        return None

    def get_previous_states(
        self,
        state_key: JobStateKey,
        last_only: bool = True,
        filter_opt: Optional[JobStateFilterType] = None,
    ) -> List[ReportingJobStatesMap]:
        if not last_only:
            raise NotImplementedError(
                "Currently supports retrieving only the last committed state."
            )
        if filter_opt is not None:
            raise NotImplementedError(
                "Support for optional filters is not implemented yet."
            )
        job_run_summaries: List[ReportingJobStatesMap] = []
        last_job_run_summary_map: ReportingJobStatesMap = {}
        for job_name in state_key.job_names:
            last_job_run_summary = self.get_latest_run_summary(
                state_key.pipeline_name, state_key.platform_instance_id, job_name
            )
            if last_job_run_summary is not None:
                last_job_run_summary_map[job_name] = last_job_run_summary
        job_run_summaries.append(last_job_run_summary_map)
        return job_run_summaries

    def commit(self) -> None:
        if not self.state_to_commit:
            # Useful to track source types for which reporting provider need to be enabled.
            logger.info(f"No state to commit for {self.name}")
            return None

        for job_name, run_summary in self.state_to_commit.items():
            # Emit the ingestion state for each job
            logger.info(
                f"Committing ingestion run summary for pipeline:'{run_summary.pipelineName}',"
                f"instance:'{run_summary.platformInstanceId}', job:'{job_name}'"
            )

            self.committed = False

            datajob_urn = self.get_data_job_urn(
                self.orchestrator_name,
                run_summary.pipelineName,
                job_name,
                run_summary.platformInstanceId,
            )

            self.graph.emit_mcp(
                MetadataChangeProposalWrapper(
                    entityType="dataJob",
                    entityUrn=datajob_urn,
                    aspectName="datahubIngestionRunSummary",
                    aspect=run_summary,
                    changeType=ChangeTypeClass.UPSERT,
                )
            )

            self.committed = True

            logger.info(
                f"Committed ingestion run summary for pipeline:'{run_summary.pipelineName}',"
                f"instance:'{run_summary.platformInstanceId}', job:'{job_name}'"
            )
