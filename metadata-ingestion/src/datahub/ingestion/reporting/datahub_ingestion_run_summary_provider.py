import json
import time
from typing import Any, Dict, Optional

from datahub import nice_version_name
from datahub.configuration.common import ConfigModel, ConfigurationError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import make_data_platform_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.pipeline_run_listener import PipelineRunListener
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    DataHubIngestionSourceConfigClass,
    DataHubIngestionSourceInfoClass,
    ExecutionRequestInputClass,
    ExecutionRequestResultClass,
    ExecutionRequestSourceClass,
    _Aspect,
)
from datahub.utilities.urns.urn import Urn


class DatahubIngestionRunSummaryProviderConfig(ConfigModel):
    datahub_api: Optional[DatahubClientConfig] = DatahubClientConfig()


class DatahubIngestionRunSummaryProvider(PipelineRunListener):

    _EXECUTOR_ID: str = "__datahub_cli_"
    _EXECUTION_REQUEST_SOURCE_TYPE: str = "INGESTION_SOURCE"
    _INGESTION_TASK_NAME: str = "CLI Ingestion"

    @staticmethod
    def get_cur_time_in_ms() -> int:
        return int(time.time() * 1000)

    @classmethod
    def create(
        cls,
        config_dict: Dict[str, Any],
        ctx: PipelineContext,
    ) -> PipelineRunListener:
        if ctx.graph:
            return cls(ctx.graph, ctx)
        elif config_dict is None:
            raise ConfigurationError("Missing provider configuration.")
        else:
            reporter_config = DatahubIngestionRunSummaryProviderConfig.parse_obj(
                config_dict
            )
            if reporter_config.datahub_api:
                graph = DataHubGraph(reporter_config.datahub_api)
                return cls(graph, ctx)
            else:
                raise ConfigurationError(
                    "Missing datahub_api. Provide either a global one or under the state_provider."
                )

    def __init__(self, graph: DataHubGraph, ctx: PipelineContext) -> None:
        assert ctx.pipeline_config is not None

        def generate_entity_name() -> str:
            assert ctx.pipeline_config is not None
            # Construct the unique entity name
            entity_name_parts = ["CLI", ctx.pipeline_config.source.type]
            if ctx.pipeline_config.pipeline_name:
                entity_name_parts.append(ctx.pipeline_config.pipeline_name)
            try:
                entity_name_parts.append(getattr(ctx.pipeline_config.source, "platform_instance"))  # type: ignore
            except AttributeError:
                pass
            return "-".join(entity_name_parts)

        self.graph: DataHubGraph = graph
        self.entity_name: str = generate_entity_name()
        self.ingestion_source_urn: Urn = Urn(
            entity_type="dataHubIngestionSource", entity_id=[self.entity_name]
        )
        self.execution_request_input_urn: Urn = Urn(
            entity_type="dataHubExecutionRequest", entity_id=[ctx.run_id]
        )
        self.start_time_ms: int = int(time.time() * 1000)

        # Construct the dataHubIngestionSourceInfo aspect
        source_info_aspect = DataHubIngestionSourceInfoClass(
            name=self.entity_name,
            type=ctx.pipeline_config.source.type,
            platform=make_data_platform_urn(
                getattr(ctx.pipeline_config.source, "platform", "unknown")
            ),
            config=DataHubIngestionSourceConfigClass(
                recipe=ctx.pipeline_config.json(),
                version=nice_version_name(),
                executorId=self._EXECUTOR_ID,
            ),
        )

        # Emit the dataHubIngestionSourceInfo aspect
        self._emit_aspect(
            entity_urn=self.ingestion_source_urn,
            aspect_name="dataHubIngestionSourceInfo",
            aspect_value=source_info_aspect,
        )

    def _emit_aspect(
        self, entity_urn: Urn, aspect_name: str, aspect_value: _Aspect
    ) -> None:
        self.graph.emit_mcp(
            MetadataChangeProposalWrapper(
                entityType=entity_urn.get_type(),
                entityUrn=str(entity_urn),
                aspectName=aspect_name,
                aspect=aspect_value,
                changeType="UPSERT",
            )
        )

    def on_start(self, ctx: PipelineContext) -> None:
        assert ctx.pipeline_config is not None
        # Construct the dataHubExecutionRequestInput aspect
        execution_input_aspect = ExecutionRequestInputClass(
            task=self._INGESTION_TASK_NAME,
            args={
                "recipe": ctx.pipeline_config.json(),
                "version": nice_version_name(),
            },
            executorId=self._EXECUTOR_ID,
            requestedAt=self.get_cur_time_in_ms(),
            source=ExecutionRequestSourceClass(
                type=self._EXECUTION_REQUEST_SOURCE_TYPE,
                ingestionSource=str(self.ingestion_source_urn),
            ),
        )
        # Emit the dataHubExecutionRequestInput aspect
        self._emit_aspect(
            entity_urn=self.execution_request_input_urn,
            aspect_name="dataHubExecutionRequestInput",
            aspect_value=execution_input_aspect,
        )

    def on_completion(
        self,
        status: str,
        report: Dict[str, Any],
        ctx: PipelineContext,
    ) -> None:
        # Construct the dataHubExecutionRequestResult aspect
        execution_result_aspect = ExecutionRequestResultClass(
            status=status,
            startTimeMs=self.start_time_ms,
            durationMs=self.get_cur_time_in_ms() - self.start_time_ms,
            report=json.dumps(report),
        )

        # Emit the dataHubExecutionRequestResult aspect
        self._emit_aspect(
            entity_urn=self.execution_request_input_urn,
            aspect_name="dataHubExecutionRequestResult",
            aspect_value=execution_result_aspect,
        )
