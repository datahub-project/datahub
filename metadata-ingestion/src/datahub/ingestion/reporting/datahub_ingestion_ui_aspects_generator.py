import json
import time

from datahub import nice_version_name
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import make_data_platform_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DataHubIngestionSourceConfigClass,
    DataHubIngestionSourceInfoClass,
    ExecutionRequestInputClass,
    ExecutionRequestResultClass,
    ExecutionRequestSourceClass,
    _Aspect,
)
from datahub.utilities.urns.urn import Urn


class DatahubIngestionUIHistoryAspectsGenerator:

    _EXECUTOR_ID: str = "Datahub Ingestion CLI"
    _TASK_NAME: str = "CLI Ingestion"
    _EXECUTION_REQUEST_SOURCE_TYPE: str = "INGESTION_SOURCE"

    @staticmethod
    def get_cur_time_in_ms() -> int:
        return int(time.time() * 1000)

    def __init__(self, graph: DataHubGraph, ctx: PipelineContext) -> None:
        def generate_entity_name() -> str:
            assert self.ctx.owning_pipeline is not None
            # Construct the unique entity name
            pipeline_config = self.ctx.owning_pipeline.config
            entity_name_parts = ["CLI", pipeline_config.source.type]
            if pipeline_config.pipeline_name:
                entity_name_parts.append(pipeline_config.pipeline_name)
            try:
                entity_name_parts.append(self.ctx.owning_pipeline.source.get_platform_instance_id())  # type: ignore
            except AttributeError:
                pass
            return "-".join(entity_name_parts)

        self.graph: DataHubGraph = graph
        self.ctx: PipelineContext = ctx
        self.entity_name: str = generate_entity_name()
        self.ingestion_source_urn: Urn = Urn(
            entity_type="dataHubIngestionSource", entity_id=[self.entity_name]
        )
        self.execution_request_input_urn: Urn = Urn(
            entity_type="dataHubExecutionRequest", entity_id=[self.ctx.run_id]
        )
        self.start_time_ms: int = int(time.time() * 1000)

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

    def emit_datahub_ingestion_source_info_aspect(self) -> None:
        assert self.ctx.owning_pipeline is not None

        # Construct the dataHubIngestionSourceInfo aspect
        pipeline_config = self.ctx.owning_pipeline.config
        source_info_aspect = DataHubIngestionSourceInfoClass(
            name=self.entity_name,
            type=pipeline_config.source.type,
            platform=make_data_platform_urn(
                getattr(pipeline_config.source, "platform", "unknown")
            ),
            config=DataHubIngestionSourceConfigClass(
                recipe=pipeline_config.json(),
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

    def emit_execution_input_request(self):
        assert self.ctx.owning_pipeline is not None

        # Construct the dataHubExecutionRequestInput aspect
        pipeline_config = self.ctx.owning_pipeline.config
        execution_input_aspect = ExecutionRequestInputClass(
            task=self._TASK_NAME,
            args={
                "recipe": pipeline_config.json(),
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

    def emit_execution_request_result(self):
        assert self.ctx.owning_pipeline is not None

        # Construct the dataHubExecutionRequestResult aspect
        pipeline = self.ctx.owning_pipeline
        status = "SUCCESS"
        try:
            pipeline.raise_from_status()
        except Exception:
            status = "FAILED"

        execution_result_aspect = ExecutionRequestResultClass(
            status=status,
            startTimeMs=self.start_time_ms,
            durationMs=self.get_cur_time_in_ms() - self.start_time_ms,
            report=json.dumps(
                {
                    "source": {
                        "type": pipeline.config.source.type,
                        "report": pipeline.source.get_report().as_obj(),
                    },
                    "sink": {
                        "type": pipeline.config.sink.type,
                        "report": pipeline.sink.get_report().as_obj(),
                    },
                }
            ),
        )

        # Emit the dataHubExecutionRequestResult aspect
        self._emit_aspect(
            entity_urn=self.execution_request_input_urn,
            aspect_name="dataHubExecutionRequestResult",
            aspect_value=execution_result_aspect,
        )
