import json
import logging
import time
from typing import Any, Dict, Optional

from datahub import nice_version_name
from datahub.configuration.common import ConfigModel, DynamicTypedConfig
from datahub.emitter.mce_builder import datahub_guid
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import make_data_platform_urn
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.pipeline_run_listener import PipelineRunListener
from datahub.ingestion.api.sink import NoopWriteCallback, Sink
from datahub.ingestion.run.pipeline_config import PipelineConfig
from datahub.ingestion.sink.sink_registry import sink_registry
from datahub.metadata.schema_classes import (
    DataHubIngestionSourceConfigClass,
    DataHubIngestionSourceInfoClass,
    ExecutionRequestInputClass,
    ExecutionRequestResultClass,
    ExecutionRequestSourceClass,
    _Aspect,
)
from datahub.utilities.urns.urn import Urn

logger = logging.getLogger(__name__)


class DatahubIngestionRunSummaryProviderConfig(ConfigModel):
    report_recipe: bool = True
    sink: Optional[DynamicTypedConfig] = None


class DatahubIngestionRunSummaryProvider(PipelineRunListener):

    _EXECUTOR_ID: str = "__datahub_cli_"
    _EXECUTION_REQUEST_SOURCE_TYPE: str = "CLI_INGESTION_SOURCE"
    _INGESTION_TASK_NAME: str = "CLI Ingestion"

    @staticmethod
    def get_cur_time_in_ms() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def generate_unique_key(pipeline_config: PipelineConfig) -> dict:
        key = {}
        key["type"] = pipeline_config.source.type
        if pipeline_config.pipeline_name:
            key["pipeline_name"] = pipeline_config.pipeline_name
        if (
            pipeline_config.source.config
            and "platform_instance" in pipeline_config.source.config
        ):
            key["platform_instance"] = pipeline_config.source.config[
                "platform_instance"
            ]
        return key

    @staticmethod
    def generate_entity_name(key: dict) -> str:
        # Construct the unique entity name
        entity_name = f"[CLI] {key['type']}"
        if "platform_instance" in key:
            entity_name = f"{entity_name} ({key['platform_instance']})"

        if "pipeline_name" in key:
            entity_name = f"{entity_name} [{key['pipeline_name']}]"
        return entity_name

    @classmethod
    def create(
        cls,
        config_dict: Dict[str, Any],
        ctx: PipelineContext,
    ) -> PipelineRunListener:

        sink_config_holder: Optional[DynamicTypedConfig] = None

        reporter_config = DatahubIngestionRunSummaryProviderConfig.parse_obj(
            config_dict or {}
        )
        if reporter_config.sink:
            sink_config_holder = reporter_config.sink

        if sink_config_holder is None:
            # Populate sink from global recipe
            assert ctx.pipeline_config
            sink_config_holder = ctx.pipeline_config.sink
            # Global instances are safe to use only if the types are datahub-rest and datahub-kafka
            # Re-using a shared file sink will result in clobbering the events
            if sink_config_holder.type not in ["datahub-rest", "datahub-kafka"]:
                raise ValueError(
                    f"Datahub ingestion reporter will be disabled because sink type {sink_config_holder.type} is not supported"
                )

        sink_type = sink_config_holder.type
        sink_class = sink_registry.get(sink_type)
        sink_config = sink_config_holder.dict().get("config") or {}
        if sink_type == "datahub-rest":
            # for the rest emitter we want to use sync mode to emit
            # regardless of the default sink config since that makes it
            # immune to process shutdown related failures
            sink_config["mode"] = "SYNC"

        sink: Sink = sink_class.create(sink_config, ctx)
        return cls(sink, reporter_config.report_recipe, ctx)

    def __init__(self, sink: Sink, report_recipe: bool, ctx: PipelineContext) -> None:
        assert ctx.pipeline_config is not None

        self.sink: Sink = sink
        self.report_recipe = report_recipe
        ingestion_source_key = self.generate_unique_key(ctx.pipeline_config)
        self.entity_name: str = self.generate_entity_name(ingestion_source_key)

        self.ingestion_source_urn: Urn = Urn(
            entity_type="dataHubIngestionSource",
            entity_id=["cli-" + datahub_guid(ingestion_source_key)],
        )
        logger.debug(f"Ingestion source urn = {self.ingestion_source_urn}")
        self.execution_request_input_urn: Urn = Urn(
            entity_type="dataHubExecutionRequest", entity_id=[ctx.run_id]
        )
        self.start_time_ms: int = self.get_cur_time_in_ms()

        # Construct the dataHubIngestionSourceInfo aspect
        source_info_aspect = DataHubIngestionSourceInfoClass(
            name=self.entity_name,
            type=ctx.pipeline_config.source.type,
            platform=make_data_platform_urn(
                getattr(ctx.pipeline_config.source, "platform", "unknown")
            ),
            config=DataHubIngestionSourceConfigClass(
                recipe=self._get_recipe_to_report(ctx),
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

    def _get_recipe_to_report(self, ctx: PipelineContext) -> str:
        assert ctx.pipeline_config
        if not self.report_recipe or not ctx.pipeline_config._raw_dict:
            return ""
        else:
            return json.dumps(ctx.pipeline_config._raw_dict)

    def _emit_aspect(
        self, entity_urn: Urn, aspect_name: str, aspect_value: _Aspect
    ) -> None:
        self.sink.write_record_async(
            RecordEnvelope(
                record=MetadataChangeProposalWrapper(
                    entityType=entity_urn.get_type(),
                    entityUrn=str(entity_urn),
                    aspectName=aspect_name,
                    aspect=aspect_value,
                    changeType="UPSERT",
                ),
                metadata={},
            ),
            NoopWriteCallback(),
        )

    def on_start(self, ctx: PipelineContext) -> None:
        assert ctx.pipeline_config is not None
        # Construct the dataHubExecutionRequestInput aspect
        execution_input_aspect = ExecutionRequestInputClass(
            task=self._INGESTION_TASK_NAME,
            args={
                "recipe": self._get_recipe_to_report(ctx),
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
            report=json.dumps(report, indent=2),
        )

        # Emit the dataHubExecutionRequestResult aspect
        self._emit_aspect(
            entity_urn=self.execution_request_input_urn,
            aspect_name="dataHubExecutionRequestResult",
            aspect_value=execution_result_aspect,
        )
        self.sink.close()
