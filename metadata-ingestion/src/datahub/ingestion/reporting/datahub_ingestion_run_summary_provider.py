import json
import logging
import time
from typing import Any, Dict, Optional

from datahub._version import nice_version_name
from datahub.configuration.common import (
    ConfigModel,
    DynamicTypedConfig,
    IgnorableError,
    redact_raw_config,
)
from datahub.emitter.aspect import JSON_CONTENT_TYPE
from datahub.emitter.mce_builder import datahub_guid, make_data_platform_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
    StructuredExecutionReportClass,
    _Aspect,
)
from datahub.utilities.logging_manager import get_log_buffer
from datahub.utilities.urns.urn import Urn

logger = logging.getLogger(__name__)


class DatahubIngestionRunSummaryProviderConfig(ConfigModel):
    report_recipe: bool = True
    sink: Optional[DynamicTypedConfig] = None


class DatahubIngestionRunSummaryProvider(PipelineRunListener):
    _EXECUTOR_ID: str = "__datahub_cli_"
    _EXECUTION_REQUEST_SOURCE_TYPE: str = "CLI_INGESTION_SOURCE"
    _INGESTION_TASK_NAME: str = "CLI Ingestion"
    _MAX_SUMMARY_SIZE: int = 800000

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
        sink: Sink,
    ) -> PipelineRunListener:
        reporter_config = DatahubIngestionRunSummaryProviderConfig.parse_obj(
            config_dict or {}
        )
        if reporter_config.sink:
            sink_class = sink_registry.get(reporter_config.sink.type)
            sink_config = reporter_config.sink.config or {}
            sink = sink_class.create(sink_config, ctx)
        else:
            if not isinstance(
                sink,
                tuple(
                    [
                        kls
                        for kls in [
                            sink_registry.get_optional("datahub-rest"),
                            sink_registry.get_optional("datahub-kafka"),
                        ]
                        if kls
                    ]
                ),
            ):
                raise IgnorableError(
                    f"Datahub ingestion reporter will be disabled because sink type {type(sink)} is not supported"
                )

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
            aspect_value=source_info_aspect,
        )

    @staticmethod
    def _convert_sets_to_lists(obj: Any) -> Any:
        """
        Recursively converts all sets to lists in a Python object.
        Works with nested dictionaries, lists, and sets.

        Args:
            obj: Any Python object that might contain sets

        Returns:
            The object with all sets converted to lists
        """
        if isinstance(obj, dict):
            return {
                key: DatahubIngestionRunSummaryProvider._convert_sets_to_lists(value)
                for key, value in obj.items()
            }
        elif isinstance(obj, list):
            return [
                DatahubIngestionRunSummaryProvider._convert_sets_to_lists(element)
                for element in obj
            ]
        elif isinstance(obj, set):
            return [
                DatahubIngestionRunSummaryProvider._convert_sets_to_lists(element)
                for element in obj
            ]
        elif isinstance(obj, tuple):
            return tuple(
                DatahubIngestionRunSummaryProvider._convert_sets_to_lists(element)
                for element in obj
            )
        else:
            return obj

    def _get_recipe_to_report(self, ctx: PipelineContext) -> str:
        assert ctx.pipeline_config
        if not self.report_recipe or not ctx.pipeline_config.get_raw_dict():
            return ""
        else:
            redacted_recipe = redact_raw_config(ctx.pipeline_config.get_raw_dict())
            # This is required otherwise json dumps will fail
            # with a TypeError: Object of type set is not JSON serializable
            converted_recipe = (
                DatahubIngestionRunSummaryProvider._convert_sets_to_lists(
                    redacted_recipe
                )
            )
            return json.dumps(converted_recipe)

    def _emit_aspect(self, entity_urn: Urn, aspect_value: _Aspect) -> None:
        self.sink.write_record_async(
            RecordEnvelope(
                record=MetadataChangeProposalWrapper(
                    entityUrn=str(entity_urn),
                    aspect=aspect_value,
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
            aspect_value=execution_input_aspect,
        )

    def on_completion(
        self,
        status: str,
        report: Dict[str, Any],
        ctx: PipelineContext,
    ) -> None:
        # Prepare a nicely formatted summary
        structured_report_str = json.dumps(report, indent=2)
        summary = f"~~~~ Ingestion Report ~~~~\n{structured_report_str}\n\n"
        summary += "~~~~ Ingestion Logs ~~~~\n"
        summary += get_log_buffer().format_lines()

        # Construct the dataHubExecutionRequestResult aspect
        structured_report = StructuredExecutionReportClass(
            type="CLI_INGEST",
            serializedValue=structured_report_str,
            contentType=JSON_CONTENT_TYPE,
        )
        execution_result_aspect = ExecutionRequestResultClass(
            status=status,
            startTimeMs=self.start_time_ms,
            durationMs=self.get_cur_time_in_ms() - self.start_time_ms,
            # Truncate summary such that the generated MCP will not exceed GMS's payload limit.
            # Hardcoding the overall size of dataHubExecutionRequestResult to >1MB by trimming summary to 800,000 chars
            report=summary[-self._MAX_SUMMARY_SIZE :],
            structuredReport=structured_report,
        )

        # Emit the dataHubExecutionRequestResult aspect
        self._emit_aspect(
            entity_urn=self.execution_request_input_urn,
            aspect_value=execution_result_aspect,
        )
        self.sink.close()
