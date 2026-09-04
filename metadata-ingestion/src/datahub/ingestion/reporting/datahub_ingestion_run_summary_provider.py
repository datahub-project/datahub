import copy
import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

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
from datahub.emitter.rest_emitter import EmitMode
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.pipeline_run_listener import PipelineRunListener
from datahub.ingestion.api.sink import NoopWriteCallback, Sink
from datahub.ingestion.run.pipeline_config import PipelineConfig
from datahub.ingestion.sink.sink_registry import sink_registry
from datahub.masking.masking_filter import SecretMaskingFilter
from datahub.masking.secret_registry import SecretRegistry
from datahub.metadata.schema_classes import (
    DataHubIngestionSourceConfigClass,
    DataHubIngestionSourceInfoClass,
    ExecutionRequestInputClass,
    ExecutionRequestResultClass,
    ExecutionRequestSourceClass,
    StructuredExecutionReportClass,
    _Aspect,
)
from datahub.metadata.urns import DataHubExecutionRequestUrn
from datahub.utilities.logging_manager import get_log_buffer
from datahub.utilities.urns.error import InvalidUrnError
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
    # Cap serializedValue at the same 800 KB budget so the whole
    # dataHubExecutionRequestResult MCP fits under Kafka's max.request.size.
    _MAX_STRUCTURED_REPORT_SIZE: int = 800000
    _MAX_STRUCTURED_REPORT_TRUNCATION_PASSES: int = 100

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
        reporter_config = DatahubIngestionRunSummaryProviderConfig.model_validate(
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

        self.ctx = ctx
        self.sink: Sink = sink
        self.report_recipe = report_recipe
        ingestion_source_key = self.generate_unique_key(ctx.pipeline_config)
        self.entity_name: str = self.generate_entity_name(ingestion_source_key)

        # If run_id is an execution request URN, the executor owns the source/request lifecycle.
        try:
            parsed = Urn.from_string(ctx.run_id)
            self._is_running_under_executor = (
                parsed.entity_type == DataHubExecutionRequestUrn.ENTITY_TYPE
            )
        except InvalidUrnError:
            self._is_running_under_executor = False
        except Exception:
            logger.warning(
                f"Unexpected error parsing run_id={ctx.run_id!r} as URN; "
                "assuming standalone CLI context.",
                exc_info=True,
            )
            self._is_running_under_executor = False

        if self._is_running_under_executor:
            logger.debug(f"Executor-managed run detected (run_id={ctx.run_id}).")

        self.ingestion_source_urn: Urn = Urn(
            entity_type="dataHubIngestionSource",
            entity_id=["cli-" + datahub_guid(ingestion_source_key)],
        )
        logger.debug(f"Ingestion source urn = {self.ingestion_source_urn}")
        # Use typed URN only in the executor path (run_id already validated as such).
        # For standalone CLI runs, run_id is a plain string; passing a foreign URN type
        # to DataHubExecutionRequestUrn would raise InvalidUrnError.
        if self._is_running_under_executor:
            self.execution_request_input_urn: Urn = DataHubExecutionRequestUrn(
                ctx.run_id
            )
        else:
            self.execution_request_input_urn = Urn(
                entity_type="dataHubExecutionRequest", entity_id=[ctx.run_id]
            )
        self.start_time_ms: int = self.get_cur_time_in_ms()

        if not self._is_running_under_executor:
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
    def _collect_lists(obj: Any) -> List[list]:
        found: List[list] = []
        if isinstance(obj, dict):
            for value in obj.values():
                found.extend(DatahubIngestionRunSummaryProvider._collect_lists(value))
        elif isinstance(obj, list):
            found.append(obj)
            for value in obj:
                found.extend(DatahubIngestionRunSummaryProvider._collect_lists(value))
        return found

    @staticmethod
    def _truncate_report_to_fit(
        report: Dict[str, Any], max_size: int
    ) -> Tuple[Dict[str, Any], bool]:
        # Halve the largest list repeatedly until the serialized JSON fits
        # under max_size. Preserves top-level shape (the frontend parses this)
        # and leaves a sentinel entry so the truncation is visible.
        # Non-mutating.
        if len(json.dumps(report, indent=2)) <= max_size:
            return report, False

        shrunk = copy.deepcopy(report)
        for _ in range(
            DatahubIngestionRunSummaryProvider._MAX_STRUCTURED_REPORT_TRUNCATION_PASSES
        ):
            candidates = [
                lst
                for lst in DatahubIngestionRunSummaryProvider._collect_lists(shrunk)
                if len(lst) > 1
            ]
            if not candidates:
                break
            biggest = max(candidates, key=lambda lst: len(json.dumps(lst)))
            keep = max(1, len(biggest) // 2)
            dropped = len(biggest) - keep
            biggest[keep:] = [f"[truncated {dropped} entries for size]"]
            if len(json.dumps(shrunk, indent=2)) <= max_size:
                return shrunk, True
        return shrunk, True

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
        elif isinstance(obj, (list, set)):
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

    def _emit_aspect(
        self, entity_urn: Urn, aspect_value: _Aspect, try_sync: bool = False
    ) -> None:
        mcp = MetadataChangeProposalWrapper(
            entityUrn=str(entity_urn),
            aspect=aspect_value,
        )

        if try_sync and self.ctx.graph:
            self.ctx.graph.emit_mcp(mcp, emit_mode=EmitMode.SYNC_PRIMARY)
        else:
            self.sink.write_record_async(
                RecordEnvelope(
                    record=mcp,
                    metadata={},
                ),
                NoopWriteCallback(),
            )

    def on_start(self, ctx: PipelineContext) -> None:
        assert ctx.pipeline_config is not None

        if self._is_running_under_executor:
            return

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
            try_sync=True,
        )

    def on_completion(
        self,
        status: str,
        report: Dict[str, Any],
        ctx: PipelineContext,
    ) -> None:
        masking_filter = SecretMaskingFilter(SecretRegistry.get_instance())
        masked_report = masking_filter.mask_structure(report)
        structured_report_str = json.dumps(masked_report, indent=2)
        summary = f"~~~~ Ingestion Report ~~~~\n{structured_report_str}\n\n"
        summary += "~~~~ Ingestion Logs ~~~~\n"
        summary += masking_filter.mask_text(get_log_buffer().format_lines())

        # Truncate on the masked report so redacted values survive the trim.
        structured_report = self._build_structured_report(
            masked_report, structured_report_str
        )

        execution_result_aspect = ExecutionRequestResultClass(
            status=status,
            startTimeMs=self.start_time_ms,
            durationMs=self.get_cur_time_in_ms() - self.start_time_ms,
            report=summary[-self._MAX_SUMMARY_SIZE :],
            structuredReport=structured_report,
        )

        self._emit_aspect(
            entity_urn=self.execution_request_input_urn,
            aspect_value=execution_result_aspect,
        )

    def _build_structured_report(
        self, report: Dict[str, Any], full_serialized: str
    ) -> Optional[StructuredExecutionReportClass]:
        # `report` is already capped elsewhere; serializedValue used to be
        # written in full, which pushed large runs past Kafka's
        # max.request.size and left them stuck in PENDING.
        if len(full_serialized) <= self._MAX_STRUCTURED_REPORT_SIZE:
            return StructuredExecutionReportClass(
                type="CLI_INGEST",
                serializedValue=full_serialized,
                contentType=JSON_CONTENT_TYPE,
            )

        bounded_report, _ = self._truncate_report_to_fit(
            report, self._MAX_STRUCTURED_REPORT_SIZE
        )
        bounded_serialized = json.dumps(bounded_report, indent=2)
        if len(bounded_serialized) <= self._MAX_STRUCTURED_REPORT_SIZE:
            logger.info(
                "Truncated ingestion structured report from %d to %d chars "
                "to fit under the execution-result aspect size limit.",
                len(full_serialized),
                len(bounded_serialized),
            )
            return StructuredExecutionReportClass(
                type="CLI_INGEST",
                serializedValue=bounded_serialized,
                contentType=JSON_CONTENT_TYPE,
            )

        # Nothing left to shrink (e.g. a single huge string value). Skip
        # structuredReport so the completion aspect still lands and the run
        # transitions out of PENDING; the plain-text `report` field is enough.
        logger.warning(
            "Ingestion structured report is %d chars even after truncation; "
            "omitting structuredReport so the run can complete.",
            len(bounded_serialized),
        )
        return None
