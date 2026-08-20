# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import time
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Optional

from pydantic import ConfigDict, field_validator

from datahub.configuration.common import OperationalError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.executor.common.env_config import get_payload_max_length
from datahub.executor.execution.default_executor import (
    DefaultExecutor,
    DefaultExecutorConfig,
)
from datahub.executor.execution.task import TaskConfig
from datahub.executor.request.execution_request import (
    ExecutionProgressCallback,
    ExecutionRequest,
)
from datahub.executor.request.signal_request import SignalRequest
from datahub.executor.result.execution_result import ExecutionResult, Type
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.schema_classes import (
    ExecutionRequestKeyClass,
    ExecutionRequestResultClass,
    StructuredExecutionReportClass,
    SystemMetadataClass,
)
from datahub.secret.secret_store import SecretStoreConfig

DATAHUB_EXECUTION_REQUEST_ENTITY_NAME = "dataHubExecutionRequest"
DATAHUB_EXECUTION_REQUEST_RESULT_ASPECT_NAME = "dataHubExecutionRequestResult"
REPORTS_TO_EMIT_MAX_SIZE = 10

logger = logging.getLogger(__name__)


class ReportingExecutorConfig(DefaultExecutorConfig):
    id: str = "default"
    task_configs: list[TaskConfig]
    secret_stores: list[SecretStoreConfig]

    graph_client: Optional[DataHubGraph] = None
    graph_client_config: Optional[DatahubClientConfig] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator("graph_client", mode="after")
    @classmethod
    def check_graph_connection(
        cls, v: Optional[DataHubGraph]
    ) -> Optional[DataHubGraph]:
        if v is not None:
            v.test_connection()
        return v


# Supports RUN_INGEST commands.
class ReportingExecutor(DefaultExecutor):
    _datahub_graph: DataHubGraph
    _execution_timeout_sec: Optional[float] = 86400  # Default timeout is 1 day

    def __init__(self, exec_config: ReportingExecutorConfig) -> None:
        super().__init__(exec_config)
        # Stores execution results in case completion mcp can't be emitted
        # Implements LRU cache via OrderedDict to avoid memory leak
        self.results_to_emit: OrderedDict[str, ExecutionResult] = OrderedDict()
        self.dropped_reports: set[str] = set()  # Leaks memory slowly, like task_futures
        if exec_config.graph_client is not None:
            self._datahub_graph = exec_config.graph_client
        elif exec_config.graph_client_config is not None:
            self._datahub_graph = DataHubGraph(exec_config.graph_client_config)
        else:
            raise Exception(
                "Invalid configuration provided. Missing DataHub graph client configs"
            )

    # Run a list of tasks in sequence
    def execute(self, request: ExecutionRequest) -> ExecutionResult:
        # Capture execution start time
        request.start_time = datetime.now(timezone.utc)

        # Build & emit an ACK mcp
        kickoff_mcp = self._build_kickoff_mcp(request)
        self._datahub_graph.emit_mcp(kickoff_mcp, async_flag=False)

        # Execute the request
        request.progress_callback = self._make_progress_callback(request)
        exec_result = super().execute(request)

        if request.exec_id is not None:
            self.results_to_emit[request.exec_id] = exec_result
            if len(self.results_to_emit) > REPORTS_TO_EMIT_MAX_SIZE:
                exec_id, _ = self.results_to_emit.popitem(last=False)
                self.dropped_reports.add(exec_id)

        completion_mcp = self._build_completion_mcp(exec_result)
        # Additional retry logic as we really want to emit here, for total of 15 minutes
        for retry in range(6):
            if retry:
                time.sleep(60 * retry)
            if self._emit_completion_mcp(request.exec_id, completion_mcp):
                break

        return exec_result

    def signal(self, request: SignalRequest) -> None:
        super().signal(request)
        # First, try to emit stored completion MCP
        # If that doesn't exist and the task cannot be found (is not actively executing)
        # emit an empty cancellation event
        if request.signal == "KILL":
            exec_id = request.exec_id
            should_emit_cancellation = (
                exec_id not in self.task_futures or exec_id in self.dropped_reports
            )

            exec_result = self.results_to_emit.get(exec_id)
            if exec_result:
                completion_mcp = self._build_completion_mcp(exec_result)
                if self._emit_completion_mcp(exec_id, completion_mcp):
                    return
                else:
                    should_emit_cancellation = True

            if should_emit_cancellation:
                # No task found. Simply emit a cancelled MCE. The start time is unclear.
                # Build & emit the cancellation mcp
                cancellation_mcp = self._build_empty_cancel_mcp(exec_id=exec_id)
                self._datahub_graph.emit_mcp(cancellation_mcp, async_flag=False)

    def _make_progress_callback(
        self, exec_request: ExecutionRequest
    ) -> ExecutionProgressCallback:
        def callback(partial_report: str) -> None:
            try:
                mcp = self._build_progress_mcp(
                    exec_request,
                    exec_request.start_time_ms,
                    partial_report=partial_report,
                )
                self._datahub_graph.emit_mcp(mcp, async_flag=False)
            except Exception:
                logger.warning("Failed to emit progress MCP", exc_info=True)

        return callback

    def _shared_system_metadata(self) -> SystemMetadataClass:
        sm = SystemMetadataClass()
        sm.properties = {}
        sm.properties["appSource"] = "reportingExecutor"
        return sm

    # Builds an MCP to report the start of execution request handling
    def _build_kickoff_mcp(
        self, exec_request: ExecutionRequest
    ) -> MetadataChangeProposalWrapper:
        assert exec_request.exec_id, f"Missing exec_id for request {exec_request}"
        key_aspect = self._build_execution_request_key_aspect(exec_request.exec_id)
        result_aspect = self._build_execution_request_result_aspect(
            status=Type.RUNNING.name, start_time_ms=exec_request.start_time_ms
        )

        return MetadataChangeProposalWrapper(
            entityType=DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
            changeType="UPSERT",
            auditHeader=None,
            entityKeyAspect=key_aspect,
            aspectName=DATAHUB_EXECUTION_REQUEST_RESULT_ASPECT_NAME,
            aspect=result_aspect,
            systemMetadata=self._shared_system_metadata(),
        )

    def _build_progress_mcp(
        self, exec_request: ExecutionRequest, start_time_ms: int, partial_report: str
    ) -> MetadataChangeProposalWrapper:
        assert exec_request.exec_id, f"Missing exec_id for request {exec_request}"
        key_aspect = self._build_execution_request_key_aspect(exec_request.exec_id)
        result_aspect = self._build_execution_request_result_aspect(
            status=Type.RUNNING.name,
            start_time_ms=start_time_ms,
            report=partial_report,
        )

        return MetadataChangeProposalWrapper(
            entityType=DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
            changeType="UPSERT",
            auditHeader=None,
            entityKeyAspect=key_aspect,
            aspectName=DATAHUB_EXECUTION_REQUEST_RESULT_ASPECT_NAME,
            aspect=result_aspect,
            systemMetadata=self._shared_system_metadata(),
        )

    def _build_completion_mcp(
        self, exec_result: ExecutionResult
    ) -> MetadataChangeProposalWrapper:
        # TODO: Support timed out state.

        exec_request = exec_result.context.request
        status = exec_result.type.name
        report = exec_result.get_summary()
        structured_report = exec_result.get_structured_report()

        start_time_ms = exec_request.start_time_ms
        end_time_ms = (
            exec_result.end_time_ms
            if exec_result.end_time_ms
            else int(time.time() * 1000)
        )

        key_aspect = self._build_execution_request_key_aspect(
            exec_request.exec_id or "missing execution id"
        )
        result_aspect = self._build_execution_request_result_aspect(
            status=status,
            start_time_ms=start_time_ms,
            duration_ms=end_time_ms - start_time_ms if end_time_ms else None,
            report=report,
            structured_report=structured_report,
            exec_request=exec_request,
        )

        return MetadataChangeProposalWrapper(
            entityType=DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
            changeType="UPSERT",
            auditHeader=None,
            entityKeyAspect=key_aspect,
            aspect=result_aspect,
            systemMetadata=self._shared_system_metadata(),
        )

    # Builds an MCP to report the completion of execution request handling
    def _build_empty_cancel_mcp(self, exec_id: str) -> MetadataChangeProposalWrapper:
        key_aspect = self._build_execution_request_key_aspect(exec_id)

        # TODO: Determine whether this is the "right" thing to do.
        result_aspect = self._build_execution_request_result_aspect(
            status=Type.CANCELLED.name,
            start_time_ms=0,  # TODO: Make start time optional
            duration_ms=None,
            report="No active execution request found.",
        )

        return MetadataChangeProposalWrapper(
            entityType=DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
            entityKeyAspect=key_aspect,
            aspect=result_aspect,
            systemMetadata=self._shared_system_metadata(),
        )

    def _build_execution_request_key_aspect(
        self, execution_request_id: str
    ) -> ExecutionRequestKeyClass:
        return ExecutionRequestKeyClass(id=execution_request_id)

    def _build_execution_request_result_aspect(
        self,
        status: str,
        start_time_ms: int,
        duration_ms: Optional[int] = None,
        report: Optional[str] = None,
        structured_report: Optional[str] = None,
        exec_request: Optional[ExecutionRequest] = None,
    ) -> ExecutionRequestResultClass:
        # If total MCP size exceeds INGESTION_MAX_SERIALIZED_STRING_LENGTH and/or MySQL's max_allowed_packet,
        # which causes hard-to-explain behavior througout the stack. To protect from this report/structured_report
        # is truncated/removed if it exceeds the limit, and a warning is logged.

        max_length = get_payload_max_length()
        if (
            structured_report is not None
            and report is not None
            and ((len(structured_report) + len(report)) > max_length)
        ):
            exec_id = exec_request.exec_id if exec_request else "unknown"
            message = f"{exec_id} structured report exceeded limit of {max_length} chars and was truncated."
            logger.warning(message)
            structured_report = None
            report = f"WARNING: {message}\n{report}"

        if report is not None and len(report) > max_length:
            exec_id = exec_request.exec_id if exec_request else "unknown"
            message = f"{exec_id} report exceeded limit of {max_length} chars and was truncated."
            logger.warning(message)
            report = report[:max_length]
            report = f"WARNING: {message}\n{report}"

        # Build the arguments for ExecutionRequestResultClass
        result_args = {
            "status": status,
            "startTimeMs": start_time_ms,
            "durationMs": duration_ms,
            "report": report,
            "structuredReport": StructuredExecutionReportClass(
                type=exec_request.name,
                serializedValue=structured_report,
                contentType="application/json",
            )
            if structured_report and exec_request
            else None,
        }

        # Included only when configured. The ExecutionRequestResult aspect does not
        # declare executorInstanceId, and generated aspect classes reject unknown
        # keyword arguments, so passing it unconditionally would fail. Deployments
        # that set it supply a custom models package whose aspect declares the field
        # (see datahub.utilities._custom_package_loader).
        if self._config.executor_instance_id is not None:
            result_args["executorInstanceId"] = self._config.executor_instance_id

        return ExecutionRequestResultClass(**result_args)  # type: ignore[arg-type]

    def _emit_completion_mcp(
        self, exec_id: Optional[str], mcp: MetadataChangeProposalWrapper
    ) -> bool:
        noretry_status = [401, 404, 422]
        try:
            self._datahub_graph.emit_mcp(mcp, async_flag=False)
        except Exception as e:
            if isinstance(e, OperationalError):
                status = e.info.get("status", -1)
                if status in noretry_status:
                    logger.warning(
                        f"Will not retry failed completion MCP for status code {status}"
                    )
                    return True
            logger.warning("Failed to emit completion MCP", exc_info=True)
            return False

        if exec_id:
            self.results_to_emit.pop(exec_id, None)
        return True
