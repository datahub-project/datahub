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
import contextlib
import json
import logging
import os
import sys
import time
from typing import Dict, Iterator, List, Optional

from acryl.executor.dispatcher.default_dispatcher import DefaultDispatcher
from acryl.executor.execution.reporting_executor import (
    ReportingExecutor,
    ReportingExecutorConfig,
)
from acryl.executor.execution.task import TaskConfig
from acryl.executor.request.execution_request import ExecutionRequest
from acryl.executor.request.signal_request import SignalRequest
from pydantic import BaseModel, ConfigDict, Field

from datahub.metadata.schema_classes import MetadataChangeLogClass
from datahub.secret.datahub_secret_store import DataHubSecretStoreConfig
from datahub.secret.secret_store import SecretStoreConfig
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import METADATA_CHANGE_LOG_EVENT_V1_TYPE
from datahub_actions.pipeline.pipeline_context import PipelineContext

logger = logging.getLogger(__name__)


def _parse_json_list(raw: str, field_name: str = "value") -> List[str]:
    """Parse a JSON string as a list of strings.

    Delegates to datahub.utilities.ingest_utils.parse_json_list when available,
    with a minimal fallback for environments where the import path differs.
    """
    try:
        from datahub.utilities.ingest_utils import parse_json_list

        return parse_json_list(raw, field_name)
    except ImportError:
        if not raw or not raw.strip():
            return []
        parsed = json.loads(raw)
        if not isinstance(parsed, list) or not all(isinstance(s, str) for s in parsed):
            raise ValueError(f"{field_name} must be a JSON array of strings") from None
        return parsed


DATAHUB_EXECUTION_REQUEST_ENTITY_NAME = "dataHubExecutionRequest"
DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME = "dataHubExecutionRequestInput"
DATAHUB_EXECUTION_REQUEST_SIGNAL_ASPECT_NAME = "dataHubExecutionRequestSignal"
APPLICATION_JSON_CONTENT_TYPE = "application/json"


class ExecutorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    executor_id: Optional[str] = Field(
        default=None,
        description="ID of the executor; defaults to 'default' at runtime.",
    )
    task_configs: Optional[List[TaskConfig]] = Field(
        default=None,
        description="Custom task configurations; uses built-in defaults when absent.",
    )


# Listens to new Execution Requests & dispatches them to the appropriate handler.
class ExecutorAction(Action):
    @classmethod
    def create(cls, config_dict: Dict[str, object], ctx: PipelineContext) -> "Action":
        config = ExecutorConfig.model_validate(config_dict or {})
        return cls(config, ctx)

    def __init__(self, config: ExecutorConfig, ctx: PipelineContext):
        self.ctx = ctx

        executors = []

        executor_config = self._build_executor_config(config, ctx)
        executors.append(ReportingExecutor(executor_config))

        # Construct execution request dispatcher
        self.dispatcher = DefaultDispatcher(executors)

    def act(self, event: EventEnvelope) -> None:
        """This method listens for ExecutionRequest changes to execute in schedule and trigger events"""
        if event.event_type is METADATA_CHANGE_LOG_EVENT_V1_TYPE:
            if not isinstance(event.event, MetadataChangeLogClass):
                logger.warning(
                    "Expected MetadataChangeLogClass but got %s",
                    type(event.event).__name__,
                )
                return
            orig_event = event.event
            if (
                orig_event.get("entityType") == DATAHUB_EXECUTION_REQUEST_ENTITY_NAME
                and orig_event.get("changeType") == "UPSERT"
            ):
                if (
                    orig_event.get("aspectName")
                    == DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME
                ):
                    logger.debug("Received execution request input. Processing...")
                    self._handle_execution_request_input(orig_event)
                elif (
                    orig_event.get("aspectName")
                    == DATAHUB_EXECUTION_REQUEST_SIGNAL_ASPECT_NAME
                ):
                    logger.debug("Received execution request signal. Processing...")
                    self._handle_execution_request_signal(orig_event)

    @staticmethod
    def _extract_exec_id(orig_event: MetadataChangeLogClass) -> Optional[str]:
        """Extract the execution request ID from the event key or URN."""
        entity_key = orig_event.get("entityKeyAspect")
        if entity_key is not None:
            raw_key = entity_key.get("value")
            if raw_key is not None:
                try:
                    return json.loads(raw_key).get("id")
                except (json.JSONDecodeError, TypeError):
                    logger.warning(
                        "Failed to parse entityKeyAspect value as JSON: %s",
                        raw_key[:200] if raw_key else raw_key,
                    )
                    return None
            logger.warning("entityKeyAspect present but 'value' is None")
            return None

        entity_urn = orig_event.get("entityUrn")
        if entity_urn is not None:
            return entity_urn.split(":")[-1]
        return None

    def _handle_execution_request_input(
        self, orig_event: MetadataChangeLogClass
    ) -> None:
        exec_request_id = self._extract_exec_id(orig_event)

        # Decode the aspect json into something more readable :)
        aspect = orig_event.get("aspect")
        if aspect is None:
            logger.warning(
                "Missing aspect in execution request event for %s", exec_request_id
            )
            return
        try:
            exec_request_input = json.loads(aspect.get("value"))
        except (json.JSONDecodeError, TypeError) as e:
            logger.error(
                "Failed to parse execution request aspect as JSON for %s: %s",
                exec_request_id,
                e,
            )
            self._report_execution_failure(
                exec_request_id,
                f"Failed to parse execution request input: {e}",
            )
            return

        task_name = exec_request_input.get("task")
        args = exec_request_input.get("args")

        # Resolve external plugins before running ingestion
        if task_name == "RUN_INGEST" and args:
            try:
                self._install_external_plugins(args)
            except Exception as e:
                logger.error(
                    "Plugin resolution failed, aborting execution %s: %s",
                    exec_request_id,
                    e,
                    exc_info=True,
                )
                self._report_execution_failure(
                    exec_request_id, f"Failed to resolve external plugins: {e}"
                )
                return

        # Build an Execution Request
        exec_request = ExecutionRequest(
            executor_id=exec_request_input.get("executorId"),
            exec_id=exec_request_id,
            name=task_name,
            args=args,
        )

        # Try to dispatch the execution request
        try:
            self.dispatcher.dispatch(exec_request)
        except Exception:
            logger.error(
                "Failed to dispatch execution request %s",
                exec_request_id,
                exc_info=sys.exc_info(),
            )
            self._report_execution_failure(
                exec_request_id,
                f"Failed to dispatch execution request: {sys.exc_info()[1]}",
            )

    def _handle_execution_request_signal(
        self, orig_event: MetadataChangeLogClass
    ) -> None:
        entity_urn = orig_event.get("entityUrn")

        aspect = orig_event.get("aspect")
        if aspect is None or entity_urn is None:
            logger.debug(
                "Ignoring signal event with missing aspect or entityUrn "
                "(aspect=%s, entityUrn=%s)",
                "present" if aspect is not None else "None",
                entity_urn,
            )
            return

        if aspect.get("contentType") == APPLICATION_JSON_CONTENT_TYPE:
            try:
                signal_request_input = json.loads(aspect.get("value"))
            except (json.JSONDecodeError, TypeError) as e:
                logger.error(
                    "Failed to parse signal aspect as JSON for %s: %s",
                    entity_urn,
                    e,
                )
                return

            exec_id = entity_urn.split(":")[-1]
            signal_request = SignalRequest(
                executor_id=signal_request_input.get("executorId"),
                exec_id=exec_id,
                signal=signal_request_input.get("signal"),
            )

            # Try to dispatch the signal request
            try:
                self.dispatcher.dispatch_signal(signal_request)
            except Exception:
                logger.error(
                    "Failed to dispatch signal for execution %s",
                    exec_id,
                    exc_info=sys.exc_info(),
                )

    @staticmethod
    @contextlib.contextmanager
    def _temporary_github_token(args: Dict[str, str]) -> Iterator[None]:
        """Temporarily inject GITHUB_TOKEN from extra_env_vars into os.environ.

        The UI passes credentials via extra_env_vars, but the GitHub resolver
        reads os.environ. This context manager bridges the gap and guarantees
        the original value is restored on exit.
        """
        extra_env_raw = args.get("extra_env_vars", "")
        try:
            extra_env = json.loads(extra_env_raw) if extra_env_raw else {}
        except (json.JSONDecodeError, TypeError):
            logger.warning(
                "Failed to parse extra_env_vars as JSON; GITHUB_TOKEN injection skipped"
            )
            extra_env = {}

        token = extra_env.get("GITHUB_TOKEN") if isinstance(extra_env, dict) else None
        if not token:
            yield
            return

        old_token = os.environ.get("GITHUB_TOKEN")
        os.environ["GITHUB_TOKEN"] = token
        try:
            yield
        finally:
            if old_token is not None:
                os.environ["GITHUB_TOKEN"] = old_token
            else:
                os.environ.pop("GITHUB_TOKEN", None)

    @staticmethod
    def _resolve_single_spec(spec: str, existing_reqs: List[str]) -> Optional[str]:
        """Resolve one plugin spec and append the result to *existing_reqs*.

        Supported spec formats:
        - ``github:owner/repo`` or ``github:owner/repo@version`` — resolved via GitHub API
        - ``pypi:package-name`` or ``pypi:package-name==version`` — passed directly to pip

        Returns an error string on failure, or ``None`` on success.
        """
        try:
            if spec.startswith("pypi:"):
                # PyPI packages are pip-installable directly
                pip_req = spec[len("pypi:") :]
                logger.info("Resolved %s -> pip requirement: %s", spec, pip_req)
                existing_reqs.append(pip_req)
            else:
                from datahub.plugin.github_resolver import (
                    ResolvedWheel,
                    download_wheel,
                    resolve_github_spec,
                )

                resolved = resolve_github_spec(spec)
                is_wheel = isinstance(resolved, ResolvedWheel)
                logger.info(
                    "Resolved %s -> %s (version=%s, wheel=%s)",
                    spec,
                    resolved.download_url,
                    resolved.version,
                    is_wheel,
                )
                # uv/pip can't send GitHub auth headers on its own,
                # so download wheels locally with auth for private repos.
                # Use isinstance directly (not the is_wheel bool) so mypy narrows the type.
                if isinstance(resolved, ResolvedWheel):
                    local_path = download_wheel(resolved)
                    existing_reqs.append(local_path)
                else:
                    existing_reqs.append(resolved.download_url)
        except Exception as e:
            logger.error("Failed to resolve external plugin: %s", spec, exc_info=True)
            return f"{spec}: {e}"
        return None

    def _install_external_plugins(self, args: Dict[str, str]) -> None:
        """Resolve external DataHub plugin specs and inject them into extra_pip_requirements.

        The datahub_plugins arg is a JSON-encoded list of plugin specs
        (e.g. '["github:owner/repo", "github:owner/other@v1.0"]').
        Each spec is resolved to a pip-installable URL (wheel download URL
        or git+https://) and appended to extra_pip_requirements so that
        SubProcessIngestionTask installs them in the subprocess venv.
        """
        raw = args.get("datahub_plugins")
        if not raw:
            return

        try:
            specs = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            logger.warning(
                "datahub_plugins is not valid JSON; treating as single spec: %s",
                raw[:200],
            )
            specs = [raw]

        if not isinstance(specs, list):
            raise ValueError(
                f"datahub_plugins must be a JSON array of strings, "
                f"but got {type(specs).__name__}. "
                f"Example: '[\"github:owner/repo\"]'"
            )
        if not specs:
            return

        try:
            existing_reqs = _parse_json_list(
                args.get("extra_pip_requirements", ""), "extra_pip_requirements"
            )
        except ValueError:
            logger.warning(
                "extra_pip_requirements is malformed; starting with empty list",
                exc_info=True,
            )
            existing_reqs = []
        errors: List[str] = []

        with self._temporary_github_token(args):
            for spec in specs:
                if not isinstance(spec, str) or not spec.strip():
                    logger.debug("Skipping blank or non-string plugin spec: %r", spec)
                    continue
                spec = spec.strip()
                logger.info("Resolving external plugin spec: %s", spec)
                error = self._resolve_single_spec(spec, existing_reqs)
                if error:
                    errors.append(error)

        if errors:
            raise RuntimeError(
                "Failed to resolve external plugin(s): " + "; ".join(errors)
            )

        args["extra_pip_requirements"] = json.dumps(existing_reqs)

    def _report_execution_failure(
        self, exec_id: Optional[str], error_message: str
    ) -> None:
        """Report an execution failure directly to GMS so it's visible in the UI."""
        try:
            from datahub.emitter.mcp import MetadataChangeProposalWrapper
            from datahub.metadata.schema_classes import (
                ExecutionRequestKeyClass,
                ExecutionRequestResultClass,
            )

            now_ms = int(time.time() * 1000)
            key = ExecutionRequestKeyClass(id=exec_id or "unknown")
            result = ExecutionRequestResultClass(
                status="FAILURE",
                report=error_message,
                startTimeMs=now_ms,
                durationMs=0,
            )
            mcp = MetadataChangeProposalWrapper(
                entityType=DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
                changeType="UPSERT",
                entityKeyAspect=key,
                aspect=result,
            )
            if self.ctx.graph is None:
                logger.warning(
                    "No DataHub graph available; cannot report execution failure for %s",
                    exec_id,
                )
                return
            graph = self.ctx.graph.graph
            graph.emit_mcp(mcp, async_flag=False)
            logger.info("Reported execution failure for %s to GMS", exec_id)
        except Exception:
            # Preserve the original error message so operators can still diagnose
            logger.error(
                "Failed to report execution failure to GMS for %s. Original error: %s",
                exec_id,
                error_message,
                exc_info=True,
            )

    def _build_executor_config(
        self, config: ExecutorConfig, ctx: PipelineContext
    ) -> ReportingExecutorConfig:
        if not ctx.graph:
            raise Exception(
                "Invalid configuration provided to action. DataHub Graph Client Required. Try including the 'datahub' block in your configuration."
            )

        graph = ctx.graph.graph

        if config.task_configs:
            task_configs = config.task_configs
        else:
            task_configs = [
                TaskConfig(
                    name="RUN_INGEST",
                    type="acryl.executor.execution.sub_process_ingestion_task.SubProcessIngestionTask",
                    configs={},
                ),
                TaskConfig(
                    name="TEST_CONNECTION",
                    type="acryl.executor.execution.sub_process_test_connection_task.SubProcessTestConnectionTask",
                    configs={},
                ),
            ]

        # Build default executor config
        local_executor_config = ReportingExecutorConfig(
            id=config.executor_id or "default",
            task_configs=task_configs,
            secret_stores=[
                SecretStoreConfig(type="env", config={}),
                SecretStoreConfig(
                    type="datahub",
                    # TODO: Once SecretStoreConfig is updated to accept arbitrary types
                    # and not just dicts, we can just pass in the DataHubSecretStoreConfig
                    # object directly.
                    config=DataHubSecretStoreConfig(graph_client=graph).model_dump(),
                ),
            ],
            graph_client=graph,
        )

        return local_executor_config

    def close(self) -> None:
        # TODO: Handle closing action ingestion processing.
        pass
