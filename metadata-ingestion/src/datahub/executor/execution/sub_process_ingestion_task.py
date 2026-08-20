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

import asyncio
import asyncio.exceptions
import json
import logging
import os
import signal
import sys
from asyncio import tasks
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import IO, Any, Optional

import pydantic
import yaml
from pydantic import Field

from datahub.configuration.env_vars import get_debug
from datahub.executor.common.config import ConfigModel
from datahub.executor.common.env_config import get_print_subprocess_logs
from datahub.executor.context.execution_context import ExecutionContext
from datahub.executor.context.executor_context import ExecutorContext
from datahub.executor.execution.runner import (
    LogHolder,
    SubprocessRunner,
    VenvConfig,
    VenvReference,
    setup_venv,
)
from datahub.executor.execution.sub_process_task_common import (
    SubProcessRecipeTaskArgs,
    SubProcessTaskUtil,
    resolve_wrapper_script,
)
from datahub.executor.execution.task import Task, TaskError
from datahub.masking.bootstrap import shutdown_secret_masking
from datahub.masking.masking_filter import SecretMaskingFilter
from datahub.masking.secret_registry import SecretRegistry

logger = logging.getLogger(__name__)

ARTIFACTS_DIR_NAME = "artifacts"


class SubProcessIngestionTaskConfig(ConfigModel):
    tmp_dir: str = "/tmp/datahub/ingest"
    log_dir: str = "/tmp/datahub/logs"
    heartbeat_time_seconds: int = 2
    max_log_lines: int = SubProcessTaskUtil.MAX_LOG_LINES

    # Retired: these three configured uploading the artifact directory. This task now
    # just writes artifacts and publishes the directory on the ExecutionContext, and
    # whatever collects them supplies its own destination.
    #
    # Still ACCEPTED rather than removed, because ConfigModel is extra="forbid": a
    # leftover key in an operator's task_configs[].configs would otherwise fail
    # validation at startup, and "extra fields not permitted" gives no hint why.
    # Accepted-and-warned degrades; forbidden does not.
    cloud_log_bucket: Optional[str] = None
    cloud_log_path: Optional[str] = None
    cloud_log_cleanup: Optional[bool] = None

    @pydantic.model_validator(mode="after")
    def _warn_on_retired_cloud_log_fields(self) -> "SubProcessIngestionTaskConfig":
        retired = [
            name
            for name in ("cloud_log_bucket", "cloud_log_path", "cloud_log_cleanup")
            if getattr(self, name) is not None
        ]
        if retired:
            logger.warning(
                "Ignoring retired task config field(s) %s. This task no longer uploads "
                "logs or artifacts anywhere; it writes them to its artifact directory "
                "and publishes that path. Remove these from task_configs to silence "
                "this warning.",
                ", ".join(retired),
            )
        return self


class SubProcessIngestionTaskArgs(SubProcessRecipeTaskArgs):
    debug_mode: str = Field(
        default="false", alias="debugMode"
    )  # Expected values are "true" or "false".

    # Security: Hide input values in validation errors to prevent sensitive data exposure.
    # Ingestion task arguments may contain credentials, connection strings, API keys, and
    # other secrets that should not be logged to UI or error messages. Only show input
    # values when DATAHUB_DEBUG is explicitly enabled for troubleshooting purposes.
    model_config = pydantic.ConfigDict(
        populate_by_name=True, hide_input_in_errors=not get_debug()
    )

    @pydantic.field_validator("debug_mode", mode="before")
    @classmethod
    def normalize_debug_mode(cls, v: Any) -> str:
        if isinstance(v, str):
            return v.lower()
        return str(v).lower()


class SubProcessIngestionTask(Task):
    # How long to wait for the monitoring tasks to wind down after a cancellation
    # or a monitoring failure, before falling back to killing the subprocess.
    # A class attribute so tests can shorten it -- otherwise exercising that path
    # costs a full minute per test.
    CLEANUP_TIMEOUT_SECONDS = 60

    config: SubProcessIngestionTaskConfig
    tmp_dir: str  # Location where tmp files will be written (recipes)
    ctx: ExecutorContext

    @classmethod
    def create(cls, config: dict, ctx: ExecutorContext) -> "Task":
        return cls(SubProcessIngestionTaskConfig.model_validate(config), ctx)

    def __init__(self, config: SubProcessIngestionTaskConfig, ctx: ExecutorContext):
        self.config = config
        self.tmp_dir = config.tmp_dir
        self.ctx = ctx

    @contextmanager
    def _temporary_log_level(self, level: int) -> Generator:
        """Temporarily change the log level for the current logger and its handlers."""
        original_levels: dict[Any, int] = {}
        try:
            original_levels[logger] = logger.level
            for handler in logger.handlers:
                original_levels[handler] = handler.level
            logger.setLevel(level)
            for handler in logger.handlers:
                handler.setLevel(level)

            yield
        finally:
            logger.setLevel(original_levels[logger])
            for handler in logger.handlers:
                if handler in original_levels:
                    handler.setLevel(original_levels[handler])

    def _setup_directories(self, exec_id: str) -> tuple[str, str, str]:
        """Setup execution directories and return paths."""
        exec_out_dir = f"{self.tmp_dir}/{exec_id}"
        artifact_output_dir = f"{self.config.log_dir}/{exec_id}"
        mode = 0o755

        Path(exec_out_dir).mkdir(mode, parents=True, exist_ok=True)
        (Path(artifact_output_dir) / "executor-logs").mkdir(
            mode, parents=True, exist_ok=True
        )
        Path(artifact_output_dir).joinpath("artifacts").mkdir(
            mode, parents=True, exist_ok=True
        )

        return (
            exec_out_dir,
            artifact_output_dir,
            f"{artifact_output_dir}/artifacts/ingestion_report.json",
        )

    def _prepare_subprocess_environment(
        self,
        validated_args: SubProcessIngestionTaskArgs,
        exec_out_dir: str,
        artifact_output_dir: str,
    ) -> dict:
        """Prepare environment variables for subprocess."""
        subprocess_env = validated_args.get_combined_env_vars()

        subprocess_env["INGESTION_ARTIFACT_DIR"] = f"{artifact_output_dir}/artifacts"
        subprocess_env.setdefault("TMPDIR", exec_out_dir)

        # Enable secret masking in subprocess
        subprocess_env["DATAHUB_ENABLE_SECRET_MASKING"] = "true"

        # Set DATAHUB_DEBUG based on debug_mode to control hide_input_in_errors
        subprocess_env["DATAHUB_DEBUG"] = validated_args.debug_mode

        return subprocess_env

    async def _setup_venv(
        self,
        validated_args: SubProcessIngestionTaskArgs,
        plugin: str,
        exec_out_dir: str,
        shared_logs: LogHolder,
    ) -> VenvReference:
        """Set up the virtual environment using Python utilities with shared logging."""
        # Create venv configuration from subprocess args
        venv_config = VenvConfig(
            version=validated_args.version,
            main_plugin=plugin,
            extra_pip_requirements=validated_args.extra_pip_requirements,
            extra_pip_plugins=validated_args.extra_pip_plugins,
            extra_env_vars=validated_args.extra_env_vars,
        )

        # Use shared LogHolder for venv setup - logs will appear in subprocess output
        venv_runner = SubprocessRunner(logs=shared_logs)

        logger.info(
            f"Setting up venv for plugin '{plugin}' with version '{validated_args.version}'"
        )

        # Add venv setup status to shared logs so it appears in subprocess output
        shared_logs.append(
            f"Setting up venv for plugin '{plugin}' with version '{validated_args.version}'\n"
        )

        if validated_args.should_use_bundled_venv():
            logger.info("Using Bundled startup (pre-built) venv")
            shared_logs.append("Using Bundled startup (pre-built) venv\n")
        else:
            logger.info("Creating dynamic venv - this may take a few minutes...")
            shared_logs.append(
                "Creating dynamic venv - this may take a few minutes...\n"
            )

        try:
            # Set up the venv using our Python utilities
            venv_ref = await setup_venv(
                venv_config=venv_config,
                runner=venv_runner,
                tmp_dir=Path(exec_out_dir),
            )

            logger.info(f"Venv ready at: {venv_ref.venv_loc}")
            shared_logs.append(f"✅ Venv ready at: {venv_ref.venv_loc}\n")

            return venv_ref

        except Exception as e:
            error_msg = SubProcessTaskUtil.format_subprocess_error(e)
            logger.error(f"Venv setup failed: {error_msg}")
            shared_logs.append(f"❌ Venv setup failed: {error_msg}\n")
            raise TaskError(f"Failed to set up virtual environment: {error_msg}") from e

    async def _create_subprocess(
        self,
        validated_args: SubProcessIngestionTaskArgs,
        plugin: str,
        recipe: dict,
        report_out_file: str,
        subprocess_env: dict,
        exec_out_dir: str,
        shared_logs: LogHolder,
        secret_values: dict[str, str],
    ) -> asyncio.subprocess.Process:
        """Create and return the ingestion subprocess.

        Secrets and recipe are passed via stdin as a JSON envelope to avoid
        writing secrets to env vars or recipe to disk.
        """
        # First, set up the venv using Python utilities with shared logging
        venv_ref = await self._setup_venv(
            validated_args, plugin, exec_out_dir, shared_logs
        )

        # Now create subprocess with Python wrapper that enables secret masking
        # Invoked as a module with this interpreter rather than by bare name off PATH:
        # the wrapper must run in the executor's own environment (it then activates the
        # per-run target venv itself), and this code also mutates PATH, so resolving our
        # own helper through PATH would be fragile. By absolute path rather than -m:
        # see resolve_wrapper_script.
        command_script = resolve_wrapper_script("datahub.executor.wrappers.run_ingest")
        debug_mode = validated_args.debug_mode

        # Log the execution mode
        if validated_args.should_use_bundled_venv():
            logger.info(
                f"Running ingestion with Bundled startup venv: {venv_ref.venv_loc}"
            )
        else:
            logger.info(f"Running ingestion with dynamic venv: {venv_ref.venv_loc}")

        venv_env = {
            **subprocess_env,
            "VENV_PATH": str(venv_ref.venv_loc),
        }

        # Build stdin envelope in datahub-compatible format.
        # __recipe_yaml__ and __secrets__ are consumed by datahub's config_loader.
        # __report_out_file__ and __debug_mode__ are consumed by the wrapper script.
        # All envelope keys use dunder prefix to distinguish from recipe content.
        stdin_envelope = json.dumps(
            {
                "__recipe_yaml__": yaml.dump(recipe),
                "__secrets__": secret_values,
                "__report_out_file__": report_out_file,
                "__debug_mode__": debug_mode,
            }
        )

        process = await asyncio.create_subprocess_exec(
            sys.executable,
            command_script,
            str(venv_ref.venv_loc),
            env=venv_env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            limit=SubProcessTaskUtil.SUBPROCESS_BUFFER_SIZE,
            # Own process group, so cancellation can signal the whole tree. Without
            # this, terminating the wrapper leaves the datahub grandchild running.
            start_new_session=True,
        )

        # Write the envelope to stdin and close it
        assert process.stdin is not None
        process.stdin.write(stdin_envelope.encode("utf-8"))
        process.stdin.close()

        return process

    async def execute(self, args: dict, ctx: ExecutionContext) -> None:
        exec_id = ctx.exec_id  # The unique execution id.

        # 0. Validate arguments
        validated_args = SubProcessIngestionTaskArgs.model_validate(args)

        # Set debug log level if debug_mode is "true"
        if validated_args.debug_mode == "true":
            with self._temporary_log_level(logging.DEBUG):
                logger.debug("Debug mode enabled - setting log level to DEBUG")
                await self._execute_with_debug(validated_args, ctx, exec_id)
        else:
            await self._execute_with_debug(validated_args, ctx, exec_id)

    async def _execute_with_debug(
        self,
        validated_args: SubProcessIngestionTaskArgs,
        ctx: ExecutionContext,
        exec_id: str,
    ) -> None:
        """Execute the ingestion task with the given arguments."""
        # 1. Resolve the recipe and secrets (secrets stay in memory, not in os.environ)
        recipe, secret_values = SubProcessTaskUtil._resolve_recipe(
            validated_args.recipe, ctx, self.ctx
        )
        plugin: str = SubProcessTaskUtil._get_plugin_from_recipe(recipe)

        # 2. Setup directories
        exec_out_dir, artifact_output_dir, report_out_file = self._setup_directories(
            exec_id
        )

        # 3. Prepare subprocess environment and create subprocess
        subprocess_env = self._prepare_subprocess_environment(
            validated_args, exec_out_dir, artifact_output_dir
        )
        # Only log env var keys, not values — values may contain user-provided secrets
        logger.debug(f"Subprocess environment keys: {sorted(subprocess_env.keys())}")

        # Create shared LogHolder for both venv setup and subprocess monitoring
        shared_logs = LogHolder(
            max_log_lines=self.config.max_log_lines,
            echo_to_stdout_prefix=f"[{exec_id} logs] "
            if get_print_subprocess_logs()
            else None,
        )
        full_log_file = open(
            f"{artifact_output_dir}/executor-logs/ingestion-logs.log", "w"
        )

        logger.info(f"Starting ingestion subprocess for exec_id={exec_id} ({plugin})")
        ingest_process = await self._create_subprocess(
            validated_args,
            plugin,
            recipe,
            report_out_file,
            subprocess_env,
            exec_out_dir,
            shared_logs,
            secret_values,
        )

        # Publish the artifact directory on the context so callers can locate this run's
        # logs and artifacts after execute() returns. Unlike exec_out_dir, this directory
        # is not removed on completion.
        #
        # Deliberately after the subprocess exists, not right after the directories are
        # created. Venv setup happens inside _create_subprocess and can raise; at that
        # point ingestion-logs.log has been opened but nothing has been written to it
        # (venv output lives in shared_logs until _read_output_lines runs). Publishing
        # earlier would advertise a directory whose only log file is empty, so a consumer
        # that uploads artifacts would ship a zero-byte log for every venv or
        # dependency-resolution failure. Those failures are common, and the useful error
        # text is in the result report, not here.
        ctx.set_artifact_dir(artifact_output_dir)

        cancelled = False
        try:
            await self._monitor_subprocess(
                ingest_process, exec_id, ctx, shared_logs, full_log_file
            )
        except asyncio.CancelledError:
            # Track for the finally cleanup; the bare raise re-raises so
            # DefaultExecutor.execute_task reports the task as CANCELLED.
            cancelled = True
            raise
        finally:
            # _handle_subprocess_completion is contractually safe to call here:
            # it only raises TaskError on a real non-cancelled failure. All
            # cleanup steps are internally guarded, so this finally cannot mask
            # the in-flight CancelledError.
            self._handle_subprocess_completion(
                ingest_process,
                ctx,
                report_out_file,
                artifact_output_dir,
                recipe,
                exec_out_dir,
                shared_logs,
                cancelled=cancelled,
            )

    @staticmethod
    def _signal_process_group(process: asyncio.subprocess.Process, sig: int) -> None:
        """Signal the wrapper's whole process group so the datahub grandchild is
        terminated too, not just the direct child."""
        try:
            os.killpg(os.getpgid(process.pid), sig)
        except ProcessLookupError:
            pass  # already exited

    async def _monitor_subprocess(
        self,
        ingest_process: asyncio.subprocess.Process,
        exec_id: str,
        ctx: ExecutionContext,
        shared_logs: LogHolder,
        full_log_file: IO[str],
    ) -> None:
        """Monitor subprocess execution with async tasks for output reading and progress reporting."""
        most_recent_log_ts: Optional[datetime] = None

        async def _read_output_lines() -> None:
            nonlocal most_recent_log_ts
            while True:
                assert ingest_process.stdout

                # We can't use the readline method directly.
                # When the readline method hits a LimitOverrunError, it will
                # discard the line or possibly the entire buffer.
                try:
                    line_bytes = await ingest_process.stdout.readuntil(b"\n")
                except asyncio.exceptions.CancelledError:
                    logger.info(
                        f"Got asyncio.CancelledError for exec_id={exec_id} - stopping log monitor"
                    )
                    break
                except asyncio.exceptions.IncompleteReadError as e:
                    # This happens when we reach the end of the stream.
                    line_bytes = e.partial
                except asyncio.exceptions.LimitOverrunError:
                    line_bytes = await ingest_process.stdout.read(
                        SubProcessTaskUtil.MAX_BYTES_PER_LINE
                    )

                # At this point, if line_bytes is empty, then we're at EOF.
                # If it ends with a newline, then we successfully read a line.
                # If it does not end with a newline, then we hit a LimitOverrunError
                # and it contains a partial line.

                if not line_bytes:
                    logger.info(
                        f"Got EOF from subprocess exec_id={exec_id} - stopping log monitor"
                    )
                    break
                line = line_bytes.decode("utf-8")

                most_recent_log_ts = datetime.now(tz=timezone.utc)

                full_log_file.write(line)

                # Use LogHolder's built-in functionality - it handles all the line management
                shared_logs.append(line)

                await asyncio.sleep(0)

        async def _report_progress() -> None:
            while True:
                if ingest_process.returncode is not None:
                    logger.info(
                        f"Detected subprocess return code {ingest_process.returncode}, "
                        f"exec_id={exec_id} - stopping logs reporting"
                    )
                    break

                await asyncio.sleep(self.config.heartbeat_time_seconds)

                # Report progress
                if ctx.request.progress_callback:
                    if most_recent_log_ts is None:
                        report = "No logs yet"
                    else:
                        report = SubProcessTaskUtil._format_log_lines(
                            shared_logs.get_lines()
                        )
                        current_time = datetime.now(tz=timezone.utc)
                        if most_recent_log_ts < current_time - timedelta(minutes=2):
                            message = (
                                f"WARNING: These logs appear to be stale. No new logs have been received since {most_recent_log_ts} ({(current_time - most_recent_log_ts).seconds} seconds ago). "
                                "However, the ingestion process still appears to be running and may complete normally."
                            )
                            report = f"{report}\n\n{message}"

                    # TODO maybe use the normal report field here?
                    logger.debug(f"Reporting in-progress for exec_id={exec_id}")
                    ctx.request.progress_callback(report)

                full_log_file.flush()
                await asyncio.sleep(0)

        async def _process_waiter() -> None:
            await ingest_process.wait()
            logger.info(f"Detected subprocess exited exec_id={exec_id}")

        read_output_task = asyncio.create_task(_read_output_lines())
        report_progress_task = asyncio.create_task(_report_progress())
        process_waiter_task = asyncio.create_task(_process_waiter())

        group = tasks.gather(
            read_output_task, report_progress_task, process_waiter_task
        )
        try:
            await group
        except (Exception, asyncio.exceptions.CancelledError) as e:
            # This could just be a normal cancellation or it could be that
            # one of the monitoring tasks threw an exception.
            # In this case, we should kill the subprocess and cancel the other tasks.
            self._signal_process_group(ingest_process, signal.SIGTERM)

            # If the cause of the exception was a cancellation, then this is a no-op
            # because the gather method already propagates the cancellation.
            group.cancel()

            # ALL_COMPLETED means we wait for all tasks to finish, even if one of them
            # throws an exception. Set timeout to 60s to avoid hanging forever.
            _done, pending = await asyncio.wait(
                (
                    asyncio.create_task(ingest_process.wait()),
                    read_output_task,
                    report_progress_task,
                    process_waiter_task,
                ),
                timeout=self.CLEANUP_TIMEOUT_SECONDS,
                return_when=asyncio.ALL_COMPLETED,
            )
            if pending:
                logger.info(f"Failed to cancel {len(pending)} tasks on cleanup.")
                self._signal_process_group(ingest_process, signal.SIGKILL)

            if isinstance(e, asyncio.CancelledError):
                # If it was a cancellation, then we re-raise.
                raise
            else:
                raise RuntimeError(
                    f"Something went wrong in the subprocess executor: {e}"
                ) from e
        finally:
            full_log_file.close()

    def _handle_subprocess_completion(
        self,
        ingest_process: asyncio.subprocess.Process,
        ctx: ExecutionContext,
        report_out_file: str,
        artifact_output_dir: str,
        recipe: dict,
        exec_out_dir: str,
        shared_logs: LogHolder,
        cancelled: bool = False,
    ) -> None:
        """Handle subprocess completion: report processing, cleanup, and status.

        Cleanup steps are individually guarded. The only exception that escapes
        this method is the intentional `TaskError` raised on a non-zero return
        code from a non-cancelled run — so callers can invoke this from a
        `finally` block without fear of masking an in-flight exception.
        """

        if os.path.exists(report_out_file):
            try:
                with open(report_out_file) as structured_report_fp:
                    report_content = structured_report_fp.read()
                try:
                    registry = SecretRegistry.get_instance()
                    if registry and registry.get_count() > 0:
                        report_content = SecretMaskingFilter(registry).mask_text(
                            report_content
                        )
                except Exception:
                    # Better to have the report than to fail completely.
                    logger.warning("Failed to mask structured report, using original")
                ctx.get_report().set_structured_report(report_content)
            except Exception:
                logger.exception(
                    "Failed to process structured report from %s", report_out_file
                )

        try:
            ctx.get_report().set_logs(
                SubProcessTaskUtil._format_log_lines(shared_logs.get_lines())
            )
        except Exception:
            logger.exception("Failed to set logs on execution report")

        SubProcessTaskUtil._remove_directory(exec_out_dir)

        try:
            shutdown_secret_masking()
        except Exception as e:
            logger.warning(f"Failed to shutdown secret masking: {e}")

        if cancelled:
            ctx.get_report().report_info("Ingestion task was cancelled")
            return

        return_code = ingest_process.returncode

        if return_code != 0:  # Failed
            if return_code and return_code < 0:
                try:
                    signal_name = signal.Signals(-return_code).name
                except ValueError:
                    signal_name = str(-return_code)
                ctx.get_report().report_error(
                    f"The ingestion process was killed by signal {signal_name} likely because it ran out of memory. "
                    "You can resolve this issue by allocating more memory to the datahub-actions container."
                )
            elif return_code == 137:
                ctx.get_report().report_error(
                    "The ingestion process was terminated with exit code 137, likely because it ran out of memory."
                    "You can resolve this issue by allocating more memory to the datahub-actions container."
                )
            else:
                ctx.get_report().report_info(
                    f"Failed to execute 'datahub ingest', exit code {return_code}"
                )
            raise TaskError("Failed to execute 'datahub ingest'")

        # Report Successful execution
        ctx.get_report().report_info("Successfully executed 'datahub ingest'")

    def close(self) -> None:
        pass
