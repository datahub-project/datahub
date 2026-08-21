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
import json
import logging
import os
import subprocess
import sys
from collections import deque
from pathlib import Path

import yaml

from datahub.executor.common.config import ConfigModel
from datahub.executor.context.execution_context import ExecutionContext
from datahub.executor.context.executor_context import ExecutorContext
from datahub.executor.execution.runner import (
    LogHolder,
    SubprocessRunner,
    VenvConfig,
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


class SubProcessTestConnectionTaskConfig(ConfigModel):
    tmp_dir: str = "/tmp/datahub/ingest"


class SubProcessTestConnectionTaskArgs(SubProcessRecipeTaskArgs):
    pass


class SubProcessTestConnectionTask(Task):
    config: SubProcessTestConnectionTaskConfig
    tmp_dir: str  # Location where tmp files will be written (recipes)
    ctx: ExecutorContext

    @classmethod
    def create(cls, config: dict, ctx: ExecutorContext) -> "Task":
        config_parsed = SubProcessTestConnectionTaskConfig.model_validate(config)
        return cls(config_parsed, ctx)

    def __init__(
        self, config: SubProcessTestConnectionTaskConfig, ctx: ExecutorContext
    ):
        self.config = config
        self.tmp_dir = config.tmp_dir
        self.ctx = ctx

    async def execute(self, args: dict, ctx: ExecutionContext) -> None:
        exec_id = ctx.exec_id  # The unique execution id.

        exec_out_dir = f"{self.tmp_dir}/{exec_id}"

        # 0. Validate arguments
        validated_args = SubProcessTestConnectionTaskArgs.model_validate(args)

        # 1. Resolve the recipe and secrets (secrets stay in memory)
        recipe, secret_values = SubProcessTaskUtil._resolve_recipe(
            validated_args.recipe, execution_ctx=ctx, executor_ctx=self.ctx
        )
        plugin: str = SubProcessTaskUtil._get_plugin_from_recipe(recipe)

        # 2. Prepare or resolve venv
        venv_config = VenvConfig(
            version=validated_args.version,
            main_plugin=plugin,
            extra_pip_requirements=validated_args.extra_pip_requirements,
            extra_pip_plugins=validated_args.extra_pip_plugins,
            extra_env_vars=validated_args.extra_env_vars,
        )
        venv_setup_logs = LogHolder()
        venv_runner = SubprocessRunner(logs=venv_setup_logs)
        try:
            venv_ref = await setup_venv(
                venv_config=venv_config,
                runner=venv_runner,
                tmp_dir=Path(exec_out_dir),
            )
        except Exception as e:
            error_msg = SubProcessTaskUtil.format_subprocess_error(e)
            raise TaskError(f"Failed to set up virtual environment: {error_msg}") from e

        # 3. Spin off subprocess to run the test-connection script with venv path
        # Invoked with this interpreter rather than by bare name off PATH: the wrapper
        # must run in the executor's own environment (it then activates the per-run
        # target venv itself). By absolute path rather than -m: see
        # resolve_wrapper_script.
        command_script: str = resolve_wrapper_script(
            "datahub.executor.wrappers.run_test_connection"
        )
        report_out_file: str = f"{exec_out_dir}/connection_report.json"
        stdout_lines: deque = deque(maxlen=SubProcessTaskUtil.MAX_LOG_LINES)

        # Prepare environment for subprocess
        subprocess_env = {
            **validated_args.get_combined_env_vars(),
            "VENV_PATH": str(venv_ref.venv_loc),
            "DATAHUB_ENABLE_SECRET_MASKING": "true",
        }

        # Build stdin envelope in datahub-compatible format.
        # All envelope keys use dunder prefix to distinguish from recipe content.
        stdin_envelope = json.dumps(
            {
                "__recipe_yaml__": yaml.dump(recipe),
                "__secrets__": secret_values,
                "__report_out_file__": report_out_file,
            }
        )

        ingest_process = subprocess.Popen(
            [
                sys.executable,
                command_script,
                str(venv_ref.venv_loc),
            ],
            env=subprocess_env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        # Write envelope to stdin and close
        assert ingest_process.stdin is not None
        ingest_process.stdin.write(stdin_envelope)
        ingest_process.stdin.close()

        try:
            # Create masking filter for subprocess stdout
            # Masking was already set up in _resolve_recipe for the executor process
            masking_filter = None
            try:
                registry = SecretRegistry.get_instance()
                if registry and registry.get_count() > 0:
                    masking_filter = SecretMaskingFilter(registry)
                    logger.info(
                        f"[TEST_CONNECTION] Created masking filter with {registry.get_count()} secret(s)"
                    )
            except Exception as e:
                logger.warning(
                    f"[TEST_CONNECTION] Failed to create masking filter: {e}"
                )

            while ingest_process.poll() is None:
                assert ingest_process.stdout
                line = ingest_process.stdout.readline()

                # Mask secrets before writing to stdout
                masked_line = masking_filter.mask_text(line) if masking_filter else line
                sys.stdout.write(masked_line)
                stdout_lines.append(masked_line)
                await asyncio.sleep(0)

            return_code = ingest_process.poll()

        except asyncio.CancelledError:
            # Terminate the running child process
            ingest_process.terminate()
            raise

        finally:
            if os.path.exists(report_out_file):
                with open(report_out_file) as structured_report_fp:
                    report_content = structured_report_fp.read()

                    # Mask secrets in structured report
                    try:
                        registry = SecretRegistry.get_instance()
                        if registry and registry.get_count() > 0:
                            temp_filter = SecretMaskingFilter(registry)
                            report_content = temp_filter.mask_text(report_content)
                    except Exception:
                        logger.warning(
                            "Failed to mask structured report, using original"
                        )

                    ctx.get_report().set_structured_report(report_content)

            ctx.get_report().set_logs(
                SubProcessTaskUtil._format_log_lines(stdout_lines)
            )

            # Cleanup execution directory
            SubProcessTaskUtil._remove_directory(exec_out_dir)

            # Shutdown DataHub masking framework
            try:
                shutdown_secret_masking()
            except Exception as e:
                logger.warning(f"Failed to shutdown secret masking: {e}")

        if return_code != 0:
            # Failed
            ctx.get_report().report_info("Failed to execute 'datahub test connection'")
            raise TaskError("Failed to execute 'datahub test connection'")

        # Report Successful execution
        ctx.get_report().report_info("Successfully executed 'datahub test connection'")

    def close(self) -> None:
        pass
