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
import logging
import subprocess
import sys
from collections import deque

from datahub.executor.common.config import ConfigModel
from datahub.executor.context.execution_context import ExecutionContext
from datahub.executor.context.executor_context import ExecutorContext
from datahub.executor.execution.sub_process_task_common import (
    SubProcessRecipeTaskArgs,
    SubProcessTaskUtil,
    resolve_wrapper_script,
)
from datahub.executor.execution.task import Task, TaskError
from datahub.masking.masking_filter import SecretMaskingFilter

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
        exec_out_dir = f"{self.tmp_dir}/{ctx.exec_id}"
        validated_args = SubProcessTestConnectionTaskArgs.model_validate(args)
        report_out_file: str = f"{exec_out_dir}/connection_report.json"

        # Recipe resolution, venv, subprocess env and stdin envelope are the
        # skeleton every recipe task shares; SubProcessTaskUtil owns it so the
        # copies cannot drift apart again.
        prepared = await SubProcessTaskUtil.prepare_recipe_run(
            validated_args,
            execution_ctx=ctx,
            executor_ctx=self.ctx,
            exec_out_dir=exec_out_dir,
            envelope_extra={"__report_out_file__": report_out_file},
        )

        # Invoked with this interpreter rather than by bare name off PATH: the wrapper
        # must run in the executor's own environment (it then activates the per-run
        # target venv itself). By absolute path rather than -m: see
        # resolve_wrapper_script.
        command_script: str = resolve_wrapper_script(
            "datahub.executor.wrappers.run_test_connection"
        )
        stdout_lines: deque = deque(maxlen=SubProcessTaskUtil.MAX_LOG_LINES)

        ingest_process = subprocess.Popen(
            [
                sys.executable,
                command_script,
                str(prepared.venv_ref.venv_loc),
            ],
            env=prepared.subprocess_env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        # Write envelope to stdin and close
        assert ingest_process.stdin is not None
        ingest_process.stdin.write(prepared.stdin_envelope)
        ingest_process.stdin.close()

        masking_filter = SecretMaskingFilter()

        try:
            while ingest_process.poll() is None:
                assert ingest_process.stdout
                line = ingest_process.stdout.readline()

                masked_line = masking_filter.mask_text(line)
                sys.stdout.write(masked_line)
                stdout_lines.append(masked_line)
                await asyncio.sleep(0)

            return_code = ingest_process.poll()

        except asyncio.CancelledError:
            # Terminate the running child process
            ingest_process.terminate()
            raise

        finally:
            SubProcessTaskUtil.finalize_task_output(
                report_out_file,
                exec_out_dir,
                stdout_lines,
                ctx,
                masking_filter=masking_filter,
            )

        if return_code != 0:
            # Failed
            ctx.get_report().report_info("Failed to execute 'datahub test connection'")
            raise TaskError("Failed to execute 'datahub test connection'")

        # Report Successful execution
        ctx.get_report().report_info("Successfully executed 'datahub test connection'")

    def close(self) -> None:
        pass
