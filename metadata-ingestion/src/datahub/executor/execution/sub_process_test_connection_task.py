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
from typing import Optional

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

        stdout_lines: deque = deque(maxlen=SubProcessTaskUtil.MAX_LOG_LINES)

        # Bound before the try so the except can tell "Popen never ran" from
        # "Popen produced a child and the stdin write then failed". Those need
        # opposite answers about the venv lock.
        ingest_process: Optional[subprocess.Popen] = None
        try:
            # Invoked with this interpreter rather than by bare name off PATH: the
            # wrapper must run in the executor's own environment (it then activates
            # the per-run target venv itself). By absolute path rather than -m: see
            # resolve_wrapper_script.
            #
            # Inside the try, not before it. prepare_recipe_run returns holding
            # the cache entry SHARED, and resolve_wrapper_script raises
            # RuntimeError when importlib.util.find_spec returns None -- a
            # packaging or partially-installed-image failure. Outside a handler
            # that is a deterministic leak: every test-connection request pins
            # one more unevictable entry for the life of the pod.
            command_script: str = resolve_wrapper_script(
                "datahub.executor.wrappers.run_test_connection"
            )
            # Also inside, and before the spawn. finalize_task_output's own
            # comment notes that constructing this can fail; after a child
            # exists that would be the same unguarded window again.
            masking_filter = SecretMaskingFilter()

            # Hand the venv-cache lock to the child; see lock_handoff.
            lock_fds, lock_env = SubProcessTaskUtil.lock_handoff(prepared.venv_ref)

            ingest_process = subprocess.Popen(
                [
                    sys.executable,
                    command_script,
                    str(prepared.venv_ref.venv_loc),
                ],
                env={**prepared.subprocess_env, **lock_env},
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                pass_fds=lock_fds,
            )

            # Write envelope to stdin and close
            assert ingest_process.stdin is not None
            ingest_process.stdin.write(prepared.stdin_envelope)
            ingest_process.stdin.close()
        except BaseException:
            # Everything above sits before the try/finally that calls
            # finalize_task_output, so a failure here -- an OSError from Popen,
            # a broken pipe, a missing wrapper module, a cancellation -- is the
            # one window where nothing releases the cached venv's SHARED lock.
            # Left held, the entry can never be evicted for the rest of the
            # pod's life. Guarded inside, so it cannot replace the exception in
            # flight.
            #
            # No "is the child alive?" question to answer any more. If the
            # spawn got far enough to produce a child, that child inherited
            # the lock descriptor and owns the hold; this only closes our own
            # copy, which the kernel would do at exit anyway.
            SubProcessTaskUtil.release_venv_lock(prepared.venv_ref)
            # finalize_task_output is what normally removes this, and it is
            # never reached from here, so on the cache-off or cache-busy path
            # the complete per-run venv inside it would leak.
            #
            # Only when nothing can still be running out of it. A
            # non-cacheable venv lives INSIDE exec_out_dir, so removing it
            # while a forked child is executing from it deletes that child's
            # interpreter -- strictly worse than leaking the disk.
            if ingest_process is None or ingest_process.poll() is not None:
                SubProcessTaskUtil._remove_directory(exec_out_dir)
            raise

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
            # Terminate the child AND wait for it. The venv lock no longer
            # depends on this -- the kernel releases it when the child dies
            # -- but exec_out_dir still does: a NON-cacheable venv lives
            # inside it, and finalize_task_output removes it. Deleting it
            # under a live interpreter is the ImportError-on-a-deleted-.so
            # failure this whole mechanism exists to prevent.
            #
            # Bounded, and blocking on purpose: this unwinds a CancelledError,
            # where awaiting invites a second cancellation and turns cleanup
            # into a new failure mode.
            SubProcessTaskUtil.terminate_and_reap(ingest_process)
            raise

        finally:
            SubProcessTaskUtil.finalize_task_output(
                report_out_file,
                exec_out_dir,
                stdout_lines,
                ctx,
                masking_filter=masking_filter,
                venv_ref=prepared.venv_ref,
            )

        if return_code != 0:
            # Failed
            ctx.get_report().report_info("Failed to execute 'datahub test connection'")
            raise TaskError("Failed to execute 'datahub test connection'")

        # Report Successful execution
        ctx.get_report().report_info("Successfully executed 'datahub test connection'")

    def close(self) -> None:
        pass
