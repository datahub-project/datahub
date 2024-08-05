import contextlib
import dataclasses
import json
import os
import pathlib
import random
import traceback
from datetime import datetime, timezone
from enum import Enum
from typing import List, Optional

import anyio
import anyio.abc
import psutil
from loguru import logger
from typing_extensions import Self

from datahub_integrations.actions.stats_util import Stage
from datahub_integrations.dispatch.runner import (
    VENV_VERSION_NATIVE,
    LogHolder,
    SubprocessRunner,
    VenvConfig,
    VenvReference,
    setup_venv,
)

ACTION_RUNNER_SCRIPT = pathlib.Path(__file__).parent / "action_runner.py"
assert ACTION_RUNNER_SCRIPT.exists()

_VENV_TEMP_DIR = pathlib.Path("/tmp/datahub/envs")
_VENV_TEMP_DIR.mkdir(parents=True, exist_ok=True)
_RECIPE_TEMP_DIR = pathlib.Path("/tmp/datahub/recipes")
_RECIPE_TEMP_DIR.mkdir(parents=True, exist_ok=True)


class ActionStatus(Enum):
    INIT = "init"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclasses.dataclass
class ActionRun:
    urn: str
    unresolved_config: dict

    logs: LogHolder

    started_at: datetime = dataclasses.field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
    ended_at: datetime | None = None
    status: ActionStatus = ActionStatus.INIT

    @property
    def action_run(self) -> "ActionRun":
        return self


@dataclasses.dataclass
class LiveActionSpec:
    action_run: ActionRun

    runner: SubprocessRunner
    venv: VenvReference
    port: int

    _action_scope: anyio.abc.CancelScope = dataclasses.field(init=False)

    @property
    def urn(self) -> str:
        return self.action_run.urn

    @property
    def base_url(self) -> str:
        return f"http://localhost:{self.port}"


@dataclasses.dataclass
class ActionsManager(contextlib.AbstractAsyncContextManager):
    pipelines: dict[str, LiveActionSpec] = dataclasses.field(default_factory=dict)
    dead_pipelines: dict[str, ActionRun] = dataclasses.field(default_factory=dict)
    job_pipelines: dict[Stage, dict[str, LiveActionSpec]] = dataclasses.field(
        default_factory=dict
    )
    job_completed_pipelines: dict[Stage, dict[str, ActionRun]] = dataclasses.field(
        default_factory=dict
    )
    _subprocess_pids: List[int] = dataclasses.field(default_factory=list)

    context_stack: contextlib.AsyncExitStack = dataclasses.field(
        default_factory=contextlib.AsyncExitStack
    )

    _main_tg: anyio.abc.TaskGroup = dataclasses.field(init=False)

    async def __aenter__(self) -> Self:
        self._main_tg = anyio.create_task_group()
        await self._main_tg.__aenter__()

        return self

    async def __aexit__(self, exc_type, exc_value, traceback):  # type: ignore[no-untyped-def]
        await self.stop_all()
        return await self.context_stack.__aexit__(exc_type, exc_value, traceback)

    async def start_pipeline(self, urn: str, config: dict) -> None:
        if urn in self.pipelines:
            raise Exception(f"Pipeline {urn} is already running.")

        # TODO: Also write the logs to a file.
        logs = LogHolder(echo_to_stdout_prefix=f"{urn}: ")
        action_run = ActionRun(
            urn=urn,
            unresolved_config=config,
            logs=logs,
        )

        runner = SubprocessRunner(logs)

        venv = await setup_venv(
            venv_config=VenvConfig(version=VENV_VERSION_NATIVE),
            runner=runner,
            tmp_dir=_VENV_TEMP_DIR,
        )
        port = random.randint(10000, 20000)

        action_spec = LiveActionSpec(
            action_run=action_run,
            runner=runner,
            venv=venv,
            port=port,
        )

        await self._main_tg.start(self._run_pipeline, action_spec)
        self.pipelines[urn] = action_spec

    async def _run_pipeline(
        self,
        action_spec: LiveActionSpec,
        *,
        task_status: anyio.abc.TaskStatus[None] = anyio.TASK_STATUS_IGNORED,
    ) -> None:
        # Because this task runs within the main task group, if it raises an exception, all other running actions
        # will automatically get cancelled. It's also shielded, which means that it will only be cancelled when
        # the overall pipeline manager exits.
        async with anyio.CancelScope(shield=True) as cs:
            action_spec._action_scope = cs

            try:
                _config_file = _RECIPE_TEMP_DIR / f"{action_spec.urn}.json"
                _config_file.write_text(
                    json.dumps(action_spec.action_run.unresolved_config, indent=2)
                )

                # TODO: Only mark the task as started once the server is reachable.
                task_status.started()

                # TODO: Add a watchdog service that restarts the pipeline if it dies?
                await action_spec.runner.execute(
                    [
                        action_spec.venv.command("python"),
                        str(ACTION_RUNNER_SCRIPT),
                        str(_config_file),
                        "--port",
                        str(action_spec.port),
                    ],
                    env={
                        **os.environ,
                        **action_spec.venv.extra_envs(),
                    },
                )

            except Exception as e:
                # This also suppresses any exceptions generated by the task, which prevents
                # them from propagating out to the main task group.
                action_spec.action_run.logs.append(
                    "".join(traceback.format_exception(e))
                )

            finally:
                # TODO: If it wasn't manually stopped, we should restart it?
                logger.info(f"Pipeline {action_spec.urn} has stopped.")
                action_spec.action_run.ended_at = datetime.now(tz=timezone.utc)
                self.dead_pipelines[action_spec.urn] = self.pipelines.pop(
                    action_spec.urn
                ).action_run

        logger.debug(
            f"Pipeline {action_spec.urn} has exited _run_pipeline without an exception."
        )

    def is_running(self, urn: str) -> bool:
        return urn in self.pipelines

    def is_rolling_back(self, urn: str) -> bool:
        return urn in self.job_pipelines.get(Stage.ROLLBACK, {})

    async def stop_pipeline(self, urn: str) -> None:
        if urn not in self.pipelines:
            raise Exception(f"No pipeline with urn {urn} found.")

        self.pipelines[urn]._action_scope.cancel()

        # Wait for the pipeline to stop. For full correctness, we should use
        # events or channels instead. But this is good enough for now.
        while urn in self.pipelines:
            await anyio.sleep(0.1)

    def is_currently_executing_stage(self, urn: str, stage: Stage) -> bool:
        return urn in self.job_pipelines.get(stage, {})

    async def rollback_pipeline(self, urn: str, config: Optional[dict] = None) -> None:
        await self.run_action_pipeline_task(
            urn, Stage.ROLLBACK, config, cancel_stages=[Stage.LIVE, Stage.BOOTSTRAP]
        )

    async def bootstrap_pipeline(self, urn: str, config: Optional[dict] = None) -> None:
        await self.run_action_pipeline_task(
            urn, Stage.BOOTSTRAP, config, cancel_stages=[Stage.ROLLBACK]
        )

    async def run_action_pipeline_task(
        self,
        urn: str,
        stage: Stage,
        config: Optional[dict] = None,
        cancel_stages: List[Stage] = [],
    ) -> None:
        if config is None:
            raise Exception(f"Cannot execute pipeline {urn} without a config.")

        if Stage.LIVE in cancel_stages:
            if urn in self.pipelines:
                logger.info(
                    f"Stopping pipeline {urn} before starting {stage} pipeline."
                )
                if config is None:
                    config = self.pipelines[urn].action_run.unresolved_config
                await self.stop_pipeline(urn)

        for task_stage in [Stage.BOOTSTRAP, Stage.ROLLBACK]:
            if task_stage in cancel_stages:
                if urn in self.job_pipelines.get(task_stage, {}):
                    logger.info(
                        f"Stopping {task_stage} pipeline {urn} before starting {stage} pipeline."
                    )
                    await self._cancel_pipeline_stage(Stage.BOOTSTRAP, urn)

        if self.is_currently_executing_stage(urn, stage):
            raise Exception(f"Pipeline {urn} is already in stage {stage}")

        # TODO: Also write the logs to a file.
        logs = LogHolder(echo_to_stdout_prefix=f"{urn}: ")
        action_run = ActionRun(
            urn=urn,
            unresolved_config=config,
            logs=logs,
        )

        runner = SubprocessRunner(logs)

        venv = await setup_venv(
            venv_config=VenvConfig(version=VENV_VERSION_NATIVE),
            runner=runner,
            tmp_dir=_VENV_TEMP_DIR,
        )
        port = random.randint(10000, 20000)

        action_spec = LiveActionSpec(
            action_run=action_run,
            runner=runner,
            venv=venv,
            port=port,
        )

        await self._main_tg.start(self._execute_pipeline_stage, stage, action_spec)
        self.job_pipelines[stage] = self.job_pipelines.get(stage, {})
        self.job_pipelines[stage][urn] = action_spec

    async def _cancel_pipeline_stage(
        self,
        stage: Stage,
        urn: str,
    ) -> None:
        if urn in self.job_pipelines.get(stage, {}):
            action_spec: LiveActionSpec = self.job_pipelines[stage][urn]
            await action_spec._action_scope.cancel()
            logger.info(f"Cancelled {stage} pipeline {urn}.")

    async def _execute_pipeline_stage(
        self,
        stage: Stage,
        action_spec: LiveActionSpec,
        *,
        task_status: anyio.abc.TaskStatus[None] = anyio.TASK_STATUS_IGNORED,
    ) -> None:
        # Because this task runs within the main task group, if it raises an exception, all other running actions
        # will automatically get cancelled. It's also shielded, which means that it will only be cancelled when
        # the overall pipeline manager exits.
        async with anyio.CancelScope(shield=True) as cs:
            action_spec._action_scope = cs

            try:
                _config_file = _RECIPE_TEMP_DIR / f"{action_spec.urn}.json"
                _config_file.write_text(
                    json.dumps(action_spec.action_run.unresolved_config, indent=2)
                )

                # TODO: Only mark the task as started once the server is reachable.
                task_status.started()

                # TODO: Add a watchdog service that restarts the pipeline if it
                # dies?
                action_spec.action_run.status = ActionStatus.RUNNING
                await action_spec.runner.execute(
                    [
                        action_spec.venv.command("python"),
                        str(ACTION_RUNNER_SCRIPT),
                        str(_config_file),
                        "--port",
                        str(action_spec.port),
                        (
                            "--rollback"
                            if stage == Stage.ROLLBACK
                            else "--bootstrap" if stage == Stage.BOOTSTRAP else ""
                        ),
                    ],
                    env={
                        **os.environ,
                        **action_spec.venv.extra_envs(),
                    },
                )
                action_spec.action_run.status = ActionStatus.SUCCEEDED

            except Exception as e:
                # This also suppresses any exceptions generated by the task, which prevents
                # them from propagating out to the main task group.
                logger.info(f"Pipeline {action_spec.urn} {stage} has failed.")
                action_spec.action_run.logs.append(
                    "".join(traceback.format_exception(e))
                )
                action_spec.action_run.status = ActionStatus.FAILED

            finally:
                # TODO: Capture exit code?
                logger.info(f"Pipeline {action_spec.urn} {stage} has finished.")
                action_spec.action_run.ended_at = datetime.now(tz=timezone.utc)
                self.job_completed_pipelines[stage] = self.job_completed_pipelines.get(
                    stage, {}
                )
                self.job_completed_pipelines[stage][action_spec.urn] = (
                    self.job_pipelines[stage].pop(action_spec.urn).action_run
                )
                # logger.info(
                #     f"Executed {stage} pipeline: {self.job_completed_pipelines[stage]}"
                # )

        logger.debug(f"Pipeline {action_spec.urn} has exited _execute_pipeline_stage.")

    async def stop_all(self) -> None:
        async with anyio.create_task_group() as tg:
            for urn in list(self.pipelines.keys()):
                # TODO: Maybe each one should be shielded?
                tg.start_soon(self.stop_pipeline, urn)

        # Terminate any remaining subprocesses
        for pid in self._subprocess_pids:
            try:
                process = psutil.Process(pid)
                process.terminate()
                process.wait(timeout=5)
            except psutil.NoSuchProcess:
                pass
            except psutil.TimeoutExpired:
                process.kill()

        self._subprocess_pids.clear()

    def report_dead_pipeline(self, urn: str, config: dict, exc: Exception) -> None:
        logs = LogHolder(echo_to_stdout_prefix=f"{urn}: ")
        logs.append("".join(traceback.format_exception(exc)))

        action_run = ActionRun(
            urn=urn,
            unresolved_config=config,
            logs=logs,
        )
        self.dead_pipelines[urn] = action_run
