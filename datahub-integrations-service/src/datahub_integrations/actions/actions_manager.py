import contextlib
import dataclasses
import json
import os
import pathlib
import random
from datetime import datetime, timezone

import anyio
import anyio.abc
from loguru import logger
from typing_extensions import Self

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


@dataclasses.dataclass
class ActionSpec:
    urn: str
    unresolved_config: dict

    logs: LogHolder
    runner: SubprocessRunner
    venv: VenvReference
    port: int

    started_at: datetime = dataclasses.field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
    ended_at: datetime | None = None
    _action_tg: anyio.abc.TaskGroup = dataclasses.field(init=False)

    @property
    def base_url(self) -> str:
        return f"http://localhost:{self.port}"

    def is_running(self) -> bool:
        return self.ended_at is None


@dataclasses.dataclass
class ActionsManager(contextlib.AbstractAsyncContextManager):
    pipelines: dict[str, ActionSpec] = dataclasses.field(default_factory=dict)
    dead_pipelines: dict[str, ActionSpec] = dataclasses.field(default_factory=dict)

    context_stack: contextlib.AsyncExitStack = dataclasses.field(
        default_factory=contextlib.AsyncExitStack
    )

    _main_tg: anyio.abc.TaskGroup = dataclasses.field(init=False)

    async def __aenter__(self) -> Self:
        self._main_tg = anyio.create_task_group()
        await self._main_tg.__aenter__()

        return self

    async def __aexit__(self, exc_type, exc_value, traceback):  # type: ignore[no-untyped-def]
        return await self.context_stack.__aexit__(exc_type, exc_value, traceback)

    async def start_pipeline(self, urn: str, config: dict) -> None:
        if urn in self.pipelines:
            raise Exception(f"Pipeline with urn {urn} is already running.")

        # TODO: Also write the logs to a file.
        logs = LogHolder(echo_to_stdout_prefix=f"{urn}: ")
        runner = SubprocessRunner(logs)

        venv = await setup_venv(
            venv_config=VenvConfig(version=VENV_VERSION_NATIVE),
            runner=runner,
            tmp_dir=_VENV_TEMP_DIR,
        )
        port = random.randint(10000, 20000)

        action_spec = ActionSpec(
            urn=urn,
            unresolved_config=config,
            logs=logs,
            runner=runner,
            venv=venv,
            port=port,
        )

        await self._main_tg.start(self._run_pipeline, action_spec)
        self.pipelines[urn] = action_spec

    async def _run_pipeline(
        self,
        action_spec: ActionSpec,
        *,
        task_status: anyio.abc.TaskStatus[None] = anyio.TASK_STATUS_IGNORED,
    ) -> None:
        _config_file = _RECIPE_TEMP_DIR / f"{action_spec.urn}.json"
        _config_file.write_text(json.dumps(action_spec.unresolved_config, indent=2))

        async with anyio.create_task_group() as tg:
            action_spec._action_tg = tg

            try:
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

            finally:
                # TODO: If it wasn't manually stopped, we should restart it.
                logger.info(f"Pipeline with urn {action_spec.urn} has stopped.")
                action_spec.ended_at = datetime.now(tz=timezone.utc)
                self.dead_pipelines[action_spec.urn] = self.pipelines.pop(
                    action_spec.urn
                )

    def is_running(self, urn: str) -> bool:
        return urn in self.pipelines

    async def stop_pipeline(self, urn: str) -> None:
        if urn not in self.pipelines:
            raise Exception(f"No pipeline with urn {urn} found.")

        self.pipelines[urn]._action_tg.cancel_scope.cancel()

        # Wait for the pipeline to stop. For full correctness, we should use
        # events or channels instead. But this is good enough for now.
        while urn in self.pipelines:
            await anyio.sleep(0.1)

    async def stop_all(self) -> None:
        async with anyio.create_task_group() as tg:
            for urn in list(self.pipelines.keys()):
                # TODO: Maybe each one should be shielded?
                tg.start_soon(self.stop_pipeline, urn)
