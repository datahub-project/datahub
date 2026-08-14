import asyncio
import json
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import Mock, mock_open, patch

import pytest
import yaml

from datahub.executor.context.execution_context import ExecutionContext
from datahub.executor.context.executor_context import ExecutorContext
from datahub.executor.execution.sub_process_test_connection_task import (
    SubProcessTestConnectionTask,
    SubProcessTestConnectionTaskConfig,
)
from datahub.executor.execution.task import TaskError


@pytest.fixture
def tmp_dir() -> str:
    return "/tmp/test"


@pytest.fixture
def task_config(tmp_dir: str) -> SubProcessTestConnectionTaskConfig:
    return SubProcessTestConnectionTaskConfig(tmp_dir=tmp_dir)


@pytest.fixture
def executor_ctx() -> ExecutorContext:
    mock = Mock(spec=ExecutorContext)
    mock.get_secret_stores.return_value = []
    return mock


@pytest.fixture
def exec_ctx() -> ExecutionContext:
    report = Mock()
    report.set_structured_report = Mock()
    report.set_logs = Mock()
    report.report_info = Mock()
    mock = Mock(spec=ExecutionContext)
    mock.exec_id = "exec-123"
    mock.get_report.return_value = report
    return mock


@pytest.fixture
def sample_recipe() -> str:
    return json.dumps(
        {
            "run_id": "test-run-id",
            "source": {"type": "demo-data", "config": {}},
            "pipeline_name": "test-pipeline",
        }
    )


@pytest.fixture
def sample_args(sample_recipe: str) -> dict[str, str]:
    return {"recipe": sample_recipe, "version": "latest"}


def test_config_defaults() -> None:
    cfg = SubProcessTestConnectionTaskConfig()
    assert cfg.tmp_dir == "/tmp/datahub/ingest"


def test_config_custom(tmp_dir: str) -> None:
    cfg = SubProcessTestConnectionTaskConfig(tmp_dir=tmp_dir)
    assert cfg.tmp_dir == tmp_dir


def test_create(
    task_config: SubProcessTestConnectionTaskConfig, executor_ctx: ExecutorContext
) -> None:
    task = SubProcessTestConnectionTask.create(
        {"tmp_dir": task_config.tmp_dir}, executor_ctx
    )
    assert isinstance(task, SubProcessTestConnectionTask)


async def test_execute_success(
    task_config: SubProcessTestConnectionTaskConfig,
    executor_ctx: ExecutorContext,
    exec_ctx: ExecutionContext,
    sample_args: dict[str, str],
) -> None:
    """Test successful test connection execution using high-level mocking like ingestion task."""
    # Arrange
    task = SubProcessTestConnectionTask(task_config, executor_ctx)

    # Provide args, including extra envs to ensure they propagate
    args: dict[str, Any] = {
        **sample_args,  # Use the demo-data recipe from fixture
        "extra_env_vars": {"FOO": "BAR"},
        "extra_pip_requirements": [],
        "extra_pip_plugins": [],
    }

    # Mock the process to simulate successful execution
    mock_process = Mock()
    mock_process.returncode = 0
    mock_process.stdout = Mock()
    mock_process.stdout.readline.side_effect = ["test connection output\n", ""]
    mock_process.stdin = Mock()

    # Mock poll to return None once, then 0 (completed)
    poll_call_count = 0

    def mock_poll():
        nonlocal poll_call_count
        poll_call_count += 1
        return 0 if poll_call_count > 1 else None

    mock_process.poll = mock_poll

    with (
        # Mock high-level task methods like ingestion task tests
        patch(
            "datahub.executor.execution.sub_process_task_common.SubProcessTaskUtil._resolve_recipe"
        ) as mock_resolve,
        patch(
            "datahub.executor.execution.sub_process_task_common.SubProcessTaskUtil._get_plugin_from_recipe"
        ) as mock_get_plugin,
        patch(
            "datahub.executor.execution.sub_process_test_connection_task.setup_venv"
        ) as mock_setup_venv,
        patch(
            "datahub.executor.execution.sub_process_test_connection_task.subprocess.Popen",
            return_value=mock_process,
        ) as mock_popen,
        patch("builtins.open", mock_open(read_data='{"ok": true}')),
        patch("os.path.exists", return_value=True),
        patch(
            "datahub.executor.execution.sub_process_task_common.SubProcessTaskUtil._remove_directory"
        ) as _mock_remove_dir,
        patch(
            "datahub.executor.execution.sub_process_test_connection_task.shutdown_secret_masking"
        ),
    ):
        # Setup mocks: _resolve_recipe now returns (recipe, secret_values)
        mock_resolve.return_value = (
            {"source": {"type": "demo-data"}},
            {"SOME_SECRET": "val"},
        )
        mock_get_plugin.return_value = "demo-data"

        # Mock venv reference
        mock_venv_ref = Mock()
        mock_venv_ref.venv_loc = "/tmp/venv-demo-data-test"
        mock_setup_venv.return_value = mock_venv_ref

        # Act
        await task.execute(args, exec_ctx)

        # Assert that the key components were called correctly
        mock_resolve.assert_called_once()
        mock_get_plugin.assert_called_once()
        mock_setup_venv.assert_called_once()
        mock_popen.assert_called_once()

        # Verify the wrapper is invoked by ABSOLUTE PATH with the executor's own
        # interpreter -- not by bare script name off PATH, and deliberately not with
        # `-m`. `-m` puts the subprocess's CWD on sys.path[0], so a stray module in the
        # working directory (e.g. a yaml.py in /tmp, which is the image's WORKDIR)
        # shadows real imports and kills the run before any wrapper code executes.
        popen_args = mock_popen.call_args[0][0]  # First argument is the command list
        assert popen_args[0] == sys.executable
        assert "-m" not in popen_args
        assert popen_args[1].endswith(
            "datahub/executor/wrappers/run_test_connection.py"
        )
        assert Path(popen_args[1]).is_absolute()
        assert popen_args[2] == "/tmp/venv-demo-data-test"

        # Verify subprocess launched with stdin=subprocess.PIPE
        popen_kwargs = mock_popen.call_args[1]
        assert popen_kwargs["stdin"] == subprocess.PIPE

        # Verify environment propagation
        env = popen_kwargs["env"]
        assert env["FOO"] == "BAR"  # from extra_env_vars
        assert env["DATAHUB_ENABLE_SECRET_MASKING"] == "true"
        # DATAHUB_SECRET_NAMES should NOT be in env (secrets via stdin now)
        assert "DATAHUB_SECRET_NAMES" not in env

        # Verify stdin envelope was written with recipe + secrets
        mock_process.stdin.write.assert_called_once()
        stdin_payload = json.loads(mock_process.stdin.write.call_args[0][0])
        # Envelope uses datahub-compatible format
        assert yaml.safe_load(stdin_payload["__recipe_yaml__"]) == {
            "source": {"type": "demo-data"}
        }
        assert stdin_payload["__secrets__"] == {"SOME_SECRET": "val"}
        mock_process.stdin.close.assert_called_once()

        # Assert logs and structured report were set
        report = exec_ctx.get_report()
        report.set_structured_report.assert_called_once()  # type: ignore[attr-defined]
        report.set_logs.assert_called_once()  # type: ignore[attr-defined]
        report.report_info.assert_called_once()  # type: ignore[attr-defined]


async def test_execute_failure_raises(
    task_config: SubProcessTestConnectionTaskConfig,
    executor_ctx: ExecutorContext,
    exec_ctx: ExecutionContext,
    sample_args: dict[str, str],
) -> None:
    """Test test connection execution failure using high-level mocking like ingestion task."""
    # Arrange
    task = SubProcessTestConnectionTask(task_config, executor_ctx)
    args: dict[str, Any] = {
        **sample_args,  # Use the demo-data recipe from fixture
    }

    # Mock the process to simulate failed execution
    mock_process = Mock()
    mock_process.returncode = 1  # Non-zero exit code = failure
    mock_process.stdout = Mock()
    mock_process.stdout.readline.side_effect = [
        "connection failed\n",
        "error details\n",
        "",
    ]
    mock_process.stdin = Mock()

    # Mock poll to return None once, then 1 (failed)
    poll_call_count = 0

    def mock_poll():
        nonlocal poll_call_count
        poll_call_count += 1
        return 1 if poll_call_count > 1 else None

    mock_process.poll = mock_poll

    with (
        # Mock high-level task methods
        patch(
            "datahub.executor.execution.sub_process_task_common.SubProcessTaskUtil._resolve_recipe"
        ) as mock_resolve,
        patch(
            "datahub.executor.execution.sub_process_task_common.SubProcessTaskUtil._get_plugin_from_recipe"
        ) as mock_get_plugin,
        patch(
            "datahub.executor.execution.sub_process_test_connection_task.setup_venv"
        ) as mock_setup_venv,
        patch(
            "datahub.executor.execution.sub_process_test_connection_task.subprocess.Popen",
            return_value=mock_process,
        ) as mock_popen,
        patch("os.path.exists", return_value=False),  # No report file on failure
        patch(
            "datahub.executor.execution.sub_process_task_common.SubProcessTaskUtil._remove_directory"
        ) as _mock_remove_dir,
        patch(
            "datahub.executor.execution.sub_process_test_connection_task.shutdown_secret_masking"
        ),
    ):
        # Setup mocks: _resolve_recipe now returns (recipe, secret_values)
        mock_resolve.return_value = ({"source": {"type": "demo-data"}}, {})
        mock_get_plugin.return_value = "demo-data"

        # Mock venv reference
        mock_venv_ref = Mock()
        mock_venv_ref.venv_loc = "/tmp/venv-demo-data-test"
        mock_setup_venv.return_value = mock_venv_ref

        # Act / Assert - should raise exception on failure
        with pytest.raises(TaskError):
            await task.execute(args, exec_ctx)

        # Verify that setup still happened before failure
        mock_resolve.assert_called_once()
        mock_get_plugin.assert_called_once()
        mock_setup_venv.assert_called_once()
        mock_popen.assert_called_once()

        # Verify subprocess was launched with stdin=subprocess.PIPE
        popen_kwargs = mock_popen.call_args[1]
        assert popen_kwargs["stdin"] == subprocess.PIPE

        # Logs should be set even on failure
        report = exec_ctx.get_report()
        report.set_logs.assert_called()  # type: ignore[attr-defined]


async def test_cancellation_terminates_the_subprocess(
    task_config: SubProcessTestConnectionTaskConfig,
    executor_ctx: ExecutorContext,
    exec_ctx: ExecutionContext,
    sample_args: dict[str, str],
) -> None:
    """An operator cancelling a connection test must not leave the child running.

    The ingestion task has TestMonitorSubprocessCancellation for the same shape; this
    task's `except asyncio.CancelledError` branch had no test at all, so the terminate
    call it makes was never executed by the suite.
    """
    task = SubProcessTestConnectionTask(task_config, executor_ctx)
    args: dict[str, Any] = {
        **sample_args,
        "extra_env_vars": {},
        "extra_pip_requirements": [],
        "extra_pip_plugins": [],
    }

    # poll() never completes, so the read loop spins on its `await asyncio.sleep(0)`
    # -- that yield point is where the cancellation lands.
    entered_read_loop = asyncio.Event()

    def _readline() -> str:
        entered_read_loop.set()
        return ""

    mock_process = Mock()
    mock_process.returncode = None
    mock_process.poll = Mock(return_value=None)
    mock_process.stdout = Mock()
    mock_process.stdout.readline = Mock(side_effect=_readline)
    mock_process.stdin = Mock()
    mock_process.terminate = Mock()

    with (
        patch(
            "datahub.executor.execution.sub_process_task_common.SubProcessTaskUtil._resolve_recipe"
        ) as mock_resolve,
        patch(
            "datahub.executor.execution.sub_process_task_common.SubProcessTaskUtil._get_plugin_from_recipe"
        ) as mock_get_plugin,
        patch(
            "datahub.executor.execution.sub_process_test_connection_task.setup_venv"
        ) as mock_setup_venv,
        patch(
            "datahub.executor.execution.sub_process_test_connection_task.subprocess.Popen",
            return_value=mock_process,
        ),
        patch("os.path.exists", return_value=False),
        patch(
            "datahub.executor.execution.sub_process_task_common.SubProcessTaskUtil._remove_directory"
        ),
        patch(
            "datahub.executor.execution.sub_process_test_connection_task.shutdown_secret_masking"
        ),
    ):
        mock_resolve.return_value = ({"source": {"type": "demo-data"}}, {})
        mock_get_plugin.return_value = "demo-data"
        mock_venv_ref = Mock()
        mock_venv_ref.venv_loc = "/tmp/venv-demo-data-test"
        mock_setup_venv.return_value = mock_venv_ref

        pending = asyncio.ensure_future(task.execute(args, exec_ctx))
        await asyncio.wait_for(entered_read_loop.wait(), timeout=5)
        pending.cancel()

        with pytest.raises(asyncio.CancelledError):
            await pending

    mock_process.terminate.assert_called_once()
