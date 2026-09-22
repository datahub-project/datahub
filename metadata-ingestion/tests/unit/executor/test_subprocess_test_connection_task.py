import asyncio
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional
from unittest.mock import Mock, mock_open, patch

import pytest
import yaml

from datahub.executor.context.execution_context import ExecutionContext
from datahub.executor.context.executor_context import ExecutorContext
from datahub.executor.execution.sub_process_task_common import SubProcessTaskUtil
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
            "datahub.executor.execution.sub_process_task_common.setup_venv"
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
        # Recipe secrets plus pip-referenced env values; extra_env_vars are
        # deliberately NOT treated as secrets (plaintext in the source config).
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
            "datahub.executor.execution.sub_process_task_common.setup_venv"
        ) as mock_setup_venv,
        patch(
            "datahub.executor.execution.sub_process_test_connection_task.subprocess.Popen",
            return_value=mock_process,
        ) as mock_popen,
        patch("os.path.exists", return_value=False),  # No report file on failure
        patch(
            "datahub.executor.execution.sub_process_task_common.SubProcessTaskUtil._remove_directory"
        ) as _mock_remove_dir,
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
            "datahub.executor.execution.sub_process_task_common.setup_venv"
        ) as mock_setup_venv,
        patch(
            "datahub.executor.execution.sub_process_test_connection_task.subprocess.Popen",
            return_value=mock_process,
        ),
        patch("os.path.exists", return_value=False),
        patch(
            "datahub.executor.execution.sub_process_task_common.SubProcessTaskUtil._remove_directory"
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


async def test_exec_out_dir_exists_when_the_subprocess_is_launched(
    executor_ctx: ExecutorContext,
    exec_ctx: ExecutionContext,
    sample_args: dict[str, str],
    tmp_path: Path,
) -> None:
    """The connection report is written into exec_out_dir, so it must exist by launch.

    Only the dynamic venv path creates it, via `uv venv`. "bundled" and "native" reuse a
    prebuilt venv and return without touching it, so the task has to create it itself.
    """
    config = SubProcessTestConnectionTaskConfig(tmp_dir=str(tmp_path / "ingest"))
    task = SubProcessTestConnectionTask(config, executor_ctx)
    exec_out_dir = Path(config.tmp_dir) / exec_ctx.exec_id

    observed: dict[str, bool] = {}

    def record_and_stop(*_args: Any, **_kwargs: Any) -> Mock:
        observed["exec_out_dir_exists"] = exec_out_dir.is_dir()
        raise RuntimeError("stop here; the directory is all this test cares about")

    venv_ref = Mock()
    venv_ref.venv_loc = tmp_path / "opt" / "datahub" / "venvs" / "demo-data-bundled"

    with (
        patch(
            "datahub.executor.execution.sub_process_task_common.setup_venv",
            return_value=venv_ref,
        ),
        patch(
            "datahub.executor.execution.sub_process_test_connection_task.subprocess.Popen",
            side_effect=record_and_stop,
        ),
        pytest.raises(RuntimeError),
    ):
        await task.execute(sample_args, exec_ctx)

    assert observed["exec_out_dir_exists"]


@pytest.mark.asyncio
async def test_a_popen_failure_releases_the_venv_cache_lock(
    executor_ctx: ExecutorContext,
    exec_ctx: ExecutionContext,
    sample_args: dict[str, str],
    tmp_path: Path,
) -> None:
    """Spawning sits before the try/finally that calls finalize_task_output.

    So an OSError from Popen -- ENOMEM, a bad interpreter path -- or a broken
    pipe on the stdin write is the one window on this path where nothing
    releases the cached venv's SHARED lock. Left held, eviction can never
    reclaim that entry, because eviction needs a non-blocking exclusive.
    """
    config = SubProcessTestConnectionTaskConfig(tmp_dir=str(tmp_path / "ingest"))
    task = SubProcessTestConnectionTask(config, executor_ctx)

    venv_ref = Mock()
    venv_ref.venv_loc = tmp_path / "venv-demo-data"

    with (
        patch(
            "datahub.executor.execution.sub_process_task_common.setup_venv",
            return_value=venv_ref,
        ),
        patch(
            "datahub.executor.execution.sub_process_test_connection_task.subprocess.Popen",
            side_effect=OSError("cannot fork"),
        ),
        pytest.raises(OSError, match="cannot fork"),
    ):
        await task.execute(sample_args, exec_ctx)

    venv_ref.lock.release.assert_called_once()


@pytest.mark.asyncio
async def test_a_successful_run_forwards_the_venv_ref_to_finalize(
    executor_ctx: ExecutorContext,
    exec_ctx: ExecutionContext,
    sample_args: dict[str, str],
    tmp_path: Path,
) -> None:
    """finalize_task_output is the only thing that releases the lock here.

    It is what releases venv_ref.lock, and the keyword in the finally block is
    the only place the reference reaches it. Drop it and every test connection
    leaves its cached entry held SHARED for the life of the pod, so eviction --
    which needs a non-blocking exclusive -- can never reclaim any of them.
    Nothing else on this path observes the forwarding.
    """
    config = SubProcessTestConnectionTaskConfig(tmp_dir=str(tmp_path / "ingest"))
    task = SubProcessTestConnectionTask(config, executor_ctx)

    venv_ref = Mock()
    venv_ref.venv_loc = tmp_path / "venv-demo-data"

    finished_process = Mock()
    finished_process.poll.return_value = 0
    finished_process.stdin = Mock()

    mock_finalize = Mock()

    with (
        patch(
            "datahub.executor.execution.sub_process_task_common.setup_venv",
            return_value=venv_ref,
        ),
        patch(
            "datahub.executor.execution.sub_process_test_connection_task.subprocess.Popen",
            return_value=finished_process,
        ),
        patch.object(SubProcessTaskUtil, "finalize_task_output", new=mock_finalize),
    ):
        await task.execute(sample_args, exec_ctx)

    assert mock_finalize.call_args.kwargs["venv_ref"] is venv_ref


@pytest.mark.asyncio
async def test_a_stdin_failure_after_popen_keeps_the_venv_cache_lock(
    executor_ctx: ExecutorContext,
    exec_ctx: ExecutionContext,
    sample_args: dict[str, str],
    tmp_path: Path,
) -> None:
    """Popen succeeding and the stdin write failing means a child is running.

    The except covering this window releases the lock, which is right when
    Popen itself failed and wrong once it produced a child: eviction would then
    be free to rmtree the venv that child is executing out of. The two cases
    reach the same handler, so the handler has to tell them apart.
    """
    config = SubProcessTestConnectionTaskConfig(tmp_dir=str(tmp_path / "ingest"))
    task = SubProcessTestConnectionTask(config, executor_ctx)

    venv_ref = Mock()
    venv_ref.venv_loc = tmp_path / "venv-demo-data"
    lock = venv_ref.lock

    live_child = Mock()
    live_child.poll = Mock(return_value=None)  # never reaped: may still be running
    live_child.stdin = Mock()
    live_child.stdin.write = Mock(side_effect=BrokenPipeError("EPIPE"))

    with (
        patch(
            "datahub.executor.execution.sub_process_task_common.setup_venv",
            return_value=venv_ref,
        ),
        patch(
            "datahub.executor.execution.sub_process_test_connection_task.subprocess.Popen",
            return_value=live_child,
        ),
        pytest.raises(BrokenPipeError),
    ):
        await task.execute(sample_args, exec_ctx)

    assert venv_ref.lock is None, "the lock must be detached, not released"
    lock.release.assert_not_called()


@pytest.mark.asyncio
async def test_cancellation_keeps_the_venv_cache_lock_while_the_child_lives(
    task_config: SubProcessTestConnectionTaskConfig,
    executor_ctx: ExecutorContext,
    exec_ctx: ExecutionContext,
    sample_args: dict[str, str],
) -> None:
    """terminate() signals and re-raises without waiting for the child to go.

    SIGTERM against a process wedged in an uninterruptible syscall is not
    delivered until that syscall returns, so finalize_task_output would drop
    the lock on a venv still in use. Ingestion already guarded this path; this
    one did not.
    """
    task = SubProcessTestConnectionTask(task_config, executor_ctx)
    args: dict[str, Any] = {
        **sample_args,
        "extra_env_vars": {},
        "extra_pip_requirements": [],
        "extra_pip_plugins": [],
    }

    entered_read_loop = asyncio.Event()

    def _readline() -> str:
        entered_read_loop.set()
        return ""

    live_child = Mock()
    live_child.returncode = None
    live_child.poll = Mock(return_value=None)
    live_child.stdout = Mock()
    live_child.stdout.readline = Mock(side_effect=_readline)
    live_child.stdin = Mock()
    live_child.terminate = Mock()

    venv_ref = Mock()
    venv_ref.venv_loc = "/tmp/venv-demo-data-test"
    lock = venv_ref.lock

    with (
        patch(
            "datahub.executor.execution.sub_process_task_common.SubProcessTaskUtil._resolve_recipe",
            return_value=({"source": {"type": "demo-data"}}, {}),
        ),
        patch(
            "datahub.executor.execution.sub_process_task_common.SubProcessTaskUtil._get_plugin_from_recipe",
            return_value="demo-data",
        ),
        patch(
            "datahub.executor.execution.sub_process_task_common.setup_venv",
            return_value=venv_ref,
        ),
        patch(
            "datahub.executor.execution.sub_process_test_connection_task.subprocess.Popen",
            return_value=live_child,
        ),
        patch("os.path.exists", return_value=False),
        patch(
            "datahub.executor.execution.sub_process_task_common.SubProcessTaskUtil._remove_directory"
        ),
    ):
        pending = asyncio.ensure_future(task.execute(args, exec_ctx))
        await asyncio.wait_for(entered_read_loop.wait(), timeout=5)
        pending.cancel()

        with pytest.raises(asyncio.CancelledError):
            await pending

    assert venv_ref.lock is None, "the lock must be detached, not released"
    lock.release.assert_not_called()


@pytest.mark.asyncio
async def test_a_wrapper_resolution_failure_releases_the_lock_and_cleans_up(
    executor_ctx: ExecutorContext,
    exec_ctx: ExecutionContext,
    sample_args: dict[str, str],
    tmp_path: Path,
) -> None:
    """The window between prepare_recipe_run and the spawn try was unguarded.

    prepare_recipe_run returns holding the entry SHARED, and
    resolve_wrapper_script runs before any handler: it raises RuntimeError
    when importlib.util.find_spec returns None, which is what a packaging or
    partially-installed-image failure looks like. Nothing released the lock
    and nothing removed exec_out_dir -- and because the failure is
    deterministic, every test-connection request repeated it, pinning one
    unevictable entry per distinct recipe key for the life of the pod.
    """
    config = SubProcessTestConnectionTaskConfig(tmp_dir=str(tmp_path / "ingest"))
    task = SubProcessTestConnectionTask(config, executor_ctx)

    venv_ref = Mock()
    venv_ref.venv_loc = tmp_path / "venv-demo-data"

    with (
        patch(
            "datahub.executor.execution.sub_process_task_common.setup_venv",
            return_value=venv_ref,
        ),
        patch(
            "datahub.executor.execution.sub_process_test_connection_task.resolve_wrapper_script",
            side_effect=RuntimeError("wrapper module not found"),
        ),
        pytest.raises(RuntimeError, match="wrapper module not found"),
    ):
        await task.execute(sample_args, exec_ctx)

    venv_ref.lock.release.assert_called_once()
    assert not Path(f"{config.tmp_dir}/{exec_ctx.exec_id}").exists(), (
        "the per-execution directory was left behind; on the cache-off path "
        "it holds a complete per-run venv"
    )


@pytest.mark.asyncio
async def test_a_popen_failure_also_removes_the_execution_directory(
    executor_ctx: ExecutorContext,
    exec_ctx: ExecutionContext,
    sample_args: dict[str, str],
    tmp_path: Path,
) -> None:
    """The ingestion task removes exec_out_dir in the identical situation.

    On the cache-off or cache-busy path that directory holds a complete
    per-run venv, so each such failure leaks a full venv's worth of disk.
    """
    config = SubProcessTestConnectionTaskConfig(tmp_dir=str(tmp_path / "ingest"))
    task = SubProcessTestConnectionTask(config, executor_ctx)

    venv_ref = Mock()
    venv_ref.venv_loc = tmp_path / "venv-demo-data"

    with (
        patch(
            "datahub.executor.execution.sub_process_task_common.setup_venv",
            return_value=venv_ref,
        ),
        patch(
            "datahub.executor.execution.sub_process_test_connection_task.subprocess.Popen",
            side_effect=OSError("cannot fork"),
        ),
        pytest.raises(OSError, match="cannot fork"),
    ):
        await task.execute(sample_args, exec_ctx)

    assert not Path(f"{config.tmp_dir}/{exec_ctx.exec_id}").exists()


@pytest.mark.asyncio
async def test_a_cancelled_run_reaps_its_child_and_releases_the_lock(
    executor_ctx: ExecutorContext,
    exec_ctx: ExecutionContext,
    sample_args: dict[str, str],
    tmp_path: Path,
) -> None:
    """Signalling without waiting pinned the shared `latest` entry forever.

    Popen.poll() answers None for a signalled-but-unreaped child, so
    keep_venv_lock_if_popen_may_be_alive handed the SHARED hold to
    retain_lock for the life of the process. flock conflicts across fds
    within one process, so once that entry passed its TTL no rebuild could
    take EXCLUSIVE again -- and `latest` is the default and is deliberately
    shared with ingestion, so one user pressing Cancel made every run on the
    pod fall back to a full per-run build.

    Reaping the child makes poll() truthful, and the lock is released
    normally.
    """
    config = SubProcessTestConnectionTaskConfig(tmp_dir=str(tmp_path / "ingest"))
    task = SubProcessTestConnectionTask(config, executor_ctx)

    venv_ref = Mock()
    venv_ref.venv_loc = tmp_path / "venv-demo-data"

    process = Mock()
    process.pid = 4321
    process.stdin = Mock()
    process.stdout = Mock()
    process.stdout.readline = Mock(side_effect=asyncio.CancelledError)
    # None while running; terminate()+wait() is what makes it report exited.
    poll_results: list[Optional[int]] = [None]
    process.poll = Mock(side_effect=lambda: poll_results[0])

    def reaped(timeout: object = None) -> int:
        poll_results[0] = -15
        return -15

    process.wait = Mock(side_effect=reaped)

    with (
        patch(
            "datahub.executor.execution.sub_process_task_common.setup_venv",
            return_value=venv_ref,
        ),
        patch(
            "datahub.executor.execution.sub_process_test_connection_task.subprocess.Popen",
            return_value=process,
        ),
        pytest.raises(asyncio.CancelledError),
    ):
        await task.execute(sample_args, exec_ctx)

    process.terminate.assert_called_once()
    process.wait.assert_called()
    venv_ref.lock.release.assert_called_once()
    assert venv_ref.lock is not None, (
        "the lock was detached and retained for the process's life even "
        "though the child was confirmed dead"
    )
