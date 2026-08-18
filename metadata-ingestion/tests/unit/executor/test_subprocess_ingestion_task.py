import asyncio
import errno
import json
import signal
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock, call, mock_open, patch

import pydantic
import pytest
import yaml

from datahub.executor.context.execution_context import ExecutionContext
from datahub.executor.context.executor_context import ExecutorContext
from datahub.executor.execution.runner import (
    LogHolder,
    SubprocessRunner,
    VenvConfig,
    VenvReference,
)
from datahub.executor.execution.sub_process_ingestion_task import (
    SubProcessIngestionTask,
    SubProcessIngestionTaskArgs,
    SubProcessIngestionTaskConfig,
)
from datahub.executor.execution.sub_process_task_common import SubProcessTaskUtil
from datahub.executor.execution.task import TaskError
from datahub.executor.report.execution_report import ExecutionReport
from datahub.executor.request.execution_request import ExecutionRequest
from datahub.masking.secret_registry import SecretRegistry

_RESOLVE_RECIPE = (
    "datahub.executor.execution.sub_process_task_common"
    ".SubProcessTaskUtil._resolve_recipe"
)
_GET_PLUGIN = (
    "datahub.executor.execution.sub_process_task_common"
    ".SubProcessTaskUtil._get_plugin_from_recipe"
)
_REMOVE_DIRECTORY = (
    "datahub.executor.execution.sub_process_task_common"
    ".SubProcessTaskUtil._remove_directory"
)
_FORMAT_LOG_LINES = (
    "datahub.executor.execution.sub_process_task_common"
    ".SubProcessTaskUtil._format_log_lines"
)
_SETUP_VENV = "datahub.executor.execution.sub_process_ingestion_task.setup_venv"


@pytest.fixture(autouse=True)
def reset_secret_registry() -> Iterator[None]:
    # SecretRegistry is a process-wide singleton and _handle_subprocess_completion
    # both reads it (to mask the structured report) and clears it via
    # shutdown_secret_masking(). Reset around every test so these tests neither
    # inherit nor leak registered secrets under --random-order.
    SecretRegistry.reset_instance()
    yield
    SecretRegistry.reset_instance()


@pytest.fixture
def mock_executor_context() -> Mock:
    ctx = Mock(spec=ExecutorContext)
    ctx.get_secret_stores.return_value = []
    return ctx


@pytest.fixture
def mock_execution_context() -> Mock:
    ctx = Mock(spec=ExecutionContext)
    ctx.exec_id = str(uuid.uuid4())
    ctx.request = Mock(spec=ExecutionRequest)
    ctx.request.progress_callback = None

    report = Mock(spec=ExecutionReport)
    report.report_info = Mock()
    report.report_error = Mock()
    report.set_structured_report = Mock()
    report.set_logs = Mock()
    ctx.get_report.return_value = report

    return ctx


@pytest.fixture
def task_config() -> SubProcessIngestionTaskConfig:
    return SubProcessIngestionTaskConfig(
        tmp_dir="/tmp/test",
        log_dir="/tmp/test/logs",
        heartbeat_time_seconds=1,
        max_log_lines=100,
    )


@pytest.fixture
def ingestion_task(
    task_config: SubProcessIngestionTaskConfig, mock_executor_context: Mock
) -> SubProcessIngestionTask:
    return SubProcessIngestionTask(task_config, mock_executor_context)


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
    return {"recipe": sample_recipe, "version": "latest", "debug_mode": "false"}


class TestSubProcessIngestionTaskConfig:
    def test_default_config(self) -> None:
        config = SubProcessIngestionTaskConfig()
        assert config.tmp_dir == "/tmp/datahub/ingest"
        assert config.log_dir == "/tmp/datahub/logs"
        assert config.heartbeat_time_seconds == 2
        assert config.max_log_lines == 2000


class TestSubProcessIngestionTaskCreation:
    def test_create_from_config(self, mock_executor_context: Mock) -> None:
        config_dict: dict[str, Any] = {
            "tmp_dir": "/custom/tmp",
            "log_dir": "/custom/logs",
            "heartbeat_time_seconds": 5,
        }

        task = SubProcessIngestionTask.create(config_dict, mock_executor_context)

        assert isinstance(task, SubProcessIngestionTask)
        assert task.config.tmp_dir == "/custom/tmp"
        assert task.config.log_dir == "/custom/logs"
        assert task.config.heartbeat_time_seconds == 5

    def test_init_sets_attributes(
        self, task_config: SubProcessIngestionTaskConfig, mock_executor_context: Mock
    ) -> None:
        task = SubProcessIngestionTask(task_config, mock_executor_context)

        assert task.config == task_config
        assert task.tmp_dir == task_config.tmp_dir
        assert task.ctx == mock_executor_context


class TestSubProcessIngestionTaskDirectorySetup:
    def test_setup_directories(self, ingestion_task: SubProcessIngestionTask) -> None:
        exec_id = "test-exec-id"

        with patch("pathlib.Path.mkdir") as mock_mkdir:
            exec_out_dir, artifact_output_dir, report_out_file = (
                ingestion_task._setup_directories(exec_id)
            )

            expected_exec_out_dir = f"/tmp/test/{exec_id}"
            expected_artifact_output_dir = f"/tmp/test/logs/{exec_id}"
            expected_report_out_file = (
                f"{expected_artifact_output_dir}/artifacts/ingestion_report.json"
            )

            assert exec_out_dir == expected_exec_out_dir
            assert artifact_output_dir == expected_artifact_output_dir
            assert report_out_file == expected_report_out_file

            # exec_out_dir + the two artifact subdirectories.
            assert mock_mkdir.call_count == 3

    @patch("pathlib.Path.mkdir")
    def test_setup_directories_creates_with_correct_permissions(
        self, mock_mkdir: Mock, ingestion_task: SubProcessIngestionTask
    ) -> None:
        exec_id = "test-exec-id"

        ingestion_task._setup_directories(exec_id)

        for mkdir_call in mock_mkdir.call_args_list:
            assert mkdir_call[0][0] == 0o755  # First positional arg should be mode
            assert mkdir_call[1]["parents"]
            assert mkdir_call[1]["exist_ok"]

    def test_setup_directories_creates_exec_out_dir_on_disk(
        self,
        tmp_path: Path,
        mock_executor_context: Mock,
    ) -> None:
        """exec_out_dir must exist on disk after _setup_directories so bundled/native runs don't raise FileNotFoundError at cleanup."""
        config = SubProcessIngestionTaskConfig(
            tmp_dir=str(tmp_path / "ingest"),
            log_dir=str(tmp_path / "logs"),
        )
        task = SubProcessIngestionTask(config, mock_executor_context)
        exec_id = "regression-exec-id"

        exec_out_dir, _, _ = task._setup_directories(exec_id)

        assert Path(exec_out_dir).is_dir()


class TestSubProcessIngestionTaskEnvironmentPreparation:
    def test_prepare_subprocess_environment(
        self,
        ingestion_task: SubProcessIngestionTask,
        sample_args: dict[str, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        validated_args = SubProcessIngestionTaskArgs.model_validate(sample_args)
        exec_out_dir = "/tmp/test/exec"
        artifact_output_dir = "/tmp/test/logs/artifacts"

        monkeypatch.setenv("TEST_VAR", "test_value")
        # TMPDIR must be absent for the setdefault fallback to be observable.
        monkeypatch.delenv("TMPDIR", raising=False)

        env = ingestion_task._prepare_subprocess_environment(
            validated_args, exec_out_dir, artifact_output_dir
        )

        assert env["INGESTION_ARTIFACT_DIR"] == f"{artifact_output_dir}/artifacts"
        assert env["TMPDIR"] == exec_out_dir
        assert env["TEST_VAR"] == "test_value"

    def test_prepare_subprocess_environment_with_custom_env_vars(
        self, ingestion_task: SubProcessIngestionTask
    ) -> None:
        args_with_env: dict[str, str] = {
            "recipe": json.dumps({"source": {"type": "test"}}),
            "extra_env_vars": json.dumps({"CUSTOM_VAR": "custom_value"}),
        }
        validated_args = SubProcessIngestionTaskArgs.model_validate(args_with_env)

        env = ingestion_task._prepare_subprocess_environment(
            validated_args, "/tmp/exec", "/tmp/logs"
        )

        assert env["CUSTOM_VAR"] == "custom_value"

    def test_prepare_subprocess_environment_preserves_tmpdir_from_args(
        self, ingestion_task: SubProcessIngestionTask
    ) -> None:
        args_with_env: dict[str, str] = {
            "recipe": json.dumps({"source": {"type": "test"}}),
            "extra_env_vars": json.dumps({"TMPDIR": "/custom/tmp"}),
        }
        validated_args = SubProcessIngestionTaskArgs.model_validate(args_with_env)

        env = ingestion_task._prepare_subprocess_environment(
            validated_args, "/tmp/exec", "/tmp/logs"
        )

        # A user-supplied TMPDIR wins over both the inherited system value and
        # the exec_out_dir setdefault fallback.
        assert env["TMPDIR"] == "/custom/tmp"


class TestSignalProcessGroup:
    def test_signals_whole_group(self) -> None:
        proc = Mock()
        proc.pid = 4321
        with (
            patch("os.getpgid", return_value=4321) as getpgid,
            patch("os.killpg") as killpg,
        ):
            SubProcessIngestionTask._signal_process_group(proc, signal.SIGTERM)
        getpgid.assert_called_once_with(4321)
        killpg.assert_called_once_with(4321, signal.SIGTERM)

    def test_swallows_already_exited(self) -> None:
        """A process that exited between the check and the signal must not raise --
        the caller is already in a cleanup path handling another exception."""
        proc = Mock()
        proc.pid = 4321
        with (
            patch("os.getpgid", side_effect=ProcessLookupError),
            patch("os.killpg") as killpg,
        ):
            SubProcessIngestionTask._signal_process_group(proc, signal.SIGKILL)
        killpg.assert_not_called()


class TestSubProcessIngestionTaskSubprocessCreation:
    async def test_create_subprocess(
        self, ingestion_task: SubProcessIngestionTask, sample_args: dict[str, str]
    ) -> None:
        validated_args = SubProcessIngestionTaskArgs.model_validate(sample_args)
        plugin = "demo-data"
        recipe_dict = {"source": {"type": "demo-data"}}
        report_out_file = "/tmp/report.json"
        subprocess_env = {"TEST": "value"}
        exec_out_dir = "/tmp/exec"
        secret_values: dict[str, str] = {}

        shared_logs = LogHolder()

        mock_process = AsyncMock()
        # stdin.write / stdin.close are sync on asyncio.subprocess, use plain Mock
        mock_process.stdin = Mock()

        # _create_subprocess sets the venv up first; stub that out.
        mock_venv_ref = Mock()
        mock_venv_ref.venv_loc = "/tmp/venv-demo-data-abc123"

        with (
            patch(
                "asyncio.create_subprocess_exec", return_value=mock_process
            ) as mock_create,
            patch.object(
                ingestion_task, "_setup_venv", return_value=mock_venv_ref
            ) as mock_setup_venv,
        ):
            result = await ingestion_task._create_subprocess(
                validated_args,
                plugin,
                recipe_dict,
                report_out_file,
                subprocess_env,
                exec_out_dir,
                shared_logs,
                secret_values,
            )

            assert result == mock_process

            mock_setup_venv.assert_called_once_with(
                validated_args, plugin, exec_out_dir, shared_logs
            )

            mock_create.assert_called_once()
            call_args = mock_create.call_args

            # The wrapper runs by ABSOLUTE PATH under this interpreter -- not as a bare
            # script name off PATH, and deliberately not with `-m`. `-m` puts the
            # subprocess's CWD on sys.path[0], so a stray module in the working
            # directory (e.g. a yaml.py in /tmp, the image's WORKDIR) shadows real
            # imports and kills the run before any wrapper code executes.
            command_args = call_args[0]
            assert command_args[0] == sys.executable
            assert "-m" not in command_args
            assert command_args[1].endswith("datahub/executor/wrappers/run_ingest.py")
            assert Path(command_args[1]).is_absolute()
            assert command_args[2] == str(mock_venv_ref.venv_loc)

            kwargs = call_args[1]
            assert kwargs["env"]["VENV_PATH"] == str(mock_venv_ref.venv_loc)
            assert kwargs["stdout"] == asyncio.subprocess.PIPE
            assert kwargs["stderr"] == asyncio.subprocess.STDOUT
            assert kwargs["stdin"] == asyncio.subprocess.PIPE
            # Own session/process group, so cancellation can signal the whole tree.
            assert kwargs["start_new_session"] is True

    async def test_create_subprocess_writes_stdin_envelope(
        self, ingestion_task: SubProcessIngestionTask, sample_args: dict[str, str]
    ) -> None:
        """Verify the JSON envelope written to subprocess stdin contains recipe and secrets."""
        validated_args = SubProcessIngestionTaskArgs.model_validate(sample_args)
        recipe_dict = {"source": {"type": "demo-data"}, "run_id": "test-run-id"}
        secret_values = {"DB_PASSWORD": "s3cret", "API_KEY": "k3y"}

        shared_logs = LogHolder()
        mock_process = AsyncMock()
        # stdin.write / stdin.close are sync on asyncio.subprocess, use plain Mock
        mock_process.stdin = Mock()

        mock_venv_ref = Mock()
        mock_venv_ref.venv_loc = "/tmp/venv-demo-data-abc123"

        with (
            patch("asyncio.create_subprocess_exec", return_value=mock_process),
            patch.object(ingestion_task, "_setup_venv", return_value=mock_venv_ref),
        ):
            await ingestion_task._create_subprocess(
                validated_args,
                "demo-data",
                recipe_dict,
                "/tmp/report.json",
                {"PATH": "/usr/bin"},
                "/tmp/exec",
                shared_logs,
                secret_values,
            )

            mock_process.stdin.write.assert_called_once()
            mock_process.stdin.close.assert_called_once()

            raw_envelope = mock_process.stdin.write.call_args[0][0]
            envelope = json.loads(raw_envelope.decode("utf-8"))

            # Envelope uses datahub-compatible format
            assert yaml.safe_load(envelope["__recipe_yaml__"]) == recipe_dict
            assert envelope["__secrets__"] == secret_values
            assert envelope["__report_out_file__"] == "/tmp/report.json"
            assert envelope["__debug_mode__"] == "false"

    async def test_create_subprocess_secrets_not_in_env(
        self, ingestion_task: SubProcessIngestionTask, sample_args: dict[str, str]
    ) -> None:
        """Verify secret values are NOT leaked into the subprocess environment dict."""
        validated_args = SubProcessIngestionTaskArgs.model_validate(sample_args)
        recipe_dict = {"source": {"type": "demo-data"}}
        secret_values = {"DB_PASSWORD": "s3cret_val", "API_KEY": "k3y_val"}

        shared_logs = LogHolder()
        mock_process = AsyncMock()
        mock_process.stdin = Mock()

        mock_venv_ref = Mock()
        mock_venv_ref.venv_loc = "/tmp/venv-demo-data-abc123"

        with (
            patch(
                "asyncio.create_subprocess_exec", return_value=mock_process
            ) as mock_create,
            patch.object(ingestion_task, "_setup_venv", return_value=mock_venv_ref),
        ):
            await ingestion_task._create_subprocess(
                validated_args,
                "demo-data",
                recipe_dict,
                "/tmp/report.json",
                {"PATH": "/usr/bin"},
                "/tmp/exec",
                shared_logs,
                secret_values,
            )

            subprocess_env = mock_create.call_args[1]["env"]

            all_env_values = " ".join(subprocess_env.values())
            for secret_val in secret_values.values():
                assert secret_val not in all_env_values, (
                    f"Secret value '{secret_val}' leaked into subprocess env"
                )

            # Secret names should not be env keys either (they're not config vars)
            assert "DB_PASSWORD" not in subprocess_env
            assert "API_KEY" not in subprocess_env


class TestSubProcessIngestionTaskExecution:
    async def test_execute_successful_ingestion(
        self,
        ingestion_task: SubProcessIngestionTask,
        sample_args: dict[str, str],
        mock_execution_context: Mock,
    ) -> None:
        mock_process = AsyncMock()
        mock_process.returncode = 0
        mock_process.stdout = AsyncMock()

        with (
            patch.multiple(
                ingestion_task,
                _setup_directories=Mock(
                    return_value=("/tmp/exec", "/tmp/logs", "/tmp/report.json")
                ),
                _prepare_subprocess_environment=Mock(return_value={}),
                _create_subprocess=AsyncMock(return_value=mock_process),
                _monitor_subprocess=AsyncMock(),
                _handle_subprocess_completion=Mock(),
            ),
            patch(_RESOLVE_RECIPE) as mock_resolve,
            patch(_GET_PLUGIN) as mock_get_plugin,
            patch("builtins.open", mock_open()),
        ):
            mock_resolve.return_value = ({"source": {"type": "demo-data"}}, {})
            mock_get_plugin.return_value = "demo-data"

            await ingestion_task.execute(sample_args, mock_execution_context)

            ingestion_task._setup_directories.assert_called_once_with(  # type: ignore[attr-defined]
                mock_execution_context.exec_id
            )
            ingestion_task._create_subprocess.assert_called_once()  # type: ignore[attr-defined]
            ingestion_task._monitor_subprocess.assert_called_once()  # type: ignore[attr-defined]
            ingestion_task._handle_subprocess_completion.assert_called_once()  # type: ignore[attr-defined]

    async def test_execute_publishes_artifact_dir_on_context(
        self,
        ingestion_task: SubProcessIngestionTask,
        sample_args: dict[str, str],
        mock_execution_context: Mock,
    ) -> None:
        # The artifact directory deliberately outlives the run, so callers locate
        # this run's logs and artifacts through the context once execute() returns.
        mock_process = AsyncMock()
        mock_process.returncode = 0

        with (
            patch.multiple(
                ingestion_task,
                _setup_directories=Mock(
                    return_value=(
                        "/tmp/exec",
                        "/tmp/logs/some-exec-id",
                        "/tmp/report.json",
                    )
                ),
                _prepare_subprocess_environment=Mock(return_value={}),
                _create_subprocess=AsyncMock(return_value=mock_process),
                _monitor_subprocess=AsyncMock(),
                _handle_subprocess_completion=Mock(),
            ),
            patch(
                _RESOLVE_RECIPE, return_value=({"source": {"type": "demo-data"}}, {})
            ),
            patch(_GET_PLUGIN, return_value="demo-data"),
            patch("builtins.open", mock_open()),
        ):
            await ingestion_task.execute(sample_args, mock_execution_context)

        mock_execution_context.set_artifact_dir.assert_called_once_with(
            "/tmp/logs/some-exec-id"
        )

    async def test_artifact_dir_is_not_published_when_the_subprocess_never_starts(
        self,
        ingestion_task: SubProcessIngestionTask,
        sample_args: dict[str, str],
        mock_execution_context: Mock,
    ) -> None:
        """Venv setup happens inside _create_subprocess and can fail.

        At that point the log file has been opened but nothing has been written to it --
        venv output lives in the in-memory LogHolder until _monitor_subprocess runs. If
        the artifact directory were published anyway, a consumer that uploads artifacts
        would ship a zero-byte log for every venv or dependency-resolution failure, and
        those are common. The useful error text is in the result report instead.
        """
        with (
            patch.multiple(
                ingestion_task,
                _setup_directories=Mock(
                    return_value=(
                        "/tmp/exec",
                        "/tmp/logs/some-exec-id",
                        "/tmp/report.json",
                    )
                ),
                _prepare_subprocess_environment=Mock(return_value={}),
                _create_subprocess=AsyncMock(
                    side_effect=TaskError("venv setup failed")
                ),
            ),
            patch(
                _RESOLVE_RECIPE, return_value=({"source": {"type": "demo-data"}}, {})
            ),
            patch(_GET_PLUGIN, return_value="demo-data"),
            patch("builtins.open", mock_open()),
            pytest.raises(TaskError),
        ):
            await ingestion_task.execute(sample_args, mock_execution_context)

        mock_execution_context.set_artifact_dir.assert_not_called()

    async def test_execute_handles_task_error_from_recipe_resolution(
        self,
        ingestion_task: SubProcessIngestionTask,
        sample_args: dict[str, str],
        mock_execution_context: Mock,
    ) -> None:
        with patch(_RESOLVE_RECIPE) as mock_resolve:
            mock_resolve.side_effect = TaskError("Failed to resolve recipe")

            with pytest.raises(TaskError, match="Failed to resolve recipe"):
                await ingestion_task.execute(sample_args, mock_execution_context)

    async def test_execute_handles_directory_creation_failure(
        self,
        ingestion_task: SubProcessIngestionTask,
        sample_args: dict[str, str],
        mock_execution_context: Mock,
    ) -> None:
        with patch.object(ingestion_task, "_setup_directories") as mock_setup:
            mock_setup.side_effect = OSError("Permission denied")

            with pytest.raises(OSError, match="Permission denied"):
                await ingestion_task.execute(sample_args, mock_execution_context)

    async def test_execute_validates_arguments(
        self, ingestion_task: SubProcessIngestionTask, mock_execution_context: Mock
    ) -> None:
        invalid_args = {"invalid": "args"}

        with pytest.raises(pydantic.ValidationError):
            await ingestion_task.execute(invalid_args, mock_execution_context)

    async def test_execute_cancellation_propagates_cancelled_error(
        self,
        ingestion_task: SubProcessIngestionTask,
        sample_args: dict[str, str],
        mock_execution_context: Mock,
    ) -> None:
        # When _monitor_subprocess raises CancelledError, it must propagate out
        # of _execute_with_debug (so DefaultExecutor reports CANCELLED) and the
        # finally cleanup must still run with cancelled=True.
        mock_process = AsyncMock()
        mock_process.returncode = -15
        mock_completion = Mock()

        with (
            patch.multiple(
                ingestion_task,
                _setup_directories=Mock(
                    return_value=("/tmp/exec", "/tmp/logs", "/tmp/report.json")
                ),
                _prepare_subprocess_environment=Mock(return_value={}),
                _create_subprocess=AsyncMock(return_value=mock_process),
                _monitor_subprocess=AsyncMock(side_effect=asyncio.CancelledError()),
                _handle_subprocess_completion=mock_completion,
            ),
            patch(_RESOLVE_RECIPE) as mock_resolve,
            patch(_GET_PLUGIN) as mock_get_plugin,
            patch("builtins.open", mock_open()),
        ):
            mock_resolve.return_value = ({"source": {"type": "demo-data"}}, {})
            mock_get_plugin.return_value = "demo-data"

            with pytest.raises(asyncio.CancelledError):
                await ingestion_task.execute(sample_args, mock_execution_context)

            mock_completion.assert_called_once()
            assert mock_completion.call_args.kwargs.get("cancelled") is True

    async def test_execute_passes_cancelled_false_on_normal_completion(
        self,
        ingestion_task: SubProcessIngestionTask,
        sample_args: dict[str, str],
        mock_execution_context: Mock,
    ) -> None:
        # On normal (non-cancelled) completion, _handle_subprocess_completion
        # is invoked with cancelled=False so its return-code interpretation
        # branch runs and the task fails for genuine subprocess failures.
        mock_process = AsyncMock()
        mock_process.returncode = 0
        mock_completion = Mock()

        with (
            patch.multiple(
                ingestion_task,
                _setup_directories=Mock(
                    return_value=("/tmp/exec", "/tmp/logs", "/tmp/report.json")
                ),
                _prepare_subprocess_environment=Mock(return_value={}),
                _create_subprocess=AsyncMock(return_value=mock_process),
                _monitor_subprocess=AsyncMock(),  # completes normally
                _handle_subprocess_completion=mock_completion,
            ),
            patch(_RESOLVE_RECIPE) as mock_resolve,
            patch(_GET_PLUGIN) as mock_get_plugin,
            patch("builtins.open", mock_open()),
        ):
            mock_resolve.return_value = ({"source": {"type": "demo-data"}}, {})
            mock_get_plugin.return_value = "demo-data"

            await ingestion_task.execute(sample_args, mock_execution_context)

            assert mock_completion.call_args.kwargs.get("cancelled") is False


class TestSubProcessIngestionTaskCompletion:
    def test_handle_subprocess_completion_success(
        self, ingestion_task: SubProcessIngestionTask, mock_execution_context: Mock
    ) -> None:
        mock_process = Mock()
        mock_process.returncode = 0

        report_out_file = "/tmp/report.json"
        artifact_output_dir = "/tmp/artifacts"
        recipe = {"pipeline_name": "test-pipeline"}
        exec_out_dir = "/tmp/exec"
        shared_logs = LogHolder()
        shared_logs.append("log line 1\n")
        shared_logs.append("log line 2\n")

        with (
            patch("os.path.exists", return_value=True),
            patch("builtins.open", mock_open(read_data='{"test": "report"}')),
            patch(_REMOVE_DIRECTORY) as mock_remove,
            patch(_FORMAT_LOG_LINES) as mock_format,
        ):
            mock_format.return_value = "formatted logs"

            ingestion_task._handle_subprocess_completion(
                mock_process,
                mock_execution_context,
                report_out_file,
                artifact_output_dir,
                recipe,
                exec_out_dir,
                shared_logs,
            )

            mock_execution_context.get_report().set_structured_report.assert_called_once_with(
                '{"test": "report"}'
            )
            mock_execution_context.get_report().set_logs.assert_called_once_with(
                "formatted logs"
            )
            mock_execution_context.get_report().report_info.assert_called_once_with(
                "Successfully executed 'datahub ingest'"
            )
            mock_remove.assert_called_once_with(exec_out_dir)

    def test_structured_report_is_masked_when_secrets_are_registered(
        self, ingestion_task: SubProcessIngestionTask, mock_execution_context: Mock
    ) -> None:
        """The structured report is published to GMS and rendered in the UI, so a
        resolved secret must not survive into it.

        The autouse reset_secret_registry fixture leaves the registry empty, so the
        masking branch is only reachable if a test registers a secret itself -- without
        this test, nothing in this file exercises it.
        """
        secret_value = "s3cr3t-p4ssw0rd"
        SecretRegistry.get_instance().register_secret("DB_PASSWORD", secret_value)
        assert SecretRegistry.get_instance().get_count() > 0

        mock_process = Mock()
        mock_process.returncode = 0
        report_with_secret = json.dumps({"source": {"password": secret_value}})

        with (
            patch("os.path.exists", return_value=True),
            patch("builtins.open", mock_open(read_data=report_with_secret)),
            patch(_REMOVE_DIRECTORY),
            patch(_FORMAT_LOG_LINES),
        ):
            ingestion_task._handle_subprocess_completion(
                mock_process,
                mock_execution_context,
                "/tmp/report.json",
                "/tmp/artifacts",
                {"pipeline_name": "test-pipeline"},
                "/tmp/exec",
                LogHolder(),
            )

        published = mock_execution_context.get_report().set_structured_report.call_args[
            0
        ][0]
        assert secret_value not in published

    def test_handle_subprocess_completion_failure_exit_code(
        self, ingestion_task: SubProcessIngestionTask, mock_execution_context: Mock
    ) -> None:
        mock_process = Mock()
        mock_process.returncode = 1

        with (
            patch("os.path.exists", return_value=False),
            patch(_REMOVE_DIRECTORY),
            patch(_FORMAT_LOG_LINES),
        ):
            with pytest.raises(TaskError, match="Failed to execute 'datahub ingest'"):
                ingestion_task._handle_subprocess_completion(
                    mock_process,
                    mock_execution_context,
                    "/tmp/report.json",
                    "/tmp/artifacts",
                    {},
                    "/tmp/exec",
                    LogHolder(),
                )

            mock_execution_context.get_report().report_info.assert_called_once_with(
                "Failed to execute 'datahub ingest', exit code 1"
            )

    def test_handle_subprocess_completion_killed_by_signal(
        self, ingestion_task: SubProcessIngestionTask, mock_execution_context: Mock
    ) -> None:
        mock_process = Mock()
        mock_process.returncode = -9  # SIGKILL

        with (
            patch("os.path.exists", return_value=False),
            patch(_REMOVE_DIRECTORY),
            patch(_FORMAT_LOG_LINES),
        ):
            with pytest.raises(TaskError, match="Failed to execute 'datahub ingest'"):
                ingestion_task._handle_subprocess_completion(
                    mock_process,
                    mock_execution_context,
                    "/tmp/report.json",
                    "/tmp/artifacts",
                    {},
                    "/tmp/exec",
                    LogHolder(),
                )

            mock_execution_context.get_report().report_error.assert_called_once()
            error_message = mock_execution_context.get_report().report_error.call_args[
                0
            ][0]
            assert "killed by signal SIGKILL" in error_message

    def test_handle_subprocess_completion_oom_exit_code_137(
        self, ingestion_task: SubProcessIngestionTask, mock_execution_context: Mock
    ) -> None:
        mock_process = Mock()
        mock_process.returncode = 137  # OOM killer

        with (
            patch("os.path.exists", return_value=False),
            patch(_REMOVE_DIRECTORY),
            patch(_FORMAT_LOG_LINES),
        ):
            with pytest.raises(TaskError, match="Failed to execute 'datahub ingest'"):
                ingestion_task._handle_subprocess_completion(
                    mock_process,
                    mock_execution_context,
                    "/tmp/report.json",
                    "/tmp/artifacts",
                    {},
                    "/tmp/exec",
                    LogHolder(),
                )

            mock_execution_context.get_report().report_error.assert_called_once()
            error_message = mock_execution_context.get_report().report_error.call_args[
                0
            ][0]
            assert "ran out of memory" in error_message

    def test_handle_subprocess_completion_cancelled_does_not_raise(
        self, ingestion_task: SubProcessIngestionTask, mock_execution_context: Mock
    ) -> None:
        # When a task is cancelled, the subprocess is killed (negative return
        # code), but that must NOT be treated as a failure — raising here would
        # mask the in-flight CancelledError and report the task as FAILED.
        mock_process = Mock()
        mock_process.returncode = -15  # SIGTERM from cancellation teardown

        with (
            patch("os.path.exists", return_value=False),
            patch(_REMOVE_DIRECTORY),
            patch(_FORMAT_LOG_LINES),
        ):
            ingestion_task._handle_subprocess_completion(
                mock_process,
                mock_execution_context,
                "/tmp/report.json",
                "/tmp/artifacts",
                {},
                "/tmp/exec",
                LogHolder(),
                cancelled=True,
            )

            mock_execution_context.get_report().report_error.assert_not_called()

    def test_handle_subprocess_completion_does_not_raise_when_cleanup_steps_fail(
        self, ingestion_task: SubProcessIngestionTask, mock_execution_context: Mock
    ) -> None:
        # Contract test: every cleanup step inside _handle_subprocess_completion
        # is individually guarded, so failures in any of them do not escape.
        # This is what lets _execute_with_debug invoke this function from a
        # `finally` block without a defensive wrapper.
        mock_process = Mock()
        mock_process.returncode = -15  # cancelled — skips the TaskError branch

        with (
            patch("os.path.exists", return_value=True),
            # Report-file read fails.
            patch("builtins.open", side_effect=OSError(errno.EIO, "I/O error")),
            # set_logs fails.
            patch(_FORMAT_LOG_LINES, side_effect=RuntimeError("format boom")),
            # Secret-masking shutdown fails.
            patch(
                "datahub.executor.execution.sub_process_ingestion_task.shutdown_secret_masking",
                side_effect=Exception("masking boom"),
            ),
            # _remove_directory is internally guarded; stub it out.
            patch(_REMOVE_DIRECTORY),
        ):
            ingestion_task._handle_subprocess_completion(
                mock_process,
                mock_execution_context,
                "/tmp/report.json",
                "/tmp/artifacts",
                {},
                "/tmp/exec",
                LogHolder(),
                cancelled=True,
            )


class TestSubProcessIngestionTaskEnvironmentSetupFailures:
    def test_setup_directories_handles_permission_error(
        self, ingestion_task: SubProcessIngestionTask
    ) -> None:
        exec_id = "test-exec-id"

        with patch(
            "pathlib.Path.mkdir", side_effect=PermissionError("Permission denied")
        ):
            with pytest.raises(PermissionError, match="Permission denied"):
                ingestion_task._setup_directories(exec_id)

    def test_prepare_subprocess_environment_has_no_none_values(
        self, ingestion_task: SubProcessIngestionTask, sample_args: dict[str, str]
    ) -> None:
        # asyncio.create_subprocess_exec rejects a None-valued env, so the
        # prepared environment must never contain one.
        validated_args = SubProcessIngestionTaskArgs.model_validate(sample_args)

        env = ingestion_task._prepare_subprocess_environment(
            validated_args, "/tmp/exec", "/tmp/artifacts"
        )

        for key, value in env.items():
            assert value is not None, f"Environment variable {key} should not be None"


class TestSubProcessIngestionTaskEdgeCases:
    def test_handle_subprocess_completion_with_missing_report_file(
        self, ingestion_task: SubProcessIngestionTask, mock_execution_context: Mock
    ) -> None:
        mock_process = Mock()
        mock_process.returncode = 0

        with (
            patch("os.path.exists", return_value=False),
            patch(_REMOVE_DIRECTORY),
            patch(_FORMAT_LOG_LINES),
        ):
            # Should not raise exception when report file is missing
            ingestion_task._handle_subprocess_completion(
                mock_process,
                mock_execution_context,
                "/tmp/nonexistent_report.json",
                "/tmp/artifacts",
                {},
                "/tmp/exec",
                LogHolder(),
            )

            mock_execution_context.get_report().set_structured_report.assert_not_called()

    def test_handle_subprocess_completion_with_corrupted_report_file(
        self, ingestion_task: SubProcessIngestionTask, mock_execution_context: Mock
    ) -> None:
        mock_process = Mock()
        mock_process.returncode = 0

        with (
            patch("os.path.exists", return_value=True),
            patch("builtins.open", mock_open(read_data="invalid json{")),
            patch(_REMOVE_DIRECTORY),
            patch(_FORMAT_LOG_LINES),
        ):
            # Should still complete successfully even with corrupted report
            ingestion_task._handle_subprocess_completion(
                mock_process,
                mock_execution_context,
                "/tmp/report.json",
                "/tmp/artifacts",
                {},
                "/tmp/exec",
                LogHolder(),
            )

            # The report is passed through verbatim; validation is not this
            # function's job.
            mock_execution_context.get_report().set_structured_report.assert_called_once_with(
                "invalid json{"
            )


class TestSubProcessIngestionTaskProgressReporting:
    async def test_progress_reporting_with_basic_functionality(
        self,
        ingestion_task: SubProcessIngestionTask,
        mock_execution_context: Mock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        mock_process = AsyncMock()
        mock_process.returncode = None  # Start as running
        mock_process.stdout = AsyncMock()

        progress_callback = Mock()
        mock_execution_context.request.progress_callback = progress_callback

        call_count = 0

        async def mock_readuntil(delimiter: bytes) -> bytes:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return b"test log line\n"
            else:
                # Signal EOF after first call
                return b""

        async def mock_wait() -> None:
            # Simulate process completion after a short delay
            await asyncio.sleep(0.05)  # Give time for progress reporting
            mock_process.returncode = 0

        mock_process.stdout.readuntil = mock_readuntil
        mock_process.wait = mock_wait

        stdout_lines = LogHolder()
        mock_log_file = Mock()

        # Sub-second heartbeat so the progress loop fires within the test timeout.
        monkeypatch.setattr(ingestion_task.config, "heartbeat_time_seconds", 0.02)

        with (
            patch("sys.stdout"),
            patch(_FORMAT_LOG_LINES) as mock_format,
        ):
            mock_format.return_value = "formatted logs"

            # Add timeout to prevent hanging
            await asyncio.wait_for(
                ingestion_task._monitor_subprocess(
                    mock_process,
                    "test-exec-id",
                    mock_execution_context,
                    stdout_lines,
                    mock_log_file,
                ),
                timeout=5.0,
            )

        assert progress_callback.call_count >= 1
        assert len(stdout_lines.get_logs()) > 0

    async def test_progress_reporting_with_no_callback(
        self,
        ingestion_task: SubProcessIngestionTask,
        mock_execution_context: Mock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that monitoring works correctly when no progress callback is provided."""
        mock_process = AsyncMock()
        mock_process.returncode = None
        mock_process.stdout = AsyncMock()

        mock_execution_context.request.progress_callback = None

        async def mock_readuntil(delimiter: bytes) -> bytes:
            return b""  # Immediate EOF

        async def mock_wait() -> None:
            await asyncio.sleep(0.01)
            mock_process.returncode = 0

        mock_process.stdout.readuntil = mock_readuntil
        mock_process.wait = mock_wait

        stdout_lines = LogHolder()
        mock_log_file = Mock()

        monkeypatch.setattr(ingestion_task.config, "heartbeat_time_seconds", 0.001)

        with patch("sys.stdout"):
            # Should complete without hanging even with no callback
            await asyncio.wait_for(
                ingestion_task._monitor_subprocess(
                    mock_process,
                    "test-exec-id",
                    mock_execution_context,
                    stdout_lines,
                    mock_log_file,
                ),
                timeout=5.0,
            )


class TestSubProcessIngestionTaskClose:
    def test_close_method(self, ingestion_task: SubProcessIngestionTask) -> None:
        # Close method should not raise any exceptions
        ingestion_task.close()


class TestSubProcessIngestionTaskVenvSetup:
    @patch(_SETUP_VENV)
    async def test_setup_venv_default_version(
        self,
        mock_setup_venv: AsyncMock,
        ingestion_task: SubProcessIngestionTask,
        sample_args: dict[str, str],
    ) -> None:
        mock_venv_config = VenvConfig(version="default", main_plugin="demo-data")
        mock_venv_ref = VenvReference(
            venv_loc=Path("/opt/datahub/venvs/demo-data-default"),
            venv_config=mock_venv_config,
        )
        mock_setup_venv.return_value = mock_venv_ref

        args = SubProcessIngestionTaskArgs.model_validate(
            {**sample_args, "version": "default"}
        )

        shared_logs = LogHolder()

        venv_ref = await ingestion_task._setup_venv(
            args, "demo-data", "/tmp/exec", shared_logs
        )

        assert venv_ref == mock_venv_ref
        mock_setup_venv.assert_called_once()

        log_content = shared_logs.get_logs()
        assert (
            "Setting up venv for plugin 'demo-data' with version 'default'"
            in log_content
        )
        assert "Creating dynamic venv" in log_content
        assert "✅ Venv ready at:" in log_content

    @patch(_SETUP_VENV)
    async def test_setup_venv_dynamic_version(
        self,
        mock_setup_venv: AsyncMock,
        ingestion_task: SubProcessIngestionTask,
        sample_args: dict[str, str],
    ) -> None:
        mock_venv_config = VenvConfig(version="0.12.1", main_plugin="demo-data")
        mock_venv_ref = VenvReference(
            venv_loc=Path("/tmp/venv-demo-data-abc123"), venv_config=mock_venv_config
        )
        mock_setup_venv.return_value = mock_venv_ref

        args = SubProcessIngestionTaskArgs.model_validate(
            {**sample_args, "version": "0.12.1"}
        )

        shared_logs = LogHolder()

        venv_ref = await ingestion_task._setup_venv(
            args, "demo-data", "/tmp/exec", shared_logs
        )

        assert venv_ref == mock_venv_ref

        log_content = shared_logs.get_logs()
        assert (
            "Setting up venv for plugin 'demo-data' with version '0.12.1'"
            in log_content
        )
        assert "Creating dynamic venv - this may take a few minutes..." in log_content

    @patch(_SETUP_VENV)
    async def test_setup_venv_with_extra_requirements(
        self,
        mock_setup_venv: AsyncMock,
        ingestion_task: SubProcessIngestionTask,
        sample_args: dict[str, str],
    ) -> None:
        mock_venv_config = VenvConfig(
            version="latest",
            main_plugin="demo-data",
            extra_pip_requirements=["pandas==1.5.0", "numpy>=1.20.0"],
        )
        mock_venv_ref = VenvReference(
            venv_loc=Path("/tmp/venv-demo-data-with-extras"),
            venv_config=mock_venv_config,
        )
        mock_setup_venv.return_value = mock_venv_ref

        args = SubProcessIngestionTaskArgs.model_validate(
            {
                **sample_args,
                "extra_pip_requirements": json.dumps(
                    ["pandas==1.5.0", "numpy>=1.20.0"]
                ),
            }
        )

        shared_logs = LogHolder()

        venv_ref = await ingestion_task._setup_venv(
            args, "demo-data", "/tmp/exec", shared_logs
        )

        mock_setup_venv.assert_called_once()
        call_args = mock_setup_venv.call_args[1]  # kwargs
        venv_config = call_args["venv_config"]
        assert venv_config.extra_pip_requirements == ["pandas==1.5.0", "numpy>=1.20.0"]

        assert venv_ref == mock_venv_ref

    @patch(_SETUP_VENV)
    async def test_setup_venv_failure_handling(
        self,
        mock_setup_venv: AsyncMock,
        ingestion_task: SubProcessIngestionTask,
        sample_args: dict[str, str],
    ) -> None:
        mock_setup_venv.side_effect = Exception("Venv creation failed")

        args = SubProcessIngestionTaskArgs.model_validate(sample_args)
        shared_logs = LogHolder()

        with pytest.raises(TaskError, match="Failed to set up virtual environment"):
            await ingestion_task._setup_venv(
                args, "demo-data", "/tmp/exec", shared_logs
            )

        log_content = shared_logs.get_logs()
        assert "❌ Venv setup failed:" in log_content

    @patch(_SETUP_VENV)
    async def test_setup_venv_with_subprocess_runner_integration(
        self,
        mock_setup_venv: AsyncMock,
        ingestion_task: SubProcessIngestionTask,
        sample_args: dict[str, str],
    ) -> None:
        """Test that _setup_venv properly integrates with SubprocessRunner."""
        mock_venv_config = VenvConfig(version="latest", main_plugin="demo-data")
        mock_venv_ref = VenvReference(
            venv_loc=Path("/tmp/venv-demo-data-abc123"), venv_config=mock_venv_config
        )

        captured_runner = None

        def capture_runner(*args: Any, **kwargs: Any) -> VenvReference:
            nonlocal captured_runner
            captured_runner = kwargs["runner"]
            return mock_venv_ref

        mock_setup_venv.side_effect = capture_runner

        args = SubProcessIngestionTaskArgs.model_validate(sample_args)
        shared_logs = LogHolder()

        await ingestion_task._setup_venv(args, "demo-data", "/tmp/exec", shared_logs)

        assert captured_runner is not None
        assert isinstance(captured_runner, SubprocessRunner)
        assert captured_runner.logs is shared_logs


class TestSubProcessIngestionTaskHybridArchitecture:
    """Tests for the complete hybrid architecture flow."""

    @patch(_SETUP_VENV)
    @patch("asyncio.create_subprocess_exec")
    async def test_end_to_end_venv_and_subprocess_flow(
        self,
        mock_subprocess_exec: AsyncMock,
        mock_setup_venv: AsyncMock,
        ingestion_task: SubProcessIngestionTask,
        sample_args: dict[str, str],
    ) -> None:
        """Test the complete flow from venv setup through subprocess creation."""
        mock_venv_config = VenvConfig(version="latest", main_plugin="demo-data")
        mock_venv_ref = VenvReference(
            venv_loc=Path("/tmp/venv-demo-data-abc123"), venv_config=mock_venv_config
        )
        mock_setup_venv.return_value = mock_venv_ref

        mock_process = AsyncMock()
        mock_process.stdin = Mock()
        mock_subprocess_exec.return_value = mock_process

        validated_args = SubProcessIngestionTaskArgs.model_validate(sample_args)
        shared_logs = LogHolder()

        result = await ingestion_task._create_subprocess(
            validated_args,
            "demo-data",
            {"source": {"type": "demo-data"}},
            "/tmp/report.json",
            {"TEST": "value"},
            "/tmp/exec",
            shared_logs,
            {},
        )

        mock_setup_venv.assert_called_once()

        mock_subprocess_exec.assert_called_once()
        call_args = mock_subprocess_exec.call_args

        # Check that venv path is passed to the wrapper module
        command_args = call_args[0]
        assert str(mock_venv_ref.venv_loc) in command_args

        env = call_args[1]["env"]
        assert env["VENV_PATH"] == str(mock_venv_ref.venv_loc)

        assert result == mock_process

    async def test_log_holder_integration_in_task_context(
        self,
        ingestion_task: SubProcessIngestionTask,
        sample_args: dict[str, str],
        mock_execution_context: Mock,
    ) -> None:
        """Test that LogHolder is properly integrated throughout the task execution."""
        with (
            patch.multiple(
                ingestion_task,
                _setup_directories=Mock(
                    return_value=("/tmp/exec", "/tmp/logs", "/tmp/report.json")
                ),
                _prepare_subprocess_environment=Mock(return_value={}),
                _create_subprocess=AsyncMock(return_value=AsyncMock()),
                _monitor_subprocess=AsyncMock(),
                _handle_subprocess_completion=Mock(),
            ),
            patch(_RESOLVE_RECIPE) as mock_resolve,
            patch(_GET_PLUGIN) as mock_get_plugin,
            patch("builtins.open", mock_open()),
        ):
            mock_resolve.return_value = ({"source": {"type": "demo-data"}}, {})
            mock_get_plugin.return_value = "demo-data"

            await ingestion_task.execute(sample_args, mock_execution_context)

            # The same LogHolder must reach both the subprocess creation and the
            # monitor, otherwise venv-setup logs would be dropped from the run.
            # LogHolder is the second-to-last arg (last is secret_values dict).
            create_subprocess_call = ingestion_task._create_subprocess.call_args  # type: ignore[attr-defined]
            shared_logs_arg = create_subprocess_call[0][-2]
            assert isinstance(shared_logs_arg, LogHolder)

            monitor_subprocess_call = ingestion_task._monitor_subprocess.call_args  # type: ignore[attr-defined]
            monitor_logs_arg = monitor_subprocess_call[0][3]
            assert monitor_logs_arg is shared_logs_arg

    def test_shared_log_holder_echo_functionality(
        self, ingestion_task: SubProcessIngestionTask
    ) -> None:
        log_holder = LogHolder(echo_to_stdout_prefix="[test-id] ")

        log_holder.append("Setting up venv...\n")
        log_holder.append("Venv ready!\n")

        logs = log_holder.get_logs()
        assert "Setting up venv..." in logs
        assert "Venv ready!" in logs

        lines = log_holder.get_lines()
        assert len(lines) == 2
        assert "Setting up venv...\n" in lines
        assert "Venv ready!\n" in lines


class TestSubProcessIngestionTaskErrorScenarios:
    async def test_venv_setup_timeout_scenario(
        self, ingestion_task: SubProcessIngestionTask, sample_args: dict[str, str]
    ) -> None:
        async def slow_venv_setup(*args: Any, **kwargs: Any) -> None:
            await asyncio.sleep(10)  # Simulate very slow setup

        with patch(_SETUP_VENV, side_effect=slow_venv_setup):
            args = SubProcessIngestionTaskArgs.model_validate(sample_args)
            shared_logs = LogHolder()

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(
                    ingestion_task._setup_venv(
                        args, "demo-data", "/tmp/exec", shared_logs
                    ),
                    timeout=0.1,
                )

    @patch(_SETUP_VENV)
    async def test_subprocess_creation_failure_after_successful_venv_setup(
        self,
        mock_setup_venv: AsyncMock,
        ingestion_task: SubProcessIngestionTask,
        sample_args: dict[str, str],
    ) -> None:
        """Test failure in subprocess creation even after successful venv setup."""
        mock_venv_ref = VenvReference(
            venv_loc=Path("/tmp/venv-demo-data-abc123"),
            venv_config=VenvConfig(version="latest", main_plugin="demo-data"),
        )
        mock_setup_venv.return_value = mock_venv_ref

        with patch(
            "asyncio.create_subprocess_exec", side_effect=OSError("Command not found")
        ):
            args = SubProcessIngestionTaskArgs.model_validate(sample_args)
            shared_logs = LogHolder()

            with pytest.raises(OSError, match="Command not found"):
                await ingestion_task._create_subprocess(
                    args,
                    "demo-data",
                    {"source": {"type": "demo-data"}},
                    "/tmp/report.json",
                    {},
                    "/tmp/exec",
                    shared_logs,
                    {},
                )

            # Verify venv was still set up successfully before the subprocess failure
            mock_setup_venv.assert_called_once()
            log_content = shared_logs.get_logs()
            assert "✅ Venv ready at:" in log_content


class TestSubProcessIngestionTaskDebugMode:
    async def test_debug_mode_enabled(
        self, ingestion_task: SubProcessIngestionTask, mock_execution_context: Mock
    ) -> None:
        debug_args = {
            "recipe": json.dumps({"source": {"type": "demo-data"}}),
            "version": "latest",
            "debug_mode": "true",
        }

        with (
            patch.multiple(
                ingestion_task,
                _execute_with_debug=AsyncMock(),
            ),
            patch.object(ingestion_task, "_temporary_log_level") as mock_log_level,
        ):
            mock_log_level.return_value.__enter__ = Mock()
            mock_log_level.return_value.__exit__ = Mock()

            await ingestion_task.execute(debug_args, mock_execution_context)

            mock_log_level.assert_called_once()
            ingestion_task._execute_with_debug.assert_called_once()  # type: ignore[attr-defined]

    async def test_debug_mode_disabled(
        self,
        ingestion_task: SubProcessIngestionTask,
        sample_args: dict[str, str],
        mock_execution_context: Mock,
    ) -> None:
        with patch.multiple(
            ingestion_task,
            _execute_with_debug=AsyncMock(),
        ):
            await ingestion_task.execute(sample_args, mock_execution_context)

            # No log level change on the default path.
            ingestion_task._execute_with_debug.assert_called_once()  # type: ignore[attr-defined]


class TestSubProcessIngestionTaskScaleHandling:
    @patch(_SETUP_VENV)
    @patch("asyncio.create_subprocess_exec")
    async def test_subprocess_creation_uses_buffer_size(
        self,
        mock_subprocess_exec: AsyncMock,
        mock_setup_venv: AsyncMock,
        ingestion_task: SubProcessIngestionTask,
        sample_args: dict[str, str],
    ) -> None:
        mock_venv_ref = VenvReference(
            venv_loc=Path("/tmp/venv"),
            venv_config=VenvConfig(version="latest", main_plugin="demo-data"),
        )
        mock_setup_venv.return_value = mock_venv_ref
        mock_created_process = AsyncMock()
        mock_created_process.stdin = Mock()
        mock_subprocess_exec.return_value = mock_created_process

        args = SubProcessIngestionTaskArgs.model_validate(sample_args)
        shared_logs = LogHolder()

        await ingestion_task._create_subprocess(
            args,
            "demo-data",
            {"source": {"type": "demo-data"}},
            "/tmp/report.json",
            {},
            "/tmp/exec",
            shared_logs,
            {},
        )

        mock_subprocess_exec.assert_called_once()
        call_kwargs = mock_subprocess_exec.call_args[1]
        assert call_kwargs["limit"] == SubProcessTaskUtil.SUBPROCESS_BUFFER_SIZE


class TestMonitorSubprocessCancellation:
    """Drives _monitor_subprocess for real.

    Every other cancellation test patches this method out, so its except block --
    signal the subprocess's process group, cancel the sibling tasks, wait with a
    timeout, kill whatever is still pending, then re-raise or wrap -- was never
    executed by the suite. That is the cleanup path an operator relies on when they
    press cancel; without it a cancelled run can leave the ingestion tree alive.
    """

    @pytest.fixture
    def killpg(self) -> Iterator[Mock]:
        """Patch the process-group signalling and expose the killpg mock.

        os.getpgid is stubbed too: the stand-in process is a Mock, so its pid does
        not belong to a real process group.
        """
        with patch("os.getpgid", return_value=4321), patch("os.killpg") as mock_killpg:
            yield mock_killpg

    def _process(self, *, wait_hangs: bool) -> Mock:
        """A stand-in subprocess whose stdout never yields, so the monitor blocks."""
        proc = Mock(spec=asyncio.subprocess.Process)
        proc.returncode = None
        proc.pid = 4321
        proc.terminate = Mock()
        proc.kill = Mock()

        never = asyncio.Event()  # never set

        async def _readuntil(_sep: bytes = b"\n") -> bytes:
            await never.wait()
            return b""

        stdout = Mock()
        stdout.readuntil = AsyncMock(side_effect=_readuntil)
        proc.stdout = stdout

        async def _wait() -> int:
            if wait_hangs:
                await never.wait()
            return 0

        proc.wait = AsyncMock(side_effect=_wait)
        return proc

    async def test_cancellation_terminates_the_subprocess_and_reraises(
        self,
        ingestion_task: SubProcessIngestionTask,
        mock_execution_context: Mock,
        tmp_path: Path,
        killpg: Mock,
    ) -> None:
        proc = self._process(wait_hangs=False)
        log_file = (tmp_path / "ingestion-logs.log").open("w")

        try:
            monitor = asyncio.ensure_future(
                ingestion_task._monitor_subprocess(
                    proc,
                    "exec-cancel",
                    mock_execution_context,
                    LogHolder(max_log_lines=10),
                    log_file,
                )
            )
            # Let the monitor reach its await points before cancelling.
            await asyncio.sleep(0)
            monitor.cancel()

            with pytest.raises(asyncio.CancelledError):
                await monitor
        finally:
            log_file.close()

        # The whole point: a cancelled run must not leave the tree running. The
        # signal goes to the process GROUP -- terminating only the direct child
        # would abandon the datahub grandchild it spawned.
        assert killpg.call_args_list == [call(4321, signal.SIGTERM)]
        proc.terminate.assert_not_called()

    async def test_a_monitoring_failure_is_wrapped_not_swallowed(
        self,
        ingestion_task: SubProcessIngestionTask,
        mock_execution_context: Mock,
        tmp_path: Path,
        killpg: Mock,
    ) -> None:
        """The non-cancellation branch: a task blowing up must still terminate the
        child, and must surface as an error rather than a silent success."""
        proc = self._process(wait_hangs=False)
        proc.stdout.readuntil = AsyncMock(side_effect=RuntimeError("stdout exploded"))
        log_file = (tmp_path / "ingestion-logs.log").open("w")
        # Without this the test waits out the real cleanup timeout, a full minute.
        ingestion_task.CLEANUP_TIMEOUT_SECONDS = 0.1  # type: ignore[assignment]

        try:
            with pytest.raises(RuntimeError, match="subprocess executor"):
                await ingestion_task._monitor_subprocess(
                    proc,
                    "exec-boom",
                    mock_execution_context,
                    LogHolder(max_log_lines=10),
                    log_file,
                )
        finally:
            log_file.close()

        # SIGTERM first, then SIGKILL once the shortened cleanup timeout expires --
        # both go to the group, so the escalation cannot orphan the grandchild either.
        assert killpg.call_args_list == [
            call(4321, signal.SIGTERM),
            call(4321, signal.SIGKILL),
        ]
        proc.terminate.assert_not_called()
        proc.kill.assert_not_called()
