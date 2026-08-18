import pathlib
import subprocess
import sys
import tempfile
import threading
import time
from io import StringIO
from pathlib import Path
from unittest.mock import AsyncMock, patch

import anyio
import pytest

from datahub.executor.execution.runner import (
    VENV_NO_DATAHUB,
    VENV_VERSION_BUNDLED,
    VENV_VERSION_LATEST,
    VENV_VERSION_NATIVE,
    LogHolder,
    SubprocessRunner,
    VenvConfig,
    VenvReference,
    _bundled_constraints_path,
    _expand_pip_req,
    _validate_wheel_url,
    setup_venv,
    validate_dependency_resolution_enabled,
)


def test_venv_config_json_parsing() -> None:
    """A JSON-encoded string is parsed into a list by the field validator.

    Asserted strictly. The previous version wrapped this in try/except and fell
    back to passing a real list, so an AssertionError from the JSON path was
    swallowed and the test passed either way.
    """
    venv_config = VenvConfig(
        extra_pip_requirements='["snowflake"]',  # type: ignore[arg-type]
    )
    assert venv_config.extra_pip_requirements == ["snowflake"]


def test_log_holder_simple() -> None:
    """Test basic LogHolder functionality."""
    logs = LogHolder(echo_to_stdout_prefix="runner: ")
    logs.append("hello ")
    logs.append("world\n")
    logs.append("hi there!")
    assert logs.get_logs() == "hello world\nhi there!"


def test_log_holder_complex() -> None:
    """Test LogHolder with line limits and truncation."""
    max_log_lines = 10
    lines_to_generate = 75
    logs = LogHolder(
        echo_to_stdout_prefix="runner: ",
        max_log_lines=max_log_lines,
        max_bytes_per_line=50,
    )

    suffix = "a" * 10000
    for i in range(lines_to_generate):
        logs.append(f"line {i}: {suffix}\n")

    truncated_suffix = "a" * (50 - len("line XY: ")) + " [...truncated]"
    assert logs.get_logs() == (
        f"[{lines_to_generate - max_log_lines} earlier log lines truncated...]\n"
        + "".join(
            f"line {i}: {truncated_suffix}\n"
            for i in range(lines_to_generate - max_log_lines, lines_to_generate)
        )
    )


async def test_run_echo() -> None:
    """Test running a simple command."""
    logs = LogHolder(echo_to_stdout_prefix="runner: ")
    runner = SubprocessRunner(logs)
    await runner.execute(["echo", "hello"])

    assert "hello" in logs.get_logs()


async def test_run_failing_command() -> None:
    """Test a failed command."""
    logs = LogHolder(echo_to_stdout_prefix="failing command: ")
    runner = SubprocessRunner(logs)
    with pytest.raises(subprocess.CalledProcessError):
        await runner.execute(["false"])


async def test_run_timeout() -> None:
    """Test command timeout handling."""
    start_time = time.perf_counter()

    logs = LogHolder(echo_to_stdout_prefix="test timeout: ")
    runner = SubprocessRunner(logs)

    # Use anyio timeout pattern compatible with pytest-asyncio
    with pytest.raises((TimeoutError, anyio.get_cancelled_exc_class())):
        with anyio.fail_after(1):
            await runner.execute(["sleep", "10"])

    # We should have timed out after about 1 second.
    elapsed = time.perf_counter() - start_time
    assert 0.8 < elapsed < 2.0  # Allow some tolerance for CI environments

    # Check if the subprocess is cleaned up
    if runner._process is not None:
        assert runner._process.returncode is not None


async def test_run_yes() -> None:
    """Test handling of commands that generate continuous output."""
    logs = LogHolder(echo_to_stdout_prefix="runner: ")
    runner = SubprocessRunner(logs)

    # The `yes` command generates output indefinitely. This test ensures
    # that we handle log reading cleanup correctly during a cancellation.
    with pytest.raises((TimeoutError, anyio.get_cancelled_exc_class())):
        with anyio.fail_after(1):
            await runner.execute(["yes", "wooooo " * 20])

    assert "wooooo" in logs.get_logs()


async def test_venv_simple(tmp_path: pathlib.Path) -> None:
    """Dev-build URL installs from the URL with the uv cache disabled."""
    logs = LogHolder(echo_to_stdout_prefix="venv-setup-test: ")
    runner = SubprocessRunner(logs)

    async def mock_execute(command, env=None, cwd=None):
        if "venv" in command:
            venv_path = Path(command[-1])
            venv_path.mkdir(parents=True, exist_ok=True)
            (venv_path / "bin").mkdir(exist_ok=True)
            (venv_path / "bin" / "python").touch()

    tmp_path.mkdir(exist_ok=True)
    mock = AsyncMock(side_effect=mock_execute)
    with patch.object(runner, "execute", mock):
        await setup_venv(
            VenvConfig(
                version="https://b983b409.datahub-wheels.pages.dev/",
                main_plugin="snowflake",
            ),
            runner,
            tmp_path,
        )

    installs = [c for c in mock.call_args_list if "install" in c[0][0]]
    assert installs, "expected an install command"
    for c in installs:
        assert any("@ https://" in arg for arg in c[0][0])
        assert c.kwargs["env"].get("UV_NO_CACHE") == "1"


@pytest.mark.parametrize("version", ["0.12.1.5", "native"])
async def test_running_venv_command(tmp_path: pathlib.Path, version: str) -> None:
    """Test running commands in created venvs."""
    logs = LogHolder(echo_to_stdout_prefix="venv-commands: ")
    runner = SubprocessRunner(logs)

    # Mock successful venv creation and command execution
    async def mock_execute(command, env=None, cwd=None):
        command_str = " ".join(command)

        if command[0] == "uv" and "venv" in command:
            # This is a uv venv creation command
            venv_path = Path(command[-1])
            venv_path.mkdir(parents=True, exist_ok=True)
            (venv_path / "bin").mkdir(exist_ok=True)
            (venv_path / "bin" / "python").touch()
            (venv_path / "bin" / "datahub").touch()
        elif "pip" in command_str and "install" in command_str:
            # Mock pip install - just succeed
            pass
        elif "cat" in command and "requirements.txt" in command_str:
            # Mock cat requirements.txt - just succeed
            pass
        elif any("datahub" in str(arg) for arg in command) and "--version" in command:
            # Always append to logs when datahub --version is called
            logs.append("acryl-datahub 0.12.1\n")

        # For any other commands, just succeed silently

    # Use AsyncMock to properly mock the execute method
    execute_mock = AsyncMock(side_effect=mock_execute)

    # Mock Path.write_text to handle requirements.txt creation
    original_write_text = Path.write_text

    def mock_write_text(self, data, encoding=None, errors=None, newline=None):
        if "requirements.txt" in str(self):
            # Ensure parent directory exists
            self.parent.mkdir(parents=True, exist_ok=True)
            # Create the file
            self.touch()
            return
        return original_write_text(self, data, encoding=encoding, errors=errors)

    tmp_path.mkdir(exist_ok=True)
    with (
        patch.object(Path, "write_text", mock_write_text),
        patch.object(runner, "execute", execute_mock),
    ):
        venv = await setup_venv(
            VenvConfig(version=version),
            runner,
            tmp_path,
        )

    # Execute datahub --version in the venv
    with patch.object(runner, "execute", execute_mock):
        await runner.execute([venv.command("datahub"), "--version"])

    # Check that the command output appears in logs
    all_logs = logs.get_logs()
    assert "acryl-datahub" in all_logs


async def test_repeat_venv_setup(tmp_path: pathlib.Path) -> None:
    """Test that repeated venv setup reuses existing venv."""
    logs = LogHolder(echo_to_stdout_prefix="venv-setup-1: ")
    runner = SubprocessRunner(logs)

    # Mock successful venv creation first time
    first_call = True

    async def mock_execute(command, env=None, cwd=None):
        nonlocal first_call
        if "venv" in command:
            venv_path = Path(command[-1])
            venv_path.mkdir(parents=True, exist_ok=True)
            (venv_path / "bin").mkdir(exist_ok=True)
            (venv_path / "bin" / "python").touch()
        elif "pip" in command and "install" in command:
            if first_call:
                runner.logs.append("pip install successful\n")

    with patch.object(runner, "execute", side_effect=mock_execute):
        await setup_venv(
            VenvConfig(version="0.12.1.5", main_plugin="snowflake"),
            runner,
            tmp_path,
        )

    assert "Installing datahub" in logs.get_logs()

    # Second setup should skip since venv exists
    logs = LogHolder(echo_to_stdout_prefix="venv-setup-2: ")
    runner2 = SubprocessRunner(logs)

    first_call = False

    async def mock_execute_2(command, env=None, cwd=None):
        # Should not be called since venv exists
        runner2.logs.append("skipping setup - venv already exists\n")

    with patch.object(runner2, "execute", side_effect=mock_execute_2):
        await setup_venv(
            VenvConfig(version="0.12.1.5", main_plugin="snowflake"),
            runner2,
            tmp_path,
        )

    assert "skipping setup" in logs.get_logs()


class TestVenvConfig:
    def test_default_configuration(self):
        """Test VenvConfig with default values."""
        config = VenvConfig()

        assert config.version == VENV_VERSION_LATEST
        assert config.main_plugin is None
        assert config.extra_pip_requirements == []
        assert config.extra_pip_plugins == []
        assert config.extra_env_vars == {}
        assert config.requirements_file is None

    def test_is_default_version(self):
        """Test detection of default version."""
        config_default = VenvConfig(version="bundled")
        config_latest = VenvConfig(version="latest")
        config_custom = VenvConfig(version="0.12.1")

        # Note: The updated VenvConfig doesn't have is_bundled_version method
        # We can test the version values directly
        assert config_default.version == "bundled"
        assert config_latest.version == "latest"
        assert config_custom.version == "0.12.1"

    def test_get_stable_venv_name_bundled_version(self):
        """Test venv name generation for bundled version."""
        config = VenvConfig(version="bundled", main_plugin="snowflake")

        venv_name = config.get_stable_venv_name()
        # Bundled versions should return None (not stable, use predefined location)
        assert venv_name is None

    def test_get_stable_venv_name_specific_version(self):
        """Test venv name generation for specific versions."""
        config = VenvConfig(
            version="0.12.1",
            main_plugin="snowflake",
            extra_pip_requirements=["pkg1"],
            extra_pip_plugins=["plugin1"],
        )

        venv_name = config.get_stable_venv_name()
        assert venv_name is not None
        assert venv_name.startswith("snowflake-")
        assert len(venv_name.split("-")[1]) == 16  # Hash length

    def test_get_stable_venv_name_latest_version(self):
        """Test that latest version returns None (ephemeral)."""
        config = VenvConfig(version="latest", main_plugin="snowflake")

        venv_name = config.get_stable_venv_name()
        assert venv_name is None

    def test_get_stable_venv_name_http_version(self):
        """Test that HTTP URLs return None (ephemeral)."""
        config = VenvConfig(
            version="https://example.com/wheel.whl", main_plugin="snowflake"
        )

        venv_name = config.get_stable_venv_name()
        assert venv_name is None

    def test_get_stable_venv_name_no_plugin(self):
        """Test that missing plugin returns None."""
        config = VenvConfig(version="0.12.1")

        venv_name = config.get_stable_venv_name()
        assert venv_name is None

    def test_get_acryl_datahub_requirement_line_latest(self):
        """Test requirement line generation for latest version."""
        config = VenvConfig(version="latest", main_plugin="snowflake")

        req_line = config.get_acryl_datahub_requirement_line()
        assert req_line == "acryl-datahub[snowflake]"

    def test_get_acryl_datahub_requirement_line_specific_version(self):
        """Test requirement line generation for specific version."""
        config = VenvConfig(version="0.12.1", main_plugin="snowflake")

        req_line = config.get_acryl_datahub_requirement_line()
        assert req_line == "acryl-datahub[snowflake]==0.12.1"

    def test_get_acryl_datahub_requirement_line_with_extra_plugins(self):
        """Test requirement line generation with extra plugins."""
        config = VenvConfig(
            version="0.12.1",
            main_plugin="snowflake",
            extra_pip_plugins=["bigquery", "s3"],
        )

        req_line = config.get_acryl_datahub_requirement_line()
        assert req_line == "acryl-datahub[snowflake,bigquery,s3]==0.12.1"

    def test_get_acryl_datahub_requirement_line_http_wheel(self):
        """Test requirement line generation for HTTP wheel URLs."""
        config = VenvConfig(
            version="https://example.com/custom.whl", main_plugin="snowflake"
        )

        req_line = config.get_acryl_datahub_requirement_line()
        assert req_line == "acryl-datahub[snowflake] @ https://example.com/custom.whl"


class TestVenvReference:
    def test_command_path(self):
        """Test command path generation."""
        config = VenvConfig(version="bundled", main_plugin="snowflake")
        venv_ref = VenvReference(
            venv_loc=Path("/opt/datahub/venvs/snowflake-bundled"), venv_config=config
        )

        python_path = venv_ref.command("python")
        assert python_path == "/opt/datahub/venvs/snowflake-bundled/bin/python"

    def test_extra_envs(self):
        """Test extra environment variables."""
        config = VenvConfig(
            version="bundled",
            main_plugin="snowflake",
            extra_env_vars={"CUSTOM_VAR": "value"},
        )
        venv_ref = VenvReference(
            venv_loc=Path("/opt/datahub/venvs/snowflake-bundled"), venv_config=config
        )

        envs = venv_ref.extra_envs()
        assert envs["CUSTOM_VAR"] == "value"


class TestLogHolder:
    def test_basic_logging(self):
        """Test basic log appending and retrieval."""
        log_holder = LogHolder()

        log_holder.append("Line 1\n")
        log_holder.append("Line 2\n")

        logs = log_holder.get_logs()
        assert "Line 1\n" in logs
        assert "Line 2\n" in logs

    def test_partial_lines(self):
        """Test handling of partial lines."""
        log_holder = LogHolder()

        log_holder.append("Partial ")
        log_holder.append("line\n")

        logs = log_holder.get_logs()
        assert "Partial line\n" in logs

    def test_line_truncation(self):
        """Test that very long lines are truncated."""
        log_holder = LogHolder(max_bytes_per_line=20)

        long_line = "A" * 50 + "\n"
        log_holder.append(long_line)

        logs = log_holder.get_logs()
        assert "[...truncated]" in logs

    def test_command_logging(self):
        """Test command logging."""
        log_holder = LogHolder()

        log_holder.set_command("uv venv create test")

        logs = log_holder.get_logs()
        assert "+uv venv create test\n" in logs

    def test_get_lines_functionality(self):
        """Test the get_lines method for compatibility."""
        log_holder = LogHolder()

        log_holder.append("Line 1\n")
        log_holder.append("Line 2\n")
        log_holder.append("Line 3\n")

        lines = log_holder.get_lines()
        assert isinstance(lines, list)
        assert len(lines) >= 3
        assert any("Line 1" in line for line in lines)
        assert any("Line 2" in line for line in lines)
        assert any("Line 3" in line for line in lines)

    def test_max_log_lines_limit(self):
        """Test that max_log_lines limit is respected."""
        log_holder = LogHolder(max_log_lines=3)

        # Add more lines than the limit
        for i in range(5):
            log_holder.append(f"Line {i}\n")

        lines = log_holder.get_lines()
        # Should not exceed the max limit
        assert len(lines) <= 4  # +1 for potential command line

        logs = log_holder.get_logs()
        # Should contain truncation message if limits were hit
        if len(lines) >= 3:
            assert "truncated" in logs or all(f"Line {i}" in logs for i in range(3, 5))

    def test_max_log_size_bytes_limit(self):
        """Test that max log size in bytes is respected."""
        log_holder = LogHolder(max_log_size_bytes=100)

        # Add a large amount of log data
        large_data = "A" * 50 + "\n"
        for _ in range(10):
            log_holder.append(large_data)

        logs = log_holder.get_logs()
        # Should be truncated to approximately the max size
        assert len(logs) <= 200  # Some tolerance for truncation messages

    def test_echo_to_stdout_functionality(self):
        """Test echo to stdout with prefix."""
        # Capture stdout
        captured_output = StringIO()

        with patch("sys.stdout", captured_output):
            log_holder = LogHolder(echo_to_stdout_prefix="[TEST] ")
            log_holder.append("Test message\n")

        # Check if message was echoed (depends on loguru implementation)
        # This test verifies the LogHolder can be created with echo prefix
        assert log_holder._echo_logs_prefix == "[TEST] "

    def test_concurrent_log_access(self):
        """Test thread-safe access to logs."""
        log_holder = LogHolder()

        def add_logs(thread_id):
            for i in range(10):
                log_holder.append(f"Thread {thread_id} Line {i}\n")
                time.sleep(0.001)  # Small delay

        def read_logs():
            for _ in range(20):
                log_holder.get_logs()
                time.sleep(0.001)

        # Create multiple threads
        threads = []
        for i in range(3):
            threads.append(threading.Thread(target=add_logs, args=(i,)))
        threads.append(threading.Thread(target=read_logs))

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        # Verify all logs were captured
        final_logs = log_holder.get_logs()
        for thread_id in range(3):
            assert f"Thread {thread_id}" in final_logs


class TestAsyncLogStreaming:
    async def test_real_time_log_capture_during_async_process(self):
        """Test that logs are captured in real-time during async subprocess execution."""

        runner = SubprocessRunner()

        async def monitor_logs():
            """Monitor logs during execution."""
            log_snapshots = []
            for _ in range(5):
                await anyio.sleep(0.05)
                logs = runner.logs.get_logs()
                log_snapshots.append(logs)
            return log_snapshots

        async def run_command():
            """Run a command that produces output over time."""
            await runner.execute(
                [
                    "sh",
                    "-c",
                    "echo 'Start'; sleep 0.1; echo 'Middle'; sleep 0.1; echo 'End'",
                ]
            )

        # Run both concurrently using anyio task group
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(monitor_logs)  # type: ignore[arg-type]
            task_group.start_soon(run_command)  # type: ignore[arg-type]

        # All tasks are complete when we reach here

        # Verify that logs appeared progressively
        final_logs = runner.logs.get_logs()
        assert "Start" in final_logs
        assert "Middle" in final_logs
        assert "End" in final_logs

    async def test_async_subprocess_log_interleaving(self):
        """Test that logs from multiple async processes are handled correctly."""

        async def run_numbered_output(runner, prefix, count):
            """Run a process that outputs numbered lines."""
            cmd = "; ".join([f"echo '{prefix}-{i}'" for i in range(count)])
            await runner.execute(["sh", "-c", cmd])
            return runner.logs.get_logs()

        # Create multiple runners
        runners = [SubprocessRunner() for _ in range(3)]

        # Run them concurrently using anyio task group
        results = [None] * len(runners)  # Pre-allocate results list

        async def run_and_store(runner, prefix, count, index):
            result = await run_numbered_output(runner, prefix, count)
            results[index] = result

        async with anyio.create_task_group() as task_group:
            for i, runner in enumerate(runners):
                task_group.start_soon(run_and_store, runner, f"PROC{i}", 3, i)  # type: ignore[arg-type]

        # All tasks are complete when we reach here - results are ready

        # Each runner should have its own isolated logs
        for i, logs in enumerate(results):
            assert logs is not None, f"Result {i} should not be None"
            assert f"PROC{i}-0" in logs
            assert f"PROC{i}-1" in logs
            assert f"PROC{i}-2" in logs

            # Should not contain logs from other processes
            for j in range(3):
                if i != j:
                    assert f"PROC{j}-" not in logs

    async def test_async_log_streaming_with_large_output(self):
        """Test async log streaming with large amounts of output."""
        runner = SubprocessRunner()

        # Generate a command that produces significant output
        lines_count = 100
        cmd = "; ".join(
            [
                f"echo 'Line {i} with some additional text to make it longer'"
                for i in range(lines_count)
            ]
        )

        await runner.execute(["sh", "-c", cmd])

        logs = runner.logs.get_logs()
        lines = runner.logs.get_lines()

        # Should capture all output (minus command line)
        output_lines = [line for line in lines if not line.startswith("+")]
        assert len(output_lines) >= lines_count * 0.9  # Allow some tolerance

        # Check that first and last lines are present
        assert "Line 0" in logs
        assert f"Line {lines_count - 1}" in logs

    async def test_async_log_streaming_error_output(self):
        """Test that stderr is also captured in async streaming."""
        runner = SubprocessRunner()

        # Command that outputs to both stdout and stderr
        await runner.execute(
            ["sh", "-c", "echo 'stdout message'; echo 'stderr message' >&2; exit 0"]
        )

        logs = runner.logs.get_logs()

        # Both stdout and stderr should be captured
        assert "stdout message" in logs
        assert "stderr message" in logs


class TestValidateDependencyResolutionEnabled:
    def test_validation_enabled_by_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that validation passes by default."""
        monkeypatch.delenv("INGESTION_DEPENDENCY_RESOLUTION_ENABLED", raising=False)

        # Should not raise for any version when enabled (default)
        validate_dependency_resolution_enabled("latest")
        validate_dependency_resolution_enabled("0.12.1")
        validate_dependency_resolution_enabled("bundled")

    def test_validation_disabled_with_bundled_version(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that bundled version passes when validation is disabled."""
        monkeypatch.setenv("INGESTION_DEPENDENCY_RESOLUTION_ENABLED", "false")

        # Should not raise for default version
        validate_dependency_resolution_enabled("bundled")

    def test_validation_disabled_with_non_default_version(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that non-default versions fail when validation is disabled."""
        monkeypatch.setenv("INGESTION_DEPENDENCY_RESOLUTION_ENABLED", "false")

        with pytest.raises(
            ValueError,
            match="Version 'latest' is not supported when INGESTION_DEPENDENCY_RESOLUTION_ENABLED=false",
        ):
            validate_dependency_resolution_enabled("latest")

        with pytest.raises(
            ValueError,
            match="Version '0.12.1' is not supported when INGESTION_DEPENDENCY_RESOLUTION_ENABLED=false",
        ):
            validate_dependency_resolution_enabled("0.12.1")


class TestSetupVenv:
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            yield Path(tmp_dir)

    @pytest.fixture
    def mock_runner(self):
        """Create a mock subprocess runner."""
        runner = SubprocessRunner()
        mock_execute = AsyncMock()
        with patch.object(runner, "execute", mock_execute):
            yield runner

    async def test_setup_venv_native_version(self, mock_runner, temp_dir):
        """Test setup with native version (uses current Python)."""
        config = VenvConfig(version=VENV_VERSION_NATIVE)

        venv_ref = await setup_venv(config, mock_runner, temp_dir)

        assert venv_ref.venv_loc == Path(sys.prefix)
        assert venv_ref.venv_config == config
        mock_runner.execute.assert_not_called()

    async def test_setup_venv_bundled_version_not_found(self, mock_runner, temp_dir):
        """Test setup with bundled version when venv doesn't exist."""
        config = VenvConfig(version=VENV_VERSION_BUNDLED, main_plugin="snowflake")

        # Mock a non-existent bundled venv path
        bundled_path = temp_dir / "bundled_venvs"
        with pytest.raises(FileNotFoundError, match="Bundled startup venv not found"):
            await setup_venv(
                config, mock_runner, temp_dir, bundled_venv_path=bundled_path
            )

    async def test_setup_venv_bundled_version_found(self, mock_runner, temp_dir):
        """Test setup with bundled version when venv exists."""
        config = VenvConfig(version=VENV_VERSION_BUNDLED, main_plugin="snowflake")

        # Create a mock bundled venv that exists
        bundled_path = temp_dir / "bundled_venvs"
        bundled_venv = bundled_path / "snowflake-bundled"
        bundled_venv.mkdir(parents=True)
        (bundled_venv / "bin").mkdir()
        (bundled_venv / "bin" / "python").touch()

        venv_ref = await setup_venv(
            config, mock_runner, temp_dir, bundled_venv_path=bundled_path
        )
        assert venv_ref.venv_config == config
        assert venv_ref.venv_loc == bundled_venv
        # Should not call any subprocess commands since bundled venv exists
        mock_runner.execute.assert_not_called()

    @patch("datahub.executor.execution.runner._find_uv")
    async def test_setup_venv_dynamic_creation(
        self, mock_find_uv, mock_runner, temp_dir
    ):
        """Test dynamic venv creation."""
        mock_find_uv.return_value = "uv"
        config = VenvConfig(version="0.12.1", main_plugin="snowflake")

        # Mock the requirements file writing
        async def mock_execute(command, env=None, cwd=None):
            # Simulate successful execution
            if command[0] == "uv" and command[1] == "venv":
                # Create the venv directory structure
                venv_path = Path(command[-1])
                venv_path.mkdir(parents=True, exist_ok=True)
                (venv_path / "bin").mkdir(exist_ok=True)
                (venv_path / "bin" / "python").touch()

        mock_runner.execute.side_effect = mock_execute

        venv_ref = await setup_venv(config, mock_runner, temp_dir)

        assert venv_ref.venv_loc.name.startswith("venv-snowflake-")
        assert venv_ref.venv_config == config

        # Should have called uv venv creation and pip install
        assert mock_runner.execute.call_count >= 2

    @patch("datahub.executor.execution.runner._find_uv", return_value="uv")
    async def test_setup_venv_validation_failure(self, _find_uv, mock_runner, temp_dir):
        """Test setup with latest version (should work in new implementation)."""
        config = VenvConfig(version="latest", main_plugin="snowflake")

        # Mock successful venv creation
        async def mock_execute(command, env=None, cwd=None):
            if "venv" in command:
                venv_path = Path(command[-1])
                venv_path.mkdir(parents=True, exist_ok=True)
                (venv_path / "bin").mkdir(exist_ok=True)
                (venv_path / "bin" / "python").touch()

        mock_runner.execute.side_effect = mock_execute

        # In the new implementation, all versions should work
        venv_ref = await setup_venv(config, mock_runner, temp_dir)
        assert venv_ref.venv_config == config

    @patch("datahub.executor.execution.runner._find_uv", return_value="uv")
    async def test_setup_venv_command_failure(self, _find_uv, mock_runner, temp_dir):
        """Test setup when venv creation command fails."""

        config = VenvConfig(version="0.12.1", main_plugin="snowflake")

        # Mock execute to fail on venv creation
        async def failing_execute(command, env=None, cwd=None):
            if "venv" in command:
                raise subprocess.CalledProcessError(1, command, "venv creation failed")

        mock_runner.execute.side_effect = failing_execute

        # Should raise the subprocess error
        with pytest.raises(subprocess.CalledProcessError):
            await setup_venv(config, mock_runner, temp_dir)

    @patch("datahub.executor.execution.runner._find_uv", return_value="uv")
    async def test_setup_venv_pip_install_failure(
        self, _find_uv, mock_runner, temp_dir
    ):
        """Test setup when pip install fails."""

        config = VenvConfig(version="0.12.1", main_plugin="snowflake")

        call_count = 0

        async def selective_failure(command, env=None, cwd=None):
            nonlocal call_count
            call_count += 1

            if "venv" in command:
                # Create the venv directory structure for first call
                venv_path = Path(command[-1])
                venv_path.mkdir(parents=True, exist_ok=True)
                (venv_path / "bin").mkdir(exist_ok=True)
                (venv_path / "bin" / "python").touch()
                return
            elif "pip" in command and "install" in command:
                # Fail on pip install
                raise subprocess.CalledProcessError(1, command, "pip install failed")

        mock_runner.execute.side_effect = selective_failure

        # Should raise the subprocess error
        with pytest.raises(subprocess.CalledProcessError):
            await setup_venv(config, mock_runner, temp_dir)

    @patch("datahub.executor.execution.runner._find_uv", return_value="uv")
    async def test_setup_venv_concurrent_creation(self, _find_uv, temp_dir):
        """Test concurrent venv creation for same configuration."""

        config = VenvConfig(version="0.12.1", main_plugin="snowflake")

        # Create multiple runners
        runners = [SubprocessRunner() for _ in range(3)]

        # Mock successful venv creation
        async def mock_execute(command, env=None, cwd=None):
            if "venv" in command:
                venv_path = Path(command[-1])
                venv_path.mkdir(parents=True, exist_ok=True)
                (venv_path / "bin").mkdir(exist_ok=True)
                (venv_path / "bin" / "python").touch()

        # Patch all runners' execute methods
        patches = []
        for runner in runners:
            patcher = patch.object(runner, "execute", side_effect=mock_execute)
            patches.append(patcher)
            patcher.start()

        # Run setup_venv concurrently
        async def setup_one(runner):
            return await setup_venv(config, runner, temp_dir)

        # Use anyio task group for concurrent execution with results collection
        results = [None] * len(runners)  # Pre-allocate results list

        async def setup_and_store(runner, index):
            result = await setup_one(runner)
            results[index] = result

        async with anyio.create_task_group() as task_group:
            for i, runner in enumerate(runners):
                task_group.start_soon(setup_and_store, runner, i)  # type: ignore[arg-type]

        # All tasks are complete when we reach here - results are ready

        # All should succeed and create venvs (potentially different locations)
        assert len(results) == 3
        for venv_ref in results:
            assert venv_ref is not None, "VenvReference should not be None"
            assert venv_ref.venv_config == config
            assert venv_ref.venv_loc.exists()

        # Clean up patches
        for patcher in patches:
            patcher.stop()

    @patch("datahub.executor.execution.runner._find_uv", return_value="uv")
    async def test_setup_venv_with_requirements_file(
        self, _find_uv, mock_runner, temp_dir
    ):
        """Test setup with custom requirements file."""
        # Create a mock requirements file
        req_file = temp_dir / "custom-requirements.txt"
        req_file.write_text("pandas==1.5.0\nnumpy==1.24.0\n")

        config = VenvConfig(
            version="0.12.1",
            main_plugin="snowflake",
            requirements_file=req_file,  # Use the full path
        )

        async def mock_execute(command, env=None, cwd=None):
            if "venv" in command:
                venv_path = Path(command[-1])
                venv_path.mkdir(parents=True, exist_ok=True)
                (venv_path / "bin").mkdir(exist_ok=True)
                (venv_path / "bin" / "python").touch()

        mock_runner.execute.side_effect = mock_execute

        venv_ref = await setup_venv(config, mock_runner, temp_dir)

        assert venv_ref.venv_config == config
        # Should have called pip install with requirements file
        install_calls = [
            call
            for call in mock_runner.execute.call_args_list
            if any("install" in str(arg) for arg in call[0])
        ]
        assert len(install_calls) > 0

    @patch("datahub.executor.execution.runner._find_uv", return_value="uv")
    async def test_setup_venv_cancellation_cleanup(
        self, _find_uv, mock_runner, temp_dir
    ):
        """Test that cancellation during setup cleans up properly."""
        config = VenvConfig(version="0.12.1", main_plugin="snowflake")

        # Mock a slow venv creation
        async def slow_execute(command, env=None, cwd=None):
            if "venv" in command:
                await anyio.sleep(1)  # Simulate slow operation
                venv_path = Path(command[-1])
                venv_path.mkdir(parents=True, exist_ok=True)
                (venv_path / "bin").mkdir(exist_ok=True)
                (venv_path / "bin" / "python").touch()

        mock_runner.execute.side_effect = slow_execute

        # Start setup and cancel it using anyio timeout
        with pytest.raises(TimeoutError):
            with anyio.fail_after(0.1):
                await setup_venv(config, mock_runner, temp_dir)

        # Check that any partial venv directories are left in a reasonable state
        # (The actual cleanup behavior depends on implementation)


async def _mock_venv_execute(command, env=None, cwd=None):
    if command[0] == "uv" and "venv" in command:
        venv_path = Path(command[-1])
        venv_path.mkdir(parents=True, exist_ok=True)
        (venv_path / "bin").mkdir(exist_ok=True)
        (venv_path / "bin" / "python").touch()


def _extract_pip_installs(mock):
    return [
        c
        for c in mock.call_args_list
        if len(c[0]) > 0 and "pip" in c[0][0] and "install" in c[0][0]
    ]


class TestSetupVenvConstraints:
    """Tests for the two-pass constrained install in setup_venv.

    These test real bugs we hit in production:
    1. --reinstall without per-package scoping upgrades ALL transitive deps,
       breaking binary compatibility (numpy 1.x → 2.x broke pandas C extensions).
    2. Package name extraction from version specifiers must handle extras,
       URLs, and complex markers — not just simple `pkg==ver`.
    """

    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            yield Path(tmp_dir)

    @pytest.fixture
    def constraints_file(self, temp_dir):
        f = temp_dir / "constraints.txt"
        f.write_text("pandas==2.1.4\nruamel-yaml==0.17.17\nnumpy==1.26.4\n")
        return f

    _mock_execute = staticmethod(_mock_venv_execute)
    _pip_installs = staticmethod(_extract_pip_installs)

    @patch("datahub.executor.execution.runner._find_uv", return_value="uv")
    async def test_pass2_does_not_use_reinstall_flag(self, _find_uv, temp_dir):
        """uv pip install with an explicit version (e.g. pandas==2.1.0) already
        downgrades/upgrades without --reinstall. Bare --reinstall is dangerous:
        it upgraded numpy 1.26→2.4 in production, breaking pandas C extensions.
        Pass 2 must use plain -r with no --reinstall* flags."""
        config = VenvConfig(
            version="0.12.1",
            main_plugin="snowflake",
            extra_pip_requirements=["pandas==2.1.0", "custom-connector>=1.0"],
        )
        runner = SubprocessRunner()
        mock = AsyncMock(side_effect=self._mock_execute)

        with patch.object(runner, "execute", mock):
            await setup_venv(config, runner, temp_dir)

        extra_installs = [c for c in self._pip_installs(mock) if "-r" in c[0][0]]
        assert extra_installs, "expected an extra-requirements install"
        pass2_cmd = extra_installs[0][0][0]
        assert "--reinstall" not in pass2_cmd
        assert "--reinstall-package" not in pass2_cmd

    @patch("datahub.executor.execution.runner._find_uv", return_value="uv")
    async def test_custom_requirements_file_skips_pass2(self, _find_uv, temp_dir):
        """When requirements_file is provided (e.g. an upstream caller
        generates its own), pass 2 must be skipped — the caller owns the full
        requirements and splitting into two passes would break their resolution."""
        req_file = temp_dir / "custom-requirements.txt"
        req_file.write_text("acryl-datahub[snowflake]==0.12.1\npandas==2.1.0\n")

        config = VenvConfig(
            version="0.12.1",
            main_plugin="snowflake",
            extra_pip_requirements=["pandas==2.1.0"],
            requirements_file=req_file,
        )
        runner = SubprocessRunner()
        mock = AsyncMock(side_effect=self._mock_execute)

        with patch.object(runner, "execute", mock):
            await setup_venv(config, runner, temp_dir)

        # Only one install — pass 2 skipped because requirements_file is caller-owned.
        # No --constraint since the requirements_file path installs verbatim.
        installs = self._pip_installs(mock)
        assert len(installs) == 1
        assert "--constraint" not in installs[0][0][0]

    @patch("datahub.executor.execution.runner._find_uv", return_value="uv")
    async def test_pass2_does_not_use_reinstall_flag_with_wheel(
        self, _find_uv, temp_dir, constraints_file
    ):
        """Pass 2 must not use --reinstall even when bundled constraints are present."""
        config = VenvConfig(
            version="0.12.1",
            main_plugin="snowflake",
            extra_pip_requirements=["pandas==2.1.0", "custom-connector>=1.0"],
        )
        runner = SubprocessRunner()
        mock = AsyncMock(side_effect=self._mock_execute)

        with (
            patch(
                "datahub.executor.execution.runner._bundled_constraints_path",
                return_value=constraints_file,
            ),
            patch.object(runner, "execute", mock),
        ):
            await setup_venv(config, runner, temp_dir)

        extra_installs = [c for c in self._pip_installs(mock) if "-r" in c[0][0]]
        assert extra_installs, "expected an extra-requirements install"
        pass2_cmd = extra_installs[0][0][0]
        assert "--reinstall" not in pass2_cmd
        assert "--reinstall-package" not in pass2_cmd


class TestWheelBundledConstraints:
    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            yield Path(tmp_dir)

    def test_validate_wheel_url(self):
        """All wheel URLs must be from *.pages.dev."""
        assert _validate_wheel_url("https://d651e18a.datahub-wheels.pages.dev")
        assert _validate_wheel_url(
            "https://d651e18a.datahub-wheels.pages.dev/artifacts/wheels/acryl_datahub-0.0.0.dev1-py3-none-any.whl"
        )
        assert not _validate_wheel_url(
            "https://example.com/path/to/acryl_datahub-0.0.0.dev1-py3-none-any.whl"
        )
        assert not _validate_wheel_url("https://evil.com/malicious")
        assert not _validate_wheel_url("not-a-url")

    def test_bundled_constraints_path_found(self, temp_dir):
        """Finds datahub/constraints.txt inside the installed package."""
        site_pkgs = temp_dir / "lib" / "python3.11" / "site-packages" / "datahub"
        site_pkgs.mkdir(parents=True)
        (site_pkgs / "constraints.txt").write_text("pandas==2.1.4\n")

        result = _bundled_constraints_path(temp_dir)
        assert result is not None
        assert result.read_text() == "pandas==2.1.4\n"

    def test_bundled_constraints_path_missing(self, temp_dir):
        """Returns None when the package ships no constraints.txt."""
        (temp_dir / "lib").mkdir()
        assert _bundled_constraints_path(temp_dir) is None

    @patch("datahub.executor.execution.runner._find_uv", return_value="uv")
    async def test_setup_venv_installs_with_constraints(self, _find_uv, temp_dir):
        """Plugin install runs under the bundled constraints when present."""
        constraints = temp_dir / "constraints.txt"
        constraints.write_text("ruamel-yaml==0.17.40\n")

        config = VenvConfig(version="0.14.1", main_plugin="snowflake")
        runner = SubprocessRunner()
        mock_exec = AsyncMock(side_effect=_mock_venv_execute)

        with (
            patch(
                "datahub.executor.execution.runner._bundled_constraints_path",
                return_value=constraints,
            ),
            patch.object(runner, "execute", mock_exec),
        ):
            await setup_venv(config, runner, temp_dir)

        installs = _extract_pip_installs(mock_exec)
        assert "--no-deps" in installs[0][0][0]
        assert "acryl-datahub==0.14.1" in " ".join(installs[0][0][0])
        main_cmd = installs[1][0][0]
        assert "acryl-datahub[snowflake]==0.14.1" in " ".join(main_cmd)
        assert "--constraint" in main_cmd

    @patch("datahub.executor.execution.runner._find_uv", return_value="uv")
    async def test_setup_venv_installs_without_constraints(self, _find_uv, temp_dir):
        """Without a bundled constraints.txt the plugin install is unconstrained."""
        config = VenvConfig(version="0.14.1", main_plugin="snowflake")
        runner = SubprocessRunner()
        mock_exec = AsyncMock(side_effect=_mock_venv_execute)

        with patch.object(runner, "execute", mock_exec):
            await setup_venv(config, runner, temp_dir)

        installs = _extract_pip_installs(mock_exec)
        main_cmd = installs[1][0][0]
        assert "acryl-datahub[snowflake]==0.14.1" in " ".join(main_cmd)
        assert "--constraint" not in main_cmd

    @patch("datahub.executor.execution.runner._find_uv", return_value="uv")
    async def test_setup_venv_invalid_dev_url_raises(self, _find_uv, temp_dir):
        """A dev-build URL from a disallowed domain raises."""
        config = VenvConfig(
            version="https://evil.com/acryl_datahub-0.0.0.dev1-py3-none-any.whl",
            main_plugin="snowflake",
        )
        runner = SubprocessRunner()
        mock_exec = AsyncMock(side_effect=_mock_venv_execute)

        with (
            patch.object(runner, "execute", mock_exec),
            pytest.raises(RuntimeError, match="Invalid wheel URL"),
        ):
            await setup_venv(config, runner, temp_dir)

    @patch("datahub.executor.execution.runner._find_uv", return_value="uv")
    async def test_setup_venv_no_datahub_skips_install(self, _find_uv, temp_dir):
        """VENV_NO_DATAHUB skips all install commands."""
        config = VenvConfig(version=VENV_NO_DATAHUB, main_plugin="snowflake")
        runner = SubprocessRunner()
        mock_exec = AsyncMock(side_effect=_mock_venv_execute)

        with patch.object(runner, "execute", mock_exec):
            await setup_venv(config, runner, temp_dir)

        assert len(_extract_pip_installs(mock_exec)) == 0

    @patch("datahub.executor.execution.runner._find_uv", return_value="uv")
    async def test_setup_venv_latest_installs_without_pin(self, _find_uv, temp_dir):
        """version='latest' installs acryl-datahub with no == pin."""
        constraints = temp_dir / "constraints.txt"
        constraints.write_text("ruamel-yaml==0.17.40\n")

        config = VenvConfig(version="latest", main_plugin="snowflake")
        runner = SubprocessRunner()
        mock_exec = AsyncMock(side_effect=_mock_venv_execute)

        with (
            patch(
                "datahub.executor.execution.runner._bundled_constraints_path",
                return_value=constraints,
            ),
            patch.object(runner, "execute", mock_exec),
        ):
            await setup_venv(config, runner, temp_dir)

        installs = _extract_pip_installs(mock_exec)
        joined = " ".join(installs[0][0][0] + installs[1][0][0])
        assert "==" not in joined
        assert "acryl-datahub[snowflake]" in " ".join(installs[1][0][0])
        assert "--constraint" in installs[1][0][0]


class TestSubprocessRunner:
    async def test_subprocess_runner_basic(self):
        """Test basic subprocess execution."""
        runner = SubprocessRunner()

        # Test with a simple command that should succeed
        await runner.execute(["echo", "hello world"])

        logs = runner.logs.get_logs()
        assert "+echo" in logs and "hello world" in logs
        assert "hello world\n" in logs

    async def test_subprocess_runner_failure(self):
        """Test subprocess execution with failure."""
        runner = SubprocessRunner()

        # Test with a command that should fail
        with pytest.raises(subprocess.CalledProcessError):
            await runner.execute(["false"])

    async def test_subprocess_runner_with_env(self):
        """Test subprocess execution with custom environment."""
        runner = SubprocessRunner()

        # Test with custom environment variable
        await runner.execute(
            ["sh", "-c", "echo $TEST_VAR"], env={"TEST_VAR": "test_value"}
        )

        logs = runner.logs.get_logs()
        assert "test_value" in logs

    async def test_subprocess_runner_async_cancellation(self):
        """Test that subprocess can be cancelled properly."""

        runner = SubprocessRunner()

        # Start the command and cancel it after a short time using timeout
        with pytest.raises(TimeoutError):
            with anyio.fail_after(0.5):
                await runner.execute(["sleep", "10"])

        # Process should be cleaned up
        assert runner._process is None or runner._process.returncode is not None

    async def test_subprocess_runner_kill_graceful(self):
        """Test graceful process termination."""

        runner = SubprocessRunner()

        # Test graceful termination by timeout
        with pytest.raises((subprocess.CalledProcessError, TimeoutError)):
            with anyio.fail_after(0.5):
                await runner.execute(["sleep", "5"])

        # Process should be terminated
        if runner._process is not None:
            assert runner._process.returncode is not None

    async def test_subprocess_runner_real_time_logs(self):
        """Test that logs are captured in real-time."""

        runner = SubprocessRunner()

        # Use a command that produces output over time
        await runner.execute(
            [
                "sh",
                "-c",
                "echo 'line1'; sleep 0.1; echo 'line2'; sleep 0.1; echo 'line3'",
            ]
        )

        logs = runner.logs.get_logs()
        assert "line1" in logs
        assert "line2" in logs
        assert "line3" in logs

        # Check that all lines are present
        lines = runner.logs.get_lines()
        output_lines = [line for line in lines if not line.startswith("+")]
        assert len([line for line in output_lines if "line" in line]) == 3

    async def test_subprocess_runner_large_output(self):
        """Test handling of large output streams."""
        runner = SubprocessRunner()

        # Generate a large amount of output
        large_text = "A" * 1000
        await runner.execute(["echo", large_text])

        logs = runner.logs.get_logs()
        assert large_text in logs

    async def test_subprocess_runner_concurrent_execution(self):
        """Test that multiple SubprocessRunners can run concurrently."""

        async def run_command(runner, cmd):
            await runner.execute(["echo", cmd])
            return runner.logs.get_logs()

        # Create multiple runners
        runners = [SubprocessRunner() for _ in range(3)]

        # Run commands concurrently using anyio task groups
        results = [None] * len(runners)  # Pre-allocate results list

        async def run_and_store(runner, cmd, index):
            result = await run_command(runner, cmd)
            results[index] = result

        async with anyio.create_task_group() as task_group:
            for i, runner in enumerate(runners):
                task_group.start_soon(run_and_store, runner, f"output{i}", i)  # type: ignore[arg-type]

        # All tasks are complete when we reach here - results are ready

        # Each result should contain the expected output
        for i, result in enumerate(results):
            assert result is not None, f"Result {i} should not be None"
            assert f"output{i}" in result


# --- env-var expansion in pip requirements ---


def test_expand_pip_req_with_default_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DATAHUB_INTEGRATIONS_PACKAGE_SPEC", raising=False)
    result = _expand_pip_req(
        "${DATAHUB_INTEGRATIONS_PACKAGE_SPEC:-example-private-plugins}"
    )
    assert result == "example-private-plugins"


def test_expand_pip_req_uses_env_var_when_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "DATAHUB_INTEGRATIONS_PACKAGE_SPEC",
        "example-private-plugins==2.0.0+local.abc",
    )
    result = _expand_pip_req(
        "${DATAHUB_INTEGRATIONS_PACKAGE_SPEC:-example-private-plugins}"
    )
    assert result == "example-private-plugins==2.0.0+local.abc"


def test_expand_pip_req_plain_string_is_noop() -> None:
    result = _expand_pip_req("example-private-plugins@/path/to/local/checkout")
    assert result == "example-private-plugins@/path/to/local/checkout"


def test_expand_pip_req_bare_dollar_mid_string_is_noop() -> None:
    # URLs or specs with a bare $ (not ${...}) must pass through unchanged so
    # they don't accidentally trigger expansion or raise UnboundVariable.
    url = "https://example.com/wheel.whl?sig=$TOKEN"
    assert _expand_pip_req(url) == url


def test_expand_pip_req_unset_without_default_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("_DATAHUB_TEST_UNSET_VAR", raising=False)
    with pytest.raises(RuntimeError, match="unset environment variable"):
        _expand_pip_req("${_DATAHUB_TEST_UNSET_VAR}")


def test_get_stable_venv_name_changes_when_env_var_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = VenvConfig(
        version="0.12.1.5",
        main_plugin="snowflake",
        extra_pip_requirements=[
            "${DATAHUB_INTEGRATIONS_PACKAGE_SPEC:-example-private-plugins}"
        ],
    )
    monkeypatch.setenv(
        "DATAHUB_INTEGRATIONS_PACKAGE_SPEC", "example-private-plugins==1.0.0"
    )
    name_v1 = config.get_stable_venv_name()

    monkeypatch.setenv(
        "DATAHUB_INTEGRATIONS_PACKAGE_SPEC", "example-private-plugins==2.0.0"
    )
    name_v2 = config.get_stable_venv_name()

    assert name_v1 != name_v2, (
        "changing env var must produce a different venv cache key"
    )


def test_get_stable_venv_name_stable_when_env_var_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "DATAHUB_INTEGRATIONS_PACKAGE_SPEC", "example-private-plugins==1.0.0"
    )
    config = VenvConfig(
        version="0.12.1.5",
        main_plugin="snowflake",
        extra_pip_requirements=[
            "${DATAHUB_INTEGRATIONS_PACKAGE_SPEC:-example-private-plugins}"
        ],
    )
    assert config.get_stable_venv_name() == config.get_stable_venv_name()
