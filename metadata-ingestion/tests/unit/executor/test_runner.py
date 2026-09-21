import logging
import os
import pathlib
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, List, Optional, Tuple
from unittest.mock import AsyncMock, patch

import anyio
import pytest

from datahub.executor.execution import venv_utils
from datahub.executor.execution.runner import (
    VENV_NO_DATAHUB,
    VENV_VERSION_BUNDLED,
    VENV_VERSION_LATEST,
    VENV_VERSION_NATIVE,
    LogHolder,
    SubprocessRunner,
    VenvConfig,
    VenvReference,
    _acquire_cache_entry,
    _bundled_constraints_path,
    _expand_pip_req,
    _validate_wheel_url,
    setup_venv,
    validate_dependency_resolution_enabled,
)
from datahub.executor.execution.task import TaskError
from datahub.executor.execution.venv_cache import EntryLock
from datahub.masking.secret_registry import SecretRegistry


@pytest.fixture(autouse=True)
def _isolate_venv_cache_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory
) -> None:
    """Give every test in this module its own venv-cache root.

    Without this, any VenvConfig with a pinned version (or `latest`) plus a
    main_plugin is cacheable by default, and get_venv_cache_path()'s default
    is `tmp_dir.parent / "_venv_cache"`. For fixtures built on
    tempfile.TemporaryDirectory() or pytest's shared tmp_path base, that
    parent is a directory many tests -- or even unrelated pytest runs on this
    machine -- share. Tests would silently reuse each other's cached venvs,
    or block forever on a lock another test's leaked file descriptor still
    holds. Tests that exercise the cache path explicitly override this with
    their own monkeypatch.setenv, which simply wins because it runs later.
    """
    monkeypatch.setenv(
        "DATAHUB_VENV_CACHE_PATH", str(tmp_path_factory.mktemp("venv_cache"))
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
    """Test that repeated venv setup reuses existing venv.

    A pinned version + main_plugin is cacheable, so this now goes through the
    node-local cache rather than the plain tmp_dir existence check. The first
    call's exclusive lock must be released before the second call reuses the
    same entry, or the second call's acquire would
    hang forever waiting on a lock this test's own first call still holds.
    """
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
        ref = await setup_venv(
            VenvConfig(version="0.12.1.5", main_plugin="snowflake"),
            runner,
            tmp_path,
        )

    assert "Installing datahub" in logs.get_logs()
    assert ref.lock is not None
    ref.lock.release()

    # Second setup should skip since the cached venv is already complete.
    logs = LogHolder(echo_to_stdout_prefix="venv-setup-2: ")
    runner2 = SubprocessRunner(logs)

    first_call = False

    async def mock_execute_2(command, env=None, cwd=None):
        # Should not be called since the venv is reused from the cache.
        runner2.logs.append("skipping setup - venv already exists\n")

    with patch.object(runner2, "execute", side_effect=mock_execute_2):
        ref2 = await setup_venv(
            VenvConfig(version="0.12.1.5", main_plugin="snowflake"),
            runner2,
            tmp_path,
        )

    assert ref2.venv_loc == ref.venv_loc
    assert "Reusing cached venv" in logs.get_logs()
    assert ref2.lock is not None
    ref2.lock.release()


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


class _SetupVenvThread(threading.Thread):
    """setup_venv on its own OS thread and event loop, as production runs it.

    DefaultExecutor.execute_task documents "each time execute_task is called,
    it is called from a different thread", each with its own event loop. A
    real thread is also the only thing that can observe a lock hang from
    OUTSIDE: a blocking flock() freezes the very event loop an in-loop timeout
    (anyio.fail_after) would have to fire on.
    """

    def __init__(self, config: VenvConfig, tmp_dir: Path, execute: Any) -> None:
        super().__init__()
        self._config = config
        self._tmp_dir = tmp_dir
        self._execute = execute
        self.result: Optional[VenvReference] = None
        self.error: Optional[BaseException] = None

    def run(self) -> None:
        runner = SubprocessRunner()

        async def _go() -> VenvReference:
            with patch.object(runner, "execute", side_effect=self._execute):
                return await setup_venv(self._config, runner, self._tmp_dir)

        try:
            self.result = anyio.run(_go)
        except BaseException as exc:  # surfaced via `error`, not swallowed
            self.error = exc

    def finish(self, hang_message: str, timeout: float = 30.0) -> VenvReference:
        self.join(timeout=timeout)
        assert not self.is_alive(), hang_message
        if self.error is not None:
            raise self.error
        assert self.result is not None
        return self.result


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

    def test_echo_goes_through_stdlib_logging(self, caplog):
        """Echoed lines are stdlib log records, so masking filters on logging
        handlers apply to them."""
        with caplog.at_level(logging.DEBUG, logger="datahub.executor.execution.runner"):
            log_holder = LogHolder(echo_to_stdout_prefix="[TEST] ")
            log_holder.append("Test message\n")

        assert "[TEST] Test message" in caplog.text

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
        """Concurrent setups of one entry build it once and never block.

        Two things are asserted, and each catches a distinct regression:

        * Exactly one build across the three threads. A loser must find the
          winner's finished entry, not build its own. Deterministic even
          though the threads genuinely race: at most one can hold the entry
          EXCLUSIVE, and the mocked build finishes far inside the retry
          budget, so every loser's next pass is a shared hit.
        * A fourth thread arriving while those three shared holds are
          outstanding still returns promptly. This is the C1 regression: an
          unconditional blocking exclusive acquire makes it wait for holders
          that, in production, are ingestion runs lasting hours.
        """
        config = VenvConfig(version="0.12.1", main_plugin="snowflake")
        installs = [0] * 4

        def make_execute(index: int) -> Any:
            async def execute(
                command: List[str],
                env: Optional[dict] = None,
                cwd: Optional[str] = None,
            ) -> None:
                if "venv" in command:
                    venv_path = Path(command[-1])
                    venv_path.mkdir(parents=True, exist_ok=True)
                    (venv_path / "bin").mkdir(exist_ok=True)
                    (venv_path / "bin" / "python").touch()
                elif "install" in command:
                    installs[index] += 1

            return execute

        threads = [
            _SetupVenvThread(config, temp_dir, make_execute(i)) for i in range(3)
        ]
        refs: List[VenvReference] = []
        try:
            for thread in threads:
                thread.start()
            for thread in threads:
                refs.append(
                    thread.finish("a thread never finished -- setup_venv blocked")
                )

            for ref in refs:
                assert ref.venv_config == config
                assert ref.venv_loc.exists()
            assert len({ref.venv_loc for ref in refs}) == 1, (
                "concurrent setups of one config must converge on one entry"
            )
            assert sum(1 for count in installs[:3] if count) == 1, (
                f"exactly one thread should have built the entry: {installs}"
            )

            # Every thread above is still holding its shared lock, exactly as
            # a running ingestion would for its whole life.
            late = _SetupVenvThread(config, temp_dir, make_execute(3))
            late.start()
            refs.append(
                late.finish(
                    "a complete entry held SHARED by running tasks blocked a "
                    "new setup_venv -- the acquire must be non-blocking"
                )
            )

            assert refs[-1].venv_loc == refs[0].venv_loc
            assert installs[3] == 0, "a complete entry was rebuilt"
        finally:
            for ref in refs:
                if ref.lock is not None:
                    ref.lock.release()

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
    async def test_setup_venv_registers_referenced_env_values(
        self, _find_uv, temp_dir, monkeypatch
    ):
        """Env values referenced in pip requirements must be maskable before
        any expanded content reaches a log line."""
        SecretRegistry.reset_instance()
        monkeypatch.setenv("VENV_PIP_TOKEN", "venv-pip-token-value")
        config = VenvConfig(
            version="0.12.1",
            main_plugin="snowflake",
            extra_pip_requirements=[
                "pkg @ https://user:${VENV_PIP_TOKEN}@example.com/simple"
            ],
        )
        runner = SubprocessRunner()
        mock = AsyncMock(side_effect=self._mock_execute)

        try:
            with patch.object(runner, "execute", mock):
                await setup_venv(config, runner, temp_dir)

            assert (
                SecretRegistry.get_instance().get_secret_value("VENV_PIP_TOKEN")
                == "venv-pip-token-value"
            )
        finally:
            SecretRegistry.reset_instance()

    @patch("datahub.executor.execution.runner._find_uv", return_value="uv")
    async def test_setup_venv_masks_the_value_the_venv_actually_uses(
        self, _find_uv, temp_dir, monkeypatch
    ):
        """extra_env_vars wins when building the venv, so it must be masked too.

        Registration read os.environ while the venv was built with
        {**os.environ, **extra_env_vars}, so an overridden name had its
        AMBIENT value masked and its EFFECTIVE value -- the one pip actually
        received, and the one a failing index URL echoes back -- masked
        nowhere. The envelope cannot cover it either: subprocess_env_secrets
        excludes overridden names on purpose, to keep get_combined_env_vars
        precedence in the child.
        """
        SecretRegistry.reset_instance()
        monkeypatch.setenv("VENV_PIP_TOKEN", "ambient-token-value")
        config = VenvConfig(
            version="0.12.1",
            main_plugin="snowflake",
            extra_pip_requirements=[
                "pkg @ https://user:${VENV_PIP_TOKEN}@example.com/simple"
            ],
            extra_env_vars={"VENV_PIP_TOKEN": "override-token-value"},
        )
        runner = SubprocessRunner()
        mock = AsyncMock(side_effect=self._mock_execute)

        try:
            with patch.object(runner, "execute", mock):
                await setup_venv(config, runner, temp_dir)

            # get_all_secrets maps each maskable RENDERING to its name, and
            # the registry keeps MAX_SECRET_VERSIONS per name, so both the
            # ambient and the effective value stay maskable at once.
            maskable = SecretRegistry.get_instance().get_all_secrets()
            assert "override-token-value" in maskable, sorted(maskable)
            assert "ambient-token-value" in maskable, sorted(maskable)
        finally:
            SecretRegistry.reset_instance()

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


class TestVenvCacheInSetupVenv:
    """The reuse branch in setup_venv has never fired.

    It was written for this and could not work: setup_task_venv passes
    tmp_dir=exec_out_dir, which finalize_task_output removes in its finally, so
    the check always looked in a directory that was new and about to be
    deleted.
    """

    @staticmethod
    def _mock_execute():
        async def execute(command, env=None, cwd=None):
            if "venv" in command:
                venv_path = Path(command[-1])
                venv_path.mkdir(parents=True, exist_ok=True)
                (venv_path / "bin").mkdir(exist_ok=True)
                (venv_path / "bin" / "python").touch()
                (venv_path / "bin" / "datahub").touch()

        return AsyncMock(side_effect=execute)

    async def _setup(
        self, tmp_path: pathlib.Path, version: str, mock: AsyncMock
    ) -> VenvReference:
        runner = SubprocessRunner(LogHolder())
        with patch.object(runner, "execute", mock):
            return await setup_venv(
                VenvConfig(version=version, main_plugin="snowflake"),
                runner,
                tmp_path,
            )

    async def _setup_with_env_vars(
        self,
        tmp_path: pathlib.Path,
        version: str,
        mock: AsyncMock,
        extra_env_vars: dict,
    ) -> VenvReference:
        runner = SubprocessRunner(LogHolder())
        with patch.object(runner, "execute", mock):
            return await setup_venv(
                VenvConfig(
                    version=version,
                    main_plugin="snowflake",
                    extra_env_vars=extra_env_vars,
                ),
                runner,
                tmp_path,
            )

    async def test_the_second_setup_of_the_same_venv_installs_nothing(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", str(tmp_path / "cache"))

        first = self._mock_execute()
        ref = await self._setup(tmp_path / "exec-1", "0.15.0.1", first)
        assert ref.lock is not None
        ref.lock.release()
        assert [c for c in first.call_args_list if "install" in c[0][0]]

        second = self._mock_execute()
        ref2 = await self._setup(tmp_path / "exec-2", "0.15.0.1", second)
        assert ref2.lock is not None
        ref2.lock.release()

        assert not [c for c in second.call_args_list if "install" in c[0][0]], (
            "a cached venv was rebuilt"
        )
        assert not [c for c in second.call_args_list if "venv" in c[0][0]], (
            "a complete entry was re-created rather than reused"
        )
        assert ref2.venv_loc == ref.venv_loc

    async def test_latest_is_cached_too(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`latest` is the default for every recipe. get_stable_venv_name()
        returns None for it because it is a moving target -- true for a cache
        that outlives the pod, and this one cannot."""
        monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", str(tmp_path / "cache"))

        first = self._mock_execute()
        ref = await self._setup(tmp_path / "exec-1", "latest", first)
        assert ref.lock is not None
        ref.lock.release()

        second = self._mock_execute()
        ref2 = await self._setup(tmp_path / "exec-2", "latest", second)
        assert ref2.lock is not None
        ref2.lock.release()

        assert not [c for c in second.call_args_list if "install" in c[0][0]]

    async def test_different_requirements_get_different_entries(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The cache key must be the venv's CONTENTS, not the connector.

        get_stable_venv_name() already hashes the EXPANDED requirements, so
        changing DATAHUB_INTEGRATIONS_PACKAGE_SPEC or any other env template
        forces a new entry rather than silently serving one built from the old
        spec. Serving one venv for two different requirement sets would be a
        wrong-answer bug, not a slow one.
        """
        monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", str(tmp_path / "cache"))
        runner = SubprocessRunner(LogHolder())

        async def build(reqs: List[str]) -> Tuple[VenvReference, int]:
            """Returns the reference and how many installs it took.

            A tuple rather than an attribute on VenvReference: mypy covers
            tests/ in this repo, and assigning an undeclared field to a
            dataclass instance fails it.
            """
            mock = self._mock_execute()
            with patch.object(runner, "execute", mock):
                ref = await setup_venv(
                    VenvConfig(
                        version="0.15.0.1",
                        main_plugin="snowflake",
                        extra_pip_requirements=reqs,
                    ),
                    runner,
                    tmp_path / "exec",
                )
            return ref, len([c for c in mock.call_args_list if "install" in c[0][0]])

        first, _ = await build(["pandas==2.0.0"])
        assert first.lock is not None
        first.lock.release()

        second, second_installs = await build(["pandas==2.1.0"])
        assert second.lock is not None
        second.lock.release()

        assert first.venv_loc != second.venv_loc
        assert second_installs, "a different requirement set reused an entry"

    # Every version whose name the cache is willing to reuse. The two existing
    # identity tests only used a pinned version, which routes through
    # get_stable_venv_name -- so _node_local_stable_name, the function that
    # makes `latest` (the default for EVERY recipe) and dev wheels cacheable,
    # had no identity coverage at all. Deleting both of its hash inputs left
    # the whole suite green.
    CACHEABLE_VERSIONS = [
        pytest.param("0.15.0.1", id="pinned"),
        pytest.param("latest", id="latest"),
        pytest.param("https://b983b409.datahub-wheels.pages.dev/", id="dev-wheel"),
    ]

    async def _build_counting_installs(
        self, tmp_path: pathlib.Path, config: VenvConfig
    ) -> Tuple[VenvReference, int]:
        """Build once, returning the reference and how many installs it took.

        A tuple rather than an attribute on VenvReference: mypy covers tests/
        here, and assigning an undeclared field to a dataclass fails it.
        """
        runner = SubprocessRunner(LogHolder())
        mock = self._mock_execute()
        with patch.object(runner, "execute", mock):
            ref = await setup_venv(config, runner, tmp_path / "exec")
        return ref, len([c for c in mock.call_args_list if "install" in c[0][0]])

    @pytest.mark.parametrize("version", CACHEABLE_VERSIONS)
    async def test_different_requirements_never_share_an_entry(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, version: str
    ) -> None:
        """Two requirement sets must not land on one venv, on ANY cacheable version.

        Serving one venv for two different requirement sets is a wrong-answer
        bug, not a slow one: the second recipe runs against the first's
        dependencies. The pinned case was already covered; `latest` and dev
        wheels were not, and they are the ones that go through
        _node_local_stable_name.
        """
        monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", str(tmp_path / "cache"))

        first, _ = await self._build_counting_installs(
            tmp_path,
            VenvConfig(
                version=version,
                main_plugin="snowflake",
                extra_pip_requirements=["pandas==2.0.0"],
            ),
        )
        assert first.lock is not None
        first.lock.release()

        second, second_installs = await self._build_counting_installs(
            tmp_path,
            VenvConfig(
                version=version,
                main_plugin="snowflake",
                extra_pip_requirements=["pandas==2.1.0"],
            ),
        )
        assert second.lock is not None
        second.lock.release()

        assert first.venv_loc != second.venv_loc, (
            "two requirement sets shared one cache entry"
        )
        assert second_installs, "the second requirement set reused an entry"

    @pytest.mark.parametrize("version", CACHEABLE_VERSIONS)
    async def test_different_plugins_never_share_an_entry(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, version: str
    ) -> None:
        """extra_pip_plugins changes what is installed, so it must change the key.

        Not varied anywhere else at the cache level, for any version, even
        though it is hashed alongside the requirements.
        """
        monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", str(tmp_path / "cache"))

        first, _ = await self._build_counting_installs(
            tmp_path,
            VenvConfig(
                version=version, main_plugin="snowflake", extra_pip_plugins=["athena"]
            ),
        )
        assert first.lock is not None
        first.lock.release()

        second, second_installs = await self._build_counting_installs(
            tmp_path,
            VenvConfig(
                version=version, main_plugin="snowflake", extra_pip_plugins=["bigquery"]
            ),
        )
        assert second.lock is not None
        second.lock.release()

        assert first.venv_loc != second.venv_loc, (
            "two plugin sets shared one cache entry"
        )
        assert second_installs, "the second plugin set reused an entry"

    async def test_a_dev_build_is_cached_but_its_packages_are_not(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A dev wheel URL names one immutable build, so the VENV is reusable.

        This reverses an earlier decision that excluded dev builds, on the
        grounds that they set UV_NO_CACHE=1 to stop pods over-consuming
        storage. That conflated two caches. UV_NO_CACHE governs the PACKAGE
        cache and must stay, because every dev wheel ships as the same name
        and version -- a cache keyed on those would hand one commit's build to
        another. The venv cache is keyed on the version STRING, which for a
        wheel is a deployment address naming exactly one build, so it is a
        content address in a way `latest` is not. Storage is bounded by
        evict_to_budget, which did not exist when the exclusion was written.

        Both halves are asserted here on purpose: reuse, and the package cache
        still being bypassed. Turning UV_NO_CACHE off would look like a tidy
        follow-up and would silently reintroduce the cross-commit collision.
        """
        monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", str(tmp_path / "cache"))
        url = "https://b983b409.datahub-wheels.pages.dev/"

        first = self._mock_execute()
        ref = await self._setup(tmp_path / "exec-1", url, first)
        assert ref.lock is not None, "a dev build should now take a cache entry"
        ref.lock.release()

        install_calls = [c for c in first.call_args_list if "install" in c[0][0]]
        assert install_calls, "the first build must actually install"
        assert all(
            (c.kwargs.get("env") or {}).get("UV_NO_CACHE") == "1" for c in install_calls
        ), "the package cache must stay bypassed for a dev build"

        second = self._mock_execute()
        ref2 = await self._setup(tmp_path / "exec-2", url, second)
        assert ref2.lock is not None
        ref2.lock.release()

        assert ref2.venv_loc == ref.venv_loc, "the second run should reuse the entry"
        assert not [c for c in second.call_args_list if "install" in c[0][0]], (
            "a cached dev build was rebuilt instead of reused"
        )

    async def test_a_partially_built_entry_is_rebuilt_not_reused(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The poisoned-entry case: a build killed midway leaves bin/python and
        no packages, which the old check accepted.

        Asserting only that installs happened (the original version of this
        test) is satisfiable even if the stale directory is never discarded --
        a build could silently run ON TOP of a dirty tree and still trigger
        installs. A sentinel file inside the partial entry, and asserting it
        is gone afterward, is what actually proves the discard happened.
        """
        cache = tmp_path / "cache"
        monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", str(cache))

        first = self._mock_execute()
        ref = await self._setup(tmp_path / "exec-1", "0.15.0.1", first)
        assert ref.lock is not None
        ref.lock.release()
        (ref.venv_loc / venv_utils.COMPLETE_MARKER).unlink()
        sentinel = ref.venv_loc / "stale-from-the-killed-build.txt"
        sentinel.write_text("leftover")

        second = self._mock_execute()
        ref2 = await self._setup(tmp_path / "exec-2", "0.15.0.1", second)
        assert ref2.lock is not None
        ref2.lock.release()

        assert [c for c in second.call_args_list if "install" in c[0][0]], (
            "an incomplete venv was reused"
        )
        assert not sentinel.exists(), (
            "the stale partial directory must be discarded, not built on top of"
        )

    async def test_the_kill_switch_restores_per_run_venvs(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DATAHUB_VENV_CACHE_ENABLED", "false")
        monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", str(tmp_path / "cache"))

        exec_dir = tmp_path / "exec-1"
        ref = await self._setup(exec_dir, "latest", self._mock_execute())

        assert ref.lock is None
        assert str(exec_dir) in str(ref.venv_loc)
        assert "venv-eph-" in str(ref.venv_loc), (
            "with the cache off, `latest` must fall back to today's ephemeral "
            "random name, not keep a stable cache name"
        )

        second = self._mock_execute()
        await self._setup(tmp_path / "exec-2", "latest", second)
        assert [c for c in second.call_args_list if "install" in c[0][0]]

    async def test_an_unwritable_cache_root_falls_back_to_a_per_run_venv(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The cache is an optimisation. It must never fail a task that would
        otherwise have run."""
        blocked = tmp_path / "blocked"
        blocked.mkdir()
        blocked.chmod(0o500)
        monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", str(blocked / "cache"))
        try:
            exec_dir = tmp_path / "exec-1"
            ref = await self._setup(exec_dir, "0.15.0.1", self._mock_execute())

            assert ref.lock is None
            assert str(exec_dir) in str(ref.venv_loc)
        finally:
            blocked.chmod(0o700)

    async def test_a_failed_build_releases_its_lock_for_the_next_attempt(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A build that fails must not leave its entry permanently locked.

        setup_venv never returns a VenvReference on the exception path, so
        nothing outside setup_venv -- not even Task 6's finalize_task_output --
        ever gets a chance to release the lock; the except block has to do it
        itself. Regression: without that release, a second attempt at the
        SAME entry in the SAME process (a later task in a long-lived pod)
        would spin through its retry budget and fall back to a per-run
        venv on every later task, for the life of the pod.

        The second attempt runs in a real OS thread with its own event loop,
        matching production (default_executor.py gives each task its own
        thread and loop) and, more importantly here, giving a real
        thread.join(timeout=) to detect a hang: a blocking flock() call
        freezes its own event loop entirely, so an in-loop cancellation
        timeout (anyio.fail_after) could never fire to rescue it.
        """
        monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", str(tmp_path / "cache"))
        config = VenvConfig(version="0.15.0.1", main_plugin="snowflake")

        async def failing_execute(command, env=None, cwd=None):
            if "venv" in command:
                venv_path = Path(command[-1])
                venv_path.mkdir(parents=True, exist_ok=True)
                (venv_path / "bin").mkdir(exist_ok=True)
                (venv_path / "bin" / "python").touch()
                return
            raise subprocess.CalledProcessError(1, command, "install failed")

        first_runner = SubprocessRunner(LogHolder())
        with patch.object(first_runner, "execute", side_effect=failing_execute):
            with pytest.raises(subprocess.CalledProcessError):
                await setup_venv(config, first_runner, tmp_path / "exec-1")

        result: List[Optional[VenvReference]] = [None]
        error: List[Optional[BaseException]] = [None]

        def run_second() -> None:
            async def _go() -> VenvReference:
                second_runner = SubprocessRunner(LogHolder())
                with patch.object(second_runner, "execute", self._mock_execute()):
                    return await setup_venv(config, second_runner, tmp_path / "exec-2")

            try:
                result[0] = anyio.run(_go)
            except BaseException as exc:  # surfaced via `error`, not swallowed
                error[0] = exc

        thread = threading.Thread(target=run_second)
        thread.start()
        thread.join(timeout=10)
        assert not thread.is_alive(), (
            "the second attempt hung -- the failed first build's lock was "
            "never released"
        )

        if error[0] is not None:
            raise error[0]
        assert result[0] is not None
        assert result[0].lock is not None
        result[0].lock.release()

    async def test_extra_env_vars_get_different_cache_entries(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """extra_env_vars can carry a different package index URL or
        credentials per recipe, and IS merged into the environment the venv is
        built and installed under. get_stable_venv_name() doesn't hash it
        (pre-existing contract other callers rely on), so two recipes
        differing only here would otherwise share one node-local cache entry
        and one would silently get a venv built against the other's index.
        """
        monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", str(tmp_path / "cache"))

        first = self._mock_execute()
        ref1 = await self._setup_with_env_vars(
            tmp_path / "exec-1",
            "0.15.0.1",
            first,
            {"PIP_INDEX_URL": "https://index-a.example/simple"},
        )
        assert ref1.lock is not None
        ref1.lock.release()

        second = self._mock_execute()
        ref2 = await self._setup_with_env_vars(
            tmp_path / "exec-2",
            "0.15.0.1",
            second,
            {"PIP_INDEX_URL": "https://index-b.example/simple"},
        )
        assert ref2.lock is not None
        ref2.lock.release()

        assert ref1.venv_loc != ref2.venv_loc
        assert [c for c in first.call_args_list if "install" in c[0][0]]
        assert [c for c in second.call_args_list if "install" in c[0][0]], (
            "a config with different extra_env_vars reused another's entry"
        )

    async def test_empty_extra_env_vars_keeps_todays_name(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The overwhelmingly common case (no extra_env_vars) must produce a
        byte-identical name to before this fix, so nothing else shifts."""
        monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", str(tmp_path / "cache"))

        implicit = await self._setup(
            tmp_path / "exec-1", "0.15.0.1", self._mock_execute()
        )
        assert implicit.lock is not None
        implicit.lock.release()

        explicit_empty = await self._setup_with_env_vars(
            tmp_path / "exec-2", "0.15.0.1", self._mock_execute(), {}
        )
        assert explicit_empty.lock is not None
        explicit_empty.lock.release()

        assert implicit.venv_loc == explicit_empty.venv_loc

    async def test_downgrade_to_shared_allows_readers_but_refuses_a_new_builder(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The whole point of downgrading to shared after a build: the entry
        stays visible to other concurrent shared holders while still
        correctly refusing a new exclusive (builder) holder. Nothing in
        setup_venv's own return value distinguishes "still exclusive" from
        "downgraded to shared" -- this has to inspect the fcntl state via a
        second, independent EntryLock on the same path.
        """
        monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", str(tmp_path / "cache"))

        ref = await self._setup(tmp_path / "exec-1", "0.15.0.1", self._mock_execute())
        assert ref.lock is not None

        lock_path = ref.venv_loc.parent / f"{ref.venv_loc.name}.lock"

        other_shared = EntryLock(lock_path)
        assert other_shared.acquire(exclusive=False), (
            "a downgraded-to-shared lock must allow another shared holder"
        )
        other_shared.release()

        other_exclusive = EntryLock(lock_path)
        assert not other_exclusive.acquire(exclusive=True), (
            "an exclusive lock must still be refused while the build's "
            "shared hold is outstanding -- downgrade_to_shared must not "
            "have released it outright"
        )

        ref.lock.release()

    async def test_a_build_that_loses_its_downgrade_returns_no_lock(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The build path must not hand back a lock it no longer holds.

        flock's downgrade is not atomic, so losing the conversion means the
        exclusive hold is already gone. A VenvReference still carrying the
        EntryLock would have finalize_task_output release a lock nobody has,
        and -- worse -- would report the entry as protected when eviction is
        free to take it.
        """
        monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", str(tmp_path / "cache"))
        monkeypatch.setattr(
            EntryLock, "downgrade_to_shared", lambda self: (self.release(), False)[1]
        )

        ref = await self._setup(tmp_path / "exec-1", "0.15.0.1", self._mock_execute())

        assert ref.lock is None
        assert ref.venv_loc.exists(), (
            "losing the lock must not cost the venv: it is built and complete, "
            "just no longer guarded"
        )

    async def test_a_peer_finished_entry_that_loses_its_downgrade_returns_no_lock(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The other downgrade site: EXCLUSIVE won, but a peer already built it.

        Reached when the shared acquire loses the race and the exclusive one
        wins, so it needs both staged. Same rule as the build path -- a lost
        downgrade must be reported as no lock at all.
        """
        monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", str(tmp_path / "cache"))

        ref = await self._setup(tmp_path / "exec-1", "0.15.0.1", self._mock_execute())
        assert ref.lock is not None
        ref.lock.release()

        real_acquire = EntryLock.acquire

        def refuse_shared(self: EntryLock, *, exclusive: bool) -> bool:
            return exclusive and real_acquire(self, exclusive=True)

        monkeypatch.setattr(EntryLock, "acquire", refuse_shared)
        monkeypatch.setattr(
            EntryLock, "downgrade_to_shared", lambda self: (self.release(), False)[1]
        )

        entry = await _acquire_cache_entry(
            ref.venv_loc.name.removeprefix("venv-"), tmp_path / "exec-2", True
        )

        assert entry.ready, "the peer's venv is complete and must still be reused"
        assert entry.lock is None

    async def test_a_cache_hit_advances_the_last_used_marker(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Deleting the touch_last_used() call on the reuse path leaves every
        other test green -- nothing else reads the marker. Set it far in the
        past by hand so a real hit is unambiguous regardless of filesystem
        mtime resolution.
        """
        monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", str(tmp_path / "cache"))

        ref = await self._setup(tmp_path / "exec-1", "0.15.0.1", self._mock_execute())
        assert ref.lock is not None
        ref.lock.release()

        old_time = time.time() - 10_000
        marker = ref.venv_loc / venv_utils.LAST_USED_MARKER
        os.utime(marker, (old_time, old_time))
        assert venv_utils.last_used_at(ref.venv_loc) == pytest.approx(old_time, abs=1)

        ref2 = await self._setup(tmp_path / "exec-2", "0.15.0.1", self._mock_execute())
        assert ref2.lock is not None
        ref2.lock.release()

        assert venv_utils.last_used_at(ref2.venv_loc) > old_time + 5_000, (
            "a cache hit must advance the last-used marker"
        )

    async def test_setup_venv_invokes_eviction_before_building(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Deleting the evict_to_budget() call in _acquire_cache_entry leaves
        every other test green -- none of them ever fill the budget. Assert
        the call happens at all, with the configured budget and the right
        cache root, rather than relying on an end-to-end size scenario.
        """
        monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", str(tmp_path / "cache"))
        monkeypatch.setenv("DATAHUB_VENV_CACHE_MAX_GB", "5")

        calls: List[Tuple[pathlib.Path, int]] = []

        def fake_evict_to_budget(cache_root: pathlib.Path, max_bytes: int) -> int:
            calls.append((cache_root, max_bytes))
            return 0

        monkeypatch.setattr(
            "datahub.executor.execution.runner.evict_to_budget",
            fake_evict_to_budget,
        )

        ref = await self._setup(tmp_path / "exec-1", "0.15.0.1", self._mock_execute())
        assert ref.lock is not None
        ref.lock.release()

        assert calls, "a cacheable build must run eviction to make room"
        cache_root, max_bytes = calls[0]
        assert cache_root == ref.venv_loc.parent
        assert max_bytes == 5 * 1024**3

    async def test_a_full_cache_filesystem_does_not_fail_a_successful_build(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """mark_venv_complete can raise OSError on a full or read-only cache
        filesystem. The Global Constraint says the cache must never fail a
        task that would otherwise have run -- an unmarked venv just looks
        incomplete and gets rebuilt next time, the same degradation
        touch_last_used already accepts.
        """
        monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", str(tmp_path / "cache"))

        def raise_os_error(venv_loc: pathlib.Path) -> None:
            raise OSError("disk full")

        monkeypatch.setattr(
            "datahub.executor.execution.runner.mark_venv_complete",
            raise_os_error,
        )

        ref = await self._setup(tmp_path / "exec-1", "0.15.0.1", self._mock_execute())
        assert ref.lock is not None
        ref.lock.release()


async def test_the_expanded_requirements_file_does_not_outlive_the_install(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """extra-requirements.txt holds a token, so it must not persist.

    resolve_pip_requirements substitutes ${VAR} before the file is written, so
    a private index URL arrives here with its credential in it. The file is an
    input to `uv pip install -r` and nothing reads it afterwards.

    The venv cache is what made this matter: venv_loc for a cacheable venv is
    the node-local cache root, which get_venv_cache_path deliberately places
    OUTSIDE exec_out_dir -- so the file stopped being cleaned up with the run
    and sat in a directory shared by every task on the node.
    """
    monkeypatch.setenv("PIP_INDEX_TOKEN", "index-token-value-1")
    logs = LogHolder()
    runner = SubprocessRunner(logs)

    seen: dict = {}

    async def mock_execute(command, env=None, cwd=None):
        if "venv" in command:
            venv_path = Path(command[-1])
            venv_path.mkdir(parents=True, exist_ok=True)
            (venv_path / "bin").mkdir(exist_ok=True)
            (venv_path / "bin" / "python").touch()
        if "-r" in command:
            # While the install runs the file must exist, carry the expanded
            # token, and not be readable by anyone else on the node.
            req = Path(command[command.index("-r") + 1])
            seen["existed"] = req.is_file()
            seen["text"] = req.read_text()
            seen["mode"] = req.stat().st_mode & 0o777

    with patch.object(runner, "execute", AsyncMock(side_effect=mock_execute)):
        venv_ref = await setup_venv(
            VenvConfig(
                version="0.12.1.5",
                main_plugin="snowflake",
                extra_pip_requirements=["pkg @ https://u:${PIP_INDEX_TOKEN}@x/simple"],
            ),
            runner,
            tmp_path,
        )

    assert seen.get("existed"), "the install never saw a requirements file"
    assert "index-token-value-1" in seen["text"], "the token was not expanded"
    assert seen["mode"] == 0o600, (
        f"world/group readable while it existed: {seen['mode']:o}"
    )

    leftover = pathlib.Path(venv_ref.venv_loc) / "extra-requirements.txt"
    assert not leftover.exists(), (
        f"{leftover} outlived the install; it holds an expanded credential and "
        "for a cacheable venv this directory is never cleaned up"
    )
    # And the token is not recoverable anywhere else under the venv.
    assert not any(
        "index-token-value-1" in p.read_text(errors="ignore")
        for p in pathlib.Path(venv_ref.venv_loc).rglob("*")
        if p.is_file()
    )


async def test_a_failed_install_does_not_leave_the_token_file_behind(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The reason the cleanup is in a `finally`.

    A bad token or an unreachable private index is the common failure here,
    and it is exactly the run whose requirements file must not survive: the
    venv is left incomplete in the cache, so nothing rebuilds over it until
    the next attempt.
    """
    monkeypatch.setenv("PIP_INDEX_TOKEN", "index-token-value-2")
    runner = SubprocessRunner(LogHolder())
    req_paths: list[pathlib.Path] = []

    async def mock_execute(command, env=None, cwd=None):
        if "venv" in command:
            venv_path = Path(command[-1])
            venv_path.mkdir(parents=True, exist_ok=True)
            (venv_path / "bin").mkdir(exist_ok=True)
            (venv_path / "bin" / "python").touch()
        if "-r" in command:
            req_paths.append(Path(command[command.index("-r") + 1]))
            raise TaskError("uv pip install failed: 401 Unauthorized from index")

    with patch.object(runner, "execute", AsyncMock(side_effect=mock_execute)):
        with pytest.raises(TaskError):
            await setup_venv(
                VenvConfig(
                    version="0.12.1.5",
                    main_plugin="snowflake",
                    extra_pip_requirements=[
                        "pkg @ https://u:${PIP_INDEX_TOKEN}@x/simple"
                    ],
                ),
                runner,
                tmp_path,
            )

    assert req_paths, "the install was never attempted"
    assert not req_paths[0].exists(), (
        f"{req_paths[0]} survived a failed install still holding the token"
    )


async def test_a_failure_after_the_token_is_written_does_not_leave_it_behind(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The window between writing the file and starting the install.

    The sibling test above covers a failing install. This covers the steps
    before it: the token is on disk from write_text onwards, and the log
    appends that follow can raise too. With the try opening at the install
    instead of at the file, those failures left the credential in the
    node-local cache, which nothing cleans up because it outlives the task by
    design.

    The log append is used as the trigger because it is a real step between
    the two, not a contrived one.
    """
    monkeypatch.setenv("PIP_INDEX_TOKEN", "index-token-value-3")
    # Keep the cache under tmp_path: a cacheable venv otherwise lands in a
    # root beside it, where the search below would not find the file.
    monkeypatch.setenv("DATAHUB_VENV_CACHE_PATH", str(tmp_path / "cache"))
    runner = SubprocessRunner(LogHolder())
    written: list[pathlib.Path] = []

    async def mock_execute(command, env=None, cwd=None):
        if "venv" in command:
            venv_path = Path(command[-1])
            venv_path.mkdir(parents=True, exist_ok=True)
            (venv_path / "bin").mkdir(exist_ok=True)
            (venv_path / "bin" / "python").touch()

    def exploding_append_masked(text: str) -> None:
        # By now write_text has run, so the token is already on disk.
        written.extend(tmp_path.rglob("extra-requirements.txt"))
        raise OSError("log sink unavailable")

    with patch.object(runner, "execute", AsyncMock(side_effect=mock_execute)):
        with patch.object(runner._logs, "append_masked", exploding_append_masked):
            with pytest.raises(OSError):
                await setup_venv(
                    VenvConfig(
                        version="0.12.1.5",
                        main_plugin="snowflake",
                        extra_pip_requirements=[
                            "pkg @ https://u:${PIP_INDEX_TOKEN}@x/simple"
                        ],
                    ),
                    runner,
                    tmp_path,
                )

    assert written, "the requirements file was never written, so nothing was proven"
    assert not written[0].exists(), (
        f"{written[0]} survived a failure before the install, still holding the token"
    )
