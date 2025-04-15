import pathlib
import subprocess
import time

import anyio
import pytest
from loguru import logger

from datahub_integrations.dispatch.runner import (
    LogHolder,
    SubprocessRunner,
    VenvConfig,
    setup_venv,
)

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def test_log_holder_simple() -> None:
    logs = LogHolder(echo_to_stdout_prefix="runner: ")
    logs.append("hello ")
    logs.append("world\n")
    logs.append("hi there!")
    assert logs.get_logs() == "hello world\nhi there!"


def test_log_holder_complex() -> None:
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
    # Test running a simple command.
    logs = LogHolder(echo_to_stdout_prefix="runner: ")
    runner = SubprocessRunner(logs)
    await runner.execute(["echo", "hello"])

    assert "hello" in logs.get_logs()


async def test_run_failing_command() -> None:
    # Test a failed command.
    logs = LogHolder(echo_to_stdout_prefix="failing command: ")
    runner = SubprocessRunner(logs)
    with pytest.raises(subprocess.CalledProcessError):
        await runner.execute(["false"])


async def test_run_timeout() -> None:
    start_time = time.perf_counter()

    logs = LogHolder(echo_to_stdout_prefix="test timeout: ")
    runner = SubprocessRunner(logs)
    with pytest.raises(ExceptionGroup) as exc_info:
        async with anyio.create_task_group() as _tg:
            with anyio.fail_after(1):
                await runner.execute(["sleep", "10"])
    assert exc_info.group_contains(TimeoutError)
    assert not exc_info.group_contains(anyio.get_cancelled_exc_class())
    assert not exc_info.group_contains(subprocess.CalledProcessError)

    # We should have timed out after about 1 second.
    assert 1 < time.perf_counter() - start_time < 1.2

    logger.debug("Timeout hit, now checking if the subprocess is dead")
    assert runner._process is not None
    logger.debug(f"Subprocess return code: {runner._process.returncode}")
    assert runner._process.returncode != 0


async def test_run_yes() -> None:
    logs = LogHolder(echo_to_stdout_prefix="runner: ")
    runner = SubprocessRunner(logs)

    # The `yes` command generates output indefinitely. This test ensures
    # that we handle log reading cleanup correctly during a cancellation.
    with pytest.raises(TimeoutError):
        with anyio.fail_after(1):
            await runner.execute(["yes", "wooooo " * 20])

    assert "wooooo" in logs.get_logs()


async def test_venv_simple(tmp_path: pathlib.Path) -> None:
    # Test venv setup.
    logs = LogHolder(echo_to_stdout_prefix="venv-setup-test: ")
    runner = SubprocessRunner(logs)

    tmp_path.mkdir(exist_ok=True)
    await setup_venv(
        VenvConfig(
            version="https://b983b409.acryl-wheels.pages.dev/",
            main_plugin="snowflake",
        ),
        runner,
        tmp_path,
    )


@pytest.mark.parametrize("version", ["0.12.1.5", "native"])
async def test_running_venv_command(tmp_path: pathlib.Path, version: str) -> None:
    logs = LogHolder(echo_to_stdout_prefix="venv-commands: ")
    runner = SubprocessRunner(logs)

    tmp_path.mkdir(exist_ok=True)
    venv = await setup_venv(
        VenvConfig(version=version),
        runner,
        tmp_path,
    )

    logs.clear()
    await runner.execute([venv.command("datahub"), "--version"])
    assert "acryl-datahub" in logs.get_logs()


async def test_repeat_venv_setup(tmp_path: pathlib.Path) -> None:
    logs = LogHolder(echo_to_stdout_prefix="venv-setup-1: ")
    runner = SubprocessRunner(logs)

    await setup_venv(
        VenvConfig(version="0.12.1.5", main_plugin="snowflake"),
        runner,
        tmp_path,
    )
    logger.debug("Finished first setup")

    assert "pip install" in logs.get_logs()

    logs = LogHolder(echo_to_stdout_prefix="venv-setup-2: ")
    runner = SubprocessRunner(logs)
    await setup_venv(
        VenvConfig(version="0.12.1.5", main_plugin="snowflake"),
        runner,
        tmp_path,
    )
    logger.debug("Finished second setup")

    assert "skipping setup" in logs.get_logs()
