import logging
import subprocess

import pytest

from tests.utilities.domains import Domain

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.no_cypress_suite1, pytest.mark.domain(Domain.PLATFORM)]

# ==============================================
# ACTIONS CONTAINER SHUTDOWN TESTS
# ==============================================
# Consumer offsets are flushed by EventSource.close(), reached only through
# pipeline_manager.stop_all() on the shutdown-signal path. Two things must hold for a
# container stop to get there:
#
#   1. the entrypoint must `exec` the CLI so python is PID 1 and receives the SIGTERM a
#      container runtime sends -- bash as PID 1 swallows it, and
#   2. `datahub actions run` must register a SIGTERM handler, not just SIGINT.
#
# (2) is covered by a unit test (tests/unit/cli/test_actions.py::
# test_run_registers_sigterm_handler). (1) cannot be: it lives in a shell script, so it
# needs a real container to observe, which is what this test does.
#
# The end-to-end consequence -- the durable consumer offset advancing across a stop -- is
# deliberately not asserted here. It needs a pipeline with its own consumer group and live
# event traffic; running that alongside the shared actions container would join its
# consumer groups and disturb the doc-propagation tests. It is verified out-of-band
# instead.


def _docker(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, check=check
    )


def _infer_actions_container() -> str:
    completed = subprocess.run(
        "docker ps --format '{{.Names}}' | grep actions",
        capture_output=True,
        shell=True,
        text=True,
        check=False,
    )
    lines = str(completed.stdout).splitlines()
    if not lines:
        pytest.skip("No datahub-actions container running in this environment")
    return lines[0]


@pytest.mark.integration
def test_actions_entrypoint_execs_so_python_is_pid_1() -> None:
    """PID 1 must be the python process, otherwise SIGTERM never reaches the handler.

    Without `exec` in docker/datahub-actions/start.sh, bash stays PID 1 and the CLI runs
    as its child; a container stop then kills the process group without any pipeline ever
    running stop_all(). Reads /proc/1/cmdline rather than shelling out to ps, which is not
    present in the slim image.
    """
    container = _infer_actions_container()

    # /proc/1/cmdline is NUL-separated; tr makes it greppable.
    result = _docker(
        "exec", container, "sh", "-c", "tr '\\0' ' ' < /proc/1/cmdline", check=False
    )
    assert result.returncode == 0, f"could not read /proc/1/cmdline: {result.stderr}"
    pid1 = result.stdout.strip()
    logger.info(f"actions container {container} PID 1: {pid1!r}")

    # Deliberately not matching bare "datahub": the broken cmdline is
    # "/bin/bash /start_datahub_actions.sh", which contains it. "python" and the
    # hyphenated console-script name appear only when the CLI itself is PID 1.
    assert "python" in pid1 or "datahub-actions" in pid1, (
        f"PID 1 in the actions container is {pid1!r}, expected the datahub-actions "
        "python process. A bash PID 1 means start.sh lost its `exec` and SIGTERM "
        "will not reach the shutdown handler."
    )
    assert "bash" not in pid1, (
        f"PID 1 is bash ({pid1!r}); start.sh must `exec` the CLI so it becomes PID 1."
    )
