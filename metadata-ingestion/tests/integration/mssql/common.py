import subprocess
from pathlib import Path

import yaml

CONTAINER = "testsqlserver"
_SQLCMD = "/opt/mssql-tools18/bin/sqlcmd"
# A wedged daemon fails the fixture, not the whole batch at the process backstop.
_SQLCMD_TIMEOUT_SECONDS = 120


def sa_password() -> str:
    """The throwaway container's own SA password, read from its compose file
    so it lives in one place."""
    compose = yaml.safe_load((Path(__file__).parent / "docker-compose.yml").read_text())
    return str(compose["services"][CONTAINER]["environment"]["SA_PASSWORD"])


def run_sqlcmd(*args: str) -> "subprocess.CompletedProcess[str]":
    """sqlcmd inside the fixture container as sa, failing on the first error
    (-b). Returns the completed process; callers decide what a failure means."""
    return subprocess.run(
        [
            "docker",
            "exec",
            CONTAINER,
            _SQLCMD,
            "-C",
            "-S",
            "localhost",
            "-U",
            "sa",
            "-P",
            sa_password(),
            "-b",
            *args,
        ],
        capture_output=True,
        text=True,
        timeout=_SQLCMD_TIMEOUT_SECONDS,
    )
