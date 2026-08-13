import subprocess
from pathlib import Path
from typing import Any, List
from unittest.mock import patch

import pytest

from tests.integration.airbyte.airbyte_test_setup import (  # type: ignore[import-untyped]
    update_airbyte_database_id,
)


def _completed(returncode: int, stdout: str = "", stderr: str = "") -> Any:
    return subprocess.CompletedProcess(
        args=["kubectl"], returncode=returncode, stdout=stdout, stderr=stderr
    )


@pytest.fixture
def kubeconfig(tmp_path: Path) -> Path:
    path = tmp_path / "abctl.kubeconfig"
    path.write_text("")
    return path


def test_transient_kubectl_failure_is_retried(kubeconfig: Path) -> None:
    results = [
        _completed(1, stderr="error: unable to upgrade connection: pod not running"),
        _completed(0, stdout="UPDATE 1"),
    ]

    with (
        patch("shutil.which", return_value="/usr/bin/kubectl"),
        patch("time.sleep"),
        patch("subprocess.run", side_effect=results) as run,
    ):
        update_airbyte_database_id(kubeconfig, "workspace", "old-id", "new-id")

    assert run.call_count == 2


def test_psql_error_is_surfaced_after_retries_are_exhausted(kubeconfig: Path) -> None:
    psql_error = (
        'ERROR:  update or delete on table "workspace" violates foreign key constraint'
    )

    with (
        patch("shutil.which", return_value="/usr/bin/kubectl"),
        patch("time.sleep"),
        patch("subprocess.run", return_value=_completed(1, stderr=psql_error)),
    ):
        with pytest.raises(RuntimeError) as exc_info:
            update_airbyte_database_id(kubeconfig, "workspace", "old-id", "new-id")

    assert psql_error in str(exc_info.value)


def test_update_matching_no_rows_is_a_failure(kubeconfig: Path) -> None:
    # psql exits 0 for an UPDATE that matched nothing. Treating that as success is
    # what let the test continue with Airbyte's random workspace UUID.
    with (
        patch("shutil.which", return_value="/usr/bin/kubectl"),
        patch("time.sleep"),
        patch("subprocess.run", return_value=_completed(0, stdout="UPDATE 0")),
    ):
        with pytest.raises(RuntimeError):
            update_airbyte_database_id(kubeconfig, "workspace", "old-id", "new-id")


def test_rewrite_committed_by_a_timed_out_attempt_is_not_a_false_failure(
    kubeconfig: Path,
) -> None:
    # kubectl can be killed at the timeout after psql already committed server-side.
    # The retry then matches no rows, so the end state — not the UPDATE count — is
    # what decides success.
    row_present = " ?column?\n----------\n        1\n(1 row)"
    results = [
        subprocess.TimeoutExpired(cmd="kubectl", timeout=60),
        _completed(0, stdout=row_present),
    ]

    with (
        patch("shutil.which", return_value="/usr/bin/kubectl"),
        patch("time.sleep"),
        patch("subprocess.run", side_effect=results),
    ):
        update_airbyte_database_id(kubeconfig, "workspace", "old-id", "new-id")


def test_non_timeout_exec_failure_is_retried_and_surfaced(kubeconfig: Path) -> None:
    with (
        patch("shutil.which", return_value="/usr/bin/kubectl"),
        patch("time.sleep"),
        patch("subprocess.run", side_effect=OSError("exec format error")) as run,
    ):
        with pytest.raises(RuntimeError, match="exec format error"):
            update_airbyte_database_id(kubeconfig, "workspace", "old-id", "new-id")

    assert run.call_count > 1


def test_timeout_is_retried_then_surfaced(kubeconfig: Path) -> None:
    calls: List[Any] = []

    def fake_run(*args: Any, **kwargs: Any) -> Any:
        calls.append(args)
        raise subprocess.TimeoutExpired(cmd="kubectl", timeout=30)

    with (
        patch("shutil.which", return_value="/usr/bin/kubectl"),
        patch("time.sleep"),
        patch("subprocess.run", side_effect=fake_run),
    ):
        with pytest.raises(RuntimeError, match="timed out"):
            update_airbyte_database_id(kubeconfig, "workspace", "old-id", "new-id")

    assert len(calls) > 1
