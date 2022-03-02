from pathlib import Path
from typing import List, Optional

from click.testing import CliRunner, Result

from datahub.entrypoints import datahub
from datahub.telemetry.telemetry import telemetry_instance
from tests.test_helpers import fs_helpers

# disable telemetry for tests under this instance
telemetry_instance.enabled = False


def assert_result_ok(result: Result) -> None:
    if result.exception:
        raise result.exception
    assert result.exit_code == 0


def run_datahub_cmd(
    command: List[str], tmp_path: Optional[Path] = None, check_result: bool = True
) -> Result:
    runner = CliRunner()

    if tmp_path is None:
        result = runner.invoke(datahub, command)
    else:
        with fs_helpers.isolated_filesystem(tmp_path):
            result = runner.invoke(datahub, command)

    if check_result:
        assert_result_ok(result)
    return result
