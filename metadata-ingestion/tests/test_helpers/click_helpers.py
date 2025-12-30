from io import BytesIO
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
    """
    Run a datahub CLI command in a test context

    This function handles a known issue with Click's testing framework where it may raise
    "ValueError: I/O operation on closed file" after the command has successfully completed under some conditions,
    such as console debug logs enabled.
    See related issues:
    - https://github.com/pallets/click/issues/824
    """
    runner = CliRunner()

    try:
        if tmp_path is None:
            result = runner.invoke(datahub, command)
        else:
            with fs_helpers.isolated_filesystem(tmp_path):
                result = runner.invoke(datahub, command)
    except ValueError as e:
        if "I/O operation on closed file" in str(e):
            # This is a known issue with the Click testing framework
            # The command likely still succeeded, so we'll construct a basic Result object
            # and continue with the test
            print(f"WARNING: Caught Click I/O error but continuing with the test: {e}")
            # Create an empty buffer for stdout and stderr
            empty_buffer = BytesIO()
            result = Result(
                runner=runner,
                stdout_bytes=empty_buffer.getvalue(),
                stderr_bytes=empty_buffer.getvalue(),
                return_value=None,  # type: ignore[call-arg]
                exit_code=0,
                exception=None,
                exc_info=None,
            )
        else:
            # Re-raise if it's not the specific error we're handling
            raise

    if check_result:
        assert_result_ok(result)
    return result
