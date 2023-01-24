import logging
import pathlib

import click
import regex
from click.testing import CliRunner

from datahub.entrypoints import datahub
from datahub.utilities.logging_manager import get_log_buffer


@datahub.command()
def my_logging_fn():
    logger = logging.getLogger("datahub.my_cli_module")

    print("this is a print statement")

    click.echo("this is a click.echo statement")

    logger.debug("Example debug line")
    logger.info("This is an %s message", "info")  # test string interpolation
    logger.warning("This is a warning message")
    logger.error("this is an error with no stack trace")
    try:
        1 / 0
    except ZeroDivisionError:
        logger.exception("failed to divide by zero")


def test_cli_logging(tmp_path):
    log_file: pathlib.Path = tmp_path / "datahub.log"

    runner = CliRunner()
    result = runner.invoke(
        datahub, ["--debug", "--log-file", str(log_file), "my-logging-fn"]
    )
    assert result.exit_code == 0

    # The output should include the stdout and stderr, formatted as expected.
    regex.match(
        r"""\
this is a print statement
this is a click.echo statement
[.+] DEBUG    .datahub.my_cli_module:\d+. - Example debug line
[.+] INFO     .datahub.my_cli_module:\d+. - This is an info message
[.+] WARNING  .datahub.my_cli_module:\d+. - This is a warning message
[.+] ERROR    .datahub.my_cli_module:\d+. - this is an error with no stack trace
[.+] ERROR    .datahub.my_cli_module:\d+. - failed to divide by zero
Traceback (most recent call last):
  File .+, in my_logging_fn
    1 / 0
ZeroDivisionError: division by zero
""",
        result.output,
    )

    # The log file should match the output exactly.
    log_file_output = log_file.read_text()
    assert log_file_output == result.output

    # The in-memory log buffer should contain the log messages.
    # The first two lines are stdout, so we skip them.
    expected_log_output = "\n".join(result.output.splitlines()[2:])
    assert get_log_buffer().format_lines() == expected_log_output
