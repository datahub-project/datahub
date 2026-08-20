import logging

import pytest
from click.testing import CliRunner

import datahub.entrypoints
from datahub.cli.cli_utils import make_shim_command
from datahub.entrypoints import _evals_import_suggestion

# Shim commands all share make_shim_command's docstring, so a probe shim tells us
# whether a registered command is the real plugin or the fallback.
_SHIM_HELP = make_shim_command("probe", "run `pip install probe`").help


def test_agent_shim_command_behavior():
    """Test that the agent shim command displays the correct error message."""
    shim_command = make_shim_command("agent", "run `pip install datahub-agent-context`")

    runner = CliRunner()
    result = runner.invoke(shim_command, [])

    assert result.exit_code == 1
    assert "missing dependencies" in result.output
    assert "run `pip install datahub-agent-context`" in result.output


@pytest.mark.parametrize(
    "error,expected",
    [
        (
            ModuleNotFoundError(
                "No module named 'acryl_datahub_cloud'", name="acryl_datahub_cloud"
            ),
            "pip install 'acryl-datahub[datahub-evals]'",
        ),
        (
            ModuleNotFoundError("No module named 'graphql'", name="graphql"),
            "fix the acryl-datahub-cloud installation",
        ),
        (
            ModuleNotFoundError(
                "No module named 'acryl_datahub_cloud.cli'",
                name="acryl_datahub_cloud.cli",
            ),
            "fix the acryl-datahub-cloud installation",
        ),
        (
            ImportError(
                "cannot import name 'evals' from 'acryl_datahub_cloud.cli.evals'"
            ),
            "fix the acryl-datahub-cloud installation",
        ),
    ],
)
def test_evals_import_suggestion(error, expected, caplog):
    assert expected in _evals_import_suggestion(error)
    assert not any(record.levelno >= logging.WARNING for record in caplog.records)


def _assert_optional_command_shows_error_or_help(command_name, install_message):
    """Assert the registered command either runs or explains its missing plugin.

    The shim only prints its message from the command body, so it has to be
    invoked without --help, which click resolves before the callback runs. The
    real command may require arguments, so that one is probed with --help.
    """
    command = datahub.entrypoints.datahub.commands[command_name]

    if command.help == _SHIM_HELP:
        result = CliRunner().invoke(datahub.entrypoints.datahub, [command_name])
        assert result.exit_code == 1
        assert "missing dependencies" in result.output
        assert install_message in result.output
    else:
        result = CliRunner().invoke(
            datahub.entrypoints.datahub, [command_name, "--help"]
        )
        assert result.exit_code == 0


def test_registered_evals_command_shows_helpful_error_or_help():
    """Test that the registered evals command is usable or explains its dependency."""
    _assert_optional_command_shows_error_or_help(
        "evals", "pip install 'acryl-datahub[datahub-evals]'"
    )


def test_agent_command_exists():
    """Test that agent command is registered in the CLI."""
    # Verify agent command was added (either real or shim)
    assert "agent" in datahub.entrypoints.datahub.commands


def test_agent_command_shows_error_or_help():
    """Test that agent command either works or shows helpful error."""
    _assert_optional_command_shows_error_or_help(
        "agent", "pip install datahub-agent-context"
    )


@pytest.mark.parametrize(
    "command_name",
    ["agent", "actions", "evals", "lite"],
)
def test_optional_commands_exist(command_name):
    """Test that optional commands are always registered."""
    # Verify the command exists in the CLI
    assert command_name in datahub.entrypoints.datahub.commands


@pytest.mark.parametrize(
    "command_name,install_message",
    [
        ("agent", "pip install datahub-agent-context"),
        ("actions", "pip install acryl-datahub-actions"),
        ("evals", "pip install 'acryl-datahub[datahub-evals]'"),
        ("lite", "pip install 'acryl-datahub[datahub-lite]'"),
    ],
)
def test_shim_commands_show_helpful_error(command_name, install_message):
    """Test that shim commands created by make_shim_command show helpful error messages."""
    shim_command = make_shim_command(command_name, f"run `{install_message}`")

    runner = CliRunner()
    result = runner.invoke(shim_command, [])

    assert result.exit_code == 1
    assert "missing dependencies" in result.output
    assert install_message in result.output
