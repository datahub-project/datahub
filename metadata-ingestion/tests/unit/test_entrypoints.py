import logging

import pytest
from click.testing import CliRunner


def test_agent_shim_command_behavior():
    """Test that the agent shim command displays the correct error message."""
    from datahub.cli.cli_utils import make_shim_command

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
    from datahub.entrypoints import _evals_import_suggestion

    assert expected in _evals_import_suggestion(error)
    assert not any(record.levelno >= logging.WARNING for record in caplog.records)


def _assert_optional_command_shows_error_or_help(
    command_name, install_message, command_args
):
    import datahub.entrypoints

    result = CliRunner().invoke(
        datahub.entrypoints.datahub, [command_name, *command_args]
    )

    if result.exit_code == 1:
        assert "missing dependencies" in result.output
        assert install_message in result.output
    else:
        assert result.exit_code == 0


def test_registered_evals_command_shows_helpful_error_or_help():
    """Test that the registered evals command is usable or explains its dependency."""
    _assert_optional_command_shows_error_or_help(
        "evals", "pip install 'acryl-datahub[datahub-evals]'", []
    )


def test_agent_command_exists():
    """Test that agent command is registered in the CLI."""
    import datahub.entrypoints

    # Verify agent command was added (either real or shim)
    assert "agent" in datahub.entrypoints.datahub.commands


def test_agent_command_shows_error_or_help():
    """Test that agent command either works or shows helpful error."""
    _assert_optional_command_shows_error_or_help(
        "agent", "pip install datahub-agent-context", ["--help"]
    )


@pytest.mark.parametrize(
    "command_name",
    ["agent", "actions", "evals", "lite"],
)
def test_optional_commands_exist(command_name):
    """Test that optional commands are always registered."""
    import datahub.entrypoints

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
    from datahub.cli.cli_utils import make_shim_command

    shim_command = make_shim_command(command_name, f"run `{install_message}`")

    runner = CliRunner()
    result = runner.invoke(shim_command, [])

    assert result.exit_code == 1
    assert "missing dependencies" in result.output
    assert install_message in result.output
