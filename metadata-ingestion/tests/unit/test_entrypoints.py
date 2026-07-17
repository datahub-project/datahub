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


def test_agent_command_exists():
    """Test that agent command is registered in the CLI."""
    import datahub.entrypoints

    # Verify agent command was added (either real or shim)
    assert "agent" in datahub.entrypoints.datahub.commands


def test_agent_command_shows_error_or_help():
    """Test that agent command either works or shows helpful error."""
    import datahub.entrypoints

    runner = CliRunner()
    result = runner.invoke(datahub.entrypoints.datahub, ["agent", "--help"])

    # Either the command works (exit code 0) or it's a shim (exit code 1 with helpful message)
    if result.exit_code == 1:
        assert "missing dependencies" in result.output
        assert "pip install datahub-agent-context" in result.output
    else:
        # Real command should show help
        assert result.exit_code == 0


@pytest.mark.parametrize(
    "command_name",
    ["agent", "actions", "lite"],
)
def test_optional_commands_exist(command_name):
    """Test that optional commands (agent, actions, lite) are always registered."""
    import datahub.entrypoints

    # Verify the command exists in the CLI
    assert command_name in datahub.entrypoints.datahub.commands


@pytest.mark.parametrize(
    "command_name,install_message",
    [
        ("agent", "pip install datahub-agent-context"),
        ("actions", "pip install acryl-datahub-actions"),
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
