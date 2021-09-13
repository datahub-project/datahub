from click.testing import CliRunner

from datahub.entrypoints import datahub


def test_cli_help():
    runner = CliRunner()
    result = runner.invoke(datahub, ["--help"])
    assert result.output


def test_cli_version():
    runner = CliRunner()
    result = runner.invoke(datahub, ["--debug", "version"])
    assert result.output


def test_check_local_docker():
    # This just verifies that it runs without error.
    # We don't actually know what environment this will be run in, so
    # we can't depend on the output. Eventually, we should mock the docker SDK.
    runner = CliRunner()
    result = runner.invoke(datahub, ["check", "local-docker"])
    assert result.output
