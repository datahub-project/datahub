from click.testing import CliRunner
from datahub.entrypoints import datahub


def test_check_local_docker():
    # This just verifies that it runs without error.
    runner = CliRunner()
    result = runner.invoke(datahub, ["check", "local-docker"])
    assert result.output
