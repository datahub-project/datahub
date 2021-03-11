from click.testing import CliRunner

from datahub.entrypoints import datahub


def test_list_all():
    # This just verifies that it runs without error.
    runner = CliRunner()
    result = runner.invoke(datahub, ["ingest-list-plugins"])
    assert result.exit_code == 0
