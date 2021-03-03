import fs_helpers
import mce_helpers
from click.testing import CliRunner

from datahub.entrypoints import datahub


def test_mysql_ingest(mysql, pytestconfig, tmp_path):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/mysql"
    config_file = (test_resources_dir / "mysql_to_file.yml").resolve()

    # Run the metadata ingestion pipeline.
    runner = CliRunner()
    with fs_helpers.isolated_filesystem(tmp_path):
        result = runner.invoke(datahub, ["ingest", "-c", f"{config_file}"])
        assert result.exit_code == 0

        output = mce_helpers.load_json_file("mysql_mces.json")

    # Verify the output.
    golden = mce_helpers.load_json_file(
        str(test_resources_dir / "mysql_mce_golden.json")
    )
    mce_helpers.assert_mces_equal(output, golden)
