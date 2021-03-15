import mce_helpers
from click.testing import CliRunner

from datahub.entrypoints import datahub
from datahub.ingestion.run.pipeline import Pipeline


def test_serde_large(pytestconfig, tmp_path):
    json_filename = "test_serde_large.json"
    output_filename = "output.json"

    test_resources_dir = pytestconfig.rootpath / "tests/unit/serde"

    golden_file = test_resources_dir / json_filename
    output_file = tmp_path / output_filename

    pipeline = Pipeline.create(
        {
            "source": {"type": "file", "config": {"filename": str(golden_file)}},
            "sink": {"type": "file", "config": {"filename": str(output_file)}},
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    output = mce_helpers.load_json_file(tmp_path / output_filename)
    golden = mce_helpers.load_json_file(golden_file)
    mce_helpers.assert_mces_equal(output, golden)


def test_check_mce_schema(pytestconfig):
    json_filename = "test_serde_large.json"
    test_resources_dir = pytestconfig.rootpath / "tests/unit/serde"
    json_file_path = test_resources_dir / json_filename

    runner = CliRunner()
    result = runner.invoke(datahub, ["check", "mce-file", f"{json_file_path}"])
    assert result.exit_code == 0


def test_reader_allows_verbose_unions(pytestconfig):
    json_filename = "test_serde_backwards_compat.json"
    test_resources_dir = pytestconfig.rootpath / "tests/unit/serde"
    json_file_path = test_resources_dir / json_filename

    runner = CliRunner()
    result = runner.invoke(datahub, ["check", "mce-file", f"{json_file_path}"])
    assert result.exit_code == 0
