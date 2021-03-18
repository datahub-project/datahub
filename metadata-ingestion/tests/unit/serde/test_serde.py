import mce_helpers
import pytest
from click.testing import CliRunner

from datahub.entrypoints import datahub
from datahub.ingestion.run.pipeline import Pipeline


@pytest.mark.parametrize(
    "json_filename",
    [
        # Normal test.
        "tests/unit/serde/test_serde_large.json",
        # Ensure correct representation of chart info's input list.
        "tests/unit/serde/test_serde_chart_snapshot.json",
    ],
)
def test_serde(pytestconfig, tmp_path, json_filename):
    golden_file = pytestconfig.rootpath / json_filename

    output_filename = "output.json"
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
    assert golden == output


@pytest.mark.parametrize(
    "json_filename",
    [
        # Normal test.
        "tests/unit/serde/test_serde_large.json",
        # Check for backwards compatability with specifying all union types.
        "tests/unit/serde/test_serde_backwards_compat.json",
        # Ensure sample MCE files are valid.
        "examples/mce_files/single_mce.json",
        "examples/mce_files/mce_list.json",
        "examples/mce_files/bootstrap_mce.json",
    ],
)
def test_check_mce_schema(pytestconfig, json_filename):
    json_file_path = pytestconfig.rootpath / json_filename

    runner = CliRunner()
    result = runner.invoke(datahub, ["check", "mce-file", f"{json_file_path}"])
    assert result.exit_code == 0
