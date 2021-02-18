import mce_helpers

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
