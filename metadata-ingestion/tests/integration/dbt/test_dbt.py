from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers


def test_dbt_ingest(pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/dbt"

    pipeline = Pipeline.create(
        {
            "run_id": "dbt-test",
            "source": {
                "type": "dbt",
                "config": {
                    "manifest_path": f"{test_resources_dir}/dbt_manifest.json",
                    "catalog_path": f"{test_resources_dir}/dbt_catalog.json",
                    "target_platform": "dbt",
                    "load_schemas": True,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/dbt_mces.json",
                },
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    output = mce_helpers.load_json_file(str(tmp_path / "dbt_mces.json"))
    golden = mce_helpers.load_json_file(
        str(test_resources_dir / "dbt_mces_golden.json")
    )
    mce_helpers.assert_mces_equal(output, golden)
