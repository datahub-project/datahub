from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers


def test_dbt_ingest(pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/dbt"

    # test manifest, catalog, sources are generated from https://github.com/kevinhu/sample-dbt
    pipeline = Pipeline.create(
        {
            "run_id": "dbt-test",
            "source": {
                "type": "dbt",
                "config": {
                    "manifest_path": f"{test_resources_dir}/dbt_manifest.json",
                    "catalog_path": f"{test_resources_dir}/dbt_catalog.json",
                    "sources_path": f"{test_resources_dir}/dbt_sources.json",
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

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "dbt_mces.json",
        golden_path=test_resources_dir / "dbt_mces_golden.json",
    )
