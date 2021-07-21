from typing import Any, Dict, Optional

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers


class DbtTestConfig:
    def __init__(
        self,
        test_resources_dir,
        tmp_path,
        output_file: str,
        golden_file: str,
        source_config_modifiers: Optional[Dict[str, Any]] = {},
        sink_config_modifiers: Optional[Dict[str, Any]] = {},
    ):

        self.manifest_path = f"{test_resources_dir}/dbt_manifest.json"
        self.catalog_path = f"{test_resources_dir}/dbt_catalog.json"
        self.sources_path = f"{test_resources_dir}/dbt_sources.json"

        self.target_platform = "dbt"
        self.output_path = str(tmp_path / output_file)
        self.golden_path = str(test_resources_dir / golden_file)

        self.source_config = dict(
            {
                "manifest_path": self.manifest_path,
                "catalog_path": self.catalog_path,
                "sources_path": self.sources_path,
                "target_platform": self.target_platform,
            },
            **source_config_modifiers,
        )

        self.sink_config = dict(
            {
                "filename": self.output_path,
            },
            **sink_config_modifiers,
        )


def test_dbt_ingest(pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/dbt"

    config_variants = [
        DbtTestConfig(
            test_resources_dir,
            tmp_path,
            "dbt_with_schema_mces.json",
            "dbt_with_schema_mces_golden.json",
            source_config_modifiers={"load_schemas": True},
        ),
        DbtTestConfig(
            test_resources_dir,
            tmp_path,
            "dbt_without_schema_mces.json",
            "dbt_without_schema_mces_golden.json",
            source_config_modifiers={"load_schemas": False},
        ),
    ]

    for config in config_variants:

        # test manifest, catalog, sources are generated from https://github.com/kevinhu/sample-dbt
        pipeline = Pipeline.create(
            {
                "run_id": "dbt-test",
                "source": {"type": "dbt", "config": config.source_config},
                "sink": {
                    "type": "file",
                    "config": config.sink_config,
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=config.output_path,
            golden_path=config.golden_path,
        )
