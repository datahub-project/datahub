from os import PathLike
from typing import Any, Dict, Optional, Union

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers


class DbtTestConfig:
    def __init__(
        self,
        run_id: str,
        test_resources_dir: Union[str, PathLike],
        tmp_path: Union[str, PathLike],
        output_file: Union[str, PathLike],
        golden_file: Union[str, PathLike],
        source_config_modifiers: Optional[Dict[str, Any]] = None,
        sink_config_modifiers: Optional[Dict[str, Any]] = None,
    ):

        if source_config_modifiers is None:
            source_config_modifiers = {}

        if sink_config_modifiers is None:
            sink_config_modifiers = {}

        self.run_id = run_id

        self.manifest_path = f"{test_resources_dir}/dbt_manifest.json"
        self.catalog_path = f"{test_resources_dir}/dbt_catalog.json"
        self.sources_path = f"{test_resources_dir}/dbt_sources.json"
        self.target_platform = "postgres"

        self.output_path = f"{tmp_path}/{output_file}"

        self.golden_path = f"{test_resources_dir}/{golden_file}"

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


@pytest.mark.integration
def test_dbt_ingest(pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/dbt"

    config_variants = [
        DbtTestConfig(
            "dbt-test-with-schemas",
            test_resources_dir,
            tmp_path,
            "dbt_with_schemas_mces.json",
            "dbt_with_schemas_mces_golden.json",
            source_config_modifiers={
                "load_schemas": True,
                "disable_dbt_node_creation": True,
            },
        ),
        DbtTestConfig(
            "dbt-test-without-schemas",
            test_resources_dir,
            tmp_path,
            "dbt_without_schemas_mces.json",
            "dbt_without_schemas_mces_golden.json",
            source_config_modifiers={
                "load_schemas": False,
                "disable_dbt_node_creation": True,
            },
        ),
        DbtTestConfig(
            "dbt-test-without-schemas-with-filter",
            test_resources_dir,
            tmp_path,
            "dbt_without_schemas_with_filter_mces.json",
            "dbt_without_schemas_with_filter_mces_golden.json",
            source_config_modifiers={
                "load_schemas": False,
                "node_name_pattern": {
                    "deny": ["source.sample_dbt.pagila.payment_p2020_06"]
                },
                "disable_dbt_node_creation": True,
            },
        ),
        DbtTestConfig(
            "dbt-test-with-schemas-dbt-enabled",
            test_resources_dir,
            tmp_path,
            "dbt_enabled_with_schemas_mces.json",
            "dbt_enabled_with_schemas_mces_golden.json",
            source_config_modifiers={"load_schemas": True},
        ),
        DbtTestConfig(
            "dbt-test-without-schemas-dbt-enabled",
            test_resources_dir,
            tmp_path,
            "dbt_enabled_without_schemas_mces.json",
            "dbt_enabled_without_schemas_mces_golden.json",
            source_config_modifiers={"load_schemas": False},
        ),
        DbtTestConfig(
            "dbt-test-without-schemas-with-filter-dbt-enabled",
            test_resources_dir,
            tmp_path,
            "dbt_enabled_without_schemas_with_filter_mces.json",
            "dbt_enabled_without_schemas_with_filter_mces_golden.json",
            source_config_modifiers={
                "load_schemas": False,
                "node_name_pattern": {
                    "deny": ["source.sample_dbt.pagila.payment_p2020_06"]
                },
            },
        ),
    ]

    for config in config_variants:

        # test manifest, catalog, sources are generated from https://github.com/kevinhu/sample-dbt
        pipeline = Pipeline.create(
            {
                "run_id": config.run_id,
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
