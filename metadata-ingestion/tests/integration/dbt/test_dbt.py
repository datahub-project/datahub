from os import PathLike
from typing import Any, Dict, Optional, Union

import pytest
import requests_mock

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers


class DbtTestConfig:
    def __init__(
        self,
        run_id: str,
        dbt_metadata_uri_prefix: str,
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

        self.manifest_path = f"{dbt_metadata_uri_prefix}/dbt_manifest.json"
        self.catalog_path = f"{dbt_metadata_uri_prefix}/dbt_catalog.json"
        self.sources_path = f"{dbt_metadata_uri_prefix}/dbt_sources.json"
        self.target_platform = "postgres"

        self.output_path = f"{tmp_path}/{output_file}"

        self.golden_path = f"{test_resources_dir}/{golden_file}"
        self.source_config = dict(
            {
                "manifest_path": self.manifest_path,
                "catalog_path": self.catalog_path,
                "sources_path": self.sources_path,
                "target_platform": self.target_platform,
                "enable_meta_mapping": False,
                "write_semantics": "OVERRIDE",
                "meta_mapping": {
                    "business_owner": {
                        "match": ".*",
                        "operation": "add_owner",
                        "config": {"owner_type": "user"},
                    },
                    "has_pii": {
                        "match": True,
                        "operation": "add_tag",
                        "config": {"tag": "has_pii_test"},
                    },
                    "int_property": {
                        "match": 1,
                        "operation": "add_tag",
                        "config": {"tag": "int_meta_property"},
                    },
                    "double_property": {
                        "match": 2.5,
                        "operation": "add_term",
                        "config": {"term": "double_meta_property"},
                    },
                    "data_governance.team_owner": {
                        "match": "Finance",
                        "operation": "add_term",
                        "config": {"term": "Finance_test"},
                    },
                },
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
@requests_mock.Mocker(kw="req_mock")
def test_dbt_ingest(pytestconfig, tmp_path, mock_time, **kwargs):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/dbt"

    with open(test_resources_dir / "dbt_manifest.json", "r") as f:
        kwargs["req_mock"].get(
            "http://some-external-repo/dbt_manifest.json", text=f.read()
        )

    with open(test_resources_dir / "dbt_catalog.json", "r") as f:
        kwargs["req_mock"].get(
            "http://some-external-repo/dbt_catalog.json", text=f.read()
        )

    with open(test_resources_dir / "dbt_sources.json", "r") as f:
        kwargs["req_mock"].get(
            "http://some-external-repo/dbt_sources.json", text=f.read()
        )

    config_variants = [
        DbtTestConfig(
            "dbt-test-with-schemas",
            test_resources_dir,
            test_resources_dir,
            tmp_path,
            "dbt_with_schemas_mces.json",
            "dbt_with_schemas_mces_golden.json",
            source_config_modifiers={
                "load_schemas": True,
                "disable_dbt_node_creation": True,
                "enable_meta_mapping": True,
            },
        ),
        DbtTestConfig(
            "dbt-test-with-external-metadata-files",
            "http://some-external-repo",
            test_resources_dir,
            tmp_path,
            "dbt_with_external_metadata_files_mces.json",
            "dbt_with_external_metadata_files_mces_golden.json",
            source_config_modifiers={
                "load_schemas": True,
                "disable_dbt_node_creation": True,
            },
        ),
        DbtTestConfig(
            "dbt-test-without-schemas",
            test_resources_dir,
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
            test_resources_dir,
            tmp_path,
            "dbt_enabled_with_schemas_mces.json",
            "dbt_enabled_with_schemas_mces_golden.json",
            source_config_modifiers={"load_schemas": True, "enable_meta_mapping": True},
        ),
        DbtTestConfig(
            "dbt-test-without-schemas-dbt-enabled",
            test_resources_dir,
            test_resources_dir,
            tmp_path,
            "dbt_enabled_without_schemas_mces.json",
            "dbt_enabled_without_schemas_mces_golden.json",
            source_config_modifiers={"load_schemas": False},
        ),
        DbtTestConfig(
            "dbt-test-without-schemas-with-filter-dbt-enabled",
            test_resources_dir,
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
