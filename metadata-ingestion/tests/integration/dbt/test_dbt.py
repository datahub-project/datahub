import dataclasses
from dataclasses import dataclass
from os import PathLike
from typing import Any, Dict, List, Union

import pytest
from freezegun import freeze_time

from datahub.configuration.common import DynamicTypedConfig
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig
from datahub.ingestion.source.dbt.dbt_common import DBTEntitiesEnabled, EmitDirective
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource
from tests.test_helpers import mce_helpers, test_connection_helpers

FROZEN_TIME = "2022-02-03 07:00:00"
GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"

_default_dbt_source_args = {
    # Needed to avoid needing to access datahub server.
    "write_semantics": "OVERRIDE",
}


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    # TODO: Move this into a constant based on __file__.
    return pytestconfig.rootpath / "tests/integration/dbt"


@dataclass
class DbtTestConfig:
    run_id: str
    output_file: Union[str, PathLike]
    golden_file: Union[str, PathLike]
    manifest_file: str = "dbt_manifest.json"
    catalog_file: str = "dbt_catalog.json"
    sources_file: str = "dbt_sources.json"
    run_results_files: List[str] = dataclasses.field(default_factory=list)
    source_config_modifiers: Dict[str, Any] = dataclasses.field(default_factory=dict)
    sink_config_modifiers: Dict[str, Any] = dataclasses.field(default_factory=dict)

    def set_paths(
        self,
        dbt_metadata_uri_prefix: PathLike,
        test_resources_dir: PathLike,
        tmp_path: PathLike,
    ) -> None:
        manifest_path = f"{dbt_metadata_uri_prefix}/{self.manifest_file}"
        catalog_path = f"{dbt_metadata_uri_prefix}/{self.catalog_file}"
        sources_path = f"{dbt_metadata_uri_prefix}/{self.sources_file}"
        run_results_paths = [
            f"{dbt_metadata_uri_prefix}/{file}" for file in self.run_results_files
        ]
        target_platform = "postgres"

        self.output_path = f"{tmp_path}/{self.output_file}"

        self.golden_path = f"{test_resources_dir}/{self.golden_file}"
        self.source_config = dict(
            {
                "manifest_path": manifest_path,
                "catalog_path": catalog_path,
                "sources_path": sources_path,
                "run_results_paths": run_results_paths,
                "target_platform": target_platform,
                "enable_meta_mapping": False,
                **_default_dbt_source_args,
                "meta_mapping": {
                    "owner": {
                        "match": "^@(.*)",
                        "operation": "add_owner",
                        "config": {"owner_type": "user"},
                    },
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
                "query_tag_mapping": {
                    "tag": {
                        "match": ".*",
                        "operation": "add_tag",
                        "config": {"tag": "{{ $match }}"},
                    }
                },
            },
            **self.source_config_modifiers,
        )

        self.sink_config = dict(
            {
                "filename": self.output_path,
            },
            **self.sink_config_modifiers,
        )


@pytest.mark.parametrize(
    # test manifest, catalog, sources are generated from https://github.com/kevinhu/sample-dbt
    "dbt_test_config",
    [
        DbtTestConfig(
            "dbt-test-with-schemas-dbt-enabled",
            "dbt_enabled_with_schemas_mces.json",
            "dbt_enabled_with_schemas_mces_golden.json",
            source_config_modifiers={
                "enable_meta_mapping": True,
                "owner_extraction_pattern": r"^@(?P<owner>(.*))",
            },
        ),
        DbtTestConfig(
            "dbt-test-with-complex-owner-patterns",
            "dbt_test_with_complex_owner_patterns_mces.json",
            "dbt_test_with_complex_owner_patterns_mces_golden.json",
            manifest_file="dbt_manifest_complex_owner_patterns.json",
            source_config_modifiers={
                "node_name_pattern": {
                    "deny": ["source.sample_dbt.pagila.payment_p2020_06"]
                },
                "owner_extraction_pattern": "(.*)(?P<owner>(?<=\\().*?(?=\\)))",
                "strip_user_ids_from_email": True,
            },
        ),
        DbtTestConfig(
            "dbt-test-with-data-platform-instance",
            "dbt_test_with_data_platform_instance_mces.json",
            "dbt_test_with_data_platform_instance_mces_golden.json",
            source_config_modifiers={
                "platform_instance": "dbt-instance-1",
            },
        ),
        DbtTestConfig(
            "dbt-test-with-non-incremental-lineage",
            "dbt_test_with_non_incremental_lineage_mces.json",
            "dbt_test_with_non_incremental_lineage_mces_golden.json",
            source_config_modifiers={
                "incremental_lineage": "False",
            },
        ),
        DbtTestConfig(
            "dbt-test-with-target-platform-instance",
            "dbt_test_with_target_platform_instance_mces.json",
            "dbt_test_with_target_platform_instance_mces_golden.json",
            source_config_modifiers={
                "target_platform_instance": "ps-instance-1",
            },
        ),
        DbtTestConfig(
            "dbt-column-meta-mapping",  # this also tests snapshot support
            "dbt_test_column_meta_mapping.json",
            "dbt_test_column_meta_mapping_golden.json",
            catalog_file="sample_dbt_catalog_1.json",
            manifest_file="sample_dbt_manifest_1.json",
            sources_file="sample_dbt_sources_1.json",
            source_config_modifiers={
                "enable_meta_mapping": True,
                "column_meta_mapping": {
                    "terms": {
                        "match": ".*",
                        "operation": "add_terms",
                        "config": {"separator": ","},
                    },
                    "is_sensitive": {
                        "match": True,
                        "operation": "add_tag",
                        "config": {"tag": "sensitive"},
                    },
                    "maturity": {
                        "match": ".*",
                        "operation": "add_term",
                        "config": {"term": "maturity_{{ $match }}"},
                    },
                },
                "entities_enabled": {
                    "test_definitions": "NO",
                    "test_results": "NO",
                },
            },
        ),
        DbtTestConfig(
            "dbt-model-performance",
            "dbt_test_model_performance.json",
            "dbt_test_test_model_performance_golden.json",
            catalog_file="sample_dbt_catalog_2.json",
            manifest_file="sample_dbt_manifest_2.json",
            sources_file="sample_dbt_sources_2.json",
            run_results_files=["sample_dbt_run_results_2.json"],
            source_config_modifiers={},
        ),
        DbtTestConfig(
            "dbt-prefer-sql-parser-lineage",
            "dbt_test_prefer_sql_parser_lineage.json",
            "dbt_test_prefer_sql_parser_lineage_golden.json",
            catalog_file="sample_dbt_catalog_2.json",
            manifest_file="sample_dbt_manifest_2.json",
            sources_file="sample_dbt_sources_2.json",
            run_results_files=["sample_dbt_run_results_2.json"],
            source_config_modifiers={
                "prefer_sql_parser_lineage": True,
                "skip_sources_in_lineage": True,
                # "entities_enabled": {"sources": "NO"},
            },
        ),
    ],
    ids=lambda dbt_test_config: dbt_test_config.run_id,
)
@pytest.mark.integration
@freeze_time(FROZEN_TIME)
def test_dbt_ingest(
    dbt_test_config,
    test_resources_dir,
    pytestconfig,
    tmp_path,
    mock_time,
    requests_mock,
):
    config: DbtTestConfig = dbt_test_config
    test_resources_dir = pytestconfig.rootpath / "tests/integration/dbt"

    with open(test_resources_dir / "dbt_manifest.json") as f:
        requests_mock.get("http://some-external-repo/dbt_manifest.json", text=f.read())

    with open(test_resources_dir / "dbt_catalog.json") as f:
        requests_mock.get("http://some-external-repo/dbt_catalog.json", text=f.read())

    with open(test_resources_dir / "dbt_sources.json") as f:
        requests_mock.get("http://some-external-repo/dbt_sources.json", text=f.read())

    config.set_paths(
        dbt_metadata_uri_prefix=test_resources_dir,
        test_resources_dir=test_resources_dir,
        tmp_path=tmp_path,
    )

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


@pytest.mark.parametrize(
    "config_dict, is_success",
    [
        (
            {
                "manifest_path": "dbt_manifest.json",
                "catalog_path": "dbt_catalog.json",
                "target_platform": "postgres",
            },
            True,
        ),
        (
            {
                "manifest_path": "dbt_manifest.json",
                "catalog_path": "dbt_catalog-this-file-does-not-exist.json",
                "target_platform": "postgres",
            },
            False,
        ),
    ],
)
@pytest.mark.integration
@freeze_time(FROZEN_TIME)
def test_dbt_test_connection(test_resources_dir, config_dict, is_success):
    config_dict["manifest_path"] = str(
        (test_resources_dir / config_dict["manifest_path"]).resolve()
    )
    config_dict["catalog_path"] = str(
        (test_resources_dir / config_dict["catalog_path"]).resolve()
    )
    report = test_connection_helpers.run_test_connection(DBTCoreSource, config_dict)
    if is_success:
        test_connection_helpers.assert_basic_connectivity_success(report)
    else:
        test_connection_helpers.assert_basic_connectivity_failure(
            report, "No such file or directory"
        )


@pytest.mark.integration
@freeze_time(FROZEN_TIME)
def test_dbt_tests(test_resources_dir, pytestconfig, tmp_path, mock_time, **kwargs):
    # Run the metadata ingestion pipeline.
    output_file = tmp_path / "dbt_test_events.json"
    golden_path = test_resources_dir / "dbt_test_events_golden.json"

    pipeline = Pipeline(
        config=PipelineConfig(
            source=SourceConfig(
                type="dbt",
                config=DBTCoreConfig(
                    **_default_dbt_source_args,
                    manifest_path=str(
                        (test_resources_dir / "jaffle_shop_manifest.json").resolve()
                    ),
                    catalog_path=str(
                        (test_resources_dir / "jaffle_shop_catalog.json").resolve()
                    ),
                    target_platform="postgres",
                    run_results_paths=[
                        str(
                            (
                                test_resources_dir / "jaffle_shop_test_results.json"
                            ).resolve()
                        )
                    ],
                ),
            ),
            sink=DynamicTypedConfig(type="file", config={"filename": str(output_file)}),
        )
    )
    pipeline.run()
    pipeline.raise_from_status()
    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file,
        golden_path=golden_path,
        ignore_paths=[],
    )


@pytest.mark.integration
@freeze_time(FROZEN_TIME)
def test_dbt_tests_only_assertions(
    test_resources_dir, pytestconfig, tmp_path, mock_time, **kwargs
):
    # Run the metadata ingestion pipeline.
    output_file = tmp_path / "test_only_assertions.json"

    pipeline = Pipeline(
        config=PipelineConfig(
            source=SourceConfig(
                type="dbt",
                config=DBTCoreConfig(
                    **_default_dbt_source_args,
                    manifest_path=str(
                        (test_resources_dir / "jaffle_shop_manifest.json").resolve()
                    ),
                    catalog_path=str(
                        (test_resources_dir / "jaffle_shop_catalog.json").resolve()
                    ),
                    target_platform="postgres",
                    run_results_paths=[
                        str(
                            (
                                test_resources_dir / "jaffle_shop_test_results.json"
                            ).resolve()
                        )
                    ],
                    entities_enabled=DBTEntitiesEnabled(
                        test_results=EmitDirective.ONLY
                    ),
                ),
            ),
            sink=DynamicTypedConfig(type="file", config={"filename": str(output_file)}),
        )
    )
    pipeline.run()
    pipeline.raise_from_status()
    # Verify the output.
    # No datasets were emitted, and more than 20 events were emitted
    assert (
        mce_helpers.assert_entity_urn_not_like(
            entity_type="dataset",
            regex_pattern="urn:li:dataset:\\(urn:li:dataPlatform:dbt",
            file=output_file,
        )
        > 20
    )
    number_of_valid_assertions_in_test_results = 24
    assert (
        mce_helpers.assert_entity_urn_like(
            entity_type="assertion", regex_pattern="urn:li:assertion:", file=output_file
        )
        == number_of_valid_assertions_in_test_results
    )

    # no assertionInfo should be emitted
    with pytest.raises(
        AssertionError, match="Failed to find aspect_name assertionInfo for urns"
    ):
        mce_helpers.assert_for_each_entity(
            entity_type="assertion",
            aspect_name="assertionInfo",
            aspect_field_matcher={},
            file=output_file,
        )

    # all assertions must have an assertionRunEvent emitted (except for one assertion)
    assert (
        mce_helpers.assert_for_each_entity(
            entity_type="assertion",
            aspect_name="assertionRunEvent",
            aspect_field_matcher={},
            file=output_file,
            exception_urns=["urn:li:assertion:2ff754df689ea951ed2e12cbe356708f"],
        )
        == number_of_valid_assertions_in_test_results
    )


@pytest.mark.integration
@freeze_time(FROZEN_TIME)
def test_dbt_only_test_definitions_and_results(
    test_resources_dir, pytestconfig, tmp_path, mock_time, **kwargs
):
    # Run the metadata ingestion pipeline.
    output_file = tmp_path / "test_only_definitions_and_assertions.json"

    pipeline = Pipeline(
        config=PipelineConfig(
            source=SourceConfig(
                type="dbt",
                config=DBTCoreConfig(
                    **_default_dbt_source_args,
                    manifest_path=str(
                        (test_resources_dir / "jaffle_shop_manifest.json").resolve()
                    ),
                    catalog_path=str(
                        (test_resources_dir / "jaffle_shop_catalog.json").resolve()
                    ),
                    target_platform="postgres",
                    run_results_paths=[
                        str(
                            (
                                test_resources_dir / "jaffle_shop_test_results.json"
                            ).resolve()
                        )
                    ],
                    entities_enabled=DBTEntitiesEnabled(
                        sources=EmitDirective.NO,
                        seeds=EmitDirective.NO,
                        models=EmitDirective.NO,
                    ),
                ),
            ),
            sink=DynamicTypedConfig(type="file", config={"filename": str(output_file)}),
        )
    )
    pipeline.run()
    pipeline.raise_from_status()
    # Verify the output. No datasets were emitted
    assert (
        mce_helpers.assert_entity_urn_not_like(
            entity_type="dataset",
            regex_pattern="urn:li:dataset:\\(urn:li:dataPlatform:dbt",
            file=output_file,
        )
        > 20
    )
    number_of_assertions = 25
    assert (
        mce_helpers.assert_entity_urn_like(
            entity_type="assertion", regex_pattern="urn:li:assertion:", file=output_file
        )
        == number_of_assertions
    )
    # all assertions must have an assertionInfo emitted
    assert (
        mce_helpers.assert_for_each_entity(
            entity_type="assertion",
            aspect_name="assertionInfo",
            aspect_field_matcher={},
            file=output_file,
        )
        == number_of_assertions
    )
    # all assertions must have an assertionRunEvent emitted (except for one assertion)
    assert (
        mce_helpers.assert_for_each_entity(
            entity_type="assertion",
            aspect_name="assertionRunEvent",
            aspect_field_matcher={},
            file=output_file,
            exception_urns=["urn:li:assertion:2ff754df689ea951ed2e12cbe356708f"],
        )
        == number_of_assertions - 1
    )
