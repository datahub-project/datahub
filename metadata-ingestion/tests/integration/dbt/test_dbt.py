from os import PathLike
from typing import Any, Dict, Optional, Type, Union, cast
from unittest.mock import patch

import pytest
import requests_mock
from freezegun import freeze_time

from datahub.configuration.common import DynamicTypedConfig
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.run.pipeline_config import PipelineConfig, SourceConfig
from datahub.ingestion.source.dbt import (
    DBTConfig,
    DBTEntitiesEnabled,
    DBTSource,
    EmitDirective,
)
from datahub.ingestion.source.sql.sql_types import (
    TRINO_SQL_TYPES_MAP,
    resolve_trino_modified_type,
)
from datahub.ingestion.source.state.checkpoint import Checkpoint, CheckpointStateBase
from datahub.ingestion.source.state.dbt_state import DbtCheckpointState
from datahub.ingestion.source.state.sql_common_state import (
    BaseSQLAlchemyCheckpointState,
)
from tests.test_helpers import mce_helpers
from tests.test_helpers.state_helpers import (
    run_and_get_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2022-02-03 07:00:00"
GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"


class DbtTestConfig:
    def __init__(
        self,
        run_id: str,
        dbt_metadata_uri_prefix: str,
        test_resources_dir: Union[str, PathLike],
        tmp_path: Union[str, PathLike],
        output_file: Union[str, PathLike],
        golden_file: Union[str, PathLike],
        manifest_file: str = "dbt_manifest.json",
        source_config_modifiers: Optional[Dict[str, Any]] = None,
        sink_config_modifiers: Optional[Dict[str, Any]] = None,
    ):
        if source_config_modifiers is None:
            source_config_modifiers = {}

        if sink_config_modifiers is None:
            sink_config_modifiers = {}

        self.run_id = run_id

        self.manifest_path = f"{dbt_metadata_uri_prefix}/{manifest_file}"
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
                "owner_extraction_pattern": r"^@(?P<owner>(.*))",
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
                "owner_extraction_pattern": r"^@(?P<owner>(.*))",
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
                "owner_extraction_pattern": r"^@(?P<owner>(.*))",
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
                "owner_extraction_pattern": r"^@(?P<owner>(.*))",
            },
        ),
        DbtTestConfig(
            "dbt-test-with-schemas-dbt-enabled",
            test_resources_dir,
            test_resources_dir,
            tmp_path,
            "dbt_enabled_with_schemas_mces.json",
            "dbt_enabled_with_schemas_mces_golden.json",
            source_config_modifiers={
                "load_schemas": True,
                "enable_meta_mapping": True,
                "owner_extraction_pattern": r"^@(?P<owner>(.*))",
            },
        ),
        DbtTestConfig(
            "dbt-test-without-schemas-dbt-enabled",
            test_resources_dir,
            test_resources_dir,
            tmp_path,
            "dbt_enabled_without_schemas_mces.json",
            "dbt_enabled_without_schemas_mces_golden.json",
            source_config_modifiers={
                "load_schemas": False,
                "owner_extraction_pattern": r"^@(?P<owner>(.*))",
            },
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
                "owner_extraction_pattern": r"^@(?P<owner>(.*))",
            },
        ),
        DbtTestConfig(
            "dbt-test-with-complex-owner-patterns",
            test_resources_dir,
            test_resources_dir,
            tmp_path,
            "dbt_test_with_complex_owner_patterns_mces.json",
            "dbt_test_with_complex_owner_patterns_mces_golden.json",
            manifest_file="dbt_manifest_complex_owner_patterns.json",
            source_config_modifiers={
                "load_schemas": False,
                "node_name_pattern": {
                    "deny": ["source.sample_dbt.pagila.payment_p2020_06"]
                },
                "owner_extraction_pattern": "(.*)(?P<owner>(?<=\\().*?(?=\\)))",
                "strip_user_ids_from_email": True,
            },
        ),
        DbtTestConfig(
            "dbt-test-with-data-platform-instance",
            test_resources_dir,
            test_resources_dir,
            tmp_path,
            "dbt_test_with_data_platform_instance_mces.json",
            "dbt_test_with_data_platform_instance_mces_golden.json",
            source_config_modifiers={
                "load_schemas": True,
                "disable_dbt_node_creation": False,
                "platform_instance": "dbt-instance-1",
            },
        ),
        DbtTestConfig(
            "dbt-test-with-target-platform-instance",
            test_resources_dir,
            test_resources_dir,
            tmp_path,
            "dbt_test_with_target_platform_instance_mces.json",
            "dbt_test_with_target_platform_instance_mces_golden.json",
            source_config_modifiers={
                "load_schemas": True,
                "target_platform_instance": "ps-instance-1",
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


def get_current_checkpoint_from_pipeline(
    pipeline: Pipeline,
) -> Optional[Checkpoint]:
    dbt_source = cast(DBTSource, pipeline.source)
    return dbt_source.get_current_checkpoint(dbt_source.get_default_ingestion_job_id())


@pytest.mark.integration
@freeze_time(FROZEN_TIME)
def test_dbt_stateful(pytestconfig, tmp_path, mock_time, mock_datahub_graph):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/dbt"

    manifest_path = "{}/dbt_manifest.json".format(test_resources_dir)
    catalog_path = "{}/dbt_catalog.json".format(test_resources_dir)
    sources_path = "{}/dbt_sources.json".format(test_resources_dir)

    manifest_path_deleted_actor = "{}/dbt_manifest_deleted_actor.json".format(
        test_resources_dir
    )
    catalog_path_deleted_actor = "{}/dbt_catalog_deleted_actor.json".format(
        test_resources_dir
    )
    sources_path_deleted_actor = "{}/dbt_sources_deleted_actor.json".format(
        test_resources_dir
    )

    deleted_actor_golden_mcs = "{}/dbt_deleted_actor_mces_golden.json".format(
        test_resources_dir
    )

    stateful_config = {
        "stateful_ingestion": {
            "enabled": True,
            "remove_stale_metadata": True,
            "state_provider": {
                "type": "datahub",
                "config": {"datahub_api": {"server": GMS_SERVER}},
            },
        },
    }

    scd_before_deletion: Dict[str, Any] = {
        "manifest_path": manifest_path,
        "catalog_path": catalog_path,
        "sources_path": sources_path,
        "target_platform": "postgres",
        "load_schemas": True,
        # This will bypass check in get_workunits function of dbt.py
        "write_semantics": "OVERRIDE",
        "owner_extraction_pattern": r"^@(?P<owner>(.*))",
        # enable stateful ingestion
        **stateful_config,
    }

    scd_after_deletion: Dict[str, Any] = {
        "manifest_path": manifest_path_deleted_actor,
        "catalog_path": catalog_path_deleted_actor,
        "sources_path": sources_path_deleted_actor,
        "target_platform": "postgres",
        "load_schemas": True,
        "write_semantics": "OVERRIDE",
        "owner_extraction_pattern": r"^@(?P<owner>(.*))",
        # enable stateful ingestion
        **stateful_config,
    }

    pipeline_config_dict: Dict[str, Any] = {
        "source": {
            "type": "dbt",
            "config": scd_before_deletion,
        },
        "sink": {
            # we are not really interested in the resulting events for this test
            "type": "console"
        },
        "pipeline_name": "statefulpipeline",
    }

    with patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        mock_checkpoint.return_value = mock_datahub_graph

        # Do the first run of the pipeline and get the default job's checkpoint.
        pipeline_run1 = run_and_get_pipeline(pipeline_config_dict)
        checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)

        assert checkpoint1
        assert checkpoint1.state

        # Set dbt config where actor table is deleted.
        pipeline_config_dict["source"]["config"] = scd_after_deletion
        # Capture MCEs of second run to validate Status(removed=true)
        deleted_mces_path = "{}/{}".format(tmp_path, "dbt_deleted_mces.json")
        pipeline_config_dict["sink"]["type"] = "file"
        pipeline_config_dict["sink"]["config"] = {"filename": deleted_mces_path}

        # Do the second run of the pipeline.
        pipeline_run2 = run_and_get_pipeline(pipeline_config_dict)
        checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)

        assert checkpoint2
        assert checkpoint2.state

        # Perform all assertions on the states. The deleted table should not be
        # part of the second state
        state1 = cast(DbtCheckpointState, checkpoint1.state)
        state2 = cast(DbtCheckpointState, checkpoint2.state)
        difference_urns = list(state1.get_node_urns_not_in(state2))

        assert len(difference_urns) == 2

        urn1 = "urn:li:dataset:(urn:li:dataPlatform:dbt,pagila.public.actor,PROD)"
        urn2 = "urn:li:dataset:(urn:li:dataPlatform:postgres,pagila.public.actor,PROD)"

        assert urn1 in difference_urns
        assert urn2 in difference_urns

        # Validate that all providers have committed successfully.
        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run1, expected_providers=1
        )
        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run2, expected_providers=1
        )

        # Validate against golden MCEs where Status(removed=true)
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=deleted_mces_path,
            golden_path=deleted_actor_golden_mcs,
        )


@pytest.mark.integration
@freeze_time(FROZEN_TIME)
def test_dbt_state_backward_compatibility(
    pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/dbt"
    manifest_path = f"{test_resources_dir}/dbt_manifest.json"
    catalog_path = f"{test_resources_dir}/dbt_catalog.json"
    sources_path = f"{test_resources_dir}/dbt_sources.json"

    stateful_config: Dict[str, Any] = {
        "stateful_ingestion": {
            "enabled": True,
            "remove_stale_metadata": True,
            "state_provider": {
                "type": "datahub",
                "config": {"datahub_api": {"server": GMS_SERVER}},
            },
        },
    }

    scd_config: Dict[str, Any] = {
        "manifest_path": manifest_path,
        "catalog_path": catalog_path,
        "sources_path": sources_path,
        "target_platform": "postgres",
        "load_schemas": True,
        # This will bypass check in get_workunits function of dbt.py
        "write_semantics": "OVERRIDE",
        "owner_extraction_pattern": r"^@(?P<owner>(.*))",
        # enable stateful ingestion
        **stateful_config,
    }

    pipeline_config_dict: Dict[str, Any] = {
        "source": {
            "type": "dbt",
            "config": scd_config,
        },
        "sink": {
            # we are not really interested in the resulting events for this test
            "type": "console"
        },
        "pipeline_name": "statefulpipeline",
    }

    with patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        mock_checkpoint.return_value = mock_datahub_graph
        pipeline = Pipeline.create(pipeline_config_dict)
        dbt_source = cast(DBTSource, pipeline.source)

        def get_fake_base_sql_alchemy_checkpoint_state(
            job_id: JobId, checkpoint_state_class: Type[CheckpointStateBase]
        ) -> Optional[Checkpoint]:
            if checkpoint_state_class is DbtCheckpointState:
                raise Exception(
                    "DBT source will call this function again with BaseSQLAlchemyCheckpointState"
                )

            sql_state = BaseSQLAlchemyCheckpointState()
            urn1 = "urn:li:dataset:(urn:li:dataPlatform:dbt,pagila.public.actor,PROD)"
            urn2 = (
                "urn:li:dataset:(urn:li:dataPlatform:postgres,pagila.public.actor,PROD)"
            )
            sql_state.add_table_urn(urn1)
            sql_state.add_table_urn(urn2)

            assert dbt_source.ctx.pipeline_name is not None

            return Checkpoint(
                job_name=dbt_source.get_default_ingestion_job_id(),
                pipeline_name=dbt_source.ctx.pipeline_name,
                platform_instance_id=dbt_source.get_platform_instance_id(),
                run_id=dbt_source.ctx.run_id,
                config=dbt_source.config,
                state=sql_state,
            )

        # Set fake method to return BaseSQLAlchemyCheckpointState
        dbt_source.get_last_checkpoint = get_fake_base_sql_alchemy_checkpoint_state  # type: ignore[assignment]
        last_checkpoint = dbt_source.get_last_dbt_checkpoint(
            dbt_source.get_default_ingestion_job_id(), DbtCheckpointState
        )
        # Our fake method is returning BaseSQLAlchemyCheckpointState,however it should get converted to DbtCheckpointState
        assert last_checkpoint is not None and isinstance(
            last_checkpoint.state, DbtCheckpointState
        )

        pipeline.run()
        pipeline.raise_from_status()


@pytest.mark.integration
@freeze_time(FROZEN_TIME)
def test_dbt_tests(pytestconfig, tmp_path, mock_time, **kwargs):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/dbt"

    # Run the metadata ingestion pipeline.
    output_file = tmp_path / "dbt_test_events.json"
    golden_path = test_resources_dir / "dbt_test_events_golden.json"

    pipeline = Pipeline(
        config=PipelineConfig(
            source=SourceConfig(
                type="dbt",
                config=DBTConfig(
                    manifest_path=str(
                        (test_resources_dir / "jaffle_shop_manifest.json").resolve()
                    ),
                    catalog_path=str(
                        (test_resources_dir / "jaffle_shop_catalog.json").resolve()
                    ),
                    target_platform="postgres",
                    test_results_path=str(
                        (test_resources_dir / "jaffle_shop_test_results.json").resolve()
                    ),
                    # this is just here to avoid needing to access datahub server
                    write_semantics="OVERRIDE",
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
def test_dbt_stateful_tests(pytestconfig, tmp_path, mock_time, mock_datahub_graph):

    test_resources_dir = pytestconfig.rootpath / "tests/integration/dbt"
    output_file = tmp_path / "dbt_stateful_tests.json"
    golden_path = test_resources_dir / "dbt_stateful_tests_golden.json"
    manifest_path = str((test_resources_dir / "jaffle_shop_manifest.json").resolve())
    catalog_path = str((test_resources_dir / "jaffle_shop_catalog.json").resolve())
    test_results_path = str(
        (test_resources_dir / "jaffle_shop_test_results.json").resolve()
    )

    stateful_config = {
        "stateful_ingestion": {
            "enabled": True,
            "remove_stale_metadata": True,
            "state_provider": {
                "type": "datahub",
                "config": {"datahub_api": {"server": GMS_SERVER}},
            },
        },
    }

    scd: Dict[str, Any] = {
        "manifest_path": manifest_path,
        "catalog_path": catalog_path,
        "test_results_path": test_results_path,
        "target_platform": "postgres",
        "load_schemas": True,
        # This will bypass check in get_workunits function of dbt.py
        "write_semantics": "OVERRIDE",
        "owner_extraction_pattern": r"^@(?P<owner>(.*))",
        # enable stateful ingestion
        **stateful_config,
    }

    pipeline_config_dict: Dict[str, Any] = {
        "source": {
            "type": "dbt",
            "config": scd,
        },
        "sink": {
            # we are not really interested in the resulting events for this test
            "type": "file",
            "config": {"filename": str(output_file)},
        },
        "pipeline_name": "statefulpipeline",
        "run_id": "test_pipeline",
    }

    with patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        mock_checkpoint.return_value = mock_datahub_graph
        pipeline = Pipeline.create(pipeline_config_dict)
        pipeline.run()
        pipeline.raise_from_status()
        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=output_file,
            golden_path=golden_path,
            ignore_paths=[],
        )


@pytest.mark.parametrize(
    "data_type, expected_data_type",
    [
        ("boolean", "boolean"),
        ("tinyint", "tinyint"),
        ("smallint", "smallint"),
        ("int", "int"),
        ("integer", "integer"),
        ("bigint", "bigint"),
        ("real", "real"),
        ("double", "double"),
        ("decimal(10,0)", "decimal"),
        ("varchar(20)", "varchar"),
        ("char", "char"),
        ("varbinary", "varbinary"),
        ("json", "json"),
        ("date", "date"),
        ("time", "time"),
        ("time(12)", "time"),
        ("timestamp", "timestamp"),
        ("timestamp(3)", "timestamp"),
        ("row(x bigint, y double)", "row"),
        ("array(row(x bigint, y double))", "array"),
        ("map(varchar, varchar)", "map"),
    ],
)
def test_resolve_trino_modified_type(data_type, expected_data_type):
    assert (
        resolve_trino_modified_type(data_type)
        == TRINO_SQL_TYPES_MAP[expected_data_type]
    )


@pytest.mark.integration
@freeze_time(FROZEN_TIME)
def test_dbt_tests_only_assertions(pytestconfig, tmp_path, mock_time, **kwargs):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/dbt"

    # Run the metadata ingestion pipeline.
    output_file = tmp_path / "test_only_assertions.json"

    pipeline = Pipeline(
        config=PipelineConfig(
            source=SourceConfig(
                type="dbt",
                config=DBTConfig(
                    manifest_path=str(
                        (test_resources_dir / "jaffle_shop_manifest.json").resolve()
                    ),
                    catalog_path=str(
                        (test_resources_dir / "jaffle_shop_catalog.json").resolve()
                    ),
                    target_platform="postgres",
                    test_results_path=str(
                        (test_resources_dir / "jaffle_shop_test_results.json").resolve()
                    ),
                    # this is just here to avoid needing to access datahub server
                    write_semantics="OVERRIDE",
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
    number_of_valid_assertions_in_test_results = 23
    assert (
        mce_helpers.assert_entity_urn_like(
            entity_type="assertion", regex_pattern="urn:li:assertion:", file=output_file
        )
        == number_of_valid_assertions_in_test_results
    )
    # no assertionInfo should be emitted
    try:
        mce_helpers.assert_for_each_entity(
            entity_type="assertion",
            aspect_name="assertionInfo",
            aspect_field_matcher={},
            file=output_file,
        )
    except AssertionError:
        pass

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
    pytestconfig, tmp_path, mock_time, **kwargs
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/dbt"

    # Run the metadata ingestion pipeline.
    output_file = tmp_path / "test_only_definitions_and_assertions.json"

    pipeline = Pipeline(
        config=PipelineConfig(
            source=SourceConfig(
                type="dbt",
                config=DBTConfig(
                    manifest_path=str(
                        (test_resources_dir / "jaffle_shop_manifest.json").resolve()
                    ),
                    catalog_path=str(
                        (test_resources_dir / "jaffle_shop_catalog.json").resolve()
                    ),
                    target_platform="postgres",
                    test_results_path=str(
                        (test_resources_dir / "jaffle_shop_test_results.json").resolve()
                    ),
                    # this is just here to avoid needing to access datahub server
                    write_semantics="OVERRIDE",
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
    number_of_assertions = 24
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
