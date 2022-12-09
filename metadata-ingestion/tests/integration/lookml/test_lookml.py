import logging
import pathlib
from typing import Any, List, Optional, cast
from unittest import mock

from freezegun import freeze_time
from looker_sdk.sdk.api31.models import DBConnection

from datahub.configuration.common import PipelineExecutionError
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.looker.lookml_source import LookMLSource
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.lookml_state import LookMLCheckpointState
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    UpstreamLineageClass,
)
from tests.test_helpers import mce_helpers
from tests.test_helpers.state_helpers import (
    validate_all_providers_have_committed_successfully,
)

logging.getLogger("lkml").setLevel(logging.INFO)

FROZEN_TIME = "2020-04-14 07:00:00"
GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"


@freeze_time(FROZEN_TIME)
def test_lookml_ingest(pytestconfig, tmp_path, mock_time):
    """Test backwards compatibility with previous form of config with new flags turned off"""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/lookml"
    mce_out_file = "expected_output.json"

    # Note this config below is known to create "bad" lineage since the config author has not provided enough information
    # to resolve relative table names (which are not fully qualified)
    # We keep this check just to validate that ingestion doesn't croak on this config
    pipeline = Pipeline.create(
        {
            "run_id": "lookml-test",
            "source": {
                "type": "lookml",
                "config": {
                    "base_folder": str(test_resources_dir / "lkml_samples"),
                    "connection_to_platform_map": {"my_connection": "conn"},
                    "parse_table_names_from_sql": True,
                    "tag_measures_and_dimensions": False,
                    "project_name": "lkml_samples",
                    "model_pattern": {"deny": ["data2"]},
                    "emit_reachable_views_only": False,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/{mce_out_file}",
                },
            },
        }
    )
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status(raise_warnings=True)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / mce_out_file,
        golden_path=test_resources_dir / mce_out_file,
    )


@freeze_time(FROZEN_TIME)
def test_lookml_ingest_offline(pytestconfig, tmp_path, mock_time):
    """New form of config with offline specification of connection defaults"""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/lookml"
    mce_out = "lookml_mces_offline.json"
    pipeline = Pipeline.create(
        {
            "run_id": "lookml-test",
            "source": {
                "type": "lookml",
                "config": {
                    "base_folder": str(test_resources_dir / "lkml_samples"),
                    "connection_to_platform_map": {
                        "my_connection": {
                            "platform": "snowflake",
                            "default_db": "default_db",
                            "default_schema": "default_schema",
                        }
                    },
                    "parse_table_names_from_sql": True,
                    "project_name": "lkml_samples",
                    "model_pattern": {"deny": ["data2"]},
                    "emit_reachable_views_only": False,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/{mce_out}",
                },
            },
        }
    )
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status(raise_warnings=True)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / mce_out,
        golden_path=test_resources_dir / mce_out,
    )


@freeze_time(FROZEN_TIME)
def test_lookml_ingest_offline_with_model_deny(pytestconfig, tmp_path, mock_time):
    """New form of config with offline specification of connection defaults"""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/lookml"
    mce_out = "lookml_mces_offline_deny_pattern.json"
    pipeline = Pipeline.create(
        {
            "run_id": "lookml-test",
            "source": {
                "type": "lookml",
                "config": {
                    "base_folder": str(test_resources_dir / "lkml_samples"),
                    "connection_to_platform_map": {
                        "my_connection": {
                            "platform": "snowflake",
                            "default_db": "default_db",
                            "default_schema": "default_schema",
                        }
                    },
                    "parse_table_names_from_sql": True,
                    "project_name": "lkml_samples",
                    "model_pattern": {"deny": ["data"]},
                    "emit_reachable_views_only": False,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/{mce_out}",
                },
            },
        }
    )
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status(raise_warnings=True)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / mce_out,
        golden_path=test_resources_dir / mce_out,
    )


@freeze_time(FROZEN_TIME)
def test_lookml_ingest_offline_platform_instance(pytestconfig, tmp_path, mock_time):
    """New form of config with offline specification of connection defaults"""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/lookml"
    mce_out = "lookml_mces_offline_platform_instance.json"
    pipeline = Pipeline.create(
        {
            "run_id": "lookml-test",
            "source": {
                "type": "lookml",
                "config": {
                    "base_folder": str(test_resources_dir / "lkml_samples"),
                    "connection_to_platform_map": {
                        "my_connection": {
                            "platform": "snowflake",
                            "platform_instance": "warehouse",
                            "platform_env": "dev",
                            "default_db": "default_db",
                            "default_schema": "default_schema",
                        }
                    },
                    "parse_table_names_from_sql": True,
                    "project_name": "lkml_samples",
                    "model_pattern": {"deny": ["data2"]},
                    "emit_reachable_views_only": False,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/{mce_out}",
                },
            },
        }
    )
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status(raise_warnings=True)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / mce_out,
        golden_path=test_resources_dir / mce_out,
    )


@freeze_time(FROZEN_TIME)
def test_lookml_ingest_api_bigquery(pytestconfig, tmp_path, mock_time):
    # test with BigQuery connection
    ingestion_test(
        pytestconfig,
        tmp_path,
        mock_time,
        DBConnection(
            dialect_name="bigquery", host="project-foo", database="default-db"
        ),
    )


@freeze_time(FROZEN_TIME)
def test_lookml_ingest_api_hive(pytestconfig, tmp_path, mock_time):
    # test with Hive connection
    ingestion_test(
        pytestconfig,
        tmp_path,
        mock_time,
        DBConnection(
            dialect_name="hive2",
            database="default-hive-db",
        ),
    )


def ingestion_test(
    pytestconfig: Any,
    tmp_path: pathlib.Path,
    mock_time: int,
    mock_connection: DBConnection,
) -> None:
    test_resources_dir = pytestconfig.rootpath / "tests/integration/lookml"
    mce_out_file = f"lookml_mces_api_{mock_connection.dialect_name}.json"
    mocked_client = mock.MagicMock()
    mock_model = mock.MagicMock(project_name="lkml_samples")
    with mock.patch("looker_sdk.init31") as mock_sdk:
        mock_sdk.return_value = mocked_client
        # mock_connection = mock.MagicMock()
        mocked_client.connection.return_value = mock_connection
        mocked_client.lookml_model.return_value = mock_model

        pipeline = Pipeline.create(
            {
                "run_id": "lookml-test",
                "source": {
                    "type": "lookml",
                    "config": {
                        "base_folder": str(test_resources_dir / "lkml_samples"),
                        "api": {
                            "client_id": "fake_client_id",
                            "client_secret": "fake_secret",
                            "base_url": "fake_account.looker.com",
                        },
                        "parse_table_names_from_sql": True,
                        "model_pattern": {"deny": ["data2"]},
                        "emit_reachable_views_only": False,
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/{mce_out_file}",
                    },
                },
            }
        )
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status(raise_warnings=True)

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / mce_out_file,
            golden_path=test_resources_dir / mce_out_file,
        )


@freeze_time(FROZEN_TIME)
def test_lookml_bad_sql_parser(pytestconfig, tmp_path, mock_time):
    """Incorrect specification of sql parser should not fail ingestion"""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/lookml"
    mce_out = "lookml_mces_badsql_parser.json"
    pipeline = Pipeline.create(
        {
            "run_id": "lookml-test",
            "source": {
                "type": "lookml",
                "config": {
                    "base_folder": str(test_resources_dir / "lkml_samples"),
                    "connection_to_platform_map": {
                        "my_connection": {
                            "platform": "snowflake",
                            "default_db": "default_db",
                            "default_schema": "default_schema",
                        }
                    },
                    "parse_table_names_from_sql": True,
                    "project_name": "lkml_samples",
                    "sql_parser": "bad.sql.Parser",
                    "emit_reachable_views_only": False,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/{mce_out}",
                },
            },
        }
    )
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status(raise_warnings=False)
    try:
        pipeline.raise_from_status(raise_warnings=True)
        assert False, "Pipeline should have generated warnings"
    except PipelineExecutionError:
        pass

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / mce_out,
        golden_path=test_resources_dir / mce_out,
    )


@freeze_time(FROZEN_TIME)
def test_lookml_github_info(pytestconfig, tmp_path, mock_time):
    """Add github info to config"""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/lookml"
    mce_out = "lookml_mces_with_external_urls.json"
    pipeline = Pipeline.create(
        {
            "run_id": "lookml-test",
            "source": {
                "type": "lookml",
                "config": {
                    "base_folder": str(test_resources_dir / "lkml_samples"),
                    "connection_to_platform_map": {
                        "my_connection": {
                            "platform": "snowflake",
                            "default_db": "default_db",
                            "default_schema": "default_schema",
                        }
                    },
                    "parse_table_names_from_sql": True,
                    "project_name": "lkml_samples",
                    "model_pattern": {"deny": ["data2"]},
                    "github_info": {"repo": "datahub/looker-demo", "branch": "master"},
                    "emit_reachable_views_only": False,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/{mce_out}",
                },
            },
        }
    )
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status(raise_warnings=True)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / mce_out,
        golden_path=test_resources_dir / mce_out,
    )


@freeze_time(FROZEN_TIME)
def test_reachable_views(pytestconfig, tmp_path, mock_time):
    """Test for reachable views"""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/lookml"
    mce_out = "lookml_reachable_views.json"
    pipeline = Pipeline.create(
        {
            "run_id": "lookml-test",
            "source": {
                "type": "lookml",
                "config": {
                    "base_folder": str(test_resources_dir / "lkml_samples"),
                    "connection_to_platform_map": {
                        "my_connection": {
                            "platform": "snowflake",
                            "platform_instance": "warehouse",
                            "platform_env": "dev",
                            "default_db": "default_db",
                            "default_schema": "default_schema",
                        },
                        "my_other_connection": {
                            "platform": "redshift",
                            "platform_instance": "rs_warehouse",
                            "platform_env": "dev",
                            "default_db": "default_db",
                            "default_schema": "default_schema",
                        },
                    },
                    "parse_table_names_from_sql": True,
                    "project_name": "lkml_samples",
                    "emit_reachable_views_only": True,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/{mce_out}",
                },
            },
        }
    )
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status(raise_warnings=True)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / mce_out,
        golden_path=test_resources_dir / mce_out,
    )

    entity_urns = mce_helpers.get_entity_urns(tmp_path / mce_out)
    # we should only have two views discoverable
    assert len(entity_urns) == 2
    assert (
        "urn:li:dataset:(urn:li:dataPlatform:looker,lkml_samples.view.my_view,PROD)"
        in entity_urns
    )
    assert (
        "urn:li:dataset:(urn:li:dataPlatform:looker,lkml_samples.view.my_view2,PROD)"
        in entity_urns
    )


@freeze_time(FROZEN_TIME)
def test_hive_platform_drops_ids(pytestconfig, tmp_path, mock_time):
    """Test omit db name from hive ids"""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/lookml"
    mce_out = "lookml_mces_with_db_name_omitted.json"
    pipeline = Pipeline.create(
        {
            "run_id": "lookml-test",
            "source": {
                "type": "lookml",
                "config": {
                    "base_folder": str(test_resources_dir / "lkml_samples_hive"),
                    "connection_to_platform_map": {
                        "my_connection": {
                            "platform": "hive",
                            "default_db": "default_database",
                            "default_schema": "default_schema",
                        }
                    },
                    "parse_table_names_from_sql": True,
                    "project_name": "lkml_samples",
                    "model_pattern": {"deny": ["data2"]},
                    "github_info": {"repo": "datahub/looker-demo", "branch": "master"},
                    "emit_reachable_views_only": False,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/{mce_out}",
                },
            },
        }
    )
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status(raise_warnings=True)

    maybe_events = mce_helpers.load_json_file(tmp_path / mce_out)
    assert isinstance(maybe_events, list)
    for mce in maybe_events:
        if "proposedSnapshot" in mce:
            mce_concrete = MetadataChangeEventClass.from_obj(mce)
            if isinstance(mce_concrete.proposedSnapshot, DatasetSnapshotClass):
                lineage_aspects = [
                    a
                    for a in mce_concrete.proposedSnapshot.aspects
                    if isinstance(a, UpstreamLineageClass)
                ]
                for a in lineage_aspects:
                    for upstream in a.upstreams:
                        assert "hive." not in upstream.dataset


@freeze_time(FROZEN_TIME)
def test_lookml_ingest_stateful(pytestconfig, tmp_path, mock_time, mock_datahub_graph):
    output_file_name: str = "lookml_mces.json"
    golden_file_name: str = "expected_output.json"
    output_file_deleted_name: str = "lookml_mces_deleted_stateful.json"
    golden_file_deleted_name: str = "lookml_mces_golden_deleted_stateful.json"

    test_resources_dir = pytestconfig.rootpath / "tests/integration/lookml"

    pipeline_run1 = None
    with mock.patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:

        mock_checkpoint.return_value = mock_datahub_graph
        pipeline_run1 = Pipeline.create(
            {
                "run_id": "lookml-test",
                "pipeline_name": "lookml_stateful",
                "source": {
                    "type": "lookml",
                    "config": {
                        "base_folder": str(test_resources_dir / "lkml_samples"),
                        "connection_to_platform_map": {"my_connection": "conn"},
                        "parse_table_names_from_sql": True,
                        "tag_measures_and_dimensions": False,
                        "project_name": "lkml_samples",
                        "model_pattern": {"deny": ["data2"]},
                        "emit_reachable_views_only": False,
                        "stateful_ingestion": {
                            "enabled": True,
                            "remove_stale_metadata": True,
                            "fail_safe_threshold": 100.0,
                            "state_provider": {
                                "type": "datahub",
                                "config": {"datahub_api": {"server": GMS_SERVER}},
                            },
                        },
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/{output_file_name}",
                    },
                },
            }
        )
        pipeline_run1.run()
        pipeline_run1.raise_from_status()
        pipeline_run1.pretty_print_summary()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / output_file_name,
            golden_path=f"{test_resources_dir}/{golden_file_name}",
        )

    checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)
    assert checkpoint1
    assert checkpoint1.state

    pipeline_run2 = None
    with mock.patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:

        mock_checkpoint.return_value = mock_datahub_graph

        pipeline_run2 = Pipeline.create(
            {
                "run_id": "lookml-test",
                "pipeline_name": "lookml_stateful",
                "source": {
                    "type": "lookml",
                    "config": {
                        "base_folder": str(test_resources_dir / "lkml_samples"),
                        "connection_to_platform_map": {"my_connection": "conn"},
                        "parse_table_names_from_sql": True,
                        "tag_measures_and_dimensions": False,
                        "project_name": "lkml_samples",
                        "model_pattern": {"deny": ["data2"]},
                        "emit_reachable_views_only": True,
                        "stateful_ingestion": {
                            "enabled": True,
                            "remove_stale_metadata": True,
                            "fail_safe_threshold": 100.0,
                            "state_provider": {
                                "type": "datahub",
                                "config": {"datahub_api": {"server": GMS_SERVER}},
                            },
                        },
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": f"{tmp_path}/{output_file_deleted_name}",
                    },
                },
            }
        )
        pipeline_run2.run()
        pipeline_run2.raise_from_status()
        pipeline_run2.pretty_print_summary()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / output_file_deleted_name,
            golden_path=f"{test_resources_dir}/{golden_file_deleted_name}",
        )

    checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)
    assert checkpoint2
    assert checkpoint2.state

    # Validate that all providers have committed successfully.
    validate_all_providers_have_committed_successfully(
        pipeline=pipeline_run1, expected_providers=1
    )
    validate_all_providers_have_committed_successfully(
        pipeline=pipeline_run2, expected_providers=1
    )

    # Perform all assertions on the states. The deleted table should not be
    # part of the second state
    state1 = cast(LookMLCheckpointState, checkpoint1.state)
    state2 = cast(LookMLCheckpointState, checkpoint2.state)

    difference_dataset_urns = list(
        state1.get_urns_not_in(type="dataset", other_checkpoint_state=state2)
    )
    assert len(difference_dataset_urns) == 9
    deleted_dataset_urns: List[str] = [
        "urn:li:dataset:(urn:li:dataPlatform:looker,lkml_samples.view.fragment_derived_view,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:looker,lkml_samples.view.my_derived_view,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:looker,lkml_samples.view.test_include_external_view,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:looker,lkml_samples.view.extending_looker_events,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:looker,lkml_samples.view.customer_facts,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:looker,lkml_samples.view.include_able_view,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:looker,lkml_samples.view.autodetect_sql_name_based_on_view_name,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:looker,lkml_samples.view.ability,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:looker,lkml_samples.view.looker_events,PROD)",
    ]
    assert sorted(deleted_dataset_urns) == sorted(difference_dataset_urns)


def get_current_checkpoint_from_pipeline(
    pipeline: Pipeline,
) -> Optional[Checkpoint]:
    dbt_source = cast(LookMLSource, pipeline.source)
    return dbt_source.get_current_checkpoint(
        dbt_source.stale_entity_removal_handler.job_id
    )
