import logging
import pathlib
from typing import Any, List
from unittest import mock

import pydantic
import pytest
from deepdiff import DeepDiff
from freezegun import freeze_time
from looker_sdk.sdk.api40.models import DBConnection

from datahub.configuration.common import PipelineExecutionError
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.file import read_metadata_file
from datahub.ingestion.source.looker.lookml_source import (
    LookerModel,
    LookerRefinementResolver,
    LookMLSourceConfig,
)
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    UpstreamLineageClass,
)
from tests.test_helpers import mce_helpers
from tests.test_helpers.state_helpers import get_current_checkpoint_from_pipeline

logging.getLogger("lkml").setLevel(logging.INFO)

FROZEN_TIME = "2020-04-14 07:00:00"
GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"


def get_default_recipe(output_file_path, base_folder_path):
    return {
        "run_id": "lookml-test",
        "source": {
            "type": "lookml",
            "config": {
                "base_folder": base_folder_path,
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
                "filename": f"{output_file_path}",
            },
        },
    }


@freeze_time(FROZEN_TIME)
def test_lookml_ingest(pytestconfig, tmp_path, mock_time):
    """Test backwards compatibility with previous form of config with new flags turned off"""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/lookml"
    mce_out_file = "expected_output.json"

    # Note this config below is known to create "bad" lineage since the config author has not provided enough information
    # to resolve relative table names (which are not fully qualified)
    # We keep this check just to validate that ingestion doesn't croak on this config

    pipeline = Pipeline.create(
        get_default_recipe(
            f"{tmp_path}/{mce_out_file}", f"{test_resources_dir}/lkml_samples"
        )
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
def test_lookml_refinement_ingest(pytestconfig, tmp_path, mock_time):
    """Test backwards compatibility with previous form of config with new flags turned off"""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/lookml"
    mce_out_file = "refinement_mces_output.json"

    # Note this config below is known to create "bad" lineage since the config author has not provided enough information
    # to resolve relative table names (which are not fully qualified)
    # We keep this check just to validate that ingestion doesn't croak on this config
    new_recipe = get_default_recipe(
        f"{tmp_path}/{mce_out_file}", f"{test_resources_dir}/lkml_samples"
    )
    new_recipe["source"]["config"]["process_refinements"] = True

    new_recipe["source"]["config"][
        "view_naming_pattern"
    ] = "{project}.{file_path}.view.{name}"

    new_recipe["source"]["config"][
        "view_browse_pattern"
    ] = "/{env}/{platform}/{project}/{file_path}/views"

    pipeline = Pipeline.create(new_recipe)
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status(raise_warnings=True)

    golden_path = test_resources_dir / "refinements_ingestion_golden.json"
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / mce_out_file,
        golden_path=golden_path,
    )


@freeze_time(FROZEN_TIME)
def test_lookml_refinement_include_order(pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/lookml"
    mce_out_file = "refinement_include_order_mces_output.json"

    new_recipe = get_default_recipe(
        f"{tmp_path}/{mce_out_file}",
        f"{test_resources_dir}/lkml_refinement_samples/sample1",
    )
    new_recipe["source"]["config"]["process_refinements"] = True
    new_recipe["source"]["config"]["project_name"] = "lkml_refinement_sample1"
    new_recipe["source"]["config"]["view_naming_pattern"] = {
        "pattern": "{project}.{model}.view.{name}"
    }
    new_recipe["source"]["config"]["connection_to_platform_map"] = {
        "db-connection": "conn"
    }
    pipeline = Pipeline.create(new_recipe)
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status(raise_warnings=True)

    golden_path = test_resources_dir / "refinement_include_order_golden.json"
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / mce_out_file,
        golden_path=golden_path,
    )


@freeze_time(FROZEN_TIME)
def test_lookml_explore_refinement(pytestconfig, tmp_path, mock_time):
    looker_model = LookerModel(
        explores=[
            {
                "name": "book",
            },
            {"name": "+book", "extends__all": [["order"]]},
            {"name": "+book", "extends__all": [["transaction"]]},
        ],
        connection=str(),
        resolved_includes=[],
        includes=[],
    )

    refinement_resolver = LookerRefinementResolver(
        looker_model=looker_model,
        looker_viewfile_loader=None,  # type: ignore
        reporter=None,  # type: ignore
        source_config=LookMLSourceConfig.parse_obj(
            {
                "process_refinements": "True",
                "base_folder": ".",
                "api": {
                    "base_url": "fake",
                    "client_id": "fake_client_id",
                    "client_secret": "fake_client_secret",
                },
            }
        ),
        connection_definition=None,  # type: ignore
    )

    new_explore: dict = refinement_resolver.apply_explore_refinement(
        looker_model.explores[0]
    )

    assert new_explore.get("extends") is not None
    assert new_explore["extends"].sort() == ["order", "transaction"].sort()


@freeze_time(FROZEN_TIME)
def test_lookml_view_merge(pytestconfig, tmp_path, mock_time):
    raw_view: dict = {
        "sql_table_name": "flightstats.accidents",
        "dimensions": [
            {
                "type": "number",
                "primary_key": "yes",
                "sql": '${TABLE}."id"',
                "name": "id",
            }
        ],
        "name": "flights",
    }

    refinement_views: List[dict] = [
        {
            "dimensions": [
                {
                    "type": "string",
                    "sql": '${TABLE}."air_carrier"',
                    "name": "air_carrier",
                }
            ],
            "name": "+flights",
        },
        {
            "measures": [
                {"type": "average", "sql": "${distance}", "name": "distance_avg"},
                {
                    "type": "number",
                    "sql": "STDDEV(${distance})",
                    "name": "distance_stddev",
                },
            ],
            "dimensions": [
                {
                    "type": "tier",
                    "sql": "${distance}",
                    "tiers": [500, 1300],
                    "name": "distance_tiered2",
                },
            ],
            "name": "+flights",
        },
        {
            "dimension_groups": [
                {
                    "type": "duration",
                    "intervals": ["week", "year"],
                    "sql_start": '${TABLE}."enrollment_date"',
                    "sql_end": '${TABLE}."graduation_date"',
                    "name": "enrolled",
                },
            ],
            "name": "+flights",
        },
        {
            "dimensions": [{"type": "string", "sql": '${TABLE}."id"', "name": "id"}],
            "name": "+flights",
        },
    ]

    merged_view: dict = LookerRefinementResolver.merge_refinements(
        raw_view=raw_view, refinement_views=refinement_views
    )

    expected_view: dict = {
        "sql_table_name": "flightstats.accidents",
        "dimensions": [
            {
                "type": "string",
                "primary_key": "yes",
                "sql": '${TABLE}."id"',
                "name": "id",
            },
            {"type": "string", "sql": '${TABLE}."air_carrier"', "name": "air_carrier"},
            {
                "type": "tier",
                "sql": "${distance}",
                "tiers": [500, 1300],
                "name": "distance_tiered2",
            },
        ],
        "name": "flights",
        "measures": [
            {"type": "average", "sql": "${distance}", "name": "distance_avg"},
            {"type": "number", "sql": "STDDEV(${distance})", "name": "distance_stddev"},
        ],
        "dimension_groups": [
            {
                "type": "duration",
                "intervals": ["week", "year"],
                "sql_start": '${TABLE}."enrollment_date"',
                "sql_end": '${TABLE}."graduation_date"',
                "name": "enrolled",
            }
        ],
    }

    assert DeepDiff(expected_view, merged_view) == {}


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
                    "process_refinements": False,
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
                    "process_refinements": False,
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
                    "process_refinements": False,
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
    with mock.patch("looker_sdk.init40") as mock_sdk:
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
                        "process_refinements": False,
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
                    "process_refinements": False,
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
    with pytest.raises(PipelineExecutionError):  # we expect the source to have warnings
        pipeline.raise_from_status(raise_warnings=True)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / mce_out,
        golden_path=test_resources_dir / mce_out,
    )


@freeze_time(FROZEN_TIME)
def test_lookml_git_info(pytestconfig, tmp_path, mock_time):
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
                    "process_refinements": False,
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
                    "process_refinements": False,
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
    # we should only have three views discoverable
    assert len(entity_urns) == 3
    assert (
        "urn:li:dataset:(urn:li:dataPlatform:looker,lkml_samples.view.my_view,PROD)"
        in entity_urns
    )
    assert (
        "urn:li:dataset:(urn:li:dataPlatform:looker,lkml_samples.view.my_view2,PROD)"
        in entity_urns
    )
    assert (
        "urn:li:dataset:(urn:li:dataPlatform:looker,lkml_samples.view.owners,PROD)"
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
                    "process_refinements": False,
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

    events = read_metadata_file(tmp_path / mce_out)
    for mce in events:
        if isinstance(mce, MetadataChangeEventClass):
            if isinstance(mce.proposedSnapshot, DatasetSnapshotClass):
                lineage_aspects = [
                    a
                    for a in mce.proposedSnapshot.aspects
                    if isinstance(a, UpstreamLineageClass)
                ]
                for a in lineage_aspects:
                    for upstream in a.upstreams:
                        assert "hive." not in upstream.dataset


@freeze_time(FROZEN_TIME)
def test_lookml_stateful_ingestion(pytestconfig, tmp_path, mock_time):
    output_file_name: str = "lookml_mces.json"
    state_file_name: str = "lookml_state_mces.json"
    golden_file_name: str = "golden_test_state.json"

    test_resources_dir = pytestconfig.rootpath / "tests/integration/lookml"

    base_pipeline_config = {
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
                        "type": "file",
                        "config": {
                            "filename": f"{tmp_path}/{state_file_name}",
                        },
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

    pipeline_run1 = Pipeline.create(base_pipeline_config)
    pipeline_run1.run()
    pipeline_run1.raise_from_status()
    pipeline_run1.pretty_print_summary()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=f"{tmp_path}/{state_file_name}",
        golden_path=f"{test_resources_dir}/{golden_file_name}",
    )

    checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)
    assert checkpoint1
    assert checkpoint1.state


def test_lookml_base_folder():
    fake_api = {
        "base_url": "https://filler.cloud.looker.com",
        "client_id": "this-is-fake",
        "client_secret": "this-is-also-fake",
    }

    LookMLSourceConfig.parse_obj(
        {
            "github_info": {
                "repo": "acryldata/long-tail-companions-looker",
                "deploy_key": "this-is-fake",
            },
            "api": fake_api,
        }
    )

    with pytest.raises(
        pydantic.ValidationError, match=r"base_folder.+nor.+git_info.+provided"
    ):
        LookMLSourceConfig.parse_obj({"api": fake_api})


@freeze_time(FROZEN_TIME)
def test_same_name_views_different_file_path(pytestconfig, tmp_path, mock_time):
    """Test for reachable views"""
    test_resources_dir = pytestconfig.rootpath / "tests/integration/lookml"
    mce_out = "lookml_same_name_views_different_file_path.json"
    pipeline = Pipeline.create(
        {
            "run_id": "lookml-test",
            "source": {
                "type": "lookml",
                "config": {
                    "base_folder": str(
                        test_resources_dir
                        / "lkml_same_name_views_different_file_path_samples"
                    ),
                    "connection_to_platform_map": {
                        "my_connection": {
                            "platform": "snowflake",
                            "platform_instance": "warehouse",
                            "platform_env": "dev",
                            "default_db": "default_db",
                            "default_schema": "default_schema",
                        },
                    },
                    "parse_table_names_from_sql": True,
                    "project_name": "lkml_samples",
                    "process_refinements": False,
                    "view_naming_pattern": "{project}.{file_path}.view.{name}",
                    "view_browse_pattern": "/{env}/{platform}/{project}/{file_path}/views",
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
