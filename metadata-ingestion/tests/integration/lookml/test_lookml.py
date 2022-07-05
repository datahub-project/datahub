import logging
import pathlib
import sys
from typing import Any
from unittest import mock

import pytest
from freezegun import freeze_time
from looker_sdk.sdk.api31.models import DBConnection

from datahub.configuration.common import PipelineExecutionError
from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers  # noqa: F401

logging.getLogger("lkml").setLevel(logging.INFO)


FROZEN_TIME = "2020-04-14 07:00:00"


@freeze_time(FROZEN_TIME)
@pytest.mark.skipif(sys.version_info < (3, 7), reason="lkml requires Python 3.7+")
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
@pytest.mark.skipif(sys.version_info < (3, 7), reason="lkml requires Python 3.7+")
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
@pytest.mark.skipif(sys.version_info < (3, 7), reason="lkml requires Python 3.7+")
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
@pytest.mark.skipif(sys.version_info < (3, 7), reason="lkml requires Python 3.7+")
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
@pytest.mark.skipif(sys.version_info < (3, 7), reason="lkml requires Python 3.7+")
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
@pytest.mark.skipif(sys.version_info < (3, 7), reason="lkml requires Python 3.7+")
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
) -> None:  # noqa : No need for type annotations here
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
@pytest.mark.skipif(sys.version_info < (3, 7), reason="lkml requires Python 3.7+")
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
@pytest.mark.skipif(sys.version_info < (3, 7), reason="lkml requires Python 3.7+")
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
@pytest.mark.skipif(sys.version_info < (3, 7), reason="lkml requires Python 3.7+")
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
