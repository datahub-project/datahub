import re
import subprocess
import sys
from typing import Dict

import pytest
import requests
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import fs_helpers, mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2021-09-23 12:00:00"

data_platform = "presto-on-hive"


@pytest.mark.skipif(
    sys.version_info < (3, 7), reason="presto-on-hive requires Python 3.7+"
)
@pytest.fixture(scope="module")
def presto_on_hive_runner(docker_compose_runner, pytestconfig):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/presto-on-hive"
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "presto-on-hive"
    ) as docker_services:
        wait_for_port(docker_services, "presto", 8080)
        wait_for_port(docker_services, "hiveserver2", 10000, timeout=120)
        docker_services.wait_until_responsive(
            timeout=30,
            pause=1,
            check=lambda: requests.get("http://localhost:5300/v1/info").json()[
                "starting"
            ]
            is False,
        )

        yield docker_services


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/presto-on-hive"


@pytest.fixture(scope="module")
def loaded_presto_on_hive(presto_on_hive_runner):
    # Set up the container.
    command = "docker exec hiveserver2 /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -f /hive_setup.sql"
    subprocess.run(command, shell=True, check=True)


@freeze_time(FROZEN_TIME)
@pytest.mark.integration_batch_1
def test_presto_on_hive_ingest(
    loaded_presto_on_hive, test_resources_dir, pytestconfig, tmp_path, mock_time
):

    # Run the metadata ingestion pipeline.
    with fs_helpers.isolated_filesystem(tmp_path):

        # Run the metadata ingestion pipeline for presto catalog referring to postgres database
        mce_out_file = "presto_on_hive_mces.json"
        events_file = tmp_path / mce_out_file

        pipeline_config: Dict = {
            "run_id": "presto-on-hive-test",
            "source": {
                "type": data_platform,
                "config": {
                    "host_port": "localhost:5432",
                    "database": "metastore",
                    "database_alias": "hive",
                    "username": "postgres",
                    "scheme": "postgresql+psycopg2",
                    "include_views": True,
                    "include_tables": True,
                    "schema_pattern": {"allow": ["^public"]},
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(events_file),
                },
            },
        }

        # Run the metadata ingestion pipeline.
        pipeline = Pipeline.create(pipeline_config)
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status(raise_warnings=True)

        # Run the metadata ingestion pipeline for presto catalog referring to hive database
        # config_file = (test_resources_dir / "presto_on_hive_to_file.yml").resolve()
        # run_datahub_cmd(["ingest", "-c", f"{config_file}"])

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path="presto_on_hive_mces.json",
            golden_path=test_resources_dir / "presto_on_hive_mces_golden.json",
            ignore_paths=[
                r"root\[\d+\]\['proposedSnapshot'\]\['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com.linkedin.pegasus2avro.dataset.DatasetProperties'\]\['customProperties'\]\['transient_lastddltime'\]",
                r"root\[\d+\]\['proposedSnapshot'\]\['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com.linkedin.pegasus2avro.dataset.DatasetProperties'\]\['customProperties'\]\['numfiles'\]",
                r"root\[\d+\]\['proposedSnapshot'\]\['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com.linkedin.pegasus2avro.dataset.DatasetProperties'\]\['customProperties'\]\['totalsize'\]",
                r"root\[\d+\]\['proposedSnapshot'\]\['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com.linkedin.pegasus2avro.dataset.DatasetProperties'\]\['customProperties'\]\['create_date'\]",
            ],
        )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration_batch_1
def test_presto_on_hive_instance_ingest(
    loaded_presto_on_hive, test_resources_dir, pytestconfig, tmp_path, mock_time
):
    instance = "production_warehouse"
    platform = "presto-on-hive"
    mce_out_file = "presto_on_hive_instance_mces.json"
    events_file = tmp_path / mce_out_file

    pipeline_config: Dict = {
        "run_id": "presto-on-hive-test-2",
        "source": {
            "type": data_platform,
            "config": {
                "host_port": "localhost:5432",
                "database": "metastore",
                "database_alias": "hive",
                "username": "postgres",
                "scheme": "postgresql+psycopg2",
                "include_views": True,
                "include_tables": True,
                "schema_pattern": {"allow": ["^public"]},
                "platform_instance": "production_warehouse",
            },
        },
        "sink": {
            "type": "file",
            "config": {
                "filename": str(events_file),
            },
        },
    }

    # Run the metadata ingestion pipeline.
    pipeline = Pipeline.create(pipeline_config)
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status(raise_warnings=True)

    # Assert that all events generated have instance specific urns
    urn_pattern = "^" + re.escape(
        f"urn:li:dataset:(urn:li:dataPlatform:{platform},{instance}."
    )
    assert (
        mce_helpers.assert_mce_entity_urn(
            "ALL",
            entity_type="dataset",
            regex_pattern=urn_pattern,
            file=events_file,
        )
        >= 0
    ), "There should be at least one match"

    assert (
        mce_helpers.assert_mcp_entity_urn(
            "ALL",
            entity_type="dataset",
            regex_pattern=urn_pattern,
            file=events_file,
        )
        >= 0
    ), "There should be at least one MCP"

    # all dataset entities emitted must have a dataPlatformInstance aspect emitted
    # there must be at least one entity emitted
    assert (
        mce_helpers.assert_for_each_entity(
            entity_type="dataset",
            aspect_name="dataPlatformInstance",
            aspect_field_matcher={
                "instance": f"urn:li:dataPlatformInstance:(urn:li:dataPlatform:{platform},{instance})"
            },
            file=events_file,
        )
        >= 1
    )
