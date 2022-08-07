import re
import subprocess

import pytest
import requests
from freezegun import freeze_time

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.sink.file import FileSinkConfig
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.sql.trino import TrinoConfig
from tests.test_helpers import fs_helpers, mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2021-09-23 12:00:00"

data_platform = "trino"


@pytest.fixture(scope="module")
def trino_runner(docker_compose_runner, pytestconfig):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/trino"
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "trino"
    ) as docker_services:
        wait_for_port(docker_services, "testtrino", 8080)
        wait_for_port(docker_services, "testhiveserver2", 10000, timeout=120)
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
    return pytestconfig.rootpath / "tests/integration/trino"


@pytest.fixture(scope="module")
def loaded_trino(trino_runner):
    # Set up the container.
    command = "docker exec testhiveserver2 /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -f /hive_setup.sql"
    subprocess.run(command, shell=True, check=True)


@freeze_time(FROZEN_TIME)
@pytest.mark.xfail  # TODO: debug the flakes for this test
@pytest.mark.integration
def test_trino_ingest(
    loaded_trino, test_resources_dir, pytestconfig, tmp_path, mock_time
):

    # Run the metadata ingestion pipeline.
    with fs_helpers.isolated_filesystem(tmp_path):

        # Run the metadata ingestion pipeline for trino catalog referring to postgres database
        mce_out_file = "trino_mces.json"
        events_file = tmp_path / mce_out_file

        pipeline_config = {
            "run_id": "trino-test",
            "source": {
                "type": data_platform,
                "config": TrinoConfig(
                    host_port="localhost:5300",
                    database="postgresqldb",
                    database_alias="library_catalog",
                    username="foo",
                    schema_pattern=AllowDenyPattern(allow=["^librarydb"]),
                    profile_pattern=AllowDenyPattern(
                        allow=["library_catalog.librarydb.*"]
                    ),
                    profiling=GEProfilingConfig(
                        enabled=True,
                        include_field_null_count=True,
                        include_field_min_value=True,
                        include_field_max_value=True,
                        include_field_mean_value=True,
                        include_field_median_value=True,
                        include_field_stddev_value=True,
                        include_field_quantiles=True,
                        include_field_distinct_value_frequencies=True,
                        include_field_histogram=True,
                        include_field_sample_values=True,
                    ),
                ).dict(),
            },
            "sink": {
                "type": "file",
                "config": FileSinkConfig(filename=str(events_file)).dict(),
            },
        }

        # Run the metadata ingestion pipeline.
        pipeline = Pipeline.create(pipeline_config)
        pipeline.run()
        pipeline.pretty_print_summary()
        pipeline.raise_from_status(raise_warnings=True)
        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path="trino_mces.json",
            golden_path=test_resources_dir / "trino_mces_golden.json",
        )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_trino_hive_ingest(
    loaded_trino, test_resources_dir, pytestconfig, tmp_path, mock_time
):

    # Run the metadata ingestion pipeline for trino catalog referring to postgres database
    mce_out_file = "trino_hive_mces.json"
    events_file = tmp_path / mce_out_file

    pipeline_config = {
        "run_id": "trino-hive-test",
        "source": {
            "type": data_platform,
            "config": TrinoConfig(
                host_port="localhost:5300",
                database="hivedb",
                username="foo",
                schema_pattern=AllowDenyPattern(allow=["^db1"]),
            ).dict(),
        },
        "sink": {
            "type": "file",
            "config": FileSinkConfig(filename=str(events_file)).dict(),
        },
    }

    # Run the metadata ingestion pipeline.
    pipeline = Pipeline.create(pipeline_config)
    pipeline.run()
    pipeline.pretty_print_summary()
    pipeline.raise_from_status(raise_warnings=True)

    # Limitation 1  - MCE contains "nullable": true for all fields in trino database, irrespective of not null constraints present in underlying postgres database.
    # This is issue with trino, also reported here - https://github.com/trinodb/trino/issues/6400, Related : https://github.com/trinodb/trino/issues/4070
    # Limitation 2 - Dataset properties for postgres view (view definition, etc) are not part of MCE from trino.
    # Postgres views are exposed as tables in trino. This setting depends on trino connector implementation - https://trino.io/episodes/18.html

    # Run the metadata ingestion pipeline for trino catalog referring to hive database
    # config_file = (test_resources_dir / "trino_hive_to_file.yml").resolve()
    # run_datahub_cmd(["ingest", "-c", f"{config_file}"])

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=events_file,
        golden_path=test_resources_dir / "trino_hive_mces_golden.json",
        ignore_paths=[
            r"root\[\d+\]\['proposedSnapshot'\]\['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com.linkedin.pegasus2avro.dataset.DatasetProperties'\]\['customProperties'\]\['transient_lastddltime'\]",
            r"root\[\d+\]\['proposedSnapshot'\]\['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com.linkedin.pegasus2avro.dataset.DatasetProperties'\]\['customProperties'\]\['numfiles'\]",
            r"root\[\d+\]\['proposedSnapshot'\]\['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com.linkedin.pegasus2avro.dataset.DatasetProperties'\]\['customProperties'\]\['totalsize'\]",
        ],
    )

    # Limitation 3 - Limited DatasetProperties available in Trino than in direct hive source - https://trino.io/docs/current/connector/hive.html#table-properties.


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_trino_instance_ingest(
    loaded_trino, test_resources_dir, pytestconfig, tmp_path, mock_time
):
    instance = "production_warehouse"
    platform = "trino"
    mce_out_file = "trino_instance_mces.json"
    events_file = tmp_path / mce_out_file
    pipeline_config = {
        "run_id": "trino-hive-instance-test",
        "source": {
            "type": data_platform,
            "config": TrinoConfig(
                host_port="localhost:5300",
                database="hivedb",
                username="foo",
                platform_instance="production_warehouse",
                schema_pattern=AllowDenyPattern(allow=["^db1"]),
            ).dict(),
        },
        "sink": {
            "type": "file",
            "config": FileSinkConfig(filename=str(events_file)).dict(),
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
