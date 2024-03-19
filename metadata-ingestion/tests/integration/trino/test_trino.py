import subprocess

import pytest
import requests
from freezegun import freeze_time

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.glossary.classifier import (
    ClassificationConfig,
    DynamicTypedClassifierConfig,
)
from datahub.ingestion.glossary.datahub_classifier import DataHubClassifierConfig
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.sink.file import FileSinkConfig
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.sql.trino import ConnectorDetail, TrinoConfig
from tests.test_helpers import fs_helpers, mce_helpers
from tests.test_helpers.docker_helpers import wait_for_port

pytestmark = pytest.mark.integration_batch_1

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
                    username="foo",
                    schema_pattern=AllowDenyPattern(allow=["^librarydb"]),
                    profile_pattern=AllowDenyPattern(
                        allow=["postgresqldb.librarydb.*"]
                    ),
                    profiling=GEProfilingConfig(
                        enabled=True,
                        include_field_null_count=True,
                        include_field_distinct_count=True,
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
                    classification=ClassificationConfig(
                        enabled=True,
                        classifiers=[
                            DynamicTypedClassifierConfig(
                                type="datahub",
                                config=DataHubClassifierConfig(
                                    minimum_values_threshold=1,
                                ),
                            )
                        ],
                        max_workers=1,
                    ),
                    catalog_to_connector_details={
                        "postgresqldb": ConnectorDetail(
                            connector_database="postgres",
                            platform_instance="local_server",
                        )
                    },
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
                classification=ClassificationConfig(
                    enabled=True,
                    classifiers=[
                        DynamicTypedClassifierConfig(
                            type="datahub",
                            config=DataHubClassifierConfig(
                                minimum_values_threshold=1,
                            ),
                        )
                    ],
                    max_workers=1,
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
def test_trino_instance_ingest(
    loaded_trino, test_resources_dir, pytestconfig, tmp_path, mock_time
):
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
                catalog_to_connector_details={
                    "hivedb": ConnectorDetail(
                        connector_platform="glue",
                        platform_instance="local_server",
                    )
                },
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
        output_path=events_file,
        golden_path=test_resources_dir / "trino_hive_instance_mces_golden.json",
        ignore_paths=[
            r"root\[\d+\]\['proposedSnapshot'\]\['com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot'\]\['aspects'\]\[\d+\]\['com.linkedin.pegasus2avro.dataset.DatasetProperties'\]\['customProperties'\]\['transient_lastddltime'\]",
        ],
    )
