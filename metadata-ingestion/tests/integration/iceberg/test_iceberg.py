import subprocess
from typing import Any, Dict
from unittest.mock import patch

import pytest
from freezegun import freeze_time

from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import cleanup_image, wait_for_port
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    run_and_get_pipeline,
    validate_all_providers_have_committed_successfully,
)

pytestmark = pytest.mark.integration_batch_1
FROZEN_TIME = "2020-04-14 07:00:00"
GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"
# These paths change from one instance run of the clickhouse docker to the other, and the FROZEN_TIME does not apply to
# these.
PATHS_IN_GOLDEN_FILE_TO_IGNORE = [
    r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['customProperties'\]\['created-at'\]",
    r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['customProperties'\]\['snapshot-id'\]",
    r"root\[\d+\]\['proposedSnapshot'\].+\['aspects'\].+\['customProperties'\]\['manifest-list'\]",
]


@pytest.fixture(autouse=True, scope="module")
def remove_docker_image():
    yield

    # The tabulario/spark-iceberg image is pretty large, so we remove it after the test.
    cleanup_image("tabulario/spark-iceberg")


def spark_submit(file_path: str, args: str = "") -> None:
    docker = "docker"
    command = f"{docker} exec spark-iceberg spark-submit {file_path} {args}"
    ret = subprocess.run(command, shell=True, capture_output=True)
    assert ret.returncode == 0


@freeze_time(FROZEN_TIME)
def test_multiprocessing_iceberg_ingest(
    docker_compose_runner, pytestconfig, tmp_path, mock_time
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/iceberg/"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "iceberg"
    ) as docker_services:
        wait_for_port(docker_services, "spark-iceberg", 8888, timeout=120)

        # Run the create.py pyspark file to populate the table.
        spark_submit("/home/iceberg/setup/create.py", "nyc.taxis")

        # Run the metadata ingestion pipeline.
        config_file = (
            test_resources_dir / "iceberg_multiprocessing_to_file.yml"
        ).resolve()
        run_datahub_cmd(
            ["ingest", "--strict-warnings", "-c", f"{config_file}"], tmp_path=tmp_path
        )
        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            ignore_paths=PATHS_IN_GOLDEN_FILE_TO_IGNORE,
            output_path=tmp_path / "iceberg_mces.json",
            golden_path=test_resources_dir / "iceberg_ingest_mces_golden.json",
        )


@freeze_time(FROZEN_TIME)
def test_iceberg_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/iceberg/"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "iceberg"
    ) as docker_services:
        wait_for_port(docker_services, "spark-iceberg", 8888, timeout=120)

        # Run the create.py pyspark file to populate the table.
        spark_submit("/home/iceberg/setup/create.py", "nyc.taxis")

        # Run the metadata ingestion pipeline.
        config_file = (test_resources_dir / "iceberg_to_file.yml").resolve()
        run_datahub_cmd(
            ["ingest", "--strict-warnings", "-c", f"{config_file}"], tmp_path=tmp_path
        )
        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            ignore_paths=PATHS_IN_GOLDEN_FILE_TO_IGNORE,
            output_path=tmp_path / "iceberg_mces.json",
            golden_path=test_resources_dir / "iceberg_ingest_mces_golden.json",
        )


@freeze_time(FROZEN_TIME)
def test_iceberg_stateful_ingest(
    docker_compose_runner, pytestconfig, tmp_path, mock_time, mock_datahub_graph
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/iceberg"
    platform_instance = "test_platform_instance"

    pipeline_config_dict: Dict[str, Any] = {
        "source": {
            "type": "iceberg",
            "config": {
                "catalog": {
                    "default": {
                        "type": "rest",
                        "uri": "http://localhost:8181",
                        "s3.access-key-id": "admin",
                        "s3.secret-access-key": "password",
                        "s3.region": "us-east-1",
                        "warehouse": "s3a://warehouse/wh/",
                        "s3.endpoint": "http://localhost:9000",
                    },
                },
                "user_ownership_property": "owner",
                "group_ownership_property": "owner",
                "platform_instance": f"{platform_instance}",
                # enable stateful ingestion
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
            # we are not really interested in the resulting events for this test
            "type": "console"
        },
        "pipeline_name": "test_pipeline",
    }

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "iceberg"
    ) as docker_services, patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        wait_for_port(docker_services, "spark-iceberg", 8888, timeout=120)

        # Run the create.py pyspark file to populate two tables.
        spark_submit("/home/iceberg/setup/create.py", "nyc.taxis")
        spark_submit("/home/iceberg/setup/create.py", "nyc.another_taxis")

        # Both checkpoint and reporting will use the same mocked graph instance.
        mock_checkpoint.return_value = mock_datahub_graph

        # Do the first run of the pipeline and get the default job's checkpoint.
        pipeline_run1 = run_and_get_pipeline(pipeline_config_dict)
        checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)

        assert checkpoint1
        assert checkpoint1.state

        # Capture MCEs of second run to validate Status(removed=true)
        deleted_mces_path = f"{tmp_path}/iceberg_deleted_mces.json"
        pipeline_config_dict["sink"]["type"] = "file"
        pipeline_config_dict["sink"]["config"] = {"filename": deleted_mces_path}

        # Run the delete.py pyspark file to delete the table.
        spark_submit("/home/iceberg/setup/delete.py")

        # Do the second run of the pipeline.
        pipeline_run2 = run_and_get_pipeline(pipeline_config_dict)
        checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)

        assert checkpoint2
        assert checkpoint2.state

        # Perform all assertions on the states. The deleted table should not be
        # part of the second state
        state1 = checkpoint1.state
        state2 = checkpoint2.state
        difference_urns = list(
            state1.get_urns_not_in(type="dataset", other_checkpoint_state=state2)
        )

        assert len(difference_urns) == 1

        urn1 = "urn:li:dataset:(urn:li:dataPlatform:iceberg,test_platform_instance.nyc.taxis,PROD)"

        assert urn1 in difference_urns

        # Validate that all providers have committed successfully.
        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run1, expected_providers=1
        )
        validate_all_providers_have_committed_successfully(
            pipeline=pipeline_run2, expected_providers=1
        )

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            ignore_paths=PATHS_IN_GOLDEN_FILE_TO_IGNORE,
            output_path=deleted_mces_path,
            golden_path=test_resources_dir / "iceberg_deleted_table_mces_golden.json",
        )


@freeze_time(FROZEN_TIME)
def test_iceberg_profiling(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/iceberg/"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "iceberg"
    ) as docker_services:
        wait_for_port(docker_services, "spark-iceberg", 8888, timeout=120)

        # Run the create.py pyspark file to populate the table.
        spark_submit("/home/iceberg/setup/create.py", "nyc.taxis")

        # Run the metadata ingestion pipeline.
        config_file = (test_resources_dir / "iceberg_profile_to_file.yml").resolve()
        run_datahub_cmd(
            ["ingest", "--strict-warnings", "-c", f"{config_file}"], tmp_path=tmp_path
        )

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            ignore_paths=PATHS_IN_GOLDEN_FILE_TO_IGNORE,
            output_path=tmp_path / "iceberg_mces.json",
            golden_path=test_resources_dir / "iceberg_profile_mces_golden.json",
        )
