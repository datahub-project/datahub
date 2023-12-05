import subprocess

import pytest
from freezegun import freeze_time

from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.source.kafka import KafkaSource
from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2020-04-14 07:00:00"


@pytest.fixture(scope="module")
def mock_kafka_service(docker_compose_runner, pytestconfig):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/kafka"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "kafka", cleanup=False
    ) as docker_services:
        wait_for_port(docker_services, "test_zookeeper", 52181, timeout=120)

    # Running docker compose twice, since the broker sometimes fails to come up on the first try.
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "kafka"
    ) as docker_services:
        wait_for_port(docker_services, "test_broker", 29092, timeout=120)
        wait_for_port(docker_services, "test_schema_registry", 8081, timeout=120)

        # Set up topics and produce some data
        command = f"{test_resources_dir}/send_records.sh {test_resources_dir}"
        subprocess.run(command, shell=True, check=True)

        yield docker_compose_runner


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_kafka_ingest(mock_kafka_service, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/kafka"

    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir / "kafka_to_file.yml").resolve()
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "kafka_mces.json",
        golden_path=test_resources_dir / "kafka_mces_golden.json",
        ignore_paths=[],
    )


@pytest.mark.integration
@freeze_time(FROZEN_TIME)
def test_kafka_test_connection_success(mock_kafka_service):
    config = {
        "connection": {
            "bootstrap": "localhost:29092",
            "schema_registry_url": "http://localhost:28081",
        },
    }
    report = KafkaSource.test_connection(config)
    assert report is not None
    assert report.basic_connectivity
    assert report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason is None
    assert report.capability_report
    assert report.capability_report[SourceCapability.SCHEMA_METADATA].capable
    assert (
        report.capability_report[SourceCapability.SCHEMA_METADATA].failure_reason
        is None
    )


@pytest.mark.integration
@freeze_time(FROZEN_TIME)
def test_kafka_test_connection_failure(mock_kafka_service):
    config = {
        "connection": {
            "bootstrap": "localhost:2909",
            "schema_registry_url": "http://localhost:2808",
        },
    }
    report = KafkaSource.test_connection(config)
    assert report is not None
    assert report.basic_connectivity
    assert not report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason
    assert "Failed to get metadata" in report.basic_connectivity.failure_reason
    assert report.capability_report
    assert not report.capability_report[SourceCapability.SCHEMA_METADATA].capable
    failure_reason = report.capability_report[
        SourceCapability.SCHEMA_METADATA
    ].failure_reason
    assert failure_reason
    assert "Failed to establish a new connection" in failure_reason
