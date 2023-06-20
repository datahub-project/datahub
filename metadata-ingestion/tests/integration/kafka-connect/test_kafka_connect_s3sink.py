import time

import pytest
import requests
from freezegun import freeze_time

from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2021-10-25 13:00:00"
GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"
KAFKA_CONNECT_SERVER = "http://localhost:28083"
KAFKA_CONNECT_ENDPOINT = f"{KAFKA_CONNECT_SERVER}/connectors"


@pytest.fixture(scope="module")
def kafka_connect_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    test_resources_dir_kafka = pytestconfig.rootpath / "tests/integration/kafka"

    docker_compose_file = [
        str(test_resources_dir_kafka / "docker-compose.yml"),
        str(test_resources_dir / "docker-compose.override.yml"),
    ]

    with docker_compose_runner(docker_compose_file, "kafka-connect") as docker_services:
        wait_for_port(docker_services, "test_broker", 29092, timeout=120)
        wait_for_port(docker_services, "test_connect", 28083, timeout=120)
        docker_services.wait_until_responsive(
            timeout=60,
            pause=1,
            check=lambda: requests.get(
                KAFKA_CONNECT_ENDPOINT,
            ).status_code
            == 200,
        )
        yield docker_services


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/kafka-connect"


@pytest.fixture(scope="module")
def loaded_kafka_connect_with_s3sink_connector(kafka_connect_runner):
    # Set up the container.
    time.sleep(10)

    # Creating S3 Sink source
    r = requests.post(
        KAFKA_CONNECT_ENDPOINT,
        headers={"Content-Type": "application/json"},
        data=r"""{
                    "name": "confluent_s3_sink_connector",
                    "config": {
                        "tasks.max": "1",
                        "max.interval": 5000,
                        "connector.class": "io.confluent.connect.s3.S3SinkConnector",
                        "s3.region": "ap-southeast-2",
                        "s3.bucket.name": "test-bucket",
                        "s3.compression.type": "gzip",
                        "store.url": "${env:S3_ENDPOINT_URL}",
                        "storage.class": "io.confluent.connect.s3.storage.S3Storage",
                        "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
                        "flush.size": 100,
                        "partitioner.class": "io.confluent.connect.storage.partitioner.HourlyPartitioner",
                        "locale": "en_AU",
                        "timezone": "UTC",
                        "timestamp.extractor": "Record",
                        "topics": "my-topic",
                        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                        "key.converter.schemas.enable": false,
                        "value.converter.schemas.enable": false
                    }
                }""",
    )
    r.raise_for_status()
    assert r.status_code == 201

    # Give time for connectors to process the table data
    time.sleep(60)


@freeze_time(FROZEN_TIME)
@pytest.mark.integration_batch_1
def test_kafka_connect_s3sink_ingest(
    loaded_kafka_connect_with_s3sink_connector,
    pytestconfig,
    tmp_path,
    test_resources_dir,
):
    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir / "kafka_connect_s3sink_to_file.yml").resolve()
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "kafka_connect_mces.json",
        golden_path=test_resources_dir / "kafka_connect_s3sink_mces_golden.json",
        ignore_paths=[],
    )
