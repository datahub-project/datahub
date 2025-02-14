import json
import logging
import subprocess
import time
from typing import Callable, Dict

import pytest
import yaml
from confluent_kafka.avro import AvroProducer
from freezegun import freeze_time

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.kafka.kafka import KafkaSource, KafkaSourceConfig
from tests.integration.kafka import oauth  # type: ignore
from tests.test_helpers import mce_helpers, test_connection_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2020-04-14 07:00:00"


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/kafka"


@pytest.fixture(scope="module")
def mock_kafka_service(docker_compose_runner, test_resources_dir):
    print("\nStarting Kafka test services...")

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "kafka", cleanup=False
    ) as docker_services:
        print("Waiting for Zookeeper...")
        try:
            wait_for_port(docker_services, "test_zookeeper", 52181, timeout=30)
        except Exception as e:
            pytest.fail(f"Zookeeper failed to start: {str(e)}")

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "kafka"
    ) as docker_services:
        print("Waiting for Kafka broker and Schema Registry...")
        try:
            wait_for_port(docker_services, "test_broker", 29092, timeout=30)
            wait_for_port(docker_services, "test_schema_registry", 8081, timeout=30)

            print("Setting up test data...")
            command = f"{test_resources_dir}/send_records.sh {test_resources_dir}"
            try:
                result = subprocess.run(
                    command,
                    shell=True,
                    check=True,
                    timeout=60,
                    capture_output=True,
                    text=True,
                )
                print(f"Data generation stdout:\n{result.stdout}")
                if result.stderr:
                    print(f"Data generation stderr:\n{result.stderr}")

            except subprocess.TimeoutExpired:
                pytest.fail("Data generation timed out after 60 seconds")
            except subprocess.CalledProcessError as e:
                pytest.fail(
                    f"Data generation failed: {str(e)}\nStdout: {e.stdout}\nStderr: {e.stderr}"
                )
            except Exception as e:
                pytest.fail(f"Data generation failed unexpectedly: {str(e)}")

            print("Waiting for data to be available...")
            time.sleep(15)
            print("Kafka setup complete")
            yield docker_services

        except Exception as e:
            pytest.fail(f"Kafka setup failed: {str(e)}")


@pytest.mark.parametrize("approach", ["kafka_without_schemas", "kafka"])
@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_kafka_ingest(
    mock_kafka_service, test_resources_dir, pytestconfig, tmp_path, mock_time, approach
):
    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir / f"{approach}_to_file.yml").resolve()
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / f"{approach}_mces.json",
        golden_path=test_resources_dir / f"{approach}_mces_golden.json",
        ignore_paths=[],
    )


@pytest.mark.parametrize(
    "config_dict, is_success",
    [
        (
            {
                "connection": {
                    "bootstrap": "localhost:29092",
                    "schema_registry_url": "http://localhost:28081",
                },
            },
            True,
        ),
        (
            {
                "connection": {
                    "bootstrap": "localhost:2909",
                    "schema_registry_url": "http://localhost:2808",
                },
            },
            False,
        ),
    ],
)
@pytest.mark.integration
@freeze_time(FROZEN_TIME)
def test_kafka_test_connection(mock_kafka_service, config_dict, is_success):
    report = test_connection_helpers.run_test_connection(KafkaSource, config_dict)
    if is_success:
        test_connection_helpers.assert_basic_connectivity_success(report)
        test_connection_helpers.assert_capability_report(
            capability_report=report.capability_report,
            success_capabilities=[SourceCapability.SCHEMA_METADATA],
        )
    else:
        test_connection_helpers.assert_basic_connectivity_failure(
            report, "Failed to get metadata"
        )
        # Add type checking for capability_report
        if (
            report.capability_report is not None
            and SourceCapability.SCHEMA_METADATA in report.capability_report
        ):
            error_msg = report.capability_report[
                SourceCapability.SCHEMA_METADATA
            ].failure_reason
            if error_msg is not None:  # Add null check for failure_reason
                assert any(
                    msg in error_msg
                    for msg in [
                        "[Errno 111] Connection refused",
                        "[Errno 61] Connection refused",
                    ]
                ), (
                    f"Error message '{error_msg}' does not contain expected connection refused error"
                )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_kafka_oauth_callback(
    mock_kafka_service, test_resources_dir, pytestconfig, tmp_path, mock_time
):
    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir / "kafka_to_file_oauth.yml").resolve()

    log_file = tmp_path / "kafka_oauth_message.log"

    file_handler = logging.FileHandler(
        str(log_file)
    )  # Add a file handler to later validate a test-case
    logging.getLogger().addHandler(file_handler)

    recipe: dict = {}
    with open(config_file) as fp:
        recipe = yaml.safe_load(fp)

    pipeline = Pipeline.create(recipe)

    pipeline.run()

    # Initialize flags to track oauth events
    checks = {
        "consumer_polling": False,
        "consumer_oauth_callback": False,
        "admin_polling": False,
        "admin_oauth_callback": False,
    }

    # Read log file and check for oauth events
    with open(log_file, "r") as file:
        for line in file:
            # Check for polling events
            if "Initiating polling for kafka admin client" in line:
                checks["admin_polling"] = True
            elif "Initiating polling for kafka consumer" in line:
                checks["consumer_polling"] = True

            # Check for oauth callbacks
            if oauth.MESSAGE in line:
                if checks["consumer_polling"] and not checks["admin_polling"]:
                    checks["consumer_oauth_callback"] = True
                elif checks["consumer_polling"] and checks["admin_polling"]:
                    checks["admin_oauth_callback"] = True

    # Verify all oauth events occurred
    assert checks["consumer_polling"], "Consumer polling was not initiated"
    assert checks["consumer_oauth_callback"], "Consumer oauth callback not found"
    assert checks["admin_polling"], "Admin polling was not initiated"
    assert checks["admin_oauth_callback"], "Admin oauth callback not found"


def test_kafka_source_oauth_cb_signature():
    with pytest.raises(
        ConfigurationError,
        match=("oauth_cb function must accept single positional argument."),
    ):
        KafkaSourceConfig.parse_obj(
            {
                "connection": {
                    "bootstrap": "foobar:9092",
                    "consumer_config": {"oauth_cb": "oauth:create_token_no_args"},
                }
            }
        )

    with pytest.raises(
        ConfigurationError,
        match=("oauth_cb function must accept single positional argument."),
    ):
        KafkaSourceConfig.parse_obj(
            {
                "connection": {
                    "bootstrap": "foobar:9092",
                    "consumer_config": {"oauth_cb": "oauth:create_token_only_kwargs"},
                }
            }
        )


def generate_test_messages(
    topic: str, count: int, data_generator: Callable[[int], Dict]
) -> None:
    """Helper to generate test messages with specific patterns"""
    producer = AvroProducer(
        {
            "bootstrap.servers": "localhost:29092",
            "schema.registry.url": "http://localhost:28081",
        }
    )

    for i in range(count):
        value = data_generator(i)
        producer.produce(topic=topic, value=value)
    producer.flush()


def numeric_data_generator(i: int) -> Dict:
    """Generates numeric test data with known statistical properties"""
    return {"integer_field": i, "float_field": float(i) / 2}


def null_data_generator(i: int) -> Dict:
    """Generates test data with null values in specific patterns"""
    return {
        "email": None if i % 2 == 0 else f"user{i}@example.com",
        "firstName": f"First{i}",
        "lastName": None if i % 3 == 0 else f"Last{i}",
    }


@pytest.mark.parametrize(
    "test_case",
    [
        "basic_profiling",  # Use value_topic
        "schema_profiling",  # Use key_value_topic
        "numeric_profiling",  # Use numeric_topic
    ],
)
@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_kafka_profiling(
    mock_kafka_service, test_resources_dir, pytestconfig, tmp_path, mock_time, test_case
):
    # Run the metadata ingestion pipeline with profiling enabled
    config_file = (test_resources_dir / "kafka_profiling_to_file.yml").resolve()
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Read the output file as a single JSON array
    with open(tmp_path / "kafka_profiling_mces.json", "r") as f:
        mces = json.load(f)  # Changed from json.loads(line)

    # Find profile aspects
    profile_aspects = []
    for mce in mces:
        if (
            isinstance(mce, dict)
            and mce.get("entityType") == "dataset"
            and mce.get("aspectName") == "datasetProfile"
            and mce.get("aspect", {}).get("json")
        ):
            profile_aspects.append(mce["aspect"]["json"])

    # Assert we found profile data
    assert len(profile_aspects) > 0, "No profile aspects found in output"

    # For each profile, verify required fields based on config
    for profile in profile_aspects:
        # Basic structure checks
        assert "timestampMillis" in profile, "Profile missing timestampMillis"
        assert "columnCount" in profile, "Profile missing columnCount"
        assert profile["columnCount"] > 0, "Profile has no columns"
        assert "fieldProfiles" in profile, "Profile missing fieldProfiles"

        # Verify partition spec matches config
        assert "partitionSpec" in profile, "Profile missing partitionSpec"
        assert profile["partitionSpec"]["partition"].startswith("SAMPLE ("), (
            "Incorrect partition spec format"
        )

        # Check field profiles based on config
        for field_profile in profile["fieldProfiles"]:
            # Required fields
            assert "fieldPath" in field_profile, "Field profile missing fieldPath"

            # Config-specified fields
            if field_profile.get("fieldPath", "").endswith((".id", ".value", ".count")):
                # Numeric fields should have these stats as per config
                assert any(
                    key in field_profile for key in ["min", "max", "mean", "median"]
                ), (
                    f"Numeric field {field_profile['fieldPath']} missing required statistical properties"
                )
                assert "stdev" in field_profile, (
                    f"Field {field_profile['fieldPath']} missing standard deviation"
                )
                if "quantiles" in field_profile:
                    assert isinstance(field_profile["quantiles"], list), (
                        f"Field {field_profile['fieldPath']} has invalid quantiles format"
                    )

            # Check disabled fields
            assert "sampleValues" in field_profile, (
                f"Field {field_profile['fieldPath']} missing sample values"
            )
            assert "distinctValueFrequencies" in field_profile, (
                f"Field {field_profile['fieldPath']} missing frequencies"
            )

            if "uniqueCount" in field_profile:
                assert "uniqueProportion" in field_profile, (
                    f"Field {field_profile['fieldPath']} missing uniqueProportion"
                )

            # Check for histogram if enabled
            if field_profile.get("histogram"):
                assert all(
                    key in field_profile["histogram"]
                    for key in ["boundaries", "heights"]
                ), f"Field {field_profile['fieldPath']} has incomplete histogram"

        # Verify event granularity
        assert profile["eventGranularity"]["unit"] == "SECOND", (
            "Incorrect granularity unit"
        )
        assert profile["eventGranularity"]["multiple"] == 30, (
            "Incorrect granularity multiple"
        )
