import json
import logging
import os
import subprocess
import sys
import time
from typing import Callable, Dict

import pytest
import yaml
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from freezegun import freeze_time

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.kafka.kafka import KafkaSource, KafkaSourceConfig
from datahub.testing import mce_helpers
from tests.integration.kafka import oauth  # type: ignore
from tests.test_helpers import test_connection_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port

pytestmark = pytest.mark.integration_batch_4

FROZEN_TIME = "2020-04-14 07:00:00"


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/kafka"


@pytest.fixture(scope="module")
def mock_kafka_service(docker_compose_runner, test_resources_dir):
    print("\nStarting Kafka test services...")

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "kafka"
    ) as docker_services:
        print("Waiting for Kafka broker and Schema Registry...")
        try:
            wait_for_port(docker_services, "test_zookeeper", 52181, timeout=180)
            wait_for_port(docker_services, "test_broker", 29092, timeout=180)
            wait_for_port(docker_services, "test_schema_registry", 8081, timeout=180)

            print("Setting up test data...")
            command = f"{test_resources_dir}/send_records.sh {test_resources_dir}"

            # Pass the current Python executable to the script
            env = os.environ.copy()
            env["DATAHUB_TEST_PYTHON"] = sys.executable

            try:
                result = subprocess.run(
                    command,
                    shell=True,
                    check=True,
                    timeout=60,
                    capture_output=True,
                    text=True,
                    env=env,
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


@pytest.mark.integration
def test_kafka_infrastructure_debug(mock_kafka_service, test_resources_dir):
    """Debug test to verify Kafka infrastructure is working before running profiling tests."""

    print("\nDEBUGGING KAFKA INFRASTRUCTURE")
    print("=" * 60)

    # Test 1: Verify Kafka broker connectivity
    print("1. Testing Kafka broker connectivity...")
    try:
        consumer = Consumer(
            {
                "bootstrap.servers": "localhost:29092",
                "group.id": "debug-test",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

        # List topics
        topics_metadata = consumer.list_topics(timeout=10)
        topic_names = list(topics_metadata.topics.keys())
        print(f"   Connected to Kafka. Found {len(topic_names)} topics: {topic_names}")

        # Check for our expected topics
        expected_topics = ["numeric_topic", "value_topic", "key_value_topic"]
        found_topics = [t for t in expected_topics if t in topic_names]
        missing_topics = [t for t in expected_topics if t not in topic_names]

        print(f"   Expected topics found: {found_topics}")
        if missing_topics:
            print(f"   Missing topics: {missing_topics}")

        consumer.close()

    except Exception as e:
        print(f"   Kafka broker connection failed: {e}")

    # Test 2: Verify Schema Registry connectivity
    print("\n2. Testing Schema Registry connectivity...")
    try:
        schema_registry = SchemaRegistryClient({"url": "http://localhost:28081"})
        subjects = schema_registry.get_subjects()
        print(
            f"   Connected to Schema Registry. Found {len(subjects)} subjects: {subjects}"
        )

        # Check for expected schemas
        expected_subjects = [
            "numeric_topic-value",
            "value_topic-value",
            "key_value_topic-value",
            "key_value_topic-key",
        ]
        found_subjects = [s for s in expected_subjects if s in subjects]
        missing_subjects = [s for s in expected_subjects if s not in subjects]

        print(f"   Expected subjects found: {found_subjects}")
        if missing_subjects:
            print(f"   Missing subjects: {missing_subjects}")

    except Exception as e:
        print(f"   Schema Registry connection failed: {e}")

    # Test 3: Try to read messages from numeric_topic
    print("\n3. Testing message reading from numeric_topic...")
    try:
        consumer = Consumer(
            {
                "bootstrap.servers": "localhost:29092",
                "group.id": "debug-test-2",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

        # Subscribe to numeric_topic
        consumer.subscribe(["numeric_topic"])

        messages_found = 0
        timeout_seconds = 10
        print(f"   Polling for messages (timeout: {timeout_seconds}s)...")

        import time

        start_time = time.time()
        while time.time() - start_time < timeout_seconds and messages_found < 5:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"   Consumer error: {msg.error()}")
                continue

            messages_found += 1
            print(
                f"   Message {messages_found}: offset={msg.offset()}, key={msg.key()}, value_size={len(msg.value()) if msg.value() else 0}"
            )

        consumer.close()

        if messages_found > 0:
            print(f"   Successfully read {messages_found} messages from numeric_topic")
        else:
            print("   No messages found in numeric_topic!")

    except Exception as e:
        print(f"   Message reading failed: {e}")

    print("\nInfrastructure debug complete")


def _run_kafka_profiling_pipeline(config_file, tmp_path):
    """Run the Kafka profiling pipeline and return MCEs."""
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    output_file = tmp_path / "kafka_profiling_mces.json"
    if not output_file.exists():
        raise FileNotFoundError(f"Expected output file {output_file} was not created")

    with open(output_file, "r") as f:
        return json.load(f)


def _analyze_mce_summary(mces):
    """Analyze and print MCE summary for debugging."""
    from typing import Dict, List

    mce_summary: Dict[str, List[str]] = {}
    for mce in mces:
        if isinstance(mce, dict):
            entity_type = mce.get("entityType", "unknown")
            aspect_name = mce.get("aspectName", "unknown")
            entity_urn = mce.get("entityUrn", "unknown")

            key = f"{entity_type}:{aspect_name}"
            if key not in mce_summary:
                mce_summary[key] = []
            mce_summary[key].append(entity_urn)

    print("MCE Summary:")
    for key, urns in mce_summary.items():
        print(f"   {key}: {len(urns)} entities")
        for urn in urns[:3]:  # Show first 3 URNs
            print(f"      - {urn}")
        if len(urns) > 3:
            print(f"      ... and {len(urns) - 3} more")

    return mce_summary


def _extract_profile_aspects(mces):
    """Extract profile aspects from MCEs."""
    profile_aspects = []
    for mce in mces:
        if (
            isinstance(mce, dict)
            and mce.get("entityType") == "dataset"
            and mce.get("aspectName") == "datasetProfile"
            and mce.get("aspect", {}).get("json")
        ):
            profile_aspects.append(mce["aspect"]["json"])
    return profile_aspects


def _debug_profile_summaries(profile_aspects, mces):
    """Print debug summaries of profiles."""
    for i, profile in enumerate(profile_aspects):
        entity_urn = _find_entity_urn_for_profile(profile, mces)
        print(f"   Profile {i + 1}: {entity_urn}")
        print(f"      Column count: {profile.get('columnCount', 0)}")
        print(f"      Field profiles: {len(profile.get('fieldProfiles', []))}")

        # Show field profile summary
        _show_field_profile_summary(profile)


def _find_entity_urn_for_profile(profile, mces):
    """Find the entity URN for a given profile."""
    for mce in mces:
        if (
            mce.get("aspectName") == "datasetProfile"
            and mce.get("aspect", {}).get("json") == profile
        ):
            return mce.get("entityUrn", "unknown")
    return "unknown"


def _show_field_profile_summary(profile):
    """Show summary of field profiles."""
    for field_profile in profile.get("fieldProfiles", [])[:3]:  # First 3 fields
        field_path = field_profile.get("fieldPath", "unknown")
        has_stats = any(
            [
                field_profile.get("min") is not None,
                field_profile.get("max") is not None,
                field_profile.get("mean") is not None,
                field_profile.get("median") is not None,
            ]
        )
        print(f"         - {field_path}: {'has stats' if has_stats else 'no stats'}")


def _validate_basic_profile_structure(profile):
    """Validate basic profile structure."""
    assert "timestampMillis" in profile, "Profile missing timestampMillis"
    assert "columnCount" in profile, "Profile missing columnCount"
    assert profile["columnCount"] > 0, "Profile has no columns"
    assert "fieldProfiles" in profile, "Profile missing fieldProfiles"
    assert "partitionSpec" in profile, "Profile missing partitionSpec"
    assert profile["partitionSpec"]["partition"].startswith("SAMPLE ("), (
        "Incorrect partition spec format"
    )


def _validate_numeric_field_statistics(field_profile, field_path, config=None):
    """Validate numeric field statistics based on configuration."""
    # Define which stats are configurable
    configurable_stats = {
        "min": getattr(config, "include_field_min_value", True) if config else True,
        "max": getattr(config, "include_field_max_value", True) if config else True,
        "mean": getattr(config, "include_field_mean_value", True) if config else True,
        "median": getattr(config, "include_field_median_value", True)
        if config
        else True,
    }

    # Only require stats that are enabled in config
    required_stats = [stat for stat, enabled in configurable_stats.items() if enabled]
    available_stats = [
        stat
        for stat in required_stats
        if stat in field_profile and field_profile[stat] is not None
    ]
    missing_stats = [
        stat
        for stat in required_stats
        if stat not in field_profile or field_profile[stat] is None
    ]

    print(f"         Required stats (per config): {required_stats}")
    print(f"         Available stats: {available_stats}")
    print(f"         Missing stats: {missing_stats}")

    # Show actual values for debugging
    for stat in ["min", "max", "mean", "median"]:  # Show all for debugging
        value = field_profile.get(stat)
        enabled = configurable_stats.get(stat, False)
        print(
            f"         {stat}: {value} (type: {type(value).__name__}, enabled: {enabled})"
        )

    # Only assert if we have required stats enabled
    if required_stats:
        has_any_required_stat = any(
            key in field_profile and field_profile[key] is not None
            for key in required_stats
        )

        if not has_any_required_stat:
            print("         FAILURE DETAILS:")
            print(f"            Field path: {field_path}")
            print(f"            All field properties: {dict(field_profile)}")

        assert has_any_required_stat, (
            f"Numeric field {field_path} missing required statistical properties. "
            f"Required: {required_stats}, Available: {available_stats}, Missing: {missing_stats}, "
            f"All properties: {list(field_profile.keys())}"
        )

    # Validate standard deviation
    _validate_standard_deviation(field_profile, field_path, config)

    # Validate quantiles
    _validate_quantiles(field_profile, field_path, config)


def _validate_standard_deviation(field_profile, field_path, config=None):
    """Validate standard deviation field based on configuration."""
    # Default profiling config has include_field_stddev_value=True
    should_have_stdev = (
        getattr(config, "include_field_stddev_value", True) if config else True
    )

    if should_have_stdev:
        if "stdev" not in field_profile or field_profile["stdev"] is None:
            print(f"         Missing stdev for {field_path} (required by config)")
            available_props = [k for k, v in field_profile.items() if v is not None]
            raise AssertionError(
                f"Field {field_path} missing standard deviation. Available props: {available_props}"
            )
        else:
            print(f"         Has stdev: {field_profile['stdev']}")
    else:
        if "stdev" in field_profile and field_profile["stdev"] is not None:
            print(
                f"         Has stdev: {field_profile['stdev']} (not required by config)"
            )
        else:
            print("         No stdev (disabled in config)")


def _validate_quantiles(field_profile, field_path, config=None):
    """Validate quantiles field based on configuration."""
    # Default profiling config has include_field_quantiles=False
    should_have_quantiles = (
        getattr(config, "include_field_quantiles", False) if config else False
    )

    if should_have_quantiles:
        if "quantiles" not in field_profile or not field_profile["quantiles"]:
            print(f"         Missing quantiles for {field_path} (required by config)")
        else:
            assert isinstance(field_profile["quantiles"], list), (
                f"Field {field_path} has invalid quantiles format"
            )
            print(f"         Has quantiles: {len(field_profile['quantiles'])} items")
    else:
        if "quantiles" in field_profile and field_profile["quantiles"]:
            assert isinstance(field_profile["quantiles"], list), (
                f"Field {field_path} has invalid quantiles format"
            )
            print(
                f"         Has quantiles: {len(field_profile['quantiles'])} items (not required by config)"
            )
        else:
            print("         No quantiles (disabled in config)")


def _get_test_profiling_config():
    """Get the profiling configuration used in the test."""

    # Based on kafka_profiling_to_file.yml
    class TestConfig:
        include_field_sample_values = True
        include_field_null_count = True
        include_field_min_value = True
        include_field_max_value = True
        include_field_mean_value = True
        include_field_median_value = True
        include_field_stddev_value = True
        include_field_quantiles = True
        include_field_distinct_count = True  # Default from GEProfilingConfig
        include_field_distinct_value_frequencies = True
        include_field_histogram = True

    return TestConfig()


def _validate_field_profile(field_profile):
    """Validate individual field profile."""
    assert "fieldPath" in field_profile, "Field profile missing fieldPath"

    field_path = field_profile["fieldPath"]
    print(f"      Field: {field_path}")

    # Show all available properties for debugging
    available_props = [k for k, v in field_profile.items() if v is not None]
    print(f"         Available properties: {available_props}")

    # Get the test configuration
    config = _get_test_profiling_config()

    # Check if this is a numeric field
    is_numeric_field = field_path.endswith((".id", ".value", ".count"))
    print(f"         Is numeric field: {is_numeric_field}")

    if is_numeric_field:
        _validate_numeric_field_statistics(field_profile, field_path, config)
    else:
        print("         Non-numeric field, skipping stat checks")

    # Validate common field properties
    _validate_common_field_properties(field_profile, config)


def _validate_common_field_properties(field_profile, config=None):
    """Validate common field properties based on configuration."""
    field_path = field_profile["fieldPath"]

    # Only validate sample values if they're enabled in config
    # Default profiling config has include_field_sample_values=True
    if config is None or getattr(config, "include_field_sample_values", True):
        assert "sampleValues" in field_profile, (
            f"Field {field_path} missing sample values"
        )

    # Only validate frequencies if they're enabled in config
    # Default profiling config has include_field_distinct_value_frequencies=False
    if config is None or getattr(
        config, "include_field_distinct_value_frequencies", False
    ):
        assert "distinctValueFrequencies" in field_profile, (
            f"Field {field_path} missing frequencies"
        )

    # Only validate distinct counts if they're enabled in config
    # Default profiling config has include_field_distinct_count=True
    if config is None or getattr(config, "include_field_distinct_count", True):
        if "uniqueCount" in field_profile:
            assert "uniqueProportion" in field_profile, (
                f"Field {field_path} missing uniqueProportion"
            )

    # Only validate histogram if it's enabled in config
    # Default profiling config has include_field_histogram=False
    if (
        config is None or getattr(config, "include_field_histogram", False)
    ) and field_profile.get("histogram"):
        assert all(
            key in field_profile["histogram"] for key in ["boundaries", "heights"]
        ), f"Field {field_path} has incomplete histogram"


def _validate_event_granularity(profile):
    """Validate event granularity settings."""
    assert profile["eventGranularity"]["unit"] == "SECOND", "Incorrect granularity unit"
    assert profile["eventGranularity"]["multiple"] == 30, (
        "Incorrect granularity multiple"
    )


@pytest.mark.parametrize(
    "test_case",
    [
        "numeric_profiling",  # Test numeric_topic FIRST (most likely to fail)
        "basic_profiling",  # Use value_topic
        "schema_profiling",  # Use key_value_topic
    ],
)
@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_kafka_profiling(
    mock_kafka_service, test_resources_dir, pytestconfig, tmp_path, mock_time, test_case
):
    print(f"\nTESTING KAFKA PROFILING - {test_case.upper()}")
    print("=" * 60)

    # Run the metadata ingestion pipeline with profiling enabled
    config_file = (test_resources_dir / "kafka_profiling_to_file.yml").resolve()
    print(f"Using config: {config_file}")

    mces = _run_kafka_profiling_pipeline(config_file, tmp_path)
    print(f"Total MCEs found: {len(mces)}")

    # Analyze and debug MCE summary
    mce_summary = _analyze_mce_summary(mces)

    # Extract and debug profile aspects
    profile_aspects = _extract_profile_aspects(mces)
    print(f"Profile aspects found: {len(profile_aspects)}")
    _debug_profile_summaries(profile_aspects, mces)

    # Assert we found profile data
    assert len(profile_aspects) > 0, (
        f"No profile aspects found in output! Found {len(mces)} total MCEs. MCE types: {list(mce_summary.keys())}"
    )

    # Validate each profile
    for profile in profile_aspects:
        _validate_basic_profile_structure(profile)

        # Check field profiles based on config
        print(f"   Analyzing {len(profile['fieldProfiles'])} field profiles...")
        for field_profile in profile["fieldProfiles"]:
            _validate_field_profile(field_profile)

        # Validate event granularity
        _validate_event_granularity(profile)
