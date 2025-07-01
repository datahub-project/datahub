import logging
import subprocess
from typing import Any, Dict, List, Optional, cast
from unittest import mock

import jpype
import jpype.imports
import pytest
import requests
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.kafka_connect.kafka_connect import SinkTopicFilter
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.testing import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    validate_all_providers_have_committed_successfully,
)

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.integration_batch_1
FROZEN_TIME = "2021-10-25 13:00:00"
GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"
KAFKA_CONNECT_SERVER = "http://localhost:28083"
KAFKA_CONNECT_ENDPOINT = f"{KAFKA_CONNECT_SERVER}/connectors"


def check_connectors_ready(
    server_url: str = "http://localhost:28083", only_plugins: bool = False
) -> bool:
    """
    Check if Kafka Connect is fully initialized with plugins installed and all connectors are in a RUNNING state.

    Args:
        server_url: The base URL of the Kafka Connect REST API
        only_plugins: If True, only check if the connector plugins are installed

    Returns:
        bool: True if all connectors are running, False otherwise
    """
    try:
        # Check connector plugins are installed
        response = requests.get(f"{server_url}/connector-plugins")
        logger.debug(
            f"check-connectors-ready: connector-plugins: {response.status_code} {response.json()}"
        )
        response.raise_for_status()
        if not response.json():
            return False

        if only_plugins:
            return True

        # Get list of all connectors
        connectors_response = requests.get(f"{server_url}/connectors")
        logger.debug(
            f"check-connectors-ready: connector: {connectors_response.status_code} {connectors_response.json()}"
        )
        connectors_response.raise_for_status()
        connectors = connectors_response.json()
        if not connectors:  # Empty list means no connectors yet
            return False

        for connector in connectors:
            # Based on experience, these connectors can be in FAILED state and still work for tests:
            if connector in ["mysql_sink", "bigquery-sink-connector"]:
                logger.debug(
                    f"check-connectors-ready: skipping validation for {connector} as it can be in FAILED state for tests"
                )
                continue

            # Check status of each connector
            status_response = requests.get(
                f"{server_url}/connectors/{connector}/status"
            )
            logger.debug(
                f"check-connectors-ready: connector {connector}: {status_response.status_code} {status_response.json()}"
            )
            status_response.raise_for_status()
            status = status_response.json()
            if status.get("connector", {}).get("state") != "RUNNING":
                logger.debug(
                    f"check-connectors-ready: connector {connector} is not running"
                )
                return False

            # Check all tasks are running
            for task in status.get("tasks", []):
                if task.get("state") != "RUNNING":
                    logger.debug(
                        f"check-connectors-ready: connector {connector} task {task} is not running"
                    )
                    return False

            # Check topics were provisioned
            topics_response = requests.get(
                f"{server_url}/connectors/{connector}/topics"
            )
            topics_response.raise_for_status()
            topics_data = topics_response.json()

            if topics_data and topics_data.get(connector, {}).get("topics"):
                logger.debug(
                    f"Connector {connector} topics: {topics_data[connector]['topics']}"
                )
            else:
                logger.debug(f"Connector {connector} topics not found yet!")
                return False

        return True
    except Exception as e:  # This will catch any exception and return False
        logger.debug(f"check-connectors-ready: exception: {e}")
        return False


@pytest.fixture(scope="module")
def kafka_connect_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    test_resources_dir_kafka = pytestconfig.rootpath / "tests/integration/kafka"

    # Share Compose configurations between files and projects
    # https://docs.docker.com/compose/extends/
    docker_compose_file = [
        str(test_resources_dir_kafka / "docker-compose.yml"),
        str(test_resources_dir / "docker-compose.override.yml"),
    ]

    with docker_compose_runner(docker_compose_file, "kafka-connect") as docker_services:
        # We rely on Docker health checks to confirm all services are up & healthy

        # However healthcheck for test_connect service is not very trustable, so
        # a double and more robust check here is needed
        docker_services.wait_until_responsive(
            timeout=300,
            pause=10,
            check=lambda: check_connectors_ready(
                KAFKA_CONNECT_SERVER, only_plugins=True
            ),
        )

        yield docker_services


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/kafka-connect"


@pytest.fixture(scope="module")
def loaded_kafka_connect(kafka_connect_runner):
    print("Initializing MongoDB replica set...")
    command = "docker exec test_mongo mongosh test_db -f /scripts/mongo-init.js"
    ret = subprocess.run(command, shell=True, capture_output=True)
    assert ret.returncode == 0

    # Creating MySQL source with no transformations , only topic prefix
    r = requests.post(
        KAFKA_CONNECT_ENDPOINT,
        headers={"Content-Type": "application/json"},
        data="""{
            "name": "mysql_source1",
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "mode": "incrementing",
                "incrementing.column.name": "id",
                "topic.prefix": "test-mysql-jdbc-",
                "tasks.max": "1",
                "connection.url": "${env:MYSQL_CONNECTION_URL}"
            }
        }
        """,
    )
    assert r.status_code == 201  # Created
    # Creating MySQL source with regex router transformations , only topic prefix
    r = requests.post(
        KAFKA_CONNECT_ENDPOINT,
        headers={"Content-Type": "application/json"},
        data="""{
            "name": "mysql_source2",
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "mode": "incrementing",
                "incrementing.column.name": "id",
                "tasks.max": "1",
                "connection.url": "${env:MYSQL_CONNECTION_URL}",
                "transforms": "TotalReplacement",
                "transforms.TotalReplacement.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.TotalReplacement.regex": ".*(book)",
                "transforms.TotalReplacement.replacement": "my-new-topic-$1"
            }
        }
        """,
    )
    assert r.status_code == 201  # Created
    # Creating MySQL source with regex router transformations , no topic prefix, table whitelist
    r = requests.post(
        KAFKA_CONNECT_ENDPOINT,
        headers={"Content-Type": "application/json"},
        data="""{
            "name": "mysql_source3",
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "mode": "incrementing",
                "incrementing.column.name": "id",
                "table.whitelist": "book",
                "tasks.max": "1",
                "connection.url": "${env:MYSQL_CONNECTION_URL}",
                "transforms": "TotalReplacement",
                "transforms.TotalReplacement.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.TotalReplacement.regex": ".*",
                "transforms.TotalReplacement.replacement": "my-new-topic"
            }
        }
        """,
    )
    assert r.status_code == 201  # Created
    # Creating MySQL source with query , topic prefix
    r = requests.post(
        KAFKA_CONNECT_ENDPOINT,
        headers={"Content-Type": "application/json"},
        data="""{
                        "name": "mysql_source4",
                        "config": {
                            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                            "mode": "incrementing",
                            "incrementing.column.name": "id",
                            "query": "select * from member",
                            "topic.prefix": "query-topic",
                            "tasks.max": "1",
                            "connection.url": "jdbc:mysql://foo:datahub@test_mysql:${env:MYSQL_PORT}/${env:MYSQL_DB}"
                        }
                    }
                    """,
    )
    assert r.status_code == 201  # Created
    # Creating MySQL source with ExtractTopic router transformations - source dataset not added
    r = requests.post(
        KAFKA_CONNECT_ENDPOINT,
        headers={"Content-Type": "application/json"},
        data="""{
                    "name": "mysql_source5",
                    "config": {
                        "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                        "mode": "incrementing",
                        "incrementing.column.name": "id",
                        "table.whitelist": "book",
                        "topic.prefix": "test-mysql-jdbc2-",
                        "tasks.max": "1",
                        "connection.url": "${env:MYSQL_CONNECTION_URL}",
                        "transforms": "changetopic",
                        "transforms.changetopic.type": "io.confluent.connect.transforms.ExtractTopic$Value",
                        "transforms.changetopic.field": "name"
                    }
                }
                """,
    )
    assert r.status_code == 201  # Created
    # Creating MySQL sink connector - not added
    r = requests.post(
        KAFKA_CONNECT_ENDPOINT,
        headers={"Content-Type": "application/json"},
        data="""{
            "name": "mysql_sink",
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                "insert.mode": "insert",
                "auto.create": true,
                "topics": "my-topic",
                "tasks.max": "1",
                "connection.url": "${env:MYSQL_CONNECTION_URL}"
            }
        }
        """,
    )
    assert r.status_code == 201  # Created

    # Creating Debezium MySQL source connector
    r = requests.post(
        KAFKA_CONNECT_ENDPOINT,
        headers={"Content-Type": "application/json"},
        data="""{
            "name": "debezium-mysql-connector",
            "config": {
                "name": "debezium-mysql-connector",
                "connector.class": "io.debezium.connector.mysql.MySqlConnector",
                "database.hostname": "test_mysql",
                "database.port": "3306",
                "database.user": "root",
                "database.password": "rootpwd",
                "database.server.name": "debezium.topics",
                "database.history.kafka.bootstrap.servers": "test_broker:9092",
                "database.history.kafka.topic": "dbhistory.debeziummysql",
                "database.allowPublicKeyRetrieval": "true",
                "include.schema.changes": "false"
            }
        }
        """,
    )
    assert r.status_code == 201  # Created

    # Creating Postgresql source
    r = requests.post(
        KAFKA_CONNECT_ENDPOINT,
        headers={"Content-Type": "application/json"},
        data="""{
            "name": "postgres_source",
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "mode": "incrementing",
                "incrementing.column.name": "id",
                "table.whitelist": "member",
                "topic.prefix": "test-postgres-jdbc-",
                "tasks.max": "1",
                "connection.url": "${env:POSTGRES_CONNECTION_URL}"
            }
        }""",
    )
    assert r.status_code == 201  # Created

    # Creating Generic source
    r = requests.post(
        KAFKA_CONNECT_ENDPOINT,
        headers={"Content-Type": "application/json"},
        data="""{
            "name": "generic_source",
            "config": {
                "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
                "kafka.topic": "my-topic",
                "quickstart": "product",
                "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "max.interval": 1000,
                "iterations": 10000000,
                "tasks.max": "1"
            }
        }""",
    )
    r.raise_for_status()
    assert r.status_code == 201  # Created

    print("Populating MongoDB with test data...")
    # we populate the database before creating the connector
    # and we use copy_existing mode to copy the data from the database to the kafka topic
    # so ingestion is consistent and deterministic
    command = "docker exec test_mongo mongosh test_db -f /scripts/mongo-populate.js"
    ret = subprocess.run(command, shell=True, capture_output=True)
    assert ret.returncode == 0

    # Creating MongoDB source
    r = requests.post(
        KAFKA_CONNECT_ENDPOINT,
        headers={"Content-Type": "application/json"},
        data=r"""{
            "name": "source_mongodb_connector",
            "config": {
                "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
                "connection.uri": "mongodb://test_mongo:27017",
                "topic.prefix": "mongodb",
                "database": "test_db",
                "collection": "purchases",
                "startup.mode": "copy_existing"
            }
        }""",
    )
    r.raise_for_status()
    assert r.status_code == 201  # Created

    # Creating S3 Sink source
    r = requests.post(
        KAFKA_CONNECT_ENDPOINT,
        headers={"Content-Type": "application/json"},
        data=r"""{
                        "name": "confluent_s3_sink_connector",
                        "config": {
                            "aws.access.key.id": "x",
                            "aws.secret.access.key": "x",
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

    # Creating BigQuery sink connector
    r = requests.post(
        KAFKA_CONNECT_ENDPOINT,
        headers={"Content-Type": "application/json"},
        data="""{
            "name": "bigquery-sink-connector",
            "config": {
                "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
                "autoCreateTables": "true",
                "transforms.TableNameTransformation.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.TableNameTransformation.replacement": "my_dest_table_name",
                "topics": "kafka-topic-name",
                "transforms.TableNameTransformation.regex": ".*",
                "transforms": "TableNameTransformation",
                "name": "bigquery-sink-connector",
                "project": "my-gcp-project",
                "defaultDataset": "mybqdataset",
                "datasets": "kafka-topic-name=mybqdataset"
            }
        }
        """,
    )
    assert r.status_code == 201  # Created

    # Connectors should be ready to process data thanks to Docker health checks
    print("Waiting for Kafka Connect connectors to initialize and process data...")
    kafka_connect_runner.wait_until_responsive(
        timeout=120,
        pause=10,
        check=lambda: check_connectors_ready(KAFKA_CONNECT_SERVER, only_plugins=False),
    )

    print("Kafka Connect connectors are ready!")


@freeze_time(FROZEN_TIME)
def test_kafka_connect_ingest(
    loaded_kafka_connect, pytestconfig, tmp_path, test_resources_dir
):
    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir / "kafka_connect_to_file.yml").resolve()
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "kafka_connect_mces.json",
        golden_path=test_resources_dir / "kafka_connect_mces_golden.json",
        ignore_paths=[],
    )


@freeze_time(FROZEN_TIME)
def test_kafka_connect_mongosourceconnect_ingest(
    loaded_kafka_connect, pytestconfig, tmp_path, test_resources_dir
):
    # Run the metadata ingestion pipeline.
    config_file = (test_resources_dir / "kafka_connect_mongo_to_file.yml").resolve()
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "kafka_connect_mces.json",
        golden_path=test_resources_dir / "kafka_connect_mongo_mces_golden.json",
        ignore_paths=[],
    )


@freeze_time(FROZEN_TIME)
def test_kafka_connect_s3sink_ingest(
    loaded_kafka_connect, pytestconfig, tmp_path, test_resources_dir
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


@freeze_time(FROZEN_TIME)
def test_kafka_connect_ingest_stateful(
    loaded_kafka_connect, pytestconfig, tmp_path, mock_datahub_graph, test_resources_dir
):
    output_file_name: str = "kafka_connect_before_mces.json"
    golden_file_name: str = "kafka_connect_before_golden_mces.json"
    output_file_deleted_name: str = "kafka_connect_after_mces.json"
    golden_file_deleted_name: str = "kafka_connect_after_golden_mces.json"

    base_pipeline_config = {
        "run_id": "kafka-connect-stateful-test",
        "pipeline_name": "kafka-connect-stateful",
        "source": {
            "type": "kafka-connect",
            "config": {
                "platform_instance": "connect-instance-1",
                "connect_uri": KAFKA_CONNECT_SERVER,
                "connector_patterns": {"allow": [".*"]},
                "provided_configs": [
                    {
                        "provider": "env",
                        "path_key": "MYSQL_CONNECTION_URL",
                        "value": "jdbc:mysql://test_mysql:3306/librarydb",
                    },
                    {
                        "provider": "env",
                        "path_key": "MYSQL_PORT",
                        "value": "3306",
                    },
                    {
                        "provider": "env",
                        "path_key": "MYSQL_DB",
                        "value": "librarydb",
                    },
                ],
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
            "type": "file",
            "config": {},
        },
    }

    pipeline_run1 = None
    with mock.patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        mock_checkpoint.return_value = mock_datahub_graph
        pipeline_run1_config: Dict[str, Dict[str, Dict[str, Any]]] = dict(  # type: ignore
            base_pipeline_config  # type: ignore
        )
        # Set the special properties for this run
        pipeline_run1_config["source"]["config"]["connector_patterns"]["allow"] = [
            "mysql_source1",
            "mysql_source2",
        ]
        pipeline_run1_config["sink"]["config"]["filename"] = (
            f"{tmp_path}/{output_file_name}"
        )
        pipeline_run1 = Pipeline.create(pipeline_run1_config)
        pipeline_run1.run()
        pipeline_run1.raise_from_status()
        pipeline_run1.pretty_print_summary()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / output_file_name,
            golden_path=f"{test_resources_dir}/{golden_file_name}",
        )

    checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)
    assert checkpoint1
    assert checkpoint1.state

    pipeline_run2 = None
    with mock.patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        mock_checkpoint.return_value = mock_datahub_graph
        pipeline_run2_config: Dict[str, Dict[str, Dict[str, Any]]] = dict(
            base_pipeline_config  # type: ignore
        )
        # Set the special properties for this run
        pipeline_run1_config["source"]["config"]["connector_patterns"]["allow"] = [
            "mysql_source1",
        ]
        pipeline_run2_config["sink"]["config"]["filename"] = (
            f"{tmp_path}/{output_file_deleted_name}"
        )
        pipeline_run2 = Pipeline.create(pipeline_run2_config)
        pipeline_run2.run()
        pipeline_run2.raise_from_status()
        pipeline_run2.pretty_print_summary()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / output_file_deleted_name,
            golden_path=f"{test_resources_dir}/{golden_file_deleted_name}",
        )
    checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)
    assert checkpoint2
    assert checkpoint2.state

    # Validate that all providers have committed successfully.
    validate_all_providers_have_committed_successfully(
        pipeline=pipeline_run1, expected_providers=1
    )
    validate_all_providers_have_committed_successfully(
        pipeline=pipeline_run2, expected_providers=1
    )

    # Perform all assertions on the states. The deleted table should not be
    # part of the second state
    state1 = cast(GenericCheckpointState, checkpoint1.state)
    state2 = cast(GenericCheckpointState, checkpoint2.state)

    difference_pipeline_urns = list(
        state1.get_urns_not_in(type="dataFlow", other_checkpoint_state=state2)
    )

    assert len(difference_pipeline_urns) == 1
    deleted_pipeline_urns: List[str] = [
        "urn:li:dataFlow:(kafka-connect,connect-instance-1.mysql_source2,PROD)"
    ]
    assert sorted(deleted_pipeline_urns) == sorted(difference_pipeline_urns)

    difference_job_urns = list(
        state1.get_urns_not_in(type="dataJob", other_checkpoint_state=state2)
    )
    assert len(difference_job_urns) == 3
    deleted_job_urns = [
        "urn:li:dataJob:(urn:li:dataFlow:(kafka-connect,connect-instance-1.mysql_source2,PROD),librarydb.MixedCaseTable)",
        "urn:li:dataJob:(urn:li:dataFlow:(kafka-connect,connect-instance-1.mysql_source2,PROD),librarydb.book)",
        "urn:li:dataJob:(urn:li:dataFlow:(kafka-connect,connect-instance-1.mysql_source2,PROD),librarydb.member)",
    ]
    assert sorted(deleted_job_urns) == sorted(difference_job_urns)


def register_mock_api(request_mock: Any, override_data: Optional[dict] = None) -> None:
    api_vs_response = {
        "http://localhost:28083": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "version": "7.4.0-ccs",
                "commit": "30969fa33c185e880b9e02044761dfaac013151d",
                "kafka_cluster_id": "MDgRZlZhSZ-4fXhwRR79bw",
            },
        },
    }

    api_vs_response.update(override_data or {})

    for url in api_vs_response:
        request_mock.register_uri(
            api_vs_response[url]["method"],
            url,
            json=api_vs_response[url]["json"],
            status_code=api_vs_response[url]["status_code"],
        )


@freeze_time(FROZEN_TIME)
def test_kafka_connect_snowflake_sink_ingest(
    pytestconfig, tmp_path, mock_time, requests_mock
):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/kafka-connect"
    override_data = {
        "http://localhost:28083/connectors": {
            "method": "GET",
            "status_code": 200,
            "json": ["snowflake_sink1"],
        },
        "http://localhost:28083/connectors/snowflake_sink1": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "name": "snowflake_sink1",
                "config": {
                    "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
                    "snowflake.database.name": "kafka_db",
                    "snowflake.schema.name": "kafka_schema",
                    "snowflake.topic2table.map": "topic1:table1",
                    "tasks.max": "1",
                    "topics": "topic1,_topic+2",
                    "snowflake.user.name": "kafka_connector_user_1",
                    "snowflake.private.key": "rrSnqU=",
                    "name": "snowflake_sink1",
                    "snowflake.url.name": "bcaurux-lc62744.snowflakecomputing.com:443",
                },
                "tasks": [{"connector": "snowflake_sink1", "task": 0}],
                "type": "sink",
            },
        },
        "http://localhost:28083/connectors/snowflake_sink1/topics": {
            "method": "GET",
            "status_code": 200,
            "json": {
                "snowflake_sink1": {"topics": ["topic1", "_topic+2", "extra_old_topic"]}
            },
        },
    }

    register_mock_api(request_mock=requests_mock, override_data=override_data)

    pipeline = Pipeline.create(
        {
            "run_id": "kafka-connect-test",
            "source": {
                "type": "kafka-connect",
                "config": {
                    "platform_instance": "connect-instance-1",
                    "connect_uri": KAFKA_CONNECT_SERVER,
                    "connector_patterns": {
                        "allow": [
                            "snowflake_sink1",
                        ]
                    },
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": f"{tmp_path}/kafka_connect_snowflake_sink_mces.json",
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()
    golden_file = "kafka_connect_snowflake_sink_mces_golden.json"

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "kafka_connect_snowflake_sink_mces.json",
        golden_path=f"{test_resources_dir}/{golden_file}",
    )


@freeze_time(FROZEN_TIME)
def test_kafka_connect_bigquery_sink_ingest(
    loaded_kafka_connect, pytestconfig, tmp_path, test_resources_dir
):
    # Run the metadata ingestion pipeline.
    config_file = (
        test_resources_dir / "kafka_connect_bigquery_sink_to_file.yml"
    ).resolve()
    run_datahub_cmd(["ingest", "-c", f"{config_file}"], tmp_path=tmp_path)

    # Verify the output.
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=tmp_path / "kafka_connect_mces.json",
        golden_path=test_resources_dir / "kafka_connect_bigquery_sink_mces_golden.json",
        ignore_paths=[],
    )


def test_filter_stale_topics_topics_list():
    """
    Test case for filter_stale_topics method when sink_config has 'topics' key.
    """
    # Create an instance of SinkTopicFilter
    sink_filter = SinkTopicFilter()

    # Set up test data
    processed_topics = ["topic1", "topic2", "topic3", "topic4"]
    sink_config = {"topics": "topic1,topic3,topic5"}

    # Call the method under test
    result = sink_filter.filter_stale_topics(processed_topics, sink_config)

    # Assert the expected result
    expected_result = ["topic1", "topic3"]
    assert result == expected_result, f"Expected {expected_result}, but got {result}"


def test_filter_stale_topics_regex_filtering():
    """
    Test filter_stale_topics when using topics.regex for filtering.
    """
    if not jpype.isJVMStarted():
        jpype.startJVM()

    # Create an instance of SinkTopicFilter
    sink_filter = SinkTopicFilter()

    # Set up test data
    processed_topics = ["topic1", "topic2", "other_topic", "test_topic"]
    sink_config = {"topics.regex": "topic.*"}

    # Call the method under test
    result = sink_filter.filter_stale_topics(processed_topics, sink_config)

    # Assert the result matches the expected filtered topics
    assert result == ["topic1", "topic2"]


def test_filter_stale_topics_no_topics_config():
    """
    Test filter_stale_topics when using neither topics.regex not topics
    Ideally, this will never happen for kafka-connect sink connector
    """

    # Create an instance of SinkTopicFilter
    sink_filter = SinkTopicFilter()

    # Set up test data
    processed_topics = ["topic1", "topic2", "other_topic", "test_topic"]
    sink_config = {"X": "Y"}

    # Call the method under test
    result = sink_filter.filter_stale_topics(processed_topics, sink_config)

    # Assert the result matches the expected filtered topics
    assert result == processed_topics
