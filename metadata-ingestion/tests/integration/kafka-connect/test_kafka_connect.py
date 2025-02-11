import subprocess
from typing import Any, Dict, List, Optional, cast
from unittest import mock

import pytest
import requests
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    validate_all_providers_have_committed_successfully,
)

pytestmark = pytest.mark.integration_batch_1
FROZEN_TIME = "2021-10-25 13:00:00"
GMS_PORT = 8080
GMS_SERVER = f"http://localhost:{GMS_PORT}"
KAFKA_CONNECT_SERVER = "http://localhost:28083"
KAFKA_CONNECT_ENDPOINT = f"{KAFKA_CONNECT_SERVER}/connectors"


def is_mysql_up(container_name: str, port: int) -> bool:
    """A cheap way to figure out if mysql is responsive on a container"""

    cmd = f"docker logs {container_name} 2>&1 | grep '/var/run/mysqld/mysqld.sock' | grep {port}"
    ret = subprocess.run(
        cmd,
        shell=True,
    )
    return ret.returncode == 0


def have_connectors_processed(container_name: str) -> bool:
    """A cheap way to figure out if postgres is responsive on a container"""

    cmd = f"docker logs {container_name} 2>&1 | grep 'Session key updated'"
    return subprocess.run(cmd, shell=True).returncode == 0


@pytest.fixture(scope="module")
def kafka_connect_runner(docker_compose_runner, pytestconfig, test_resources_dir):
    test_resources_dir_kafka = pytestconfig.rootpath / "tests/integration/kafka"

    # Share Compose configurations between files and projects
    # https://docs.docker.com/compose/extends/
    docker_compose_file = [
        str(test_resources_dir_kafka / "docker-compose.yml"),
        str(test_resources_dir / "docker-compose.override.yml"),
    ]
    with docker_compose_runner(
        docker_compose_file, "kafka-connect", cleanup=False
    ) as docker_services:
        wait_for_port(
            docker_services,
            "test_mysql",
            3306,
            timeout=120,
            checker=lambda: is_mysql_up("test_mysql", 3306),
        )

    with docker_compose_runner(docker_compose_file, "kafka-connect") as docker_services:
        # We sometimes run into issues where the broker fails to come up on the first try because
        # of all the other processes that are running. By running docker compose twice, we can
        # avoid some test flakes. How does this work? The "key" is the same between both
        # calls to the docker_compose_runner and the first one sets cleanup=False.

        wait_for_port(docker_services, "test_broker", 29092, timeout=120)
        wait_for_port(docker_services, "test_connect", 28083, timeout=120)
        docker_services.wait_until_responsive(
            timeout=30,
            pause=1,
            check=lambda: requests.get(KAFKA_CONNECT_ENDPOINT).status_code == 200,
        )
        yield docker_services


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/kafka-connect"


@pytest.fixture(scope="module")
def loaded_kafka_connect(kafka_connect_runner):
    # # Setup mongo cluster
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
                "collection": "purchases"
            }
        }""",
    )
    r.raise_for_status()
    assert r.status_code == 201  # Created

    command = "docker exec test_mongo mongosh test_db -f /scripts/mongo-populate.js"
    ret = subprocess.run(command, shell=True, capture_output=True)
    assert ret.returncode == 0

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

    # Give time for connectors to process the table data
    kafka_connect_runner.wait_until_responsive(
        timeout=30,
        pause=1,
        check=lambda: have_connectors_processed("test_connect"),
    )


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

    for url in api_vs_response.keys():
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
            "json": {"snowflake_sink1": {"topics": ["topic1", "_topic+2"]}},
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
