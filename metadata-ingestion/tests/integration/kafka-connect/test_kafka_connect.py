import time

import pytest
import requests
from click.testing import CliRunner
from freezegun import freeze_time

from datahub.entrypoints import datahub
from tests.test_helpers import fs_helpers, mce_helpers
from tests.test_helpers.click_helpers import assert_result_ok
from tests.test_helpers.docker_helpers import wait_for_port

FROZEN_TIME = "2021-10-25 13:00:00"


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_kafka_connect_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/kafka-connect"
    test_resources_dir_kafka = pytestconfig.rootpath / "tests/integration/kafka"

    # Share Compose configurations between files and projects
    # https://docs.docker.com/compose/extends/
    docker_compose_file = [
        str(test_resources_dir_kafka / "docker-compose.yml"),
        str(test_resources_dir / "docker-compose.override.yml"),
    ]
    with docker_compose_runner(docker_compose_file, "kafka-connect") as docker_services:
        wait_for_port(docker_services, "test_broker", 59092, timeout=120)
        wait_for_port(docker_services, "test_connect", 58083, timeout=120)
        docker_services.wait_until_responsive(
            timeout=30,
            pause=1,
            check=lambda: requests.get(
                "http://localhost:58083/connectors",
            ).status_code
            == 200,
        )
        # Creating MySQL source with no transformations , only topic prefix
        r = requests.post(
            "http://localhost:58083/connectors",
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
            "http://localhost:58083/connectors",
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
            "http://localhost:58083/connectors",
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
            "http://localhost:58083/connectors",
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
                            "connection.url": "${env:MYSQL_CONNECTION_URL}"
                        }
                    }
                    """,
        )
        assert r.status_code == 201  # Created
        # Creating MySQL source with ExtractTopic router transformations - source dataset not added
        r = requests.post(
            "http://localhost:58083/connectors",
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
            "http://localhost:58083/connectors",
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
            "http://localhost:58083/connectors",
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
                            "include.schema.changes": "false"
                        }
                    }
                    """,
        )
        assert r.status_code == 201  # Created

        # Give time for connectors to process the table data
        time.sleep(45)

        # Run the metadata ingestion pipeline.
        runner = CliRunner()
        with fs_helpers.isolated_filesystem(tmp_path):
            print(tmp_path)
            config_file = (test_resources_dir / "kafka_connect_to_file.yml").resolve()
            result = runner.invoke(datahub, ["ingest", "-c", f"{config_file}"])
            # import pdb;pdb.set_trace();
            assert_result_ok(result)

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "kafka_connect_mces.json",
            golden_path=test_resources_dir / "kafka_connect_mces_golden.json",
            ignore_paths=[],
        )
