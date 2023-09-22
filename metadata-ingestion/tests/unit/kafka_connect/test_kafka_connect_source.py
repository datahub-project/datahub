from typing import Any

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers import mce_helpers

KAFKA_CONNECT_SERVER = "http://localhost:28083"


def register_mock_api(request_mock: Any, override_data: dict = {}) -> None:
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

    api_vs_response.update(override_data)

    for url in api_vs_response.keys():
        request_mock.register_uri(
            api_vs_response[url]["method"],
            url,
            json=api_vs_response[url]["json"],
            status_code=api_vs_response[url]["status_code"],
        )


def test_kafka_connect_snowflake_sink_ingest(
    pytestconfig, tmp_path, mock_time, requests_mock
):
    test_resources_dir = pytestconfig.rootpath / "tests/unit/kafka_connect"
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
