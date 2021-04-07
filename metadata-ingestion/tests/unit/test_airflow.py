import json
from contextlib import contextmanager
from typing import Iterator
from unittest import mock

from airflow.models import Connection, DagBag

import datahub.emitter.mce_builder as builder
from datahub.integrations.airflow.get_provider_info import get_provider_info
from datahub.integrations.airflow.hooks import DatahubKafkaHook, DatahubRestHook
from datahub.integrations.airflow.operators import DatahubEmitterOperator

lineage_mce = builder.make_lineage_mce(
    [
        builder.make_dataset_urn("bigquery", "upstream1"),
        builder.make_dataset_urn("bigquery", "upstream2"),
    ],
    builder.make_dataset_urn("bigquery", "downstream1"),
)

datahub_rest_connection_config = Connection(
    conn_id="datahub_rest_test",
    conn_type="datahub_rest",
    host="http://test_host:8080/",
    extra=None,
)
datahub_kafka_connection_config = Connection(
    conn_id="datahub_kafka_test",
    conn_type="datahub_kafka",
    host="test_broker:9092",
    extra=json.dumps(
        {
            "connection": {
                "producer_config": {},
                "schema_registry_url": "http://localhost:8081",
            }
        }
    ),
)


def test_airflow_provider_info():
    assert get_provider_info()


def test_dags_load_with_no_errors(pytestconfig):
    airflow_examples_folder = pytestconfig.rootpath / "examples/airflow"

    dag_bag = DagBag(dag_folder=str(airflow_examples_folder), include_examples=False)
    assert dag_bag.import_errors == {}
    assert len(dag_bag.dag_ids) > 0


@contextmanager
def patch_airflow_connection(conn: Connection) -> Iterator[Connection]:
    # The return type should really by ContextManager, but mypy doesn't like that.
    # See https://stackoverflow.com/questions/49733699/python-type-hints-and-context-managers#comment106444758_58349659.
    with mock.patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        yield conn


@mock.patch("datahub.integrations.airflow.hooks.DatahubRestEmitter", autospec=True)
def test_datahub_rest_hook(mock_emitter):
    with patch_airflow_connection(datahub_rest_connection_config) as config:
        hook = DatahubRestHook(config.conn_id)
        hook.emit_mces([lineage_mce])

        mock_emitter.assert_called_once_with(config.host)
        instance = mock_emitter.return_value
        instance.emit_mce.assert_called_with(lineage_mce)


@mock.patch("datahub.integrations.airflow.hooks.DatahubKafkaEmitter", autospec=True)
def test_datahub_kafka_hook(mock_emitter):
    with patch_airflow_connection(datahub_kafka_connection_config) as config:
        hook = DatahubKafkaHook(config.conn_id)
        hook.emit_mces([lineage_mce])

        mock_emitter.assert_called_once()
        instance = mock_emitter.return_value
        instance.emit_mce_async.assert_called()
        instance.flush.assert_called_once()


@mock.patch("datahub.integrations.airflow.operators.DatahubRestHook", autospec=True)
def test_datahub_lineage_operator(mock_hook):
    task = DatahubEmitterOperator(
        task_id="emit_lineage",
        datahub_rest_conn_id=datahub_rest_connection_config.conn_id,
        mces=[
            builder.make_lineage_mce(
                [
                    builder.make_dataset_urn("snowflake", "mydb.schema.tableA"),
                    builder.make_dataset_urn("snowflake", "mydb.schema.tableB"),
                ],
                builder.make_dataset_urn("snowflake", "mydb.schema.tableC"),
            )
        ],
    )
    task.execute(None)

    mock_hook.assert_called()
    mock_hook.return_value.emit_mces.assert_called_once()
