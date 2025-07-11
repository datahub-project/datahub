import json
from contextlib import contextmanager
from typing import Iterator
from unittest import mock

import airflow.configuration
import pytest
from airflow.models import Connection, DagBag

import datahub.emitter.mce_builder as builder
from datahub.ingestion.graph.config import ClientMode
from datahub_airflow_plugin import get_provider_info
from datahub_airflow_plugin._airflow_shims import AIRFLOW_PATCHED
from datahub_airflow_plugin.entities import Dataset, Urn
from datahub_airflow_plugin.hooks.datahub import DatahubKafkaHook, DatahubRestHook
from datahub_airflow_plugin.operators.datahub import DatahubEmitterOperator

assert AIRFLOW_PATCHED

# TODO: Remove default_view="tree" arg. Figure out why is default_view being picked as "grid" and how to fix it ?


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
    host="http://test_host:8080",
    extra=None,
)
datahub_rest_connection_config_with_timeout = Connection(
    conn_id="datahub_rest_test",
    conn_type="datahub_rest",
    host="http://test_host:8080",
    extra=json.dumps({"timeout_sec": 5}),
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


def setup_module(module):
    airflow.configuration.conf.load_test_config()


def test_airflow_provider_info():
    assert get_provider_info()


@pytest.mark.filterwarnings("ignore:.*is deprecated.*")
def test_dags_load_with_no_errors(pytestconfig: pytest.Config) -> None:
    airflow_examples_folder = (
        pytestconfig.rootpath / "src/datahub_airflow_plugin/example_dags"
    )

    # Note: the .airflowignore file skips the snowflake DAG.
    dag_bag = DagBag(dag_folder=str(airflow_examples_folder), include_examples=False)

    import_errors = dag_bag.import_errors

    assert len(import_errors) == 0
    assert dag_bag.size() > 0


@contextmanager
def patch_airflow_connection(conn: Connection) -> Iterator[Connection]:
    # The return type should really by ContextManager, but mypy doesn't like that.
    # See https://stackoverflow.com/questions/49733699/python-type-hints-and-context-managers#comment106444758_58349659.
    with mock.patch(
        "datahub_provider.hooks.datahub.BaseHook.get_connection", return_value=conn
    ):
        yield conn


@mock.patch("datahub.emitter.rest_emitter.DataHubRestEmitter", autospec=True)
def test_datahub_rest_hook(mock_emitter):
    with patch_airflow_connection(datahub_rest_connection_config) as config:
        assert config.conn_id
        hook = DatahubRestHook(config.conn_id)
        hook.emit_mces([lineage_mce])

        mock_emitter.assert_called_once_with(
            config.host,
            None,
            client_mode=ClientMode.INGESTION,
            datahub_component="airflow-plugin",
        )
        instance = mock_emitter.return_value
        instance.emit.assert_called_with(lineage_mce)


@mock.patch("datahub.emitter.rest_emitter.DataHubRestEmitter", autospec=True)
def test_datahub_rest_hook_with_timeout(mock_emitter):
    with patch_airflow_connection(
        datahub_rest_connection_config_with_timeout
    ) as config:
        assert config.conn_id
        hook = DatahubRestHook(config.conn_id)
        hook.emit_mces([lineage_mce])

        mock_emitter.assert_called_once_with(
            config.host,
            None,
            timeout_sec=5,
            client_mode=ClientMode.INGESTION,
            datahub_component="airflow-plugin",
        )
        instance = mock_emitter.return_value
        instance.emit.assert_called_with(lineage_mce)


@mock.patch("datahub.emitter.kafka_emitter.DatahubKafkaEmitter", autospec=True)
def test_datahub_kafka_hook(mock_emitter):
    with patch_airflow_connection(datahub_kafka_connection_config) as config:
        assert config.conn_id
        hook = DatahubKafkaHook(config.conn_id)
        hook.emit_mces([lineage_mce])

        mock_emitter.assert_called_once()
        instance = mock_emitter.return_value
        instance.emit.assert_called()
        instance.flush.assert_called_once()


@mock.patch("datahub_provider.hooks.datahub.DatahubRestHook.emit")
def test_datahub_lineage_operator(mock_emit):
    with patch_airflow_connection(datahub_rest_connection_config) as config:
        assert config.conn_id
        task = DatahubEmitterOperator(
            task_id="emit_lineage",
            datahub_conn_id=config.conn_id,
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

        mock_emit.assert_called()


@pytest.mark.parametrize(
    "hook",
    [
        DatahubRestHook,
        DatahubKafkaHook,
    ],
)
def test_hook_airflow_ui(hook):
    # Simply ensure that these run without issue. These will also show up
    # in the Airflow UI, where it will be even more clear if something
    # is wrong.
    hook.get_connection_form_widgets()
    hook.get_ui_field_behaviour()


def test_entities():
    assert (
        Dataset("snowflake", "mydb.schema.tableConsumed").urn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableConsumed,PROD)"
    )

    assert (
        Urn(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableConsumed,PROD)"
        ).urn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableConsumed,PROD)"
    )

    assert (
        Urn("urn:li:dataJob:(urn:li:dataFlow:(airflow,testDag,PROD),testTask)").urn
        == "urn:li:dataJob:(urn:li:dataFlow:(airflow,testDag,PROD),testTask)"
    )

    assert (
        Urn(
            "urn:li:dataJob:(urn:li:dataFlow:(airflow,platform.testDag,PROD),testTask)"
        ).urn
        == "urn:li:dataJob:(urn:li:dataFlow:(airflow,platform.testDag,PROD),testTask)"
    )

    with pytest.raises(ValueError, match="invalid"):
        Urn("not a URN")

    with pytest.raises(
        ValueError, match="only supports datasets and upstream datajobs"
    ):
        Urn("urn:li:mlModel:(urn:li:dataPlatform:science,scienceModel,PROD)")
