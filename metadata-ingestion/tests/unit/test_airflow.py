import json
import os
from contextlib import contextmanager
from typing import Iterator
from unittest import mock

import airflow.configuration
import airflow.version
import pytest
from airflow.lineage import apply_lineage, prepare_lineage
from airflow.models import DAG, Connection, DagBag
from airflow.models import TaskInstance as TI
from airflow.utils.dates import days_ago

try:
    from airflow.operators.dummy import DummyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy_operator import DummyOperator

import datahub.emitter.mce_builder as builder
from datahub_provider import get_provider_info
from datahub_provider.entities import Dataset
from datahub_provider.hooks.datahub import DatahubKafkaHook, DatahubRestHook
from datahub_provider.operators.datahub import DatahubEmitterOperator

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


def setup_module(module):
    airflow.configuration.conf.load_test_config()


def test_airflow_provider_info():
    assert get_provider_info()


def test_dags_load_with_no_errors(pytestconfig):
    airflow_examples_folder = (
        pytestconfig.rootpath / "src/datahub_provider/example_dags"
    )

    dag_bag = DagBag(dag_folder=str(airflow_examples_folder), include_examples=False)
    assert dag_bag.import_errors == {}
    assert len(dag_bag.dag_ids) > 0


@contextmanager
def patch_airflow_connection(conn: Connection) -> Iterator[Connection]:
    # The return type should really by ContextManager, but mypy doesn't like that.
    # See https://stackoverflow.com/questions/49733699/python-type-hints-and-context-managers#comment106444758_58349659.
    with mock.patch(
        "datahub_provider.hooks.datahub.BaseHook.get_connection", return_value=conn
    ):
        yield conn


@mock.patch("datahub_provider.hooks.datahub.DatahubRestEmitter", autospec=True)
def test_datahub_rest_hook(mock_emitter):
    with patch_airflow_connection(datahub_rest_connection_config) as config:
        hook = DatahubRestHook(config.conn_id)
        hook.emit_mces([lineage_mce])

        mock_emitter.assert_called_once_with(config.host, None)
        instance = mock_emitter.return_value
        instance.emit_mce.assert_called_with(lineage_mce)


@mock.patch("datahub_provider.hooks.datahub.DatahubKafkaEmitter", autospec=True)
def test_datahub_kafka_hook(mock_emitter):
    with patch_airflow_connection(datahub_kafka_connection_config) as config:
        hook = DatahubKafkaHook(config.conn_id)
        hook.emit_mces([lineage_mce])

        mock_emitter.assert_called_once()
        instance = mock_emitter.return_value
        instance.emit_mce_async.assert_called()
        instance.flush.assert_called_once()


@mock.patch("datahub_provider.hooks.datahub.DatahubRestHook.emit_mces")
def test_datahub_lineage_operator(mock_emit):
    with patch_airflow_connection(datahub_rest_connection_config) as config:
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


@pytest.mark.parametrize(
    ["inlets", "outlets"],
    [
        (
            # Airflow 1.10.x uses a dictionary structure for inlets and outlets.
            # We want the lineage backend to support this structure for backwards
            # compatability reasons, so this test is not conditional.
            {"datasets": [Dataset("snowflake", "mydb.schema.tableConsumed")]},
            {"datasets": [Dataset("snowflake", "mydb.schema.tableProduced")]},
        ),
        pytest.param(
            # Airflow 2.x also supports a flattened list for inlets and outlets.
            # We want to test this capability.
            [Dataset("snowflake", "mydb.schema.tableConsumed")],
            [Dataset("snowflake", "mydb.schema.tableProduced")],
            marks=pytest.mark.skipif(
                airflow.version.version.startswith("1"),
                reason="list-style lineage is only supported in Airflow 2.x",
            ),
        ),
    ],
    ids=[
        "airflow-1-10-x-decl",
        "airflow-2-x-decl",
    ],
)
@mock.patch("datahub_provider.hooks.datahub.DatahubRestHook.emit_mces")
def test_lineage_backend(mock_emit, inlets, outlets):
    DEFAULT_DATE = days_ago(2)

    with mock.patch.dict(
        os.environ,
        {
            "AIRFLOW__LINEAGE__BACKEND": "datahub_provider.lineage.datahub.DatahubLineageBackend",
            "AIRFLOW__LINEAGE__DATAHUB_CONN_ID": datahub_rest_connection_config.conn_id,
            "AIRFLOW__LINEAGE__DATAHUB_KWARGS": json.dumps(
                {"graceful_exceptions": False}
            ),
        },
    ), mock.patch("airflow.models.BaseOperator.xcom_pull", autospec=True), mock.patch(
        "airflow.models.BaseOperator.xcom_push", autospec=True
    ), patch_airflow_connection(
        datahub_rest_connection_config
    ):
        func = mock.Mock()
        func.__name__ = "foo"

        dag = DAG(dag_id="test_lineage_is_sent_to_backend", start_date=DEFAULT_DATE)

        with dag:
            op1 = DummyOperator(
                task_id="task1_upstream",
                inlets=inlets,
                outlets=outlets,
            )
            op2 = DummyOperator(
                task_id="task2",
                inlets=inlets,
                outlets=outlets,
            )
            op1 >> op2

        ti = TI(task=op2, execution_date=DEFAULT_DATE)
        ctx1 = {
            "dag": dag,
            "task": op2,
            "ti": ti,
            "task_instance": ti,
            "execution_date": DEFAULT_DATE,
            "ts": "2021-04-08T00:54:25.771575+00:00",
        }

        prep = prepare_lineage(func)
        prep(op2, ctx1)
        post = apply_lineage(func)
        post(op2, ctx1)

        # Verify that the inlets and outlets are registered and recognized by Airflow correctly,
        # or that our lineage backend forces it to.
        assert len(op2.inlets) == 1
        assert len(op2.outlets) == 1
        assert all(map(lambda let: isinstance(let, Dataset), op2.inlets))
        assert all(map(lambda let: isinstance(let, Dataset), op2.outlets))

        # Check that the right things were emitted.
        mock_emit.assert_called_once()
        assert len(mock_emit.call_args[0][0]) == 4
        assert all(mce.validate() for mce in mock_emit.call_args[0][0])
