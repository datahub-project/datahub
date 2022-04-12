import datetime
import json
import os
import sys
from contextlib import contextmanager
from typing import Iterator
from unittest import mock
from unittest.mock import Mock

import airflow.configuration
import airflow.version
import packaging.version
import pytest
from airflow.lineage import apply_lineage, prepare_lineage
from airflow.models import DAG, Connection, DagBag, DagRun, TaskInstance
from airflow.utils.dates import days_ago

try:
    from airflow.operators.dummy import DummyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy_operator import DummyOperator

import datahub.emitter.mce_builder as builder
from datahub_provider import get_provider_info
from datahub_provider.entities import Dataset
from datahub_provider.hooks.datahub import AIRFLOW_1, DatahubKafkaHook, DatahubRestHook
from datahub_provider.operators.datahub import DatahubEmitterOperator

# Approach suggested by https://stackoverflow.com/a/11887885/5004662.
AIRFLOW_VERSION = packaging.version.parse(airflow.version.version)

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
datahub_rest_connection_config_with_timeout = Connection(
    conn_id="datahub_rest_test",
    conn_type="datahub_rest",
    host="http://test_host:8080/",
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


def test_dags_load_with_no_errors(pytestconfig):
    airflow_examples_folder = (
        pytestconfig.rootpath / "src/datahub_provider/example_dags"
    )

    dag_bag = DagBag(dag_folder=str(airflow_examples_folder), include_examples=False)

    import_errors = dag_bag.import_errors
    if AIRFLOW_1:
        # The TaskFlow API is new in Airflow 2.x, so we don't expect that demo DAG
        # to work on earlier versions.
        import_errors = {
            dag_filename: dag_errors
            for dag_filename, dag_errors in import_errors.items()
            if "taskflow" not in dag_filename
        }

    assert import_errors == {}
    assert len(dag_bag.dag_ids) > 0


@contextmanager
def patch_airflow_connection(conn: Connection) -> Iterator[Connection]:
    # The return type should really by ContextManager, but mypy doesn't like that.
    # See https://stackoverflow.com/questions/49733699/python-type-hints-and-context-managers#comment106444758_58349659.
    with mock.patch(
        "datahub_provider.hooks.datahub.BaseHook.get_connection", return_value=conn
    ):
        yield conn


@mock.patch("datahub.emitter.rest_emitter.DatahubRestEmitter", autospec=True)
def test_datahub_rest_hook(mock_emitter):
    with patch_airflow_connection(datahub_rest_connection_config) as config:
        hook = DatahubRestHook(config.conn_id)
        hook.emit_mces([lineage_mce])

        mock_emitter.assert_called_once_with(config.host, None, None)
        instance = mock_emitter.return_value
        instance.emit_mce.assert_called_with(lineage_mce)


@mock.patch("datahub.emitter.rest_emitter.DatahubRestEmitter", autospec=True)
def test_datahub_rest_hook_with_timeout(mock_emitter):
    with patch_airflow_connection(
        datahub_rest_connection_config_with_timeout
    ) as config:
        hook = DatahubRestHook(config.conn_id)
        hook.emit_mces([lineage_mce])

        mock_emitter.assert_called_once_with(config.host, None, 5)
        instance = mock_emitter.return_value
        instance.emit_mce.assert_called_with(lineage_mce)


@mock.patch("datahub.emitter.kafka_emitter.DatahubKafkaEmitter", autospec=True)
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
        pytest.param(
            # Airflow 1.10.x uses a dictionary structure for inlets and outlets.
            # We want the lineage backend to support this structure for backwards
            # compatability reasons, so this test is not conditional.
            {"datasets": [Dataset("snowflake", "mydb.schema.tableConsumed")]},
            {"datasets": [Dataset("snowflake", "mydb.schema.tableProduced")]},
            id="airflow-1-10-lineage-syntax",
        ),
        pytest.param(
            # Airflow 2.x also supports a flattened list for inlets and outlets.
            # We want to test this capability.
            [Dataset("snowflake", "mydb.schema.tableConsumed")],
            [Dataset("snowflake", "mydb.schema.tableProduced")],
            marks=pytest.mark.skipif(
                AIRFLOW_VERSION < packaging.version.parse("2.0.0"),
                reason="list-style lineage is only supported in Airflow 2.x",
            ),
            id="airflow-2-lineage-syntax",
        ),
    ],
)
@mock.patch("datahub_provider.hooks.datahub.DatahubRestHook.make_emitter")
def test_lineage_backend(mock_emit, inlets, outlets):
    DEFAULT_DATE = days_ago(2)
    mock_emitter = Mock()
    mock_emit.return_value = mock_emitter
    # Using autospec on xcom_pull and xcom_push methods fails on Python 3.6.
    with mock.patch.dict(
        os.environ,
        {
            "AIRFLOW__LINEAGE__BACKEND": "datahub_provider.lineage.datahub.DatahubLineageBackend",
            "AIRFLOW__LINEAGE__DATAHUB_CONN_ID": datahub_rest_connection_config.conn_id,
            "AIRFLOW__LINEAGE__DATAHUB_KWARGS": json.dumps(
                {"graceful_exceptions": False, "capture_executions": False}
            ),
        },
    ), mock.patch("airflow.models.BaseOperator.xcom_pull"), mock.patch(
        "airflow.models.BaseOperator.xcom_push"
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

        # Airflow < 2.2 requires the execution_date parameter. Newer Airflow
        # versions do not require it, but will attempt to find the associated
        # run_id in the database if execution_date is provided. As such, we
        # must fake the run_id parameter for newer Airflow versions.
        if AIRFLOW_VERSION < packaging.version.parse("2.2.0"):
            ti = TaskInstance(task=op2, execution_date=DEFAULT_DATE)
        else:
            ti = TaskInstance(task=op2, run_id=f"test_airflow-{DEFAULT_DATE}")
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
        assert mock_emitter.emit.call_count == 9
        # Running further checks based on python version because args only exists in python 3.7+
        if sys.version_info[:3] > (3, 7):
            assert mock_emitter.method_calls[0].args[0].aspectName == "dataFlowInfo"
            assert (
                mock_emitter.method_calls[0].args[0].entityUrn
                == "urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod)"
            )

            assert mock_emitter.method_calls[1].args[0].aspectName == "ownership"
            assert (
                mock_emitter.method_calls[1].args[0].entityUrn
                == "urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod)"
            )

            assert mock_emitter.method_calls[2].args[0].aspectName == "globalTags"
            assert (
                mock_emitter.method_calls[2].args[0].entityUrn
                == "urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod)"
            )

            assert mock_emitter.method_calls[3].args[0].aspectName == "dataJobInfo"
            assert (
                mock_emitter.method_calls[3].args[0].entityUrn
                == "urn:li:dataJob:(urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod),task2)"
            )

            assert (
                mock_emitter.method_calls[4].args[0].aspectName == "dataJobInputOutput"
            )
            assert (
                mock_emitter.method_calls[4].args[0].entityUrn
                == "urn:li:dataJob:(urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod),task2)"
            )
            assert (
                mock_emitter.method_calls[4].args[0].aspect.inputDatajobs[0]
                == "urn:li:dataJob:(urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod),task1_upstream)"
            )
            assert (
                mock_emitter.method_calls[4].args[0].aspect.inputDatasets[0]
                == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableConsumed,PROD)"
            )
            assert (
                mock_emitter.method_calls[4].args[0].aspect.outputDatasets[0]
                == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableProduced,PROD)"
            )

            assert mock_emitter.method_calls[5].args[0].aspectName == "status"
            assert (
                mock_emitter.method_calls[5].args[0].entityUrn
                == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableConsumed,PROD)"
            )

            assert mock_emitter.method_calls[6].args[0].aspectName == "status"
            assert (
                mock_emitter.method_calls[6].args[0].entityUrn
                == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableProduced,PROD)"
            )

            assert mock_emitter.method_calls[7].args[0].aspectName == "ownership"
            assert (
                mock_emitter.method_calls[7].args[0].entityUrn
                == "urn:li:dataJob:(urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod),task2)"
            )

            assert mock_emitter.method_calls[8].args[0].aspectName == "globalTags"
            assert (
                mock_emitter.method_calls[8].args[0].entityUrn
                == "urn:li:dataJob:(urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod),task2)"
            )


@pytest.mark.parametrize(
    ["inlets", "outlets"],
    [
        pytest.param(
            # Airflow 1.10.x uses a dictionary structure for inlets and outlets.
            # We want the lineage backend to support this structure for backwards
            # compatability reasons, so this test is not conditional.
            {"datasets": [Dataset("snowflake", "mydb.schema.tableConsumed")]},
            {"datasets": [Dataset("snowflake", "mydb.schema.tableProduced")]},
            id="airflow-1-10-lineage-syntax",
        ),
        pytest.param(
            # Airflow 2.x also supports a flattened list for inlets and outlets.
            # We want to test this capability.
            [Dataset("snowflake", "mydb.schema.tableConsumed")],
            [Dataset("snowflake", "mydb.schema.tableProduced")],
            marks=pytest.mark.skipif(
                AIRFLOW_VERSION < packaging.version.parse("2.0.0"),
                reason="list-style lineage is only supported in Airflow 2.x",
            ),
            id="airflow-2-lineage-syntax",
        ),
    ],
)
@mock.patch("datahub_provider.hooks.datahub.DatahubRestHook.make_emitter")
def test_lineage_backend_capture_executions(mock_emit, inlets, outlets):
    DEFAULT_DATE = datetime.datetime(2020, 5, 17)
    mock_emitter = Mock()
    mock_emit.return_value = mock_emitter
    # Using autospec on xcom_pull and xcom_push methods fails on Python 3.6.
    with mock.patch.dict(
        os.environ,
        {
            "AIRFLOW__LINEAGE__BACKEND": "datahub_provider.lineage.datahub.DatahubLineageBackend",
            "AIRFLOW__LINEAGE__DATAHUB_CONN_ID": datahub_rest_connection_config.conn_id,
            "AIRFLOW__LINEAGE__DATAHUB_KWARGS": json.dumps(
                {"graceful_exceptions": False, "capture_executions": True}
            ),
        },
    ), mock.patch("airflow.models.BaseOperator.xcom_pull"), mock.patch(
        "airflow.models.BaseOperator.xcom_push"
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

        # Airflow < 2.2 requires the execution_date parameter. Newer Airflow
        # versions do not require it, but will attempt to find the associated
        # run_id in the database if execution_date is provided. As such, we
        # must fake the run_id parameter for newer Airflow versions.
        if AIRFLOW_VERSION < packaging.version.parse("2.2.0"):
            ti = TaskInstance(task=op2, execution_date=DEFAULT_DATE)
            # Ignoring type here because DagRun state is just a sring at Airflow 1
            dag_run = DagRun(state="success", run_id=f"scheduled_{DEFAULT_DATE}")  # type: ignore
            ti.dag_run = dag_run
            ti.start_date = datetime.datetime.utcnow()
            ti.execution_date = DEFAULT_DATE

        else:
            from airflow.utils.state import DagRunState

            ti = TaskInstance(task=op2, run_id=f"test_airflow-{DEFAULT_DATE}")
            dag_run = DagRun(
                state=DagRunState.SUCCESS, run_id=f"scheduled_{DEFAULT_DATE}"
            )
            ti.dag_run = dag_run
            ti.start_date = datetime.datetime.utcnow()
            ti.execution_date = DEFAULT_DATE

        ctx1 = {
            "dag": dag,
            "task": op2,
            "ti": ti,
            "dag_run": dag_run,
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
        assert mock_emitter.emit.call_count == 17
        # Running further checks based on python version because args only exists in python 3.7+
        if sys.version_info[:3] > (3, 7):
            assert mock_emitter.method_calls[0].args[0].aspectName == "dataFlowInfo"
            assert (
                mock_emitter.method_calls[0].args[0].entityUrn
                == "urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod)"
            )

            assert mock_emitter.method_calls[1].args[0].aspectName == "ownership"
            assert (
                mock_emitter.method_calls[1].args[0].entityUrn
                == "urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod)"
            )

            assert mock_emitter.method_calls[2].args[0].aspectName == "globalTags"
            assert (
                mock_emitter.method_calls[2].args[0].entityUrn
                == "urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod)"
            )

            assert mock_emitter.method_calls[3].args[0].aspectName == "dataJobInfo"
            assert (
                mock_emitter.method_calls[3].args[0].entityUrn
                == "urn:li:dataJob:(urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod),task2)"
            )

            assert (
                mock_emitter.method_calls[4].args[0].aspectName == "dataJobInputOutput"
            )
            assert (
                mock_emitter.method_calls[4].args[0].entityUrn
                == "urn:li:dataJob:(urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod),task2)"
            )
            assert (
                mock_emitter.method_calls[4].args[0].aspect.inputDatajobs[0]
                == "urn:li:dataJob:(urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod),task1_upstream)"
            )
            assert (
                mock_emitter.method_calls[4].args[0].aspect.inputDatasets[0]
                == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableConsumed,PROD)"
            )
            assert (
                mock_emitter.method_calls[4].args[0].aspect.outputDatasets[0]
                == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableProduced,PROD)"
            )

            assert mock_emitter.method_calls[5].args[0].aspectName == "status"
            assert (
                mock_emitter.method_calls[5].args[0].entityUrn
                == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableConsumed,PROD)"
            )

            assert mock_emitter.method_calls[6].args[0].aspectName == "status"
            assert (
                mock_emitter.method_calls[6].args[0].entityUrn
                == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableProduced,PROD)"
            )

            assert mock_emitter.method_calls[7].args[0].aspectName == "ownership"
            assert (
                mock_emitter.method_calls[7].args[0].entityUrn
                == "urn:li:dataJob:(urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod),task2)"
            )

            assert mock_emitter.method_calls[8].args[0].aspectName == "globalTags"
            assert (
                mock_emitter.method_calls[8].args[0].entityUrn
                == "urn:li:dataJob:(urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod),task2)"
            )

            assert (
                mock_emitter.method_calls[9].args[0].aspectName
                == "dataProcessInstanceProperties"
            )
            assert (
                mock_emitter.method_calls[9].args[0].entityUrn
                == "urn:li:dataProcessInstance:b6375e5f5faeb543cfb5d7d8a47661fb"
            )

            assert (
                mock_emitter.method_calls[10].args[0].aspectName
                == "dataProcessInstanceRelationships"
            )
            assert (
                mock_emitter.method_calls[10].args[0].entityUrn
                == "urn:li:dataProcessInstance:b6375e5f5faeb543cfb5d7d8a47661fb"
            )
            assert (
                mock_emitter.method_calls[11].args[0].aspectName
                == "dataProcessInstanceInput"
            )
            assert (
                mock_emitter.method_calls[11].args[0].entityUrn
                == "urn:li:dataProcessInstance:b6375e5f5faeb543cfb5d7d8a47661fb"
            )
            assert (
                mock_emitter.method_calls[12].args[0].aspectName
                == "dataProcessInstanceOutput"
            )
            assert (
                mock_emitter.method_calls[12].args[0].entityUrn
                == "urn:li:dataProcessInstance:b6375e5f5faeb543cfb5d7d8a47661fb"
            )
            assert mock_emitter.method_calls[13].args[0].aspectName == "status"
            assert (
                mock_emitter.method_calls[13].args[0].entityUrn
                == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableConsumed,PROD)"
            )
            assert mock_emitter.method_calls[14].args[0].aspectName == "status"
            assert (
                mock_emitter.method_calls[14].args[0].entityUrn
                == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableProduced,PROD)"
            )
            assert (
                mock_emitter.method_calls[15].args[0].aspectName
                == "dataProcessInstanceRunEvent"
            )
            assert (
                mock_emitter.method_calls[15].args[0].entityUrn
                == "urn:li:dataProcessInstance:b6375e5f5faeb543cfb5d7d8a47661fb"
            )
            assert (
                mock_emitter.method_calls[16].args[0].aspectName
                == "dataProcessInstanceRunEvent"
            )
            assert (
                mock_emitter.method_calls[16].args[0].entityUrn
                == "urn:li:dataProcessInstance:b6375e5f5faeb543cfb5d7d8a47661fb"
            )
