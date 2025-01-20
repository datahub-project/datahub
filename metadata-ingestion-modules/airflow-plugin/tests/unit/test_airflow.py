import datetime
import json
import os
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

import datahub.emitter.mce_builder as builder
from datahub_airflow_plugin import get_provider_info
from datahub_airflow_plugin._airflow_shims import (
    AIRFLOW_PATCHED,
    AIRFLOW_VERSION,
    EmptyOperator,
)
from datahub_airflow_plugin.entities import Dataset, Urn
from datahub_airflow_plugin.hooks.datahub import DatahubKafkaHook, DatahubRestHook
from datahub_airflow_plugin.operators.datahub import DatahubEmitterOperator
from tests.utils import PytestConfig

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
def test_dags_load_with_no_errors(pytestconfig: PytestConfig) -> None:
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

        mock_emitter.assert_called_once_with(config.host, None)
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

        mock_emitter.assert_called_once_with(config.host, None, timeout_sec=5)
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

    with pytest.raises(ValueError, match="invalid"):
        Urn("not a URN")

    with pytest.raises(
        ValueError, match="only supports datasets and upstream datajobs"
    ):
        Urn("urn:li:mlModel:(urn:li:dataPlatform:science,scienceModel,PROD)")


@pytest.mark.parametrize(
    ["inlets", "outlets", "capture_executions"],
    [
        pytest.param(
            [
                Dataset("snowflake", "mydb.schema.tableConsumed"),
                Urn("urn:li:dataJob:(urn:li:dataFlow:(airflow,testDag,PROD),testTask)"),
            ],
            [Dataset("snowflake", "mydb.schema.tableProduced")],
            False,
            id="airflow-lineage-no-executions",
        ),
        pytest.param(
            [
                Dataset("snowflake", "mydb.schema.tableConsumed"),
                Urn("urn:li:dataJob:(urn:li:dataFlow:(airflow,testDag,PROD),testTask)"),
            ],
            [Dataset("snowflake", "mydb.schema.tableProduced")],
            True,
            id="airflow-lineage-capture-executions",
        ),
    ],
)
@mock.patch("datahub_provider.hooks.datahub.DatahubRestHook.make_emitter")
def test_lineage_backend(mock_emit, inlets, outlets, capture_executions):
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
                {"graceful_exceptions": False, "capture_executions": capture_executions}
            ),
        },
    ), mock.patch("airflow.models.BaseOperator.xcom_pull"), mock.patch(
        "airflow.models.BaseOperator.xcom_push"
    ), patch_airflow_connection(datahub_rest_connection_config):
        func = mock.Mock()
        func.__name__ = "foo"

        dag = DAG(
            dag_id="test_lineage_is_sent_to_backend",
            start_date=DEFAULT_DATE,
            default_view="tree",
        )

        with dag:
            op1 = EmptyOperator(
                task_id="task1_upstream",
                inlets=inlets,
                outlets=outlets,
            )
            op2 = EmptyOperator(
                task_id="task2",
                inlets=inlets,
                outlets=outlets,
            )
            op1 >> op2

        # Airflow < 2.2 requires the execution_date parameter. Newer Airflow
        # versions do not require it, but will attempt to find the associated
        # run_id in the database if execution_date is provided. As such, we
        # must fake the run_id parameter for newer Airflow versions.
        # We need to add type:ignore in else to suppress mypy error in Airflow < 2.2
        if AIRFLOW_VERSION < packaging.version.parse("2.2.0"):
            ti = TaskInstance(task=op2, execution_date=DEFAULT_DATE)
            # Ignoring type here because DagRun state is just a sring at Airflow 1
            dag_run = DagRun(
                state="success",  # type: ignore[arg-type]
                run_id=f"scheduled_{DEFAULT_DATE.isoformat()}",
            )
        else:
            from airflow.utils.state import DagRunState

            ti = TaskInstance(task=op2, run_id=f"test_airflow-{DEFAULT_DATE}")  # type: ignore[call-arg]
            dag_run = DagRun(
                state=DagRunState.SUCCESS,
                run_id=f"scheduled_{DEFAULT_DATE.isoformat()}",
            )

        ti.dag_run = dag_run  # type: ignore
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
        assert len(op2.inlets) == 2
        assert len(op2.outlets) == 1
        assert all(
            map(
                lambda let: isinstance(let, Dataset) or isinstance(let, Urn), op2.inlets
            )
        )
        assert all(map(lambda let: isinstance(let, Dataset), op2.outlets))

        # Check that the right things were emitted.
        assert mock_emitter.emit.call_count == 19 if capture_executions else 11

        # TODO: Replace this with a golden file-based comparison.
        assert mock_emitter.method_calls[0].args[0].aspectName == "dataFlowInfo"
        assert (
            mock_emitter.method_calls[0].args[0].entityUrn
            == "urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod)"
        )

        assert mock_emitter.method_calls[1].args[0].aspectName == "status"
        assert (
            mock_emitter.method_calls[1].args[0].entityUrn
            == "urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod)"
        )

        assert mock_emitter.method_calls[2].args[0].aspectName == "ownership"
        assert (
            mock_emitter.method_calls[2].args[0].entityUrn
            == "urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod)"
        )

        assert mock_emitter.method_calls[3].args[0].aspectName == "globalTags"
        assert (
            mock_emitter.method_calls[3].args[0].entityUrn
            == "urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod)"
        )

        assert mock_emitter.method_calls[4].args[0].aspectName == "dataJobInfo"
        assert (
            mock_emitter.method_calls[4].args[0].entityUrn
            == "urn:li:dataJob:(urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod),task2)"
        )

        assert mock_emitter.method_calls[5].args[0].aspectName == "status"
        assert (
            mock_emitter.method_calls[5].args[0].entityUrn
            == "urn:li:dataJob:(urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod),task2)"
        )

        assert mock_emitter.method_calls[6].args[0].aspectName == "dataJobInputOutput"
        assert (
            mock_emitter.method_calls[6].args[0].entityUrn
            == "urn:li:dataJob:(urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod),task2)"
        )
        assert (
            mock_emitter.method_calls[6].args[0].aspect.inputDatajobs[0]
            == "urn:li:dataJob:(urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod),task1_upstream)"
        )
        assert (
            mock_emitter.method_calls[6].args[0].aspect.inputDatajobs[1]
            == "urn:li:dataJob:(urn:li:dataFlow:(airflow,testDag,PROD),testTask)"
        )
        assert (
            mock_emitter.method_calls[6].args[0].aspect.inputDatasets[0]
            == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableConsumed,PROD)"
        )
        assert (
            mock_emitter.method_calls[6].args[0].aspect.outputDatasets[0]
            == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableProduced,PROD)"
        )

        assert mock_emitter.method_calls[7].args[0].aspectName == "datasetKey"
        assert (
            mock_emitter.method_calls[7].args[0].entityUrn
            == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableConsumed,PROD)"
        )

        assert mock_emitter.method_calls[8].args[0].aspectName == "datasetKey"
        assert (
            mock_emitter.method_calls[8].args[0].entityUrn
            == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableProduced,PROD)"
        )

        assert mock_emitter.method_calls[9].args[0].aspectName == "ownership"
        assert (
            mock_emitter.method_calls[9].args[0].entityUrn
            == "urn:li:dataJob:(urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod),task2)"
        )

        assert mock_emitter.method_calls[10].args[0].aspectName == "globalTags"
        assert (
            mock_emitter.method_calls[10].args[0].entityUrn
            == "urn:li:dataJob:(urn:li:dataFlow:(airflow,test_lineage_is_sent_to_backend,prod),task2)"
        )

        if capture_executions:
            assert (
                mock_emitter.method_calls[11].args[0].aspectName
                == "dataProcessInstanceProperties"
            )
            assert (
                mock_emitter.method_calls[11].args[0].entityUrn
                == "urn:li:dataProcessInstance:5e274228107f44cc2dd7c9782168cc29"
            )

            assert (
                mock_emitter.method_calls[12].args[0].aspectName
                == "dataProcessInstanceRelationships"
            )
            assert (
                mock_emitter.method_calls[12].args[0].entityUrn
                == "urn:li:dataProcessInstance:5e274228107f44cc2dd7c9782168cc29"
            )
            assert (
                mock_emitter.method_calls[13].args[0].aspectName
                == "dataProcessInstanceInput"
            )
            assert (
                mock_emitter.method_calls[13].args[0].entityUrn
                == "urn:li:dataProcessInstance:5e274228107f44cc2dd7c9782168cc29"
            )
            assert (
                mock_emitter.method_calls[14].args[0].aspectName
                == "dataProcessInstanceOutput"
            )
            assert (
                mock_emitter.method_calls[14].args[0].entityUrn
                == "urn:li:dataProcessInstance:5e274228107f44cc2dd7c9782168cc29"
            )
            assert mock_emitter.method_calls[15].args[0].aspectName == "datasetKey"
            assert (
                mock_emitter.method_calls[15].args[0].entityUrn
                == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableConsumed,PROD)"
            )
            assert mock_emitter.method_calls[16].args[0].aspectName == "datasetKey"
            assert (
                mock_emitter.method_calls[16].args[0].entityUrn
                == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableProduced,PROD)"
            )
            assert (
                mock_emitter.method_calls[17].args[0].aspectName
                == "dataProcessInstanceRunEvent"
            )
            assert (
                mock_emitter.method_calls[17].args[0].entityUrn
                == "urn:li:dataProcessInstance:5e274228107f44cc2dd7c9782168cc29"
            )
            assert (
                mock_emitter.method_calls[18].args[0].aspectName
                == "dataProcessInstanceRunEvent"
            )
            assert (
                mock_emitter.method_calls[18].args[0].entityUrn
                == "urn:li:dataProcessInstance:5e274228107f44cc2dd7c9782168cc29"
            )
