from typing import ContextManager
from unittest import mock
from contextlib import contextmanager

from airflow.models import DagBag

from airflow.models import Connection

from datahub.integrations.airflow.hooks import DatahubRestHook

from datahub.metadata.schema_classes import (
    MetadataChangeEventClass,
    CorpUserSnapshotClass,
    CorpUserInfoClass,
)

# TODO test using DagBag
person = MetadataChangeEventClass(
    proposedSnapshot=CorpUserSnapshotClass(
        urn="urn:li:corpuser:jane_ds",
        aspects=[
            CorpUserInfoClass(
                active=True,
                displayName="Jane the Data Scientist",
                email="jane@example.com",
                title="Data Scientist",
                fullName="Jane Doe",
            )
        ],
    )
)

datahub_rest_connection_config = Connection(
    conn_id="datahub_rest_test",
    conn_type="datahub_rest",
    host="http://test_host:8080/",
    extra=None,
)


def test_dags_load_with_no_errors(pytestconfig):
    airflow_examples_folder = pytestconfig.rootpath / "examples/airflow"

    dag_bag = DagBag(dag_folder=str(airflow_examples_folder), include_examples=False)
    assert len(dag_bag.import_errors) == 0
    assert len(dag_bag.dag_ids) > 0


@contextmanager
def patch_airflow_connection(conn: Connection) -> ContextManager[Connection]:
    with mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=conn,
    ):
        yield conn


@mock.patch("datahub.integrations.airflow.hooks.DatahubRestEmitter")
def test_datahub_rest_emit(mock_emitter):
    with patch_airflow_connection(datahub_rest_connection_config) as config:
        hook = DatahubRestHook(config.conn_id)
        hook.emit_mces([person])
        assert mock_emitter.called_with(config.host)
        assert mock_emitter.emit_mce.called_with(person)
