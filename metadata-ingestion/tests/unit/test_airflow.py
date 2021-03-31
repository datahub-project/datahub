from unittest import mock

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


@mock.patch("datahub.integrations.airflow.hooks.DatahubRestEmitter")
def test_datahub_rest_emit(mock_emitter):
    with mock.patch(
        "airflow.hooks.base.BaseHook.get_connection",
        return_value=datahub_rest_connection_config,
    ):
        hook = DatahubRestHook(datahub_rest_connection_config.conn_id)
        hook.emit_mces([person])
        assert mock_emitter.called_with(datahub_rest_connection_config.host)
        assert mock_emitter.emit_mce.called_with(person)
