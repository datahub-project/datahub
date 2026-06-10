"""Tests for the pgQueue sink (DatahubPgQueueSink)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import RecordEnvelope
from datahub.ingestion.sink.datahub_pg_queue import DatahubPgQueueSink
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetSnapshotClass,
    MetadataChangeProposalClass,
    StatusClass,
)


@pytest.fixture()
def sink() -> DatahubPgQueueSink:
    with patch(
        "datahub.ingestion.sink.datahub_pg_queue.DatahubPgQueueEmitter"
    ) as mock_cls:
        mock_emitter = MagicMock()
        mock_cls.return_value = mock_emitter
        config = {
            "queue": {
                "host_port": "localhost:5432",
                "database": "datahub",
                "username": "datahub",
                "password": "datahub",
            }
        }
        s = DatahubPgQueueSink.create(config, MagicMock())
        assert s.emitter is mock_emitter
        return s


def test_write_record_async_mcp_wrapper(sink: DatahubPgQueueSink) -> None:
    mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:hive,mydb.table,PROD)",
        aspect=StatusClass(removed=False),
    )
    envelope: RecordEnvelope[Any] = RecordEnvelope(record=mcp, metadata={})
    callback = MagicMock()

    sink.write_record_async(envelope, callback)

    mock_emitter: Any = sink.emitter
    mock_emitter.emit.assert_called_once()
    assert mock_emitter.emit.call_args[0][0] is mcp


def test_write_record_async_mcp_class(sink: DatahubPgQueueSink) -> None:
    mcp = MetadataChangeProposalClass(
        entityType="dataset",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:hive,mydb.table,PROD)",
    )
    envelope: RecordEnvelope[Any] = RecordEnvelope(record=mcp, metadata={})
    callback = MagicMock()

    sink.write_record_async(envelope, callback)

    mock_emitter: Any = sink.emitter
    mock_emitter.emit.assert_called_once()
    assert mock_emitter.emit.call_args[0][0] is mcp


def test_write_record_async_mce_calls_emit_batch(sink: DatahubPgQueueSink) -> None:
    snapshot = DatasetSnapshotClass(
        urn="urn:li:dataset:(urn:li:dataPlatform:hive,mydb.table,PROD)",
        aspects=[StatusClass(removed=False)],
    )
    mce = MetadataChangeEvent(proposedSnapshot=snapshot)
    envelope: RecordEnvelope[Any] = RecordEnvelope(record=mce, metadata={})
    callback = MagicMock()

    sink.write_record_async(envelope, callback)

    mock_emitter: Any = sink.emitter
    mock_emitter.emit_batch.assert_called_once()
    mcps = mock_emitter.emit_batch.call_args[0][0]
    assert len(mcps) >= 1


def test_write_record_async_mce_zero_aspects(sink: DatahubPgQueueSink) -> None:
    snapshot = DatasetSnapshotClass(
        urn="urn:li:dataset:(urn:li:dataPlatform:hive,mydb.table,PROD)",
        aspects=[],
    )
    mce = MetadataChangeEvent(proposedSnapshot=snapshot)
    envelope: RecordEnvelope[Any] = RecordEnvelope(record=mce, metadata={})
    callback = MagicMock()

    sink.write_record_async(envelope, callback)

    mock_emitter: Any = sink.emitter
    mock_emitter.emit_batch.assert_not_called()
    callback.on_success.assert_called_once()


def test_close_calls_emitter_close(sink: DatahubPgQueueSink) -> None:
    sink.close()

    mock_emitter: Any = sink.emitter
    mock_emitter.close.assert_called_once()


def test_handle_work_unit_end_flushes(sink: DatahubPgQueueSink) -> None:
    sink.handle_work_unit_end(MagicMock())

    mock_emitter: Any = sink.emitter
    mock_emitter.flush.assert_called_once()
