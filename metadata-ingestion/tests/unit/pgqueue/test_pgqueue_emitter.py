"""Unit tests for :mod:`datahub.pgqueue.emitter` (no database, no Schema Registry)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.kafka_emitter import MCP_KEY
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass as MetadataChangeEvent,
    MetadataChangeProposalClass as MetadataChangeProposal,
)
from datahub.pgqueue.config import PgQueueConnectionConfig, PgQueueEmitterConfig


def _make_config() -> PgQueueEmitterConfig:
    return PgQueueEmitterConfig(
        queue=PgQueueConnectionConfig(
            host_port="localhost:5432",
            database="datahub",
            username="pguser",
            password="secret",
        ),
        schema_registry_url="http://localhost:8081",
    )


@pytest.fixture()
def emitter_config() -> PgQueueEmitterConfig:
    return _make_config()


# ---------------------------------------------------------------------------
# _serialize_for_enqueue
# ---------------------------------------------------------------------------


class TestSerializeForEnqueue:
    """Bypass __init__ side-effects (SR client, DB conn) by patching them out."""

    @patch("datahub.pgqueue.emitter.create_pgqueue_connection")
    @patch("datahub.pgqueue.emitter.AvroSerializer")
    @patch("datahub.pgqueue.emitter.SchemaRegistryClient")
    def test_mcp_with_urn(
        self,
        _sr_cls: MagicMock,
        avro_ser_cls: MagicMock,
        _conn_fn: MagicMock,
        emitter_config: PgQueueEmitterConfig,
    ) -> None:
        fake_serializer = MagicMock(return_value=b"\x00payload")
        avro_ser_cls.return_value = fake_serializer

        from datahub.pgqueue.emitter import DatahubPgQueueEmitter

        emitter = DatahubPgQueueEmitter(emitter_config)

        mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:postgres,db.tbl,PROD)",
            aspect=None,
        )
        topic, key, payload = emitter._serialize_for_enqueue(mcp)

        assert topic == emitter_config.topic_routes[MCP_KEY]
        assert key == mcp.entityUrn
        assert payload == b"\x00payload"

    @patch("datahub.pgqueue.emitter.create_pgqueue_connection")
    @patch("datahub.pgqueue.emitter.AvroSerializer")
    @patch("datahub.pgqueue.emitter.SchemaRegistryClient")
    def test_mce_rejected(
        self,
        _sr_cls: MagicMock,
        avro_ser_cls: MagicMock,
        _conn_fn: MagicMock,
        emitter_config: PgQueueEmitterConfig,
    ) -> None:
        avro_ser_cls.return_value = MagicMock(return_value=b"\x00mce")

        from datahub.pgqueue.emitter import DatahubPgQueueEmitter

        emitter = DatahubPgQueueEmitter(emitter_config)

        mce = MagicMock(spec=MetadataChangeEvent)
        mce.proposedSnapshot = MagicMock()
        mce.proposedSnapshot.urn = (
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.tbl,PROD)"
        )

        with pytest.raises(ValueError, match="legacy snapshot MCE"):
            emitter._serialize_for_enqueue(mce)

    @patch("datahub.pgqueue.emitter.create_pgqueue_connection")
    @patch("datahub.pgqueue.emitter.AvroSerializer")
    @patch("datahub.pgqueue.emitter.SchemaRegistryClient")
    def test_mcp_missing_urn_raises(
        self,
        _sr_cls: MagicMock,
        avro_ser_cls: MagicMock,
        _conn_fn: MagicMock,
        emitter_config: PgQueueEmitterConfig,
    ) -> None:
        avro_ser_cls.return_value = MagicMock(return_value=b"x")

        from datahub.pgqueue.emitter import DatahubPgQueueEmitter

        emitter = DatahubPgQueueEmitter(emitter_config)

        mcp = MetadataChangeProposal(entityType="dataset", changeType="UPSERT")
        assert mcp.entityUrn is None
        with pytest.raises(ValueError, match="entityUrn"):
            emitter._serialize_for_enqueue(mcp)

    @patch("datahub.pgqueue.emitter.create_pgqueue_connection")
    @patch("datahub.pgqueue.emitter.AvroSerializer")
    @patch("datahub.pgqueue.emitter.SchemaRegistryClient")
    def test_mce_missing_snapshot_still_rejected(
        self,
        _sr_cls: MagicMock,
        avro_ser_cls: MagicMock,
        _conn_fn: MagicMock,
        emitter_config: PgQueueEmitterConfig,
    ) -> None:
        avro_ser_cls.return_value = MagicMock(return_value=b"x")

        from datahub.pgqueue.emitter import DatahubPgQueueEmitter

        emitter = DatahubPgQueueEmitter(emitter_config)

        mce = MagicMock(spec=MetadataChangeEvent)
        mce.proposedSnapshot = None

        with pytest.raises(ValueError, match="legacy snapshot MCE"):
            emitter._serialize_for_enqueue(mce)


# ---------------------------------------------------------------------------
# emit (single)
# ---------------------------------------------------------------------------


class TestEmit:
    @patch("datahub.pgqueue.emitter.create_pgqueue_connection")
    @patch("datahub.pgqueue.emitter.AvroSerializer")
    @patch("datahub.pgqueue.emitter.SchemaRegistryClient")
    def test_emit_happy_path(
        self,
        _sr_cls: MagicMock,
        avro_ser_cls: MagicMock,
        conn_fn: MagicMock,
        emitter_config: PgQueueEmitterConfig,
    ) -> None:
        avro_ser_cls.return_value = MagicMock(return_value=b"\x00avro")

        from datahub.pgqueue.emitter import DatahubPgQueueEmitter

        emitter = DatahubPgQueueEmitter(emitter_config)

        mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:postgres,db.tbl,PROD)",
            aspect=None,
        )

        with patch.object(emitter, "_repo") as mock_repo:
            callback = MagicMock()
            emitter.emit(mcp, callback=callback)

            mock_repo.enqueue.assert_called_once()
            callback.assert_called_once()
            assert callback.call_args[0][0] is None  # no error


# ---------------------------------------------------------------------------
# emit_batch
# ---------------------------------------------------------------------------


class TestEmitBatch:
    @patch("datahub.pgqueue.emitter.create_pgqueue_connection")
    @patch("datahub.pgqueue.emitter.AvroSerializer")
    @patch("datahub.pgqueue.emitter.SchemaRegistryClient")
    def test_emit_batch_happy_path(
        self,
        _sr_cls: MagicMock,
        avro_ser_cls: MagicMock,
        _conn_fn: MagicMock,
        emitter_config: PgQueueEmitterConfig,
    ) -> None:
        avro_ser_cls.return_value = MagicMock(return_value=b"\x00avro")

        from datahub.pgqueue.emitter import DatahubPgQueueEmitter

        emitter = DatahubPgQueueEmitter(emitter_config)

        items = [
            MetadataChangeProposalWrapper(
                entityUrn=f"urn:li:dataset:(urn:li:dataPlatform:postgres,db.tbl{i},PROD)",
                aspect=None,
            )
            for i in range(3)
        ]

        with patch.object(emitter, "_repo") as mock_repo:
            callback = MagicMock()
            emitter.emit_batch(items, callback=callback)

            mock_repo.enqueue_batch.assert_called_once()
            assert callback.call_count == 3

    @patch("datahub.pgqueue.emitter.create_pgqueue_connection")
    @patch("datahub.pgqueue.emitter.AvroSerializer")
    @patch("datahub.pgqueue.emitter.SchemaRegistryClient")
    def test_emit_batch_mce_rejected_reports_error(
        self,
        _sr_cls: MagicMock,
        avro_ser_cls: MagicMock,
        _conn_fn: MagicMock,
    ) -> None:
        """Legacy MCE in a batch is rejected and reported via callback."""
        avro_ser_cls.return_value = MagicMock(return_value=b"\x00avro")

        config = _make_config()
        from datahub.pgqueue.emitter import DatahubPgQueueEmitter

        emitter = DatahubPgQueueEmitter(config)

        mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:postgres,db.tbl,PROD)",
            aspect=None,
        )
        mce = MagicMock(spec=MetadataChangeEvent)
        mce.proposedSnapshot = MagicMock()
        mce.proposedSnapshot.urn = (
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.tbl,PROD)"
        )

        callback = MagicMock()
        emitter.emit_batch([mcp, mce], callback=callback)

        assert callback.call_count == 1
        err = callback.call_args[0][0]
        assert isinstance(err, ValueError)
        assert "legacy snapshot MCE" in str(err)

    @patch("datahub.pgqueue.emitter.create_pgqueue_connection")
    @patch("datahub.pgqueue.emitter.AvroSerializer")
    @patch("datahub.pgqueue.emitter.SchemaRegistryClient")
    def test_emit_batch_empty_is_noop(
        self,
        _sr_cls: MagicMock,
        avro_ser_cls: MagicMock,
        _conn_fn: MagicMock,
        emitter_config: PgQueueEmitterConfig,
    ) -> None:
        avro_ser_cls.return_value = MagicMock(return_value=b"\x00avro")

        from datahub.pgqueue.emitter import DatahubPgQueueEmitter

        emitter = DatahubPgQueueEmitter(emitter_config)

        with patch.object(emitter, "_repo") as mock_repo:
            emitter.emit_batch([])
            mock_repo.enqueue_batch.assert_not_called()
