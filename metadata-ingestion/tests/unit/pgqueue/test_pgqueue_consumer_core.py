"""Unit tests for :mod:`datahub.pgqueue.consumer` core behaviour (no database)."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.kafka_emitter import MCE_KEY, MCP_KEY
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass as MetadataChangeEvent,
    MetadataChangeLogClass,
    MetadataChangeProposalClass as MetadataChangeProposal,
)
from datahub.pgqueue.config import PgQueueConnectionConfig, PgQueueConsumerConfig
from datahub.pgqueue.repository import PgQueueMessageHandle


def _make_consumer_config(
    visibility_override: int | None = None,
) -> PgQueueConsumerConfig:
    return PgQueueConsumerConfig(
        queue=PgQueueConnectionConfig(
            host_port="localhost:5432",
            database="datahub",
            username="pguser",
            password="secret",
        ),
        schema_registry_url="http://localhost:8081",
        consumer_group="test-group",
        visibility_timeout_seconds=visibility_override,
    )


# ---------------------------------------------------------------------------
# visibility_timedelta
# ---------------------------------------------------------------------------


class TestVisibilityTimedelta:
    @patch("datahub.pgqueue.consumer.create_pgqueue_connection")
    @patch("datahub.pgqueue.consumer.SchemaRegistryClient")
    @patch("datahub.pgqueue.consumer.AvroDeserializer")
    def test_uses_config_override_when_set(
        self,
        _deser_cls: MagicMock,
        _sr_cls: MagicMock,
        _conn_fn: MagicMock,
    ) -> None:
        from datahub.pgqueue.consumer import DatahubPgQueueConsumer

        cfg = _make_consumer_config(visibility_override=120)
        consumer = DatahubPgQueueConsumer(cfg)
        assert consumer.visibility_timedelta() == timedelta(seconds=120)

    @patch("datahub.pgqueue.consumer.create_pgqueue_connection")
    @patch("datahub.pgqueue.consumer.SchemaRegistryClient")
    @patch("datahub.pgqueue.consumer.AvroDeserializer")
    def test_falls_back_to_topic_defaults(
        self,
        _deser_cls: MagicMock,
        _sr_cls: MagicMock,
        _conn_fn: MagicMock,
    ) -> None:
        from datahub.pgqueue.consumer import DatahubPgQueueConsumer

        cfg = _make_consumer_config(visibility_override=None)
        expected = cfg.queue.topic_defaults.visibility_timeout_seconds
        consumer = DatahubPgQueueConsumer(cfg)
        assert consumer.visibility_timedelta() == timedelta(seconds=expected)


# ---------------------------------------------------------------------------
# _deserialize
# ---------------------------------------------------------------------------


class TestDeserialize:
    """Patch the AvroDeserializer and verify each payload-kind branch."""

    @patch("datahub.pgqueue.consumer.create_pgqueue_connection")
    @patch("datahub.pgqueue.consumer.SchemaRegistryClient")
    @patch("datahub.pgqueue.consumer.AvroDeserializer")
    def _build_consumer(
        self,
        _deser_cls: MagicMock,
        _sr_cls: MagicMock,
        _conn_fn: MagicMock,
        payload_kinds: dict | None = None,
    ) -> tuple:
        from datahub.pgqueue.consumer import DatahubPgQueueConsumer

        cfg = _make_consumer_config()
        if payload_kinds:
            cfg = cfg.model_copy(update={"payload_kind_by_route_key": payload_kinds})
        consumer = DatahubPgQueueConsumer(cfg)
        return consumer, consumer._avro_deserializer

    def test_mcp_branch(self) -> None:
        consumer, mock_deser = self._build_consumer()
        mock_deser.return_value = {
            "entityUrn": "urn:li:dataset:foo",
            "entityType": "dataset",
            "changeType": "UPSERT",
        }

        result = consumer._deserialize(MCP_KEY, "MetadataChangeProposal_v1", b"\x00")
        assert isinstance(result, MetadataChangeProposal)

    def test_mce_branch(self) -> None:
        consumer, mock_deser = self._build_consumer()
        mock_deser.return_value = {
            "proposedSnapshot": (
                "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot",
                {
                    "urn": "urn:li:dataset:(urn:li:dataPlatform:mysql,db.tbl,PROD)",
                    "aspects": [],
                },
            )
        }

        result = consumer._deserialize(MCE_KEY, "MetadataChangeEvent_v4", b"\x00")
        assert isinstance(result, MetadataChangeEvent)

    def test_mcl_branch(self) -> None:
        consumer, mock_deser = self._build_consumer(
            payload_kinds={MCP_KEY: "mcp", MCE_KEY: "mce", "mcl_route": "mcl"},
        )
        mock_deser.return_value = {
            "entityUrn": "urn:li:dataset:foo",
            "entityType": "dataset",
            "changeType": "UPSERT",
            "aspectName": "ownership",
        }

        result = consumer._deserialize("mcl_route", "MetadataChangeLog_v1", b"\x00")
        assert isinstance(result, MetadataChangeLogClass)

    def test_pe_branch(self) -> None:
        consumer, mock_deser = self._build_consumer(
            payload_kinds={MCP_KEY: "mcp", "pe_route": "pe"},
        )
        raw_dict = {"entityUrn": "urn:li:dataset:bar", "eventType": "usage"}
        mock_deser.return_value = raw_dict

        result = consumer._deserialize("pe_route", "PlatformEvent_v1", b"\x00")
        assert isinstance(result, dict)
        assert result == raw_dict

    def test_unsupported_kind_raises(self) -> None:
        consumer, mock_deser = self._build_consumer(
            payload_kinds={MCP_KEY: "mcp", "bad_route": "unknown"},  # type: ignore[dict-item]
        )
        mock_deser.return_value = {}

        with pytest.raises(ValueError, match="Unsupported payload kind"):
            consumer._deserialize("bad_route", "SomeTopic_v1", b"\x00")


# ---------------------------------------------------------------------------
# ack
# ---------------------------------------------------------------------------


class TestAck:
    @patch("datahub.pgqueue.consumer.create_pgqueue_connection")
    @patch("datahub.pgqueue.consumer.SchemaRegistryClient")
    @patch("datahub.pgqueue.consumer.AvroDeserializer")
    def test_delegates_to_repo(
        self,
        _deser_cls: MagicMock,
        _sr_cls: MagicMock,
        _conn_fn: MagicMock,
    ) -> None:
        from datetime import datetime, timezone

        from datahub.pgqueue.consumer import DatahubPgQueueConsumer

        cfg = _make_consumer_config()
        consumer = DatahubPgQueueConsumer(cfg)

        handles = [
            PgQueueMessageHandle(
                id=1,
                enqueued_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
                topic_id=10,
                partition_id=0,
                enqueue_seq=42,
            ),
        ]

        with patch.object(consumer, "_repo") as mock_repo:
            mock_repo.commit_for_group.return_value = 1
            result = consumer.ack(handles)

        mock_repo.commit_for_group.assert_called_once_with(
            consumer._conn,
            cfg.consumer_group,
            handles,
        )
        assert result == 1


class TestExtendVisibility:
    @patch("datahub.pgqueue.consumer.create_pgqueue_connection")
    @patch("datahub.pgqueue.consumer.SchemaRegistryClient")
    @patch("datahub.pgqueue.consumer.AvroDeserializer")
    def test_delegates_to_repo(
        self,
        _deser_cls: MagicMock,
        _sr_cls: MagicMock,
        _conn_fn: MagicMock,
    ) -> None:
        from datetime import datetime, timedelta, timezone

        from datahub.pgqueue.consumer import DatahubPgQueueConsumer

        cfg = _make_consumer_config()
        consumer = DatahubPgQueueConsumer(cfg)
        handles = [
            PgQueueMessageHandle(
                id=3,
                enqueued_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
                topic_id=10,
                partition_id=1,
                enqueue_seq=9,
            ),
        ]

        with patch.object(consumer, "_repo") as mock_repo:
            mock_repo.extend_visibility_for_group.return_value = 1
            result = consumer.extend_visibility(
                handles, extend_by=timedelta(seconds=30)
            )

        mock_repo.extend_visibility_for_group.assert_called_once_with(
            consumer._conn,
            cfg.consumer_group,
            handles,
            lock_owner=consumer._lock_owner,
            extend_by=timedelta(seconds=30),
        )
        assert result == 1


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------


class TestClose:
    @patch("datahub.pgqueue.consumer.create_pgqueue_connection")
    @patch("datahub.pgqueue.consumer.SchemaRegistryClient")
    @patch("datahub.pgqueue.consumer.AvroDeserializer")
    def test_closes_connection(
        self,
        _deser_cls: MagicMock,
        _sr_cls: MagicMock,
        conn_fn: MagicMock,
    ) -> None:
        from datahub.pgqueue.consumer import DatahubPgQueueConsumer

        cfg = _make_consumer_config()
        consumer = DatahubPgQueueConsumer(cfg)

        consumer.close()
        consumer._conn.close.assert_called_once()
