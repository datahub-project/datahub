"""Unit tests for :mod:`datahub.pgqueue.mcl_producer` (no database, no Schema Registry)."""

from __future__ import annotations

from typing import Optional
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka.serialization import MessageField

from datahub.metadata.schema_classes import MetadataChangeLogClass
from datahub.pgqueue.compression import PgQueuePayloadCompression, encode_inner
from datahub.pgqueue.config import PgQueueConnectionConfig, PgQueueMclProducerConfig

_DEFAULT_URN = "urn:li:dataset:(urn:li:dataPlatform:postgres,db.tbl,PROD)"


def _make_config(
    *,
    payload_compression: str = "SNAPPY",
) -> PgQueueMclProducerConfig:
    return PgQueueMclProducerConfig(
        queue=PgQueueConnectionConfig(
            host_port="localhost:5432",
            database="datahub",
            username="pguser",
            password="secret",
        ),
        schema_registry_url="http://localhost:8081",
        payload_compression=payload_compression,  # type: ignore[arg-type]
    )


def _make_mcl(*, entity_urn: Optional[str] = _DEFAULT_URN) -> MagicMock:
    mcl = MagicMock(spec=MetadataChangeLogClass)
    mcl.entityUrn = entity_urn
    mcl.to_obj = MagicMock(return_value={})
    return mcl


@pytest.fixture()
def producer_config() -> PgQueueMclProducerConfig:
    return _make_config()


class TestProduce:
    @patch("datahub.pgqueue.mcl_producer.create_pgqueue_connection")
    @patch("datahub.pgqueue.mcl_producer.AvroSerializer")
    @patch("datahub.pgqueue.mcl_producer.SchemaRegistryClient")
    def test_uses_caller_topic_for_serialization_and_enqueue(
        self,
        _sr_cls: MagicMock,
        avro_ser_cls: MagicMock,
        _conn_fn: MagicMock,
        producer_config: PgQueueMclProducerConfig,
    ) -> None:
        fake_serializer = MagicMock(return_value=b"\x00payload")
        avro_ser_cls.return_value = fake_serializer

        from datahub.pgqueue.mcl_producer import DatahubPgQueueMclProducer

        producer = DatahubPgQueueMclProducer(producer_config)
        mcl = _make_mcl()
        topic = "WorkEvent_v1"

        with patch.object(producer, "_repo") as mock_repo:
            callback = MagicMock()
            producer.produce(topic, mcl, callback=callback)

            fake_serializer.assert_called_once()
            event_arg, ctx = fake_serializer.call_args[0]
            assert event_arg is mcl
            assert ctx.topic == topic
            assert ctx.field == MessageField.VALUE

            mock_repo.enqueue.assert_called_once()
            assert mock_repo.enqueue.call_args.kwargs["topic_name"] == topic
            assert mock_repo.enqueue.call_args.kwargs["routing_key"] == mcl.entityUrn
            callback.assert_called_once()
            assert callback.call_args[0][0] is None

    @patch("datahub.pgqueue.mcl_producer.create_pgqueue_connection")
    @patch("datahub.pgqueue.mcl_producer.AvroSerializer")
    @patch("datahub.pgqueue.mcl_producer.SchemaRegistryClient")
    def test_routing_key_override(
        self,
        _sr_cls: MagicMock,
        avro_ser_cls: MagicMock,
        _conn_fn: MagicMock,
        producer_config: PgQueueMclProducerConfig,
    ) -> None:
        avro_ser_cls.return_value = MagicMock(return_value=b"\x00avro")

        from datahub.pgqueue.mcl_producer import DatahubPgQueueMclProducer

        producer = DatahubPgQueueMclProducer(producer_config)
        mcl = _make_mcl()

        with patch.object(producer, "_repo") as mock_repo:
            producer.produce(
                "MetadataChangeLog_Versioned_v1",
                mcl,
                routing_key="custom-key",
                callback=MagicMock(),
            )
            assert mock_repo.enqueue.call_args.kwargs["routing_key"] == "custom-key"

    @patch("datahub.pgqueue.mcl_producer.create_pgqueue_connection")
    @patch("datahub.pgqueue.mcl_producer.AvroSerializer")
    @patch("datahub.pgqueue.mcl_producer.SchemaRegistryClient")
    def test_missing_urn_reports_error_and_skips_enqueue(
        self,
        _sr_cls: MagicMock,
        avro_ser_cls: MagicMock,
        _conn_fn: MagicMock,
        producer_config: PgQueueMclProducerConfig,
    ) -> None:
        avro_ser_cls.return_value = MagicMock(return_value=b"\x00avro")

        from datahub.pgqueue.mcl_producer import DatahubPgQueueMclProducer

        producer = DatahubPgQueueMclProducer(producer_config)
        mcl = _make_mcl(entity_urn=None)

        with patch.object(producer, "_repo") as mock_repo:
            callback = MagicMock()
            producer.produce("WorkEvent_v1", mcl, callback=callback)

            mock_repo.enqueue.assert_not_called()
            err = callback.call_args[0][0]
            assert isinstance(err, ValueError)
            assert "entityUrn" in str(err)

    @patch("datahub.pgqueue.mcl_producer.create_pgqueue_connection")
    @patch("datahub.pgqueue.mcl_producer.AvroSerializer")
    @patch("datahub.pgqueue.mcl_producer.SchemaRegistryClient")
    def test_enqueue_failure_invokes_callback_with_error(
        self,
        _sr_cls: MagicMock,
        avro_ser_cls: MagicMock,
        _conn_fn: MagicMock,
        producer_config: PgQueueMclProducerConfig,
    ) -> None:
        avro_ser_cls.return_value = MagicMock(return_value=b"\x00avro")

        from datahub.pgqueue.mcl_producer import DatahubPgQueueMclProducer

        producer = DatahubPgQueueMclProducer(producer_config)
        mcl = _make_mcl()

        with patch.object(producer, "_repo") as mock_repo:
            mock_repo.enqueue.side_effect = RuntimeError("db down")
            callback = MagicMock()
            producer.produce("WorkEvent_v1", mcl, callback=callback)

            err = callback.call_args[0][0]
            assert isinstance(err, RuntimeError)
            assert "db down" in str(err)

    @patch("datahub.pgqueue.mcl_producer.encode_inner", wraps=encode_inner)
    @patch("datahub.pgqueue.mcl_producer.create_pgqueue_connection")
    @patch("datahub.pgqueue.mcl_producer.AvroSerializer")
    @patch("datahub.pgqueue.mcl_producer.SchemaRegistryClient")
    def test_compression_mode_passed_through(
        self,
        _sr_cls: MagicMock,
        avro_ser_cls: MagicMock,
        _conn_fn: MagicMock,
        encode_inner_mock: MagicMock,
    ) -> None:
        avro_ser_cls.return_value = MagicMock(return_value=b"\x00avro")
        config = _make_config(payload_compression="NONE")

        from datahub.pgqueue.mcl_producer import DatahubPgQueueMclProducer

        producer = DatahubPgQueueMclProducer(config)
        mcl = _make_mcl()

        with patch.object(producer, "_repo") as mock_repo:
            producer.produce("WorkEvent_v1", mcl, callback=MagicMock())

            encode_inner_mock.assert_called_once_with(
                b"\x00avro", PgQueuePayloadCompression.NONE
            )
            assert mock_repo.enqueue.call_args.kwargs["payload_compression"] == int(
                PgQueuePayloadCompression.NONE
            )


class TestFlushClose:
    @patch("datahub.pgqueue.mcl_producer.flush_pg_connection")
    @patch("datahub.pgqueue.mcl_producer.create_pgqueue_connection")
    @patch("datahub.pgqueue.mcl_producer.AvroSerializer")
    @patch("datahub.pgqueue.mcl_producer.SchemaRegistryClient")
    def test_flush_and_close(
        self,
        _sr_cls: MagicMock,
        avro_ser_cls: MagicMock,
        conn_fn: MagicMock,
        flush_fn: MagicMock,
        producer_config: PgQueueMclProducerConfig,
    ) -> None:
        avro_ser_cls.return_value = MagicMock(return_value=b"x")
        mock_conn = MagicMock()
        conn_fn.return_value = mock_conn

        from datahub.pgqueue.mcl_producer import DatahubPgQueueMclProducer

        producer = DatahubPgQueueMclProducer(producer_config)
        producer.flush()
        flush_fn.assert_called_once_with(mock_conn)

        producer.close()
        assert flush_fn.call_count == 2
        mock_conn.close.assert_called_once()
