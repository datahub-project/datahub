import threading
import unittest
from typing import Union
from unittest.mock import MagicMock, call, patch

import pydantic
import pytest
from confluent_kafka import KafkaError, KafkaException

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.configuration.common import ConfigurationError
from datahub.emitter.kafka_emitter import MCP_KEY, MessageTooLargeError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DataHubRestEmitter, EmitMode
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import SinkReport, WriteCallback
from datahub.ingestion.sink.datahub_kafka import (
    DatahubKafkaSink,
    KafkaSinkConfig,
    _AggregatingKafkaCallback,
    _enhance_schema_registry_error,
    _KafkaCallback,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)


class KafkaSinkTest(unittest.TestCase):
    @patch("datahub.ingestion.api.sink.PipelineContext", autospec=True)
    @patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
    def test_kafka_sink_config(self, mock_producer, mock_context):
        kafka_sink = DatahubKafkaSink.create(
            {"connection": {"bootstrap": "foobar:9092"}}, mock_context
        )
        kafka_sink.close()
        assert (
            mock_producer.call_count == 2
        )  # constructor should be called twice (once each for mce,mcp)

    @patch("datahub.ingestion.sink.datahub_kafka._KafkaCallback", autospec=True)
    @patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
    def test_kafka_sink_mcp(self, mock_producer, mock_callback):
        from datahub.emitter.mcp import MetadataChangeProposalWrapper

        mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount,PROD)",
            aspect=models.DatasetProfileClass(
                rowCount=2000,
                columnCount=15,
                timestampMillis=1626995099686,
            ),
        )
        kafka_sink = DatahubKafkaSink.create(
            {"connection": {"bootstrap": "localhost:9092"}},
            PipelineContext(run_id="test"),
        )
        kafka_sink.write_record_async(
            RecordEnvelope(record=mcp, metadata={}), mock_callback
        )
        kafka_sink.close()
        assert mock_producer.call_count == 2  # constructor should be called

    @patch("datahub.ingestion.api.sink.PipelineContext", autospec=True)
    @patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
    def test_kafka_sink_write(self, mock_producer, mock_context):
        callback = MagicMock(spec=WriteCallback)
        kafka_sink = DatahubKafkaSink.create(
            {"connection": {"bootstrap": "foobar:9092"}}, mock_context
        )
        mock_mcp_producer = kafka_sink.emitter.producers[MCP_KEY]

        mce = builder.make_lineage_mce(
            [
                builder.make_dataset_urn("bigquery", "upstream1"),
                builder.make_dataset_urn("bigquery", "upstream2"),
            ],
            builder.make_dataset_urn("bigquery", "downstream1"),
        )

        re: RecordEnvelope[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
            ]
        ] = RecordEnvelope(record=mce, metadata={})
        kafka_sink.write_record_async(re, callback)

        # The lineage MCE has 1 aspect (UpstreamLineage), so expect 1 produce call
        assert mock_mcp_producer.produce.call_count == 1

        # The produced value should be an MCP (not the original MCE)
        args, kwargs = mock_mcp_producer.produce.call_args
        produced_value = kwargs["value"]
        assert isinstance(produced_value, MetadataChangeProposalWrapper)
        assert kwargs["key"]  # produce call should include a Kafka key

        # The on_delivery callback should be the aggregating callback, not
        # the inner _KafkaCallback directly.
        agg_callback = kwargs["on_delivery"]

        # Simulate successful Kafka delivery -- the aggregating callback should
        # forward to the write_callback exactly once.
        agg_callback(None, MagicMock())
        callback.on_success.assert_called_once()
        assert callback.on_success.call_args[0][0] == re

    @patch("datahub.ingestion.api.sink.PipelineContext", autospec=True)
    @patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
    def test_kafka_sink_mce_with_multiple_aspects(self, mock_producer, mock_context):
        """Test that MCEs with multiple aspects are unpacked into separate MCPs.

        Validates that the aggregating callback fires on_success exactly once
        after all MCP deliveries succeed.
        """
        callback = MagicMock(spec=WriteCallback)
        kafka_sink = DatahubKafkaSink.create(
            {"connection": {"bootstrap": "foobar:9092"}}, mock_context
        )
        mock_mcp_producer = kafka_sink.emitter.producers[MCP_KEY]

        user_urn = builder.make_user_urn("testuser")
        group_urns = [
            builder.make_group_urn("group1"),
            builder.make_group_urn("group2"),
        ]

        user_snapshot = models.CorpUserSnapshotClass(
            urn=user_urn,
            aspects=[
                models.CorpUserInfoClass(
                    active=True,
                    displayName="Test User",
                    email="test@example.com",
                ),
                models.GroupMembershipClass(groups=group_urns),
            ],
        )

        mce = MetadataChangeEvent(proposedSnapshot=user_snapshot)
        re: RecordEnvelope[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
            ]
        ] = RecordEnvelope(record=mce, metadata={})

        kafka_sink.write_record_async(re, callback)

        # MCE with 2 aspects should produce 2 MCP messages
        assert mock_mcp_producer.produce.call_count == 2

        # All produced values should be MCPs with the same entity URN
        agg_callbacks = []
        for call_args in mock_mcp_producer.produce.call_args_list:
            _, kwargs = call_args
            assert isinstance(kwargs["value"], MetadataChangeProposalWrapper)
            assert kwargs["key"] == user_urn
            agg_callbacks.append(kwargs["on_delivery"])

        # All produce calls should share the same aggregating callback instance
        assert agg_callbacks[0] == agg_callbacks[1]

        # Before any delivery, write_callback should not have been called
        callback.on_success.assert_not_called()
        callback.on_failure.assert_not_called()

        # Simulate first MCP delivery -- should not trigger the write_callback yet
        agg_callbacks[0](None, MagicMock())
        callback.on_success.assert_not_called()

        # Simulate second MCP delivery -- now the aggregating callback fires
        agg_callbacks[1](None, MagicMock())
        callback.on_success.assert_called_once()
        assert callback.on_success.call_args[0][0] == re

    @patch("datahub.ingestion.api.sink.PipelineContext", autospec=True)
    @patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
    def test_kafka_sink_mce_partial_failure(self, mock_producer, mock_context):
        """Test that partial delivery failure fires on_failure exactly once."""
        callback = MagicMock(spec=WriteCallback)
        kafka_sink = DatahubKafkaSink.create(
            {"connection": {"bootstrap": "foobar:9092"}}, mock_context
        )
        mock_mcp_producer = kafka_sink.emitter.producers[MCP_KEY]

        user_snapshot = models.CorpUserSnapshotClass(
            urn=builder.make_user_urn("testuser"),
            aspects=[
                models.CorpUserInfoClass(
                    active=True,
                    displayName="Test User",
                    email="test@example.com",
                ),
                models.GroupMembershipClass(groups=[builder.make_group_urn("group1")]),
            ],
        )

        mce = MetadataChangeEvent(proposedSnapshot=user_snapshot)
        re: RecordEnvelope[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
            ]
        ] = RecordEnvelope(record=mce, metadata={})
        kafka_sink.write_record_async(re, callback)

        assert mock_mcp_producer.produce.call_count == 2

        agg_callbacks = [
            c[1]["on_delivery"] for c in mock_mcp_producer.produce.call_args_list
        ]

        # First MCP succeeds
        agg_callbacks[0](None, MagicMock())
        callback.on_failure.assert_not_called()

        # Second MCP fails -- should trigger on_failure exactly once
        mock_error = MagicMock()
        agg_callbacks[1](mock_error, MagicMock())
        callback.on_failure.assert_called_once()
        callback.on_success.assert_not_called()

    @patch("datahub.ingestion.api.sink.PipelineContext", autospec=True)
    @patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
    def test_kafka_sink_mce_empty_aspects(self, mock_producer, mock_context):
        """Test that an MCE with zero aspects calls on_success immediately."""
        callback = MagicMock(spec=WriteCallback)
        kafka_sink = DatahubKafkaSink.create(
            {"connection": {"bootstrap": "foobar:9092"}}, mock_context
        )
        mock_mcp_producer = kafka_sink.emitter.producers[MCP_KEY]

        user_snapshot = models.CorpUserSnapshotClass(
            urn=builder.make_user_urn("testuser"),
            aspects=[],
        )

        mce = MetadataChangeEvent(proposedSnapshot=user_snapshot)
        re: RecordEnvelope[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
            ]
        ] = RecordEnvelope(record=mce, metadata={})
        kafka_sink.write_record_async(re, callback)

        # No MCPs should be produced
        mock_mcp_producer.produce.assert_not_called()

        # on_success should have been called immediately
        callback.on_success.assert_called_once()
        assert callback.on_success.call_args[0][0] == re

    # TODO: Test that kafka producer is configured correctly

    @patch("datahub.ingestion.api.sink.PipelineContext", autospec=True)
    @patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
    def test_kafka_sink_close(self, mock_producer, mock_context):
        mock_producer_instance = mock_producer.return_value
        mock_producer_instance.flush.return_value = 0
        kafka_sink = DatahubKafkaSink.create({}, mock_context)
        kafka_sink.close()
        # flush is now bounded by a timeout (max_queue_full_block_seconds).
        timeout = kafka_sink.config.max_queue_full_block_seconds
        mock_producer_instance.flush.assert_has_calls([call(timeout), call(timeout)])

    @patch("datahub.ingestion.sink.datahub_kafka.RecordEnvelope", autospec=True)
    @patch("datahub.ingestion.sink.datahub_kafka.WriteCallback", autospec=True)
    def test_kafka_callback_class(self, mock_w_callback, mock_re):
        callback = _KafkaCallback(
            SinkReport(), record_envelope=mock_re, write_callback=mock_w_callback
        )
        mock_error = MagicMock()
        mock_message = MagicMock()
        callback.kafka_callback(mock_error, mock_message)
        mock_w_callback.on_failure.assert_called_once()
        assert mock_w_callback.on_failure.call_args[0][0] == mock_re
        # Error is now wrapped in an Exception for consistent error handling
        assert isinstance(mock_w_callback.on_failure.call_args[0][1], Exception)
        assert str(mock_error) in str(mock_w_callback.on_failure.call_args[0][1])
        callback.kafka_callback(None, mock_message)
        mock_w_callback.on_success.assert_called_once()
        assert mock_w_callback.on_success.call_args[0][0] == mock_re


def test_aggregating_callback_all_success():
    """Cover _AggregatingKafkaCallback: all N deliveries succeed."""
    mock_inner = MagicMock(spec=_KafkaCallback)
    agg = _AggregatingKafkaCallback(total=3, inner=mock_inner)

    agg.kafka_callback(None, MagicMock())
    mock_inner.kafka_callback.assert_not_called()

    agg.kafka_callback(None, MagicMock())
    mock_inner.kafka_callback.assert_not_called()

    agg.kafka_callback(None, MagicMock())
    mock_inner.kafka_callback.assert_called_once()
    # The single call should be a success (err=None)
    assert mock_inner.kafka_callback.call_args[0][0] is None


def test_aggregating_callback_first_fails():
    """Cover _AggregatingKafkaCallback: first delivery fails, rest succeed."""
    mock_inner = MagicMock(spec=_KafkaCallback)
    agg = _AggregatingKafkaCallback(total=3, inner=mock_inner)

    mock_error = MagicMock()
    agg.kafka_callback(mock_error, MagicMock())
    # Failure fires immediately
    mock_inner.kafka_callback.assert_called_once()
    assert mock_inner.kafka_callback.call_args[0][0] is mock_error

    # Subsequent deliveries (success or failure) should not fire again
    agg.kafka_callback(None, MagicMock())
    agg.kafka_callback(None, MagicMock())
    assert mock_inner.kafka_callback.call_count == 1


def test_aggregating_callback_last_fails():
    """Cover _AggregatingKafkaCallback: last delivery fails after others succeed."""
    mock_inner = MagicMock(spec=_KafkaCallback)
    agg = _AggregatingKafkaCallback(total=2, inner=mock_inner)

    agg.kafka_callback(None, MagicMock())
    mock_inner.kafka_callback.assert_not_called()

    mock_error = MagicMock()
    agg.kafka_callback(mock_error, MagicMock())
    mock_inner.kafka_callback.assert_called_once()
    assert mock_inner.kafka_callback.call_args[0][0] is mock_error


def _run_concurrent_callbacks(
    agg: _AggregatingKafkaCallback,
    num_threads: int,
    error_thread_idx: int = -1,
) -> None:
    """Helper: fire num_threads concurrent callbacks through a barrier.

    Args:
        agg: The aggregating callback under test.
        num_threads: How many threads to launch.
        error_thread_idx: If >= 0, that thread delivers an error; others succeed.
    """
    barrier = threading.Barrier(num_threads)
    mock_error = MagicMock()

    def deliver(
        idx: int,
        _barrier: threading.Barrier = barrier,
        _agg: _AggregatingKafkaCallback = agg,
        _mock_error: MagicMock = mock_error,
    ) -> None:
        _barrier.wait()
        if idx == error_thread_idx:
            _agg.kafka_callback(_mock_error, MagicMock())
        else:
            _agg.kafka_callback(None, MagicMock())

    threads = [threading.Thread(target=deliver, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def test_aggregating_callback_thread_safety():
    """Verify that concurrent delivery callbacks fire the inner callback exactly once.

    Kafka delivery callbacks are invoked from librdkafka's background thread,
    so _AggregatingKafkaCallback must be thread-safe. This test uses a
    threading.Barrier to force all callbacks to execute simultaneously,
    maximizing the chance of hitting a race condition if the lock is missing.
    """
    num_threads = 20
    iterations = 200

    for _ in range(iterations):
        mock_inner = MagicMock(spec=_KafkaCallback)
        agg = _AggregatingKafkaCallback(total=num_threads, inner=mock_inner)
        _run_concurrent_callbacks(agg, num_threads)

        assert mock_inner.kafka_callback.call_count == 1, (
            f"Expected exactly 1 inner callback call, got {mock_inner.kafka_callback.call_count}"
        )


def test_aggregating_callback_thread_safety_with_failure():
    """Verify that concurrent callbacks with one failure fire on_failure exactly once."""
    num_threads = 20
    iterations = 200

    for _ in range(iterations):
        mock_inner = MagicMock(spec=_KafkaCallback)
        agg = _AggregatingKafkaCallback(total=num_threads, inner=mock_inner)
        _run_concurrent_callbacks(agg, num_threads, error_thread_idx=0)

        assert mock_inner.kafka_callback.call_count == 1, (
            f"Expected exactly 1 inner callback call, got {mock_inner.kafka_callback.call_count}"
        )
        # The single call must have been the failure
        err_arg = mock_inner.kafka_callback.call_args[0][0]
        assert err_arg is not None, "Expected on_failure (err != None), got on_success"


def test_kafka_sink_producer_config_without_oauth_cb():
    """Test that standard producer_config works without oauth_cb (no breaking change)"""
    # Verify backward compatibility: existing configs without oauth_cb continue to work
    config = KafkaSinkConfig.model_validate(
        {
            "connection": {
                "bootstrap": "foobar:9092",
                "producer_config": {
                    "security.protocol": "SASL_SSL",
                    "sasl.mechanism": "PLAIN",
                },
            }
        }
    )

    assert config.connection.producer_config["security.protocol"] == "SASL_SSL"
    assert config.connection.producer_config["sasl.mechanism"] == "PLAIN"
    assert "oauth_cb" not in config.connection.producer_config


def test_kafka_sink_producer_config_with_oauth_cb():
    """Test that producer_config properly resolves oauth_cb string to callable"""
    # This verifies the new oauth_cb functionality works for producers
    config = KafkaSinkConfig.model_validate(
        {
            "connection": {
                "bootstrap": "foobar:9092",
                "producer_config": {
                    "security.protocol": "SASL_SSL",
                    "sasl.mechanism": "OAUTHBEARER",
                    "oauth_cb": "tests.integration.kafka.oauth:create_token",
                },
            }
        }
    )

    # Verify oauth_cb was resolved from string to callable function
    assert callable(config.connection.producer_config["oauth_cb"])
    assert config.connection.producer_config["security.protocol"] == "SASL_SSL"


def test_kafka_sink_oauth_cb_rejects_callable():
    """Test that oauth_cb in producer_config must be a string, not a direct callable"""
    # Edge case: Passing a callable directly (instead of string path) should fail
    with pytest.raises(
        ConfigurationError,
        match=(
            "oauth_cb must be a string representing python function reference "
            "in the format <python-module>:<function-name>."
        ),
    ):
        KafkaSinkConfig.model_validate(
            {
                "connection": {
                    "bootstrap": "foobar:9092",
                    "producer_config": {
                        "oauth_cb": test_kafka_sink_producer_config_without_oauth_cb
                    },
                }
            }
        )


def test_enhance_schema_registry_error_with_404():
    """Test that schema registry 404 errors get enhanced with helpful guidance"""
    error_str = (
        "KafkaError{code=_VALUE_SERIALIZATION,val=-161,"
        "str=\"Unknown Schema Registry Error: b'' (HTTP status code 404, SR code -1)\"}"
    )

    enhanced = _enhance_schema_registry_error(error_str)

    # Should contain the original error
    assert error_str in enhanced
    # Should contain the hint
    assert "HINT:" in enhanced
    assert "topic_routes" in enhanced


def test_enhance_schema_registry_error_non_404():
    """Test that non-schema registry errors are returned unchanged"""
    error_str = "Some other Kafka error"

    enhanced = _enhance_schema_registry_error(error_str)

    # Should be unchanged
    assert enhanced == error_str
    assert "HINT:" not in enhanced


def _make_delete_mcp() -> MetadataChangeProposal:
    return MetadataChangeProposal(
        entityType="dataset",
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount,PROD)",
        changeType=models.ChangeTypeClass.DELETE,
        aspectName="datasetKey",
    )


@patch("datahub.ingestion.sink.datahub_rest.DatahubRestSink.make_emitter")
@patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
def test_kafka_sink_delete_routes_to_rest_fallback(mock_producer, mockmake_emitter):
    """DELETE MCPs must be routed to the REST fallback, not produced to Kafka.

    DELETE change types are not supported over async Kafka ingestion (GMS
    rejects them), so when rest_fallback is configured the sink emits them
    synchronously via REST.
    """
    mock_rest_emitter = MagicMock(spec=DataHubRestEmitter)
    mockmake_emitter.return_value = mock_rest_emitter

    callback = MagicMock(spec=WriteCallback)
    kafka_sink = DatahubKafkaSink.create(
        {
            "connection": {"bootstrap": "foobar:9092"},
            "rest_fallback": {"server": "http://fake-gms:8080"},
        },
        PipelineContext(run_id="test"),
    )
    mock_mcp_producer = kafka_sink.emitter.producers[MCP_KEY]
    mock_mcp_producer.flush.return_value = 0  # fully drained

    mcp = _make_delete_mcp()
    re: RecordEnvelope[
        Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ]
    ] = RecordEnvelope(record=mcp, metadata={})
    kafka_sink.write_record_async(re, callback)

    # DELETE went to the REST fallback, not the Kafka producer.
    mock_rest_emitter.emit_mcp.assert_called_once_with(
        mcp, emit_mode=EmitMode.SYNC_PRIMARY
    )
    mock_mcp_producer.produce.assert_not_called()
    callback.on_success.assert_called_once()
    assert callback.on_success.call_args[0][0] == re

    kafka_sink.close()


@patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
def test_kafka_sink_delete_without_fallback_fails_not_silently_produced(mock_producer):
    """Without a rest_fallback, a DELETE is NOT produced to Kafka (GMS would
    silently reject it) -- it is failed loudly instead."""
    callback = MagicMock(spec=WriteCallback)
    kafka_sink = DatahubKafkaSink.create(
        {"connection": {"bootstrap": "foobar:9092"}},
        PipelineContext(run_id="test"),
    )
    assert kafka_sink.config.rest_fallback is None
    mock_mcp_producer = kafka_sink.emitter.producers[MCP_KEY]

    mcp = _make_delete_mcp()
    re: RecordEnvelope[
        Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ]
    ] = RecordEnvelope(record=mcp, metadata={})
    kafka_sink.write_record_async(re, callback)

    mock_mcp_producer.produce.assert_not_called()
    callback.on_failure.assert_called_once()
    callback.on_success.assert_not_called()
    kafka_sink.close()


@patch("datahub.ingestion.sink.datahub_rest.DatahubRestSink.make_emitter")
@patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
def test_kafka_sink_delete_skipped_after_prior_delivery_failure(
    mock_producer, mockmake_emitter
):
    """A DELETE must NOT be applied via REST if an earlier async Kafka delivery
    failed in the same run (would delete an entity whose write never landed)."""
    mock_rest_emitter = MagicMock(spec=DataHubRestEmitter)
    mockmake_emitter.return_value = mock_rest_emitter

    callback = MagicMock(spec=WriteCallback)
    kafka_sink = DatahubKafkaSink.create(
        {
            "connection": {"bootstrap": "foobar:9092"},
            "rest_fallback": {"server": "http://fake-gms:8080"},
        },
        PipelineContext(run_id="test"),
    )
    kafka_sink.emitter.producers[MCP_KEY].flush.return_value = 0  # drained
    # Simulate a prior async delivery failure in this run.
    kafka_sink._delivery_failed.set()

    mcp = _make_delete_mcp()
    re: RecordEnvelope[
        Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ]
    ] = RecordEnvelope(record=mcp, metadata={})
    kafka_sink.write_record_async(re, callback)

    # DELETE was not emitted; the record was failed instead.
    mock_rest_emitter.emit_mcp.assert_not_called()
    callback.on_failure.assert_called_once()
    callback.on_success.assert_not_called()
    kafka_sink.close()


@patch("datahub.ingestion.sink.datahub_rest.DatahubRestSink.make_emitter")
@patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
def test_kafka_sink_delete_rest_failure_is_reported(mock_producer, mockmake_emitter):
    """If the REST fallback emit raises (e.g. GMS 500), the record is failed
    (not silently swallowed) and success is never signalled."""
    mock_rest_emitter = MagicMock(spec=DataHubRestEmitter)
    mock_rest_emitter.emit_mcp.side_effect = Exception("GMS 500")
    mockmake_emitter.return_value = mock_rest_emitter

    callback = MagicMock(spec=WriteCallback)
    kafka_sink = DatahubKafkaSink.create(
        {
            "connection": {"bootstrap": "foobar:9092"},
            "rest_fallback": {"server": "http://fake-gms:8080"},
        },
        PipelineContext(run_id="test"),
    )
    kafka_sink.emitter.producers[MCP_KEY].flush.return_value = 0  # drained

    mcp = _make_delete_mcp()
    re: RecordEnvelope[
        Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ]
    ] = RecordEnvelope(record=mcp, metadata={})
    kafka_sink.write_record_async(re, callback)

    mock_rest_emitter.emit_mcp.assert_called_once_with(
        mcp, emit_mode=EmitMode.SYNC_PRIMARY
    )
    callback.on_failure.assert_called_once()
    callback.on_success.assert_not_called()
    kafka_sink.close()


@patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
def test_kafka_sink_close_reports_undelivered_as_failure(mock_producer):
    """Messages still queued at close() are lost on exit; close() must surface
    them as a failure so the run does not report success while dropping data."""
    kafka_sink = DatahubKafkaSink.create(
        {"connection": {"bootstrap": "foobar:9092"}},
        PipelineContext(run_id="test"),
    )
    # Broker stalled: flush times out with messages still queued.
    kafka_sink.emitter.producers[MCP_KEY].flush.return_value = 3

    assert not kafka_sink.get_report().failures
    kafka_sink.close()
    assert kafka_sink.get_report().failures  # undelivered surfaced as a failure


@patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
def test_kafka_sink_flush_reports_undelivered_as_failure(mock_producer):
    """The pipeline calls sink.flush() before committing state; undelivered
    messages must be reported so the commit gate sees the failure."""
    kafka_sink = DatahubKafkaSink.create(
        {"connection": {"bootstrap": "foobar:9092"}},
        PipelineContext(run_id="test"),
    )
    kafka_sink.emitter.producers[MCP_KEY].flush.return_value = 2

    assert not kafka_sink.get_report().failures
    kafka_sink.flush()
    assert kafka_sink.get_report().failures


@patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
def test_kafka_sink_flush_then_close_does_not_double_report(mock_producer):
    """close() must not re-report undelivered messages the pre-commit flush()
    already reported (pipeline calls flush() then exit_stack calls close())."""
    kafka_sink = DatahubKafkaSink.create(
        {"connection": {"bootstrap": "foobar:9092"}},
        PipelineContext(run_id="test"),
    )
    kafka_sink.emitter.producers[MCP_KEY].flush.return_value = 5

    kafka_sink.flush()
    kafka_sink.close()
    # flush() reported once; close() skipped its redundant flush.
    assert len(kafka_sink.get_report().failures) == 1


@patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
def test_kafka_sink_handle_work_unit_end_does_not_flush(mock_producer):
    """Per-work-unit flush was removed for throughput (delivery is confirmed by
    the pipeline's pre-commit sink.flush()); guards against a re-added flush."""
    kafka_sink = DatahubKafkaSink.create(
        {"connection": {"bootstrap": "foobar:9092"}},
        PipelineContext(run_id="test"),
    )
    mock_mcp_producer = kafka_sink.emitter.producers[MCP_KEY]

    kafka_sink.handle_work_unit_end(MagicMock())

    mock_mcp_producer.flush.assert_not_called()


def test_kafka_sink_config_rejects_negative_block_seconds():
    """max_queue_full_block_seconds < 0 is rejected (would make the backpressure
    deadline already in the past)."""
    with pytest.raises(pydantic.ValidationError):
        KafkaSinkConfig.model_validate(
            {
                "connection": {"bootstrap": "foobar:9092"},
                "max_queue_full_block_seconds": -1,
            }
        )


@patch("datahub.ingestion.sink.datahub_rest.DatahubRestSink.make_emitter")
@patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
def test_kafka_sink_delete_skipped_when_flush_undelivered(
    mock_producer, mockmake_emitter
):
    """If flush() leaves messages undelivered, the DELETE must be skipped-and-
    failed -- prior writes for the entity are not confirmed landed."""
    mock_rest_emitter = MagicMock(spec=DataHubRestEmitter)
    mockmake_emitter.return_value = mock_rest_emitter

    callback = MagicMock(spec=WriteCallback)
    kafka_sink = DatahubKafkaSink.create(
        {
            "connection": {"bootstrap": "foobar:9092"},
            "rest_fallback": {"server": "http://fake-gms:8080"},
        },
        PipelineContext(run_id="test"),
    )
    # flush leaves messages undelivered (no _delivery_failed set).
    kafka_sink.emitter.producers[MCP_KEY].flush.return_value = 3

    mcp = _make_delete_mcp()
    re: RecordEnvelope[
        Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ]
    ] = RecordEnvelope(record=mcp, metadata={})
    kafka_sink.write_record_async(re, callback)

    mock_rest_emitter.emit_mcp.assert_not_called()
    callback.on_failure.assert_called_once()
    callback.on_success.assert_not_called()


@patch("datahub.ingestion.sink.datahub_rest.DatahubRestSink.make_emitter")
@patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
def test_kafka_sink_restate_routes_to_rest_fallback(mock_producer, mockmake_emitter):
    """RESTATE (like DELETE) is not supported over async Kafka -> REST fallback."""
    mock_rest_emitter = MagicMock(spec=DataHubRestEmitter)
    mockmake_emitter.return_value = mock_rest_emitter

    callback = MagicMock(spec=WriteCallback)
    kafka_sink = DatahubKafkaSink.create(
        {
            "connection": {"bootstrap": "foobar:9092"},
            "rest_fallback": {"server": "http://fake-gms:8080"},
        },
        PipelineContext(run_id="test"),
    )
    mock_mcp_producer = kafka_sink.emitter.producers[MCP_KEY]
    mock_mcp_producer.flush.return_value = 0  # fully drained

    mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount,PROD)",
        aspect=models.DatasetProfileClass(
            rowCount=1, columnCount=1, timestampMillis=1626995099686
        ),
        changeType=models.ChangeTypeClass.RESTATE,
    )
    re: RecordEnvelope[
        Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ]
    ] = RecordEnvelope(record=mcp, metadata={})
    kafka_sink.write_record_async(re, callback)

    mock_rest_emitter.emit_mcp.assert_called_once_with(
        mcp, emit_mode=EmitMode.SYNC_PRIMARY
    )
    mock_mcp_producer.produce.assert_not_called()
    callback.on_success.assert_called_once()
    kafka_sink.close()


@patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
def test_kafka_sink_patch_stays_on_kafka(mock_producer):
    """PATCH IS supported over async Kafka (GMS supportsPatch) -> must NOT fall back."""
    mock_rest_emitter = MagicMock(spec=DataHubRestEmitter)
    with patch(
        "datahub.ingestion.sink.datahub_rest.DatahubRestSink.make_emitter",
        return_value=mock_rest_emitter,
    ):
        callback = MagicMock(spec=WriteCallback)
        kafka_sink = DatahubKafkaSink.create(
            {
                "connection": {"bootstrap": "foobar:9092"},
                "rest_fallback": {"server": "http://fake-gms:8080"},
            },
            PipelineContext(run_id="test"),
        )
        mock_mcp_producer = kafka_sink.emitter.producers[MCP_KEY]

        mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount,PROD)",
            aspect=models.GlobalTagsClass(tags=[]),
            changeType=models.ChangeTypeClass.PATCH,
        )
        re: RecordEnvelope[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
            ]
        ] = RecordEnvelope(record=mcp, metadata={})
        kafka_sink.write_record_async(re, callback)

        # PATCH is Kafka-supported: produced to Kafka, REST fallback untouched.
        mock_mcp_producer.produce.assert_called_once()
        mock_rest_emitter.emit_mcp.assert_not_called()
        kafka_sink.close()


@patch("datahub.ingestion.sink.datahub_rest.DatahubRestSink.make_emitter")
@patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
def test_kafka_sink_timeseries_patch_routes_to_rest_fallback(
    mock_producer, mockmake_emitter
):
    """PATCH on a timeseries aspect -> REST (GMS accepts only UPSERT for those)."""
    mock_rest_emitter = MagicMock(spec=DataHubRestEmitter)
    mockmake_emitter.return_value = mock_rest_emitter

    callback = MagicMock(spec=WriteCallback)
    kafka_sink = DatahubKafkaSink.create(
        {
            "connection": {"bootstrap": "foobar:9092"},
            "rest_fallback": {"server": "http://fake-gms:8080"},
        },
        PipelineContext(run_id="test"),
    )
    mock_mcp_producer = kafka_sink.emitter.producers[MCP_KEY]
    mock_mcp_producer.flush.return_value = 0  # fully drained

    # datasetProfile is a timeseries aspect.
    mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount,PROD)",
        aspect=models.DatasetProfileClass(
            rowCount=1, columnCount=1, timestampMillis=1626995099686
        ),
        changeType=models.ChangeTypeClass.PATCH,
    )
    re: RecordEnvelope[
        Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ]
    ] = RecordEnvelope(record=mcp, metadata={})
    kafka_sink.write_record_async(re, callback)

    mock_rest_emitter.emit_mcp.assert_called_once_with(
        mcp, emit_mode=EmitMode.SYNC_PRIMARY
    )
    mock_mcp_producer.produce.assert_not_called()
    kafka_sink.close()


@patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
def test_kafka_sink_timeseries_upsert_stays_on_kafka(mock_producer):
    """UPSERT on a timeseries aspect (the primary managed workload -- profiles,
    usage) stays on Kafka; only non-UPSERT timeseries changes fall back to REST."""
    mock_rest_emitter = MagicMock(spec=DataHubRestEmitter)
    with patch(
        "datahub.ingestion.sink.datahub_rest.DatahubRestSink.make_emitter",
        return_value=mock_rest_emitter,
    ):
        callback = MagicMock(spec=WriteCallback)
        kafka_sink = DatahubKafkaSink.create(
            {
                "connection": {"bootstrap": "foobar:9092"},
                "rest_fallback": {"server": "http://fake-gms:8080"},
            },
            PipelineContext(run_id="test"),
        )
        mock_mcp_producer = kafka_sink.emitter.producers[MCP_KEY]

        # datasetProfile is a timeseries aspect; UPSERT is Kafka-supported.
        mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount,PROD)",
            aspect=models.DatasetProfileClass(
                rowCount=1, columnCount=1, timestampMillis=1626995099686
            ),
            changeType=models.ChangeTypeClass.UPSERT,
        )
        re: RecordEnvelope[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
            ]
        ] = RecordEnvelope(record=mcp, metadata={})
        kafka_sink.write_record_async(re, callback)

        mock_mcp_producer.produce.assert_called_once()
        mock_rest_emitter.emit_mcp.assert_not_called()
        kafka_sink.close()


def test_produce_with_backpressure_retries_on_buffer_error():
    """A full producer queue (BufferError) must block-and-retry, not crash.

    This is the sink's backpressure mechanism: produce() polls to drain
    delivery callbacks and retries instead of raising.
    """
    from datahub.emitter.kafka_emitter import DatahubKafkaEmitter

    mock_producer = MagicMock()
    # First produce() call hits a full queue, second succeeds.
    mock_producer.produce.side_effect = [BufferError("queue full"), None]

    DatahubKafkaEmitter._produce_with_backpressure(
        mock_producer,
        poll_timeout_seconds=0,
        topic="MetadataChangeProposal_v1",
        key="urn:li:dataset:(x)",
        value=MagicMock(),
        on_delivery=MagicMock(),
    )

    assert mock_producer.produce.call_count == 2
    mock_producer.poll.assert_called_once_with(0)


def test_produce_with_backpressure_gives_up_when_broker_down():
    """A permanently full queue (broker down) must raise after max_block_seconds,
    not hang forever."""
    from datahub.emitter.kafka_emitter import DatahubKafkaEmitter

    mock_producer = MagicMock()
    mock_producer.produce.side_effect = BufferError("queue full")

    with pytest.raises(BufferError):
        DatahubKafkaEmitter._produce_with_backpressure(
            mock_producer,
            poll_timeout_seconds=0,
            max_block_seconds=0,
            topic="MetadataChangeProposal_v1",
            key="urn:li:dataset:(x)",
            value=MagicMock(),
            on_delivery=MagicMock(),
        )


def test_produce_with_backpressure_converts_msg_size_too_large():
    """An oversize message (MSG_SIZE_TOO_LARGE) is not retriable and not a queue-full
    condition: it must surface as MessageTooLargeError so the sink can degrade to REST."""
    from datahub.emitter.kafka_emitter import DatahubKafkaEmitter

    mock_producer = MagicMock()
    mock_producer.produce.side_effect = KafkaException(
        KafkaError(KafkaError.MSG_SIZE_TOO_LARGE, "message too large")
    )

    with pytest.raises(MessageTooLargeError):
        DatahubKafkaEmitter._produce_with_backpressure(
            mock_producer,
            poll_timeout_seconds=0,
            topic="MetadataChangeProposal_v1",
            key="urn:li:dataset:(x)",
            value=MagicMock(),
            on_delivery=MagicMock(),
        )


def test_produce_with_backpressure_reraises_other_kafka_exception():
    """A non-size KafkaException must propagate unchanged (not swallowed/misrouted)."""
    from datahub.emitter.kafka_emitter import DatahubKafkaEmitter

    mock_producer = MagicMock()
    mock_producer.produce.side_effect = KafkaException(
        KafkaError(KafkaError._ALL_BROKERS_DOWN, "brokers down")
    )

    with pytest.raises(KafkaException):
        DatahubKafkaEmitter._produce_with_backpressure(
            mock_producer,
            poll_timeout_seconds=0,
            topic="MetadataChangeProposal_v1",
            key="urn:li:dataset:(x)",
            value=MagicMock(),
            on_delivery=MagicMock(),
        )


def _make_oversize_upsert_mcp() -> MetadataChangeProposalWrapper:
    # datasetProperties is non-timeseries; default changeType is UPSERT, so this
    # reaches the Kafka produce path (not the DELETE/RESTATE REST branch).
    return MetadataChangeProposalWrapper(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount,PROD)",
        aspect=models.DatasetPropertiesClass(description="x"),
    )


@patch("datahub.ingestion.sink.datahub_rest.DatahubRestSink.make_emitter")
@patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
def test_kafka_sink_oversize_mcp_routes_to_rest_fallback(
    mock_producer, mockmake_emitter
):
    """An MCP exceeding message.max.bytes must degrade to the REST fallback (parity
    with the REST sink's ~16 MiB cap) instead of failing the run."""
    mock_rest_emitter = MagicMock(spec=DataHubRestEmitter)
    mockmake_emitter.return_value = mock_rest_emitter

    callback = MagicMock(spec=WriteCallback)
    kafka_sink = DatahubKafkaSink.create(
        {
            "connection": {"bootstrap": "foobar:9092"},
            "rest_fallback": {"server": "http://fake-gms:8080"},
        },
        PipelineContext(run_id="test"),
    )
    kafka_sink.emitter.producers[MCP_KEY].produce.side_effect = MessageTooLargeError(
        "exceeds message.max.bytes"
    )

    re: RecordEnvelope[
        Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ]
    ] = RecordEnvelope(record=_make_oversize_upsert_mcp(), metadata={})
    kafka_sink.write_record_async(re, callback)

    mock_rest_emitter.emit_mcp.assert_called_once_with(
        re.record, emit_mode=EmitMode.SYNC_PRIMARY
    )
    callback.on_success.assert_called_once()
    callback.on_failure.assert_not_called()
    kafka_sink.close()


@patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
def test_kafka_sink_oversize_mcp_without_fallback_fails_loudly(mock_producer):
    """No rest_fallback -> an oversize MCP fails the record (never silently dropped)."""
    callback = MagicMock(spec=WriteCallback)
    kafka_sink = DatahubKafkaSink.create(
        {"connection": {"bootstrap": "foobar:9092"}},
        PipelineContext(run_id="test"),
    )
    assert kafka_sink.config.rest_fallback is None
    kafka_sink.emitter.producers[MCP_KEY].produce.side_effect = MessageTooLargeError(
        "exceeds message.max.bytes"
    )

    re: RecordEnvelope[
        Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ]
    ] = RecordEnvelope(record=_make_oversize_upsert_mcp(), metadata={})
    kafka_sink.write_record_async(re, callback)

    callback.on_failure.assert_called_once()
    callback.on_success.assert_not_called()
    kafka_sink.close()


@patch("datahub.ingestion.sink.datahub_rest.DatahubRestSink.make_emitter")
@patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
def test_kafka_sink_oversize_mcp_rest_failure_is_reported(
    mock_producer, mockmake_emitter
):
    """If the REST fallback itself raises on the oversize MCP, the record is failed."""
    mock_rest_emitter = MagicMock(spec=DataHubRestEmitter)
    mock_rest_emitter.emit_mcp.side_effect = Exception("GMS 500")
    mockmake_emitter.return_value = mock_rest_emitter

    callback = MagicMock(spec=WriteCallback)
    kafka_sink = DatahubKafkaSink.create(
        {
            "connection": {"bootstrap": "foobar:9092"},
            "rest_fallback": {"server": "http://fake-gms:8080"},
        },
        PipelineContext(run_id="test"),
    )
    kafka_sink.emitter.producers[MCP_KEY].produce.side_effect = MessageTooLargeError(
        "exceeds message.max.bytes"
    )

    re: RecordEnvelope[
        Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ]
    ] = RecordEnvelope(record=_make_oversize_upsert_mcp(), metadata={})
    kafka_sink.write_record_async(re, callback)

    mock_rest_emitter.emit_mcp.assert_called_once_with(
        re.record, emit_mode=EmitMode.SYNC_PRIMARY
    )
    callback.on_failure.assert_called_once()
    callback.on_success.assert_not_called()
    kafka_sink.close()


@patch("datahub.ingestion.sink.datahub_rest.DatahubRestSink.make_emitter")
@patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
def test_kafka_sink_oversize_mce_all_aspects_route_to_rest_fallback(
    mock_producer, mockmake_emitter
):
    """An MCE whose every unpacked MCP is oversize: each degrades to REST and the
    aggregating callback still fires on_success exactly once (N REST ticks = 1
    write_callback)."""
    mock_rest_emitter = MagicMock(spec=DataHubRestEmitter)
    mockmake_emitter.return_value = mock_rest_emitter

    callback = MagicMock(spec=WriteCallback)
    kafka_sink = DatahubKafkaSink.create(
        {
            "connection": {"bootstrap": "foobar:9092"},
            "rest_fallback": {"server": "http://fake-gms:8080"},
        },
        PipelineContext(run_id="test"),
    )
    kafka_sink.emitter.producers[MCP_KEY].produce.side_effect = MessageTooLargeError(
        "exceeds message.max.bytes"
    )

    user_snapshot = models.CorpUserSnapshotClass(
        urn=builder.make_user_urn("testuser"),
        aspects=[
            models.CorpUserInfoClass(
                active=True, displayName="Test User", email="t@example.com"
            ),
            models.GroupMembershipClass(groups=[builder.make_group_urn("g1")]),
        ],
    )
    mce = MetadataChangeEvent(proposedSnapshot=user_snapshot)
    re: RecordEnvelope[
        Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ]
    ] = RecordEnvelope(record=mce, metadata={})
    kafka_sink.write_record_async(re, callback)

    assert mock_rest_emitter.emit_mcp.call_count == 2
    callback.on_success.assert_called_once()
    callback.on_failure.assert_not_called()
    kafka_sink.close()


@patch("datahub.ingestion.sink.datahub_rest.DatahubRestSink.make_emitter")
@patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
def test_kafka_sink_oversize_mce_mixed_routes_partial_to_rest_fallback(
    mock_producer,
    mockmake_emitter,
):
    """An MCE where one aspect fits Kafka and one is oversize: the oversize MCP
    degrades to REST, the rest produce to Kafka, and the aggregating callback
    fires on_success exactly once after the in-flight Kafka delivery reports
    success (REST tick + 1 Kafka delivery = 1 write_callback)."""
    mock_rest_emitter = MagicMock(spec=DataHubRestEmitter)
    mockmake_emitter.return_value = mock_rest_emitter

    callback = MagicMock(spec=WriteCallback)
    kafka_sink = DatahubKafkaSink.create(
        {
            "connection": {"bootstrap": "foobar:9092"},
            "rest_fallback": {"server": "http://fake-gms:8080"},
        },
        PipelineContext(run_id="test"),
    )
    mock_mcp_producer = kafka_sink.emitter.producers[MCP_KEY]
    # First unpacked MCP produces to Kafka (queued); second is oversize -> REST.
    mock_mcp_producer.produce.side_effect = [None, MessageTooLargeError("too big")]

    user_snapshot = models.CorpUserSnapshotClass(
        urn=builder.make_user_urn("testuser"),
        aspects=[
            models.CorpUserInfoClass(
                active=True, displayName="Test User", email="t@example.com"
            ),
            models.GroupMembershipClass(groups=[builder.make_group_urn("g1")]),
        ],
    )
    mce = MetadataChangeEvent(proposedSnapshot=user_snapshot)
    re: RecordEnvelope[
        Union[
            MetadataChangeEvent,
            MetadataChangeProposal,
            MetadataChangeProposalWrapper,
        ]
    ] = RecordEnvelope(record=mce, metadata={})
    kafka_sink.write_record_async(re, callback)

    assert mock_mcp_producer.produce.call_count == 2
    assert mock_rest_emitter.emit_mcp.call_count == 1
    # Agg not yet complete: the queued Kafka MCP delivery is pending.
    callback.on_success.assert_not_called()
    callback.on_failure.assert_not_called()

    # Simulate the in-flight Kafka delivery (the first, queued produce)
    # succeeding -> agg ticks to 0 -> on_success fires exactly once.
    kafka_delivery = mock_mcp_producer.produce.call_args_list[0].kwargs["on_delivery"]
    kafka_delivery(None, MagicMock())
    callback.on_success.assert_called_once()
    callback.on_failure.assert_not_called()
    kafka_sink.close()


@patch("datahub.ingestion.sink.datahub_rest.DatahubRestSink.make_emitter")
@patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
def test_kafka_sink_oversize_rest_failure_blocks_subsequent_delete(
    mock_producer,
    mockmake_emitter,
):
    """An oversize MCP whose REST fallback also fails sets the delivery-failed
    signal, so a subsequent DELETE is NOT applied via REST (its prior write never
    landed) -- it is reported as a failure instead."""
    mock_rest_emitter = MagicMock(spec=DataHubRestEmitter)
    mockmake_emitter.return_value = mock_rest_emitter

    kafka_sink = DatahubKafkaSink.create(
        {
            "connection": {"bootstrap": "foobar:9092"},
            "rest_fallback": {"server": "http://fake-gms:8080"},
        },
        PipelineContext(run_id="test"),
    )
    mock_mcp_producer = kafka_sink.emitter.producers[MCP_KEY]

    # First record: oversize -> REST fallback raises -> failure signal set.
    mock_mcp_producer.produce.side_effect = MessageTooLargeError("too big")
    mock_mcp_producer.flush.return_value = 0
    mock_rest_emitter.emit_mcp.side_effect = Exception("GMS 500")

    oversize_re = RecordEnvelope(record=_make_oversize_upsert_mcp(), metadata={})
    kafka_sink.write_record_async(oversize_re, MagicMock(spec=WriteCallback))

    assert kafka_sink._delivery_failed.is_set()

    # Second record: a DELETE. Prior write failed -> must not be applied via REST.
    mock_mcp_producer.produce.side_effect = None
    mock_rest_emitter.emit_mcp.side_effect = None
    del_callback = MagicMock(spec=WriteCallback)
    delete_re = RecordEnvelope(record=_make_delete_mcp(), metadata={})
    kafka_sink.write_record_async(delete_re, del_callback)

    # REST emit happened only for the oversize attempt, never for the DELETE.
    mock_rest_emitter.emit_mcp.assert_called_once_with(
        oversize_re.record, emit_mode=EmitMode.SYNC_PRIMARY
    )
    del_callback.on_failure.assert_called_once()
    del_callback.on_success.assert_not_called()
    kafka_sink.close()
