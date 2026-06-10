import threading
import unittest
from typing import Union
from unittest.mock import MagicMock, call, patch

import pytest

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.configuration.common import ConfigurationError
from datahub.emitter.kafka_emitter import MCP_KEY
from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
        kafka_sink = DatahubKafkaSink.create({}, mock_context)
        kafka_sink.close()
        mock_producer_instance.flush.assert_has_calls([call(), call()])

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
