"""Unit tests for Kafka utility functions."""

import base64
from typing import Dict
from unittest.mock import Mock

from datahub.ingestion.source.kafka.kafka_utils import (
    decode_kafka_message_value,
    process_kafka_message_for_sampling,
)


class TestDecodeKafkaMessageValue:
    """Test the decode_kafka_message_value utility function."""

    def test_decode_already_decoded_value(self):
        """Test that already decoded values are returned as-is."""
        value = {"already": "decoded"}
        # Type ignore because we're testing the function's handling of non-bytes input
        result = decode_kafka_message_value(value, "test-topic")  # type: ignore
        assert result == value

    def test_decode_json_dict(self):
        """Test decoding JSON dictionary."""
        value = b'{"field1": "value1", "field2": 123}'
        result = decode_kafka_message_value(value, "test-topic")
        expected = {"field1": "value1", "field2": 123}
        assert result == expected

    def test_decode_json_list(self):
        """Test decoding JSON list (wrapped in dict)."""
        value = b'["item1", "item2", 123]'
        result = decode_kafka_message_value(value, "test-topic")
        expected = {"item": ["item1", "item2", 123]}
        assert result == expected

    def test_decode_json_primitive(self):
        """Test decoding JSON primitive values."""
        # String
        result = decode_kafka_message_value(b'"hello"', "test-topic")
        assert result == "hello"

        # Number
        result = decode_kafka_message_value(b"42", "test-topic")
        assert result == 42

        # Boolean
        result = decode_kafka_message_value(b"true", "test-topic")
        assert result is True

    def test_decode_plain_text(self):
        """Test decoding plain text (not JSON)."""
        value = b"plain text message"
        result = decode_kafka_message_value(value, "test-topic")
        expected = {"text_value": "plain text message"}
        assert result == expected

    def test_decode_empty_message(self):
        """Test decoding empty or whitespace-only messages."""
        # Empty string
        result = decode_kafka_message_value(b"", "test-topic")
        expected = {"empty_message": True}
        assert result == expected

        # Whitespace only
        result = decode_kafka_message_value(b"   \n\t  ", "test-topic")
        expected = {"empty_message": True}
        assert result == expected

    def test_decode_binary_data(self):
        """Test decoding binary data (non-UTF8)."""
        value = b"\x8a\x8b\x8c\xde\xad\xbe\xef"
        result = decode_kafka_message_value(value, "test-topic")
        expected_b64 = base64.b64encode(value).decode("utf-8")
        expected = {"binary_data": expected_b64}
        assert result == expected

    def test_decode_with_flatten_json_function(self):
        """Test decoding with JSON flattening function."""
        mock_flatten = Mock(return_value={"flattened": "result"})
        value = b'{"nested": {"field": "value"}}'

        result = decode_kafka_message_value(
            value, "test-topic", flatten_json_func=mock_flatten, max_depth=5
        )

        # Should call flatten function with the decoded dict
        mock_flatten.assert_called_once()
        call_args = mock_flatten.call_args
        assert call_args[0][0] == {"nested": {"field": "value"}}  # First positional arg
        assert call_args[0][3] == 5  # max_depth
        assert result == {"flattened": "result"}

    def test_decode_exception_handling(self):
        """Test that exceptions are handled gracefully."""
        # Create a bytes object that will cause an exception in our processing
        value = b'{"malformed": json}'  # This will cause JSONDecodeError but should be handled

        result = decode_kafka_message_value(value, "test-topic")
        expected = {"text_value": '{"malformed": json}'}
        assert result == expected


class TestProcessKafkaMessageForSampling:
    """Test the process_kafka_message_for_sampling utility function."""

    def test_process_none_value(self):
        """Test processing None values."""
        result = process_kafka_message_for_sampling(None)
        expected = {"null_value": True}
        assert result == expected

    def test_process_dict_value(self):
        """Test processing dictionary values (returned as-is)."""
        value = {"field1": "value1", "field2": 123}
        result = process_kafka_message_for_sampling(value)
        assert result == value

    def test_process_json_bytes(self):
        """Test processing bytes containing JSON."""
        value = b'{"field1": "value1", "field2": 123}'
        result = process_kafka_message_for_sampling(value)
        expected = {"field1": "value1", "field2": 123}
        assert result == expected

    def test_process_json_list_bytes(self):
        """Test processing bytes containing JSON list."""
        value = b'["item1", "item2"]'
        result = process_kafka_message_for_sampling(value)
        expected = {"json_value": ["item1", "item2"]}
        assert result == expected

    def test_process_json_primitive_bytes(self):
        """Test processing bytes containing JSON primitives."""
        # String
        result = process_kafka_message_for_sampling(b'"hello"')
        expected = {"json_value": "hello"}
        assert result == expected

        # Number
        result = process_kafka_message_for_sampling(b"42")
        expected_num = {"json_value": 42}
        assert result == expected_num

    def test_process_text_bytes(self):
        """Test processing bytes containing plain text."""
        value = b"plain text message"
        result = process_kafka_message_for_sampling(value)
        expected = {"text_value": "plain text message"}
        assert result == expected

    def test_process_empty_bytes(self):
        """Test processing empty or whitespace bytes."""
        # Empty
        result = process_kafka_message_for_sampling(b"")
        expected = {"empty_message": True}
        assert result == expected

        # Whitespace only
        result = process_kafka_message_for_sampling(b"   \n\t  ")
        expected = {"empty_message": True}
        assert result == expected

    def test_process_binary_bytes(self):
        """Test processing binary (non-UTF8) bytes."""
        value = b"\x8a\x8b\x8c\xde\xad\xbe\xef"
        result = process_kafka_message_for_sampling(value)
        expected_b64 = base64.b64encode(value).decode("utf-8")
        expected = {"binary_data": expected_b64}
        assert result == expected

    def test_process_other_types(self):
        """Test processing other Python types."""
        # String
        result = process_kafka_message_for_sampling("hello")
        expected = {"value": "hello"}
        assert result == expected

        # Integer
        result = process_kafka_message_for_sampling(123)
        expected = {"value": "123"}
        assert result == expected

        # Float
        result = process_kafka_message_for_sampling(45.6)
        expected = {"value": "45.6"}
        assert result == expected

        # Boolean
        result = process_kafka_message_for_sampling(True)
        expected = {"value": "True"}
        assert result == expected

        # List
        result = process_kafka_message_for_sampling([1, 2, 3])
        expected_list: Dict[str, str] = {"value": "[1, 2, 3]"}
        assert result == expected_list

    def test_process_malformed_json_bytes(self):
        """Test processing bytes with malformed JSON."""
        value = b'{"malformed": json}'
        result = process_kafka_message_for_sampling(value)
        expected = {"text_value": '{"malformed": json}'}
        assert result == expected
