"""Test BigQuery table name sanitization logic."""

import pytest

from datahub.ingestion.source.kafka_connect.common import (
    ConnectorManifest,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.sink_connectors import BigQuerySinkConnector


class TestBigQuerySinkConnectorSanitization:
    """Test BigQuery sink connector table name sanitization following Kafka Connect logic."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.manifest = ConnectorManifest(
            name="test-bq-sink",
            type="sink",
            config={
                "project": "test-project",
                "defaultDataset": "test_dataset",
                "sanitizeTopics": "true",
            },
            tasks=[],
        )

        self.config = KafkaConnectSourceConfig()
        self.report = KafkaConnectSourceReport()
        self.connector = BigQuerySinkConnector(self.manifest, self.config, self.report)

    def test_valid_table_names_unchanged(self) -> None:
        """Test that valid table names are left unchanged."""
        test_cases = [
            "valid_table",
            "Valid_Table123",
            "table123",
            "_valid_table",
            "table_name_123",
            "UPPERCASE_TABLE",
            "mixedCase_Table_123",
        ]

        for table_name in test_cases:
            result = self.connector.sanitize_table_name(table_name)
            assert result == table_name, (
                f"Valid name '{table_name}' should remain unchanged, got '{result}'"
            )

    def test_invalid_character_replacement(self) -> None:
        """Test replacement of invalid characters with underscores."""
        test_cases = [
            # (input, expected_output)
            ("topic-with-dashes", "topic_with_dashes"),
            ("topic.with.dots", "topic_with_dots"),
            ("topic with spaces", "topic_with_spaces"),
            ("topic@special#chars$", "topic_special_chars"),
            ("topic/with\\slashes", "topic_with_slashes"),
            ("topic+with=symbols", "topic_with_symbols"),
            ("topic(with)parentheses", "topic_with_parentheses"),
            ("topic[with]brackets", "topic_with_brackets"),
            ("topic{with}braces", "topic_with_braces"),
            ("topic,with;punctuation:", "topic_with_punctuation"),
            ("topic!with?exclamation", "topic_with_exclamation"),
        ]

        for input_name, expected in test_cases:
            result = self.connector.sanitize_table_name(input_name)
            assert result == expected, (
                f"'{input_name}' should become '{expected}', got '{result}'"
            )

    def test_numeric_start_handling(self) -> None:
        """Test prepending underscore for names starting with digits."""
        test_cases = [
            # (input, expected_output)
            ("123numeric", "_123numeric"),
            ("9test", "_9test"),
            ("0table", "_0table"),
            ("2023_events", "_2023_events"),
            ("1_table", "_1_table"),
            ("42answer", "_42answer"),
        ]

        for input_name, expected in test_cases:
            result = self.connector.sanitize_table_name(input_name)
            assert result == expected, (
                f"'{input_name}' should become '{expected}', got '{result}'"
            )

    def test_consecutive_underscore_cleanup(self) -> None:
        """Test removal of consecutive underscores."""
        test_cases = [
            # (input, expected_output)
            ("multiple___underscores", "multiple_underscores"),
            ("table____name", "table_name"),
            ("a_____b", "a_b"),
            ("leading____and____trailing", "leading_and_trailing"),
            (
                "_____many_underscores_____",
                "_many_underscores",
            ),  # Preserves one leading underscore from original
        ]

        for input_name, expected in test_cases:
            result = self.connector.sanitize_table_name(input_name)
            assert result == expected, (
                f"'{input_name}' should become '{expected}', got '{result}'"
            )

    def test_leading_trailing_underscore_removal(self) -> None:
        """Test handling of leading and trailing underscores following Kafka Connect behavior."""
        test_cases = [
            # (input, expected_output) - Kafka Connect preserves single leading underscore if original had it
            (
                "__leading_underscores",
                "_leading_underscores",
            ),  # Preserves one leading underscore
            (
                "trailing_underscores__",
                "trailing_underscores",
            ),  # Removes trailing underscores
            (
                "__both_sides__",
                "_both_sides",
            ),  # Preserves one leading, removes trailing
            ("_single_leading", "_single_leading"),  # Already valid, unchanged
            ("single_trailing_", "single_trailing"),  # Removes trailing underscore
        ]

        for input_name, expected in test_cases:
            result = self.connector.sanitize_table_name(input_name)
            assert result == expected, (
                f"'{input_name}' should become '{expected}', got '{result}'"
            )

    def test_digit_handling_preserves_underscore(self) -> None:
        """Test that digit-handling underscore is preserved even during cleanup."""
        test_cases = [
            # (input, expected_output) - underscore added for digit should be preserved
            ("123___table___", "_123_table"),
            ("9__test__", "_9_test"),
            ("0___event___data___", "_0_event_data"),
            (
                "___123___",
                "_123",
            ),  # Leading underscores removed, digit underscore preserved
        ]

        for input_name, expected in test_cases:
            result = self.connector.sanitize_table_name(input_name)
            assert result == expected, (
                f"'{input_name}' should become '{expected}', got '{result}'"
            )

    def test_complex_mixed_cases(self) -> None:
        """Test complex cases with multiple sanitization rules applied."""
        test_cases = [
            # (input, expected_output)
            ("123-topic.with@special", "_123_topic_with_special"),
            ("user-events-2023", "user_events_2023"),
            ("9user@events#2023$data", "_9user_events_2023_data"),
            ("___123--.topic..name___", "_123_topic_name"),
            ("2023/user-data@domain.com", "_2023_user_data_domain_com"),
            ("order_events-2023!@#$%", "order_events_2023"),
        ]

        for input_name, expected in test_cases:
            result = self.connector.sanitize_table_name(input_name)
            assert result == expected, (
                f"'{input_name}' should become '{expected}', got '{result}'"
            )

    def test_empty_input_raises_error(self) -> None:
        """Test that empty input raises ValueError."""
        invalid_inputs = ["", "   ", "\t\n", "  \t  "]

        for invalid_input in invalid_inputs:
            with pytest.raises(ValueError, match="Table name cannot be empty"):
                self.connector.sanitize_table_name(invalid_input)

    def test_all_invalid_chars_raises_error(self) -> None:
        """Test that input with only invalid characters raises ValueError."""
        invalid_inputs = ["___", "@#$%", "---", "...", "!!!"]

        for invalid_input in invalid_inputs:
            with pytest.raises(
                ValueError, match="cannot be sanitized to a valid BigQuery table name"
            ):
                self.connector.sanitize_table_name(invalid_input)

    def test_length_truncation(self) -> None:
        """Test that overly long names are truncated to BigQuery's 1024 character limit."""
        # Create a name longer than 1024 characters
        long_name = "a" * 1030  # 1030 characters

        result = self.connector.sanitize_table_name(long_name)

        # Should be truncated to 1024 characters
        assert len(result) == 1024, f"Expected length 1024, got {len(result)}"
        assert result == "a" * 1024, (
            "Should be truncated to exactly 1024 'a' characters"
        )

    def test_length_truncation_with_trailing_underscores(self) -> None:
        """Test that trailing underscores are removed during truncation."""
        # Create a name that becomes exactly 1024 chars + trailing underscores after sanitization
        long_name = (
            "a" * 1020 + "____"
        )  # Will become 1024 chars, then truncated and rstripped

        result = self.connector.sanitize_table_name(long_name)

        # Should be truncated and rstripped
        assert len(result) == 1020, f"Expected length 1020, got {len(result)}"
        assert result == "a" * 1020, (
            "Should be 1020 'a' characters with trailing underscores removed"
        )

    def test_real_world_topic_names(self) -> None:
        """Test sanitization with realistic Kafka topic names."""
        test_cases = [
            # Common real-world patterns
            ("user.events", "user_events"),
            ("order-processing", "order_processing"),
            ("payment_notifications", "payment_notifications"),
            ("inventory.stock.updates", "inventory_stock_updates"),
            ("user-profile-updates", "user_profile_updates"),
            ("event.stream.v2", "event_stream_v2"),
            ("api.gateway.logs", "api_gateway_logs"),
            ("metrics-collector-data", "metrics_collector_data"),
            ("2023.audit.logs", "_2023_audit_logs"),
            ("db.change.events", "db_change_events"),
        ]

        for input_name, expected in test_cases:
            result = self.connector.sanitize_table_name(input_name)
            assert result == expected, (
                f"Real-world case: '{input_name}' should become '{expected}', got '{result}'"
            )

    def test_kafka_connect_compatibility(self) -> None:
        """Test that our implementation matches official Kafka Connect BigQuery connector behavior."""
        # These test cases are based on the official Kafka Connect BigQuery connector documentation
        # Reference: https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka
        # Reference: https://github.com/confluentinc/kafka-connect-bigquery
        test_cases = [
            # Official documented behavior: "All invalid characters are replaced by underscores"
            ("topic-name", "topic_name"),
            ("topic.name", "topic_name"),
            ("topic name", "topic_name"),
            # Official documented behavior: "If the resulting name would start with a digit, an underscore is prepended"
            ("1topic", "_1topic"),
            ("2023-events", "_2023_events"),
            ("9data", "_9data"),
            # Combined cases
            ("123.topic-name", "_123_topic_name"),
            ("user@events#123", "user_events_123"),
        ]

        for input_name, expected in test_cases:
            result = self.connector.sanitize_table_name(input_name)
            assert result == expected, (
                f"Kafka Connect compatibility: '{input_name}' should become '{expected}', got '{result}'"
            )

    def test_bigquery_naming_rules_compliance(self) -> None:
        """Test compliance with BigQuery table naming rules."""
        # Reference: https://cloud.google.com/bigquery/docs/tables#table_naming
        test_cases = [
            # Must contain only letters, numbers, and underscores
            ("valid-topic", "valid_topic"),
            ("topic.name", "topic_name"),
            ("topic@domain.com", "topic_domain_com"),
            # Must start with a letter or underscore
            ("123table", "_123table"),
            ("9events", "_9events"),
            # Maximum 1024 characters (tested separately in length tests)
            # Unicode characters should be replaced
            ("topic™️", "topic"),  # Trademark symbol replaced
            ("café-events", "caf_events"),  # Accented characters replaced
        ]

        for input_name, expected in test_cases:
            result = self.connector.sanitize_table_name(input_name)
            assert result == expected, (
                f"BigQuery naming rules: '{input_name}' should become '{expected}', got '{result}'"
            )

    def test_edge_cases(self) -> None:
        """Test edge cases and boundary conditions."""
        test_cases = [
            # Single characters
            ("a", "a"),
            ("1", "_1"),
            ("_", ""),  # Should result in empty string and raise error
            # Mixed valid/invalid patterns
            ("a1b2c3", "a1b2c3"),  # Valid mixed alphanumeric
            ("1a2b3c", "_1a2b3c"),  # Starts with digit
            (
                "___a___b___",
                "_a_b",
            ),  # Multiple consecutive underscores, preserves leading
        ]

        for input_name, expected in test_cases:
            if expected == "":
                # Empty result should raise error
                with pytest.raises(
                    ValueError,
                    match="cannot be sanitized to a valid BigQuery table name",
                ):
                    self.connector.sanitize_table_name(input_name)
            else:
                result = self.connector.sanitize_table_name(input_name)
                assert result == expected, (
                    f"Edge case: '{input_name}' should become '{expected}', got '{result}'"
                )
