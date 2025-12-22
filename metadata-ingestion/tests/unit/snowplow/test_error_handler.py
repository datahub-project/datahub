"""Unit tests for ErrorHandler."""

from unittest.mock import patch

from datahub.ingestion.api.source import StructuredLogEntry
from datahub.ingestion.source.snowplow.services.error_handler import ErrorHandler
from datahub.ingestion.source.snowplow.snowplow_report import SnowplowSourceReport


class TestErrorHandlerAPIErrors:
    """Test API error handling."""

    def test_handle_api_error_basic(self):
        """Test basic API error handling."""
        report = SnowplowSourceReport()
        handler = ErrorHandler(report)

        error = Exception("Connection timeout")

        handler.handle_api_error(error, "fetch schemas")

        # Should log to report
        assert len(report.failures) > 0

    def test_handle_api_error_with_entity(self):
        """Test API error handling with entity identifier."""
        report = SnowplowSourceReport()
        handler = ErrorHandler(report)

        error = Exception("404 Not Found")

        handler.handle_api_error(
            error,
            "fetch pipeline configuration",
            entity="pipeline-abc123",
        )

        # Should include entity in context
        assert len(report.failures) > 0
        log_entry = report.failures[0]
        assert isinstance(log_entry, StructuredLogEntry)
        assert "pipeline-abc123" in str(log_entry.context)

    def test_handle_api_error_with_context(self):
        """Test API error handling with additional context."""
        report = SnowplowSourceReport()
        handler = ErrorHandler(report)

        error = Exception("Rate limit exceeded")

        handler.handle_api_error(
            error,
            "fetch data structures",
            context="page=5",
        )

        # Should include context
        assert len(report.failures) > 0
        log_entry = report.failures[0]
        assert isinstance(log_entry, StructuredLogEntry)
        assert "page=5" in str(log_entry.context)

    def test_handle_api_error_with_all_params(self):
        """Test API error handling with all optional parameters."""
        report = SnowplowSourceReport()
        handler = ErrorHandler(report)

        error = Exception("Permission denied")

        handler.handle_api_error(
            error,
            "update schema",
            context="org_id=test",
            entity="schema-xyz",
        )

        # Should include all context
        assert len(report.failures) > 0
        log_entry = report.failures[0]
        assert isinstance(log_entry, StructuredLogEntry)
        context_str = str(log_entry.context)
        assert "schema-xyz" in context_str
        assert "org_id=test" in context_str


class TestErrorHandlerValidationErrors:
    """Test validation error handling."""

    def test_handle_validation_error_basic(self):
        """Test basic validation error handling."""
        report = SnowplowSourceReport()
        handler = ErrorHandler(report)

        error = ValueError("Missing required field")

        handler.handle_validation_error(
            error,
            entity_type="schema",
            entity_id="com.example/test_schema/jsonschema/1-0-0",
        )

        # Should log to report
        assert len(report.failures) > 0
        log_entry = report.failures[0]
        assert isinstance(log_entry, StructuredLogEntry)
        assert "schema" in str(log_entry.title or "")
        assert "test_schema" in str(log_entry.context)

    def test_handle_validation_error_with_field(self):
        """Test validation error with specific field."""
        report = SnowplowSourceReport()
        handler = ErrorHandler(report)

        error = ValueError("Invalid value format")

        handler.handle_validation_error(
            error,
            entity_type="pipeline",
            entity_id="pipeline-1",
            field="enrichments",
        )

        # Should include field in context
        assert len(report.failures) > 0
        log_entry = report.failures[0]
        assert isinstance(log_entry, StructuredLogEntry)
        assert "enrichments" in str(log_entry.context)

    def test_handle_validation_error_different_entity_types(self):
        """Test validation errors for different entity types."""
        report = SnowplowSourceReport()
        handler = ErrorHandler(report)

        # Test with different entity types
        entity_types = ["schema", "pipeline", "event_spec", "data_product"]

        for entity_type in entity_types:
            handler.handle_validation_error(
                ValueError("Test error"),
                entity_type=entity_type,
                entity_id=f"{entity_type}-1",
            )

        # Should log all failures
        assert len(report.failures) == len(entity_types)


class TestErrorHandlerProcessingErrors:
    """Test processing error handling."""

    def test_handle_processing_error_non_fatal(self):
        """Test non-fatal processing error."""
        report = SnowplowSourceReport()
        handler = ErrorHandler(report)

        error = Exception("Parse error")

        handler.handle_processing_error(
            error,
            operation="parse schema",
            entity_type="schema",
            entity_id="test-schema",
            fatal=False,
        )

        # Should log as warning (non-fatal)
        assert len(report.failures) > 0

    def test_handle_processing_error_fatal(self):
        """Test fatal processing error."""
        report = SnowplowSourceReport()
        handler = ErrorHandler(report)

        error = Exception("Critical failure")

        handler.handle_processing_error(
            error,
            operation="initialize connection",
            entity_type="source",
            entity_id="snowplow-source",
            fatal=True,
        )

        # Should log as error (fatal)
        assert len(report.failures) > 0
        log_entry = report.failures[0]
        assert isinstance(log_entry, StructuredLogEntry)
        assert "initialize connection" in str(log_entry.title or "")

    def test_handle_processing_error_different_operations(self):
        """Test processing errors for different operations."""
        report = SnowplowSourceReport()
        handler = ErrorHandler(report)

        operations = [
            "parse schema",
            "build lineage",
            "extract metadata",
            "transform data",
        ]

        for operation in operations:
            handler.handle_processing_error(
                Exception("Test error"),
                operation=operation,
                entity_type="test",
                entity_id="test-1",
            )

        # Should log all failures with different operations
        assert len(report.failures) == len(operations)


class TestErrorHandlerMissingDependency:
    """Test missing dependency error handling."""

    def test_handle_missing_dependency_basic(self):
        """Test basic missing dependency handling."""
        report = SnowplowSourceReport()
        handler = ErrorHandler(report)

        handler.handle_missing_dependency(
            dependency="snowplow-api-client",
            operation="fetch enrichments",
        )

        # Should log to report
        assert len(report.failures) > 0
        log_entry = report.failures[0]
        assert isinstance(log_entry, StructuredLogEntry)
        assert "snowplow-api-client" in str(log_entry.title or "")

    def test_handle_missing_dependency_with_suggestion(self):
        """Test missing dependency with suggestion."""
        report = SnowplowSourceReport()
        handler = ErrorHandler(report)

        handler.handle_missing_dependency(
            dependency="sqlalchemy",
            operation="connect to warehouse",
            suggestion="Install with: pip install sqlalchemy",
        )

        # Should include suggestion in message
        assert len(report.failures) > 0
        log_entry = report.failures[0]
        assert isinstance(log_entry, StructuredLogEntry)
        assert "pip install" in str(log_entry.message)

    def test_handle_missing_dependency_multiple(self):
        """Test handling multiple missing dependencies."""
        report = SnowplowSourceReport()
        handler = ErrorHandler(report)

        dependencies = ["package1", "package2", "package3"]

        for dep in dependencies:
            handler.handle_missing_dependency(
                dependency=dep,
                operation="test operation",
            )

        # Should log all dependencies
        assert len(report.failures) == len(dependencies)


class TestErrorHandlerLogging:
    """Test logging methods."""

    @patch("datahub.ingestion.source.snowplow.services.error_handler.logger")
    def test_log_warning_with_context(self, mock_logger):
        """Test warning logging with context."""
        report = SnowplowSourceReport()
        handler = ErrorHandler(report)

        handler.log_warning(
            "Schema missing optional field", context="field=description"
        )

        # Should include context in log message
        call_args = mock_logger.warning.call_args[0]
        assert "field=description" in call_args[0]

    @patch("datahub.ingestion.source.snowplow.services.error_handler.logger")
    def test_log_info_with_context(self, mock_logger):
        """Test info logging with context."""
        report = SnowplowSourceReport()
        handler = ErrorHandler(report)

        handler.log_info("Processed batch", context="count=100")

        # Should include context in log message
        call_args = mock_logger.info.call_args[0]
        assert "count=100" in call_args[0]


class TestErrorHandlerIntegration:
    """Test error handler integration scenarios."""

    def test_multiple_error_types_same_report(self):
        """Test handling multiple error types in same report."""
        report = SnowplowSourceReport()
        handler = ErrorHandler(report)

        # API error
        handler.handle_api_error(
            Exception("API failed"),
            "fetch data",
        )

        # Validation error
        handler.handle_validation_error(
            ValueError("Invalid"),
            entity_type="schema",
            entity_id="test-1",
        )

        # Processing error
        handler.handle_processing_error(
            Exception("Processing failed"),
            operation="transform",
            entity_type="dataset",
            entity_id="test-2",
        )

        # Should have all three failures
        assert len(report.failures) == 3

    def test_error_handler_with_real_exceptions(self):
        """Test error handler with real exception types."""
        report = SnowplowSourceReport()
        handler = ErrorHandler(report)

        # Different exception types
        exceptions = [
            ValueError("Value error"),
            TypeError("Type error"),
            KeyError("Key error"),
            AttributeError("Attribute error"),
            RuntimeError("Runtime error"),
        ]

        for i, exc in enumerate(exceptions):
            handler.handle_api_error(exc, f"operation-{i}")

        # Should handle all exception types
        assert len(report.failures) == len(exceptions)

    def test_error_context_formatting(self):
        """Test that error context is formatted consistently."""
        report = SnowplowSourceReport()
        handler = ErrorHandler(report)

        # Test various context combinations
        handler.handle_api_error(
            Exception("Test"),
            "operation1",
            context="ctx1",
            entity="ent1",
        )

        handler.handle_api_error(
            Exception("Test"),
            "operation2",
            entity="ent2",
        )

        handler.handle_api_error(
            Exception("Test"),
            "operation3",
            context="ctx3",
        )

        # All should have consistent context formatting
        assert len(report.failures) == 3
        for log_entry in report.failures:
            assert log_entry.context  # Not empty
