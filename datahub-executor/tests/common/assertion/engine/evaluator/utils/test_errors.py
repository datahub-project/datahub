from typing import Any, Dict, cast

import pytest

from datahub_executor.common.assertion.engine.evaluator.utils.errors import (
    extract_assertion_evaluation_result_error,
)
from datahub_executor.common.exceptions import (
    AssertionResultException,
    CustomSQLErrorException,
    FieldAssertionErrorException,
    InsufficientDataException,
    InvalidParametersException,
    InvalidSourceTypeException,
    SourceConnectionErrorException,
    SourceQueryFailedException,
    UnsupportedPlatformException,
)
from datahub_executor.common.types import (
    AssertionEvaluationResultError,
    AssertionResultErrorType,
    DatasetFreshnessSourceType,
)


class TestErrorUtils:
    """Test suite for error utility functions."""

    def test_extract_insufficient_data_exception(self) -> None:
        """Test extraction of InsufficientDataException."""
        exception = InsufficientDataException(message="Not enough data to evaluate")

        result = extract_assertion_evaluation_result_error(exception)

        assert isinstance(result, AssertionEvaluationResultError)
        assert result.type == AssertionResultErrorType.INSUFFICIENT_DATA
        assert result.properties is not None
        assert result.properties["message"] == "Not enough data to evaluate"

    def test_extract_invalid_parameters_exception(self) -> None:
        """Test extraction of InvalidParametersException."""
        parameters: Dict[str, Any] = {"operator": "EQUAL_TO", "value": None}
        exception = InvalidParametersException(
            message="Missing required parameter", parameters=parameters
        )

        result = extract_assertion_evaluation_result_error(exception)

        assert isinstance(result, AssertionEvaluationResultError)
        assert result.type == AssertionResultErrorType.INVALID_PARAMETERS
        assert result.properties is not None
        assert result.properties["message"] == "Missing required parameter"
        assert result.properties["parameters"] == f"{parameters}"

    def test_extract_invalid_source_type_exception(self) -> None:
        """Test extraction of InvalidSourceTypeException."""
        source_type = cast(DatasetFreshnessSourceType, "UNKNOWN_SOURCE")
        exception = InvalidSourceTypeException(
            message="Invalid source type", source_type=source_type
        )

        result = extract_assertion_evaluation_result_error(exception)

        assert isinstance(result, AssertionEvaluationResultError)
        assert result.type == AssertionResultErrorType.INVALID_SOURCE_TYPE
        assert result.properties is not None
        assert result.properties["message"] == "Invalid source type"
        assert result.properties["source_type"] == source_type

    def test_extract_source_connection_error_exception(self) -> None:
        """Test extraction of SourceConnectionErrorException."""
        connection_urn = "urn:li:connection:123"
        exception = SourceConnectionErrorException(
            message="Failed to connect", connection_urn=connection_urn
        )

        result = extract_assertion_evaluation_result_error(exception)

        assert isinstance(result, AssertionEvaluationResultError)
        assert result.type == AssertionResultErrorType.SOURCE_CONNECTION_ERROR
        assert result.properties is not None
        assert result.properties["message"] == "Failed to connect"
        assert result.properties["connection_urn"] == connection_urn

    def test_extract_source_query_failed_exception_with_filter(self) -> None:
        """Test extraction of SourceQueryFailedException with filter."""
        query = "SELECT * FROM table"
        filter_value = "WHERE id > 100"
        exception = SourceQueryFailedException(
            message="Query execution failed", query=query, filter=filter_value
        )

        result = extract_assertion_evaluation_result_error(exception)

        assert isinstance(result, AssertionEvaluationResultError)
        assert result.type == AssertionResultErrorType.SOURCE_QUERY_FAILED
        assert result.properties is not None
        assert result.properties["message"] == "Query execution failed"
        assert result.properties["query"] == query
        assert result.properties["filter"] == filter_value

    def test_extract_source_query_failed_exception_without_filter(self) -> None:
        """Test extraction of SourceQueryFailedException without filter."""
        query = "SELECT * FROM table"
        exception = SourceQueryFailedException(
            message="Query execution failed", query=query, filter=None
        )

        result = extract_assertion_evaluation_result_error(exception)

        assert isinstance(result, AssertionEvaluationResultError)
        assert result.type == AssertionResultErrorType.SOURCE_QUERY_FAILED
        assert result.properties is not None
        assert result.properties["message"] == "Query execution failed"
        assert result.properties["query"] == query
        assert result.properties["filter"] == ""

    def test_extract_unsupported_platform_exception(self) -> None:
        """Test extraction of UnsupportedPlatformException."""
        platform_urn = "urn:li:dataPlatform:unknown"
        exception = UnsupportedPlatformException(
            message="Platform not supported", platform_urn=platform_urn
        )

        result = extract_assertion_evaluation_result_error(exception)

        assert isinstance(result, AssertionEvaluationResultError)
        assert (
            result.type == AssertionResultErrorType.SOURCE_QUERY_FAILED
        )  # Note: This maps to SOURCE_QUERY_FAILED
        assert result.properties is not None
        assert result.properties["message"] == "Platform not supported"
        assert result.properties["platform_urn"] == platform_urn

    def test_extract_custom_sql_error_exception(self) -> None:
        """Test extraction of CustomSQLErrorException."""
        exception = CustomSQLErrorException(message="SQL syntax error")

        result = extract_assertion_evaluation_result_error(exception)

        assert isinstance(result, AssertionEvaluationResultError)
        assert result.type == AssertionResultErrorType.CUSTOM_SQL_ERROR
        assert result.properties is not None
        assert result.properties["message"] == "SQL syntax error"

    def test_extract_field_assertion_error_exception_with_query(self) -> None:
        """Test extraction of FieldAssertionErrorException with query."""
        query = "SELECT AVG(value) FROM table"
        exception = FieldAssertionErrorException(
            message="Field assertion failed", query=query
        )

        result = extract_assertion_evaluation_result_error(exception)

        assert isinstance(result, AssertionEvaluationResultError)
        assert result.type == AssertionResultErrorType.FIELD_ASSERTION_ERROR
        assert result.properties is not None
        assert result.properties["message"] == "Field assertion failed"
        assert result.properties["query"] == query

    def test_extract_field_assertion_error_exception_without_query(self) -> None:
        """Test extraction of FieldAssertionErrorException without query."""
        exception = FieldAssertionErrorException(
            message="Field assertion failed", query=None
        )

        result = extract_assertion_evaluation_result_error(exception)

        assert isinstance(result, AssertionEvaluationResultError)
        assert result.type == AssertionResultErrorType.FIELD_ASSERTION_ERROR
        assert result.properties is not None
        assert result.properties["message"] == "Field assertion failed"
        assert result.properties["query"] == ""

    def test_extract_unknown_assertion_result_exception(self) -> None:
        """Test extraction of an unknown AssertionResultException type."""

        # Create a custom subclass that doesn't match any of the expected types
        class UnknownAssertionException(AssertionResultException):
            pass

        exception = UnknownAssertionException(message="Unknown error type")

        with pytest.raises(Exception) as excinfo:
            extract_assertion_evaluation_result_error(exception)

        assert "Unknown error type" in str(excinfo.value)
