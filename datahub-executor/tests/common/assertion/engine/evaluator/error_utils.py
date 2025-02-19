import pytest

from datahub_executor.common.assertion.engine.evaluator.utils.errors import (
    extract_assertion_evaluation_result_error,
)
from datahub_executor.common.constants import SNOWFLAKE_PLATFORM_URN
from datahub_executor.common.exceptions import (
    AssertionResultException,
    InsufficientDataException,
    InvalidParametersException,
    InvalidSourceTypeException,
    SourceConnectionErrorException,
    SourceQueryFailedException,
    UnsupportedPlatformException,
)
from datahub_executor.common.types import (
    AssertionResultErrorType,
    DatasetFreshnessSourceType,
)


def test_extract_assertion_evaluation_result_error_with_insufficient_data_exception() -> (
    None
):
    error = InsufficientDataException(message="Error")
    result = extract_assertion_evaluation_result_error(error)
    assert result.type == AssertionResultErrorType.INSUFFICIENT_DATA
    assert result.properties == {}


def test_extract_assertion_evaluation_result_error_with_invalid_parameters_exception() -> (
    None
):
    error = InvalidParametersException(message="Error", parameters={"foo": "bar"})
    result = extract_assertion_evaluation_result_error(error)
    assert result.type == AssertionResultErrorType.INVALID_PARAMETERS
    assert result.properties == {"parameters": {"foo": "bar"}}


def test_extract_assertion_evaluation_result_error_with_invalid_source_type_exception() -> (
    None
):
    error = InvalidSourceTypeException(
        message="Error", source_type=DatasetFreshnessSourceType.AUDIT_LOG
    )
    result = extract_assertion_evaluation_result_error(error)
    assert result.type == AssertionResultErrorType.INVALID_SOURCE_TYPE
    assert result.properties == {"source_type": "AUDIT_LOG"}


def test_extra_assertion_evaluation_result_error_with_source_connection_error_exception() -> (
    None
):
    error = SourceConnectionErrorException(
        message="Error", connection_urn=SNOWFLAKE_PLATFORM_URN
    )
    result = extract_assertion_evaluation_result_error(error)
    assert result.type == AssertionResultErrorType.SOURCE_CONNECTION_ERROR
    assert result.properties == {"connection_urn": SNOWFLAKE_PLATFORM_URN}


def test_extra_assertion_evaluation_result_error_with_source_query_failed_exception() -> (
    None
):
    error = SourceQueryFailedException(
        message="Error",
        query="SELECT * FROM table_name WHERE foo = 'bar'",
        filter="foo = 'bar'",
    )
    result = extract_assertion_evaluation_result_error(error)
    assert result.type == AssertionResultErrorType.SOURCE_QUERY_FAILED
    assert result.properties == {
        "query": "SELECT * FROM table_name WHERE foo = 'bar'",
        "filter": "foo = 'bar'",
    }


def test_extract_assertion_evaluation_result_error_with_unsupported_platform_exception() -> (
    None
):
    error = UnsupportedPlatformException(
        message="Error", platform_urn=SNOWFLAKE_PLATFORM_URN
    )
    result = extract_assertion_evaluation_result_error(error)
    assert result.type == AssertionResultErrorType.SOURCE_QUERY_FAILED
    assert result.properties == {"platform_urn": SNOWFLAKE_PLATFORM_URN}


def test_extract_assertion_evaluation_result_error_with_unknown_exception() -> None:
    error = AssertionResultException("foo")
    with pytest.raises(Exception, match="Unknown error type foo"):
        extract_assertion_evaluation_result_error(error)
