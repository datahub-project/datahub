from datahub_monitors.assertion.engine.evaluator.utils.errors import (
    extract_assertion_evaluation_result_error,
)
from datahub_monitors.constants import SNOWFLAKE_PLATFORM_URN
from datahub_monitors.exceptions import (
    AssertionResultException,
    InvalidParametersException,
    InvalidSourceTypeException,
    SourceConnectionErrorException,
    SourceQueryFailedException,
    UnsupportedPlatformException,
)
from datahub_monitors.types import AssertionResultErrorType, DatasetFreshnessSourceType


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
    try:
        extract_assertion_evaluation_result_error(error)
        assert False
    except Exception as e:
        assert str(e) == "Unknown error type foo"
