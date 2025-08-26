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
)


def extract_assertion_evaluation_result_error(
    error: AssertionResultException,
) -> AssertionEvaluationResultError:
    """
    Converts an AssertionResultException into a dictionary of properties that can be serialized into JSON.
    """

    if isinstance(error, InsufficientDataException):
        return AssertionEvaluationResultError(
            type=AssertionResultErrorType.INSUFFICIENT_DATA,
            properties={"message": error.message},
        )
    elif isinstance(error, InvalidParametersException):
        return AssertionEvaluationResultError(
            type=AssertionResultErrorType.INVALID_PARAMETERS,
            properties={"message": error.message, "parameters": error.parameters},
        )
    elif isinstance(error, InvalidSourceTypeException):
        return AssertionEvaluationResultError(
            type=AssertionResultErrorType.INVALID_SOURCE_TYPE,
            properties={"message": error.message, "source_type": error.source_type},
        )
    elif isinstance(error, SourceConnectionErrorException):
        return AssertionEvaluationResultError(
            type=AssertionResultErrorType.SOURCE_CONNECTION_ERROR,
            properties={
                "message": error.message,
                "connection_urn": error.connection_urn,
            },
        )
    elif isinstance(error, SourceQueryFailedException):
        return AssertionEvaluationResultError(
            type=AssertionResultErrorType.SOURCE_QUERY_FAILED,
            properties={
                "message": error.message,
                "query": error.query,
                "filter": error.filter if error.filter else "",
            },
        )
    elif isinstance(error, UnsupportedPlatformException):
        return AssertionEvaluationResultError(
            type=AssertionResultErrorType.SOURCE_QUERY_FAILED,
            properties={"message": error.message, "platform_urn": error.platform_urn},
        )
    elif isinstance(error, CustomSQLErrorException):
        return AssertionEvaluationResultError(
            type=AssertionResultErrorType.CUSTOM_SQL_ERROR,
            properties={"message": error.message},
        )
    elif isinstance(error, FieldAssertionErrorException):
        return AssertionEvaluationResultError(
            type=AssertionResultErrorType.FIELD_ASSERTION_ERROR,
            properties={
                "message": error.message,
                "query": error.query if error.query else "",
            },
        )
    raise Exception(f"Unknown error type {error}")
