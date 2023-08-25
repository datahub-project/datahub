from datahub_monitors.exceptions import (
    AssertionResultException,
    InvalidParametersException,
    InvalidSourceTypeException,
    SourceConnectionErrorException,
    SourceQueryFailedException,
    UnsupportedPlatformException,
)
from datahub_monitors.types import (
    AssertionEvaluationResultError,
    AssertionResultErrorType,
)


def extract_assertion_evaluation_result_error(
    error: AssertionResultException,
) -> AssertionEvaluationResultError:
    """
    Converts an AssertionResultException into a dictionary of properties that can be serialized into JSON.
    """

    if isinstance(error, InvalidParametersException):
        return AssertionEvaluationResultError(
            type=AssertionResultErrorType.INVALID_PARAMETERS,
            properties={"parameters": error.parameters},
        )
    elif isinstance(error, InvalidSourceTypeException):
        return AssertionEvaluationResultError(
            type=AssertionResultErrorType.INVALID_SOURCE_TYPE,
            properties={"source_type": error.source_type},
        )
    elif isinstance(error, SourceConnectionErrorException):
        return AssertionEvaluationResultError(
            type=AssertionResultErrorType.SOURCE_CONNECTION_ERROR,
            properties={"connection_urn": error.connection_urn},
        )
    elif isinstance(error, SourceQueryFailedException):
        return AssertionEvaluationResultError(
            type=AssertionResultErrorType.SOURCE_QUERY_FAILED,
            properties={
                "query": error.query,
                "filter": error.filter if error.filter else "",
            },
        )
    elif isinstance(error, UnsupportedPlatformException):
        return AssertionEvaluationResultError(
            type=AssertionResultErrorType.SOURCE_QUERY_FAILED,
            properties={"platform_urn": error.platform_urn},
        )
    raise Exception(f"Unknown error type {error}")
