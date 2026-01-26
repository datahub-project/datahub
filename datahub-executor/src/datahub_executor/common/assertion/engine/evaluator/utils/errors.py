from datahub_executor.common.exceptions import (
    AssertionResultException,
    CustomSQLErrorException,
    EvaluatorNotFoundException,
    FieldAssertionErrorException,
    InsufficientDataException,
    InvalidParametersException,
    InvalidSourceTypeException,
    MetricPersistenceException,
    MissingEvaluationParametersException,
    ResultEmissionException,
    SourceConnectionErrorException,
    SourceQueryFailedException,
    StatePersistenceException,
    UnsupportedPlatformException,
)
from datahub_executor.common.metric.types import (
    InvalidMetricResolverSourceTypeException,
    UnsupportedMetricException,
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
            type=AssertionResultErrorType.UNSUPPORTED_PLATFORM,
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
    elif isinstance(error, MissingEvaluationParametersException):
        return AssertionEvaluationResultError(
            type=AssertionResultErrorType.MISSING_EVALUATION_PARAMETERS,
            properties={"message": error.message},
        )
    elif isinstance(error, EvaluatorNotFoundException):
        return AssertionEvaluationResultError(
            type=AssertionResultErrorType.EVALUATOR_NOT_FOUND,
            properties={"message": error.message},
        )
    elif isinstance(error, UnsupportedMetricException):
        return AssertionEvaluationResultError(
            type=AssertionResultErrorType.METRIC_RESOLVER_UNSUPPORTED_METRIC,
            properties={
                "message": error.message,
                "metric_name": error.metric_name,
            },
        )
    elif isinstance(error, InvalidMetricResolverSourceTypeException):
        return AssertionEvaluationResultError(
            type=AssertionResultErrorType.METRIC_RESOLVER_INVALID_SOURCE_TYPE,
            properties={
                "message": error.message,
                "source_type": error.source_type,
            },
        )
    elif isinstance(error, StatePersistenceException):
        return AssertionEvaluationResultError(
            type=AssertionResultErrorType.STATE_PERSISTENCE_FAILED,
            properties={"message": error.message},
        )
    elif isinstance(error, MetricPersistenceException):
        return AssertionEvaluationResultError(
            type=AssertionResultErrorType.METRIC_PERSISTENCE_FAILED,
            properties={"message": error.message},
        )
    elif isinstance(error, ResultEmissionException):
        return AssertionEvaluationResultError(
            type=AssertionResultErrorType.RESULT_EMISSION_FAILED,
            properties={"message": error.message},
        )
    raise Exception(f"Unknown error type {error}")
