from typing import TYPE_CHECKING, Optional, Union

from datahub.metadata.schema_classes import MonitorErrorTypeClass

if TYPE_CHECKING:
    from datahub_executor.common.types import (
        DatasetFreshnessSourceType,
        EntityEventType,
    )


class AssertionResultException(Exception):
    """Base class for assertion result exceptions"""

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class InsufficientDataException(AssertionResultException):
    """Raised when there is insufficient data to evaluate an assertion"""

    def __init__(self, message: str):
        super().__init__(message)


class InvalidParametersException(AssertionResultException):
    """Raised when assertion parameters are invalid"""

    def __init__(self, message: str, parameters: dict):
        super().__init__(message)
        self.parameters = str(parameters)


class InvalidSourceTypeException(AssertionResultException):
    """Raised when a source type is invalid"""

    def __init__(
        self,
        message: str,
        source_type: Union["DatasetFreshnessSourceType", "EntityEventType"],
    ):
        super().__init__(message)
        self.source_type = str(source_type)


class SourceConnectionErrorException(AssertionResultException):
    """Raised when a source connection is unreachable"""

    def __init__(
        self,
        message: str,
        connection_urn: str,
        response_code: Optional[int] = None,
        sqlstate: Optional[str] = None,
    ):
        super().__init__(message)
        self.connection_urn = connection_urn
        self.response_code = response_code
        self.sqlstate = sqlstate


class SourceQueryFailedException(AssertionResultException):
    """Raised when a source query fails"""

    def __init__(
        self,
        message: str,
        query: str,
        filter: Optional[str] = None,
        response_code: Optional[int] = None,
        sqlstate: Optional[str] = None,
    ):
        super().__init__(message)
        self.query = query
        self.filter = filter
        self.response_code = response_code
        self.sqlstate = sqlstate


class UnsupportedPlatformException(AssertionResultException):
    """Raised when specified platform is unsupported"""

    def __init__(self, message: str, platform_urn: str):
        super().__init__(message)
        self.platform_urn = platform_urn


class CustomSQLErrorException(AssertionResultException):
    """Raised when a custom SQL assertion is run and results are unsupported"""

    def __init__(self, message: str):
        super().__init__(message)


class FieldAssertionErrorException(AssertionResultException):
    """Raised when a field metrics assertion is run and results are unsupported"""

    def __init__(self, message: str, query: Optional[str] = None):
        super().__init__(message)
        self.query = query


class MissingEvaluationParametersException(AssertionResultException):
    """Raised when evaluation parameters are missing."""

    def __init__(self, message: str):
        super().__init__(message)


class EvaluatorNotFoundException(AssertionResultException):
    """Raised when no evaluator exists for a given assertion type."""

    def __init__(self, message: str):
        super().__init__(message)


# NOTE: Metric resolver-specific exceptions now live in
# datahub_executor.common.metric.types to keep metric-layer failures centralized.


class StatePersistenceException(AssertionResultException):
    """Raised when assertion state persistence fails."""

    def __init__(self, message: str):
        super().__init__(message)


class MetricPersistenceException(AssertionResultException):
    """Raised when saving a collected metric fails."""

    def __init__(self, message: str):
        super().__init__(message)


class ResultEmissionException(AssertionResultException):
    """Raised when emitting assertion results fails."""

    def __init__(self, message: str):
        super().__init__(message)


class TrainingErrorException(Exception):
    """Raised when training/inference fails with actionable details."""

    def __init__(
        self,
        message: str,
        error_type: Union[
            MonitorErrorTypeClass, str
        ] = MonitorErrorTypeClass.MODEL_TRAINING_FAILED,
        properties: Optional[dict[str, str]] = None,
        state: Optional[str] = None,
    ):
        super().__init__(message)
        self.error_type = error_type
        self.properties = properties or {}
        self.state = state
