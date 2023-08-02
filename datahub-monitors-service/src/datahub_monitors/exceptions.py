from typing import Optional, Union

from datahub_monitors.types import DatasetFreshnessSourceType, EntityEventType


class AssertionResultException(Exception):
    """Base class for assertion result exceptions"""

    pass


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
        source_type: Union[DatasetFreshnessSourceType, EntityEventType],
    ):
        super().__init__(message)
        self.source_type = str(source_type)


class SourceConnectionErrorException(AssertionResultException):
    """Raised when a source connection is unreachable"""

    def __init__(self, message: str, connection_urn: str):
        super().__init__(message)
        self.connection_urn = connection_urn


class SourceQueryFailedException(AssertionResultException):
    """Raised when a source query fails"""

    def __init__(self, message: str, query: str, filter: Optional[str] = None):
        super().__init__(message)
        self.query = query
        self.filter = filter


class UnsupportedPlatformException(AssertionResultException):
    """Raised when specified platform is unsupported"""

    def __init__(self, message: str, platform_urn: str):
        super().__init__(message)
        self.platform_urn = platform_urn
