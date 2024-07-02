from enum import Enum


class ScanUnauthorizedException(Exception):
    pass


class LineageUnauthorizedException(Exception):
    pass


class UsageUnauthorizedException(Exception):
    pass


class ProfilingUnauthorizedException(Exception):
    pass


class LineageQueryParsingFailedException(Exception):
    pass


class UsageQueryParsingFailedException(Exception):
    pass


class ConnectionFailedCoordinatesException(Exception):
    pass


class ConnectionFailedCredentialsException(Exception):
    pass


class ConnectionFailedServiceUnavailableException(Exception):
    pass


class ConnectionFailedServiceTimeoutException(Exception):
    pass


class ConnectionFailedUnknownException(Exception):
    pass


class StructuredReportLogType(Enum):
    SCAN_UNAUTHORIZED = "SCAN_UNAUTHORIZED"
    LINEAGE_UNAUTHORIZED = "LINEAGE_UNAUTHORIZED"
    USAGE_UNAUTHORIZED = "USAGE_UNAUTHORIZED"
    PROFILING_UNAUTHORIZED = "PROFILING_UNAUTHORIZED"
    LINEAGE_QUERY_PARSING_FAILED = "LINEAGE_QUERY_PARSING_FAILED"
    USAGE_QUERY_PARSING_FAILED = "USAGE_QUERY_PARSING_FAILED"
    CONNECTION_FAILED_COORDINATES = "CONNECTION_FAILED_COORDINATES"
    CONNECTION_FAILED_CREDENTIALS = "CONNECTION_FAILED_CREDENTIALS"
    CONNECTION_FAILED_SERVICE_UNAVAILABLE = "CONNECTION_FAILED_SERVICE_UNAVAILABLE"
    CONNECTION_FAILED_SERVICE_TIMEOUT = "CONNECTION_FAILED_SERVICE_TIMEOUT"
    CONNECTION_FAILED_UNKNOWN = "CONNECTION_FAILED_UNKNOWN"
    UNKNOWN = "UNKNOWN"


EXCEPTION_TO_REPORT_TYPE = {
    ScanUnauthorizedException: StructuredReportLogType.SCAN_UNAUTHORIZED,
    LineageUnauthorizedException: StructuredReportLogType.LINEAGE_UNAUTHORIZED,
    UsageUnauthorizedException: StructuredReportLogType.USAGE_UNAUTHORIZED,
    ProfilingUnauthorizedException: StructuredReportLogType.PROFILING_UNAUTHORIZED,
    LineageQueryParsingFailedException: StructuredReportLogType.LINEAGE_QUERY_PARSING_FAILED,
    UsageQueryParsingFailedException: StructuredReportLogType.USAGE_QUERY_PARSING_FAILED,
    ConnectionFailedCoordinatesException: StructuredReportLogType.CONNECTION_FAILED_COORDINATES,
    ConnectionFailedCredentialsException: StructuredReportLogType.CONNECTION_FAILED_CREDENTIALS,
    ConnectionFailedServiceUnavailableException: StructuredReportLogType.CONNECTION_FAILED_SERVICE_UNAVAILABLE,
    ConnectionFailedServiceTimeoutException: StructuredReportLogType.CONNECTION_FAILED_SERVICE_TIMEOUT,
    ConnectionFailedUnknownException: StructuredReportLogType.CONNECTION_FAILED_UNKNOWN,
}
