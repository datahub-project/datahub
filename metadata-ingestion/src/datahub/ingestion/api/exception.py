from enum import Enum


class StructuredReportLogType(Enum):
    """
    A standardized set of human-readable log message types that will be displayed to the user
    along with the log message.

    It is recommended to reuse this across sources for bubbling up standardized errors to the user.
    """

    SCAN_UNAUTHORIZED = "Scan Unauthorized"
    LINEAGE_UNAUTHORIZED = "Lineage Unauthorized"
    USAGE_UNAUTHORIZED = "Usage Unauthorized"
    PROFILING_UNAUTHORIZED = "Profiling Unauthorized"
    LINEAGE_QUERY_PARSING_FAILED = "Lineage Query Parsing Failure"
    USAGE_QUERY_PARSING_FAILED = "Usage Query Parsing Failure"
    CONNECTION_FAILED_COORDINATES = "Failed to Connect"
    CONNECTION_FAILED_CREDENTIALS = "Invalid Credentials"
    CONNECTION_FAILED_SERVICE_UNAVAILABLE = "Service Unavailable"
    CONNECTION_FAILED_SERVICE_TIMEOUT = "Service Timeout"
    CONNECTION_FAILED_UNKNOWN = "Unknown Connection Failure"
    UNKNOWN = "An unknown error occurred"
