from typing import Optional

from google.api_core.exceptions import Forbidden, GoogleAPICallError

_ERROR_INFO = "type.googleapis.com/google.rpc.ErrorInfo"


def error_info_reason(exc: GoogleAPICallError) -> Optional[str]:
    """The ErrorInfo `reason` a GCP error carries, or None.

    `GoogleAPICallError.reason` reads `self._error_info.reason`, which only
    works for a gRPC client: google-api-core stores a protobuf there for gRPC
    and the raw JSON detail -- a plain dict -- for REST. So `.reason` raises
    AttributeError on every REST 403 that carries an ErrorInfo detail, which
    is every modern one, and google-cloud-bigquery is REST only.
    """
    try:
        return exc.reason
    except AttributeError:
        return next(
            (
                detail.get("reason")
                for detail in exc.details or ()
                if isinstance(detail, dict) and detail.get("@type") == _ERROR_INFO
            ),
            None,
        )


def error_info_service(exc: GoogleAPICallError) -> Optional[str]:
    """The API the ErrorInfo names (`bigquery.googleapis.com`), or None, so a
    message can name the service that actually refused rather than guess."""
    try:
        metadata = exc._error_info.metadata if exc._error_info else None
    except AttributeError:
        metadata = next(
            (
                detail.get("metadata")
                for detail in exc.details or ()
                if isinstance(detail, dict) and detail.get("@type") == _ERROR_INFO
            ),
            None,
        )
    return (metadata or {}).get("service")


def is_service_disabled(exc: Forbidden) -> bool:
    """True if the error means the target GCP API is not enabled for the project.

    Keys on the ErrorInfo reason, not the message text (which GCP rewords).
    """
    return error_info_reason(exc) == "SERVICE_DISABLED"


def gcp_error_code(exc: BaseException) -> Optional[str]:
    """The HTTP status a google-api-core error carries as `.code` ("HTTP 403"),
    for a GCP probe provider's probe_error_code. Read from those errors only:
    elsewhere `.code` is anything."""
    # Late import: ingestion sources import this module, and the probe
    # framework is loaded only when a probe runs.
    from datahub.ingestion.agent.error_policy import http_status_code

    if not isinstance(exc, GoogleAPICallError):
        return None
    return http_status_code(exc.code)
