from typing import Optional

from google.api_core.exceptions import Forbidden, GoogleAPICallError


def is_service_disabled(exc: Forbidden) -> bool:
    """True if the error means the target GCP API is not enabled for the project.

    Keys on the ErrorInfo reason, not the message text (which GCP rewords).
    """
    return exc.reason == "SERVICE_DISABLED"


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
