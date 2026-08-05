from google.api_core.exceptions import Forbidden


def is_service_disabled(exc: Forbidden) -> bool:
    """True if the error means the target GCP API is not enabled for the project.

    Keys on the ErrorInfo reason, not the message text (which GCP rewords).
    """
    return exc.reason == "SERVICE_DISABLED"


def is_iam_permission_denied(exc: Forbidden) -> bool:
    """True if the error means the caller is missing a required IAM permission.

    Keys on the ErrorInfo reason, not the message text (which GCP rewords).
    """
    return exc.reason == "IAM_PERMISSION_DENIED"
