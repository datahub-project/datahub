from google.api_core.exceptions import PermissionDenied
from google.rpc.error_details_pb2 import ErrorInfo

from datahub.ingestion.source.common.gcp_errors import is_service_disabled


def test_is_service_disabled_true_for_service_disabled_reason() -> None:
    # When reason is SERVICE_DISABLED, this must evaluate to True.
    exc = PermissionDenied(
        "Agent Platform API has not been used in project ... before or it is disabled.",
        error_info=ErrorInfo(reason="SERVICE_DISABLED", domain="googleapis.com"),
    )
    assert is_service_disabled(exc)


def test_is_service_disabled_false_for_other_reason() -> None:
    # A IAM denial has a different reason, this must evaluate to false.
    exc = PermissionDenied(
        "Permission denied on resource.",
        error_info=ErrorInfo(reason="IAM_PERMISSION_DENIED", domain="googleapis.com"),
    )
    assert not is_service_disabled(exc)


def test_is_service_disabled_false_for_missing_reason() -> None:
    # Without ErrorInfo, PermissionDenied has no .reason, and this must evaluate to False.
    assert not is_service_disabled(PermissionDenied("denied"))
