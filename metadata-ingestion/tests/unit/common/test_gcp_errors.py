from google.api_core.exceptions import PermissionDenied
from google.rpc.error_details_pb2 import ErrorInfo

from datahub.ingestion.source.common.gcp_errors import (
    error_info_permission,
    is_service_disabled,
)


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


def test_error_info_permission_reads_grpc_and_rest_errors() -> None:
    grpc = PermissionDenied(
        "denied",
        error_info=ErrorInfo(
            reason="IAM_PERMISSION_DENIED",
            metadata={"permission": "aiplatform.models.list"},
        ),
    )
    # A REST client stores the raw JSON detail as the error info.
    rest_detail = {
        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
        "reason": "IAM_PERMISSION_DENIED",
        "metadata": {"permission": "bigquery.tables.list"},
    }
    rest = PermissionDenied("denied", details=[rest_detail], error_info=rest_detail)
    assert error_info_permission(grpc) == "aiplatform.models.list"
    assert error_info_permission(rest) == "bigquery.tables.list"
    assert error_info_permission(PermissionDenied("denied")) is None
