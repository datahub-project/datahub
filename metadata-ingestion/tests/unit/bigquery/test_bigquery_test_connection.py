from typing import Optional
from unittest.mock import MagicMock, patch

from google.api_core.exceptions import PermissionDenied
from google.rpc.error_details_pb2 import ErrorInfo

from datahub.ingestion.api.source import CapabilityReport
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_test_connection import (
    BigQueryTestConnection,
)

# The Analytics Hub client is constructed in bigquery_linked_datasets via the
# shared create_analyticshub_client helper, so patch it there.
_AH_CLIENT = (
    "datahub.ingestion.source.bigquery_v2.bigquery_linked_datasets"
    ".bigquery_analyticshub_v1.AnalyticsHubServiceClient"
)


def _conn_conf() -> BigQueryV2Config:
    return BigQueryV2Config.model_validate(
        {"project_ids": ["proj-a"], "include_linked_datasets": True}
    )


def _capability_report(
    client: Optional[MagicMock] = None,
    *,
    side_effect: Optional[BaseException] = None,
) -> CapabilityReport:
    """Capability report with the AH client patched to `client`, or raising `side_effect`."""
    with patch(_AH_CLIENT, return_value=client, side_effect=side_effect):
        return BigQueryTestConnection.linked_datasets_capability_test(
            _conn_conf(), ["proj-a"]
        )


def test_linked_datasets_capability_client_init_failure_is_not_fatal():
    # A client-construction failure must map to this capability, never escape to
    # the caller where it would overwrite basic_connectivity.
    report = _capability_report(side_effect=ValueError("no creds"))
    assert report.capable is False
    assert "Analytics Hub client" in (report.failure_reason or "")


def test_linked_datasets_capability_api_disabled_reports_enable_api():
    client = MagicMock()
    client.list_subscriptions.side_effect = PermissionDenied(
        "API disabled",
        error_info=ErrorInfo(reason="SERVICE_DISABLED", domain="googleapis.com"),
    )
    report = _capability_report(client)
    assert report.capable is False
    assert "not enabled" in (report.failure_reason or "")


def test_linked_datasets_capability_iam_denied_reports_grant():
    client = MagicMock()
    client.list_subscriptions.side_effect = PermissionDenied(
        "permission denied",
        error_info=ErrorInfo(
            reason="IAM_PERMISSION_DENIED", domain="analyticshub.googleapis.com"
        ),
    )
    report = _capability_report(client)
    assert report.capable is False
    assert "analyticshub.subscriptions.list" in (report.failure_reason or "")


def test_linked_datasets_capability_success():
    client = MagicMock()
    client.list_subscriptions.return_value = iter([])
    report = _capability_report(client)
    assert report.capable is True
