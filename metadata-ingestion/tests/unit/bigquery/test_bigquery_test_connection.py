from unittest.mock import MagicMock, patch

from google.api_core.exceptions import PermissionDenied
from google.rpc.error_details_pb2 import ErrorInfo

from datahub.ingestion.source.bigquery_v2.bigquery_test_connection import (
    BigQueryTestConnection,
)

_AH_CLIENT = (
    "datahub.ingestion.source.bigquery_v2.bigquery_test_connection"
    ".bigquery_analyticshub_v1.AnalyticsHubServiceClient"
)


def test_linked_datasets_capability_client_init_failure_is_not_fatal():
    # A client-construction failure must map to this capability, never escape to
    # the caller where it would overwrite basic_connectivity.
    with patch(_AH_CLIENT, side_effect=ValueError("no creds")):
        report = BigQueryTestConnection.linked_datasets_capability_test(["proj-a"])
    assert report.capable is False
    assert "Analytics Hub client" in (report.failure_reason or "")


def test_linked_datasets_capability_api_disabled_reports_enable_api():
    client = MagicMock()
    client.list_subscriptions.side_effect = PermissionDenied(
        "API disabled",
        error_info=ErrorInfo(reason="SERVICE_DISABLED", domain="googleapis.com"),
    )
    with patch(_AH_CLIENT, return_value=client):
        report = BigQueryTestConnection.linked_datasets_capability_test(["proj-a"])
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
    with patch(_AH_CLIENT, return_value=client):
        report = BigQueryTestConnection.linked_datasets_capability_test(["proj-a"])
    assert report.capable is False
    assert "analyticshub.subscriptions.list" in (report.failure_reason or "")


def test_linked_datasets_capability_success():
    client = MagicMock()
    client.list_subscriptions.return_value = iter([])
    with patch(_AH_CLIENT, return_value=client):
        report = BigQueryTestConnection.linked_datasets_capability_test(["proj-a"])
    assert report.capable is True
