from typing import Any, Dict, Optional
from unittest import mock

from botocore.exceptions import ClientError

from datahub.ingestion.source.quicksight.processors.enrichment import AssetEnricher
from datahub.ingestion.source.quicksight.quicksight_config import (
    QuickSightSourceConfig,
)
from datahub.ingestion.source.quicksight.quicksight_report import (
    QuickSightSourceReport,
)

_ACCOUNT = "064369473231"
_USER_OWNER = f"arn:aws:quicksight:us-east-1:{_ACCOUNT}:user/default/Alice"
_USER_VIEWER = f"arn:aws:quicksight:us-east-1:{_ACCOUNT}:user/default/Bob"
_GROUP_OWNER = f"arn:aws:quicksight:us-east-1:{_ACCOUNT}:group/default/analysts"
# IAM/SSO-federated principal: name is "<role>/<session>".
_SSO_OWNER = (
    f"arn:aws:quicksight:us-east-1:{_ACCOUNT}:user/default/"
    "AWSReservedSSO_Admin_abc123/jane@acme.com"
)

_OWNER_ACTIONS = [
    "quicksight:DescribeDashboard",
    "quicksight:UpdateDashboardPermissions",
]
_VIEWER_ACTIONS = ["quicksight:DescribeDashboard", "quicksight:QueryDashboard"]


def _enricher(
    api: mock.MagicMock, config_dict: Optional[Dict[str, Any]] = None
) -> AssetEnricher:
    config = QuickSightSourceConfig.model_validate(
        {"aws_region": "us-east-1", **(config_dict or {})}
    )
    return AssetEnricher(config, QuickSightSourceReport(), api)


def test_only_owner_principals_become_owners():
    api = mock.MagicMock()
    api.describe_dashboard_permissions.return_value = [
        {"Principal": _USER_OWNER, "Actions": _OWNER_ACTIONS},
        {"Principal": _USER_VIEWER, "Actions": _VIEWER_ACTIONS},
        {"Principal": _GROUP_OWNER, "Actions": _OWNER_ACTIONS},
    ]
    owners = _enricher(api).owners("dashboard", "dash-1")

    assert owners is not None
    owner_urns = {o.owner for o in owners}
    # Only principals with an Update*Permissions action are owners; the viewer
    # (describe/query only) is excluded.
    assert owner_urns == {
        "urn:li:corpuser:Alice",
        "urn:li:corpGroup:analysts",
    }


def test_sso_federated_principal_uses_session_name():
    # The CorpUser id must be the trailing session segment (the real identity,
    # typically the email) — NOT the "role/session" composite — matching the
    # plain email/username convention of the other BI connectors.
    api = mock.MagicMock()
    api.describe_dashboard_permissions.return_value = [
        {"Principal": _SSO_OWNER, "Actions": _OWNER_ACTIONS},
    ]
    owners = _enricher(api).owners("dashboard", "dash-1")

    assert owners is not None
    assert [o.owner for o in owners] == ["urn:li:corpuser:jane@acme.com"]


def test_strip_user_email_domain():
    api = mock.MagicMock()
    api.describe_dashboard_permissions.return_value = [
        {"Principal": _SSO_OWNER, "Actions": _OWNER_ACTIONS},
    ]
    owners = _enricher(api, {"strip_user_email_domain": True}).owners(
        "dashboard", "dash-1"
    )

    assert owners is not None
    assert [o.owner for o in owners] == ["urn:li:corpuser:jane"]


def test_ownership_disabled_returns_none():
    api = mock.MagicMock()
    enricher = _enricher(api, {"extract_ownership": False})
    assert enricher.owners("dashboard", "dash-1") is None
    api.describe_dashboard_permissions.assert_not_called()


def test_permission_error_degrades_to_none():
    api = mock.MagicMock()
    api.describe_data_set_permissions.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "denied"}},
        "DescribeDataSetPermissions",
    )
    assert _enricher(api).owners("data_set", "ds-1") is None


def test_tags_mapped_and_filtered():
    api = mock.MagicMock()
    api.list_tags_for_resource.return_value = [
        {"Key": "team", "Value": "analytics"},
        {"Key": "env", "Value": "prod"},
    ]
    tags = _enricher(api, {"tag_pattern": {"allow": ["^team=.*"]}}).tags("arn:x")

    assert tags is not None
    assert [t.tag for t in tags] == ["urn:li:tag:team=analytics"]


def test_tags_disabled_or_empty_arn_returns_none():
    api = mock.MagicMock()
    assert _enricher(api, {"extract_tags": False}).tags("arn:x") is None
    assert _enricher(api).tags("") is None
