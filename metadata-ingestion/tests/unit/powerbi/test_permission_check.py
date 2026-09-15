"""Unit tests for the pre-ingestion Power BI API permission check.

Exercises ``PowerBiAPI._check_permissions`` (powerbi_api.py), which runs a
cheap, read-only call against the public and Admin APIs right after the
resolvers are constructed, so a missing tenant permission is surfaced once,
clearly, before the main ingestion loop starts.
"""

from typing import Any, Dict, Optional
from unittest.mock import MagicMock

import requests

from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api import PowerBiAPI


def _config(**overrides: Any) -> PowerBiDashboardSourceConfig:
    base: Dict[str, Any] = {
        "client_id": "foo",
        "client_secret": "bar",
        "tenant_id": "0B0C960B-FCDF-4D0F-8C45-2E03BB59DDEB",
    }
    base.update(overrides)
    return PowerBiDashboardSourceConfig.parse_obj(base)


def _http_error(status_code: int) -> requests.HTTPError:
    response = MagicMock()
    response.status_code = status_code
    return requests.HTTPError(response=response)


def _api(
    *,
    config_overrides: Optional[Dict[str, Any]] = None,
    regular_error: Optional[Exception] = None,
    admin_error: Optional[Exception] = None,
) -> PowerBiAPI:
    api = PowerBiAPI.__new__(PowerBiAPI)
    # Name-mangled private attributes, set directly to avoid a real MSAL/HTTP
    # handshake in __init__.
    api._PowerBiAPI__config = _config(**(config_overrides or {}))  # type: ignore[attr-defined]
    api.reporter = PowerBiDashboardSourceReport()

    regular_resolver = MagicMock()
    regular_resolver.get_groups_endpoint.return_value = (
        "https://api.powerbi.com/v1.0/myorg/groups"
    )
    if regular_error is not None:
        regular_resolver.ping.side_effect = regular_error
    api._PowerBiAPI__regular_api_resolver = regular_resolver  # type: ignore[attr-defined]

    admin_resolver = MagicMock()
    admin_resolver.get_apps_endpoint.return_value = (
        "https://api.powerbi.com/v1.0/myorg/admin/apps"
    )
    if admin_error is not None:
        admin_resolver.ping.side_effect = admin_error
    api._PowerBiAPI__admin_api_resolver = admin_resolver  # type: ignore[attr-defined]

    return api


def test_check_permissions_all_ok_emits_no_warnings() -> None:
    api = _api()
    api._check_permissions()
    assert api.reporter.warnings == []
    api._PowerBiAPI__regular_api_resolver.ping.assert_called_once()  # type: ignore[attr-defined]
    # extract_lineage defaults to True, so the admin check should also run.
    api._PowerBiAPI__admin_api_resolver.ping.assert_called_once()  # type: ignore[attr-defined]


def test_check_permissions_regular_403_warns() -> None:
    api = _api(regular_error=_http_error(403))
    api._check_permissions()
    assert any(
        w.title == "Missing PowerBI Public API Permission"
        for w in api.reporter.warnings
    )


def test_check_permissions_admin_403_warns() -> None:
    api = _api(admin_error=_http_error(403))
    api._check_permissions()
    assert any(
        w.title == "Missing PowerBI Admin API Permission" for w in api.reporter.warnings
    )


def test_check_permissions_401_is_also_treated_as_permission_error() -> None:
    api = _api(admin_error=_http_error(401))
    api._check_permissions()
    assert any(
        w.title == "Missing PowerBI Admin API Permission" for w in api.reporter.warnings
    )


def test_check_permissions_non_permission_error_is_silent() -> None:
    # A transient 500 (or a network error) is not a permission gap - it
    # shouldn't be reported as one.
    api = _api(admin_error=_http_error(500))
    api._check_permissions()
    assert api.reporter.warnings == []


def test_check_permissions_admin_apis_only_skips_regular_check() -> None:
    api = _api(config_overrides={"admin_apis_only": True})
    api._check_permissions()
    api._PowerBiAPI__regular_api_resolver.ping.assert_not_called()  # type: ignore[attr-defined]
    api._PowerBiAPI__admin_api_resolver.ping.assert_called_once()  # type: ignore[attr-defined]


def test_check_permissions_no_admin_features_skips_admin_check() -> None:
    api = _api(
        config_overrides={
            "extract_lineage": False,
            "extract_column_level_lineage": False,
            "extract_ownership": False,
            "extract_endorsements_to_tags": False,
        }
    )
    api._check_permissions()
    api._PowerBiAPI__admin_api_resolver.ping.assert_not_called()  # type: ignore[attr-defined]
    api._PowerBiAPI__regular_api_resolver.ping.assert_called_once()  # type: ignore[attr-defined]


def test_check_permissions_extract_ownership_alone_requires_admin_check() -> None:
    api = _api(
        config_overrides={
            "extract_lineage": False,
            "extract_column_level_lineage": False,
            "extract_ownership": True,
            "extract_endorsements_to_tags": False,
        },
        admin_error=_http_error(403),
    )
    api._check_permissions()
    assert any(
        w.title == "Missing PowerBI Admin API Permission" for w in api.reporter.warnings
    )
