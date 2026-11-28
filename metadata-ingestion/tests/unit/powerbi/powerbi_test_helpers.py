"""Scaffolding shared by the Power BI unit tests that drive PowerBiAPI
without a network: a PowerBiAPI built around mocked resolvers, a config with
the mandatory credentials, a bare workspace and an MSAL stand-in."""

from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import Workspace
from datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api import PowerBiAPI

WORKSPACE_ID = "64ed5cad-7c10-4684-8180-826122881108"
WORKSPACE_NAME = "demo-workspace"


class FakeMsal:
    def __init__(self, token: Optional[Dict[str, Any]] = None) -> None:
        self.token = token or {"access_token": "fabric-token", "expires_in": 3600}
        self.scopes: List[List[str]] = []

    def acquire_token_for_client(self, scopes: List[str]) -> Dict[str, Any]:
        self.scopes.append(scopes)
        return self.token


def make_config(**overrides: Any) -> PowerBiDashboardSourceConfig:
    base: Dict[str, Any] = {
        "tenant_id": "fake",
        "client_id": "foo",
        "client_secret": "bar",
    }
    base.update(overrides)
    return PowerBiDashboardSourceConfig.model_validate(base)


def make_workspace(
    workspace_id: str = WORKSPACE_ID,
    name: str = WORKSPACE_NAME,
    workspace_type: str = "Workspace",
    web_url: Optional[str] = None,
) -> Workspace:
    return Workspace(
        id=workspace_id,
        name=name,
        type=workspace_type,
        webUrl=web_url,
        datasets={},
        dashboards={},
        reports={},
        report_endorsements={},
        dashboard_endorsements={},
        scan_result={},
        independent_datasets={},
        app=None,
    )


def make_api(
    config: PowerBiDashboardSourceConfig,
    *,
    regular_resolver: Optional[Any] = None,
    fabric_resolver: Optional[Any] = None,
    dataset_registry: Optional[Dict[str, Any]] = None,
) -> PowerBiAPI:
    """A PowerBiAPI that skips the constructor (no token fetch). Resolvers
    default to MagicMocks; pass a real FabricDefinitionResolver to exercise
    the HTTP layer through requests_mock."""
    api = PowerBiAPI.__new__(PowerBiAPI)
    api._PowerBiAPI__config = config  # type: ignore[attr-defined]
    api.reporter = PowerBiDashboardSourceReport()
    api.dataset_registry = dataset_registry if dataset_registry is not None else {}
    api.datasets_by_workspace_and_name = {}
    api.ambiguous_remote_model_keys = set()
    api.dataflow_registry = {}
    api.dataflows_listed = set()
    # log_http_error inspects sys.exc_info(); it is not under test here.
    api.log_http_error = MagicMock()  # type: ignore[method-assign]
    api._PowerBiAPI__workspace_users = {}  # type: ignore[attr-defined]
    api._PowerBiAPI__regular_api_resolver = (  # type: ignore[attr-defined]
        regular_resolver if regular_resolver is not None else MagicMock()
    )
    api._PowerBiAPI__fabric_resolver = (  # type: ignore[attr-defined]
        fabric_resolver if fabric_resolver is not None else MagicMock()
    )
    return api
