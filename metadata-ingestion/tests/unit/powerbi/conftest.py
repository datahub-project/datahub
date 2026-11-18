"""Shared builders for the PowerBI unit tests.

Everything here is HTTP-free: the builders produce the connector's own data
classes (``Workspace``, ``PowerBIDataset``, ``Report`` ...), a config with the
mandatory credentials filled in, and a ``Mapper``/``PowerBiAPI``/resolver
wired to that config. ``msal_stub`` is the only fixture: it replaces MSAL so
constructing a resolver never reaches the network, and counts token
acquisitions so caching can be asserted.
"""

from typing import Any, Dict, Iterator, List, Optional, Tuple, Type
from unittest import mock

import pytest
import requests

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    create_dataplatform_instance_resolver,
)
from datahub.ingestion.source.powerbi.m_query import parser as mquery_parser
from datahub.ingestion.source.powerbi.m_query.data_classes import Lineage
from datahub.ingestion.source.powerbi.powerbi import Mapper
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    Column,
    Dashboard,
    Measure,
    Page,
    PowerBIDataset,
    Report,
    ReportType,
    Table,
    Tile,
    User,
    Workspace,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_resolver import (
    AdminAPIResolver,
    DataResolverBase,
    RegularAPIResolver,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api import PowerBiAPI
from datahub.metadata.schema_classes import NumberTypeClass

POWERBI_ORG_URL = "https://api.powerbi.com/v1.0/myorg"

POWERBI_BASE_CONFIG: Dict[str, Any] = {
    "tenant_id": "t",
    "client_id": "c",
    "client_secret": "s",
}


def powerbi_config(**overrides: Any) -> PowerBiDashboardSourceConfig:
    return PowerBiDashboardSourceConfig.model_validate(
        {**POWERBI_BASE_CONFIG, **overrides}
    )


# --- MSAL -------------------------------------------------------------------


class MsalStub:
    """Counts token acquisitions so token caching can be asserted."""

    def __init__(self, expires_in: int = 3600) -> None:
        self.expires_in = expires_in
        self.calls = 0

    def acquire_token_for_client(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        self.calls += 1
        return {"access_token": f"token-{self.calls}", "expires_in": self.expires_in}


@pytest.fixture
def msal_stub() -> Iterator[MsalStub]:
    stub = MsalStub()
    with mock.patch("msal.ConfidentialClientApplication", return_value=stub):
        yield stub


# --- REST layer -------------------------------------------------------------


def make_resolver(cls: Type[DataResolverBase] = RegularAPIResolver) -> Any:
    """A resolver with dummy credentials; needs MSAL patched (``msal_stub``)."""
    return cls(
        client_id="client",
        client_secret="secret",
        tenant_id="tenant",
        metadata_api_timeout=10,
    )


def make_api(**config_overrides: Any) -> PowerBiAPI:
    """A PowerBiAPI with a fresh reporter; needs MSAL patched (``msal_stub``)."""
    return PowerBiAPI(
        config=powerbi_config(**config_overrides),
        reporter=PowerBiDashboardSourceReport(),
    )


def admin_resolver(api: PowerBiAPI) -> AdminAPIResolver:
    return api._PowerBiAPI__admin_api_resolver  # type: ignore[attr-defined]


def http_error(status: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status
    return requests.HTTPError(response=response)


# --- data classes -----------------------------------------------------------


def make_workspace(ws_id: str = "ws-1", name: str = "Sales") -> Workspace:
    return Workspace(
        id=ws_id,
        name=name,
        type="Workspace",
        webUrl=f"https://app.powerbi.com/groups/{ws_id}",
        datasets={},
        dashboards={},
        reports={},
        report_endorsements={},
        dashboard_endorsements={},
        scan_result={},
        independent_datasets={},
        app=None,
    )


def make_column(
    name: str, hidden: bool = False, description: Optional[str] = None
) -> Column:
    return Column(
        name=name,
        dataType="Int64",
        isHidden=hidden,
        datahubDataType=NumberTypeClass(),
        description=description,
    )


def make_table(
    name: str = "Orders",
    *,
    dataset_name: str = "Model",
    expression: Optional[str] = None,
    columns: Optional[List[Column]] = None,
    measures: Optional[List[Measure]] = None,
    dataset: Optional[PowerBIDataset] = None,
) -> Table:
    return Table(
        name=name,
        full_name=f"{dataset_name}.{name}",
        expression=expression,
        columns=columns,
        measures=measures,
        dataset=dataset,
    )


def make_dataset(
    dataset_id: str = "ds-1",
    *,
    workspace: Optional[Workspace] = None,
    tags: Optional[List[str]] = None,
    configured_by: Optional[str] = None,
) -> PowerBIDataset:
    """A dataset named ``Model`` with no tables; append ``make_table`` results."""
    workspace = workspace or make_workspace()
    return PowerBIDataset(
        id=dataset_id,
        name="Model",
        description="desc",
        webUrl=f"{workspace.webUrl}/datasets/{dataset_id}",
        workspace_id=workspace.id,
        workspace_name=workspace.name,
        parameters={},
        tables=[],
        tags=tags or [],
        configuredBy=configured_by,
    )


def make_report(
    dataset: Optional[PowerBIDataset],
    *,
    report_id: str = "r-1",
    pages: Optional[List[Page]] = None,
    users: Optional[List[User]] = None,
) -> Report:
    return Report(
        id=report_id,
        name="Report",
        type=ReportType.PowerBIReport,
        webUrl=f"https://app.powerbi.com/groups/ws-1/reports/{report_id}",
        embedUrl=None,
        description="",
        dataset_id=dataset.id if dataset else None,
        dataset=dataset,
        pages=pages or [],
        users=users or [],
        tags=[],
    )


def make_tile(
    dataset: Optional[PowerBIDataset] = None,
    report: Optional[Report] = None,
    *,
    dataset_id: Optional[str] = None,
    report_id: Optional[str] = None,
    created_from: Tile.CreatedFrom = Tile.CreatedFrom.REPORT,
) -> Tile:
    """``dataset_id``/``report_id`` default to the ids of the linked objects, so
    an unresolved reference is spelled by passing the id without the object."""
    return Tile(
        id="t-1",
        title="Tile",
        embedUrl=None,
        dataset_id=dataset_id or (dataset.id if dataset else None),
        report_id=report_id or (report.id if report else None),
        createdFrom=created_from,
        dataset=dataset,
        report=report,
    )


def make_dashboard(
    tiles: Optional[List[Tile]] = None, users: Optional[List[User]] = None
) -> Dashboard:
    return Dashboard(
        id="d-1",
        displayName="Dash",
        description="",
        embedUrl=None,
        isReadOnly=False,
        workspace_id="ws-1",
        workspace_name="Sales",
        tiles=tiles or [],
        users=users or [],
        tags=[],
        webUrl=None,
    )


def make_user(
    user_id: str = "u-1",
    *,
    display_name: str = "Alice",
    email: Optional[str] = "alice@example.com",
    principal_type: str = "User",
    access_right: str = "Owner",
) -> User:
    return User(
        id=user_id,
        displayName=display_name,
        emailAddress=email,
        graphId=f"g-{user_id}",
        principalType=principal_type,
        dashboardUserAccessRight=access_right,
    )


# --- mapper and M-Query -----------------------------------------------------


def make_mapper(config: PowerBiDashboardSourceConfig, workspace: Workspace) -> Mapper:
    mapper = Mapper(
        ctx=PipelineContext(run_id="test"),
        config=config,
        reporter=PowerBiDashboardSourceReport(),
        dataplatform_instance_resolver=create_dataplatform_instance_resolver(config),
    )
    # The source emits the workspace container first; that call also fixes
    # the parent container key every later entity is attached to.
    list(mapper.generate_container_for_workspace(workspace))
    return mapper


def m_query_lineages(
    expression: str,
    *,
    config_overrides: Optional[Dict[str, Any]] = None,
    parameters: Optional[Dict[str, str]] = None,
    columns: Optional[List[Column]] = None,
) -> Tuple[List[Lineage], PowerBiDashboardSourceReport]:
    """Run one M expression through the real parser bridge and pattern handlers.

    Advanced (sqlglot) lineage and column-level lineage are off unless a test
    opts in, so the legacy pattern handlers are what is exercised by default.
    """
    config = powerbi_config(
        **{
            "enable_advance_lineage_sql_construct": False,
            "extract_column_level_lineage": False,
            **(config_overrides or {}),
        }
    )
    reporter = PowerBiDashboardSourceReport()
    lineages = mquery_parser.get_upstream_tables(
        table=Table(
            name="t", full_name="ws.ds.t", expression=expression, columns=columns
        ),
        reporter=reporter,
        platform_instance_resolver=create_dataplatform_instance_resolver(config),
        ctx=PipelineContext(run_id="test"),
        config=config,
        parameters=parameters,
    )
    return lineages, reporter


def upstream_urns(lineages: List[Lineage]) -> List[str]:
    return [upstream.urn for lineage in lineages for upstream in lineage.upstreams]
