from typing import Any, Dict, List
from unittest import mock

from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
    PowerBiEnvironment,
)
from datahub.ingestion.source.powerbi.powerbi import PowerBiDashboardSource
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import Workspace
from datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api import (
    workspace_from_group,
)


def _source(groups: List[Dict[str, Any]]) -> PowerBiDashboardSource:
    # Only the three attributes get_allowed_workspaces reads; the constructor
    # would build an API client.
    source = PowerBiDashboardSource.__new__(PowerBiDashboardSource)
    source.source_config = PowerBiDashboardSourceConfig.model_validate(
        {"tenant_id": "tenant", "client_id": "client", "client_secret": "secret"}
    )
    source.reporter = PowerBiDashboardSourceReport()
    source.powerbi_client = mock.MagicMock()
    source.powerbi_client.get_workspaces.return_value = [
        workspace_from_group(g, PowerBiEnvironment.COMMERCIAL) for g in groups
    ]
    return source


def test_ingestion_excludes_a_workspace_whose_type_is_null() -> None:
    # The API's type is a required field, but a null one was always excluded
    # by workspace_type_filter (None is never in it); keep it that way.
    source = _source(
        [
            {"id": "ws-1", "name": "Sales", "type": "Workspace"},
            {"id": "ws-2", "name": "Untyped", "type": None},
        ]
    )
    kept: List[Workspace] = source.get_allowed_workspaces()
    assert [ws.name for ws in kept] == ["Sales"]
    assert list(source.reporter.filtered_workspace_types) == [
        "ws-2 - Untyped (type = None)"
    ]
