"""Shared builders for the Fabric OneLake unit tests.

`FakeOneLakeClient` is a hand-written fake tenant served to the probe and to
ingestion through the parts of `OneLakeClient` each calls. `onelake_client` is
the real client over a specced auth helper, for tests that stub its HTTP
session. `make_onelake_source` builds sources and closes them at teardown.
"""

from typing import Any, Callable, Dict, Iterator, List, Optional, cast
from unittest.mock import NonCallableMagicMock, create_autospec

import pytest
import requests

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.fabric.common.models import FabricWorkspace
from datahub.ingestion.source.fabric.onelake.client import OneLakeClient
from datahub.ingestion.source.fabric.onelake.models import (
    FabricLakehouse,
    FabricTable,
    FabricWarehouse,
)
from datahub.ingestion.source.fabric.onelake.source import FabricOneLakeSource
from tests.unit.fabric_common.conftest import fabric_response, specced_auth_helper

WS = FabricWorkspace(id="00000000-0000-0000-0000-00000000000a", name="sales-ws")
LH_ID = "00000000-0000-0000-0000-00000000000b"
WH_ID = "00000000-0000-0000-0000-00000000000c"

ENDPOINT_HOST = "placeholder.datawarehouse.fabric.microsoft.com"
PROVISIONED: Dict[str, object] = {
    "properties": {
        "sqlEndpointProperties": {
            "provisioningStatus": "Success",
            "connectionString": ENDPOINT_HOST,
        }
    }
}

# No SQL Analytics Endpoint: tables come from the REST listing alone.
REST_ONLY: Dict[str, Any] = {
    "sql_endpoint": None,
    "extract_views": False,
    "extract_schema": {"enabled": False},
    "usage": {"include_usage_statistics": False},
}


def tables() -> List[FabricTable]:
    # "" is a schemas-disabled lakehouse table; ingestion files it under dbo.
    return [
        FabricTable(name="orders", schema_name="", item_id=LH_ID, workspace_id=WS.id),
        FabricTable(
            name="refunds", schema_name="finance", item_id=LH_ID, workspace_id=WS.id
        ),
        FabricTable(
            name="tmp_load", schema_name="staging", item_id=LH_ID, workspace_id=WS.id
        ),
    ]


class FakeOneLakeClient:
    """Answers only what the probe and ingestion call, and records the calls
    so resolution cost is visible."""

    def __init__(self) -> None:
        self.calls: List[str] = []
        self.closed = False
        self.auth_helper = specced_auth_helper()
        self.lakehouse_tables: List[FabricTable] = tables()
        self.degrade_message: str = ""
        self.workspaces_error: Optional[Exception] = None
        self.lakehouses_error: Optional[Exception] = None
        # The item GET the SQL endpoint lookup makes: a response body (here,
        # a provisioned endpoint), or an error to raise.
        self.item_body: Dict[str, object] = PROVISIONED
        self.item_error: Optional[Exception] = None

    def list_workspaces(self) -> Iterator[FabricWorkspace]:
        self.calls.append("workspaces")
        if self.workspaces_error is not None:
            raise self.workspaces_error
        yield WS
        yield FabricWorkspace(id="00000000-0000-0000-0000-0000000000ff", name="hr-ws")

    def list_lakehouses(self, workspace_id: str) -> Iterator[FabricLakehouse]:
        self.calls.append("lakehouses")
        if self.lakehouses_error is not None:
            raise self.lakehouses_error
        yield FabricLakehouse(
            id=LH_ID, name="lh_main", type="Lakehouse", workspace_id=workspace_id
        )
        yield FabricLakehouse(
            id="lh-dup", name="shared_name", type="Lakehouse", workspace_id=workspace_id
        )

    def list_warehouses(self, workspace_id: str) -> Iterator[FabricWarehouse]:
        self.calls.append("warehouses")
        yield FabricWarehouse(
            id=WH_ID, name="wh_main", type="Warehouse", workspace_id=workspace_id
        )
        yield FabricWarehouse(
            id="wh-dup", name="shared_name", type="Warehouse", workspace_id=workspace_id
        )

    def list_lakehouse_tables(
        self,
        workspace_id: str,
        lakehouse_id: str,
        *,
        on_degraded: Optional[Callable[[str], None]] = None,
    ) -> Iterator[FabricTable]:
        self.calls.append("lakehouse_tables")
        if self.degrade_message and on_degraded is not None:
            on_degraded(self.degrade_message)
            return iter([])
        return iter(self.lakehouse_tables)

    def list_warehouse_tables(
        self,
        workspace_id: str,
        warehouse_id: str,
        *,
        on_degraded: Optional[Callable[[str], None]] = None,
    ) -> Iterator[FabricTable]:
        self.calls.append("warehouse_tables")
        return iter(
            [
                FabricTable(
                    name="facts",
                    schema_name="dbo",
                    item_id=warehouse_id,
                    workspace_id=workspace_id,
                )
            ]
        )

    def get(self, endpoint: str) -> requests.Response:
        self.calls.append(f"get {endpoint}")
        if self.item_error is not None:
            raise self.item_error
        return fabric_response(200, self.item_body)

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def auth_helper() -> NonCallableMagicMock:
    return specced_auth_helper()


@pytest.fixture
def onelake_client(auth_helper: NonCallableMagicMock) -> Iterator[OneLakeClient]:
    client = OneLakeClient(auth_helper=auth_helper)
    yield client
    client.close()


OneLakeSourceFactory = Callable[..., FabricOneLakeSource]


@pytest.fixture
def make_onelake_source() -> Iterator[OneLakeSourceFactory]:
    """`make(client=None, **recipe)`: a source over `client` (a
    `FakeOneLakeClient`), or over a specced `OneLakeClient` mock when none is
    given."""
    sources: List[FabricOneLakeSource] = []

    def make(
        client: Optional[FakeOneLakeClient] = None, **recipe: Any
    ) -> FabricOneLakeSource:
        source = FabricOneLakeSource.create(
            dict(recipe), PipelineContext(run_id="fabric-onelake-test")
        )
        source.client.close()
        source.client = (
            cast(OneLakeClient, client)
            if client is not None
            else create_autospec(OneLakeClient, instance=True)
        )
        sources.append(source)
        return source

    yield make
    for source in sources:
        source.close()
