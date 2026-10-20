"""A fake Fabric OneLake tenant, served to the probe and to ingestion through
the parts of OneLakeClient each calls."""

from typing import Callable, Dict, Iterator, List, Optional
from unittest.mock import MagicMock

from datahub.ingestion.source.fabric.common.models import FabricWorkspace
from datahub.ingestion.source.fabric.onelake.models import (
    FabricLakehouse,
    FabricTable,
    FabricWarehouse,
)

WS = FabricWorkspace(id="00000000-0000-0000-0000-00000000000a", name="sales-ws")
LH_ID = "00000000-0000-0000-0000-00000000000b"


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


class FakeOneLakeClient:
    """Answers only what the probe calls, and records the calls so resolution
    cost is visible."""

    def __init__(self) -> None:
        self.calls: List[str] = []
        self.closed = False
        self.auth_helper = MagicMock()
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

    def get(self, endpoint: str) -> MagicMock:
        self.calls.append(f"get {endpoint}")
        if self.item_error is not None:
            raise self.item_error
        response = MagicMock()
        response.json.return_value = self.item_body
        return response

    def close(self) -> None:
        self.closed = True
