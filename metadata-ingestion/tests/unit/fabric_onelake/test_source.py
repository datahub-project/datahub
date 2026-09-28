"""Unit tests for Fabric OneLake source key hierarchy and item processing."""

from typing import Iterator, List
from unittest.mock import MagicMock, patch

import pytest
import requests

from datahub.emitter.mce_builder import datahub_guid
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.fabric.common.models import (
    FABRIC_WORKSPACE_PLATFORM,
    FabricWorkspace,
    WorkspaceKey,
)
from datahub.ingestion.source.fabric.onelake.client import OneLakeClient
from datahub.ingestion.source.fabric.onelake.models import (
    FabricLakehouse,
    FabricTable,
    FabricWarehouse,
)
from datahub.ingestion.source.fabric.onelake.source import (
    PLATFORM,
    FabricOneLakeSource,
    LakehouseKey,
    LakehouseSchemaKey,
    WarehouseKey,
    WarehouseSchemaKey,
    _iter_isolated,
)


def test_lakehouse_parent_workspace_uses_fabric_platform() -> None:
    key = LakehouseKey(
        platform=PLATFORM,
        instance="instance-1",
        env="PROD",
        workspace_id="ws-123",
        lakehouse_id="lh-456",
    )

    parent = key.parent_key()
    assert isinstance(parent, WorkspaceKey)
    assert parent.platform == FABRIC_WORKSPACE_PLATFORM
    assert parent.workspace_id == "ws-123"


def test_warehouse_parent_workspace_uses_fabric_platform() -> None:
    key = WarehouseKey(
        platform=PLATFORM,
        instance="instance-1",
        env="PROD",
        workspace_id="ws-123",
        warehouse_id="wh-456",
    )

    parent = key.parent_key()
    assert isinstance(parent, WorkspaceKey)
    assert parent.platform == FABRIC_WORKSPACE_PLATFORM
    assert parent.workspace_id == "ws-123"


def test_schema_parent_chain_keeps_onelake_then_fabric() -> None:
    schema_key = LakehouseSchemaKey(
        platform=PLATFORM,
        instance="instance-1",
        env="PROD",
        workspace_id="ws-123",
        lakehouse_id="lh-456",
        schema_name="dbo",
    )

    lakehouse_parent = schema_key.parent_key()
    assert isinstance(lakehouse_parent, LakehouseKey)
    assert lakehouse_parent.platform == PLATFORM

    workspace_parent = lakehouse_parent.parent_key()
    assert isinstance(workspace_parent, WorkspaceKey)
    assert workspace_parent.platform == FABRIC_WORKSPACE_PLATFORM


def test_warehouse_schema_parent_chain_keeps_onelake_then_fabric() -> None:
    schema_key = WarehouseSchemaKey(
        platform=PLATFORM,
        instance="instance-1",
        env="PROD",
        workspace_id="ws-123",
        warehouse_id="wh-456",
        schema_name="sales",
    )

    warehouse_parent = schema_key.parent_key()
    assert isinstance(warehouse_parent, WarehouseKey)
    assert warehouse_parent.platform == PLATFORM

    workspace_parent = warehouse_parent.parent_key()
    assert isinstance(workspace_parent, WorkspaceKey)
    assert workspace_parent.platform == FABRIC_WORKSPACE_PLATFORM


def test_lakehouse_key_guid_uses_fabric_onelake_platform() -> None:
    key = LakehouseKey(
        platform=PLATFORM,
        instance="instance-1",
        env="PROD",
        workspace_id="ws-123",
        lakehouse_id="lh-456",
    )

    expected = datahub_guid(
        {
            "platform": "fabric-onelake",
            "instance": "instance-1",
            "workspace_id": "ws-123",
            "lakehouse_id": "lh-456",
        }
    )
    assert key.guid() == expected


def test_warehouse_key_guid_uses_fabric_onelake_platform() -> None:
    key = WarehouseKey(
        platform=PLATFORM,
        instance="instance-1",
        env="PROD",
        workspace_id="ws-123",
        warehouse_id="wh-456",
    )

    expected = datahub_guid(
        {
            "platform": "fabric-onelake",
            "instance": "instance-1",
            "workspace_id": "ws-123",
            "warehouse_id": "wh-456",
        }
    )
    assert key.guid() == expected


def test_norm_respects_convert_urns_to_lowercase() -> None:
    """_norm lowercases identifiers iff convert_urns_to_lowercase=True.

    _norm gates URN-bound identifier casing so the dataset URNs match what
    sqlglot emits during view-lineage parsing. Bypassing __init__ is fine here
    because _norm only reads self.config.convert_urns_to_lowercase.
    """
    src = MagicMock()

    src.config.convert_urns_to_lowercase = True
    assert FabricOneLakeSource._norm(src, "Sales") == "sales"
    assert FabricOneLakeSource._norm(src, "CUSTOMERS") == "customers"
    assert FabricOneLakeSource._norm(src, "already_lower") == "already_lower"

    src.config.convert_urns_to_lowercase = False
    assert FabricOneLakeSource._norm(src, "Sales") == "Sales"
    assert FabricOneLakeSource._norm(src, "CUSTOMERS") == "CUSTOMERS"


_WORKSPACE = FabricWorkspace(id="ws-1", name="Analytics")


def _make_source(**config: object) -> FabricOneLakeSource:
    return FabricOneLakeSource.create(
        {
            "credential": {
                "authentication_method": "service_principal",
                "client_id": "test-client",
                "client_secret": "test-secret",
                "tenant_id": "test-tenant",
            },
            **config,
        },
        PipelineContext(run_id="fabric-onelake-unit"),
    )


def _lakehouse(item_id: str, name: str) -> FabricLakehouse:
    return FabricLakehouse(
        id=item_id, name=name, workspace_id=_WORKSPACE.id, type="Lakehouse"
    )


def test_iter_isolated_reports_producer_errors() -> None:
    def producer() -> Iterator[int]:
        yield 1
        raise RuntimeError("listing broke")

    errors: List[Exception] = []
    assert list(_iter_isolated(producer(), on_error=errors.append)) == [1]
    assert len(errors) == 1 and isinstance(errors[0], RuntimeError)


def test_iter_isolated_does_not_swallow_consumer_errors() -> None:
    """An exception thrown in at the yield point is the consumer's, not the
    producer's: it must propagate rather than be reported as a fetch failure."""
    errors: List[Exception] = []
    wrapped = _iter_isolated(iter([1, 2, 3]), on_error=errors.append)
    assert next(wrapped) == 1
    with pytest.raises(ValueError, match="sink failed"):
        wrapped.throw(ValueError("sink failed"))
    assert errors == []


def test_failing_lakehouse_does_not_skip_the_next_one() -> None:
    source = _make_source()
    broken, healthy = _lakehouse("lh-1", "broken_lh"), _lakehouse("lh-2", "ok_lh")

    def process(
        workspace: FabricWorkspace, lakehouse: FabricLakehouse
    ) -> Iterator[str]:
        yield f"container:{lakehouse.id}"
        if lakehouse.id == broken.id:
            raise RuntimeError("tables API failed")
        yield f"table:{lakehouse.id}"

    with patch.object(source, "_process_lakehouse", side_effect=process):
        emitted = list(
            source._process_workspace_items(_WORKSPACE, [broken, healthy], [])
        )

    assert emitted == ["container:lh-1", "container:lh-2", "table:lh-2"]
    warnings = [
        w for w in source.report.warnings if w.title == "Failed to Process Lakehouse"
    ]
    assert len(warnings) == 1
    assert any("broken_lh" in c for c in warnings[0].context)
    source.close()


def test_item_listing_failure_is_the_reported_unresolved_cause() -> None:
    source = _make_source()
    warehouse = FabricWarehouse(
        id="wh-1", name="gold_wh", workspace_id=_WORKSPACE.id, type="Warehouse"
    )
    with (
        patch.object(
            OneLakeClient, "list_lakehouses", side_effect=RuntimeError("HTTP 500")
        ),
        patch.object(OneLakeClient, "list_warehouses", return_value=[warehouse]),
    ):
        lakehouses, warehouses = source._list_workspace_items(_WORKSPACE)

    assert lakehouses is None
    assert warehouses == [warehouse]
    assert any(w.title == "Failed to List Lakehouses" for w in source.report.warnings)
    # The warehouse is still indexed; a lakehouse reference is a miss whose
    # reason points at the failed listing, not at a missing item.
    assert source.item_catalog.resolve_item(_WORKSPACE.id, "gold_wh").item is not None
    reason = source.item_catalog.resolve_item(_WORKSPACE.id, "silver_lh").reason
    assert reason is not None and "listing" in reason
    source.close()


def _http_error(status_code: int) -> requests.exceptions.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    return requests.exceptions.HTTPError(f"HTTP {status_code}", response=response)


def _table(item_id: str, schema: str, name: str) -> FabricTable:
    return FabricTable(
        name=name, schema_name=schema, item_id=item_id, workspace_id=_WORKSPACE.id
    )


def test_warehouse_tables_come_from_sql_endpoint_not_rest() -> None:
    """The Fabric REST Tables API is Lakehouse-only; Warehouse tables must be
    discovered via INFORMATION_SCHEMA.TABLES on the SQL endpoint."""
    source = _make_source()
    schema_client = MagicMock()
    schema_client.get_all_tables.return_value = [
        _table("wh-1", "dbo", "customer_totals")
    ]
    with patch.object(
        OneLakeClient,
        "list_warehouse_tables",
        side_effect=AssertionError("REST API must not be called"),
    ):
        tables = source._list_item_tables(
            _WORKSPACE, "wh-1", "Warehouse", "gold_wh", schema_client
        )

    assert [t.name for t in tables] == ["customer_totals"]
    schema_client.get_all_tables.assert_called_once_with(
        workspace_id=_WORKSPACE.id, item_id="wh-1"
    )
    assert source.report.num_warehouse_tables_discovered_via_sql_endpoint == 1
    assert not source.report.warnings
    source.close()


def test_lakehouse_tables_still_come_from_rest() -> None:
    source = _make_source()
    schema_client = MagicMock()
    with patch.object(
        OneLakeClient,
        "list_lakehouse_tables",
        return_value=iter([_table("lh-1", "dbo", "customers")]),
    ):
        tables = source._list_item_tables(
            _WORKSPACE, "lh-1", "Lakehouse", "silver_lh", schema_client
        )

    assert [t.name for t in tables] == ["customers"]
    schema_client.get_all_tables.assert_not_called()
    source.close()


def test_warehouse_sql_table_discovery_failure_is_reported() -> None:
    source = _make_source()
    schema_client = MagicMock()
    schema_client.get_all_tables.side_effect = RuntimeError("login timeout")

    tables = source._list_item_tables(
        _WORKSPACE, "wh-1", "Warehouse", "gold_wh", schema_client
    )

    assert tables == []
    warnings = [
        w
        for w in source.report.warnings
        if w.title == "Warehouse Table Discovery Failed"
    ]
    assert len(warnings) == 1
    assert any("gold_wh" in c for c in warnings[0].context)
    source.close()


def test_warehouse_rest_404_without_sql_endpoint_reports_the_fix() -> None:
    """Without a SQL endpoint the REST fallback 404s; the report must say why
    (REST is Lakehouse-only, not a 'staging warehouse' quirk) and how to fix."""
    source = _make_source()
    with patch.object(
        OneLakeClient, "list_warehouse_tables", side_effect=_http_error(404)
    ):
        tables = source._list_item_tables(
            _WORKSPACE, "wh-1", "Warehouse", "gold_wh", None
        )

    assert tables == []
    assert source.report.num_warehouses_without_table_discovery == 1
    warnings = [
        w
        for w in source.report.warnings
        if w.title == "Warehouse Tables Not Discovered"
    ]
    assert len(warnings) == 1
    assert "sql_endpoint.enabled" in warnings[0].message
    assert "staging" not in warnings[0].message
    source.close()


def test_warehouse_rest_non_404_error_is_reported_generically() -> None:
    source = _make_source()
    with patch.object(
        OneLakeClient, "list_warehouse_tables", side_effect=_http_error(500)
    ):
        tables = source._list_item_tables(
            _WORKSPACE, "wh-1", "Warehouse", "gold_wh", None
        )

    assert tables == []
    assert source.report.num_warehouses_without_table_discovery == 0
    assert any(w.title == "Failed to Process Tables" for w in source.report.warnings)
    source.close()


def test_schema_client_not_created_when_sql_endpoint_disabled() -> None:
    source = _make_source(
        sql_endpoint={"enabled": False},
        extract_views=False,
        extract_schema={"enabled": False},
        usage={"include_usage_statistics": False},
    )
    with patch(
        "datahub.ingestion.source.fabric.onelake.schema_client."
        "create_schema_extraction_client"
    ) as factory:
        client = source._create_schema_client(
            _WORKSPACE, "wh-1", "Warehouse", "gold_wh"
        )

    assert client is None
    factory.assert_not_called()
    source.close()


def test_schema_client_created_for_warehouse_table_discovery_alone() -> None:
    """Warehouse table discovery needs the endpoint even with every other
    endpoint-backed feature turned off."""
    source = _make_source(
        extract_views=False,
        extract_schema={"enabled": False},
        usage={"include_usage_statistics": False},
    )
    with patch(
        "datahub.ingestion.source.fabric.onelake.schema_client."
        "create_schema_extraction_client"
    ) as factory:
        warehouse_client = source._create_schema_client(
            _WORKSPACE, "wh-1", "Warehouse", "gold_wh"
        )
        lakehouse_client = source._create_schema_client(
            _WORKSPACE, "lh-1", "Lakehouse", "silver_lh"
        )

    assert warehouse_client is factory.return_value
    assert lakehouse_client is None
    source.close()
