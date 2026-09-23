"""Integration tests for Fabric OneLake source.

These tests use mocked REST API responses to verify the full ingestion pipeline
produces the expected metadata events.
"""

import json
import re
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.fabric.common.models import FabricWorkspace
from datahub.ingestion.source.fabric.onelake.client import OneLakeClient
from datahub.ingestion.source.fabric.onelake.models import (
    FabricColumn,
    FabricLakehouse,
    FabricQueryInsightsRow,
    FabricTable,
    FabricView,
    FabricWarehouse,
)
from datahub.ingestion.source.fabric.onelake.source import FabricOneLakeSource
from datahub.testing import mce_helpers

FROZEN_TIME = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_fabric_onelake_workspace_ingestion() -> None:
    """Test ingestion of a single workspace."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        output_file = tmp.name

    try:
        with (
            patch.object(
                OneLakeClient,
                "list_workspaces",
                return_value=[
                    FabricWorkspace(
                        id="ws-123",
                        name="Test Workspace",
                        description="Test description",
                    )
                ],
            ),
            patch.object(OneLakeClient, "list_lakehouses", return_value=[]),
            patch.object(OneLakeClient, "list_warehouses", return_value=[]),
        ):
            # Run pipeline
            pipeline = Pipeline.create(
                {
                    "source": {
                        "type": "fabric-onelake",
                        "config": {
                            "credential": {
                                "authentication_method": "service_principal",
                                "client_id": "test-client",
                                "client_secret": "test-secret",
                                "tenant_id": "test-tenant",
                            }
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": output_file},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

            # Verify output file was created and contains expected data
            assert Path(output_file).exists()
            with open(output_file) as f:
                data = json.load(f)
                # Should contain at least one workspace container
                assert len(data) > 0

    finally:
        Path(output_file).unlink(missing_ok=True)


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_fabric_onelake_lakehouse_with_tables(pytestconfig: pytest.Config) -> None:
    """Test ingestion of a lakehouse with tables."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        output_file = tmp.name

    try:
        with (
            patch.object(
                OneLakeClient,
                "list_workspaces",
                return_value=[FabricWorkspace(id="ws-123", name="Test Workspace")],
            ),
            patch.object(
                OneLakeClient,
                "list_lakehouses",
                return_value=[
                    FabricLakehouse(
                        id="lh-456",
                        name="Test Lakehouse",
                        workspace_id="ws-123",
                        type="Lakehouse",
                    )
                ],
            ),
            patch.object(OneLakeClient, "list_warehouses", return_value=[]),
            patch.object(
                OneLakeClient,
                "list_lakehouse_tables",
                return_value=[
                    FabricTable(
                        name="customers",
                        schema_name="dbo",
                        item_id="lh-456",
                        workspace_id="ws-123",
                    ),
                    FabricTable(
                        name="orders",
                        schema_name="dbo",
                        item_id="lh-456",
                        workspace_id="ws-123",
                    ),
                ],
            ),
        ):
            # Run pipeline
            pipeline = Pipeline.create(
                {
                    "source": {
                        "type": "fabric-onelake",
                        "config": {
                            "credential": {
                                "authentication_method": "service_principal",
                                "client_id": "test-client",
                                "client_secret": "test-secret",
                                "tenant_id": "test-tenant",
                            }
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": output_file},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

            # Validate against golden file
            golden_path = (
                Path(__file__).parent
                / "golden"
                / "test_fabric_onelake_lakehouse_with_tables_golden.json"
            )
            mce_helpers.check_golden_file(
                pytestconfig,
                output_path=output_file,
                golden_path=str(golden_path),
            )

    finally:
        Path(output_file).unlink(missing_ok=True)


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_fabric_onelake_lakehouse_with_views(pytestconfig: pytest.Config) -> None:
    """View extraction emits a view dataset with definition and View subtype.

    Views are discovered via INFORMATION_SCHEMA.VIEWS over the SQL Analytics
    Endpoint, so we patch _create_schema_client to return a mock client whose
    get_all_views() yields one view, and _fetch_schema_map to return the
    matching column metadata.
    """
    view = FabricView(
        name="v_active_customers",
        schema_name="dbo",
        item_id="lh-456",
        workspace_id="ws-123",
        view_definition=(
            "SELECT customer_id, name FROM dbo.customers WHERE active = 1"
        ),
    )
    schema_map = {
        ("dbo", "v_active_customers"): [
            FabricColumn(name="customer_id", data_type="int", is_nullable=False),
            FabricColumn(name="name", data_type="varchar", is_nullable=True),
        ],
    }

    mock_schema_client = MagicMock()
    mock_schema_client.get_all_views.return_value = [view]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        output_file = tmp.name

    try:
        with (
            patch.object(
                OneLakeClient,
                "list_workspaces",
                return_value=[FabricWorkspace(id="ws-123", name="Test Workspace")],
            ),
            patch.object(
                OneLakeClient,
                "list_lakehouses",
                return_value=[
                    FabricLakehouse(
                        id="lh-456",
                        name="Test Lakehouse",
                        workspace_id="ws-123",
                        type="Lakehouse",
                    )
                ],
            ),
            patch.object(OneLakeClient, "list_warehouses", return_value=[]),
            patch.object(OneLakeClient, "list_lakehouse_tables", return_value=[]),
            patch.object(
                FabricOneLakeSource,
                "_create_schema_client",
                return_value=mock_schema_client,
            ),
            patch.object(
                FabricOneLakeSource,
                "_fetch_schema_map",
                return_value=schema_map,
            ),
        ):
            pipeline = Pipeline.create(
                {
                    "source": {
                        "type": "fabric-onelake",
                        "config": {
                            "credential": {
                                "authentication_method": "service_principal",
                                "client_id": "test-client",
                                "client_secret": "test-secret",
                                "tenant_id": "test-tenant",
                            },
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": output_file},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

            golden_path = (
                Path(__file__).parent
                / "golden"
                / "test_fabric_onelake_lakehouse_with_views_golden.json"
            )
            mce_helpers.check_golden_file(
                pytestconfig,
                output_path=output_file,
                golden_path=str(golden_path),
            )

    finally:
        Path(output_file).unlink(missing_ok=True)


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_fabric_onelake_warehouse_with_views(pytestconfig: pytest.Config) -> None:
    """View extraction works on warehouses, not just lakehouses.

    Mirrors test_fabric_onelake_lakehouse_with_views but exercises the warehouse
    code path (`_process_warehouse` -> `_process_item_views`) to guard against
    regressions where view discovery is wired up for lakehouses only.

    The view's upstream `dbo.orders` is a T-SQL-created Warehouse table. The
    Fabric REST Tables API is Lakehouse-only, so it must be discovered through
    INFORMATION_SCHEMA.TABLES on the SQL endpoint (`get_all_tables`) and emitted
    with schemaMetadata, giving the view column-level lineage.
    """
    view = FabricView(
        name="v_total_orders",
        schema_name="dbo",
        item_id="wh-789",
        workspace_id="ws-123",
        view_definition=(
            "SELECT order_id, SUM(amount) AS total FROM dbo.orders GROUP BY order_id"
        ),
    )
    schema_map = {
        ("dbo", "orders"): [
            FabricColumn(name="order_id", data_type="int", is_nullable=False),
            FabricColumn(name="amount", data_type="decimal", is_nullable=True),
        ],
        ("dbo", "v_total_orders"): [
            FabricColumn(name="order_id", data_type="int", is_nullable=False),
            FabricColumn(name="total", data_type="decimal", is_nullable=True),
        ],
    }

    mock_schema_client = MagicMock()
    mock_schema_client.get_all_tables.return_value = [
        FabricTable(
            name="orders", schema_name="dbo", item_id="wh-789", workspace_id="ws-123"
        )
    ]
    mock_schema_client.get_all_views.return_value = [view]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        output_file = tmp.name

    try:
        with (
            patch.object(
                OneLakeClient,
                "list_workspaces",
                return_value=[FabricWorkspace(id="ws-123", name="Test Workspace")],
            ),
            patch.object(OneLakeClient, "list_lakehouses", return_value=[]),
            patch.object(
                OneLakeClient,
                "list_warehouses",
                return_value=[
                    FabricWarehouse(
                        id="wh-789",
                        name="Test Warehouse",
                        workspace_id="ws-123",
                        type="Warehouse",
                    )
                ],
            ),
            patch.object(
                OneLakeClient,
                "list_warehouse_tables",
                side_effect=AssertionError("REST warehouse tables API called"),
            ),
            patch.object(
                FabricOneLakeSource,
                "_create_schema_client",
                return_value=mock_schema_client,
            ),
            patch.object(
                FabricOneLakeSource,
                "_fetch_schema_map",
                return_value=schema_map,
            ),
        ):
            pipeline = Pipeline.create(
                {
                    "source": {
                        "type": "fabric-onelake",
                        "config": {
                            "credential": {
                                "authentication_method": "service_principal",
                                "client_id": "test-client",
                                "client_secret": "test-secret",
                                "tenant_id": "test-tenant",
                            },
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": output_file},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

            assert isinstance(pipeline.source, FabricOneLakeSource)
            report = pipeline.source.report
            assert report.num_warehouse_tables_discovered_via_sql_endpoint == 1
            events = json.loads(Path(output_file).read_text())
            orders_urn = (
                "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,"
                "ws-123.wh-789.dbo.orders,PROD)"
            )
            assert any(
                e.get("entityUrn") == orders_urn
                and e.get("aspectName") == "schemaMetadata"
                for e in events
            )
            view_lineage = [
                e
                for e in events
                if e.get("aspectName") == "upstreamLineage"
                and "v_total_orders" in e.get("entityUrn", "")
            ]
            assert view_lineage
            assert view_lineage[0]["aspect"]["json"].get("fineGrainedLineages")

            golden_path = (
                Path(__file__).parent
                / "golden"
                / "test_fabric_onelake_warehouse_with_views_golden.json"
            )
            mce_helpers.check_golden_file(
                pytestconfig,
                output_path=output_file,
                golden_path=str(golden_path),
            )

    finally:
        Path(output_file).unlink(missing_ok=True)


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_fabric_onelake_with_usage_statistics(pytestconfig: pytest.Config) -> None:
    """Usage extraction emits datasetUsageStatistics for queried tables.

    Patches `stream_usage_history` to yield a small set of queryinsights rows
    (a SELECT against `customers`) and verifies the SQL aggregator drains
    `datasetUsageStatistics` aspects through the pipeline. The query's
    `default_db` matches the URN scheme `<workspace_id>.<item_id>` so the
    parser resolves `dbo.customers` to the same dataset URN we emit.
    """
    table = FabricTable(
        name="customers",
        schema_name="dbo",
        item_id="lh-456",
        workspace_id="ws-123",
    )
    schema_map = {
        ("dbo", "customers"): [
            FabricColumn(name="customer_id", data_type="int", is_nullable=False),
            FabricColumn(name="email", data_type="varchar", is_nullable=True),
        ],
    }

    # Pick a timestamp comfortably inside the default usage window
    # (default end_time = FROZEN_TIME, default start_time = FROZEN_TIME - 1 day floored to UTC midnight).
    query_ts = FROZEN_TIME - timedelta(hours=2)
    usage_rows = [
        FabricQueryInsightsRow(
            start_time=query_ts,
            statement_type="SELECT",
            login_name="alice@example.com",
            row_count=42,
            status="Succeeded",
            command="SELECT customer_id, email FROM dbo.customers WHERE customer_id > 0",
        ),
    ]

    mock_schema_client = MagicMock()
    mock_schema_client.get_all_views.return_value = []
    mock_schema_client.stream_usage_history.return_value = iter(usage_rows)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        output_file = tmp.name

    try:
        with (
            patch.object(
                OneLakeClient,
                "list_workspaces",
                return_value=[FabricWorkspace(id="ws-123", name="Test Workspace")],
            ),
            patch.object(
                OneLakeClient,
                "list_lakehouses",
                return_value=[
                    FabricLakehouse(
                        id="lh-456",
                        name="Test Lakehouse",
                        workspace_id="ws-123",
                        type="Lakehouse",
                    )
                ],
            ),
            patch.object(OneLakeClient, "list_warehouses", return_value=[]),
            patch.object(
                OneLakeClient,
                "list_lakehouse_tables",
                return_value=[table],
            ),
            patch.object(
                FabricOneLakeSource,
                "_create_schema_client",
                return_value=mock_schema_client,
            ),
            patch.object(
                FabricOneLakeSource,
                "_fetch_schema_map",
                return_value=schema_map,
            ),
        ):
            pipeline = Pipeline.create(
                {
                    "source": {
                        "type": "fabric-onelake",
                        "config": {
                            "credential": {
                                "authentication_method": "service_principal",
                                "client_id": "test-client",
                                "client_secret": "test-secret",
                                "tenant_id": "test-tenant",
                            },
                            "usage": {
                                "include_usage_statistics": True,
                                "include_operational_stats": True,
                            },
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": output_file},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

            assert isinstance(pipeline.source, FabricOneLakeSource)
            assert pipeline.source.report.num_usage_queries_fetched == 1
            assert mock_schema_client.stream_usage_history.called

            golden_path = (
                Path(__file__).parent
                / "golden"
                / "test_fabric_onelake_with_usage_statistics_golden.json"
            )
            mce_helpers.check_golden_file(
                pytestconfig,
                output_path=output_file,
                golden_path=str(golden_path),
            )

    finally:
        Path(output_file).unlink(missing_ok=True)


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_fabric_onelake_dml_emits_operation_aspect() -> None:
    """A DML query (INSERT) should produce an `operation` aspect for the target table."""
    table = FabricTable(
        name="customers",
        schema_name="dbo",
        item_id="lh-456",
        workspace_id="ws-123",
    )
    schema_map = {
        ("dbo", "customers"): [
            FabricColumn(name="customer_id", data_type="int", is_nullable=False),
            FabricColumn(name="email", data_type="varchar", is_nullable=True),
        ],
    }

    query_ts = FROZEN_TIME - timedelta(hours=2)
    usage_rows = [
        FabricQueryInsightsRow(
            start_time=query_ts,
            statement_type="INSERT",
            login_name="alice@example.com",
            row_count=1,
            status="Succeeded",
            command=(
                "INSERT INTO dbo.customers (customer_id, email) "
                "VALUES (1, 'foo@bar.com')"
            ),
        ),
    ]

    mock_schema_client = MagicMock()
    mock_schema_client.get_all_views.return_value = []
    mock_schema_client.stream_usage_history.return_value = iter(usage_rows)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        output_file = tmp.name

    try:
        with (
            patch.object(
                OneLakeClient,
                "list_workspaces",
                return_value=[FabricWorkspace(id="ws-123", name="Test Workspace")],
            ),
            patch.object(
                OneLakeClient,
                "list_lakehouses",
                return_value=[
                    FabricLakehouse(
                        id="lh-456",
                        name="Test Lakehouse",
                        workspace_id="ws-123",
                        type="Lakehouse",
                    )
                ],
            ),
            patch.object(OneLakeClient, "list_warehouses", return_value=[]),
            patch.object(
                OneLakeClient,
                "list_lakehouse_tables",
                return_value=[table],
            ),
            patch.object(
                FabricOneLakeSource,
                "_create_schema_client",
                return_value=mock_schema_client,
            ),
            patch.object(
                FabricOneLakeSource,
                "_fetch_schema_map",
                return_value=schema_map,
            ),
        ):
            pipeline = Pipeline.create(
                {
                    "source": {
                        "type": "fabric-onelake",
                        "config": {
                            "credential": {
                                "authentication_method": "service_principal",
                                "client_id": "test-client",
                                "client_secret": "test-secret",
                                "tenant_id": "test-tenant",
                            },
                            "usage": {
                                "include_usage_statistics": True,
                                "include_operational_stats": True,
                            },
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": output_file},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

            with Path(output_file).open() as f:
                events = json.load(f)

            operation_events = [
                event for event in events if event.get("aspectName") == "operation"
            ]
            assert operation_events, (
                "Expected at least one `operation` aspect for the INSERT query; "
                "got none. This means the generate_operations path is not "
                "actually wired up for DML queries."
            )
            # Operation must target the customers dataset (parser resolves
            # `dbo.customers` via default_db=<workspace>.<item> and default_schema=dbo).
            target_urns = {event.get("entityUrn") for event in operation_events}
            assert any("customers" in (urn or "") for urn in target_urns), (
                f"Operation aspects emitted but none target the customers "
                f"dataset; got entityUrns={target_urns!r}"
            )

    finally:
        Path(output_file).unlink(missing_ok=True)


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_fabric_onelake_schema_pattern_filters_schemas() -> None:
    """Schema pattern should exclude tables and schema containers for denied schemas."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        output_file = tmp.name

    try:
        with (
            patch.object(
                OneLakeClient,
                "list_workspaces",
                return_value=[FabricWorkspace(id="ws-123", name="Test Workspace")],
            ),
            patch.object(
                OneLakeClient,
                "list_lakehouses",
                return_value=[
                    FabricLakehouse(
                        id="lh-456",
                        name="Test Lakehouse",
                        workspace_id="ws-123",
                        type="Lakehouse",
                    )
                ],
            ),
            patch.object(OneLakeClient, "list_warehouses", return_value=[]),
            patch.object(
                OneLakeClient,
                "list_lakehouse_tables",
                return_value=[
                    FabricTable(
                        name="customers",
                        schema_name="dbo",
                        item_id="lh-456",
                        workspace_id="ws-123",
                    ),
                    FabricTable(
                        name="orders",
                        schema_name="dbo",
                        item_id="lh-456",
                        workspace_id="ws-123",
                    ),
                    FabricTable(
                        name="raw_events",
                        schema_name="staging",
                        item_id="lh-456",
                        workspace_id="ws-123",
                    ),
                    FabricTable(
                        name="raw_users",
                        schema_name="staging",
                        item_id="lh-456",
                        workspace_id="ws-123",
                    ),
                ],
            ),
        ):
            pipeline = Pipeline.create(
                {
                    "source": {
                        "type": "fabric-onelake",
                        "config": {
                            "credential": {
                                "authentication_method": "service_principal",
                                "client_id": "test-client",
                                "client_secret": "test-secret",
                                "tenant_id": "test-tenant",
                            },
                            "schema_pattern": {
                                "allow": ["^dbo$"],
                            },
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": output_file},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

            with Path(output_file).open() as f:
                events = json.load(f)

            # Collect all entity URNs from emitted events
            dataset_urns = {
                event["entityUrn"]
                for event in events
                if event.get("entityType") == "dataset"
            }

            # dbo tables should be present
            assert any("dbo.customers" in urn for urn in dataset_urns), (
                f"Expected dbo.customers dataset; got {dataset_urns}"
            )
            assert any("dbo.orders" in urn for urn in dataset_urns), (
                f"Expected dbo.orders dataset; got {dataset_urns}"
            )

            # staging tables should be filtered out
            assert not any("staging" in urn for urn in dataset_urns), (
                f"staging tables should be excluded by schema_pattern; got {dataset_urns}"
            )

            # Verify no staging schema container was emitted
            schema_containers = [
                event
                for event in events
                if event.get("aspectName") == "containerProperties"
                and "staging" in event.get("aspect", {}).get("json", {}).get("name", "")
            ]
            assert not schema_containers, (
                "staging schema container should not be emitted when denied by schema_pattern"
            )

    finally:
        Path(output_file).unlink(missing_ok=True)


# Cross-item lineage fixtures: one "analytics" workspace with a bronze / silver
# lakehouse and a gold warehouse, plus a "shared" workspace that also has a
# lakehouse named `silver_lh` (to prove 3-part names resolve within the
# referencing workspace) and a reference lakehouse reached via a 4-part name.
_ANALYTICS_WS = FabricWorkspace(id="ws-analytics", name="Analytics")
_SHARED_WS = FabricWorkspace(id="ws-shared", name="Shared Data")

_LAKEHOUSES = {
    "ws-analytics": [
        FabricLakehouse(
            id="lh-bronze",
            name="bronze_lh",
            workspace_id="ws-analytics",
            type="Lakehouse",
        ),
        FabricLakehouse(
            id="lh-silver",
            name="silver_lh",
            workspace_id="ws-analytics",
            type="Lakehouse",
        ),
    ],
    "ws-shared": [
        FabricLakehouse(
            id="lh-shared-silver",
            name="silver_lh",
            workspace_id="ws-shared",
            type="Lakehouse",
        ),
        FabricLakehouse(
            id="lh-ref", name="ref_lh", workspace_id="ws-shared", type="Lakehouse"
        ),
    ],
}
_WAREHOUSES = {
    "ws-analytics": [
        FabricWarehouse(
            id="wh-gold", name="gold_wh", workspace_id="ws-analytics", type="Warehouse"
        ),
    ],
    "ws-shared": [],
}
_TABLES = {
    "lh-bronze": [("raw_orders", ["order_id", "customer_id", "amount"])],
    "lh-silver": [("customers", ["customer_id", "name", "region_id"])],
    "lh-shared-silver": [("customers", ["customer_id", "name", "region_id"])],
    "lh-ref": [("regions", ["region_id", "region_name"])],
    "wh-gold": [
        ("customer_totals", ["customer_id", "total_amount"]),
        ("customer_snapshot", ["customer_id", "name"]),
    ],
}
_GOLD_VIEWS = [
    # 3-part reference to another item in the same workspace.
    (
        "v_customers",
        "CREATE VIEW dbo.v_customers AS "
        "SELECT c.customer_id, c.name FROM silver_lh.dbo.customers AS c",
        ["customer_id", "name"],
    ),
    # Bracketed identifiers, mixed case, and a join across two items.
    (
        "v_customer_orders",
        "CREATE VIEW [dbo].[v_customer_orders] AS "
        "SELECT c.customer_id, c.name, o.amount "
        "FROM [silver_lh].[dbo].[customers] AS c "
        "JOIN [Bronze_LH].[dbo].[raw_orders] AS o ON c.customer_id = o.customer_id",
        ["customer_id", "name", "amount"],
    ),
    # 4-part reference into another ingested workspace.
    (
        "v_customer_regions",
        "CREATE VIEW dbo.v_customer_regions AS "
        "SELECT c.customer_id, r.region_name "
        "FROM silver_lh.dbo.customers AS c "
        "JOIN [Shared Data].[ref_lh].[dbo].[regions] AS r ON c.region_id = r.region_id",
        ["customer_id", "region_name"],
    ),
    # SELECT * across items: columns come from the registered upstream schema.
    (
        "v_customers_all",
        "CREATE VIEW dbo.v_customers_all AS SELECT * FROM silver_lh.dbo.customers",
        ["customer_id", "name", "region_id"],
    ),
    # Unknown item: its upstream is dropped (with a warning), the known one kept.
    (
        "v_partial",
        "CREATE VIEW dbo.v_partial AS "
        "SELECT c.customer_id, x.score "
        "FROM silver_lh.dbo.customers AS c "
        "JOIN missing_lh.dbo.scores AS x ON c.customer_id = x.customer_id",
        ["customer_id", "score"],
    ),
]
_GOLD_QUERIES = [
    (
        "INSERT",
        "INSERT INTO dbo.customer_totals (customer_id, total_amount) "
        "SELECT o.customer_id, SUM(o.amount) FROM bronze_lh.dbo.raw_orders AS o "
        "GROUP BY o.customer_id",
    ),
    (
        "CREATE TABLE AS SELECT",
        "CREATE TABLE dbo.customer_snapshot AS "
        "SELECT customer_id, name FROM silver_lh.dbo.customers",
    ),
    # 4-part reference into a workspace that is processed *after* this one:
    # only resolvable because all items are indexed before any is processed.
    (
        "SELECT",
        "SELECT r.region_name FROM [Shared Data].[ref_lh].[dbo].[regions] AS r",
    ),
    ("SELECT", "SELECT * FROM missing_lh.dbo.scores"),
]
_INGESTION_LOGIN = "svc-datahub@example.com"
# queryinsights noise that must not produce any output: bodies of the ODBC
# driver's catalog procedures, catalog reads, and the connector's own queries.
_GOLD_NOISE_QUERIES = [
    ("etl@example.com", "OTHER", "set @ODBCVer = 3"),
    ("etl@example.com", "OTHER", "if @data_type = 0"),
    ("etl@example.com", "SELECT", "SELECT name FROM sys.databases"),
    ("etl@example.com", "SELECT", "SELECT * FROM sys.spt_datatype_info_view"),
    (
        _INGESTION_LOGIN,
        "SELECT",
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES",
    ),
    (_INGESTION_LOGIN, "SELECT", "SELECT customer_id FROM dbo.customer_totals"),
]


def _cross_item_tables(workspace_id: str, item_id: str) -> list[FabricTable]:
    return [
        FabricTable(
            name=name, schema_name="dbo", item_id=item_id, workspace_id=workspace_id
        )
        for name, _ in _TABLES.get(item_id, [])
    ]


def _cross_item_schema_map(
    self: FabricOneLakeSource,
    schema_client: object,
    workspace: FabricWorkspace,
    item_id: str,
    item_type: str,
) -> dict[tuple[str, str], list[FabricColumn]]:
    schema_map = {
        ("dbo", name): [
            FabricColumn(name=col, data_type="varchar", is_nullable=True)
            for col in cols
        ]
        for name, cols in _TABLES.get(item_id, [])
    }
    if item_id == "wh-gold":
        for view_name, _, cols in _GOLD_VIEWS:
            schema_map[("dbo", view_name)] = [
                FabricColumn(name=col, data_type="varchar", is_nullable=True)
                for col in cols
            ]
    return schema_map


def _cross_item_schema_client(
    self: FabricOneLakeSource,
    workspace: FabricWorkspace,
    item_id: str,
    item_type: str,
    item_display_name: str,
) -> MagicMock:
    client = MagicMock()
    client.get_all_tables.return_value = _cross_item_tables(workspace.id, item_id)
    if item_id == "wh-gold":
        client.get_all_views.return_value = [
            FabricView(
                name=name,
                schema_name="dbo",
                item_id=item_id,
                workspace_id=workspace.id,
                view_definition=definition,
            )
            for name, definition, _ in _GOLD_VIEWS
        ]
        client.get_current_login.return_value = _INGESTION_LOGIN
        rows = [
            ("etl@example.com", statement_type, command)
            for statement_type, command in _GOLD_QUERIES
        ] + _GOLD_NOISE_QUERIES
        client.stream_usage_history.return_value = iter(
            FabricQueryInsightsRow(
                start_time=FROZEN_TIME - timedelta(hours=2, minutes=i),
                statement_type=statement_type,
                login_name=login_name,
                row_count=10,
                status="Succeeded",
                command=command,
            )
            for i, (login_name, statement_type, command) in enumerate(rows)
        )
    else:
        client.get_all_views.return_value = []
        client.stream_usage_history.return_value = iter([])
    return client


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_fabric_onelake_cross_item_lineage(pytestconfig: pytest.Config) -> None:
    """3-part / 4-part display-name references resolve to GUID-keyed URNs.

    Covers views in a Warehouse reading other items in the same workspace
    (plain and bracketed), a 4-part reference into another ingested workspace,
    an unknown item (dropped, with a warning), and Warehouse INSERT...SELECT /
    CTAS observed queries that read other items (table + column lineage).
    """
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        output_file = tmp.name

    try:
        with (
            patch.object(
                OneLakeClient,
                "list_workspaces",
                return_value=[_ANALYTICS_WS, _SHARED_WS],
            ),
            patch.object(
                OneLakeClient,
                "list_lakehouses",
                side_effect=lambda workspace_id: _LAKEHOUSES[workspace_id],
            ),
            patch.object(
                OneLakeClient,
                "list_warehouses",
                side_effect=lambda workspace_id: _WAREHOUSES[workspace_id],
            ),
            patch.object(
                OneLakeClient,
                "list_lakehouse_tables",
                side_effect=_cross_item_tables,
            ),
            # Warehouse tables come from the SQL endpoint; the REST Tables API
            # is Lakehouse-only and must not be relied on.
            patch.object(
                OneLakeClient,
                "list_warehouse_tables",
                side_effect=AssertionError("REST warehouse tables API called"),
            ),
            patch.object(
                FabricOneLakeSource,
                "_create_schema_client",
                autospec=True,
                side_effect=_cross_item_schema_client,
            ),
            patch.object(
                FabricOneLakeSource,
                "_fetch_schema_map",
                autospec=True,
                side_effect=_cross_item_schema_map,
            ),
        ):
            pipeline = Pipeline.create(
                {
                    "source": {
                        "type": "fabric-onelake",
                        "config": {
                            "credential": {
                                "authentication_method": "service_principal",
                                "client_id": "test-client",
                                "client_secret": "test-secret",
                                "tenant_id": "test-tenant",
                            },
                            "usage": {
                                "include_usage_statistics": True,
                                "include_operational_stats": True,
                            },
                        },
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": output_file},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

            source = pipeline.source
            assert isinstance(source, FabricOneLakeSource)
            # `missing_lh.dbo.scores` (view + query) is one distinct reference.
            assert source.report.num_cross_item_references_unresolved == 1
            assert source.report.num_cross_item_references_resolved > 0
            assert any(
                w.title == "Unresolved Cross-Item SQL Reference"
                for w in source.report.warnings
            )

            # No dangling display-name URNs anywhere in the output (query text
            # still shows the SQL exactly as written).
            emitted_urns = set(
                re.findall(
                    r"urn:li:dataset:\(urn:li:dataPlatform:fabric-onelake,([^,]+),",
                    Path(output_file).read_text(),
                )
            )
            assert emitted_urns
            events = json.loads(Path(output_file).read_text())
            usage_urns = {
                e["entityUrn"]
                for e in events
                if e.get("aspectName") == "datasetUsageStatistics"
            }
            assert (
                "urn:li:dataset:(urn:li:dataPlatform:fabric-onelake,"
                "ws-shared.lh-ref.dbo.regions,PROD)"
            ) in usage_urns
            assert all(
                name.startswith(("ws-analytics.", "ws-shared."))
                for name in emitted_urns
            ), emitted_urns

            # System objects and driver / connector noise produce nothing.
            assert not any(
                ".sys." in name or ".information_schema." in name.lower()
                for name in emitted_urns
            ), emitted_urns
            skipped = source.report.num_usage_queries_skipped
            assert skipped.get("procedural_statement") == 2
            assert skipped.get("ingestion_identity") == 2
            assert source.report.num_system_object_references_filtered == 2
            assert source.report.num_usage_queries_fetched == len(_GOLD_QUERIES) + 2

            golden_path = (
                Path(__file__).parent
                / "golden"
                / "test_fabric_onelake_cross_item_lineage_golden.json"
            )
            mce_helpers.check_golden_file(
                pytestconfig,
                output_path=output_file,
                golden_path=str(golden_path),
            )

    finally:
        Path(output_file).unlink(missing_ok=True)
