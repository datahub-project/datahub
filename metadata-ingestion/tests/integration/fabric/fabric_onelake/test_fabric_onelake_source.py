"""Integration tests for Fabric OneLake source.

These tests use mocked REST API responses to verify the full ingestion pipeline
produces the expected metadata events.
"""

import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.fabric.onelake.client import OneLakeClient
from datahub.ingestion.source.fabric.onelake.models import (
    FabricColumn,
    FabricLakehouse,
    FabricQueryInsightsRow,
    FabricTable,
    FabricView,
    FabricWarehouse,
    FabricWorkspace,
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
        ("dbo", "v_total_orders"): [
            FabricColumn(name="order_id", data_type="int", is_nullable=False),
            FabricColumn(name="total", data_type="decimal", is_nullable=True),
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
            patch.object(OneLakeClient, "list_warehouse_tables", return_value=[]),
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
