"""Integration tests for Fabric OneLake source.

These tests use mocked REST API responses to verify the full ingestion pipeline
produces the expected metadata events.
"""

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.fabric.onelake.client import OneLakeClient
from datahub.ingestion.source.fabric.onelake.models import (
    FabricColumn,
    FabricLakehouse,
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
