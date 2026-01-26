"""Unit tests for Fabric URN generator.

Tests the centralized URN generation logic to ensure patterns are correct
and can be easily updated in the future.
"""

from typing import Any, Callable

import pytest

from datahub.ingestion.source.fabric.common.urn_generator import (
    make_lakehouse_name,
    make_schema_name,
    make_table_name,
    make_warehouse_name,
    make_workspace_name,
)


class TestURNGenerator:
    """Tests for URN generation functions."""

    @pytest.mark.parametrize(
        "func,kwargs,expected",
        [
            # Workspace name
            (
                make_workspace_name,
                {"workspace_id": "workspace-123-guid"},
                "workspace-123-guid",
            ),
            # Lakehouse name
            (
                make_lakehouse_name,
                {
                    "workspace_id": "workspace-123-guid",
                    "lakehouse_id": "lakehouse-456-guid",
                },
                "workspace-123-guid.lakehouse-456-guid",
            ),
            # Warehouse name
            (
                make_warehouse_name,
                {
                    "workspace_id": "workspace-123-guid",
                    "warehouse_id": "warehouse-789-guid",
                },
                "workspace-123-guid.warehouse-789-guid",
            ),
            # Schema name
            (
                make_schema_name,
                {
                    "workspace_id": "workspace-123-guid",
                    "item_id": "lakehouse-456-guid",
                    "schema_name": "dbo",
                },
                "workspace-123-guid.lakehouse-456-guid.dbo",
            ),
            # Table name - full pattern with schema
            (
                make_table_name,
                {
                    "workspace_id": "workspace-123-guid",
                    "item_id": "lakehouse-456-guid",
                    "schema_name": "dbo",
                    "table_name": "customers",
                },
                "workspace-123-guid.lakehouse-456-guid.dbo.customers",
            ),
            # Table name - different schema
            (
                make_table_name,
                {
                    "workspace_id": "workspace-123-guid",
                    "item_id": "warehouse-789-guid",
                    "schema_name": "sales",
                    "table_name": "orders",
                },
                "workspace-123-guid.warehouse-789-guid.sales.orders",
            ),
            # Table name - schemas-disabled (defaults to "dbo")
            (
                make_table_name,
                {
                    "workspace_id": "workspace-123-guid",
                    "item_id": "lakehouse-456-guid",
                    "schema_name": "dbo",
                    "table_name": "customers",
                },
                "workspace-123-guid.lakehouse-456-guid.dbo.customers",
            ),
        ],
    )
    def test_urn_generation(
        self, func: Callable[..., str], kwargs: dict[str, Any], expected: str
    ) -> None:
        """Test URN generation for all functions with various configurations."""
        result = func(**kwargs)
        assert result == expected

    def test_urn_pattern_consistency(self) -> None:
        """Test that URN pattern is consistent across all functions."""
        workspace_id = "ws-123"
        item_id = "item-456"
        schema_name = "schema1"
        table_name = "table1"

        # All should use the same separator
        workspace = make_workspace_name(workspace_id)
        lakehouse = make_lakehouse_name(workspace_id, item_id)
        schema = make_schema_name(workspace_id, item_id, schema_name)
        table = make_table_name(workspace_id, item_id, schema_name, table_name)

        # Verify hierarchy
        assert lakehouse.startswith(workspace + ".")
        assert schema.startswith(lakehouse + ".")
        assert table.startswith(schema + ".")
