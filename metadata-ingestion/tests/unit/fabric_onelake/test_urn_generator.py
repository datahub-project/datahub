"""Unit tests for Fabric URN generator.

Tests the centralized URN generation logic to ensure patterns are correct
and can be easily updated in the future.
"""

from datahub.ingestion.source.fabric.common.urn_generator import (
    make_lakehouse_name,
    make_schema_name,
    make_table_name,
    make_warehouse_name,
    make_workspace_name,
)


class TestURNGenerator:
    """Tests for URN generation functions."""

    def test_workspace_name(self) -> None:
        """Test workspace name generation."""
        workspace_id = "workspace-123-guid"
        result = make_workspace_name(workspace_id)
        assert result == workspace_id

    def test_lakehouse_name(self) -> None:
        """Test lakehouse name generation."""
        workspace_id = "workspace-123-guid"
        lakehouse_id = "lakehouse-456-guid"
        result = make_lakehouse_name(workspace_id, lakehouse_id)
        assert result == f"{workspace_id}.{lakehouse_id}"

    def test_warehouse_name(self) -> None:
        """Test warehouse name generation."""
        workspace_id = "workspace-123-guid"
        warehouse_id = "warehouse-789-guid"
        result = make_warehouse_name(workspace_id, warehouse_id)
        assert result == f"{workspace_id}.{warehouse_id}"

    def test_schema_name(self) -> None:
        """Test schema name generation."""
        workspace_id = "workspace-123-guid"
        item_id = "lakehouse-456-guid"
        schema_name = "dbo"
        result = make_schema_name(workspace_id, item_id, schema_name)
        assert result == f"{workspace_id}.{item_id}.{schema_name}"

    def test_table_name(self) -> None:
        """Test table name generation with full pattern."""
        workspace_id = "workspace-123-guid"
        item_id = "lakehouse-456-guid"
        schema_name = "dbo"
        table_name = "customers"
        result = make_table_name(workspace_id, item_id, schema_name, table_name)
        assert result == f"{workspace_id}.{item_id}.{schema_name}.{table_name}"

    def test_table_name_with_different_schema(self) -> None:
        """Test table name with non-default schema."""
        workspace_id = "workspace-123-guid"
        item_id = "warehouse-789-guid"
        schema_name = "sales"
        table_name = "orders"
        result = make_table_name(workspace_id, item_id, schema_name, table_name)
        assert result == f"{workspace_id}.{item_id}.{schema_name}.{table_name}"

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
