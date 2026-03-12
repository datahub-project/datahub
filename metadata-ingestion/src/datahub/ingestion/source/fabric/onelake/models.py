"""Data models for Microsoft Fabric OneLake entities.

References:
- Workspace API: https://learn.microsoft.com/en-us/rest/api/fabric/workspaces
- Items API: https://learn.microsoft.com/en-us/rest/api/fabric/items
- Tables API: https://learn.microsoft.com/en-us/rest/api/fabric/tables
"""

from dataclasses import dataclass
from typing import List, Literal, Optional


@dataclass
class FabricWorkspace:
    """Microsoft Fabric workspace metadata.

    Reference: https://learn.microsoft.com/en-us/rest/api/fabric/workspaces/list
    """

    id: str
    name: str
    description: Optional[str] = None
    type: Optional[str] = None  # Workspace type
    capacity_id: Optional[str] = None


@dataclass
class FabricItem:
    """Base class for Fabric items (lakehouses, warehouses, etc.).

    Reference: https://learn.microsoft.com/en-us/rest/api/fabric/items/list
    """

    id: str
    name: str
    type: Literal["Lakehouse", "Warehouse"]  # Item type
    workspace_id: str
    description: Optional[str] = None
    # TODO: Add support for shortcuts when available in API


@dataclass
class FabricLakehouse(FabricItem):
    """Microsoft Fabric lakehouse metadata.

    Reference: https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-overview
    """

    pass


@dataclass
class FabricWarehouse(FabricItem):
    """Microsoft Fabric warehouse metadata.

    Reference: https://learn.microsoft.com/en-us/fabric/data-warehouse/warehouse-overview
    """

    pass


@dataclass
class FabricTable:
    """Microsoft Fabric table metadata.

    Reference: https://learn.microsoft.com/en-us/rest/api/fabric/tables/list
    """

    name: str
    schema_name: str
    item_id: str  # Lakehouse or Warehouse ID
    workspace_id: str
    description: Optional[str] = None


@dataclass
class FabricColumn:
    """Microsoft Fabric table column metadata.

    Reference: https://learn.microsoft.com/en-us/rest/api/fabric/tables/get-schema
    """

    name: str
    data_type: str
    is_nullable: bool
    ordinal_position: Optional[int] = None
    description: Optional[str] = None


@dataclass
class FabricTableSchema:
    """Complete schema for a Fabric table."""

    table: FabricTable
    columns: List[FabricColumn]
