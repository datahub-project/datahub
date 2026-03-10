import logging
from typing import Iterator, Optional

from datahub.ingestion.source.fabric.common.auth import FabricAuthHelper
from datahub.ingestion.source.fabric.common.base_client import BaseFabricClient
from datahub.ingestion.source.fabric.common.models import FabricItem, FabricWorkspace
from datahub.ingestion.source.fabric.common.report import FabricClientReport

logger = logging.getLogger(__name__)


class FabricCoreClient(BaseFabricClient):
    """Client for the Microsoft Fabric Core API.

    Wraps the common Fabric REST APIs used across the entire Fabric ecosystem —
    workspaces, items, and containers. All workload-specific clients can
    (OneLake, Data Factory, etc.) extend this class.
    """

    def __init__(
        self,
        auth_helper: FabricAuthHelper,
        timeout: int = 30,
        report: Optional[FabricClientReport] = None,
    ):
        super().__init__(auth_helper, timeout, report)

    def get_base_endpoint(self) -> str:
        return "workspaces"

    def list_workspaces(self) -> Iterator[FabricWorkspace]:
        """List all accessible Fabric workspaces.

        Reference: https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/list-workspaces

        Yields:
            FabricWorkspace objects
        """
        logger.info("Listing Fabric workspaces")
        for workspace_data in self._list_workspaces_raw():
            yield FabricWorkspace(
                id=workspace_data.get("id", ""),
                name=workspace_data.get("displayName", ""),
                description=workspace_data.get("description"),
                type=workspace_data.get("type"),
                capacity_id=workspace_data.get("capacityId"),
            )

    def list_items(
        self, workspace_id: str, item_type: Optional[str] = None
    ) -> Iterator[FabricItem]:
        """List items in a workspace, optionally filtered by type.

        Reference: https://learn.microsoft.com/en-us/rest/api/fabric/core/items/list-items

        Args:
            workspace_id: Workspace GUID
            item_type: Optional item type filter (e.g. "DataPipeline", "CopyJob",
                       "Dataflow", "Lakehouse", "Warehouse"). Pass None to list all.

        Yields:
            FabricItem objects
        """
        log_type = item_type or "all"
        logger.info(f"Listing {log_type} items in workspace {workspace_id}")
        params = {"type": item_type} if item_type else {}
        for item_data in self._paginate(
            f"workspaces/{workspace_id}/items", params=params
        ):
            yield FabricItem(
                id=item_data.get("id", ""),
                name=item_data.get("displayName", ""),
                type=item_data.get("type", item_type or ""),
                workspace_id=workspace_id,
                description=item_data.get("description"),
            )
