import base64
import json
import logging
from typing import Dict, Iterator, List, Optional

from datahub.ingestion.source.fabric.common.auth import FabricAuthHelper
from datahub.ingestion.source.fabric.common.base_client import BaseFabricClient
from datahub.ingestion.source.fabric.common.models import (
    FabricConnection,
    FabricItem,
    FabricJobInstance,
    FabricWorkspace,
)
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
            try:
                yield FabricWorkspace(
                    id=workspace_data["id"],
                    name=workspace_data.get("displayName", ""),
                    description=workspace_data.get("description"),
                    type=workspace_data.get("type"),
                    capacity_id=workspace_data.get("capacityId"),
                )
            except KeyError as e:
                self.report.report_parse_failure(
                    f"Skipping malformed workspace: missing required field {e}"
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
            try:
                yield FabricItem(
                    id=item_data["id"],
                    name=item_data.get("displayName", ""),
                    type=item_data.get("type", item_type or ""),
                    workspace_id=workspace_id,
                    description=item_data.get("description"),
                )
            except KeyError as e:
                self.report.report_parse_failure(
                    f"Skipping malformed item in workspace "
                    f"{workspace_id}: missing required field {e}"
                )

    def get_item_definition(
        self, workspace_id: str, item_id: str
    ) -> List[Dict[str, object]]:
        """Get the definition parts for a Fabric item.

        Calls POST /v1/workspaces/{workspaceId}/items/{itemId}/getDefinition
        and decodes each base64 payload in the response.

        Reference: https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item-definition

        Args:
            workspace_id: Workspace GUID
            item_id: Item GUID

        Returns:
            List of dicts, each with 'path' (str) and 'content' (parsed JSON object).
        """
        endpoint = f"workspaces/{workspace_id}/items/{item_id}/getDefinition"
        logger.debug(f"Fetching item definition for {item_id}")

        response = self.post(endpoint)
        data = response.json()

        result: List[Dict[str, object]] = []
        parts = data.get("definition", {}).get("parts", [])
        for part in parts:
            path = part.get("path", "")
            payload_b64 = part.get("payload", "")
            try:
                content = json.loads(base64.b64decode(payload_b64))
            except Exception as e:
                self.report.report_parse_failure(
                    f"Failed to decode definition part '{path}' for item {item_id}: "
                    f"{type(e).__name__}: {e}"
                )
                continue
            result.append({"path": path, "content": content})

        return result

    def list_item_connections(
        self, workspace_id: str, item_id: str
    ) -> Iterator[FabricConnection]:
        """List connections for a specific item.

        Returns only connections bound to the given item.

        Some connections (e.g. Automatic/SSO) may not have an id —
        these are skipped since they cannot be referenced by activities.

        Reference: https://learn.microsoft.com/en-us/rest/api/fabric/core/items/list-item-connections

        Args:
            workspace_id: Workspace GUID
            item_id: Item GUID

        Yields:
            FabricConnection objects (only those with an id)
        """
        endpoint = f"workspaces/{workspace_id}/items/{item_id}/connections"
        logger.debug(f"Listing connections for item {item_id}")
        for raw in self._paginate(endpoint):
            # Skip connections without an id (e.g. Automatic/SSO)
            if not raw.get("id"):
                continue
            try:
                yield FabricConnection.from_dict(raw)
            except KeyError as e:
                self.report.report_parse_failure(
                    f"Skipping malformed item connection for "
                    f"item {item_id}: missing required field {e}"
                )

    def list_item_job_instances(
        self, workspace_id: str, item_id: str
    ) -> Iterator[FabricJobInstance]:
        """List job execution instances for a Fabric item.

        Reference: https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/list-item-job-instances
        Limitations: Most items have a limit of 100 recently completed entities, and there is not limit for active entities.

        Args:
            workspace_id: Workspace GUID
            item_id: Item GUID (pipeline, copy job, dataflow, etc.)

        Yields:
            FabricJobInstance objects (newest first)
        """
        endpoint = f"workspaces/{workspace_id}/items/{item_id}/jobs/instances"
        logger.debug(f"Listing job instances for item {item_id}")
        for data in self._paginate(endpoint):
            try:
                # failureReason is an ErrorResponse object; extract the message
                raw_failure = data.get("failureReason")
                failure_reason = raw_failure.get("message") if raw_failure else None

                yield FabricJobInstance(
                    id=data["id"],
                    item_id=item_id,
                    workspace_id=workspace_id,
                    status=data["status"],
                    start_time_utc=data.get("startTimeUtc"),
                    end_time_utc=data.get("endTimeUtc"),
                    invoke_type=data.get("invokeType"),
                    failure_reason=failure_reason,
                )
            except KeyError as e:
                self.report.report_parse_failure(
                    f"Skipping malformed job instance for item "
                    f"{item_id}: missing required field {e}"
                )
