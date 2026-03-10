"""REST API client for Microsoft Fabric Data Factory items."""

import logging
from typing import Optional

from datahub.ingestion.source.fabric.common.auth import FabricAuthHelper
from datahub.ingestion.source.fabric.common.core_client import FabricCoreClient
from datahub.ingestion.source.fabric.common.report import FabricClientReport

logger = logging.getLogger(__name__)


class FabricDataFactoryClient(FabricCoreClient):
    """Client for Microsoft Fabric Data Factory.

    Inherits workspace and item listing from FabricCoreClient.
    Use list_workspaces() and list_items(workspace_id, item_type) directly.

    Supported item types: "DataPipeline", "CopyJob", "Dataflow"
    """

    def __init__(
        self,
        auth_helper: FabricAuthHelper,
        timeout: int = 30,
        report: Optional[FabricClientReport] = None,
    ):
        super().__init__(auth_helper, timeout, report)
