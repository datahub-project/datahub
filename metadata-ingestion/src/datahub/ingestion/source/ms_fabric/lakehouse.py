import copy
import logging
from typing import Dict, Iterable, List, Optional, Union

import requests

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, WorkUnit
from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig
from datahub.ingestion.source.delta_lake.config import Azure, DeltaLakeSourceConfig
from datahub.ingestion.source.delta_lake.source import DeltaLakeSource
from datahub.ingestion.source.ms_fabric.constants import LakehouseTableType
from datahub.ingestion.source.ms_fabric.fabric_utils import set_session
from datahub.ingestion.source.ms_fabric.reporting import AzureFabricSourceReport
from datahub.ingestion.source.ms_fabric.types import (
    Lakehouse,
    LakehouseTable,
    Workspace,
)
from datahub.metadata.schema_classes import ContainerPropertiesClass

logger = logging.getLogger(__name__)


class LakehouseManager:
    azure_config: AzureConnectionConfig
    fabric_session: requests.Session
    lakehouse_map: Dict[str, Dict[str, Union[List[Lakehouse], Workspace]]]
    ctx: PipelineContext
    report: AzureFabricSourceReport

    def __init__(
        self,
        azure_config: AzureConnectionConfig,
        workspaces: List[Workspace],
        ctx: PipelineContext,
        report: AzureFabricSourceReport,
    ):
        self.azure_config = azure_config
        self.fabric_session = set_session(requests.Session(), azure_config)
        self.lakehouse_map = self.get_lakehouses(workspaces)
        self.ctx = ctx
        self.report = report

    def get_lakehouse_wus(self) -> Iterable[WorkUnit]:
        for _, workspace_data in self.lakehouse_map.items():
            workspace: Workspace = workspace_data.get("workspace")
            lakehouses: List[Lakehouse] = workspace_data.get("lakehouses")

            for lakehouse in lakehouses:
                delta_lake_locations: List[str] = []

                for table in self._get_lakehouse_tables(workspace.id, lakehouse.id):
                    if table.type == LakehouseTableType.MANAGED:
                        delta_lake_locations.append(table.location)

                for delta_base_path in set(delta_lake_locations):
                    logger.debug(f"Processing delta base path: {delta_base_path}")

                    azure = Azure(
                        azure_config=self.azure_config,
                        use_abs_container_properties=True,
                        use_abs_blob_properties=True,
                        use_abs_blob_tags=True,
                    )

                    delta_config = DeltaLakeSourceConfig(
                        azure=azure,
                        base_path=delta_base_path,
                        platform="delta-lake",
                        platform_instance=workspace.display_name,
                    )

                    try:
                        ctx_copy = copy.deepcopy(self.ctx)
                        delta_source = DeltaLakeSource(
                            config=delta_config, ctx=ctx_copy
                        )
                        yield from (
                            MetadataChangeProposalWrapper(
                                entityUrn=wu.metadata.entityUrn,
                                aspect=ContainerPropertiesClass(
                                    name=lakehouse.display_name
                                    if wu.metadata.aspect.name == lakehouse.id
                                    else workspace.display_name
                                    if wu.metadata.aspect.name == workspace.id
                                    else wu.metadata.aspect.name,
                                    customProperties=wu.metadata.aspect.customProperties,
                                    externalUrl=wu.metadata.aspect.externalUrl,
                                    qualifiedName=wu.metadata.aspect.qualifiedName,
                                    description=wu.metadata.aspect.description,
                                    env=wu.metadata.aspect.env,
                                    created=wu.metadata.aspect.created,
                                    lastModified=wu.metadata.aspect.lastModified,
                                ),
                            ).as_workunit()
                            if "containerProperties" in wu.id
                            and hasattr(wu.metadata, "aspect")
                            and isinstance(wu.metadata.aspect, ContainerPropertiesClass)
                            else wu
                            for wu in delta_source.get_workunits()
                        )
                    except Exception as e:
                        logger.error(
                            f"Failed to process lakehouse {lakehouse.id} at path {delta_base_path}: {str(e)}"
                        )
                        continue

    def get_lakehouses(
        self, workspaces: List[Workspace]
    ) -> Dict[str, Dict[str, Union[Lakehouse, Workspace]]]:
        lakehouses_map: Dict[str, Dict[str, Union[List[Lakehouse], Workspace]]] = {}

        for workspace in workspaces:
            lakehouses_map[workspace.id] = {
                "workspace": workspace,
                "lakehouses": self._get_lakehouses_for_workspace(workspace.id),
            }

        return lakehouses_map

    def _get_lakehouses_for_workspace(
        self, workspace_id: str, continuation_token: Optional[str] = None
    ) -> List[Lakehouse]:
        lakehouses: List[Lakehouse] = []
        params: Dict[str, str] = {}
        url = (
            f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
        )

        if continuation_token:
            params["continuation_token"] = continuation_token

        response = self.fabric_session.get(
            url, headers=self.fabric_session.headers, params=params
        )
        response.raise_for_status()

        response_data = response.json()
        if response_data:
            lakehouses_data = response_data.get("value")
            continuation_token = response_data.get("continuationToken")
            if continuation_token:
                lakehouses.extend(
                    self._get_lakehouses_for_workspace(workspace_id, continuation_token)
                )

            if lakehouses_data:
                lakehouses.extend(
                    [Lakehouse.parse_obj(lakehouse) for lakehouse in lakehouses_data]
                )

        return lakehouses

    def _get_lakehouse_tables(
        self,
        workspace_id: str,
        lakehouse_id: str,
        continuation_token: Optional[str] = None,
    ) -> List[LakehouseTable]:
        lakehouse_tables: List[LakehouseTable] = []
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables"
        params: Dict[str, str] = {"maxResults": 100}

        if continuation_token:
            params["continuation_token"] = continuation_token

        response = self.fabric_session.get(
            url, headers=self.fabric_session.headers, params=params
        )
        response_data = response.json()

        if "errorCode" not in response_data:
            lakehouse_tables_data = response_data.get("data")
            continuation_token = response_data.get("continuationToken")

            if continuation_token:
                lakehouse_tables.extend(
                    self._get_lakehouse_tables(
                        workspace_id, lakehouse_id, continuation_token
                    )
                )

            if lakehouse_tables_data:
                lakehouse_tables.extend(
                    [LakehouseTable.parse_obj(table) for table in lakehouse_tables_data]
                )
        else:
            error_text = response_data.get("message")
            logger.error(error_text)
            self.report.report_warning(
                message="Unable to extract tables for lakehouse",
                context=lakehouse_id,
                exc=BaseException(error_text),
            )

        return lakehouse_tables
