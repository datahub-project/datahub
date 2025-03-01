import copy
import logging
import struct
from itertools import chain, repeat
from typing import Dict, Iterable, List, Optional, Union
from urllib import parse

import msal
import requests

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, WorkUnit
from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig
from datahub.ingestion.source.ms_fabric.fabric_utils import set_session
from datahub.ingestion.source.ms_fabric.reporting import AzureFabricSourceReport
from datahub.ingestion.source.ms_fabric.types import Warehouse, Workspace
from datahub.ingestion.source.sql.mssql import SQLServerConfig, SQLServerSource
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfig,
)
from datahub.metadata.schema_classes import ContainerPropertiesClass

logger = logging.getLogger(__name__)


class WarehouseManager:
    azure_config: AzureConnectionConfig
    fabric_session: requests.Session
    warehouse_map: Dict[str, Dict[str, Union[List[Warehouse], Workspace]]]
    ctx: PipelineContext

    def __init__(
        self,
        azure_config: AzureConnectionConfig,
        workspaces: List[Workspace],
        ctx: PipelineContext,
        report: AzureFabricSourceReport,
    ):
        self.azure_config = azure_config
        self.fabric_session = set_session(requests.Session(), azure_config)
        self.warehouse_map = self.get_warhouses(workspaces)
        self.ctx = ctx
        self.report = report

    def get_warehouse_wus(self) -> Iterable[WorkUnit]:
        token_bytes = self._get_sql_server_authentication()

        for _, workspace_data in self.warehouse_map.items():
            workspace: Workspace = workspace_data.get("workspace")
            warehouses: List[Warehouse] = workspace_data.get("warehouses")

            for warehouse in warehouses:
                params = self.create_sql_server_connection_string(
                    warehouse.properties.connection_string, warehouse.display_name
                )

                sql_config = SQLServerConfig(
                    sqlalchemy_uri=f"mssql+pyodbc:///?odbc_connect={params}",
                    use_odbc=True,
                    include_jobs=False,
                    include_stored_procedures=False,
                    platform_instance=workspace.display_name,
                    stateful_ingestion=StatefulIngestionConfig(
                        enabled=True,
                    ),
                    options={"connect_args": {"attrs_before": {1256: token_bytes}}},
                )

                try:
                    ctx_copy = copy.deepcopy(self.ctx)
                    ctx_copy.pipeline_name = f"{workspace.id}.{warehouse.id}"
                    source = SQLServerSource(config=sql_config, ctx=ctx_copy)
                    yield from (
                        MetadataChangeProposalWrapper(
                            entityUrn=wu.metadata.entityUrn,
                            aspect=ContainerPropertiesClass(
                                name=warehouse.display_name
                                if not wu.metadata.aspect.name
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
                        else wu
                        for wu in source.get_workunits()
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to process warehouse {warehouse.properties.connection_string}: {str(e)}"
                    )
                    continue

    def get_warhouses(
        self, workspaces: List[Workspace]
    ) -> Dict[str, Dict[str, Union[List[Warehouse], Workspace]]]:
        warehouses_map: Dict[str, Dict[str, Union[List[Warehouse], Workspace]]] = {}

        for workspace in workspaces:
            warehouses_map[workspace.id] = {
                "workspace": workspace,
                "warehouses": self._get_warehouses_for_workspace(workspace.id),
            }

        return warehouses_map

    def _get_warehouses_for_workspace(
        self, workspace_id: str, continuation_token: Optional[str] = None
    ) -> List[Warehouse]:
        warehouses: List[Warehouse] = []
        params: Dict[str, str] = {}
        url = (
            f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/warehouses"
        )

        if continuation_token:
            params["continuation_token"] = continuation_token

        response = self.fabric_session.get(
            url, headers=self.fabric_session.headers, params=params
        )
        response.raise_for_status()

        response_data = response.json()
        if response_data:
            warehouses_data = response_data.get("value")
            continuation_token = response_data.get("continuationToken")
            if continuation_token:
                warehouses.extend(
                    self._get_warehouses_for_workspace(workspace_id, continuation_token)
                )

            if warehouses_data:
                warehouses.extend(
                    [Warehouse.parse_obj(warehouse) for warehouse in warehouses_data]
                )

        return warehouses

    def _get_sql_server_authentication(self) -> struct:
        authority = f"https://login.microsoftonline.com/{self.azure_config.tenant_id}"
        scope = ["https://database.windows.net/.default"]

        # Authenticate and acquire token
        app = msal.ConfidentialClientApplication(
            self.azure_config.client_id,
            self.azure_config.client_secret,
            authority,
        )

        token_response = app.acquire_token_for_client(scopes=scope)
        token = token_response["access_token"]
        token_as_bytes = bytes(token, "UTF-8")
        encoded_bytes = bytes(chain.from_iterable(zip(token_as_bytes, repeat(0))))
        return struct.pack("<i", len(encoded_bytes)) + encoded_bytes

    def create_sql_server_connection_string(self, server: str, database: str) -> str:
        connection_string = f"driver={{ODBC Driver 18 for SQL Server}};Server={server},1433;Database={database};Encrypt=yes;TrustServerCertificate=Yes;ssl=True"
        return parse.quote(connection_string)
