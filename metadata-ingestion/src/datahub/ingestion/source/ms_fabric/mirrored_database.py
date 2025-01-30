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
from datahub.ingestion.source.ms_fabric.types import MirroredDatabase, Workspace
from datahub.ingestion.source.sql.mssql import SQLServerConfig, SQLServerSource
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfig,
)
from datahub.metadata.schema_classes import ContainerPropertiesClass

logger = logging.getLogger(__name__)


class MirroredDatabasesManager:
    azure_config: AzureConnectionConfig
    fabric_session: requests.Session
    mirrored_databases_map: Dict[str, Dict[str, Union[MirroredDatabase, Workspace]]]
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
        self.mirrored_databases_map = self.get_mirrored_databases(workspaces)
        self.ctx = ctx
        self.report = report

    def get_mirrored_databases_wus(self) -> Iterable[WorkUnit]:
        token_bytes = self._get_sql_server_authentication()

        for _, workspace_data in self.mirrored_databases_map.items():
            workspace = workspace_data.get("workspace")
            mirrored_databases = workspace_data.get("mirrored_databases")

            for mirrored_database in mirrored_databases:
                params = self.create_sql_server_connection_string(
                    mirrored_database.properties.sql_endpoint_properties.connection_string
                )

                sql_config = SQLServerConfig(
                    sqlalchemy_uri=f"mssql+pyodbc:///?odbc_connect={params}",
                    use_odbc=True,
                    include_jobs=False,
                    include_stored_procedures=False,
                    platform_instance=f"{workspace.display_name}: {mirrored_database.display_name}",
                    stateful_ingestion=StatefulIngestionConfig(
                        enabled=True,
                    ),
                    options={"connect_args": {"attrs_before": {1256: token_bytes}}},
                )

                try:
                    ctx_copy = copy.deepcopy(self.ctx)
                    source = SQLServerSource(config=sql_config, ctx=ctx_copy)
                    yield from (
                        MetadataChangeProposalWrapper(
                            entityUrn=wu.metadata.entityUrn,
                            aspect=ContainerPropertiesClass(
                                name=mirrored_database.display_name
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
                        f"Failed to process mirrored database {mirrored_databases.properties.connection_string}: {str(e)}"
                    )
                    continue

    def get_mirrored_databases(
        self, workspaces: List[Workspace]
    ) -> Dict[str, Dict[str, Union[MirroredDatabase, Workspace]]]:
        mirrored_databases_map: Dict[
            str, Dict[str, Union[MirroredDatabase, Workspace]]
        ] = {}

        for workspace in workspaces:
            mirrored_databases_map[workspace.id] = {
                "workspace": workspace,
                "mirrored_databases": self._get_mirrored_databases_for_workspace(
                    workspace.id
                ),
            }

        return mirrored_databases_map

    def _get_mirrored_databases_for_workspace(
        self, workspace_id: str, continuation_token: Optional[str] = None
    ) -> List[MirroredDatabase]:
        mirrored_databases: List[MirroredDatabase] = []
        params: Dict[str, str] = {}
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/mirroredDatabases"

        if continuation_token:
            params["continuation_token"] = continuation_token

        response = self.fabric_session.get(
            url, headers=self.fabric_session.headers, params=params
        )
        response.raise_for_status()

        response_data = response.json()
        if response_data:
            mirrored_databases_data = response_data.get("value")
            continuation_token = response_data.get("continuationToken")
            if continuation_token:
                mirrored_databases.extend(
                    self._get_mirrored_databases_for_workspace(
                        workspace_id, continuation_token
                    )
                )

            if mirrored_databases_data:
                mirrored_databases.extend(
                    [
                        MirroredDatabase.parse_obj(mirrored_database)
                        for mirrored_database in mirrored_databases_data
                    ]
                )

        return mirrored_databases

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

    def create_sql_server_connection_string(self, server: str) -> str:
        connection_string = (
            f"driver={{ODBC Driver 18 for SQL Server}};Server={server},1433;"
        )
        return parse.quote(connection_string)
