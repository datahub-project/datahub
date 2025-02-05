import copy
import logging
import struct
from itertools import chain, repeat
from typing import Dict, Iterable, List, Optional, Union
from urllib import parse

import msal
import requests

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, WorkUnit
from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig
from datahub.ingestion.source.delta_lake.config import Azure, DeltaLakeSourceConfig
from datahub.ingestion.source.delta_lake.source import DeltaLakeSource
from datahub.ingestion.source.ms_fabric.constants import LakehouseTableType
from datahub.ingestion.source.ms_fabric.fabric_utils import set_session
from datahub.ingestion.source.ms_fabric.lineage_state import DatasetLineageState
from datahub.ingestion.source.ms_fabric.reporting import AzureFabricSourceReport
from datahub.ingestion.source.ms_fabric.types import (
    Lakehouse,
    LakehouseTable,
    Workspace,
)
from datahub.ingestion.source.sql.mssql.source import SQLServerConfig, SQLServerSource
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfig,
)
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    MetadataChangeEventClass,
    SchemaMetadataClass,
    UpstreamClass,
    UpstreamLineageClass,
)

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
        lineage_state: DatasetLineageState,
    ):
        self.azure_config = azure_config
        self.fabric_session = set_session(requests.Session(), azure_config)
        self.lakehouse_map = self.get_lakehouses(workspaces)
        self.ctx = ctx
        self.report = report
        self.lineage_state = lineage_state

    def get_lakehouse_wus(self) -> Iterable[WorkUnit]:
        for _, workspace_data in self.lakehouse_map.items():
            workspace: Workspace = workspace_data.get("workspace")
            lakehouses: List[Lakehouse] = workspace_data.get("lakehouses")

            for lakehouse in lakehouses:
                delta_lake_locations: List[str] = []
                token_bytes = self._get_sql_server_authentication()

                for table in self._get_lakehouse_tables(workspace.id, lakehouse.id):
                    if table.type == LakehouseTableType.MANAGED:
                        delta_lake_locations.append(table.location)

                # Process Delta Lake tables
                for delta_base_path in set(delta_lake_locations):
                    try:
                        ctx_copy = copy.deepcopy(self.ctx)
                        delta_source = DeltaLakeSource(
                            config=DeltaLakeSourceConfig(
                                azure=Azure(
                                    azure_config=self.azure_config,
                                    use_abs_container_properties=True,
                                    use_abs_blob_properties=True,
                                    use_abs_blob_tags=True,
                                ),
                                base_path=delta_base_path,
                                platform="delta-lake",
                                platform_instance=workspace.display_name,
                            ),
                            ctx=ctx_copy,
                        )

                        for wu in delta_source.get_workunits():
                            # Register Delta tables when we see a schema
                            if isinstance(
                                wu.metadata, MetadataChangeEventClass
                            ) and isinstance(
                                wu.metadata.proposedSnapshot, DatasetSnapshotClass
                            ):
                                table_name = None
                                columns = []

                                for aspect in wu.metadata.proposedSnapshot.aspects:
                                    if isinstance(aspect, DatasetPropertiesClass):
                                        table_name = aspect.name
                                    elif isinstance(aspect, SchemaMetadataClass):
                                        columns = [
                                            field.fieldPath for field in aspect.fields
                                        ]

                                    if table_name and columns:
                                        self.lineage_state.register_dataset(
                                            table_name=table_name,
                                            urn=wu.metadata.proposedSnapshot.urn,
                                            columns=[
                                                self.lineage_state.normalize_delta_column(
                                                    col
                                                )
                                                for col in columns
                                            ],
                                            platform="delta-lake",
                                        )

                                # # Handle container properties
                                # if "containerProperties" in wu.id:
                                #     yield MetadataChangeProposalWrapper(
                                #         entityUrn=wu.metadata.entityUrn,
                                #         aspect=ContainerPropertiesClass(
                                #             name=lakehouse.display_name
                                #             if not wu.metadata.aspect.name
                                #             else wu.metadata.aspect.name,
                                #             customProperties=wu.metadata.aspect.customProperties,
                                #             externalUrl=wu.metadata.aspect.externalUrl,
                                #             qualifiedName=wu.metadata.aspect.qualifiedName,
                                #             description=wu.metadata.aspect.description,
                                #             env=wu.metadata.aspect.env,
                                #             created=wu.metadata.aspect.created,
                                #             lastModified=wu.metadata.aspect.lastModified,
                                #         ),
                                #     ).as_workunit()
                                # else:
                                yield wu

                    except Exception as e:
                        logger.error(f"Failed to process Delta tables: {str(e)}")
                        continue

                # Process SQL Server tables
                try:
                    params = self.create_sql_server_connection_string(
                        lakehouse.properties.sql_endpoint_properties.connection_string,
                        lakehouse.display_name,
                    )

                    ctx_copy = copy.deepcopy(self.ctx)
                    ctx_copy.pipeline_name = f"{workspace.id}.{lakehouse.id}"
                    source = SQLServerSource(
                        config=SQLServerConfig(
                            sqlalchemy_uri=f"mssql+pyodbc:///?odbc_connect={params}",
                            use_odbc=True,
                            include_jobs=False,
                            include_stored_procedures=False,
                            platform_instance=f"{workspace.display_name}",
                            stateful_ingestion=StatefulIngestionConfig(enabled=True),
                            options={
                                "connect_args": {"attrs_before": {1256: token_bytes}}
                            },
                        ),
                        ctx=ctx_copy,
                    )

                    for wu in source.get_workunits():
                        # Register SQL tables when we see a schema
                        if isinstance(
                            wu.metadata, MetadataChangeEventClass
                        ) and isinstance(
                            wu.metadata.proposedSnapshot, DatasetSnapshotClass
                        ):
                            table_name = None
                            columns = []

                            for aspect in wu.metadata.proposedSnapshot.aspects:
                                if isinstance(aspect, DatasetPropertiesClass):
                                    table_name = aspect.name
                                elif isinstance(aspect, SchemaMetadataClass):
                                    columns = [
                                        field.fieldPath for field in aspect.fields
                                    ]

                                if table_name and columns:
                                    self.lineage_state.register_dataset(
                                        table_name=table_name,
                                        urn=wu.metadata.proposedSnapshot.urn,
                                        columns=columns,
                                        platform="mssql",
                                    )

                            # # Handle container properties
                            # if "containerProperties" in wu.id:
                            #     yield MetadataChangeProposalWrapper(
                            #         entityUrn=wu.metadata.entityUrn,
                            #         aspect=ContainerPropertiesClass(
                            #             name=lakehouse.display_name
                            #             if not wu.metadata.aspect.name
                            #             else wu.metadata.aspect.name,
                            #             customProperties=wu.metadata.aspect.customProperties,
                            #             externalUrl=wu.metadata.aspect.externalUrl,
                            #             qualifiedName=wu.metadata.aspect.qualifiedName,
                            #             description=wu.metadata.aspect.description,
                            #             env=wu.metadata.aspect.env,
                            #             created=wu.metadata.aspect.created,
                            #             lastModified=wu.metadata.aspect.lastModified,
                            #         ),
                            #     ).as_workunit()
                            # else:
                            yield wu

                except Exception as e:
                    logger.error(
                        f"Failed to process warehouse {lakehouse.properties.connection_string}: {str(e)}"
                    )
                    continue

                # Generate lineage between Delta and SQL tables for this lakehouse
                yield from self.generate_delta_sql_lineage()

    def generate_delta_sql_lineage(self) -> Iterable[WorkUnit]:
        """Generate lineage work units between Delta and SQL tables"""

        # Get all delta tables
        delta_tables = self.lineage_state.get_upstream_datasets(platform="delta-lake")

        for delta_info in delta_tables:
            # Look for matching SQL tables
            sql_tables = self.lineage_state.get_upstream_datasets(
                table_name=self.lineage_state.normalize_name(delta_info["table_name"]),
                platform="mssql",
            )

            for sql_info in sql_tables:
                logger.info(
                    f"Creating lineage: Delta table {delta_info['table_name']} -> "
                    f"SQL table {sql_info['table_name']}"
                )

                # Create fine-grained column lineage
                fine_grained_lineages = []

                # Find matching columns between delta and sql tables
                for column in delta_info["columns"]:
                    if column in sql_info["columns"]:
                        fine_grained_lineages.append(
                            FineGrainedLineageClass(
                                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                                upstreams=[
                                    make_schema_field_urn(delta_info["urn"], column)
                                ],
                                downstreams=[
                                    make_schema_field_urn(sql_info["urn"], column)
                                ],
                            )
                        )

                # Create lineage with fine-grained column mapping
                upstream_lineage = UpstreamLineageClass(
                    upstreams=[UpstreamClass(dataset=delta_info["urn"], type="COPY")],
                    fineGrainedLineages=fine_grained_lineages
                    if fine_grained_lineages
                    else None,
                )

                mcp = MetadataChangeProposalWrapper(
                    entityUrn=sql_info["urn"], aspect=upstream_lineage
                )

                yield mcp.as_workunit()

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
