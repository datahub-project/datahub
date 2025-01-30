import logging
from time import sleep
from typing import Dict, Iterable, List, Optional, Union

import requests

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig
from datahub.ingestion.source.ms_fabric.fabric_utils import (
    TmdlParser,
    _map_powerbi_type_to_datahub_type,
    _parse_m_query,
    set_session,
)
from datahub.ingestion.source.ms_fabric.reporting import AzureFabricSourceReport
from datahub.ingestion.source.ms_fabric.types import (
    SemanticModel,
    Warehouse,
    Workspace,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    UpstreamClass,
    UpstreamLineageClass,
)

logger = logging.getLogger(__name__)


class SemanticModelManager:
    fabric_session: requests.Session
    warehouse_map: Dict[str, Dict[str, Union[SemanticModel, Workspace]]]
    platform_instance: str
    ctx: PipelineContext
    report: AzureFabricSourceReport

    def __init__(
        self,
        azure_config: AzureConnectionConfig,
        workspaces: List[Workspace],
        platform_instance: str,
        env: str,
        ctx: PipelineContext,
        report: AzureFabricSourceReport,
    ):
        self.fabric_session = set_session(requests.Session(), azure_config)
        self.semantic_model_map = self.get_semantic_models(workspaces)
        self.platform_instance = platform_instance
        self.env = env
        self.ctx = ctx
        self.report = report

    def get_semantic_model_wus(self) -> Iterable[MetadataWorkUnit]:
        for _, semantic_model_data in self.semantic_model_map.items():
            workspace = semantic_model_data.get("workspace")
            semantic_models = semantic_model_data.get("semantic_model")

            if not isinstance(semantic_models, list):
                continue

            for semantic_model in semantic_models:
                if semantic_model.definition and semantic_model.definition.parts:
                    for part in semantic_model.definition.parts:
                        yield from self._process_tmdl_content(
                            workspace_display_name=workspace.display_name,
                            semantic_model=semantic_model,
                            tmdl_content=part.payload,
                        )

    def _process_tmdl_content(
        self,
        workspace_display_name: str,
        semantic_model: SemanticModel,
        tmdl_content: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Process TMDL content and generate workunits for tables and lineage"""
        try:
            # Parse TMDL content
            tmdl_parser = TmdlParser(tmdl_content)
            table = tmdl_parser.parse_table()

            # Create dataset URN
            dataset_urn = make_dataset_urn_with_platform_instance(
                platform="powerbi",
                name=f"{workspace_display_name}.{semantic_model.display_name}.{table.name}",
                platform_instance=self.platform_instance,
                env=self.env,
            )

            # Create schema fields with enhanced metadata
            fields = []
            for column in table.columns:
                logger.error(column)
                # Get calculation info if it exists
                calculation_info = None
                if "formula" in column.annotations:
                    calculation_info = {
                        "formula": column.annotations["formula"],
                        "referencedColumns": column.annotations.get(
                            "referencedColumns", ""
                        ).split(","),
                        "isCalculated": column.annotations.get("isCalculated", "false")
                        == "true",
                    }

                # Create description that includes calculation if present
                description = column.annotations.get("description", "")
                if calculation_info and calculation_info["isCalculated"]:
                    description = f"{description}\nCalculation: {calculation_info['formula']}\nReferenced columns: {', '.join(calculation_info['referencedColumns'])}"

                fields.append(
                    SchemaFieldClass(
                        fieldPath=column.name,
                        type=SchemaFieldDataTypeClass(
                            type=_map_powerbi_type_to_datahub_type(column.data_type)
                        ),
                        description=description,
                        nativeDataType=column.source_provider_type,
                        isPartOfKey=False,
                        globalTags=None,
                        glossaryTerms=None,
                    )
                )

            schema_props = SchemaMetadataClass(
                schemaName=table.name,
                platform=make_data_platform_urn("powerbi"),
                version=0,
                platformSchema=OtherSchemaClass(""),
                hash="",
                fields=fields,
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=schema_props
            ).as_workunit()

            # Create dataset properties
            dataset_props = DatasetPropertiesClass(
                name=table.name,
                description=table.annotations.get("Description", ""),
                customProperties={
                    "lineageTag": table.lineage_tag,
                    "semanticModelId": semantic_model.id,
                    "semanticModelName": semantic_model.display_name,
                    **table.annotations,
                },
            )

            # Emit dataset workunit
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=dataset_props
            ).as_workunit()

            # Extract and emit lineage
            source_tables = []
            for partition in table.partitions:
                if partition.source_query:
                    source_tables.extend(_parse_m_query(partition.source_query))

            if source_tables:
                upstreams = []
                for source_table in source_tables:
                    source_urn = make_dataset_urn_with_platform_instance(
                        platform="powerbi",
                        name=source_table,
                        platform_instance=self.platform_instance,
                        env=self.env,
                    )
                    upstreams.append(
                        UpstreamClass(
                            dataset=source_urn, type=DatasetLineageTypeClass.TRANSFORMED
                        )
                    )

                lineage_aspect = UpstreamLineageClass(upstreams=upstreams)
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn, aspect=lineage_aspect
                ).as_workunit()

                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn, aspect=StatusClass(removed=False)
                ).as_workunit()

        except Exception as e:
            self.report.report_warning(
                message="Failed to process TMDL content",
                context=f"{workspace_display_name}.{semantic_model.id}",
                exc=e,
            )

    def get_semantic_models(
        self, workspaces: List[Workspace]
    ) -> Dict[str, Dict[str, Union[SemanticModel, Workspace]]]:
        semantic_model_map: Dict[str, Dict[str, Union[Warehouse, Workspace]]] = {}

        for workspace in workspaces:
            semantic_model_map[workspace.id] = {
                "workspace": workspace,
                "semantic_model": self._get_semantic_models_for_workspace(workspace.id),
            }

        return semantic_model_map

    def _get_semantic_models_for_workspace(
        self, workspace_id: str, continuation_token: Optional[str] = None
    ) -> List[SemanticModel]:
        semantic_models: List[SemanticModel] = []
        params: Dict[str, str] = {}
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/semanticModels"

        if continuation_token:
            params["continuation_token"] = continuation_token

        response = self.fabric_session.get(
            url, headers=self.fabric_session.headers, params=params
        )
        response.raise_for_status()

        response_data = response.json()
        if response_data:
            semantic_models_data = response_data.get("value")
            continuation_token = response_data.get("continuationToken")
            if continuation_token:
                semantic_models.extend(
                    self._get_semantic_models_for_workspace(
                        workspace_id, continuation_token
                    )
                )

            if semantic_models_data:
                semantic_models.extend(
                    [
                        SemanticModel.parse_obj(
                            self._get_semantic_model_definition(
                                workspace_id, semantic_model
                            )
                        )
                        for semantic_model in semantic_models_data
                    ]
                )

        return semantic_models

    def _get_semantic_model_definition(
        self, workspace_id: str, semantic_model: Dict[str, str]
    ) -> Dict[str, Union[Dict[str, List[Dict[str, str]]], str]]:
        semantic_model_id = semantic_model.get("id")
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/semanticModels/{semantic_model_id}/getDefinition"
        response = self.fabric_session.post(url, headers=self.fabric_session.headers)

        if response.status_code == 202:
            # Get operation ID from headers
            operation_id = response.headers.get("x-ms-operation-id")
            if not operation_id and "Location" in response.headers:
                operation_id = response.headers["Location"].split("/")[-1]

            while True:
                # Check operation status
                status_url = (
                    f"https://api.fabric.microsoft.com/v1/operations/{operation_id}"
                )
                status_response = self.fabric_session.get(
                    status_url, headers=self.fabric_session.headers
                )
                status_response.raise_for_status()
                status = status_response.json()["status"]

                if status == "Succeeded":
                    # Get the results
                    results_url = f"https://api.fabric.microsoft.com/v1/operations/{operation_id}/result"
                    response = self.fabric_session.get(
                        results_url, headers=self.fabric_session.headers
                    )
                    response.raise_for_status()
                    break
                elif status == "Failed":
                    raise Exception(f"Operation failed: {status_response.json()}")
                sleep(0.5)
        elif response.status_code == 429:
            self.report.report_failure(
                message="Access denied for semantic model",
                context=semantic_model_id,
            )
            return semantic_model

        response.raise_for_status()
        semantic_model = {**semantic_model, **response.json()}

        return semantic_model
