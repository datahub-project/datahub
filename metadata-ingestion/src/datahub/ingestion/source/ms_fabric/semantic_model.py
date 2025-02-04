import logging
import re
import urllib
from time import sleep
from typing import Dict, Iterable, List, Optional, Tuple, Union

import requests

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig
from datahub.ingestion.source.ms_fabric.constants import (
    PowerBiSourceTypeMapper,
    SchemaFieldTypeMapper,
)
from datahub.ingestion.source.ms_fabric.fabric_utils import (
    _clean_table_name,
    flatten_dict,
    set_session,
)
from datahub.ingestion.source.ms_fabric.reporting import AzureFabricSourceReport
from datahub.ingestion.source.ms_fabric.tmdl.models.table import (
    Column,
    Table,
    TMDLModel,
)
from datahub.ingestion.source.ms_fabric.tmdl.parser import TMDLParser
from datahub.ingestion.source.ms_fabric.types import (
    SemanticModel,
    SemanticModelContainerKey,
    Workspace,
)
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ContainerClass,
    ContainerPropertiesClass,
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    StatusClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
)

logger = logging.getLogger(__name__)


class SemanticModelManager:
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
        self.processed_datasets = set()

    def get_semantic_model_wus(self) -> Iterable[MetadataWorkUnit]:
        processed_workspaces: List[Workspace] = []
        instance_class: DataPlatformInstanceClass = DataPlatformInstanceClass(
            platform=make_data_platform_urn("powerbi"),
            instance=make_dataplatform_instance_urn(
                platform=make_data_platform_urn("powerbi"),
                instance=self.platform_instance,
            )
            if self.platform_instance
            else None,
        )

        for _, semantic_model_data in self.semantic_model_map.items():
            workspace = semantic_model_data.get("workspace")
            semantic_models = semantic_model_data.get("semantic_model")

            if not isinstance(semantic_models, list):
                continue

            workspace_urn = self.get_container_key(
                name=workspace.id, path=None
            ).as_urn()

            # Emit workspace metadata if not already processed
            if workspace not in processed_workspaces:
                yield MetadataChangeProposalWrapper(
                    entityUrn=workspace_urn,
                    aspect=BrowsePathsV2Class(
                        path=[BrowsePathEntryClass(id="Semantic Models")]
                    ),
                ).as_workunit()

                yield MetadataChangeProposalWrapper(
                    entityUrn=workspace_urn,
                    aspect=ContainerPropertiesClass(
                        name=workspace.display_name,
                        qualifiedName=workspace.display_name,
                        customProperties={
                            "Type": workspace.type.value,
                            "Capacity ID": workspace.capacity_id
                            if workspace.capacity_id
                            else "",
                        },
                        externalUrl=f"https://app.fabric.microsoft.com/groups/{workspace.id}",
                    ),
                ).as_workunit()

                yield MetadataChangeProposalWrapper(
                    entityUrn=workspace_urn,
                    aspect=instance_class,
                ).as_workunit()

                yield MetadataChangeProposalWrapper(
                    entityUrn=workspace_urn,
                    aspect=SubTypesClass(typeNames=["Workspace"]),
                ).as_workunit()

                processed_workspaces.append(workspace)

            # Process each semantic model
            for semantic_model in semantic_models:
                # Create container for semantic model
                container_urn = self.get_container_key(
                    name=semantic_model.id, path=None
                ).as_urn()

                # Emit semantic model container metadata
                yield MetadataChangeProposalWrapper(
                    entityUrn=container_urn,
                    aspect=ContainerPropertiesClass(
                        name=semantic_model.display_name,
                        qualifiedName=f"{workspace.display_name}.{semantic_model.display_name}",
                        customProperties={
                            "Type": semantic_model.type.value,
                            "Capacity ID": workspace.capacity_id
                            if workspace.capacity_id
                            else "",
                        },
                        externalUrl=f"https://app.fabric.microsoft.com/groups/{workspace.id}/datasets/{semantic_model.id}/details"
                        if workspace.capacity_id
                        else None,
                    ),
                ).as_workunit()

                yield MetadataChangeProposalWrapper(
                    entityUrn=container_urn,
                    aspect=ContainerClass(container=workspace_urn),
                ).as_workunit()

                yield MetadataChangeProposalWrapper(
                    entityUrn=container_urn,
                    aspect=instance_class,
                ).as_workunit()

                yield MetadataChangeProposalWrapper(
                    entityUrn=container_urn,
                    aspect=SubTypesClass(typeNames=["Semantic Model"]),
                ).as_workunit()

                # Process TMDL content if available
                if semantic_model.definition and semantic_model.definition.parts:
                    try:
                        # Combine all TMDL parts
                        model_parts = []
                        for part in semantic_model.definition.parts:
                            model_parts.append(part.payload)

                        combined_tmdl = "\n\n".join(model_parts)

                        # Process TMDL content and emit table/column/lineage metadata
                        yield from self._process_tmdl_content(
                            workspace=workspace,
                            semantic_model=semantic_model,
                            tmdl_content=combined_tmdl,
                            container_urn=container_urn,
                        )
                    except Exception as e:
                        self.report.report_warning(
                            message="Failed to process TMDL content for semantic model",
                            context=f"{workspace.display_name}.{semantic_model.id}",
                            exc=e,
                        )

    def _process_tmdl_content(
        self,
        workspace: Workspace,
        semantic_model: SemanticModel,
        tmdl_content: str,
        container_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Process TMDL content and generate workunits for tables and lineage."""
        try:
            logger.debug(
                f"Processing TMDL content for {workspace.display_name}.{semantic_model.display_name}"
            )

            model = self._parse_tmdl_content(tmdl_content)
            logger.error(tmdl_content)
            logger.error(model)

            for table in model.tables:
                if not self._should_process_table(table.name):
                    continue

                dataset_urn = self._create_dataset_urn(workspace, semantic_model, table)

                if self._is_duplicate(dataset_urn):
                    continue

                yield from self._emit_container_relationship(dataset_urn, container_urn)
                yield from self._process_schema_metadata(table, dataset_urn)
                yield from self._emit_dataset_properties(table, dataset_urn)
                yield from self._process_lineage(
                    workspace, semantic_model, table, dataset_urn
                )
                yield from self._emit_status(dataset_urn)

        except Exception as e:
            self.report.report_warning(
                message="Failed to process TMDL content",
                context=f"{workspace.display_name}.{semantic_model.id}",
                exc=e,
            )

    def _parse_tmdl_content(self, tmdl_content: str) -> TMDLModel:
        """Parse TMDL content using the parser."""
        parser = TMDLParser()
        model = parser.parse_raw(tmdl_content)
        return model

    def _create_dataset_urn(
        self, workspace: Workspace, semantic_model: SemanticModel, table: Table
    ) -> str:
        """Create dataset URN for the table."""
        return make_dataset_urn_with_platform_instance(
            platform="powerbi",
            name=self._clean_name(
                f"{workspace.display_name}.{semantic_model.display_name}.{table.name}"
            ),
            platform_instance=self.platform_instance,
            env=self.env,
        )

    def _emit_container_relationship(
        self, dataset_urn: str, container_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        """Emit container relationship workunit."""
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=ContainerClass(container=container_urn),
        ).as_workunit()

    def _create_schema_fields(self, table) -> List[SchemaFieldClass]:
        """Create schema fields from table columns and measures."""
        fields = []

        # Process regular and calculated columns
        for column in table.columns:
            if not column.is_hidden:
                field = self._create_column_field(column)
                fields.append(field)

        # Process measures
        for measure in table.measures:
            if not measure.is_hidden:
                measure_field = self._create_measure_field(measure)
                fields.append(measure_field)

        return fields

    def _create_column_field(self, column: Column) -> SchemaFieldClass:
        """Create a schema field for a column."""
        field = SchemaFieldClass(
            fieldPath=column.name,
            type=SchemaFieldTypeMapper.get_field_type(column.data_type.name),
            description=column.description or "",
            nativeDataType=str(column.data_type.name),
            isPartOfKey=False,
        )

        if column.expression:
            field.description = self._build_column_description(
                field.description, column
            )

        return field

    def _build_column_description(self, base_description: str, column) -> str:
        """Build the complete description for a column including calculation and dependencies."""
        description = base_description
        if column.expression:
            description = (
                f"{description}\n\nCalculation:\n{column.expression.expression}"
                if description
                else f"Calculation:\n{column.expression.expression}"
            )
            if column.lineage_dependencies:
                description += (
                    f"\n\nDependencies:\n{', '.join(column.lineage_dependencies)}"
                )
        return description

    def _create_measure_field(self, measure) -> SchemaFieldClass:
        """Create a schema field for a measure."""
        description = (
            f"{measure.description}\n\nDAX Expression: {measure.expression.expression}"
            if measure.description
            else f"DAX Expression: {measure.expression.expression}"
        )

        if measure.lineage_dependencies:
            description += (
                f"\n\nDependencies:\n{', '.join(measure.lineage_dependencies)}"
            )

        return SchemaFieldClass(
            fieldPath=measure.name,
            type=SchemaFieldTypeMapper.get_field_type("decimal"),
            description=description,
            nativeDataType="measure",
            isPartOfKey=False,
        )

    def _process_schema_metadata(
        self, table: Table, dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        """Process and emit schema metadata."""
        fields = self._create_schema_fields(table)

        if fields:
            schema_metadata = SchemaMetadataClass(
                schemaName=table.name,
                platform=make_data_platform_urn("powerbi"),
                version=0,
                fields=fields,
                platformSchema=OtherSchemaClass(rawSchema=""),
                hash="",
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=schema_metadata
            ).as_workunit()

    def _emit_dataset_properties(
        self, table: Table, dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        """Emit dataset properties workunit."""
        props = {
            **flatten_dict(table.annotations),
            "isHidden": str(table.is_hidden),
        }
        if table.annotations:
            props.update(table.annotations)

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(
                name=table.name,
                description=table.description or "",
                customProperties=props,
            ),
        ).as_workunit()

    def _process_lineage(
        self,
        workspace: Workspace,
        semantic_model: SemanticModel,
        table,
        dataset_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Process and emit lineage information."""
        upstreams = self._process_partition_lineage(table)
        fine_grained_lineages = self._process_column_lineage(
            workspace, semantic_model, table, dataset_urn
        )

        if upstreams or fine_grained_lineages:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineageClass(
                    upstreams=upstreams,
                    fineGrainedLineages=fine_grained_lineages
                    if fine_grained_lineages
                    else None,
                ),
            ).as_workunit()

    def _process_partition_lineage(self, table: Table) -> List[UpstreamClass]:
        """Process partition source lineage."""
        upstreams = []
        logger.error(table)
        for partition in table.partitions:
            logger.error(partition)
            if partition.source and partition.source.expression:
                source_tables = self._extract_source_tables(partition.source.expression)
                for source_table in source_tables:
                    source_platform = PowerBiSourceTypeMapper.get_source_type(
                        partition.source.expression
                    )
                    source_urn = make_dataset_urn_with_platform_instance(
                        platform=source_platform,
                        name=source_table,
                        platform_instance=self.platform_instance,
                        env=self.env,
                    )
                    upstreams.append(
                        UpstreamClass(
                            dataset=source_urn,
                            type=DatasetLineageTypeClass.TRANSFORMED,
                        )
                    )
        return upstreams

    def _process_column_lineage(
        self,
        workspace: Workspace,
        semantic_model: SemanticModel,
        table: Table,
        dataset_urn: str,
    ) -> List[FineGrainedLineageClass]:
        """Process column-level lineage."""
        fine_grained_lineages = []
        for column in table.columns:
            if column.expression and column.lineage_dependencies:
                source_refs = self._extract_column_references(
                    column.expression.expression
                )
                for source_table, source_column in source_refs:
                    source_urn = make_dataset_urn_with_platform_instance(
                        platform="powerbi",
                        name=self._clean_name(
                            f"{workspace.display_name}.{semantic_model.display_name}.{source_table}"
                        ),
                        platform_instance=self.platform_instance,
                        env=self.env,
                    )
                    fine_grained_lineages.append(
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                            upstreams=[
                                make_schema_field_urn(source_urn, source_column)
                            ],
                            downstreams=[
                                make_schema_field_urn(dataset_urn, column.name)
                            ],
                            transformOperation=column.expression.expression,
                        )
                    )
        return fine_grained_lineages

    def _emit_status(self, dataset_urn: str) -> Iterable[MetadataWorkUnit]:
        """Emit status workunit."""
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

    def _extract_column_references(self, formula: str) -> List[Tuple[str, str]]:
        """Extract table and column references from a DAX formula"""
        refs = []
        # Match patterns like 'Table'[Column] or [Column]
        for match in re.finditer(r"(?:\'([^\']+)\'|(\w+))?\[([^\]]+)\]", formula):
            table_name = match.group(1) or match.group(2) or ""
            column_name = match.group(3)
            if table_name and column_name:
                refs.append((table_name, column_name))
        return refs

    def _should_process_table(self, table_name: str) -> bool:
        """Filter out internal PowerBI tables and known system tables"""
        internal_patterns = [
            "DateTableTemplate_",
            "LocalDateTable_",
            "System_",
            "_System",
            "_RS_",
            "_Time_",
            "_Date_",
        ]
        return not any(pattern in table_name for pattern in internal_patterns)

    def _is_duplicate(self, dataset_urn: str) -> bool:
        """Check if dataset has already been processed"""
        if dataset_urn in self.processed_datasets:
            return True
        self.processed_datasets.add(dataset_urn)
        return False

    def _extract_source_tables(self, source_query: str) -> List[str]:
        """Extract source tables from a query"""
        tables = []
        try:
            # Extract SQL database references
            sql_db_pattern = r'Sql\.Database\s*\(\s*"([^"]+)"'
            sql_db_matches = re.finditer(sql_db_pattern, source_query)
            for match in sql_db_matches:
                db_server = match.group(1)
                if ".datawarehouse.fabric.microsoft.com" in db_server:
                    # This is a Fabric SQL warehouse
                    tables.append(f"fabric-sql://{db_server}")
                else:
                    # Regular SQL server
                    tables.append(f"sql://{db_server}")

            # Extract standard database/schema/table patterns
            db_match = re.search(r'Database\s*\(\s*"([^"]+)"', source_query)
            schema_match = re.search(
                r'\[Name\s*=\s*"([^"]+)".*Kind\s*=\s*"Schema"', source_query
            )
            table_match = re.search(
                r'\[Name\s*=\s*"([^"]+)".*Kind\s*=\s*"(?:Table|View)"', source_query
            )

            if all([db_match, schema_match, table_match]):
                tables.append(
                    f"{db_match.group(1)}.{schema_match.group(1)}.{table_match.group(1)}"
                )
            elif schema_match and table_match:
                tables.append(f"{schema_match.group(1)}.{table_match.group(1)}")
            elif table_match:
                tables.append(table_match.group(1))

            # Clean up any table names with brackets
            tables = [_clean_table_name(table) for table in tables]

        except Exception as e:
            logger.warning(f"Error extracting source tables: {str(e)}")

        return tables

    def get_semantic_models(
        self, workspaces: List[Workspace]
    ) -> Dict[str, Dict[str, Union[SemanticModel, Workspace]]]:
        """Get semantic models for all workspaces"""
        semantic_model_map = {}

        for workspace in workspaces:
            semantic_model_map[workspace.id] = {
                "workspace": workspace,
                "semantic_model": self._get_semantic_models_for_workspace(workspace.id),
            }

        return semantic_model_map

    def _get_semantic_models_for_workspace(
        self, workspace_id: str, continuation_token: Optional[str] = None
    ) -> List[SemanticModel]:
        """Get semantic models for a specific workspace"""
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

    def _clean_name(self, name: str) -> str:
        """Clean a name by handling URL encoding and special characters"""
        name = urllib.parse.unquote(name)  # Handle %282%29 -> (2)
        return name.strip()

    def _get_semantic_model_definition(
        self, workspace_id: str, semantic_model: Dict[str, str]
    ) -> Dict[str, Union[Dict[str, List[Dict[str, str]]], str]]:
        """Get the definition of a semantic model"""
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
                sleep(1)
        elif response.status_code == 429:
            self.report.report_failure(
                message="Access denied for semantic model",
                context=semantic_model_id,
            )
            return semantic_model

        response.raise_for_status()
        return {**semantic_model, **response.json()}

    def get_container_key(
        self, name: Optional[str], path: Optional[List[str]]
    ) -> SemanticModelContainerKey:
        key = name
        if path:
            key = ".".join(path) + "." + name if name else ".".join(path)

        return SemanticModelContainerKey(
            platform="powerbi",
            instance=self.platform_instance,
            env=self.env,
            key=key,
        )
