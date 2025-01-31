import logging
import re
import urllib
from time import sleep
from typing import Dict, Iterable, List, Optional, Set, Union

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
    TmdlParser,
    _clean_table_name,
    set_session,
)
from datahub.ingestion.source.ms_fabric.reporting import AzureFabricSourceReport
from datahub.ingestion.source.ms_fabric.types import (
    SemanticModel,
    SemanticModelContainerKey,
    TmdlColumn,
    TmdlMeasure,
    TmdlTable,
    Workspace,
)
from datahub.metadata._schema_classes import SubTypesClass
from datahub.metadata.schema_classes import (
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

            if workspace not in processed_workspaces:
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

            for semantic_model in semantic_models:
                container_urn = self.get_container_key(
                    name=semantic_model.id, path=None
                ).as_urn()

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

                if semantic_model.definition and semantic_model.definition.parts:
                    model_parts = []
                    logger.error(semantic_model)
                    for part in semantic_model.definition.parts:
                        model_parts.append(part.payload)

                    # Combine all parts together for complete model analysis
                    combined_tmdl = "\n\n".join(model_parts)
                    yield from self._process_tmdl_content(
                        workspace=workspace,
                        semantic_model=semantic_model,
                        tmdl_content=combined_tmdl,
                        container_urn=container_urn,
                    )

            processed_workspaces.append(workspace)

    def _process_tmdl_content(
        self,
        workspace: Workspace,
        semantic_model: SemanticModel,
        tmdl_content: str,
        container_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Process TMDL content and generate workunits for tables and lineage"""
        try:
            logger.debug(
                f"Processing TMDL content for {workspace.display_name}.{semantic_model.display_name}:\n{tmdl_content}"
            )
            tmdl_parser = TmdlParser(tmdl_content)
            logger.error(tmdl_content)
            model = tmdl_parser.parse()

            # Process each table in the model
            for table in model.tables:
                if not self._should_process_table(table.name):
                    continue

                # Create dataset URN
                dataset_urn = make_dataset_urn_with_platform_instance(
                    platform="powerbi",
                    name=self._clean_name(
                        f"{workspace.display_name}.{semantic_model.display_name}.{table.name}"
                    ),
                    platform_instance=self.platform_instance,
                    env=self.env,
                )

                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=ContainerClass(
                        container=container_urn,
                    ),
                ).as_workunit()

                if self._is_duplicate(dataset_urn):
                    continue

                # Create schema fields
                fields = []
                field_lineage = {}

                # Process columns
                for column in table.columns:
                    field = self._create_schema_field(column)
                    fields.append(field)

                    # Track column lineage
                    if column.source_lineage_tag or column.source_column:
                        field_lineage[column.name] = {
                            "source_column": column.source_column,
                            "source_lineage": column.source_lineage_tag,
                            "source_ref": column.annotations.get(
                                "sourceLineageTag", ""
                            ),
                        }

                # Process measures
                if hasattr(table, "measures"):
                    for measure in table.measures:
                        measure_field = self._create_measure_field(measure)
                        fields.append(measure_field)
                        # Add measure dependencies to lineage
                        if measure.formula:
                            refs = self._extract_referenced_columns(measure.formula)
                            if refs:
                                field_lineage[measure.name] = {
                                    "referenced_columns": list(refs),
                                    "formula": measure.formula,
                                }

                if fields:
                    # Create schema metadata
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
                    yield MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn, aspect=dataset_props
                    ).as_workunit()

                    # Process source lineage
                    source_info = {
                        "tables": [],
                        "platform": "powerbi",  # default
                    }

                    # Check table source lineage tag
                    if table.source_lineage_tag:
                        source_info["tables"].append(table.source_lineage_tag)

                    # Process partition source queries
                    for partition in table.partitions:
                        if partition.source_query:
                            source_info["platform"] = (
                                PowerBiSourceTypeMapper.get_source_type(
                                    partition.source_query
                                )
                            )
                            source_info["tables"].extend(
                                self._extract_source_tables(partition.source_query)
                            )

                    if source_info["tables"]:
                        lineage_aspect = self._build_lineage(
                            table=table,
                            field_lineage=field_lineage,
                            source_info=source_info,
                            dataset_urn=dataset_urn,
                        )
                        if lineage_aspect:
                            yield MetadataChangeProposalWrapper(
                                entityUrn=dataset_urn, aspect=lineage_aspect
                            ).as_workunit()

                    yield MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn, aspect=StatusClass(removed=False)
                    ).as_workunit()

        except Exception as e:
            self.report.report_warning(
                message="Failed to process TMDL content",
                context=f"{workspace.display_name}.{semantic_model.id}",
                exc=e,
            )

    def _build_lineage(
        self,
        table: TmdlTable,
        field_lineage: Dict[str, Dict],
        source_info: Dict[str, Union[List[str], str]],
        dataset_urn: str,
    ) -> Optional[UpstreamLineageClass]:
        """Build comprehensive lineage including column level"""
        upstreams: List[UpstreamClass] = []
        fine_grained_lineages: List[FineGrainedLineageClass] = []

        source_tables = source_info["tables"]
        source_platform = source_info["platform"]
        seen_urns = set()

        for source_table in source_tables:
            try:
                # Clean and parse source table name
                source_table = self._clean_name(source_table)
                source_urn = make_dataset_urn_with_platform_instance(
                    platform=source_platform,
                    name=source_table,
                    platform_instance=self.platform_instance,
                    env=self.env,
                )

                if source_urn not in seen_urns:
                    seen_urns.add(source_urn)

                    # Process field lineage
                    for field_name, lineage_info in field_lineage.items():
                        source_col = lineage_info.get("source_column")
                        source_lineage = lineage_info.get("source_lineage")

                        # Direct column mapping
                        if source_col:
                            fine_grained_lineages.append(
                                FineGrainedLineageClass(
                                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                                    upstreams=[
                                        make_schema_field_urn(source_urn, source_col)
                                    ],
                                    downstreams=[
                                        make_schema_field_urn(dataset_urn, field_name)
                                    ],
                                )
                            )

                        # Source lineage tag matching
                        elif source_lineage and source_lineage in source_table:
                            fine_grained_lineages.append(
                                FineGrainedLineageClass(
                                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                                    upstreams=[
                                        make_schema_field_urn(source_urn, field_name)
                                    ],
                                    downstreams=[
                                        make_schema_field_urn(dataset_urn, field_name)
                                    ],
                                )
                            )

                        # Referenced columns (for measures and calculated columns)
                        elif "referenced_columns" in lineage_info:
                            fine_grained_lineages.append(
                                FineGrainedLineageClass(
                                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                                    upstreams=[
                                        make_schema_field_urn(source_urn, field_name)
                                        for field_name in lineage_info[
                                            "referenced_columns"
                                        ]
                                    ],
                                    downstreams=[
                                        make_schema_field_urn(dataset_urn, field_name)
                                    ],
                                    transformOperation=lineage_info.get("formula"),
                                )
                            )

                    upstreams.append(
                        UpstreamClass(
                            dataset=source_urn,
                            type=DatasetLineageTypeClass.TRANSFORMED,
                        )
                    )

            except Exception as e:
                logger.warning(
                    f"Failed to process source table {source_table}: {str(e)}"
                )

        if not upstreams:
            return None

        return UpstreamLineageClass(
            upstreams=upstreams,
            fineGrainedLineages=fine_grained_lineages
            if fine_grained_lineages
            else None,
        )

    def _create_schema_field(self, column: TmdlColumn) -> SchemaFieldClass:
        """Create a schema field with proper type mapping and all metadata"""
        # Get the field type
        field_type = SchemaFieldTypeMapper.get_field_type(column.data_type)

        # Build description including formula if it's a calculated column
        description = column.annotations.get("description", "")
        if "formula" in column.annotations:
            formula = column.annotations["formula"]
            referenced_cols = column.annotations.get("referencedColumns", "").split(",")
            description = (
                f"{description}\nCalculated Column Formula: {formula}\n"
                f"Referenced columns: {', '.join(c for c in referenced_cols if c)}"
            ).strip()

        # Handle format string
        native_data_type = column.source_provider_type or column.data_type
        if "formatString" in column.annotations:
            native_data_type = (
                f"{native_data_type} ({column.annotations['formatString']})"
            )

        # Include source information
        if column.source_column:
            description = f"{description}\nSource column: {column.source_column}"
        if column.source_lineage_tag:
            description = (
                f"{description}\nSource lineage tag: {column.source_lineage_tag}"
            )

        return SchemaFieldClass(
            fieldPath=column.name,
            type=field_type,
            description=description,
            nativeDataType=native_data_type,
            isPartOfKey=False,
        )

    def _create_measure_field(self, measure: TmdlMeasure) -> SchemaFieldClass:
        """Create a schema field for a measure"""
        return SchemaFieldClass(
            fieldPath=measure.name,
            type=SchemaFieldTypeMapper.get_field_type("decimal"),
            description=f"Measure: {measure.formula}\n{measure.description}".strip(),
            nativeDataType="measure",
            isPartOfKey=False,
        )

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

    def _extract_referenced_columns(self, formula: str) -> Set[str]:
        """Extract column references from a DAX formula"""
        refs = set()
        try:
            # Match column references in square brackets
            matches = re.finditer(r"\[([^]]+)]", formula)
            refs.update(match.group(1) for match in matches)
        except Exception as e:
            logger.warning(f"Error extracting column references: {str(e)}")
        return refs

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
