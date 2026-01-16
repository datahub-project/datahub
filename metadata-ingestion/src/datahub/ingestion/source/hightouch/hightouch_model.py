import logging
from typing import Callable, Dict, Iterable, List, Optional, Union

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.hightouch.config import (
    HightouchSourceConfig,
    HightouchSourceReport,
)
from datahub.ingestion.source.hightouch.constants import HIGHTOUCH_PLATFORM
from datahub.ingestion.source.hightouch.hightouch_container import (
    HightouchContainerHandler,
)
from datahub.ingestion.source.hightouch.hightouch_lineage import (
    HightouchLineageHandler,
)
from datahub.ingestion.source.hightouch.hightouch_schema import (
    HightouchSchemaHandler,
)
from datahub.ingestion.source.hightouch.hightouch_utils import normalize_column_name
from datahub.ingestion.source.hightouch.models import (
    HightouchModel,
    HightouchModelDatasetResult,
    HightouchSourceConnection,
)
from datahub.ingestion.source.hightouch.urn_builder import HightouchUrnBuilder
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    GlobalTagsClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemalessClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.metadata.urns import DatasetUrn
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result

logger = logging.getLogger(__name__)


class HightouchModelHandler:
    def __init__(
        self,
        config: "HightouchSourceConfig",
        report: "HightouchSourceReport",
        urn_builder: "HightouchUrnBuilder",
        schema_handler: "HightouchSchemaHandler",
        lineage_handler: "HightouchLineageHandler",
        container_handler: "HightouchContainerHandler",
        get_platform_for_source: Callable,
        get_aggregator_for_platform: Callable,
        model_schema_fields_cache: Dict[str, List[SchemaFieldClass]],
    ):
        self.config = config
        self.report = report
        self.urn_builder = urn_builder
        self.schema_handler = schema_handler
        self.lineage_handler = lineage_handler
        self.container_handler = container_handler
        self.get_platform_for_source = get_platform_for_source
        self.get_aggregator_for_platform = get_aggregator_for_platform
        self.model_schema_fields_cache = model_schema_fields_cache

    def build_model_custom_properties(
        self, model: HightouchModel, source: Optional[HightouchSourceConnection]
    ) -> Dict[str, str]:
        custom_properties = {
            "model_id": model.id,
            "query_type": model.query_type,
            "is_schema": str(model.is_schema),
        }

        if model.primary_key:
            custom_properties["primary_key"] = model.primary_key

        if source:
            custom_properties["source_id"] = source.id
            custom_properties["source_name"] = source.name
            custom_properties["source_type"] = source.type

        if model.tags:
            for key, value in model.tags.items():
                custom_properties[f"tag_{key}"] = value

        if model.folder_id:
            custom_properties["folder_id"] = model.folder_id

        if model.raw_sql:
            sql_truncated = (
                model.raw_sql[:2000] if len(model.raw_sql) > 2000 else model.raw_sql
            )
            custom_properties["raw_sql"] = sql_truncated
            if len(model.raw_sql) > 2000:
                custom_properties["raw_sql_truncated"] = "true"
                custom_properties["raw_sql_length"] = str(len(model.raw_sql))

        return custom_properties

    def build_model_schema_fields(
        self,
        model: HightouchModel,
        source: Optional[HightouchSourceConnection],
        referenced_columns: Optional[List[str]],
    ) -> List[SchemaFieldClass]:
        schema_fields = self.schema_handler.resolve_schema(
            model=model, source=source, referenced_columns=referenced_columns
        )

        if not schema_fields:
            return []

        sql_table_urns = []
        if model.raw_sql and source:
            sql_table_urns = self.extract_table_urns_from_sql(model, source)

        upstream_field_casing = self.lineage_handler.get_upstream_field_casing(
            model, source, sql_table_urns
        )

        if upstream_field_casing:
            logger.debug(
                f"Using upstream field casing for model {model.slug}: {len(upstream_field_casing)} fields mapped"
            )

        schema_field_classes: List[SchemaFieldClass] = []
        for field in schema_fields:
            normalized_name = normalize_column_name(field.name)
            field_path = upstream_field_casing.get(normalized_name, field.name)

            schema_field_classes.append(
                SchemaFieldClass(
                    fieldPath=field_path,
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType=field.type,
                    description=field.description,
                    isPartOfKey=field.is_primary_key,
                )
            )

        if schema_field_classes:
            logger.debug(
                f"Final schema for model {model.slug}: {[f.fieldPath for f in schema_field_classes]}"
            )

        return schema_field_classes

    def register_model_schema_with_aggregator(
        self,
        model: HightouchModel,
        dataset_urn: str,
        source: Optional[HightouchSourceConnection],
        schema_field_classes: List[SchemaFieldClass],
    ) -> None:
        if not source:
            return

        source_platform = self.get_platform_for_source(source)
        if not source_platform.platform:
            return

        aggregator = self.get_aggregator_for_platform(source_platform)
        if not aggregator:
            return

        try:
            aggregator.register_schema(
                dataset_urn,
                SchemaMetadataClass(
                    schemaName=model.slug,
                    platform=f"urn:li:dataPlatform:{HIGHTOUCH_PLATFORM}",
                    version=0,
                    hash="",
                    platformSchema=SchemalessClass(),
                    fields=schema_field_classes,
                ),
            )
            logger.debug(f"Registered model schema for {model.slug} with aggregator")
        except Exception as e:
            logger.debug(f"Failed to register model schema for {model.slug}: {e}")

    def setup_table_model_upstream(
        self,
        model: HightouchModel,
        source: Optional[HightouchSourceConnection],
        custom_properties: Dict[str, str],
    ) -> Optional[Union[str, DatasetUrn]]:
        if not source or model.query_type != "table" or not model.name:
            return None

        table_name = model.name

        if source.configuration:
            database = source.configuration.get("database", "")
            schema = source.configuration.get("schema", "")
            source_details = self.urn_builder._get_cached_source_details(source)

            if source_details.include_schema_in_urn and schema:
                table_name = f"{database}.{schema}.{table_name}"
            elif database and "." not in table_name:
                table_name = f"{database}.{table_name}"

        upstream_urn = self.urn_builder.make_upstream_table_urn(table_name, source)
        custom_properties["table_lineage"] = "true"
        custom_properties["upstream_table"] = table_name
        custom_properties["source_table_urn"] = str(upstream_urn)

        logger.debug(f"Set direct upstream lineage for table model {model.slug}")
        return upstream_urn

    def generate_model_dataset(
        self,
        model: HightouchModel,
        source: Optional[HightouchSourceConnection],
        referenced_columns: Optional[List[str]] = None,
    ) -> HightouchModelDatasetResult:
        custom_properties = self.build_model_custom_properties(model, source)

        dataset = Dataset(
            name=model.slug,
            platform=HIGHTOUCH_PLATFORM,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            display_name=model.name,
            description=model.description,
            created=model.created_at,
            last_modified=model.updated_at,
            custom_properties=custom_properties,
        )

        schema_field_classes = self.build_model_schema_fields(
            model, source, referenced_columns
        )
        if schema_field_classes:
            dataset._set_schema(schema_field_classes)
            self.register_model_schema_with_aggregator(
                model, str(dataset.urn), source, schema_field_classes
            )

        upstream_urn = self.setup_table_model_upstream(model, source, custom_properties)
        if upstream_urn:
            dataset.set_upstreams([upstream_urn])

        dataset.set_custom_properties(custom_properties)

        if schema_field_classes:
            self.model_schema_fields_cache[model.id] = schema_field_classes

        return HightouchModelDatasetResult(
            dataset=dataset, schema_fields=schema_field_classes
        )

    def emit_model_aspects(
        self,
        model: HightouchModel,
        model_dataset: Dataset,
        source: Optional[HightouchSourceConnection] = None,
        schema_fields: Optional[List[SchemaFieldClass]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        from datahub.emitter.mcp import MetadataChangeProposalWrapper

        dataset_urn = str(model_dataset.urn)

        subtypes: List[str] = [str(DatasetSubTypes.HIGHTOUCH_MODEL)]
        if model.raw_sql:
            subtypes.append(str(DatasetSubTypes.VIEW))
        elif model.query_type == "table":
            subtypes.append(str(DatasetSubTypes.TABLE))
        else:
            subtypes.append(str(DatasetSubTypes.VIEW))

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=subtypes),
        ).as_workunit()

        if model.raw_sql:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=ViewPropertiesClass(
                    materialized=False,
                    viewLanguage="SQL",
                    viewLogic=model.raw_sql,
                ),
            ).as_workunit()

        if model.tags:
            tags_to_emit = [
                TagAssociationClass(tag=f"urn:li:tag:ht_{key}_{value}")
                for key, value in model.tags.items()
                if key and value
            ]
            if tags_to_emit:
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=GlobalTagsClass(tags=tags_to_emit),
                ).as_workunit()
                self.report.tags_emitted += len(tags_to_emit)

        source_table_urn = None

        if model.query_type == "table" and model.name and source:
            table_name = model.name
            if source.configuration:
                database = source.configuration.get("database", "")
                schema = source.configuration.get("schema", "")
                source_details = self.urn_builder._get_cached_source_details(source)
                if source_details.include_schema_in_urn and schema:
                    table_name = f"{database}.{schema}.{table_name}"
                elif database and "." not in table_name:
                    table_name = f"{database}.{table_name}"

            source_table_urn = self.urn_builder.make_upstream_table_urn(
                table_name, source
            )
        elif model.raw_sql and source:
            sql_table_urns = self.extract_table_urns_from_sql(model, source)
            if len(sql_table_urns) == 1:
                source_table_urn = sql_table_urns[0]

        if source_table_urn:
            if self.config.include_table_lineage_to_sibling:
                yield from self.lineage_handler.emit_sibling_aspects(
                    dataset_urn, str(source_table_urn)
                )

            if model.query_type == "table":
                fine_grained_lineages = (
                    self.lineage_handler.generate_table_model_column_lineage(
                        model, dataset_urn, str(source_table_urn), schema_fields or []
                    )
                )
                if fine_grained_lineages:
                    yield MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn,
                        aspect=UpstreamLineageClass(
                            upstreams=[
                                UpstreamClass(
                                    dataset=str(source_table_urn),
                                    type=DatasetLineageTypeClass.COPY,
                                )
                            ],
                            fineGrainedLineages=fine_grained_lineages,
                        ),
                    ).as_workunit()
                    self.report.column_lineage_emitted += len(fine_grained_lineages)

    def get_model_workunits(
        self, model: HightouchModel, source: Optional[HightouchSourceConnection]
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        self.report.report_models_scanned()

        result = self.generate_model_dataset(model, source)
        self.report.report_models_emitted()
        yield result.dataset

        yield from self.emit_model_aspects(
            model, result.dataset, source, result.schema_fields
        )

        if source:
            self.lineage_handler.register_model_lineage(
                model,
                str(result.dataset.urn),
                source,
                self.get_platform_for_source,
                self.get_aggregator_for_platform,
            )

        if model.workspace_id:
            workspace_key = self.container_handler.get_workspace_key(model.workspace_id)
            yield from self.container_handler.generate_workspace_container(
                model.workspace_id
            )
            self.report.workspaces_emitted += 1

            if model.folder_id:
                folder_key = self.container_handler.get_folder_key(
                    model.folder_id, model.workspace_id
                )
                yield from self.container_handler.generate_folder_container(
                    model.folder_id,
                    model.workspace_id,
                    parent_container_key=workspace_key,
                )
                self.report.folders_emitted += 1
                yield from self.container_handler.add_entity_to_container(
                    str(result.dataset.urn), folder_key
                )
            else:
                yield from self.container_handler.add_entity_to_container(
                    str(result.dataset.urn), workspace_key
                )

    def extract_table_urns_from_sql(
        self,
        model: HightouchModel,
        source: HightouchSourceConnection,
    ) -> List[str]:
        """Extract table URNs from SQL query using SQL parser."""
        if not model.raw_sql:
            return []

        source_platform = self.get_platform_for_source(source)
        if not source_platform.platform:
            return []

        try:
            sql_result = create_lineage_sql_parsed_result(
                query=model.raw_sql,
                default_db=source_platform.database,
                platform=source_platform.platform,
                platform_instance=source_platform.platform_instance,
                env=source_platform.env,
                graph=None,
                schema_aware=False,
            )

            if sql_result.in_tables:
                logger.debug(
                    f"Extracted {len(sql_result.in_tables)} table references from SQL in model {model.slug}"
                )
                return [str(urn) for urn in sql_result.in_tables]
        except Exception as e:
            logger.debug(f"Could not extract tables from SQL for model {model.id}: {e}")

        return []
