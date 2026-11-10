import logging
from typing import Dict, Iterable, List, Optional, Union

from datahub.emitter.mce_builder import make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.hightouch.config import (
    HightouchSourceConfig,
    HightouchSourceReport,
)
from datahub.ingestion.source.hightouch.constants import (
    HIGHTOUCH_PLATFORM,
)
from datahub.ingestion.source.hightouch.hightouch_container import (
    HightouchContainerHandler,
)
from datahub.ingestion.source.hightouch.hightouch_lineage import (
    HightouchLineageHandler,
)
from datahub.ingestion.source.hightouch.hightouch_schema import (
    HightouchSchemaHandler,
)
from datahub.ingestion.source.hightouch.hightouch_utils import (
    normalize_column_name,
    reraise_if_programming_error,
    resolve_datahub_field_type,
)
from datahub.ingestion.source.hightouch.models import (
    HightouchModel,
    HightouchModelDatasetResult,
    HightouchSourceConnection,
)
from datahub.ingestion.source.hightouch.protocols import (
    GetAggregatorForPlatform,
    GetPlatformForSource,
)
from datahub.ingestion.source.hightouch.urn_builder import HightouchUrnBuilder
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    GlobalTagsClass,
    SchemaFieldClass,
    SchemalessClass,
    SchemaMetadataClass,
    SubTypesClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.metadata.urns import DatasetUrn
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity

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
        get_platform_for_source: GetPlatformForSource,
        get_aggregator_for_platform: GetAggregatorForPlatform,
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

        schema_field_classes: List[SchemaFieldClass] = []
        for field in schema_fields:
            normalized_name = normalize_column_name(field.name)
            upstream_cased = upstream_field_casing.get(normalized_name)
            # Only adopt the upstream field's casing when it differs from the model
            # field purely by letter case. A looser match (underscores/hyphens
            # stripped, or a SQL alias that normalizes to the same key) would rename
            # the model's actual output column, so keep the model field name then.
            if upstream_cased and upstream_cased.lower() == field.name.lower():
                field_path = upstream_cased
            else:
                field_path = field.name

            schema_field_classes.append(
                SchemaFieldClass(
                    fieldPath=field_path,
                    type=resolve_datahub_field_type(field.type),
                    nativeDataType=field.type,
                    description=field.description,
                    isPartOfKey=field.is_primary_key,
                )
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
        except Exception as e:
            reraise_if_programming_error(e, f"registering schema for {model.slug}")
            self.report.report_datahub_lookup_failure(
                "model schema registration for SQL parsing",
                f"model_slug: {model.slug}",
                e,
            )

    def setup_table_model_upstream(
        self,
        model: HightouchModel,
        source: Optional[HightouchSourceConnection],
        custom_properties: Dict[str, str],
    ) -> Optional[Union[str, DatasetUrn]]:
        if not source:
            return None
        upstream_urn = self.urn_builder.source_table_urn(model, source)
        if not upstream_urn:
            return None
        custom_properties["table_lineage"] = "true"
        custom_properties["upstream_table"] = DatasetUrn.from_string(upstream_urn).name
        custom_properties["source_table_urn"] = upstream_urn

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

        # The table model's UpstreamLineage, with its column lineage, is emitted
        # once in emit_model_aspects; setting it here too would be overwritten.
        self.setup_table_model_upstream(model, source, custom_properties)

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
        dataset_urn = str(model_dataset.urn)

        subtypes: List[str] = [str(DatasetSubTypes.HIGHTOUCH_MODEL)]
        if model.table_name and not model.raw_sql:
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
                TagAssociationClass(tag=make_tag_urn(f"ht_{key}_{value}"))
                for key, value in model.tags.items()
                if key and value
            ]
            if tags_to_emit:
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=GlobalTagsClass(tags=tags_to_emit),
                ).as_workunit()
                self.report.tags_emitted += len(tags_to_emit)

        if not source:
            return

        # Only a table or dbt model is the same asset as the table it reads, so only
        # those get a sibling. A query over a table is a different asset.
        source_table_urn = self.urn_builder.source_table_urn(model, source)
        if source_table_urn:
            if self.config.include_sibling_relationships:
                yield from self.lineage_handler.emit_sibling_aspects(
                    dataset_urn, source_table_urn
                )

            # Table-model lineage is emitted only here, not through the SQL
            # aggregator: the aggregator's own UpstreamLineage for this URN would
            # replace this aspect and drop its column-level lineage.
            fine_grained_lineages = (
                self.lineage_handler.generate_table_model_column_lineage(
                    model, dataset_urn, source_table_urn, schema_fields or []
                )
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=source_table_urn,
                            type=DatasetLineageTypeClass.COPY,
                        )
                    ],
                    fineGrainedLineages=fine_grained_lineages or None,
                ),
            ).as_workunit()
            self.report.column_lineage_emitted += len(fine_grained_lineages or [])
        elif (
            model.raw_sql
            and not self.get_platform_for_source(source).include_schema_in_urn
        ):
            # The SQL aggregator would build upstream URNs with the schema segment
            # the source's own URNs drop, so emit the normalized upstreams here.
            # Table-level only: column lineage needs the aggregator.
            upstream_urns = self.lineage_handler.sql_upstream_urns(model, source)
            if upstream_urns:
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=UpstreamLineageClass(
                        upstreams=[
                            UpstreamClass(
                                dataset=urn, type=DatasetLineageTypeClass.VIEW
                            )
                            for urn in upstream_urns
                        ]
                    ),
                ).as_workunit()

    def emit_model(
        self,
        model: HightouchModel,
        source: Optional[HightouchSourceConnection],
        referenced_columns: Optional[List[str]] = None,
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        # Shared by the standalone-model path and the sync path so both emit an
        # identical dataset/aspects/lineage sequence.
        result = self.generate_model_dataset(
            model, source, referenced_columns=referenced_columns
        )
        self.report.report_models_emitted()
        yield result.dataset

        yield from self.emit_model_aspects(
            model, result.dataset, source, result.schema_fields
        )

        yield from self.container_handler.add_model_to_container(result.dataset.urn)

        if source:
            self.lineage_handler.register_model_lineage(
                model,
                str(result.dataset.urn),
                source,
                self.get_platform_for_source,
                self.get_aggregator_for_platform,
            )

    def extract_table_urns_from_sql(
        self,
        model: HightouchModel,
        source: HightouchSourceConnection,
    ) -> List[str]:
        return self.lineage_handler.sql_upstream_urns(model, source)
