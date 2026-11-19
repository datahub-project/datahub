from typing import Dict, Iterable, List, Optional, Union

from datahub.emitter.mce_builder import make_schema_field_urn, make_tag_urn
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.hightouch.config import (
    HightouchSourceConfig,
    HightouchSourceReport,
)
from datahub.ingestion.source.hightouch.constants import HIGHTOUCH_PLATFORM
from datahub.ingestion.source.hightouch.models import (
    HightouchModel,
    HightouchSourceConnection,
    HightouchUserReference,
)
from datahub.ingestion.source.hightouch.resolver import (
    HightouchResolver,
    ParsedModelSql,
    match_field,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    NullTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.metadata.urns import CorpUserUrn
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.sql_parsing.sqlglot_lineage import ColumnRef


def owners_of(
    config: HightouchSourceConfig, user: Optional[HightouchUserReference]
) -> Optional[List[CorpUserUrn]]:
    if not config.extract_owners or not user or not user.email:
        return None
    return [CorpUserUrn(user.email)]


def tags_of(tags: Optional[Dict[str, str]]) -> Optional[List[str]]:
    if not tags:
        return None
    return [make_tag_urn(f"{key}:{value}") for key, value in tags.items() if key]


class HightouchModelEmitter:
    def __init__(
        self,
        config: HightouchSourceConfig,
        report: HightouchSourceReport,
        resolver: HightouchResolver,
    ):
        self.config = config
        self.report = report
        self.resolver = resolver

    def emit(
        self,
        model: HightouchModel,
        referenced_columns: List[str],
        workspace: ContainerKey,
    ) -> Iterable[Union[Entity, MetadataWorkUnit]]:
        """Emit a model once, with the union of the columns its syncs reference."""
        model_urn = self.resolver.model_urn(model)
        source = self.resolver.sources.get(model.source_id)
        source_table_urn = (
            self.resolver.source_table_urn(model, source) if source else None
        )
        parsed = self.resolver.parse_model_sql(model, source) if source else None

        fields = self._schema_fields(
            model, source_table_urn, parsed, referenced_columns
        )
        if fields:
            self.resolver.model_fields[model.id] = fields

        yield Dataset(
            platform=HIGHTOUCH_PLATFORM,
            name=model.slug,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            display_name=model.name,
            description=model.description,
            created=model.created_at,
            last_modified=model.updated_at,
            custom_properties=self._custom_properties(model, source),
            subtype=DatasetSubTypes.HIGHTOUCH_MODEL,
            parent_container=workspace,
            view_definition=(
                ViewPropertiesClass(
                    materialized=False, viewLanguage="SQL", viewLogic=model.raw_sql
                )
                if model.raw_sql
                else None
            ),
            # Lineage comes from the resolver's parse, which resolves URNs the way
            # the source platform's connector writes them.
            parse_view_lineage=False,
            schema=fields or None,
            upstreams=self._upstreams(model_urn, source_table_urn, parsed, fields),
            owners=owners_of(self.config, model.created_by),
            tags=tags_of(model.tags),
        )
        self.report.models_emitted += 1

        # A dbt model's table already has the dbt node as its primary sibling, so
        # only table models get one; dbt models keep their upstream lineage.
        if (
            source_table_urn
            and model.table
            and self.config.include_sibling_relationships
        ):
            yield from self._siblings(model_urn, source_table_urn)

    def _custom_properties(
        self, model: HightouchModel, source: Optional[HightouchSourceConnection]
    ) -> Dict[str, str]:
        properties = {"model_id": model.id, "query_type": model.query_type}
        if model.primary_key:
            properties["primary_key"] = model.primary_key
        if model.table_name:
            properties["table"] = model.table_name
        if source:
            properties.update(
                source_id=source.id, source_name=source.name, source_type=source.type
            )
        return properties

    def _schema_fields(
        self,
        model: HightouchModel,
        source_table_urn: Optional[str],
        parsed: Optional[ParsedModelSql],
        referenced_columns: List[str],
    ) -> List[SchemaFieldClass]:
        # The API returns no column list for a model. A table model has the columns
        # of its table as ingested into DataHub, a SQL model the output columns of
        # its parsed query, and otherwise the model has the columns its syncs use.
        upstream_fields = (
            self.resolver.schema_fields(source_table_urn) if source_table_urn else None
        )
        if upstream_fields:
            self.report.model_schemas_from_datahub += 1
            fields = [
                SchemaFieldClass(
                    fieldPath=f.fieldPath,
                    type=f.type,
                    nativeDataType=f.nativeDataType,
                    description=f.description,
                )
                for f in upstream_fields
            ]
        elif parsed and parsed.output_fields:
            self.report.model_schemas_from_sql += 1
            fields = list(parsed.output_fields)
        elif referenced_columns:
            self.report.model_schemas_from_referenced_columns += 1
            fields = [
                SchemaFieldClass(
                    fieldPath=self._upstream_casing(column, parsed),
                    type=SchemaFieldDataTypeClass(type=NullTypeClass()),
                    nativeDataType="",
                )
                for column in referenced_columns
            ]
        else:
            return []

        if model.primary_key:
            for field in fields:
                field.isPartOfKey = field.fieldPath.lower() == model.primary_key.lower()
        return fields

    def _upstream_casing(self, column: str, parsed: Optional[ParsedModelSql]) -> str:
        # Sync columns are written as the user typed them. When the model reads a
        # single table, use that table's casing so column lineage joins; only an
        # exact case-insensitive match is adopted, never a looser one.
        if not parsed or len(parsed.upstream_urns) != 1:
            return column
        upstream_fields = self.resolver.schema_fields(parsed.upstream_urns[0]) or []
        for field in upstream_fields:
            if field.fieldPath.lower() == column.lower():
                return field.fieldPath
        return column

    def _upstreams(
        self,
        model_urn: str,
        source_table_urn: Optional[str],
        parsed: Optional[ParsedModelSql],
        fields: List[SchemaFieldClass],
    ) -> Optional[UpstreamLineageClass]:
        field_paths = [f.fieldPath for f in fields]
        if source_table_urn:
            # A table model passes each column through unchanged.
            upstream_paths = [
                f.fieldPath for f in self.resolver.schema_fields(source_table_urn) or []
            ]
            column_lineage = {
                path: [ColumnRef(table=source_table_urn, column=upstream)]
                for path in field_paths
                if (upstream := match_field(path, upstream_paths))
            }
            return self._lineage(
                model_urn,
                [source_table_urn],
                DatasetLineageTypeClass.COPY,
                column_lineage,
            )
        if parsed and parsed.upstream_urns:
            sql_column_lineage: Dict[str, List[ColumnRef]] = {}
            for column, refs in parsed.column_upstreams.items():
                path = match_field(column, field_paths)
                if path and refs:
                    sql_column_lineage[path] = refs
            return self._lineage(
                model_urn,
                parsed.upstream_urns,
                DatasetLineageTypeClass.VIEW,
                sql_column_lineage,
            )
        return None

    def _lineage(
        self,
        model_urn: str,
        upstream_urns: List[str],
        lineage_type: str,
        column_lineage: Dict[str, List[ColumnRef]],
    ) -> UpstreamLineageClass:
        fine_grained = [
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=[
                    make_schema_field_urn(ref.table, ref.column) for ref in upstreams
                ],
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[make_schema_field_urn(model_urn, path)],
            )
            for path, upstreams in column_lineage.items()
        ]
        self.report.column_lineage_emitted += len(fine_grained)
        return UpstreamLineageClass(
            upstreams=[
                UpstreamClass(dataset=u, type=lineage_type) for u in upstream_urns
            ],
            fineGrainedLineages=fine_grained or None,
        )

    def _siblings(
        self, model_urn: str, source_table_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        # Patches rather than a Siblings aspect: the table may already have
        # siblings, e.g. from dbt, and several models can read one table.
        for mcp in (
            DatasetPatchBuilder(model_urn)
            .add_sibling(source_table_urn, primary=True)
            .build()
        ):
            yield MetadataWorkUnit(
                id=MetadataWorkUnit.generate_workunit_id(mcp), mcp_raw=mcp
            )
        # Linking back from a table nobody ingested would create it as a ghost.
        if self.resolver.exists(source_table_urn):
            for mcp in (
                DatasetPatchBuilder(source_table_urn).add_sibling(model_urn).build()
            ):
                yield MetadataWorkUnit(
                    id=MetadataWorkUnit.generate_workunit_id(mcp),
                    mcp_raw=mcp,
                    is_primary_source=False,
                )
