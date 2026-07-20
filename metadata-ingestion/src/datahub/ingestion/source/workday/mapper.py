import logging
import time
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Type, Union

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_domain_urn,
    make_group_urn,
    make_schema_field_urn,
    make_tag_urn,
    make_term_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    ContainerKey,
    add_dataset_to_container,
    add_domain_to_entity_wu,
    gen_containers,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.workday.config import WorkdayConfig
from datahub.ingestion.source.workday.constants import (
    PRIMARY_KEY_TAG_URN,
    PRISM_SOURCE_TYPE_WORKDAY,
    PRISM_TYPE_TO_DATAHUB,
    SUBTYPE_BUCKET,
    SUBTYPE_BUSINESS_OBJECT,
    SUBTYPE_DATA_SOURCE,
    SUBTYPE_FUNCTIONAL_AREA,
    SUBTYPE_PRISM_DATASET,
    SUBTYPE_PRISM_TABLE,
    SUBTYPE_REPORT,
    SUBTYPE_TENANT,
    WORKDAY_PLATFORM,
)
from datahub.ingestion.source.workday.lineage import resolve_external_upstream_urn
from datahub.ingestion.source.workday.models import (
    CustomReport,
    PrismBucket,
    PrismDataset,
    PrismDataSource,
    PrismField,
    PrismTable,
    WorkdayObject,
    WqlDataSource,
)
from datahub.ingestion.source.workday.report import WorkdayReport
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BooleanTypeClass,
    DatasetLineageTypeClass,
    DatasetProfileClass,
    DatasetPropertiesClass,
    DateTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    NumberTypeClass,
    OperationClass,
    OperationTypeClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    TagAssociationClass,
    TimeStampClass,
    TimeTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
    _Aspect,
)

logger = logging.getLogger(__name__)

_DATAHUB_ACTOR = "urn:li:corpuser:datahub"

# Field types this connector emits; keeps the type lookup concrete for mypy.
_SchemaFieldType = Union[
    StringTypeClass,
    NumberTypeClass,
    BooleanTypeClass,
    DateTypeClass,
    TimeTypeClass,
]


def _parse_iso_millis(value: Optional[str]) -> Optional[int]:
    """Parse an ISO-8601 timestamp to epoch millis, tolerating a trailing Z.

    Returns None on any unparseable value so a malformed timestamp never breaks
    an otherwise-good aspect.
    """
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


_TYPE_CLASS_BY_NAME: Dict[str, Type[_SchemaFieldType]] = {
    "string": StringTypeClass,
    "number": NumberTypeClass,
    "boolean": BooleanTypeClass,
    "date": DateTypeClass,
    "time": TimeTypeClass,
}


class TenantKey(ContainerKey):
    tenant: str


class FunctionalAreaKey(TenantKey):
    """A subject-area sub-container nested under the tenant."""

    functional_area: str


class WorkdayMapper:
    """Maps Workday Prism objects to DataHub aspects.

    URN dataset names are namespaced by kind under the tenant to keep tables,
    pipeline datasets, and data sources from colliding when they share a name
    (a Prism pipeline and its output table commonly do). Lineage edges are
    resolved through an id->URN index built by the source across all objects.
    """

    def __init__(self, config: WorkdayConfig, report: WorkdayReport):
        self.config = config
        self.report = report
        self.platform_urn = make_data_platform_urn(WORKDAY_PLATFORM)

    # -- URN + container helpers -------------------------------------------

    def tenant_key(self) -> TenantKey:
        return TenantKey(
            platform=WORKDAY_PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
            tenant=self.config.tenant,
        )

    def dataset_urn(self, kind: str, object_id: str) -> str:
        # Kind-prefixed, tenant-scoped name; ids are stable Workday WIDs so the
        # URN is stable across runs while the display name lives in properties.
        name = f"{self.config.tenant}.{kind}.{object_id}"
        return make_dataset_urn_with_platform_instance(
            platform=WORKDAY_PLATFORM,
            name=name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def gen_tenant_container(self) -> Iterable[MetadataWorkUnit]:
        yield from gen_containers(
            container_key=self.tenant_key(),
            name=self.config.tenant,
            sub_types=[SUBTYPE_TENANT],
        )

    def functional_area_key(self, area: str) -> FunctionalAreaKey:
        return FunctionalAreaKey(
            platform=WORKDAY_PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
            tenant=self.config.tenant,
            functional_area=area,
        )

    def gen_functional_area_container(self, area: str) -> Iterable[MetadataWorkUnit]:
        yield from gen_containers(
            container_key=self.functional_area_key(area),
            name=area,
            sub_types=[SUBTYPE_FUNCTIONAL_AREA],
            parent_container_key=self.tenant_key(),
        )

    def _container_key(self, obj: WorkdayObject) -> ContainerKey:
        """Place objects that declare a functional area in its sub-container."""
        if self.config.use_functional_area_containers and obj.category:
            return self.functional_area_key(obj.category)
        return self.tenant_key()

    # -- Shared aspect builders --------------------------------------------

    def _properties_aspect(
        self, obj: WorkdayObject, source_type: Optional[str]
    ) -> DatasetPropertiesClass:
        custom_properties: Dict[str, str] = {"workday_id": obj.id}
        if source_type:
            custom_properties["source_type"] = source_type
        created = _parse_iso_millis(obj.created)
        updated = _parse_iso_millis(obj.updated)
        return DatasetPropertiesClass(
            name=obj.name,
            description=obj.description,
            customProperties=custom_properties,
            qualifiedName=f"{self.config.tenant}.{obj.name}",
            created=TimeStampClass(time=created) if created is not None else None,
            lastModified=(
                TimeStampClass(time=updated) if updated is not None else None
            ),
        )

    def _ownership_aspect(self, obj: WorkdayObject) -> Optional[OwnershipClass]:
        if not self.config.ingest_owner:
            return None
        owners: List[OwnerClass] = []
        if obj.owner:
            owners.append(
                OwnerClass(
                    owner=make_user_urn(obj.owner),
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                )
            )
        if obj.owner_group:
            owners.append(
                OwnerClass(
                    owner=make_group_urn(obj.owner_group),
                    type=OwnershipTypeClass.BUSINESS_OWNER,
                )
            )
        return OwnershipClass(owners=owners) if owners else None

    def _tags_aspect(self, obj: WorkdayObject) -> Optional[GlobalTagsClass]:
        if not self.config.ingest_tags or not obj.tags:
            return None
        return GlobalTagsClass(
            tags=[TagAssociationClass(tag=make_tag_urn(tag)) for tag in obj.tags]
        )

    def _terms_aspect(self, obj: WorkdayObject) -> Optional[GlossaryTermsClass]:
        if not self.config.ingest_glossary_terms or not obj.glossary_terms:
            return None
        return GlossaryTermsClass(
            terms=[
                GlossaryTermAssociationClass(urn=make_term_urn(term))
                for term in obj.glossary_terms
            ],
            auditStamp=AuditStampClass(
                time=int(time.time() * 1000), actor=_DATAHUB_ACTOR
            ),
        )

    def _domain_urn(self, name: str) -> Optional[str]:
        for domain, pattern in self.config.domain.items():
            if pattern.allowed(name):
                return make_domain_urn(domain)
        return None

    def _schema_aspect(
        self, dataset_urn: str, fields: List[PrismField]
    ) -> Optional[SchemaMetadataClass]:
        if not fields:
            return None
        schema_fields: List[SchemaFieldClass] = []
        for field in fields:
            self.report.report_field_scanned()
            schema_fields.append(self._schema_field(field))
        return SchemaMetadataClass(
            schemaName=dataset_urn,
            platform=self.platform_urn,
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=schema_fields,
        )

    def _schema_field(self, field: PrismField) -> SchemaFieldClass:
        descriptor = (field.type_descriptor or "").strip().lower()
        type_name = PRISM_TYPE_TO_DATAHUB.get(descriptor)
        if type_name is None:
            # Unknown Prism type: keep the field (as a string) rather than drop
            # it, and record the descriptor so gaps in the type map are visible.
            if descriptor:
                self.report.report_unknown_field_type(descriptor)
            type_name = "string"
        type_class = _TYPE_CLASS_BY_NAME[type_name]
        tag_urns: List[str] = []
        if field.is_primary_key:
            tag_urns.append(PRIMARY_KEY_TAG_URN)
        if self.config.ingest_tags:
            tag_urns.extend(make_tag_urn(tag) for tag in field.tags)
        tags = (
            GlobalTagsClass(tags=[TagAssociationClass(tag=urn) for urn in tag_urns])
            if tag_urns
            else None
        )
        # A primary-key field is implicitly non-nullable; otherwise honor the
        # API's nullability when it reported one (default to nullable=True, the
        # SchemaField default, when unknown).
        nullable = False if field.is_primary_key else field.nullable
        return SchemaFieldClass(
            fieldPath=field.name,
            type=SchemaFieldDataTypeClass(type=type_class()),
            nativeDataType=self._native_type(field, type_name),
            description=field.description,
            nullable=nullable if nullable is not None else True,
            isPartOfKey=field.is_primary_key,
            globalTags=tags,
        )

    @staticmethod
    def _native_type(field: PrismField, fallback: str) -> str:
        """Native type string, enriched with precision/scale/length when known."""
        base = field.type_descriptor or fallback
        if field.precision is not None and field.scale is not None:
            return f"{base}({field.precision},{field.scale})"
        if field.precision is not None:
            return f"{base}({field.precision})"
        if field.length is not None:
            return f"{base}({field.length})"
        return base

    def _emit(
        self,
        dataset_urn: str,
        aspects: List[Optional[_Aspect]],
        container_key: Optional[ContainerKey] = None,
        domain_urn: Optional[str] = None,
    ) -> Iterable[MetadataWorkUnit]:
        for aspect in aspects:
            if aspect is None:
                continue
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=aspect
            ).as_workunit()
        yield from add_dataset_to_container(
            container_key or self.tenant_key(), dataset_urn
        )
        if domain_urn is not None:
            yield from add_domain_to_entity_wu(dataset_urn, domain_urn)

    # -- Lineage ------------------------------------------------------------

    def _upstream_lineage(
        self,
        upstream_urns: List[str],
        fine_grained: Optional[List[FineGrainedLineageClass]] = None,
    ) -> Optional[UpstreamLineageClass]:
        if not upstream_urns:
            return None
        now = int(time.time() * 1000)
        upstreams = [
            UpstreamClass(
                dataset=urn,
                type=DatasetLineageTypeClass.TRANSFORMED,
                auditStamp=AuditStampClass(time=now, actor=_DATAHUB_ACTOR),
            )
            for urn in upstream_urns
        ]
        return UpstreamLineageClass(
            upstreams=upstreams,
            fineGrainedLineages=fine_grained or None,
        )

    def _fine_grained_lineage(
        self,
        table: PrismTable,
        dataset: Optional[PrismDataset],
        id_to_urn: Dict[str, str],
    ) -> List[FineGrainedLineageClass]:
        """Build column-level edges from the producing dataset's field mappings.

        Each mapping names an output field and (optionally) its source field and
        source object; edges with a resolvable upstream field + URN are emitted,
        the rest are skipped rather than guessed.
        """
        if dataset is None or not dataset.field_mappings:
            return []
        table_urn = id_to_urn[table.id]
        edges: List[FineGrainedLineageClass] = []
        for mapping in dataset.field_mappings:
            if not mapping.upstream_field:
                continue
            upstream_urn = (
                id_to_urn.get(mapping.upstream_object_id)
                if mapping.upstream_object_id
                else None
            )
            if upstream_urn is None:
                continue
            edges.append(
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    upstreams=[
                        make_schema_field_urn(upstream_urn, mapping.upstream_field)
                    ],
                    downstreams=[
                        make_schema_field_urn(table_urn, mapping.downstream_field)
                    ],
                )
            )
        return edges

    def _profile_aspect(self, table: PrismTable) -> Optional[DatasetProfileClass]:
        if not self.config.include_row_counts or table.row_count is None:
            return None
        return DatasetProfileClass(
            timestampMillis=int(time.time() * 1000),
            rowCount=table.row_count,
            columnCount=len(table.fields) or None,
        )

    def _operation_aspect(self, table: PrismTable) -> Optional[OperationClass]:
        if not self.config.include_operational_stats:
            return None
        last_updated = _parse_iso_millis(table.updated)
        if last_updated is None:
            return None
        return OperationClass(
            timestampMillis=int(time.time() * 1000),
            operationType=OperationTypeClass.UPDATE,
            lastUpdatedTimestamp=last_updated,
            actor=_DATAHUB_ACTOR,
        )

    def _resolve_upstreams(
        self,
        source_ids: Iterable[str],
        id_to_urn: Dict[str, str],
        data_source_names: Dict[str, str],
    ) -> List[str]:
        """Turn upstream object ids into URNs.

        A data source that maps to an external warehouse resolves to that
        warehouse dataset URN; otherwise it resolves to the Workday object's
        own URN via the id index.
        """
        upstreams: List[str] = []
        for source_id in source_ids:
            external: Optional[str] = None
            name = data_source_names.get(source_id)
            if name is not None:
                external = resolve_external_upstream_urn(name, self.config)
            if external is not None:
                upstreams.append(external)
                self.report.report_external_upstream_resolved()
            elif source_id in id_to_urn:
                upstreams.append(id_to_urn[source_id])
        # Deduplicate while preserving order.
        return list(dict.fromkeys(upstreams))

    # -- Entity mappers -----------------------------------------------------

    def map_table(
        self,
        table: PrismTable,
        id_to_urn: Dict[str, str],
        data_source_names: Dict[str, str],
        producing_dataset: Optional[PrismDataset] = None,
        extra_upstream_urns: Optional[List[str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_table_scanned()
        urn = id_to_urn[table.id]
        aspects: List[Optional[_Aspect]] = [
            self._properties_aspect(table, table.source_type),
            SubTypesClass(typeNames=[SUBTYPE_PRISM_TABLE]),
            self._schema_aspect(urn, table.fields),
            self._ownership_aspect(table),
            self._tags_aspect(table),
            self._terms_aspect(table),
            self._profile_aspect(table),
            self._operation_aspect(table),
        ]
        if self.config.extract_lineage:
            source_ids = list(table.data_source_ids)
            if table.dataset_id:
                source_ids.append(table.dataset_id)
            upstreams = self._resolve_upstreams(
                source_ids, id_to_urn, data_source_names
            )
            # Buckets load into this table, so they are additional upstreams.
            upstreams.extend(extra_upstream_urns or [])
            upstreams = list(dict.fromkeys(upstreams))
            fine_grained: List[FineGrainedLineageClass] = []
            if self.config.extract_column_level_lineage:
                fine_grained = self._fine_grained_lineage(
                    table, producing_dataset, id_to_urn
                )
            lineage = self._upstream_lineage(upstreams, fine_grained)
            if lineage is not None:
                self.report.report_lineage_edges(len(lineage.upstreams))
                if fine_grained:
                    self.report.report_cll_edges(len(fine_grained))
                aspects.append(lineage)
        yield from self._emit(urn, aspects, domain_urn=self._domain_urn(table.name))

    def map_dataset(
        self,
        dataset: PrismDataset,
        id_to_urn: Dict[str, str],
        data_source_names: Dict[str, str],
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_dataset_scanned()
        urn = id_to_urn[dataset.id]
        aspects: List[Optional[_Aspect]] = [
            self._properties_aspect(dataset, dataset.source_type),
            SubTypesClass(typeNames=[SUBTYPE_PRISM_DATASET]),
            self._ownership_aspect(dataset),
            self._tags_aspect(dataset),
            self._terms_aspect(dataset),
        ]
        if self.config.extract_transformation_logic and dataset.transform_logic:
            aspects.append(
                ViewPropertiesClass(
                    materialized=True,
                    viewLogic=dataset.transform_logic,
                    viewLanguage="Workday Data Prep Language",
                )
            )
            self.report.report_transform_logic_captured()
        if self.config.extract_lineage:
            source_ids = list(dataset.source_table_ids) + list(dataset.data_source_ids)
            upstreams = self._resolve_upstreams(
                source_ids, id_to_urn, data_source_names
            )
            lineage = self._upstream_lineage(upstreams)
            if lineage is not None:
                self.report.report_lineage_edges(len(lineage.upstreams))
                aspects.append(lineage)
        yield from self._emit(urn, aspects, domain_urn=self._domain_urn(dataset.name))

    def map_data_source(
        self,
        data_source: PrismDataSource,
        id_to_urn: Dict[str, str],
    ) -> Iterable[MetadataWorkUnit]:
        # A Workday-sourced data source represents a RaaS custom report or a
        # Workday business-object source, so it gets the Report subtype;
        # external inputs get the generic Data Source subtype.
        is_report = data_source.source_type == PRISM_SOURCE_TYPE_WORKDAY
        if is_report:
            self.report.report_report_scanned()
            subtype = SUBTYPE_REPORT
        else:
            self.report.report_data_source_scanned()
            subtype = SUBTYPE_DATA_SOURCE
        urn = id_to_urn[data_source.id]
        aspects: List[Optional[_Aspect]] = [
            self._properties_aspect(data_source, data_source.source_type),
            SubTypesClass(typeNames=[subtype]),
            self._ownership_aspect(data_source),
            self._tags_aspect(data_source),
            self._terms_aspect(data_source),
        ]
        yield from self._emit(
            urn, aspects, domain_urn=self._domain_urn(data_source.name)
        )

    def map_bucket(
        self,
        bucket: PrismBucket,
        id_to_urn: Dict[str, str],
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_bucket_scanned()
        urn = id_to_urn[bucket.id]
        properties = self._properties_aspect(bucket, None)
        if bucket.state:
            properties.customProperties["state"] = bucket.state
        aspects: List[Optional[_Aspect]] = [
            properties,
            SubTypesClass(typeNames=[SUBTYPE_BUCKET]),
            self._ownership_aspect(bucket),
            self._tags_aspect(bucket),
            self._terms_aspect(bucket),
        ]
        yield from self._emit(urn, aspects, domain_urn=self._domain_urn(bucket.name))

    def map_business_object(
        self,
        business_object: WqlDataSource,
        id_to_urn: Dict[str, str],
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_business_object_scanned()
        urn = id_to_urn[business_object.id]
        properties = self._properties_aspect(business_object, None)
        if business_object.alias:
            properties.customProperties["alias"] = business_object.alias
        aspects: List[Optional[_Aspect]] = [
            properties,
            SubTypesClass(typeNames=[SUBTYPE_BUSINESS_OBJECT]),
            self._schema_aspect(urn, business_object.fields),
            self._ownership_aspect(business_object),
            self._tags_aspect(business_object),
            self._terms_aspect(business_object),
        ]
        if self.config.extract_business_object_relationships:
            related = [
                id_to_urn[related_id]
                for related_id in business_object.related_object_ids
                if related_id in id_to_urn
            ]
            lineage = self._upstream_lineage(list(dict.fromkeys(related)))
            if lineage is not None:
                self.report.report_related_object_edges(len(lineage.upstreams))
                aspects.append(lineage)
        yield from self._emit(
            urn,
            aspects,
            container_key=self._container_key(business_object),
            domain_urn=self._domain_urn(business_object.name),
        )

    def map_custom_report(
        self,
        report: CustomReport,
        id_to_urn: Dict[str, str],
        business_object_by_name: Dict[str, WqlDataSource],
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_custom_report_scanned()
        urn = id_to_urn[report.id]
        properties = self._properties_aspect(report, None)
        if report.enabled_as_web_service is not None:
            properties.customProperties["enabled_as_web_service"] = str(
                report.enabled_as_web_service
            ).lower()
        aspects: List[Optional[_Aspect]] = [
            properties,
            SubTypesClass(typeNames=[SUBTYPE_REPORT]),
            self._schema_aspect(urn, report.fields),
            self._ownership_aspect(report),
            self._tags_aspect(report),
            self._terms_aspect(report),
        ]
        # A custom report reads from one business object; link it as an upstream
        # when that object was ingested (name match against the WQL catalog).
        source_object = (
            business_object_by_name.get(report.data_source)
            if report.data_source
            else None
        )
        source_urn = (
            id_to_urn.get(source_object.id) if source_object is not None else None
        )
        if (
            self.config.extract_lineage
            and source_object is not None
            and source_urn is not None
        ):
            fine_grained: List[FineGrainedLineageClass] = []
            if self.config.extract_column_level_lineage:
                fine_grained = self._report_field_lineage(
                    report, source_object, source_urn, urn
                )
            lineage = self._upstream_lineage([source_urn], fine_grained)
            if lineage is not None:
                self.report.report_lineage_edges(len(lineage.upstreams))
                if fine_grained:
                    self.report.report_cll_edges(len(fine_grained))
                aspects.append(lineage)
        yield from self._emit(
            urn,
            aspects,
            container_key=self._container_key(report),
            domain_urn=self._domain_urn(report.name),
        )

    def _report_field_lineage(
        self,
        report: CustomReport,
        source_object: WqlDataSource,
        source_urn: str,
        report_urn: str,
    ) -> List[FineGrainedLineageClass]:
        """Match report output fields to source business-object fields by name.

        ponytail: RaaS does not expose an explicit field-derivation map, so a
        report field is linked to the source field of the same name. This misses
        renamed/computed fields but never invents an edge that isn't a 1:1 match.
        """
        source_field_names = {field.name for field in source_object.fields}
        edges: List[FineGrainedLineageClass] = []
        for field in report.fields:
            if field.name not in source_field_names:
                continue
            edges.append(
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    upstreams=[make_schema_field_urn(source_urn, field.name)],
                    downstreams=[make_schema_field_urn(report_urn, field.name)],
                )
            )
        return edges
