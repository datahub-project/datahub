import logging
import time
from typing import Dict, Iterable, List, Optional

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    ContainerKey,
    add_dataset_to_container,
    gen_containers,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.workday.config import WorkdayConfig
from datahub.ingestion.source.workday.constants import (
    PRIMARY_KEY_TAG_URN,
    PRISM_SOURCE_TYPE_WORKDAY,
    PRISM_TYPE_TO_DATAHUB,
    SUBTYPE_DATA_SOURCE,
    SUBTYPE_PRISM_DATASET,
    SUBTYPE_PRISM_TABLE,
    SUBTYPE_REPORT,
    SUBTYPE_TENANT,
    WORKDAY_PLATFORM,
)
from datahub.ingestion.source.workday.lineage import resolve_external_upstream_urn
from datahub.ingestion.source.workday.models import (
    PrismDataset,
    PrismDataSource,
    PrismField,
    PrismTable,
    WorkdayObject,
)
from datahub.ingestion.source.workday.report import WorkdayReport
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BooleanTypeClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    DateTypeClass,
    GlobalTagsClass,
    NumberTypeClass,
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
    TimeTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)

logger = logging.getLogger(__name__)

_TYPE_CLASS_BY_NAME = {
    "string": StringTypeClass,
    "number": NumberTypeClass,
    "boolean": BooleanTypeClass,
    "date": DateTypeClass,
    "time": TimeTypeClass,
}


class TenantKey(ContainerKey):
    tenant: str


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

    # -- Shared aspect builders --------------------------------------------

    def _properties_aspect(
        self, obj: WorkdayObject, source_type: Optional[str]
    ) -> DatasetPropertiesClass:
        custom_properties: Dict[str, str] = {"workday_id": obj.id}
        if source_type:
            custom_properties["source_type"] = source_type
        return DatasetPropertiesClass(
            name=obj.name,
            description=obj.description,
            customProperties=custom_properties,
            qualifiedName=f"{self.config.tenant}.{obj.name}",
        )

    def _ownership_aspect(self, obj: WorkdayObject) -> Optional[OwnershipClass]:
        if not self.config.ingest_owner or not obj.owner:
            return None
        return OwnershipClass(
            owners=[
                OwnerClass(
                    owner=make_user_urn(obj.owner),
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                )
            ]
        )

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
        tags: Optional[GlobalTagsClass] = None
        if field.is_primary_key:
            tags = GlobalTagsClass(tags=[TagAssociationClass(tag=PRIMARY_KEY_TAG_URN)])
        return SchemaFieldClass(
            fieldPath=field.name,
            type=SchemaFieldDataTypeClass(type=type_class()),
            nativeDataType=field.type_descriptor or type_name,
            description=field.description,
            isPartOfKey=field.is_primary_key,
            globalTags=tags,
        )

    def _emit(
        self, dataset_urn: str, aspects: List[object]
    ) -> Iterable[MetadataWorkUnit]:
        for aspect in aspects:
            if aspect is None:
                continue
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=aspect
            ).as_workunit()
        yield from add_dataset_to_container(self.tenant_key(), dataset_urn)

    # -- Lineage ------------------------------------------------------------

    def _upstream_lineage(
        self,
        upstream_urns: List[str],
    ) -> Optional[UpstreamLineageClass]:
        if not upstream_urns:
            return None
        now = int(time.time() * 1000)
        upstreams = [
            UpstreamClass(
                dataset=urn,
                type=DatasetLineageTypeClass.TRANSFORMED,
                auditStamp=AuditStampClass(time=now, actor="urn:li:corpuser:datahub"),
            )
            for urn in upstream_urns
        ]
        return UpstreamLineageClass(upstreams=upstreams)

    def _resolve_upstreams(
        self,
        source_ids: Iterable[str],
        data_source_kind_by_id: Dict[str, str],
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
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_table_scanned()
        urn = id_to_urn[table.id]
        aspects: List[object] = [
            self._properties_aspect(table, table.source_type),
            SubTypesClass(typeNames=[SUBTYPE_PRISM_TABLE]),
            self._schema_aspect(urn, table.fields),
            self._ownership_aspect(table),
        ]
        if self.config.extract_lineage:
            source_ids = list(table.data_source_ids)
            if table.dataset_id:
                source_ids.append(table.dataset_id)
            upstreams = self._resolve_upstreams(
                source_ids, {}, id_to_urn, data_source_names
            )
            lineage = self._upstream_lineage(upstreams)
            if lineage is not None:
                self.report.report_lineage_edges(len(lineage.upstreams))
                aspects.append(lineage)
        yield from self._emit(urn, aspects)

    def map_dataset(
        self,
        dataset: PrismDataset,
        id_to_urn: Dict[str, str],
        data_source_names: Dict[str, str],
    ) -> Iterable[MetadataWorkUnit]:
        self.report.report_dataset_scanned()
        urn = id_to_urn[dataset.id]
        aspects: List[object] = [
            self._properties_aspect(dataset, dataset.source_type),
            SubTypesClass(typeNames=[SUBTYPE_PRISM_DATASET]),
            self._ownership_aspect(dataset),
        ]
        if self.config.extract_lineage:
            source_ids = list(dataset.source_table_ids) + list(dataset.data_source_ids)
            upstreams = self._resolve_upstreams(
                source_ids, {}, id_to_urn, data_source_names
            )
            lineage = self._upstream_lineage(upstreams)
            if lineage is not None:
                self.report.report_lineage_edges(len(lineage.upstreams))
                aspects.append(lineage)
        yield from self._emit(urn, aspects)

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
        aspects: List[object] = [
            self._properties_aspect(data_source, data_source.source_type),
            SubTypesClass(typeNames=[subtype]),
            self._ownership_aspect(data_source),
        ]
        yield from self._emit(urn, aspects)
