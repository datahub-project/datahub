from typing import Dict, Iterable, List, Optional, Set

from requests.exceptions import RequestException

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.sap_mdg.config import (
    MdgTargetPlatform,
    SapMdgSourceConfig,
)
from datahub.ingestion.source.sap_mdg.constants import (
    EDM_TYPE_TO_SCHEMA_FIELD_TYPE,
    FALLBACK_SCHEMA_FIELD_TYPE,
    KNOWN_LOGICAL_SYSTEM_TO_PLATFORM,
    PROPERTY_ODATA_ENTITY_TYPE,
    PROPERTY_ODATA_VERSION,
    SAP_MDG_PLATFORM,
    SERVICE_PATH_STRIP_PATTERN,
)
from datahub.ingestion.source.sap_mdg.edmx_parser import parse_metadata
from datahub.ingestion.source.sap_mdg.models import (
    DrfDistribution,
    NavigationTarget,
    ODataAssociation,
    ODataEntitySet,
    ODataEntityType,
    ODataMetadata,
    ODataNavigationProperty,
)
from datahub.ingestion.source.sap_mdg.odata_client import SapMdgODataClient
from datahub.ingestion.source.sap_mdg.report import SapMdgSourceReport
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    EdgeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    ForeignKeyConstraintClass,
    LogicalParentClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    UpstreamClass,
)
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.specific.dataset import DatasetPatchBuilder


class SapMdgServiceKey(ContainerKey):
    service: str


@platform_name("SAP MDG", id=SAP_MDG_PLATFORM)
@config_class(SapMdgSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default via SAP labels")
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Foreign keys from OData navigation properties, plus cross-platform lineage to "
    "systems MDG replicates to (via the DRF replication model) when `drf.enabled`",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Emitted for downstream fields matching the MDG entity by name when "
    "`drf.emit_column_lineage` is set",
    supported=True,
)
class SapMdgSource(StatefulIngestionSourceBase, TestableSource):
    platform: str = SAP_MDG_PLATFORM

    def __init__(self, config: SapMdgSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.config = config
        self.report: SapMdgSourceReport = SapMdgSourceReport()
        self.client = SapMdgODataClient(config, self.report)
        self._drf_distribution: Optional[DrfDistribution] = None
        if config.drf.enabled:
            self._drf_distribution = self._load_drf_distribution()

    def _load_drf_distribution(self) -> Optional[DrfDistribution]:
        try:
            return self.client.fetch_drf_distribution()
        except Exception as e:
            self.report.warning(
                title="Failed to read DRF replication model",
                message="Cross-platform lineage is disabled for this run.",
                exc=e,
            )
            return None

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SapMdgSource":
        config = SapMdgSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            config = SapMdgSourceConfig.model_validate(config_dict)
            SapMdgODataClient(config, SapMdgSourceReport()).test_connection()
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except RequestException as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=f"Failed to connect to SAP MDG OData service: {e}",
            )
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    def get_report(self) -> SourceReport:
        return self.report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        for service in self.config.services:
            yield from self._process_service(service)

    def close(self) -> None:
        self.client.close()
        super().close()

    def _process_service(self, service: str) -> Iterable[MetadataWorkUnit]:
        self.report.services_scanned += 1
        try:
            content = self.client.fetch_metadata(service)
            metadata = parse_metadata(content)
        except Exception as e:
            self.report.report_service_failed(service)
            self.report.warning(
                title="Failed to fetch or parse service metadata",
                message="Skipping this OData service; ingestion continues.",
                context=service,
                exc=e,
            )
            return

        container = Container(
            self._service_key(service),
            display_name=self._service_id(service),
            subtype=DatasetContainerSubTypes.SAP_MDG_ODATA_SERVICE,
            extra_properties={PROPERTY_ODATA_VERSION: metadata.version.value},
        )
        yield from container.as_workunits()

        entity_types_by_fqn = metadata.entity_types_by_fqn()
        associations_by_fqn = metadata.associations_by_fqn()
        sets_by_type_fqn = metadata.entity_sets_by_type_fqn()

        emitted_type_fqns: Set[str] = set()
        for entity_set in metadata.entity_sets:
            self.report.entity_sets_scanned += 1
            if not self.config.entity_set_pattern.allowed(entity_set.name):
                self.report.report_entity_set_filtered(entity_set.name)
                continue
            entity_type = entity_types_by_fqn.get(entity_set.entity_type_fqn)
            if entity_type is None:
                self.report.warning(
                    title="Entity set references an unknown entity type",
                    message="Skipping this entity set.",
                    context=f"{service}:{entity_set.name}",
                )
                continue
            emitted_type_fqns.add(entity_type.fqn)
            try:
                yield from self._emit_dataset(
                    name=self._entity_set_dataset_name(entity_set),
                    display_name=entity_set.label or entity_set.name,
                    subtype=DatasetSubTypes.SAP_MDG_ENTITY_SET,
                    entity_type=entity_type,
                    container=container,
                    metadata=metadata,
                    associations_by_fqn=associations_by_fqn,
                    sets_by_type_fqn=sets_by_type_fqn,
                )
                yield from self._emit_drf_lineage(service, entity_set, entity_type)
            except Exception as e:
                self.report.warning(
                    title="Failed to emit entity set",
                    message="Skipping this entity set; ingestion continues.",
                    context=f"{service}:{entity_set.name}",
                    exc=e,
                )

        if self.config.emit_entity_types_without_sets:
            yield from self._emit_orphan_entity_types(
                metadata,
                container,
                associations_by_fqn,
                sets_by_type_fqn,
                emitted_type_fqns,
            )

    def _emit_orphan_entity_types(
        self,
        metadata: ODataMetadata,
        container: Container,
        associations_by_fqn: Dict[str, ODataAssociation],
        sets_by_type_fqn: Dict[str, ODataEntitySet],
        emitted_type_fqns: Set[str],
    ) -> Iterable[MetadataWorkUnit]:
        for entity_type in metadata.entity_types:
            self.report.entity_types_scanned += 1
            if entity_type.fqn in emitted_type_fqns:
                continue
            try:
                yield from self._emit_dataset(
                    name=entity_type.fqn,
                    display_name=entity_type.label or entity_type.name,
                    subtype=DatasetSubTypes.SAP_MDG_ENTITY_TYPE,
                    entity_type=entity_type,
                    container=container,
                    metadata=metadata,
                    associations_by_fqn=associations_by_fqn,
                    sets_by_type_fqn=sets_by_type_fqn,
                )
            except Exception as e:
                self.report.warning(
                    title="Failed to emit entity type",
                    message="Skipping this entity type; ingestion continues.",
                    context=entity_type.fqn,
                    exc=e,
                )

    def _emit_dataset(
        self,
        *,
        name: str,
        display_name: str,
        subtype: str,
        entity_type: ODataEntityType,
        container: Container,
        metadata: ODataMetadata,
        associations_by_fqn: Dict[str, ODataAssociation],
        sets_by_type_fqn: Dict[str, ODataEntitySet],
    ) -> Iterable[MetadataWorkUnit]:
        dataset_urn = self._dataset_urn(name)
        foreign_keys = (
            self._build_foreign_keys(
                entity_type, dataset_urn, associations_by_fqn, sets_by_type_fqn
            )
            if self.config.include_foreign_keys
            else []
        )
        schema_metadata = self._build_schema_metadata(entity_type, foreign_keys)

        dataset = Dataset(
            platform=self.platform,
            name=name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=display_name,
            description=entity_type.label,
            subtype=subtype,
            custom_properties=self._custom_properties(entity_type, metadata),
            schema=schema_metadata,
            parent_container=container,
        )
        yield from dataset.as_workunits()
        self.report.datasets_emitted += 1

    def _build_schema_metadata(
        self,
        entity_type: ODataEntityType,
        foreign_keys: List[ForeignKeyConstraintClass],
    ) -> SchemaMetadataClass:
        key_names = set(entity_type.key_property_names)
        fields = [
            SchemaFieldClass(
                fieldPath=prop.name,
                type=self._resolve_field_type(prop.type_name),
                nativeDataType=prop.type_name,
                nullable=prop.nullable,
                isPartOfKey=prop.name in key_names,
                description=prop.description,
            )
            for prop in entity_type.properties
        ]
        return SchemaMetadataClass(
            schemaName=entity_type.fqn,
            platform=make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
            foreignKeys=foreign_keys or None,
        )

    def _resolve_field_type(self, type_name: str) -> SchemaFieldDataTypeClass:
        type_cls = EDM_TYPE_TO_SCHEMA_FIELD_TYPE.get(
            type_name, FALLBACK_SCHEMA_FIELD_TYPE
        )
        return SchemaFieldDataTypeClass(type=type_cls())

    def _build_foreign_keys(
        self,
        entity_type: ODataEntityType,
        dataset_urn: str,
        associations_by_fqn: Dict[str, ODataAssociation],
        sets_by_type_fqn: Dict[str, ODataEntitySet],
    ) -> List[ForeignKeyConstraintClass]:
        foreign_keys: List[ForeignKeyConstraintClass] = []
        for navigation in entity_type.navigation_properties:
            target = self._resolve_navigation(navigation, associations_by_fqn)
            target_set = (
                sets_by_type_fqn.get(target.target_type_fqn)
                if target.target_type_fqn is not None
                else None
            )
            if target_set is None or not target.referential_constraints:
                # Without a resolvable target set and referential constraints we
                # cannot map columns precisely, so we record it rather than guess.
                self.report.foreign_keys_unresolved += 1
                continue

            target_urn = self._dataset_urn(self._entity_set_dataset_name(target_set))
            foreign_keys.append(
                ForeignKeyConstraintClass(
                    name=navigation.name,
                    foreignFields=[
                        make_schema_field_urn(target_urn, c.principal_property)
                        for c in target.referential_constraints
                    ],
                    sourceFields=[
                        make_schema_field_urn(dataset_urn, c.dependent_property)
                        for c in target.referential_constraints
                    ],
                    foreignDataset=target_urn,
                )
            )
            self.report.foreign_keys_emitted += 1
        return foreign_keys

    def _resolve_navigation(
        self,
        navigation: ODataNavigationProperty,
        associations_by_fqn: Dict[str, ODataAssociation],
    ) -> NavigationTarget:
        # OData V4 declares the target and constraints directly on the property.
        if navigation.target_type_fqn is not None:
            return NavigationTarget(
                target_type_fqn=navigation.target_type_fqn,
                referential_constraints=navigation.referential_constraints,
            )

        # OData V2 resolves the target and constraints via a named Association.
        if navigation.relationship is None:
            return NavigationTarget()
        association = associations_by_fqn.get(navigation.relationship)
        if association is None:
            return NavigationTarget()
        target_type_fqn = next(
            (
                end.type_fqn
                for end in association.ends
                if end.role == navigation.to_role
            ),
            None,
        )
        return NavigationTarget(
            target_type_fqn=target_type_fqn,
            referential_constraints=association.referential_constraints,
        )

    def _custom_properties(
        self, entity_type: ODataEntityType, metadata: ODataMetadata
    ) -> Dict[str, str]:
        return {
            PROPERTY_ODATA_ENTITY_TYPE: entity_type.fqn,
            PROPERTY_ODATA_VERSION: metadata.version.value,
        }

    def _entity_set_dataset_name(self, entity_set: ODataEntitySet) -> str:
        return f"{entity_set.container_namespace}.{entity_set.name}"

    def _dataset_urn(self, name: str) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def _target_platform_detail(
        self, logical_system: str
    ) -> Optional[MdgTargetPlatform]:
        # A configured mapping wins; otherwise fall back to the well-known SAP tokens.
        override = self.config.logical_system_to_platform.get(logical_system)
        if override is not None:
            return override
        platform = KNOWN_LOGICAL_SYSTEM_TO_PLATFORM.get(logical_system.lower())
        return MdgTargetPlatform(platform=platform) if platform is not None else None

    def _target_dataset_urn(self, logical_system: str, name: str) -> Optional[str]:
        detail = self._target_platform_detail(logical_system)
        if detail is None:
            return None
        dataset_name = name.lower() if detail.convert_urns_to_lowercase else name
        return make_dataset_urn_with_platform_instance(
            platform=detail.platform,
            name=dataset_name,
            platform_instance=detail.platform_instance,
            env=detail.env or self.config.env,
        )

    def _emit_drf_lineage(
        self,
        service: str,
        entity_set: ODataEntitySet,
        entity_type: ODataEntityType,
    ) -> Iterable[MetadataWorkUnit]:
        if self._drf_distribution is None:
            return
        data_model = self.config.drf.service_to_data_model.get(
            self._service_id(service)
        )
        if data_model is None:
            return

        source_name = self._entity_set_dataset_name(entity_set)
        source_urn = self._dataset_urn(source_name)
        field_names = [prop.name for prop in entity_type.properties]
        for business_system in self._drf_distribution.targets_for(data_model):
            # The replicated object keeps its name on the target, so its urn is
            # deterministic from the mapped platform/instance/env. Systems without a
            # platform mapping cannot be resolved and are reported rather than guessed.
            downstream_urn = self._target_dataset_urn(business_system, source_name)
            if downstream_urn is None:
                self.report.report_target_system_unresolved(business_system)
                continue
            yield from self._emit_downstream_lineage(
                source_urn, downstream_urn, field_names
            )

    def _emit_downstream_lineage(
        self, source_urn: str, downstream_urn: str, field_names: List[str]
    ) -> Iterable[MetadataWorkUnit]:
        # MDG governs the master data and replicates it out, so it is the upstream of
        # the downstream system's dataset. Emitted as a PATCH so the edge is added
        # alongside — never overwriting — lineage the downstream's own connector sets.
        patch = DatasetPatchBuilder(downstream_urn)
        patch.add_upstream_lineage(
            UpstreamClass(dataset=source_urn, type=DatasetLineageTypeClass.COPY)
        )
        if self.config.drf.emit_column_lineage:
            matched = self._matched_downstream_fields(downstream_urn, field_names)
            for source_field, downstream_field in matched.items():
                patch.add_fine_grained_lineage(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        upstreams=[make_schema_field_urn(source_urn, source_field)],
                        downstreams=[
                            make_schema_field_urn(downstream_urn, downstream_field)
                        ],
                    )
                )
                self.report.column_lineage_edges_emitted += 1
        yield from self._patch_workunits(downstream_urn, patch)
        self.report.lineage_edges_emitted += 1
        if self.config.drf.emit_logical_parent:
            yield from self._emit_logical_parent(
                source_urn, downstream_urn, field_names
            )

    def _emit_logical_parent(
        self, source_urn: str, downstream_urn: str, field_names: List[str]
    ) -> Iterable[MetadataWorkUnit]:
        # MDG is the canonical logical model and each replicated downstream dataset is
        # one physical instantiation of it. The logicalParent aspect lives on the
        # physical child (the downstream dataset) and points to the logical parent
        # (the MDG entity), so this never overwrites the downstream connector's own
        # metadata. Column-level physical-instance links use the same case-insensitive
        # name match as column lineage.
        yield self._logical_parent_workunit(downstream_urn, source_urn)
        self.report.logical_parents_emitted += 1
        for source_field, downstream_field in self._matched_downstream_fields(
            downstream_urn, field_names
        ).items():
            yield self._logical_parent_workunit(
                make_schema_field_urn(downstream_urn, downstream_field),
                make_schema_field_urn(source_urn, source_field),
            )
            self.report.logical_parent_fields_emitted += 1

    def _logical_parent_workunit(
        self, child_urn: str, parent_urn: str
    ) -> MetadataWorkUnit:
        mcp = MetadataChangeProposalWrapper(
            entityUrn=child_urn,
            aspect=LogicalParentClass(parent=EdgeClass(destinationUrn=parent_urn)),
        )
        return MetadataWorkUnit(
            id=f"{child_urn}-logical-parent",
            mcp=mcp,
            is_primary_source=False,
        )

    def _matched_downstream_fields(
        self, downstream_urn: str, source_field_names: List[str]
    ) -> Dict[str, str]:
        # Column-level lineage is only emitted where a field of the same name exists on
        # the downstream dataset, read from DataHub. This keeps CLL correct by
        # construction even though MDG cannot see the target's physical schema.
        # Matching is case-insensitive because the downstream platform often cases
        # fields differently from MDG; the returned mapping keeps the MDG field name
        # and the downstream's real field path so each schemaField urn is correct.
        # ponytail: downstream fields differing only in case collapse to one entry
        # (last wins); acceptable for this best-effort name match.
        graph = self.ctx.graph
        if graph is None:
            return {}
        schema = graph.get_schema_metadata(downstream_urn)
        if schema is None:
            return {}
        downstream_by_fold = {
            field.fieldPath.casefold(): field.fieldPath for field in schema.fields
        }
        matched: Dict[str, str] = {}
        for name in source_field_names:
            downstream_field = downstream_by_fold.get(name.casefold())
            if downstream_field is not None:
                matched[name] = downstream_field
        return matched

    def _patch_workunits(
        self, dataset_urn: str, patch: DatasetPatchBuilder
    ) -> Iterable[MetadataWorkUnit]:
        # One MCP per patched aspect. is_primary_source is False because these
        # datasets belong to downstream systems, not to the MDG source.
        for mcp in patch.build():
            yield MetadataWorkUnit(
                id=f"{dataset_urn}-patch-{mcp.aspectName}",
                mcp_raw=mcp,
                is_primary_source=False,
            )

    def _service_id(self, service: str) -> str:
        stripped = SERVICE_PATH_STRIP_PATTERN.sub("", service)
        return stripped.split("/")[-1] or stripped

    def _service_key(self, service: str) -> SapMdgServiceKey:
        return SapMdgServiceKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            service=self._service_id(service),
        )
