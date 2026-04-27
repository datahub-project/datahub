import logging
from typing import Dict, Iterable, List, Optional, Set, Tuple

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigurationError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    add_entity_to_container,
    add_owner_to_entity_wu,
    add_tags_to_entity_wu,
    gen_containers,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIContainerSubTypes,
    DatasetSubTypes,
    SourceCapabilityModifier,
)
from datahub.ingestion.source.sigma.config import (
    PlatformDetail,
    SigmaSourceConfig,
    SigmaSourceReport,
    WorkspaceCounts,
)
from datahub.ingestion.source.sigma.data_classes import (
    DataModelElementUpstream,
    DataModelKey,
    DatasetUpstream,
    Element,
    Page,
    SheetUpstream,
    SigmaDataModel,
    SigmaDataModelElement,
    SigmaDataset,
    Workbook,
    WorkbookKey,
    Workspace,
    WorkspaceKey,
)
from datahub.ingestion.source.sigma.sigma_api import SigmaAPI
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    Status,
    SubTypes,
    TimeStamp,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    DatasetProperties,
    Upstream,
    UpstreamLineage,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ChangeAuditStampsClass,
    ChartInfoClass,
    DashboardInfoClass,
    DataPlatformInstanceClass,
    EdgeClass,
    GlobalTagsClass,
    InputFieldClass,
    InputFieldsClass,
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
)
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result
from datahub.utilities.urns.dataset_urn import DatasetUrn

# Logger instance
logger = logging.getLogger(__name__)


@platform_name("Sigma")
@config_class(SigmaSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(
    SourceCapability.CONTAINERS,
    "Enabled by default",
    subtype_modifier=[
        SourceCapabilityModifier.SIGMA_WORKSPACE,
        SourceCapabilityModifier.SIGMA_DATA_MODEL,
    ],
)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default.")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.TAGS, "Enabled by default")
@capability(
    SourceCapability.OWNERSHIP,
    "Enabled by default, configured using `ingest_owner`",
)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class SigmaSource(StatefulIngestionSourceBase, TestableSource):
    """
    This plugin extracts the following:
    - Sigma Workspaces and Workbooks as Container.
    - Sigma Datasets
    - Pages as Dashboard and its Elements as Charts
    - Sigma Data Models as Container, with one Dataset per element inside the Data Model.
    """

    config: SigmaSourceConfig
    reporter: SigmaSourceReport
    platform: str = "sigma"

    def __init__(self, config: SigmaSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.reporter = SigmaSourceReport()
        self.dataset_upstream_urn_mapping: Dict[str, List[str]] = {}
        # Sigma Dataset url_id -> dataset URN. Used to resolve DM element
        # ``inode-<urlId>`` upstreams.
        self.sigma_dataset_urn_by_url_id: Dict[str, str] = {}
        # DM urlId -> {lowercased element name: [element Dataset URN]}.
        # Bridges workbook ``data-model`` lineage nodes to the specific
        # element Dataset URN. A name may map to multiple URNs when a DM
        # has duplicate-named elements.
        self.dm_element_urn_by_name: Dict[str, Dict[str, List[str]]] = {}
        # DM urlId -> DM Container URN. Last-resort fallback.
        self.dm_container_urn_by_url_id: Dict[str, str] = {}
        # DM urlId -> total element count (includes blank-named elements
        # that are absent from ``dm_element_urn_by_name``). Used by the
        # cross-DM single-element fallback.
        self.dm_total_element_count_by_url_id: Dict[str, int] = {}
        # dataModelIds whose bridge key collided with an earlier DM. The
        # emit loop skips these to avoid unlinked orphan Containers.
        self.dm_collided_data_model_ids: Set[str] = set()
        # urlId prefixes that discovery fetched and then dropped (workspace-
        # or pattern-filtered). Used by the cross-DM resolver to distinguish
        # "operator asked to exclude this producer" from "producer missing".
        self.dm_excluded_url_ids: Set[str] = set()
        try:
            self.sigma_api = SigmaAPI(self.config, self.reporter)
        except Exception as e:
            raise ConfigurationError("Unable to connect sigma API") from e

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            SigmaAPI(
                SigmaSourceConfig.parse_obj_allow_extras(config_dict),
                SigmaSourceReport(),
            )
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    @classmethod
    def create(cls, config_dict, ctx):
        config = SigmaSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def _gen_workbook_key(self, workbook_id: str) -> WorkbookKey:
        return WorkbookKey(
            workbookId=workbook_id,
            platform=self.platform,
            instance=self.config.platform_instance,
        )

    def _gen_workspace_key(self, workspace_id: str) -> WorkspaceKey:
        return WorkspaceKey(
            workspaceId=workspace_id,
            platform=self.platform,
            instance=self.config.platform_instance,
        )

    def _get_allowed_workspaces(self) -> List[Workspace]:
        all_workspaces = self.sigma_api.workspaces.values()
        logger.info(f"Number of workspaces = {len(all_workspaces)}")

        allowed_workspaces = []
        for workspace in all_workspaces:
            if self.config.workspace_pattern.allowed(workspace.name):
                allowed_workspaces.append(workspace)
            else:
                self.reporter.workspaces.dropped(
                    f"{workspace.name} ({workspace.workspaceId})"
                )
        logger.info(f"Number of allowed workspaces = {len(allowed_workspaces)}")

        return allowed_workspaces

    def _gen_workspace_workunit(
        self, workspace: Workspace
    ) -> Iterable[MetadataWorkUnit]:
        """
        Map Sigma workspace to Datahub container
        """
        owner_username = self.sigma_api.get_user_name(workspace.createdBy)
        yield from gen_containers(
            container_key=self._gen_workspace_key(workspace.workspaceId),
            name=workspace.name,
            sub_types=[BIContainerSubTypes.SIGMA_WORKSPACE],
            owner_urn=(
                builder.make_user_urn(owner_username)
                if self.config.ingest_owner and owner_username
                else None
            ),
            created=int(workspace.createdAt.timestamp() * 1000),
            last_modified=int(workspace.updatedAt.timestamp() * 1000),
        )

    def _gen_sigma_dataset_urn(self, dataset_identifier: str) -> str:
        return builder.make_dataset_urn_with_platform_instance(
            name=dataset_identifier,
            env=self.config.env,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
        )

    def _gen_entity_status_aspect(self, entity_urn: str) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn, aspect=Status(removed=False)
        ).as_workunit()

    def _gen_dataset_properties(
        self, dataset_urn: str, dataset: SigmaDataset
    ) -> MetadataWorkUnit:
        dataset_properties = DatasetProperties(
            name=dataset.name,
            description=dataset.description,
            qualifiedName=dataset.name,
            externalUrl=dataset.url,
            created=TimeStamp(time=int(dataset.createdAt.timestamp() * 1000)),
            lastModified=TimeStamp(time=int(dataset.updatedAt.timestamp() * 1000)),
            customProperties={"datasetId": dataset.datasetId},
            tags=[dataset.badge] if dataset.badge else None,
        )
        if dataset.path:
            dataset_properties.customProperties["path"] = dataset.path
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=dataset_properties
        ).as_workunit()

    def _gen_dataplatform_instance_aspect(
        self, entity_urn: str
    ) -> Optional[MetadataWorkUnit]:
        if self.config.platform_instance:
            aspect = DataPlatformInstanceClass(
                platform=builder.make_data_platform_urn(self.platform),
                instance=builder.make_dataplatform_instance_urn(
                    self.platform, self.config.platform_instance
                ),
            )
            return MetadataChangeProposalWrapper(
                entityUrn=entity_urn, aspect=aspect
            ).as_workunit()
        else:
            return None

    def _gen_entity_owner_aspect(
        self, entity_urn: str, user_name: str
    ) -> MetadataWorkUnit:
        aspect = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=builder.make_user_urn(user_name),
                    type=OwnershipTypeClass.DATAOWNER,
                )
            ]
        )
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=aspect,
        ).as_workunit()

    def _gen_entity_browsepath_aspect(
        self,
        entity_urn: str,
        parent_entity_urn: str,
        paths: List[str],
    ) -> MetadataWorkUnit:
        entries = [
            BrowsePathEntryClass(id=parent_entity_urn, urn=parent_entity_urn)
        ] + [BrowsePathEntryClass(id=path) for path in paths]
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=BrowsePathsV2Class(entries),
        ).as_workunit()

    def _gen_dataset_workunit(
        self, dataset: SigmaDataset
    ) -> Iterable[MetadataWorkUnit]:
        dataset_urn = self._gen_sigma_dataset_urn(dataset.get_urn_part())
        # Record the url_id -> urn mapping for DM element ``inode-<urlId>``
        # upstream resolution.
        self.sigma_dataset_urn_by_url_id[dataset.get_urn_part()] = dataset_urn

        yield self._gen_entity_status_aspect(dataset_urn)

        yield self._gen_dataset_properties(dataset_urn, dataset)

        if dataset.workspaceId:
            self.reporter.workspaces.increment_datasets_count(dataset.workspaceId)
            yield from add_entity_to_container(
                container_key=self._gen_workspace_key(dataset.workspaceId),
                entity_type="dataset",
                entity_urn=dataset_urn,
            )

        dpi_aspect = self._gen_dataplatform_instance_aspect(dataset_urn)
        if dpi_aspect:
            yield dpi_aspect

        owner_username = self.sigma_api.get_user_name(dataset.createdBy)
        if self.config.ingest_owner and owner_username:
            yield self._gen_entity_owner_aspect(dataset_urn, owner_username)

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypes(typeNames=[DatasetSubTypes.SIGMA_DATASET]),
        ).as_workunit()

        if dataset.path and dataset.workspaceId:
            paths = dataset.path.split("/")[1:]
            if len(paths) > 0:
                yield self._gen_entity_browsepath_aspect(
                    entity_urn=dataset_urn,
                    parent_entity_urn=builder.make_container_urn(
                        self._gen_workspace_key(dataset.workspaceId)
                    ),
                    paths=paths,
                )

        if dataset.badge:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=GlobalTagsClass(
                    tags=[TagAssociationClass(builder.make_tag_urn(dataset.badge))]
                ),
            ).as_workunit()

    def _gen_data_model_key(self, data_model_id: str) -> DataModelKey:
        return DataModelKey(
            dataModelId=data_model_id,
            platform=self.platform,
            instance=self.config.platform_instance,
        )

    def _gen_data_model_element_urn(
        self, data_model: SigmaDataModel, element: SigmaDataModelElement
    ) -> str:
        return builder.make_dataset_urn_with_platform_instance(
            name=data_model.get_element_urn_part(element),
            env=self.config.env,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
        )

    def _resolve_dm_element_external_upstream(self, source_id: str) -> Optional[str]:
        """Resolve an ``inode-<suffix>`` source_id to a Sigma Dataset URN.

        Returns the URN if the suffix matches a Sigma Dataset ingested in
        this run; otherwise ``None``. We do not fabricate URNs for targets
        (un-ingested Sigma Datasets, warehouse tables) we didn't emit.
        """
        if not source_id.startswith("inode-"):
            return None
        suffix = source_id[len("inode-") :]
        return self.sigma_dataset_urn_by_url_id.get(suffix)

    def _resolve_dm_element_cross_dm_upstream(
        self,
        source_id: str,
        consuming_element: SigmaDataModelElement,
        consuming_data_model: SigmaDataModel,
    ) -> Tuple[Optional[str], str]:
        """Resolve a cross-DM ``sourceId`` (``<otherDmUrlId>/<suffix>``) to
        the referenced DM element Dataset URN.

        Returns ``(urn, outcome)``. Success outcomes: ``strict``,
        ``ambiguous``, ``single_element_fallback``. Failure outcomes:
        ``malformed``, ``self_reference``, ``consumer_name_missing``,
        ``dm_unknown``, ``name_unmatched_but_dm_known``. Callers bump the
        matching counter (for success outcomes, gated on dedup).
        """
        other_dm_url_id, _, suffix = source_id.partition("/")
        if not other_dm_url_id or not suffix:
            return None, "malformed"

        if (
            other_dm_url_id == consuming_data_model.get_url_id()
            or other_dm_url_id == consuming_data_model.dataModelId
        ):
            return None, "self_reference"

        if not consuming_element.name:
            # ``dm_unknown`` wins if the producer DM isn't registered.
            if other_dm_url_id not in self.dm_container_urn_by_url_id:
                return None, "dm_unknown"
            return None, "consumer_name_missing"

        # Registration (dm_container_urn_by_url_id) is the single source of
        # truth for ``dm_unknown``. ``dm_element_urn_by_name`` can legitimately
        # be ``{}`` for a registered DM with only blank-named elements, which
        # we must not conflate with "DM never registered".
        if other_dm_url_id not in self.dm_container_urn_by_url_id:
            # Distinguish "operator filtered this producer out" from
            # "producer missing" so report triage is not misleading.
            if other_dm_url_id in self.dm_excluded_url_ids:
                return None, "excluded_by_filter"
            return None, "dm_unknown"
        name_map = self.dm_element_urn_by_name.get(other_dm_url_id, {})

        candidates = name_map.get(consuming_element.name.lower())
        if not candidates:
            # Single-element fallback: if the producer DM has exactly one
            # element total (and one named), the reference is unambiguous.
            # Both checks are required: blank-named elements are excluded
            # from the name-map, so the total-count guard prevents a
            # spurious match when a DM has 1 named + N blank-named.
            total_elements = self.dm_total_element_count_by_url_id.get(other_dm_url_id)
            all_named_urns: List[str] = [
                urn for urns in name_map.values() for urn in urns
            ]
            if total_elements == 1 and len(all_named_urns) == 1:
                return all_named_urns[0], "single_element_fallback"
            return None, "name_unmatched_but_dm_known"

        if len(candidates) > 1:
            return sorted(candidates)[0], "ambiguous"
        return candidates[0], "strict"

    def _bump_cross_dm_success(self, outcome: str) -> None:
        """Bump cross-DM success counters. Callers are responsible for
        dedup-gating so diamond source_ids don't inflate the signal.
        ``ambiguous`` and ``single_element_fallback`` are sub-shapes of
        ``_resolved`` and both counters increment.
        """
        self.reporter.data_model_element_cross_dm_upstreams_resolved += 1
        if outcome == "ambiguous":
            self.reporter.data_model_element_cross_dm_upstreams_ambiguous += 1
        elif outcome == "single_element_fallback":
            self.reporter.data_model_element_cross_dm_upstreams_single_element_fallback += 1

    def _bump_cross_dm_failure(self, outcome: Optional[str]) -> None:
        """Bump cross-DM failure counters (one per source_id)."""
        if outcome == "self_reference":
            self.reporter.data_model_element_cross_dm_upstreams_self_reference += 1
        elif outcome == "consumer_name_missing":
            self.reporter.data_model_element_cross_dm_upstreams_consumer_name_missing += 1
        elif outcome == "dm_unknown":
            self.reporter.data_model_element_cross_dm_upstreams_dm_unknown += 1
        elif outcome == "excluded_by_filter":
            self.reporter.data_model_element_cross_dm_upstreams_excluded_by_filter += 1
        elif outcome == "name_unmatched_but_dm_known":
            self.reporter.data_model_element_cross_dm_upstreams_name_unmatched_but_dm_known += 1
        # ``malformed`` has no dedicated counter; falls into the caller's
        # generic ``data_model_element_upstreams_unresolved`` bump.

    def _gen_data_model_element_upstream_lineage(
        self,
        element: SigmaDataModelElement,
        data_model: SigmaDataModel,
        elementId_to_dataset_urn: Dict[str, str],
    ) -> Optional[UpstreamLineage]:
        # Success counters bump once per unique URN; diamond source_ids
        # resolving to the same URN should not inflate the signal.
        # Failure counters bump per source_id since each is a distinct
        # missing reference.
        upstream_urns: List[str] = []
        seen: Set[str] = set()
        for source_id in element.source_ids:
            upstream_urn: Optional[str] = None
            shape: str = ""
            cross_dm_outcome: Optional[str] = None
            if source_id in elementId_to_dataset_urn:
                upstream_urn = elementId_to_dataset_urn[source_id]
                shape = "intra"
            elif source_id.startswith("inode-"):
                upstream_urn = self._resolve_dm_element_external_upstream(source_id)
                shape = "external"
                if upstream_urn is None:
                    self.reporter.data_model_element_upstreams_unresolved += 1
                    logger.debug(
                        "DM %s element %s: external upstream %r unresolved",
                        data_model.dataModelId,
                        element.elementId,
                        source_id,
                    )
            elif "/" in source_id:
                upstream_urn, cross_dm_outcome = (
                    self._resolve_dm_element_cross_dm_upstream(
                        source_id, element, data_model
                    )
                )
                shape = "cross_dm"
                if upstream_urn is None:
                    self._bump_cross_dm_failure(cross_dm_outcome)
                    self.reporter.data_model_element_upstreams_unresolved += 1
                    logger.debug(
                        "DM %s element %s: cross-DM upstream %r unresolved (%s)",
                        data_model.dataModelId,
                        element.elementId,
                        source_id,
                        cross_dm_outcome,
                    )
            else:
                self.reporter.data_model_element_upstreams_unresolved += 1
                logger.debug(
                    "DM %s element %s: upstream source_id %r has an unknown shape",
                    data_model.dataModelId,
                    element.elementId,
                    source_id,
                )

            if upstream_urn and upstream_urn not in seen:
                upstream_urns.append(upstream_urn)
                seen.add(upstream_urn)
                if shape == "intra":
                    self.reporter.data_model_element_intra_upstreams += 1
                elif shape == "external":
                    self.reporter.data_model_element_external_upstreams += 1
                elif shape == "cross_dm" and cross_dm_outcome is not None:
                    self._bump_cross_dm_success(cross_dm_outcome)
                    if cross_dm_outcome == "ambiguous":
                        logger.warning(
                            "Ambiguous cross-DM element name %r for "
                            "consumer %s.%s; picked %s deterministically.",
                            element.name,
                            data_model.dataModelId,
                            element.elementId,
                            upstream_urn,
                        )

        if not upstream_urns:
            return None
        # Sort for deterministic emission order: Sigma's /lineage API does
        # not document ordering, and Upstream entries have no semantic order.
        upstream_urns.sort()
        return UpstreamLineage(
            upstreams=[
                Upstream(dataset=urn, type=DatasetLineageType.TRANSFORMED)
                for urn in upstream_urns
            ]
        )

    def _gen_data_model_element_schema_metadata(
        self, element_dataset_urn: str, element: SigmaDataModelElement
    ) -> MetadataWorkUnit:
        # Dedup by ``fieldPath`` within the element: Sigma can return two
        # columns with the same name (e.g. a calculated field shadowing a
        # native column), and GMS rejects / non-deterministically dedupes
        # duplicate ``fieldPath``s. Keep the first; count the rest.
        seen_field_paths: Set[str] = set()
        fields: List[SchemaFieldClass] = []
        for column in element.columns:
            if not column.name:
                continue
            if column.name in seen_field_paths:
                self.reporter.data_model_element_columns_duplicate_fieldpath_dropped += 1
                continue
            seen_field_paths.add(column.name)
            fields.append(
                SchemaFieldClass(
                    fieldPath=column.name,
                    type=SchemaFieldDataTypeClass(StringTypeClass()),
                    nativeDataType="String",
                    description=column.label or None,
                )
            )
        schema_metadata = SchemaMetadataClass(
            schemaName=element.name,
            platform=builder.make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
        )
        return MetadataChangeProposalWrapper(
            entityUrn=element_dataset_urn, aspect=schema_metadata
        ).as_workunit()

    def _gen_data_model_element_workunits(
        self,
        data_model: SigmaDataModel,
        data_model_key: DataModelKey,
        data_model_container_urn: str,
        elementId_to_dataset_urn: Dict[str, str],
    ) -> Iterable[MetadataWorkUnit]:
        dm_url_id = data_model.get_url_id()
        paths = data_model.path.split("/")[1:] if data_model.path else []
        workspace_container_urn: Optional[str] = None
        if data_model.workspaceId:
            workspace_container_urn = builder.make_container_urn(
                self._gen_workspace_key(data_model.workspaceId)
            )

        for element in data_model.elements:
            element_dataset_urn = elementId_to_dataset_urn[element.elementId]

            yield self._gen_entity_status_aspect(element_dataset_urn)

            # description is empty: Sigma's /elements API has no
            # element-level description. qualifiedName uses "/" as the
            # separator; names containing "/" are disambiguated by URN.
            element_properties = DatasetProperties(
                name=element.name,
                description="",
                qualifiedName=f"{data_model.name}/{element.name}",
                customProperties={
                    "dataModelId": data_model.dataModelId,
                    "dataModelUrlId": dm_url_id,
                    "elementId": element.elementId,
                    "type": element.type or "Unknown",
                },
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=element_dataset_urn, aspect=element_properties
            ).as_workunit()

            yield MetadataChangeProposalWrapper(
                entityUrn=element_dataset_urn,
                aspect=SubTypesClass(
                    typeNames=[DatasetSubTypes.SIGMA_DATA_MODEL_ELEMENT]
                ),
            ).as_workunit()

            dpi_aspect = self._gen_dataplatform_instance_aspect(element_dataset_urn)
            if dpi_aspect:
                yield dpi_aspect

            yield self._gen_data_model_element_schema_metadata(
                element_dataset_urn, element
            )

            yield from add_entity_to_container(
                container_key=data_model_key,
                entity_type="dataset",
                entity_urn=element_dataset_urn,
            )

            # BrowsePaths: workspace urn, DM path segments, DM Container
            # urn (typed so UI breadcrumbs are clickable), element name.
            # Orphan / personal-space DMs lack a workspace container; skip
            # that entry rather than duplicating the DM Container URN.
            browse_entries: List[BrowsePathEntryClass] = []
            if workspace_container_urn:
                browse_entries.append(
                    BrowsePathEntryClass(
                        id=workspace_container_urn, urn=workspace_container_urn
                    )
                )
            browse_entries.extend(BrowsePathEntryClass(id=path) for path in paths)
            browse_entries.append(
                BrowsePathEntryClass(
                    id=data_model_container_urn, urn=data_model_container_urn
                )
            )
            # ``element.name`` can be empty for anonymous worksheet joins;
            # fall back to the elementId so the breadcrumb always renders.
            browse_entries.append(
                BrowsePathEntryClass(id=element.name or element.elementId)
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=element_dataset_urn,
                aspect=BrowsePathsV2Class(browse_entries),
            ).as_workunit()

            upstream_lineage = self._gen_data_model_element_upstream_lineage(
                element, data_model, elementId_to_dataset_urn
            )
            if upstream_lineage is not None:
                yield MetadataChangeProposalWrapper(
                    entityUrn=element_dataset_urn, aspect=upstream_lineage
                ).as_workunit()

            self.reporter.data_model_elements_emitted += 1
            if data_model.workspaceId:
                self.reporter.workspaces.increment_data_model_elements_count(
                    data_model.workspaceId
                )

    def _prepopulate_dm_bridge_maps(self, data_model: SigmaDataModel) -> Dict[str, str]:
        """Populate the global DM bridge maps for one DM and return the
        local ``elementId -> Dataset URN`` map for intra-DM lineage.

        Called eagerly for every DM before any DM emits workunits so
        cross-DM and workbook-to-DM lineage resolve regardless of DM
        iteration order. Bridge keys use ``get_url_id()`` (the urlId slug
        when present, else the dataModelId UUID).
        """
        data_model_key = self._gen_data_model_key(data_model.dataModelId)
        data_model_container_urn = builder.make_container_urn(data_model_key)
        bridge_key = data_model.get_url_id()

        # ``urlId`` is a short slug; Sigma can reissue it after deletion.
        # On a true collision (different URN for the same key), keep the
        # first registration and warn. Re-registering the same DM is benign.
        existing_container = self.dm_container_urn_by_url_id.get(bridge_key)
        container_collision = (
            existing_container is not None
            and existing_container != data_model_container_urn
        )
        if container_collision:
            logger.warning(
                "Sigma DM bridge key %r already registered to %s; new DM "
                "%s (%s) resolves to %s. Keeping the first registration "
                "and skipping emission of the colliding DM.",
                bridge_key,
                existing_container,
                data_model.name,
                data_model.dataModelId,
                data_model_container_urn,
            )
            self.reporter.data_models_bridge_key_collision += 1
            self.dm_collided_data_model_ids.add(data_model.dataModelId)

        name_map: Dict[str, List[str]] = {}
        elementId_to_dataset_urn: Dict[str, str] = {}
        for element in data_model.elements:
            element_dataset_urn = self._gen_data_model_element_urn(data_model, element)
            elementId_to_dataset_urn[element.elementId] = element_dataset_urn
            # Blank-named elements are excluded from ``name_map`` so they
            # don't collapse into a single spuriously-ambiguous candidate.
            if element.name:
                name_map.setdefault(element.name.lower(), []).append(
                    element_dataset_urn
                )

        if container_collision:
            # Skip all bridge-map writes on collision so the first
            # registration wins uniformly.
            return elementId_to_dataset_urn

        self.dm_container_urn_by_url_id[bridge_key] = data_model_container_urn
        self.dm_element_urn_by_name[bridge_key] = name_map
        # Total count (including blank-named elements) used by the
        # cross-DM single-element fallback to verify "DM has exactly one
        # element" before attributing an unmatched-name reference.
        self.dm_total_element_count_by_url_id[bridge_key] = len(data_model.elements)

        return elementId_to_dataset_urn

    def _collect_unresolved_cross_dm_prefixes(
        self, data_models: List[SigmaDataModel]
    ) -> Set[str]:
        """Return cross-DM ``<prefix>`` values (from ``<prefix>/<suffix>``
        source_ids) whose DM is not yet registered. These point at
        personal-space or unlisted DMs that the discovery loop can fetch
        by urlId.

        ``inode-``-prefixed source_ids are skipped: those are the
        external-upstream shape (``inode-<datasetUrlId>`` / ``inode-<tableId>``)
        and would pollute ``data_model_external_reference_unresolved`` if
        attempted as a DM fetch.
        """
        unresolved: Set[str] = set()
        for data_model in data_models:
            for element in data_model.elements:
                for source_id in element.source_ids:
                    if "/" not in source_id:
                        continue
                    if source_id.startswith("inode-"):
                        continue
                    prefix, _, suffix = source_id.partition("/")
                    if not prefix or not suffix:
                        continue
                    if prefix in self.dm_container_urn_by_url_id:
                        continue
                    unresolved.add(prefix)
        return unresolved

    def _gen_data_model_workunit(
        self,
        data_model: SigmaDataModel,
        elementId_to_dataset_urn: Dict[str, str],
    ) -> Iterable[MetadataWorkUnit]:
        """Emit a DM as a Container plus one Dataset per element, with
        intra-DM, external, and cross-DM UpstreamLineage on each element.

        Caller must have invoked ``_prepopulate_dm_bridge_maps`` for every
        DM first so cross-DM / workbook-to-DM bridges resolve.
        """
        data_model_key = self._gen_data_model_key(data_model.dataModelId)
        data_model_container_urn = builder.make_container_urn(data_model_key)

        owner_username = (
            self.sigma_api.get_user_name(data_model.createdBy)
            if data_model.createdBy
            else None
        )
        parent_container_key: Optional[WorkspaceKey] = (
            self._gen_workspace_key(data_model.workspaceId)
            if data_model.workspaceId
            else None
        )
        extra_properties: Dict[str, str] = {
            "dataModelId": data_model.dataModelId,
            "dataModelUrlId": data_model.get_url_id(),
        }
        if data_model.latestVersion is not None:
            extra_properties["latestVersion"] = str(data_model.latestVersion)
        if data_model.path:
            extra_properties["path"] = data_model.path
        # Flag personal-space / unlisted DMs. Lowercase ``"true"`` matches
        # the JSON boolean convention used by other DataHub connectors.
        if data_model.workspaceId is None:
            extra_properties["isPersonalDataModel"] = "true"

        yield from gen_containers(
            container_key=data_model_key,
            name=data_model.name,
            sub_types=[BIContainerSubTypes.SIGMA_DATA_MODEL],
            parent_container_key=parent_container_key,
            description=data_model.description or "",
            external_url=data_model.url,
            extra_properties=extra_properties,
            owner_urn=(
                builder.make_user_urn(owner_username)
                if self.config.ingest_owner and owner_username
                else None
            ),
            tags=[data_model.badge] if data_model.badge else None,
            created=int(data_model.createdAt.timestamp() * 1000),
            last_modified=int(data_model.updatedAt.timestamp() * 1000),
        )

        if data_model.workspaceId:
            self.reporter.workspaces.increment_data_models_count(data_model.workspaceId)

        yield from self._gen_data_model_element_workunits(
            data_model,
            data_model_key,
            data_model_container_urn,
            elementId_to_dataset_urn,
        )

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def _gen_dashboard_urn(self, dashboard_identifier: str) -> str:
        return builder.make_dashboard_urn(
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            name=dashboard_identifier,
        )

    def _gen_dashboard_info_workunit(self, page: Page) -> MetadataWorkUnit:
        dashboard_urn = self._gen_dashboard_urn(page.get_urn_part())
        dashboard_info_cls = DashboardInfoClass(
            title=page.name,
            description="",
            charts=[
                builder.make_chart_urn(
                    platform=self.platform,
                    platform_instance=self.config.platform_instance,
                    name=element.get_urn_part(),
                )
                for element in page.elements
            ],
            lastModified=ChangeAuditStampsClass(),
            customProperties={"ElementsCount": str(len(page.elements))},
        )
        return MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn, aspect=dashboard_info_cls
        ).as_workunit()

    def _get_element_data_source_platform_details(
        self, full_path: str
    ) -> Optional[PlatformDetail]:
        data_source_platform_details: Optional[PlatformDetail] = None
        while full_path != "":
            if full_path in self.config.chart_sources_platform_mapping:
                data_source_platform_details = (
                    self.config.chart_sources_platform_mapping[full_path]
                )
                break
            else:
                full_path = "/".join(full_path.split("/")[:-1])
        if (
            not data_source_platform_details
            and "*" in self.config.chart_sources_platform_mapping
        ):
            data_source_platform_details = self.config.chart_sources_platform_mapping[
                "*"
            ]

        return data_source_platform_details

    def _resolve_dm_element_upstream_urn(
        self, upstream: DataModelElementUpstream
    ) -> Tuple[Optional[str], List[str]]:
        """Resolve a workbook element's DM upstream to a DM element
        Dataset URN. Returns ``(urn, candidates)``:

        - ``urn`` is None when the name cannot be matched
          (``ChartInfo.inputs`` requires Dataset URNs, not Container URNs).
        - ``candidates`` is the name-matched URN list (0 = miss, 1 = unique,
          >=2 = ambiguous). The caller uses it to bump
          ``element_dm_edge_ambiguous`` once per unique chart-to-DM edge.

        Candidates are sorted deterministically by their Dataset URN
        (which embeds ``dataModelId.elementId``), so the pick is stable
        across runs; within a single DM this degenerates to the lowest
        elementId.
        """
        dm_known = upstream.data_model_url_id in self.dm_container_urn_by_url_id
        name_map = self.dm_element_urn_by_name.get(upstream.data_model_url_id)

        if not upstream.name:
            if dm_known:
                self.reporter.element_dm_edge_upstream_name_missing += 1
            else:
                self.reporter.element_dm_edge_unresolved += 1
            return None, []

        if name_map:
            raw_candidates = name_map.get(upstream.name.lower())
            if raw_candidates:
                candidates = sorted(raw_candidates)
                return candidates[0], candidates

        if dm_known:
            self.reporter.element_dm_edge_name_unmatched_but_dm_known += 1
        else:
            self.reporter.element_dm_edge_unresolved += 1
        return None, []

    def _get_element_input_details(
        self,
        element: Element,
        workbook: Workbook,
        elementId_to_chart_urn: Dict[str, str],
    ) -> Tuple[Dict[str, List[str]], List[str]]:
        """
        Returns (dataset_inputs, chart_input_urns).

        dataset_inputs: Sigma Dataset / warehouse-table / DM element URNs to
            SQL-parsed warehouse URNs (non-empty only for Sigma Dataset
            upstreams matched against the SQL query; empty list otherwise).
        chart_input_urns: sorted list of chart URNs from intra-workbook sheet upstreams.
        """
        dataset_inputs: Dict[str, List[str]] = {}
        chart_input_urns: Set[str] = set()
        sql_parser_in_tables: List[str] = []

        data_source_platform_details = self._get_element_data_source_platform_details(
            f"{workbook.path}/{workbook.name}/{element.name}"
        )

        if element.query and data_source_platform_details:
            try:
                sql_parser_in_tables = create_lineage_sql_parsed_result(
                    query=element.query.strip(),
                    default_db=data_source_platform_details.default_db,
                    default_schema=data_source_platform_details.default_schema,
                    platform=data_source_platform_details.data_source_platform,
                    env=data_source_platform_details.env,
                    platform_instance=data_source_platform_details.platform_instance,
                    generate_column_lineage=False,
                ).in_tables
            except Exception:
                logger.debug(f"Unable to parse query of element {element.name}")

        for node_id, upstream in element.upstream_sources.items():
            if isinstance(upstream, DatasetUpstream):
                sigma_dataset_id = node_id.split("-")[-1]
                if not upstream.name:
                    # SQL-bridge cannot run without a name, so no chart-to-
                    # Sigma-dataset edge is emitted for this upstream.
                    self.reporter.chart_dataset_upstream_name_missing += 1
                    continue
                upstream_name_lower = upstream.name.lower()
                for in_table_urn in list(sql_parser_in_tables):
                    if (
                        DatasetUrn.from_string(in_table_urn).name.split(".")[-1]
                        in upstream_name_lower
                    ):
                        dataset_urn = self._gen_sigma_dataset_urn(sigma_dataset_id)
                        if dataset_urn not in dataset_inputs:
                            dataset_inputs[dataset_urn] = [in_table_urn]
                        else:
                            dataset_inputs[dataset_urn].append(in_table_urn)
                        sql_parser_in_tables.remove(in_table_urn)
            elif isinstance(upstream, SheetUpstream):
                chart_urn = elementId_to_chart_urn.get(upstream.element_id)
                if chart_urn is None:
                    # Target element type not in our allow-list (e.g. pivot-table).
                    logger.debug(
                        f"Upstream elementId {upstream.element_id} not in element map "
                        f"for element {element.name}; likely filtered by get_page_elements "
                        f"(allowlist: table, visualization)"
                    )
                    self.reporter.num_filtered_sheet_upstreams += 1
                    continue
                chart_input_urns.add(chart_urn)
            elif isinstance(upstream, DataModelElementUpstream):
                # Workbook element references a DM element. Failures are
                # surfaced through report counters.
                dm_urn, candidates = self._resolve_dm_element_upstream_urn(upstream)
                if dm_urn is not None:
                    if dm_urn in dataset_inputs:
                        # Diamond reference: same DM element reached via
                        # multiple lineage nodeIds on this chart.
                        self.reporter.element_dm_edges_deduped += 1
                    else:
                        dataset_inputs[dm_urn] = []
                        self.reporter.element_dm_edges_resolved += 1
                        # Count ambiguity once per chart-to-DM edge, not
                        # once per diamond sourceId.
                        if len(candidates) > 1:
                            self.reporter.element_dm_edge_ambiguous += 1
                            logger.warning(
                                "Ambiguous DM element name %r in DM %s: "
                                "%d candidates (%s). Picked lowest URN "
                                "%s deterministically.",
                                upstream.name,
                                upstream.data_model_url_id,
                                len(candidates),
                                ", ".join(sorted(candidates)),
                                dm_urn,
                            )

        # Unmatched SQL-parsed warehouse tables become direct dataset inputs.
        for in_table_urn in sql_parser_in_tables:
            dataset_inputs[in_table_urn] = []

        return dataset_inputs, sorted(chart_input_urns)

    def _gen_elements_workunit(
        self,
        elements: List[Element],
        workbook: Workbook,
        all_input_fields: List[InputFieldClass],
        paths: List[str],
        elementId_to_chart_urn: Dict[str, str],
    ) -> Iterable[MetadataWorkUnit]:
        """
        Map Sigma page element to Datahub Chart
        """
        for element in elements:
            chart_urn = builder.make_chart_urn(
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                name=element.get_urn_part(),
            )

            custom_properties = {
                "VizualizationType": str(element.vizualizationType),
                "type": str(element.type) if element.type else "Unknown",
            }

            yield self._gen_entity_status_aspect(chart_urn)

            dataset_inputs, chart_input_urns = self._get_element_input_details(
                element, workbook, elementId_to_chart_urn
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=ChartInfoClass(
                    title=element.name,
                    description="",
                    lastModified=ChangeAuditStampsClass(),
                    customProperties=custom_properties,
                    externalUrl=element.url,
                    inputs=sorted(dataset_inputs.keys()),
                    inputEdges=(
                        [
                            EdgeClass(destinationUrn=urn, sourceUrn=chart_urn)
                            for urn in chart_input_urns
                        ]
                        if chart_input_urns
                        else None
                    ),
                ),
            ).as_workunit()

            if workbook.workspaceId:
                self.reporter.workspaces.increment_elements_count(workbook.workspaceId)

                yield self._gen_entity_browsepath_aspect(
                    entity_urn=chart_urn,
                    parent_entity_urn=builder.make_container_urn(
                        self._gen_workspace_key(workbook.workspaceId)
                    ),
                    paths=paths + [workbook.name],
                )

            # Only Sigma Dataset URNs (with SQL-matched warehouse upstreams) need
            # the cross-entity UpstreamLineage aspect emitted later.
            for dataset_urn, warehouse_urns in dataset_inputs.items():
                if (
                    warehouse_urns
                    and dataset_urn not in self.dataset_upstream_urn_mapping
                ):
                    self.dataset_upstream_urn_mapping[dataset_urn] = warehouse_urns

            element_input_fields = [
                InputFieldClass(
                    schemaFieldUrn=builder.make_schema_field_urn(chart_urn, column),
                    schemaField=SchemaFieldClass(
                        fieldPath=column,
                        type=SchemaFieldDataTypeClass(StringTypeClass()),
                        nativeDataType="String",  # Make type default as Sting
                    ),
                )
                for column in element.columns
            ]

            yield MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=InputFieldsClass(fields=element_input_fields),
            ).as_workunit()

            all_input_fields.extend(element_input_fields)

    def _gen_pages_workunit(
        self, workbook: Workbook, paths: List[str]
    ) -> Iterable[MetadataWorkUnit]:
        """
        Map Sigma workbook page to Datahub dashboard
        """
        # Built once at workbook scope since intra-workbook lineage can
        # cross pages. Keys mirror the chart-emission allowlist in
        # get_page_elements (type in {"table","visualization"}).
        elementId_to_chart_urn: Dict[str, str] = {
            element.elementId: builder.make_chart_urn(
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                name=element.get_urn_part(),
            )
            for page in workbook.pages
            for element in page.elements
        }

        for page in workbook.pages:
            dashboard_urn = self._gen_dashboard_urn(page.get_urn_part())

            yield self._gen_entity_status_aspect(dashboard_urn)

            yield self._gen_dashboard_info_workunit(page)

            dpi_aspect = self._gen_dataplatform_instance_aspect(dashboard_urn)
            if dpi_aspect:
                yield dpi_aspect

            all_input_fields: List[InputFieldClass] = []

            if workbook.workspaceId:
                self.reporter.workspaces.increment_pages_count(workbook.workspaceId)
                yield self._gen_entity_browsepath_aspect(
                    entity_urn=dashboard_urn,
                    parent_entity_urn=builder.make_container_urn(
                        self._gen_workspace_key(workbook.workspaceId)
                    ),
                    paths=paths + [workbook.name],
                )

            yield from self._gen_elements_workunit(
                page.elements, workbook, all_input_fields, paths, elementId_to_chart_urn
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=dashboard_urn,
                aspect=InputFieldsClass(fields=all_input_fields),
            ).as_workunit()

    def _gen_workbook_workunit(self, workbook: Workbook) -> Iterable[MetadataWorkUnit]:
        """
        Map Sigma Workbook to Datahub container
        """
        owner_username = self.sigma_api.get_user_name(workbook.ownerId)

        dashboard_urn = self._gen_dashboard_urn(workbook.workbookId)

        yield self._gen_entity_status_aspect(dashboard_urn)

        lastModified = AuditStampClass(
            time=int(workbook.updatedAt.timestamp() * 1000),
            actor="urn:li:corpuser:datahub",
        )
        created = AuditStampClass(
            time=int(workbook.createdAt.timestamp() * 1000),
            actor="urn:li:corpuser:datahub",
        )

        dashboard_info_cls = DashboardInfoClass(
            title=workbook.name,
            description=workbook.description if workbook.description else "",
            dashboards=[
                EdgeClass(
                    destinationUrn=self._gen_dashboard_urn(page.get_urn_part()),
                    sourceUrn=dashboard_urn,
                )
                for page in workbook.pages
            ],
            externalUrl=workbook.url,
            lastModified=ChangeAuditStampsClass(
                created=created, lastModified=lastModified
            ),
            customProperties={
                "path": workbook.path,
                "latestVersion": str(workbook.latestVersion),
            },
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn, aspect=dashboard_info_cls
        ).as_workunit()

        # Set subtype
        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=SubTypesClass(typeNames=[BIContainerSubTypes.SIGMA_WORKBOOK]),
        ).as_workunit()

        # Ownership
        owner_urn = (
            builder.make_user_urn(owner_username)
            if self.config.ingest_owner and owner_username
            else None
        )
        if owner_urn:
            yield from add_owner_to_entity_wu(
                entity_type="dashboard",
                entity_urn=dashboard_urn,
                owner_urn=owner_urn,
            )

        # Tags
        tags = [workbook.badge] if workbook.badge else None
        if tags:
            yield from add_tags_to_entity_wu(
                entity_type="dashboard",
                entity_urn=dashboard_urn,
                tags=sorted(tags),
            )

        paths = workbook.path.split("/")[1:]
        if workbook.workspaceId:
            self.reporter.workspaces.increment_workbooks_count(workbook.workspaceId)

            yield self._gen_entity_browsepath_aspect(
                entity_urn=dashboard_urn,
                parent_entity_urn=builder.make_container_urn(
                    self._gen_workspace_key(workbook.workspaceId)
                ),
                paths=paths + [workbook.name],
            )

            if len(paths) == 0:
                yield from add_entity_to_container(
                    container_key=self._gen_workspace_key(workbook.workspaceId),
                    entity_type="dashboard",
                    entity_urn=dashboard_urn,
                )

        yield from self._gen_pages_workunit(workbook, paths)

    def _gen_sigma_dataset_upstream_lineage_workunit(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        for (
            dataset_urn,
            upstream_dataset_urns,
        ) in self.dataset_upstream_urn_mapping.items():
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineage(
                    upstreams=[
                        Upstream(
                            dataset=upstream_dataset_urn, type=DatasetLineageType.COPY
                        )
                        for upstream_dataset_urn in upstream_dataset_urns
                    ],
                ),
            ).as_workunit()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """DataHub Ingestion framework entry point."""
        logger.info("Sigma plugin execution is started")
        self.sigma_api.fill_workspaces()

        for dataset in self.sigma_api.get_sigma_datasets():
            yield from self._gen_dataset_workunit(dataset)
        # Data Models are emitted before Workbooks so the workbook-to-DM
        # bridge is populated before workbook elements resolve upstreams.
        # Two-pass: populate bridges for every DM first, then emit. Required
        # because DMs can forward-reference each other and ``/dataModels``
        # is not ordered by dependency.
        if self.config.ingest_data_models:
            # Accumulates every DM that will emit: listed DMs plus any
            # personal-space / unlisted DMs reached via discovery below.
            all_data_models: List[SigmaDataModel] = self.sigma_api.get_data_models()
            elementId_maps_by_dm: Dict[str, Dict[str, str]] = {}

            # Discovery loop: prepopulate bridges for every known DM, then
            # fetch any unresolved cross-DM ``<prefix>`` (personal-space
            # DMs absent from /v2/dataModels, or listings that failed),
            # register them, and repeat until stable. Each iteration scans
            # only ``pending`` (newly-added DMs) to keep the walk O(N) in
            # source_ids. ``unresolved_seen`` prevents failed-fetch retries.
            unresolved_seen: Set[str] = set()
            pending = list(all_data_models)
            while pending:
                for dm in pending:
                    # Defensive against re-enqueue via a different urlId
                    # for the same dataModelId.
                    if dm.dataModelId in elementId_maps_by_dm:
                        continue
                    elementId_maps_by_dm[dm.dataModelId] = (
                        self._prepopulate_dm_bridge_maps(dm)
                    )

                # Short-circuit: discovery can't yield an emittable DM when
                # ``ingest_shared_entities=False`` (every candidate is either
                # a personal-space DM or a workspace_pattern-denied one).
                # Skip the per-prefix fetches entirely. Bridges for listed
                # DMs remain populated from the prepopulate step above.
                if not self.config.ingest_shared_entities:
                    break

                new_unresolved = self._collect_unresolved_cross_dm_prefixes(pending)
                # Sort for deterministic discovery order (set iteration is
                # hash-randomized across Python interpreter runs).
                unresolved = sorted(new_unresolved - unresolved_seen)
                unresolved_seen |= new_unresolved
                pending = []
                for prefix in unresolved:
                    discovered_dm = self.sigma_api.get_data_model_by_url_id(prefix)
                    if discovered_dm is None:
                        self.reporter.data_model_external_reference_unresolved += 1
                        continue
                    # Re-apply ``workspace_pattern`` so a denied workspace
                    # cannot leak DMs via cross-DM references. If the DM
                    # has a workspaceId but the workspace is unreachable
                    # (admin-only / deleted), mirror the listed-DM path:
                    # count under ``data_models_without_workspace``.
                    if discovered_dm.workspaceId:
                        workspace = self.sigma_api.get_workspace(
                            discovered_dm.workspaceId
                        )
                        if workspace and not self.config.workspace_pattern.allowed(
                            workspace.name
                        ):
                            self.reporter.data_models.dropped(
                                f"{discovered_dm.name} ({discovered_dm.dataModelId}) "
                                f"in {workspace.name} (discovered, workspace_pattern denied)"
                            )
                            self.dm_excluded_url_ids.add(prefix)
                            continue
                        if workspace is None:
                            self.reporter.data_models_without_workspace += 1
                    if not self.config.data_model_pattern.allowed(discovered_dm.name):
                        self.reporter.data_models.dropped(
                            f"{discovered_dm.name} ({discovered_dm.dataModelId}) "
                            f"(discovered, filtered by data_model_pattern)"
                        )
                        self.dm_excluded_url_ids.add(prefix)
                        continue
                    self.reporter.data_model_external_references_discovered += 1
                    all_data_models.append(discovered_dm)
                    pending.append(discovered_dm)

            # Cheap insurance against a theoretical duplicate where the
            # same DM lands in ``all_data_models`` via both the listed and
            # discovered paths (relies on Sigma always returning ``urlId``
            # on ``/v2/dataModels``; if that ever changes we still emit
            # once per ``dataModelId``).
            emitted_data_model_ids: Set[str] = set()
            for data_model in all_data_models:
                if data_model.dataModelId in self.dm_collided_data_model_ids:
                    # First DM owns the bridge key; emitting this one would
                    # produce an unlinked orphan. Counter already bumped.
                    continue
                if data_model.dataModelId in emitted_data_model_ids:
                    continue
                emitted_data_model_ids.add(data_model.dataModelId)
                yield from self._gen_data_model_workunit(
                    data_model, elementId_maps_by_dm[data_model.dataModelId]
                )
        for workbook in self.sigma_api.get_sigma_workbooks():
            yield from self._gen_workbook_workunit(workbook)

        for workspace in self._get_allowed_workspaces():
            self.reporter.workspaces.processed(
                f"{workspace.name} ({workspace.workspaceId})"
            )
            yield from self._gen_workspace_workunit(workspace)
            if self.reporter.workspaces.workspace_counts.get(
                workspace.workspaceId, WorkspaceCounts()
            ).is_empty():
                logger.warning(
                    f"Workspace {workspace.name} ({workspace.workspaceId}) is empty. If this is not expected, add the user associated with the Client ID/Secret to each workspace with missing metadata"
                )
                self.reporter.empty_workspaces.append(
                    f"{workspace.name} ({workspace.workspaceId})"
                )
        yield from self._gen_sigma_dataset_upstream_lineage_workunit()

    def get_report(self) -> SourceReport:
        return self.reporter
