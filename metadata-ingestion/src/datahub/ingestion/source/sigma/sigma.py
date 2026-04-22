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
        # Maps Sigma Dataset url_id → dataset URN. Populated as SigmaDataset
        # workunits are emitted, consumed when resolving DM element
        # ``inode-<urlId>`` upstreams.
        self.sigma_dataset_urn_by_url_id: Dict[str, str] = {}
        # Maps DM urlId → {lowercased DM element name → [element Dataset URN]}.
        # Populated during DM ingestion; consumed by workbook element lineage
        # resolution to bridge workbook ``data-model`` lineage nodes to the
        # specific element Dataset URN. A single name may resolve to multiple
        # URNs when a DM has duplicate-named elements (Sigma coalesces such
        # references at the API contract level — see ticket §"Same-named DM
        # elements").
        self.dm_element_urn_by_name: Dict[str, Dict[str, List[str]]] = {}
        # Maps DM urlId → DM Container URN. Used as the last-resort fallback
        # for workbook elements that reference a DM without a resolvable
        # element name.
        self.dm_container_urn_by_url_id: Dict[str, str] = {}
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
        # Track the url-id → urn mapping so DM element ``inode-<urlId>``
        # upstreams can resolve to the existing SigmaDataset URN shape.
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

    def _resolve_dm_element_external_upstream(
        self, source_id: str, data_model: SigmaDataModel
    ) -> Optional[str]:
        """
        Resolve a DM element source_id of shape ``inode-<suffix>`` to an
        upstream URN. If the suffix matches a known Sigma Dataset url_id,
        we return the SigmaDataset URN; otherwise (warehouse table / unknown)
        we return None. Warehouse-table upstreams currently require SQL parsing
        that the DM API does not expose, so they are counted as unresolved
        rather than emitted as phantom URNs.
        """
        if not source_id.startswith("inode-"):
            return None
        suffix = source_id[len("inode-") :]
        # Case 1: matches a previously-emitted Sigma Dataset url_id.
        sigma_dataset_urn = self.sigma_dataset_urn_by_url_id.get(suffix)
        if sigma_dataset_urn:
            return sigma_dataset_urn
        # Case 2: lineage /entries recorded a ``type: dataset`` entry for this
        # nodeId — treat as a Sigma Dataset URL-id even if it wasn't processed
        # through get_sigma_datasets (e.g. filtered out).
        ext = data_model.external_sources.get(source_id)
        if ext and ext.get("type") == "dataset":
            return self._gen_sigma_dataset_urn(suffix)
        return None

    def _resolve_dm_element_cross_dm_upstream(
        self,
        source_id: str,
        consuming_element: SigmaDataModelElement,
        consuming_data_model: SigmaDataModel,
    ) -> Optional[str]:
        """
        Resolve a DM element ``sourceId`` of shape ``<otherDmUrlId>/<suffix>``
        (cross-DM reference — DM-A's element imports a table from DM-B) to
        the referenced DM element Dataset URN.

        Sigma's ``/dataModels/{id}/lineage`` endpoint emits this exact shape
        when one DM element reads from an element in another DM (live-probed
        2026-04-22). The suffix is opaque — Sigma does not expose a public
        endpoint that maps it to an ``elementId`` — so we fall back to the
        name-bridge used by the workbook→DM path: use the consuming
        element's own ``name`` (Sigma's default names the imported table
        after the source element) and resolve against
        ``dm_element_urn_by_name``. User rename degrades to the
        ``data_model_element_cross_dm_upstreams_name_unmatched_but_dm_known``
        counter. Self-reference (``<selfUrlId>/<suffix>``) is not expected
        from the real API but is guarded defensively.

        Returns None on any failure path; each failure bumps a distinct
        sub-counter so report triage can distinguish rename vs missing DM.
        """
        other_dm_url_id, _, suffix = source_id.partition("/")
        if not other_dm_url_id or not suffix:
            return None

        self_url_id = consuming_data_model.get_url_id()
        if other_dm_url_id in {self_url_id, consuming_data_model.dataModelId}:
            return None

        if not consuming_element.name:
            if other_dm_url_id in self.dm_container_urn_by_url_id:
                self.reporter.data_model_element_cross_dm_upstreams_name_unmatched_but_dm_known += 1
            else:
                self.reporter.data_model_element_cross_dm_upstreams_dm_unknown += 1
            return None

        name_map = self.dm_element_urn_by_name.get(other_dm_url_id)
        if not name_map:
            self.reporter.data_model_element_cross_dm_upstreams_dm_unknown += 1
            return None

        candidates = name_map.get(consuming_element.name.lower())
        if not candidates:
            self.reporter.data_model_element_cross_dm_upstreams_name_unmatched_but_dm_known += 1
            return None

        if len(candidates) > 1:
            self.reporter.data_model_element_cross_dm_upstreams_ambiguous += 1
            logger.warning(
                "Ambiguous cross-DM element name %r in DM %s — %d candidates "
                "(%s). Picking lowest elementId deterministically.",
                consuming_element.name,
                other_dm_url_id,
                len(candidates),
                ", ".join(candidates),
            )
        return sorted(candidates)[0]

    def _gen_data_model_element_upstream_lineage(
        self,
        element: SigmaDataModelElement,
        data_model: SigmaDataModel,
        elementId_to_dataset_urn: Dict[str, str],
    ) -> Optional[UpstreamLineage]:
        upstream_urns: List[str] = []
        seen: Set[str] = set()
        for source_id in element.source_ids:
            upstream_urn: Optional[str] = None
            if source_id in elementId_to_dataset_urn:
                upstream_urn = elementId_to_dataset_urn[source_id]
                self.reporter.data_model_element_intra_upstreams += 1
            elif source_id.startswith("inode-"):
                upstream_urn = self._resolve_dm_element_external_upstream(
                    source_id, data_model
                )
                if upstream_urn:
                    self.reporter.data_model_element_external_upstreams += 1
                else:
                    self.reporter.data_model_element_upstreams_unresolved += 1
                    logger.debug(
                        "DM %s element %s: external upstream %r unresolved "
                        "(inode not in sigma_dataset_urn_by_url_id and not a "
                        "type=dataset external source)",
                        data_model.dataModelId,
                        element.elementId,
                        source_id,
                    )
            elif "/" in source_id:
                upstream_urn = self._resolve_dm_element_cross_dm_upstream(
                    source_id, element, data_model
                )
                if upstream_urn:
                    self.reporter.data_model_element_cross_dm_upstreams_resolved += 1
                else:
                    self.reporter.data_model_element_upstreams_unresolved += 1
                    logger.debug(
                        "DM %s element %s: cross-DM upstream %r unresolved",
                        data_model.dataModelId,
                        element.elementId,
                        source_id,
                    )
            else:
                self.reporter.data_model_element_upstreams_unresolved += 1
                logger.debug(
                    "DM %s element %s: upstream source_id %r has an unknown "
                    "shape (not an intra-DM elementId, not inode-prefixed, "
                    "and not a <dmUrlId>/<suffix> cross-DM ref); skipping",
                    data_model.dataModelId,
                    element.elementId,
                    source_id,
                )

            if upstream_urn and upstream_urn not in seen:
                upstream_urns.append(upstream_urn)
                seen.add(upstream_urn)

        if not upstream_urns:
            return None
        return UpstreamLineage(
            upstreams=[
                Upstream(dataset=urn, type=DatasetLineageType.TRANSFORMED)
                for urn in upstream_urns
            ]
        )

    def _gen_data_model_element_schema_metadata(
        self, element_dataset_urn: str, element: SigmaDataModelElement
    ) -> MetadataWorkUnit:
        # Columns are scoped to this element's Dataset URN, so bare column
        # names are safe — no cross-element collision possible.
        fields = [
            SchemaFieldClass(
                fieldPath=column.name,
                type=SchemaFieldDataTypeClass(StringTypeClass()),
                nativeDataType="String",
                description=column.label or None,
            )
            for column in element.columns
            if column.name
        ]
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

            # description intentionally empty: the Sigma DM /elements API does
            # not expose an element-level description field (verified during
            # T2 investigation). qualifiedName uses "/" as the separator; DM
            # or element names containing "/" will produce ambiguous
            # qualifiedNames but remain unique via the URN.
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

            # BrowsePaths: workspace (urn) → DM path string segments → DM
            # Container (urn) → element name (string leaf). The DM Container
            # must appear as a typed entry so UI breadcrumbs are clickable
            # and navigate to the Data Model Container entity page;
            # plain-string segments render as inert folders.
            browse_parent_urn = (
                workspace_container_urn
                if workspace_container_urn
                else data_model_container_urn
            )
            browse_entries = [
                BrowsePathEntryClass(id=browse_parent_urn, urn=browse_parent_urn),
                *[BrowsePathEntryClass(id=path) for path in paths],
                BrowsePathEntryClass(
                    id=data_model_container_urn, urn=data_model_container_urn
                ),
                BrowsePathEntryClass(id=element.name),
            ]
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
        """
        Populate the global DM bridge maps (``dm_container_urn_by_url_id``
        and ``dm_element_urn_by_name``) for a single DM and return the local
        ``elementId → Dataset URN`` map used for intra-DM lineage resolution.

        Called eagerly for every DM *before* any DM emits workunits, so
        cross-DM lineage (DM-A references DM-B's element) resolves regardless
        of the order ``get_data_models`` returns DMs in — Sigma does not sort
        ``/dataModels`` by dependency. Same global maps are reused by the
        workbook→DM bridge, which also depends on every DM being registered
        before workbook emission starts.

        Register mappings under both ``urlId`` and ``dataModelId`` keys:
        lineage ``sourceId`` prefixes and workbook lineage node ids use
        ``urlId``, but if ``urlId`` is missing (``get_url_id`` falls back to
        ``dataModelId``) the bridge still resolves. The set collapses the
        redundant case.
        """
        data_model_key = self._gen_data_model_key(data_model.dataModelId)
        data_model_container_urn = builder.make_container_urn(data_model_key)
        dm_url_id = data_model.get_url_id()

        bridge_keys = {dm_url_id, data_model.dataModelId}
        for key in bridge_keys:
            self.dm_container_urn_by_url_id[key] = data_model_container_urn

        name_map: Dict[str, List[str]] = {}
        elementId_to_dataset_urn: Dict[str, str] = {}
        for element in data_model.elements:
            element_dataset_urn = self._gen_data_model_element_urn(data_model, element)
            elementId_to_dataset_urn[element.elementId] = element_dataset_urn
            # Skip elements with blank names to avoid collapsing multiple
            # nameless elements into one candidate list (which would otherwise
            # spuriously trip the ``element_dm_edge_ambiguous`` counter).
            if element.name:
                name_map.setdefault(element.name.lower(), []).append(
                    element_dataset_urn
                )
        for key in bridge_keys:
            self.dm_element_urn_by_name[key] = name_map

        return elementId_to_dataset_urn

    def _gen_data_model_workunit(
        self,
        data_model: SigmaDataModel,
        elementId_to_dataset_urn: Dict[str, str],
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit a Sigma Data Model as a Container (parallel to Workspaces) plus
        one Dataset per element inside it. Intra-DM, external, and cross-DM
        upstream lineage is wired via each element's UpstreamLineage aspect.

        Caller is expected to have invoked ``_prepopulate_dm_bridge_maps``
        for every DM first (see docstring there), so both the workbook→DM
        bridge and the DM→DM bridge resolve regardless of iteration order.
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

        yield from gen_containers(
            container_key=data_model_key,
            name=data_model.name,
            sub_types=[BIContainerSubTypes.SIGMA_DATA_MODEL],
            parent_container_key=parent_container_key,
            description=data_model.description,
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
    ) -> Optional[str]:
        """
        Resolve a workbook element's DM upstream (from a ``data-model`` lineage
        node) to a DM element **Dataset** URN. Returns None when the DM element
        name cannot be matched — ``ChartInfo.inputs`` is schema-typed to accept
        only Dataset URNs, so no lineage edge is emitted in the unmatched case.
        Counters surface the different failure shapes for report triage:

        - ``element_dm_edge_name_unmatched_but_dm_known``: DM was ingested, but
          the specific element name did not match any emitted DM element.
        - ``element_dm_edge_unresolved``: DM itself is not tracked by this
          ingestion run (filtered by pattern, or never fetched).

        Name-based match: Sigma coalesces workbook references to same-named DM
        elements into the first-by-creation match at the API contract level, so
        name-based matching is sufficient (see ticket §"Same-named DM elements").
        Candidates are sorted deterministically (by ``elementId``) to avoid
        run-to-run drift driven by the ordering of ``/elements`` responses.

        The caller is responsible for the ``element_dm_edges_resolved`` /
        ``element_dm_edges_deduped`` counters, since dedup happens one level
        up (multiple sourceIds on a single chart can point at the same DM
        element).
        """
        name_map = self.dm_element_urn_by_name.get(upstream.data_model_url_id)
        if name_map and upstream.name:
            candidates = name_map.get(upstream.name.lower())
            if candidates:
                if len(candidates) > 1:
                    self.reporter.element_dm_edge_ambiguous += 1
                    logger.warning(
                        "Ambiguous DM element name %r in DM %s — %d candidates "
                        "(%s). Picking lowest elementId deterministically; "
                        "consider renaming or merging the duplicates in Sigma.",
                        upstream.name,
                        upstream.data_model_url_id,
                        len(candidates),
                        ", ".join(candidates),
                    )
                return sorted(candidates)[0]

        if upstream.data_model_url_id in self.dm_container_urn_by_url_id:
            self.reporter.element_dm_edge_name_unmatched_but_dm_known += 1
        else:
            self.reporter.element_dm_edge_unresolved += 1
        return None

    def _get_element_input_details(
        self,
        element: Element,
        workbook: Workbook,
        elementId_to_chart_urn: Dict[str, str],
    ) -> Tuple[Dict[str, List[str]], List[str]]:
        """
        Returns (dataset_inputs, chart_input_urns).

        dataset_inputs: Sigma Dataset / warehouse-table / DM element URNs →
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
                for in_table_urn in list(sql_parser_in_tables):
                    if (
                        DatasetUrn.from_string(in_table_urn).name.split(".")[-1]
                        in upstream.name.lower()
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
                # Workbook element references a DM element (e.g. through the
                # Sigma app's "use data model" action). ChartInfo.inputs is
                # union[DatasetUrn] — the resolver returns only DM element
                # Dataset URNs; failures (DM unknown or name unmatched) are
                # surfaced through report counters.
                dm_urn = self._resolve_dm_element_upstream_urn(upstream)
                if dm_urn is not None:
                    if dm_urn in dataset_inputs:
                        # Diamond reference — same DM element reached via
                        # multiple lineage nodeIds on this chart. Count the
                        # skip so the report distinguishes genuine
                        # unresolved-vs-deduped outcomes.
                        self.reporter.element_dm_edges_deduped += 1
                    else:
                        dataset_inputs[dm_urn] = []
                        self.reporter.element_dm_edges_resolved += 1

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
                    inputs=list(dataset_inputs.keys()),
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
        # Build once at workbook scope — intra-workbook lineage can cross pages so
        # this map must cover all elements before any individual page is processed.
        # Keys intentionally mirror the chart-emission allow-list in get_page_elements
        # (type in {"table","visualization"}), so filtered element types (pivot-table,
        # input-table, etc.) are absent from both the map and the emitted chart entities.
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
        """
        Datahub Ingestion framework invoke this method
        """
        logger.info("Sigma plugin execution is started")
        self.sigma_api.fill_workspaces()

        for dataset in self.sigma_api.get_sigma_datasets():
            yield from self._gen_dataset_workunit(dataset)
        # Ingest Data Models before Workbooks so the workbook → DM element
        # bridge map (``dm_element_urn_by_name``, ``dm_container_urn_by_url_id``)
        # is populated before workbook elements resolve their upstreams.
        #
        # Two-pass: first fully populate the bridge maps for *every* DM,
        # then emit workunits. This is required because a DM's element can
        # reference another DM (cross-DM lineage, live-probed 2026-04-22)
        # and Sigma does not order ``/dataModels`` by dependency, so a
        # naive single-pass would miss any forward reference.
        if self.config.ingest_data_models:
            data_models = self.sigma_api.get_data_models()
            elementId_maps_by_dm: Dict[str, Dict[str, str]] = {}
            for data_model in data_models:
                elementId_maps_by_dm[data_model.dataModelId] = (
                    self._prepopulate_dm_bridge_maps(data_model)
                )
            for data_model in data_models:
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
