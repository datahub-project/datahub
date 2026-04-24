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
    DataModelKey,
    DatasetUpstream,
    Element,
    Page,
    SheetUpstream,
    SigmaDataModel,
    SigmaDataModelColumn,
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
    NullTypeClass,
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


# The Sigma ``/dataModels/{id}/columns`` endpoint does not currently
# return a per-column native type on the fields we consume (name,
# label, formula, elementId). We emit ``NullType`` + this sentinel so
# that downstream systems can recognize the absence of type info
# rather than trusting a lie. When the API starts returning a typed
# column field (or we add SQL-based inference), swap this out here.
SIGMA_DM_UNKNOWN_COLUMN_NATIVE_TYPE = "unknown"


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
        # Surface as a structured report warning so operators running
        # under ``--strict`` or CI dashboards that gate on report
        # warnings (rather than stdout logs) notice the misconfiguration.
        #
        # We compare the ``allow`` / ``deny`` lists directly rather than
        # against ``AllowDenyPattern.allow_all()`` because
        # ``AllowDenyPattern.__eq__`` is ``__dict__``-based and has two
        # ``@cached_property`` compiled-regex attributes; once either
        # side's ``.allowed()`` has been invoked its ``__dict__`` gains
        # a cache entry the other side doesn't have, flipping equality
        # to False. Today nothing calls ``.allowed()`` before this
        # check, but any future reorder in ``__init__`` would silently
        # emit a spurious warning -- the direct list check is
        # cache-state-independent.
        # ``ignoreCase`` is intentionally excluded from this check: a user
        # who pins ``ignoreCase: False`` on an otherwise-default pattern is
        # semantically still the "match everything" pattern (``.*`` matches
        # regardless of case). Including it would fire a spurious "pattern
        # ignored" warning on a benign config tweak.
        dm_pattern = self.config.data_model_pattern
        _dm_pattern_is_default = dm_pattern.allow == [".*"] and not dm_pattern.deny
        if not self.config.ingest_data_models and not _dm_pattern_is_default:
            self.reporter.warning(
                title="data_model_pattern ignored",
                message="data_model_pattern is set but ingest_data_models is "
                "False -- the pattern has no effect. Enable ingest_data_models "
                "or remove data_model_pattern to silence this warning.",
            )
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

    def _gen_data_model_element_upstream_lineage(
        self,
        element: SigmaDataModelElement,
        data_model: SigmaDataModel,
        elementId_to_dataset_urn: Dict[str, str],
    ) -> Optional[UpstreamLineage]:
        # Success counters bump once per unique URN; diamond source_ids
        # resolving to the same URN should not inflate the signal.
        # Failure counters bump once per unique ``source_id`` too -- a
        # vendor payload that repeats the same failed ``inode-X`` inside
        # one element's ``sourceIds`` would otherwise double-count the
        # unresolved buckets while the success path above carefully
        # dedupes, leaving asymmetric triage numbers.
        upstream_urns: List[str] = []
        seen: Set[str] = set()
        unresolved_seen: Set[str] = set()
        for source_id in element.source_ids:
            upstream_urn: Optional[str] = None
            shape: str = ""
            if source_id in elementId_to_dataset_urn:
                upstream_urn = elementId_to_dataset_urn[source_id]
                shape = "intra"
            elif source_id.startswith("inode-"):
                upstream_urn = self._resolve_dm_element_external_upstream(source_id)
                shape = "external"
                if upstream_urn is None and source_id not in unresolved_seen:
                    unresolved_seen.add(source_id)
                    self.reporter.data_model_element_upstreams_unresolved_external += 1
                    self.reporter.data_model_element_upstreams_unresolved += 1
                    logger.debug(
                        "DM %s element %s: external upstream %r unresolved "
                        "(target Sigma Dataset filtered out or not ingested)",
                        data_model.dataModelId,
                        element.elementId,
                        source_id,
                    )
            else:
                # Cross-DM ``<prefix>/<suffix>`` shapes and any other
                # shape we don't yet parse. Counted separately from
                # legitimate "external but un-ingested" so operators can
                # tell "upstream exists but wasn't emitted" (filter or
                # perm) apart from "we don't yet handle this shape"
                # (cross-DM resolution arrives in a follow-up PR, plus
                # any future Sigma vendor shape).
                if source_id not in unresolved_seen:
                    unresolved_seen.add(source_id)
                    self.reporter.data_model_element_upstreams_unknown_shape += 1
                    self.reporter.data_model_element_upstreams_unresolved += 1
                    logger.debug(
                        "DM %s element %s: upstream source_id %r has an unknown shape "
                        "(not ``inode-`` external, not intra-DM)",
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
        # duplicate ``fieldPath``s.
        #
        # Tie-break is deterministic across runs:
        # 1. Prefer the row with a non-empty ``formula`` -- that's the
        #    user-authored calculated field and the one Sigma surfaces
        #    in the UI when both exist with the same name.
        # 2. Fall back to the row with the lexicographically smallest
        #    ``columnId`` so the choice is stable even without
        #    ``/columns`` ordering guarantees.
        # Dropped ``columnId``s are emitted to debug logs so operators
        # can reconcile against Sigma.
        def _ranks_above(
            candidate: SigmaDataModelColumn, incumbent: SigmaDataModelColumn
        ) -> bool:
            # Prefer row with formula set (user-authored calc field).
            # Tie-break: smaller columnId wins so the choice is stable
            # across runs even if ``/columns`` ordering shifts.
            if bool(candidate.formula) != bool(incumbent.formula):
                return bool(candidate.formula)
            return candidate.columnId < incumbent.columnId

        by_name: Dict[str, SigmaDataModelColumn] = {}
        dropped_column_ids_by_name: Dict[str, List[str]] = {}
        for column in element.columns:
            if not column.name:
                continue
            existing = by_name.get(column.name)
            if existing is None:
                by_name[column.name] = column
                continue
            self.reporter.data_model_element_columns_duplicate_fieldpath_dropped += 1
            if _ranks_above(column, existing):
                winner, loser = column, existing
            else:
                winner, loser = existing, column
            by_name[column.name] = winner
            dropped_column_ids_by_name.setdefault(column.name, []).append(
                loser.columnId
            )
        if dropped_column_ids_by_name:
            logger.debug(
                "DM element %s: dropped duplicate-fieldPath columns %s "
                "(kept the row with formula set, or the lexicographically "
                "smallest columnId as a tiebreak)",
                element.elementId,
                dropped_column_ids_by_name,
            )
        # Sort fields by ``column.name`` (== ``fieldPath``) so the emitted
        # ``SchemaMetadata`` is stable across runs even if Sigma's
        # ``/columns`` response re-orders. Without this, a Sigma-side
        # reorder would churn the aspect on every ingest (re-upserting
        # the same field set under different ordering), showing up as
        # spurious graph updates in downstream consumers of the
        # aspect-version timeline. The API does not document an
        # ordering contract, so we enforce our own.
        fields: List[SchemaFieldClass] = []
        for column in sorted(by_name.values(), key=lambda c: c.name):
            fields.append(
                SchemaFieldClass(
                    fieldPath=column.name,
                    # Sigma's ``/columns`` endpoint does not expose a
                    # per-column native type today. Emit ``NullType`` +
                    # a sentinel ``nativeDataType`` so downstream type
                    # checks recognize "unknown" instead of trusting a
                    # fake "String" that was never returned by Sigma.
                    type=SchemaFieldDataTypeClass(NullTypeClass()),
                    nativeDataType=SIGMA_DM_UNKNOWN_COLUMN_NATIVE_TYPE,
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
        owner_username: Optional[str] = None,
    ) -> Iterable[MetadataWorkUnit]:
        dm_url_id = data_model.get_url_id()
        # ``data_model.path`` starts with the workspace name (e.g.
        # "Acryl Data/Marketing"); drop index 0 because the workspace is
        # already the enclosing Container. ``split`` on a path with no
        # separator yields ``["Acryl Data"]`` so [1:] safely degrades
        # to ``[]``.
        paths = data_model.path.split("/")[1:] if data_model.path else []
        workspace_container_urn: Optional[str] = None
        if data_model.workspaceId:
            workspace_container_urn = builder.make_container_urn(
                self._gen_workspace_key(data_model.workspaceId)
            )

        for element in data_model.elements:
            element_dataset_urn = elementId_to_dataset_urn[element.elementId]

            yield self._gen_entity_status_aspect(element_dataset_urn)

            # Sigma's /elements API has no element-level description, so
            # we omit the field (description=None) rather than emitting an
            # explicit empty string; aspect-replace semantics would blank
            # any description a user edited in the UI. qualifiedName uses
            # "/" as the separator; names containing "/" are still
            # disambiguated by the element URN.
            # ``?:nodeId=<elementId>`` is Sigma's standard deep-link shape
            # (same pattern used for workbook elements in sigma_api.py);
            # gate on ``data_model.url`` because it is ``Optional[str]``.
            element_external_url: Optional[str] = (
                f"{data_model.url}?:nodeId={element.elementId}"
                if data_model.url
                else None
            )
            element_properties = DatasetProperties(
                name=element.name,
                description=None,
                qualifiedName=f"{data_model.name}/{element.name}",
                externalUrl=element_external_url,
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

            # Propagate DM-level ownership (``data_model.createdBy``) onto
            # every element Dataset. The DM API has no element-level owner
            # field, so we mirror the Container owner on each child
            # Dataset -- otherwise "Datasets owned by X" filters in the
            # DataHub UI would miss DM elements entirely even though the
            # author shows up on the enclosing Container. Gated on
            # ``ingest_owner`` so operators who opt out of user-URN
            # emission (typically for privacy / SSO sync reasons) are
            # respected.
            if self.config.ingest_owner and owner_username:
                yield self._gen_entity_owner_aspect(element_dataset_urn, owner_username)

            yield from add_entity_to_container(
                container_key=data_model_key,
                entity_type="dataset",
                entity_urn=element_dataset_urn,
            )

            # BrowsePaths: workspace urn, DM path segments, DM Container
            # urn (typed so UI breadcrumbs are clickable). Matches the
            # terminal-at-parent shape used by the rest of this connector
            # (_gen_entity_browsepath_aspect) -- the element's own name
            # is rendered from DatasetProperties, not the breadcrumb, so
            # appending it here produced a duplicate unclickable crumb.
            # Orphan / personal-space DMs lack a workspace container;
            # skip that entry rather than duplicating the DM Container
            # URN.
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

    def _gen_data_model_workunit(
        self,
        data_model: SigmaDataModel,
        elementId_to_dataset_urn: Dict[str, str],
    ) -> Iterable[MetadataWorkUnit]:
        """Emit a DM as a Container plus one Dataset per element, with
        intra-DM and external UpstreamLineage on each element.
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
            # Pass ``None`` through (rather than ``""``) when Sigma has no
            # description so ``ContainerProperties`` aspect-replace cannot
            # blank out a description the user edited in the DataHub UI.
            # Mirrors the element-Dataset fix above.
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
            owner_username=owner_username,
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

        # Materialize the Sigma Dataset list once and populate the
        # ``url_id -> urn`` bridge map eagerly *before* DM iteration.
        # Previously the map was populated as a side-effect of
        # ``_gen_dataset_workunit`` yielding, which left
        # ``_resolve_dm_element_external_upstream`` dependent on
        # iteration order: the map was only complete because datasets
        # were yielded before DMs and the pipeline framework drained
        # generators sequentially. An eager pre-pass decouples
        # external-upstream resolution from emission order so any
        # future refactor that reorders or parallelizes the yields
        # cannot silently burn through the ``unresolved_external``
        # counter.
        datasets = list(self.sigma_api.get_sigma_datasets())
        for dataset in datasets:
            self.sigma_dataset_urn_by_url_id[dataset.get_urn_part()] = (
                self._gen_sigma_dataset_urn(dataset.get_urn_part())
            )

        for dataset in datasets:
            yield from self._gen_dataset_workunit(dataset)
        # Data Models are emitted before Workbooks. Cross-DM references,
        # personal-space DM discovery, and workbook-to-DM linking arrive
        # in a follow-up PR.
        if self.config.ingest_data_models:
            for data_model in self.sigma_api.get_data_models():
                elementId_to_dataset_urn: Dict[str, str] = {
                    element.elementId: self._gen_data_model_element_urn(
                        data_model, element
                    )
                    for element in data_model.elements
                }
                yield from self._gen_data_model_workunit(
                    data_model, elementId_to_dataset_urn
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
