import logging
from typing import Dict, Iterable, List, Literal, Optional, Set, Tuple

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
    SigmaDataModelColumn,
    SigmaDataModelElement,
    SigmaDataset,
    Workbook,
    WorkbookKey,
    Workspace,
    WorkspaceKey,
)
from datahub.ingestion.source.sigma.formula_parser import extract_bracket_refs
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
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
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


def _dm_column_ranks_above(
    candidate: SigmaDataModelColumn, incumbent: SigmaDataModelColumn
) -> bool:
    """Tie-breaking order for duplicate-fieldPath columns within a DM element.

    Prefers the row with a non-empty formula (the user-authored calculated
    field Sigma surfaces in the UI). Tie-break: lexicographically smaller
    columnId so the choice is stable across runs even if /columns ordering
    shifts.
    """
    if bool(candidate.formula) != bool(incumbent.formula):
        return bool(candidate.formula)
    return candidate.columnId < incumbent.columnId


def _dedup_dm_element_columns(
    columns: List[SigmaDataModelColumn],
) -> Tuple[
    Dict[str, SigmaDataModelColumn],
    List[Tuple[SigmaDataModelColumn, SigmaDataModelColumn]],
]:
    """Return (winners_by_name, displaced_pairs) for an element's column list.

    ``winners_by_name`` maps each unique column name to the winning column
    when duplicates exist (per _dm_column_ranks_above).  ``displaced_pairs``
    is a list of (winner, loser) tuples — each caller uses this to update its
    own counters and logs.

    Called by both _gen_data_model_element_schema_metadata and
    _build_dm_element_fine_grained_lineages so the two sites share a single
    source of truth for which fieldPaths are "live".  Any future change to the
    tie-break logic only needs to be made here.
    """
    by_name: Dict[str, SigmaDataModelColumn] = {}
    displaced: List[Tuple[SigmaDataModelColumn, SigmaDataModelColumn]] = []
    for column in columns:
        if not column.name:
            continue
        existing = by_name.get(column.name)
        if existing is None:
            by_name[column.name] = column
            continue
        winner, loser = (
            (column, existing)
            if _dm_column_ranks_above(column, existing)
            else (existing, column)
        )
        by_name[column.name] = winner
        displaced.append((winner, loser))
    return by_name, displaced


CrossDmOutcome = Literal[
    "strict",
    "ambiguous",
    "single_element_fallback",
    "malformed",
    "self_reference",
    "consumer_name_missing",
    "dm_unknown",
    "name_unmatched_but_dm_known",
]


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
@capability(SourceCapability.LINEAGE_FINE, "Enabled by default.")
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

    def _resolve_dm_element_cross_dm_upstream(
        self,
        source_id: str,
        consuming_element: SigmaDataModelElement,
        consuming_data_model: SigmaDataModel,
    ) -> Tuple[Optional[str], CrossDmOutcome]:
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

        # Primary self-reference check: current urlId slug or UUID.
        # Extended check: if the urlId was rotated, a stale lineage record may
        # reference the old slug. Compare container URNs so that any historical
        # alias that maps to the same DM is caught rather than misclassified as
        # ``dm_unknown`` (wasted API call + misleading triage counter).
        consuming_container_urn = self.dm_container_urn_by_url_id.get(
            consuming_data_model.get_url_id()
        ) or self.dm_container_urn_by_url_id.get(consuming_data_model.dataModelId)
        if (
            other_dm_url_id == consuming_data_model.get_url_id()
            or other_dm_url_id == consuming_data_model.dataModelId
            or (
                consuming_container_urn is not None
                and self.dm_container_urn_by_url_id.get(other_dm_url_id)
                == consuming_container_urn
            )
        ):
            return None, "self_reference"

        if not consuming_element.name:
            if other_dm_url_id not in self.dm_container_urn_by_url_id:
                return None, "dm_unknown"
            return None, "consumer_name_missing"

        # Registration (dm_container_urn_by_url_id) is the single source of
        # truth for ``dm_unknown``. ``dm_element_urn_by_name`` can legitimately
        # be ``{}`` for a registered DM with only blank-named elements, which
        # we must not conflate with "DM never registered".
        if other_dm_url_id not in self.dm_container_urn_by_url_id:
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
        elif outcome == "name_unmatched_but_dm_known":
            self.reporter.data_model_element_cross_dm_upstreams_name_unmatched_but_dm_known += 1
        # ``malformed`` has no dedicated counter; falls into the caller's
        # generic ``data_model_element_upstreams_unresolved`` bump.

    def _gen_data_model_element_upstream_lineage(
        self,
        element: SigmaDataModelElement,
        data_model: SigmaDataModel,
        element_dataset_urn: str,
        elementId_to_dataset_urn: Dict[str, str],
        element_name_to_eids: Dict[str, List[str]],
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
            cross_dm_outcome: Optional[str] = None
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
            elif "/" in source_id:
                upstream_urn, cross_dm_outcome = (
                    self._resolve_dm_element_cross_dm_upstream(
                        source_id, element, data_model
                    )
                )
                shape = "cross_dm"
                if upstream_urn is None and source_id not in unresolved_seen:
                    unresolved_seen.add(source_id)
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
                # Any other shape we don't parse (future Sigma vendor
                # shape, or an existing shape we missed). Counted
                # separately from legitimate "external but un-ingested"
                # and from cross-DM so operators can triage
                # "upstream exists but wasn't emitted" (filter / perm)
                # vs "we don't yet handle this shape." Note: cross-DM
                # ``<prefix>/<suffix>`` refs are caught in the
                # ``"/" in source_id`` branch above, so this bucket is
                # genuinely "nothing we recognize."
                if source_id not in unresolved_seen:
                    unresolved_seen.add(source_id)
                    self.reporter.data_model_element_upstreams_unknown_shape += 1
                    self.reporter.data_model_element_upstreams_unresolved += 1
                    logger.debug(
                        "DM %s element %s: upstream source_id %r has an unknown shape "
                        "(not ``inode-`` external, not intra-DM, not ``<dm>/<suffix>`` cross-DM)",
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

        # Sort for deterministic emission order: Sigma's /lineage API does
        # not document ordering, and Upstream entries have no semantic order.
        upstream_urns.sort()
        if not upstream_urns:
            return None
        fine_grained = self._build_dm_element_fine_grained_lineages(
            element=element,
            element_dataset_urn=element_dataset_urn,
            element_name_to_eids=element_name_to_eids,
            elementId_to_dataset_urn=elementId_to_dataset_urn,
            entity_level_upstream_urns=set(upstream_urns),
            data_model=data_model,
        )
        return UpstreamLineage(
            upstreams=[
                Upstream(dataset=urn, type=DatasetLineageType.TRANSFORMED)
                for urn in upstream_urns
            ],
            fineGrainedLineages=fine_grained or None,
        )

    def _gen_data_model_element_schema_metadata(
        self, element_dataset_urn: str, element: SigmaDataModelElement
    ) -> MetadataWorkUnit:
        # Dedup by ``fieldPath`` within the element: Sigma can return two
        # columns with the same name (e.g. a calculated field shadowing a
        # native column), and GMS rejects / non-deterministically dedupes
        # duplicate ``fieldPath``s. Tie-break logic lives in _dedup_dm_element_columns
        # so it stays in sync with the FGL builder's winner set.
        by_name, displaced = _dedup_dm_element_columns(element.columns)
        dropped_column_ids_by_name: Dict[str, List[str]] = {}
        for winner, loser in displaced:
            self.reporter.data_model_element_columns_duplicate_fieldpath_dropped += 1
            dropped_column_ids_by_name.setdefault(winner.name, []).append(
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

    def _build_dm_element_fine_grained_lineages(
        self,
        *,
        element: SigmaDataModelElement,
        element_dataset_urn: str,
        element_name_to_eids: Dict[str, List[str]],
        elementId_to_dataset_urn: Dict[str, str],
        entity_level_upstream_urns: Set[str],
        data_model: SigmaDataModel,
    ) -> List[FineGrainedLineageClass]:
        """Build FineGrainedLineage entries for intra-DM [ElementName/col] refs.

        element_name_to_eids maps lowercased element name → list of elementIds.
        Self-references are stripped before resolution: when a DM element is named
        after its warehouse source (e.g., element "data.csv" with formula
        "[data.csv/col]"), the formula ref matches the element's own name and must
        be filtered out to avoid self-referential FGL.  These are counted under
        fgl_warehouse_passthrough_deferred and left for warehouse-external resolution.

        When multiple sibling elements share a name and both pass the /lineage filter,
        the lexicographically-first URN is chosen (matching T2 PR1's collision policy
        and Sigma's server-side coalescing behaviour).
        """
        by_name, _ = _dedup_dm_element_columns(element.columns)

        # Build URN → winner-column map directly from elementId_to_dataset_urn,
        # which gives an unambiguous elementId→URN mapping for every DM element.
        urn_to_cols: Dict[str, Dict[str, str]] = {}
        for dm_el in data_model.elements:
            el_urn = elementId_to_dataset_urn.get(dm_el.elementId)
            if el_urn:
                el_by_name, _ = _dedup_dm_element_columns(dm_el.columns)
                urn_to_cols[el_urn] = {c.lower(): c for c in el_by_name}

        fgls: List[FineGrainedLineageClass] = []
        # Track emitted (downstream, upstream) schemaField pairs to deduplicate
        # multiple occurrences of the same bracket ref in one formula
        # (e.g. If([A/x] = 0, [A/x], [A/x] / 2) → one FGL, not three).
        emitted_pairs: Set[Tuple[str, str]] = set()
        for column in sorted(by_name.values(), key=lambda c: c.name):
            if not column.formula:
                continue
            downstream_field = builder.make_schema_field_urn(
                element_dataset_urn, column.name
            )
            for ref in extract_bracket_refs(column.formula):
                if ref.is_parameter or ref.column is None:
                    # [P_*] parameter refs and bare [col] intra-element refs
                    # are not cross-Dataset lineage; skip.
                    continue

                candidate_eids = element_name_to_eids.get(ref.source.lower(), [])

                # Strip self-references: element-name == warehouse-table name is a
                # common Sigma authoring pattern.  The formula ref resolves to the
                # element itself, which is not a valid FGL upstream (the real upstream
                # is the warehouse inode reported by /lineage).
                candidate_eids_after_self_strip = [
                    eid for eid in candidate_eids if eid != element.elementId
                ]

                if not candidate_eids_after_self_strip:
                    if candidate_eids:
                        # All candidates were self-references; actual upstream is a
                        # warehouse table — out of RESOLVE-A scope.
                        self.reporter.data_model_element_fgl_warehouse_passthrough_deferred += 1
                    else:
                        # Source not in this DM — cross-DM ref handled separately.
                        self.reporter.data_model_element_fgl_cross_dm_deferred += 1
                    continue

                # Map surviving elementIds to Dataset URNs and filter against
                # /lineage's reported entity-level upstreams.
                candidate_urns = sorted(
                    elementId_to_dataset_urn[eid]
                    for eid in candidate_eids_after_self_strip
                    if eid in elementId_to_dataset_urn
                )
                surviving_urns = sorted(
                    u for u in candidate_urns if u in entity_level_upstream_urns
                )

                if not surviving_urns:
                    # Intra-DM candidate(s) exist but none appear in /lineage.
                    # Rare on tenants where /lineage correctly reports intra-DM edges;
                    # keep counter for observability on future tenants.
                    self.reporter.data_model_element_fgl_dropped_orphan_upstream += 1
                    continue

                # Collision handling: multiple siblings passed /lineage filter.
                # Pick sorted-first to match T2 PR1's _resolve_external_upstream
                # policy and Sigma's server-side coalescing.
                if len(surviving_urns) > 1:
                    self.reporter.data_model_element_fgl_collision_pick_first += 1
                    self.reporter.warning(
                        title="Ambiguous DM element name in formula ref",
                        message=(
                            f"Formula ref {ref.raw!r} in element {element.elementId} "
                            f"resolves to {len(surviving_urns)} elements with the same "
                            f"display name — picking the lexicographically-first URN "
                            f"({surviving_urns[0]!r}). Rename duplicate elements in "
                            f"Data Model {data_model.dataModelId} to remove ambiguity."
                        ),
                        context=f"ref={ref.raw!r}, candidates={surviving_urns}",
                    )
                chosen_upstream_urn = surviving_urns[0]

                # Validate and normalise ref.column against the chosen upstream
                # element's schema winners to avoid a dangling schemaField URN.
                source_cols = urn_to_cols.get(chosen_upstream_urn, {})
                canonical_col = source_cols.get(ref.column.lower())
                if canonical_col is None:
                    self.reporter.data_model_element_fgl_dropped_unknown_upstream_column += 1
                    logger.debug(
                        "DM %s element %s: ref %r column %r not found in upstream "
                        "element %s schema winners; dropping FGL entry",
                        data_model.dataModelId,
                        element.elementId,
                        ref.raw,
                        ref.column,
                        chosen_upstream_urn,
                    )
                    continue

                upstream_field = builder.make_schema_field_urn(
                    chosen_upstream_urn, canonical_col
                )
                pair = (downstream_field, upstream_field)
                if pair in emitted_pairs:
                    continue
                emitted_pairs.add(pair)
                fgls.append(
                    FineGrainedLineageClass(
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=[downstream_field],
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=[upstream_field],
                        confidenceScore=1.0,
                    )
                )
        self.reporter.data_model_element_fgl_emitted += len(fgls)
        fgls.sort(
            key=lambda fgl: (
                (fgl.downstreams or [""])[0],
                (fgl.upstreams or [""])[0],
            )
        )
        return fgls

    def _gen_data_model_element_workunits(
        self,
        data_model: SigmaDataModel,
        data_model_key: DataModelKey,
        data_model_container_urn: str,
        elementId_to_dataset_urn: Dict[str, str],
        element_name_to_eids: Dict[str, List[str]],
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
                element,
                data_model,
                element_dataset_urn,
                elementId_to_dataset_urn,
                element_name_to_eids,
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

    def _register_dm_alias(self, alias: str, canonical_key: str) -> None:
        """Register *alias* as a secondary lookup key pointing at the same
        bridge-map entries as *canonical_key*.

        Called when the discovery loop fetches a DM by a stale/rotated slug
        that differs from the DM's current canonical urlId. The canonical key
        must already be present in ``dm_container_urn_by_url_id`` (either from
        the round-1 prepopulate or from ``_prepopulate_dm_bridge_maps``).
        No-ops if the alias already maps to any DM or if alias == canonical_key.
        """
        if alias == canonical_key or alias in self.dm_container_urn_by_url_id:
            return
        container_urn = self.dm_container_urn_by_url_id.get(canonical_key)
        if container_urn is None:
            return
        self.dm_container_urn_by_url_id[alias] = container_urn
        self.dm_element_urn_by_name[alias] = self.dm_element_urn_by_name.get(
            canonical_key, {}
        )
        self.dm_total_element_count_by_url_id[alias] = (
            self.dm_total_element_count_by_url_id.get(canonical_key, 0)
        )

    def _prepopulate_dm_bridge_maps(
        self,
        data_model: SigmaDataModel,
        requested_alias: Optional[str] = None,
    ) -> Dict[str, str]:
        """Populate the global DM bridge maps for one DM and return the
        local ``elementId -> Dataset URN`` map for intra-DM lineage.

        Called eagerly for every DM before any DM emits workunits so
        cross-DM and workbook-to-DM lineage resolve regardless of DM
        iteration order. Bridge keys use ``get_url_id()`` (the urlId slug
        when present, else the dataModelId UUID).

        ``requested_alias``: if the discovery loop fetched this DM by a
        stale/rotated prefix that differs from the canonical ``get_url_id()``,
        pass it here. The same container URN and name maps will be registered
        under both the canonical key and the alias so that source_ids carrying
        the old slug resolve correctly.
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
            # Collided DMs are filtered out of emission by
            # ``dm_collided_data_model_ids`` in ``get_workunits_internal``
            # (see the emission loop). No downstream consumer reads the
            # per-element map for a collided DM, so skip the allocation
            # entirely and return an empty dict -- the first-registered
            # DM's maps win uniformly.
            return {}

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

        self.dm_container_urn_by_url_id[bridge_key] = data_model_container_urn
        self.dm_element_urn_by_name[bridge_key] = name_map
        # Total count (including blank-named elements) used by the
        # cross-DM single-element fallback to verify "DM has exactly one
        # element" before attributing an unmatched-name reference.
        self.dm_total_element_count_by_url_id[bridge_key] = len(data_model.elements)

        if requested_alias:
            self._register_dm_alias(requested_alias, bridge_key)

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

        # Keys are lowercased so formula refs — which users type and Sigma
        # autocompletes from canonical names — match even when case differs.
        # Values are elementIds (not URNs) so the FGL builder can strip self-references
        # by elementId before mapping to URNs via elementId_to_dataset_urn.
        element_name_to_eids: Dict[str, List[str]] = {}
        for element in data_model.elements:
            if element.name:
                element_name_to_eids.setdefault(element.name.lower(), []).append(
                    element.elementId
                )

        yield from self._gen_data_model_element_workunits(
            data_model,
            data_model_key,
            data_model_container_urn,
            elementId_to_dataset_urn,
            element_name_to_eids,
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

    def _resolve_dm_element_upstream_urn(
        self, upstream: DataModelElementUpstream
    ) -> Tuple[Optional[str], List[str]]:
        """Resolve a workbook element's DM upstream to a DM element
        Dataset URN. Returns ``(urn, candidates)``:

        - ``urn`` is None when the name cannot be matched
          (``ChartInfo.inputs`` requires Dataset URNs, not Container URNs).
        - ``candidates`` is the name-matched URN list (0 = miss, 1 = unique,
          >=2 = ambiguous). The caller uses it to bump
          ``element_dm_edge.ambiguous`` once per unique chart-to-DM edge.

        Candidates are sorted deterministically by their Dataset URN
        (which embeds ``dataModelId.elementId``), so the pick is stable
        across runs; within a single DM this degenerates to the lowest
        elementId.
        """
        dm_known = upstream.data_model_url_id in self.dm_container_urn_by_url_id
        name_map = self.dm_element_urn_by_name.get(upstream.data_model_url_id)

        if not upstream.name:
            if dm_known:
                self.reporter.element_dm_edge.upstream_name_missing += 1
            else:
                self.reporter.element_dm_edge.unresolved += 1
            return None, []

        if name_map:
            raw_candidates = name_map.get(upstream.name.lower())
            if raw_candidates:
                candidates = sorted(raw_candidates)
                return candidates[0], candidates

        # Intentional asymmetry with ``_resolve_dm_element_cross_dm_upstream``:
        # that path falls back to the producer DM's sole element when the
        # producer has exactly one element, because cross-DM source_ids
        # (``<prefix>/<suffix>``) don't always carry a matching name on the
        # ``type: element`` row. Here, the workbook-to-DM edge is
        # synthesized (edge-only / formula-parsed) with ``upstream.name``
        # *set to the DM element's name at synthesis time* -- a mismatch
        # against every ``name_map`` key indicates the DM element was
        # renamed after the workbook last referenced it, not an
        # ambiguous-resolution gap. Falling back to the sole element
        # would silently bind the edge to the post-rename element, so we
        # surface via ``element_dm_edge.name_unmatched_but_dm_known`` and
        # let operators decide whether to re-save the workbook.
        if dm_known:
            self.reporter.element_dm_edge.name_unmatched_but_dm_known += 1
        else:
            self.reporter.element_dm_edge.unresolved += 1
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
                    # The previous ``DatasetUpstream.name: str`` contract
                    # raised a Pydantic ``ValidationError`` that surfaced
                    # as a ``SourceReport.warning`` with full Pydantic
                    # context. Now that ``name`` is ``Optional[str]``,
                    # surface equivalent context through ``report.warning``
                    # (``LossyList``-backed -- auto-truncates after N
                    # entries, so this is already rate-limited) alongside
                    # the counter so production operators can triage which
                    # upstream / which workbook element triggered the drop.
                    self.reporter.chart_dataset_upstream_name_missing += 1
                    self.reporter.warning(
                        title="Sigma workbook dataset upstream dropped (name missing)",
                        message="A workbook element references a Sigma Dataset "
                        "upstream whose ``name`` field was ``null`` on the "
                        "``/workbooks/{id}/lineage`` payload. No chart-to-"
                        "Sigma-dataset edge can be SQL-correlated for this "
                        "upstream; the edge is skipped. See the "
                        "``chart_dataset_upstream_name_missing`` counter for "
                        "the aggregate drop count.",
                        context=(
                            f"node={node_id}, sigma_dataset_id={sigma_dataset_id}, "
                            f"element={element.name} ({element.elementId}), "
                            f"workbook={workbook.name} ({workbook.workbookId})"
                        ),
                    )
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
                        self.reporter.element_dm_edge.deduped += 1
                    else:
                        dataset_inputs[dm_urn] = []
                        self.reporter.element_dm_edge.resolved += 1
                        # Count ambiguity once per chart-to-DM edge, not
                        # once per diamond sourceId.
                        if len(candidates) > 1:
                            self.reporter.element_dm_edge.ambiguous += 1
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

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:  # noqa: C901
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
            # Belt-and-braces cap on discovery passes. The loop terminates
            # naturally because ``unresolved_seen`` monotonically grows and
            # ``get_data_model_by_url_id`` returns None on 4xx (filtered DMs
            # are also seen-marked), so the set of reachable ``urlId`` prefixes
            # is finite. The cap exists to protect against pathological
            # Sigma payloads (e.g. a chain of personal-space DMs that each
            # reference newly-discovered personal-space DMs) by degrading
            # gracefully with a ``SourceReport.warning`` rather than looping
            # unbounded.
            max_rounds = self.config.max_personal_dm_discovery_rounds
            discovery_round = 0
            while pending:
                discovery_round += 1
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

                if not unresolved:
                    # Natural termination: no new prefixes to explore.
                    break

                if discovery_round >= max_rounds:
                    # Defensive: stop before the next API fan-out only when
                    # there are actually unresolved prefixes to abandon.
                    # Checking after collecting unresolved ensures the warning
                    # never fires spuriously when ``max_personal_dm_discovery_rounds``
                    # is set to a low value but the graph was already fully
                    # resolved in earlier rounds (e.g. the common case where
                    # all listed-DM bridges are prepopulated in round 1 and
                    # no orphan prefixes remain).
                    self.reporter.warning(
                        title="personal-space Data Model discovery cap reached",
                        message=(
                            "Personal-space DM discovery reached the "
                            "``max_personal_dm_discovery_rounds`` cap; "
                            "remaining unresolved ``<urlId>`` prefixes will "
                            "not be fetched on this run. Cross-DM edges into "
                            "those DMs will be reported under "
                            "``element_dm_edge.unresolved`` / "
                            "``data_model_element_cross_dm_upstreams_dm_unknown``. "
                            "Raise ``max_personal_dm_discovery_rounds`` if this "
                            "is legitimate, or investigate the DM graph for a "
                            "reference cycle."
                        ),
                        context=(
                            f"cap={max_rounds}, "
                            f"abandoned_count={len(unresolved)}, "
                            f"abandoned_prefixes={unresolved[:10]}"
                        ),
                    )
                    break

                for prefix in unresolved:
                    discovered_dm = self.sigma_api.get_data_model_by_url_id(prefix)
                    if discovered_dm is None:
                        self.reporter.data_model_external_reference_unresolved += 1
                        continue
                    # Re-apply ``workspace_pattern`` / ``data_model_pattern``
                    # so a denied workspace or DM name cannot leak via
                    # cross-DM references. Filtered orphans are treated as
                    # never-registered (``dm_unknown`` at resolution time)
                    # because the operator never opted them in. If the DM
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
                            continue
                        if workspace is None:
                            # For discovered DMs, drop rather than emitting
                            # with a phantom parent Container URN that points
                            # at a workspace that was never produced in this
                            # run (403 / deleted workspace). Consistent with
                            # the workspace_pattern-denied path above.
                            # Emit a structured warning so the operator can
                            # see which DM was dropped and why without
                            # tailing stdout — get_workspace() only debugs.
                            self.reporter.warning(
                                title="Sigma discovered Data Model dropped: workspace unreachable",
                                message=(
                                    "A cross-DM reference resolved to a Data Model "
                                    "whose workspace could not be fetched (403, deleted, "
                                    "or exception). The DM is dropped to avoid emitting "
                                    "a phantom parent Container URN. Cross-DM edges into "
                                    "this DM will be counted as "
                                    "``data_model_element_cross_dm_upstreams_dm_unknown``."
                                ),
                                context=(
                                    f"dm={discovered_dm.name} ({discovered_dm.dataModelId}), "
                                    f"workspace_id={discovered_dm.workspaceId}"
                                ),
                            )
                            self.reporter.data_models.dropped(
                                f"{discovered_dm.name} ({discovered_dm.dataModelId}) "
                                f"(discovered, workspace {discovered_dm.workspaceId} "
                                f"unreachable or deleted)"
                            )
                            self.reporter.data_models_without_workspace += 1
                            continue
                    if not self.config.data_model_pattern.allowed(discovered_dm.name):
                        self.reporter.data_models.dropped(
                            f"{discovered_dm.name} ({discovered_dm.dataModelId}) "
                            f"(discovered, filtered by data_model_pattern)"
                        )
                        continue
                    self.reporter.data_model_external_references_discovered += 1
                    all_data_models.append(discovered_dm)
                    pending.append(discovered_dm)
                    # Eagerly prepopulate bridge maps so that the alias
                    # (requested prefix != canonical urlId) is registered
                    # immediately. This prevents source_ids carrying the
                    # old slug from resolving as ``dm_unknown`` even when
                    # Sigma returns the DM under a rotated canonical urlId.
                    # The next-round prepopulate loop skips already-registered
                    # DMs via the ``dataModelId in elementId_maps_by_dm`` guard.
                    if discovered_dm.dataModelId not in elementId_maps_by_dm:
                        elementId_maps_by_dm[discovered_dm.dataModelId] = (
                            self._prepopulate_dm_bridge_maps(
                                discovered_dm, requested_alias=prefix
                            )
                        )
                    elif prefix != discovered_dm.get_url_id():
                        # DM is already registered from the listed path, but
                        # the discovery prefix differs from the canonical urlId
                        # (slug rotation). Register the old slug as an alias so
                        # source_ids carrying it still resolve correctly.
                        self._register_dm_alias(prefix, discovered_dm.get_url_id())

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
