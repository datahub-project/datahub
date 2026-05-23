"""
BigID → DataHub connector.

Pure enrichment connector: syncs BigID's classification findings, business
glossary terms, and tags into DataHub as GlossaryTerms, Tags, and
DatasetProfiles on existing Dataset entities.  Does not create Dataset
entities by default (create_datasets=False).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field as dataclass_field
from typing import Any, Iterable, Iterator, Optional

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
    MetadataWorkUnitProcessor,
    SourceCapability,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataPlatformInstanceClass,
    DatasetFieldProfileClass,
    DatasetProfileClass,
    DatasetPropertiesClass,
    DomainsClass,
    DomainPropertiesClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    GlossaryNodeInfoClass,
    GlossaryTermAssociationClass,
    GlossaryTermInfoClass,
    GlossaryTermsClass,
    MetadataAttributionClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    StatusClass,
    StructuredPropertyDefinitionClass,
    TagAssociationClass,
    TagPropertiesClass,
)
from datahub.specific.dataset import DatasetPatchBuilder

from datahub.ingestion.source.bigid.bigid_api import BigIDAPIError, BigIDClient
from datahub.ingestion.source.bigid.bigid_config import (
    BIGID_TYPE_TO_PLATFORM,
    LOWERCASE_PLATFORMS,
    BigIDSourceConfig,
    ConnectionPlatformConfig,
)
from datahub.ingestion.source.bigid.bigid_report import BigIDSourceReport, _rank_to_float
from datahub.ingestion.source.bigid.bigid_utils import (
    _encode_urn_name,
    _map_field_type,
    _parse_iso_to_ms,
    _slugify,
)

logger = logging.getLogger(__name__)

_BIGID_ROOT_GLOSSARY_NODE_URN = "urn:li:glossaryNode:bigid"
_BIGID_IDSOR_GLOSSARY_NODE_URN = "urn:li:glossaryNode:bigid.idsor"
_RISK_SCORE_TAG_NAME = "riskScore"


@dataclass
class _FieldEnrichment:
    """Accumulated enrichment for a single schema field across all attributeDetails entries."""

    terms: list[GlossaryTermAssociationClass] = dataclass_field(default_factory=list)
    tag_urns: list[str] = dataclass_field(default_factory=list)


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _build_field_profile(col: dict[str, Any]) -> Optional[DatasetFieldProfileClass]:
    """Build a DatasetFieldProfileClass from a single BigID column record.

    Returns None if the column has no profile data or no field path.
    """
    field_path = col.get("columnName", "")
    if not field_path:
        return None
    cp = col.get("columnProfile", {})
    if not cp:
        return None

    field_count = cp.get("fieldCount")
    distinct_pct = cp.get("distinctPct")
    empty_pct = cp.get("emptyPct")
    inferred_type = cp.get("inferredDataType", "").lower()

    unique_prop: Optional[float] = None
    unique_count: Optional[int] = None
    null_prop: Optional[float] = None
    null_count: Optional[int] = None

    if distinct_pct is not None:
        try:
            unique_prop = float(distinct_pct) / 100.0
            if field_count is not None:
                unique_count = round(float(field_count) * unique_prop)
        except (ValueError, TypeError):
            logger.debug("Could not parse distinctPct=%r for field %r", distinct_pct, field_path)

    if empty_pct is not None:
        try:
            null_prop = float(empty_pct) / 100.0
            if field_count is not None:
                null_count = round(float(field_count) * null_prop)
        except (ValueError, TypeError):
            logger.debug("Could not parse emptyPct=%r for field %r", empty_pct, field_path)

    is_numeric = inferred_type in ("numeric", "integer", "float", "number")

    min_val: Optional[str] = None
    max_val: Optional[str] = None
    mean_val: Optional[str] = None
    stdev_val: Optional[str] = None
    sample_values: Optional[list[str]] = None

    if is_numeric:
        if (v := cp.get("minNum")) is not None:
            min_val = str(v)
        if (v := cp.get("maxNum")) is not None:
            max_val = str(v)
        if (v := cp.get("avgNum")) is not None:
            mean_val = str(v)
        if (v := cp.get("numDev")) is not None:
            stdev_val = str(v)
    else:
        min_lex = cp.get("minLexStr")
        max_lex = cp.get("maxLexStr")
        if min_lex is not None:
            min_val = str(min_lex)
        if max_lex is not None:
            max_val = str(max_lex)
        if min_lex is not None and max_lex is not None:
            sample_values = [str(min_lex), str(max_lex)]

    return DatasetFieldProfileClass(
        fieldPath=field_path,
        uniqueCount=unique_count,
        uniqueProportion=unique_prop,
        nullCount=null_count,
        nullProportion=null_prop,
        min=min_val,
        max=max_val,
        mean=mean_val,
        stdev=stdev_val,
        sampleValues=sample_values,
    )


def _is_idsor_attr(attr: dict[str, Any]) -> bool:
    """Return True if this attributeDetails entry is an IDSoR correlation finding.

    IDSoR attributes have ``type`` as an array (e.g. ``["IDSoR Attribute"]``),
    whereas classifiers and business terms have ``type`` as a plain string.
    """
    attr_type = attr.get("type")
    return isinstance(attr_type, list) and "IDSoR Attribute" in attr_type


# ---------------------------------------------------------------------------
# Main source
# ---------------------------------------------------------------------------


@platform_name("BigID")
@config_class(BigIDSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.CLASSIFICATION, "BigID classification findings as GlossaryTerms on SchemaFields")
@capability(SourceCapability.TAGS, "BigID tags applied to datasets and columns")
@capability(SourceCapability.SCHEMA_METADATA, "Column schema from BigID columns API (requires create_datasets=True)")
@capability(SourceCapability.DATA_PROFILING, "Column-level profiles from BigID columnProfile data")
@capability(SourceCapability.OWNERSHIP, "Ownership on GlossaryTerms (not Datasets); controlled by owner_type config")
@capability(SourceCapability.DOMAINS, "Domain entities created when domain_mode is auto_namespaced or config_map")
@capability(SourceCapability.DELETION_DETECTION, "Stale entity removal via stateful ingestion")
@capability(SourceCapability.PLATFORM_INSTANCE, "Platform instance emitted per dataset when platform_instance is configured")
@capability(SourceCapability.LINEAGE_COARSE, "Not supported", supported=False)
class BigIDSource(StatefulIngestionSourceBase):
    """
    DataHub source connector for BigID data intelligence platform.

    Syncs:
    - Business glossary items as GlossaryTerms under a 'BigID' root node
    - Dataset-level and column-level tags
    - Column classification findings as GlossaryTerms with MetadataAttribution
    - DatasetProfile statistics from BigID column profiles
    - (Optional) Dataset and schema creation for sources not yet in DataHub

    Note on enrichment semantics: aspects emitted by this connector (GlobalTags,
    EditableSchemaMetadata, GlossaryTerms) use UPSERT change type, which replaces
    the full aspect on each run. Tags or terms manually added in the DataHub UI will
    be overwritten on the next ingestion. This is a known limitation of MCP-based
    enrichment and is documented here for transparency.

    Note on SDK V2: this connector uses MetadataChangeProposalWrapper (the
    MCP/Gen-2 pattern) throughout rather than the newer datahub.sdk builders.
    The SDK V2 standard is mandatory for contributions to the main DataHub repo,
    but as of the time of writing datahub.sdk has no builders for GlossaryTerm,
    GlossaryNode, Domain, or StructuredProperty — the four entity types this
    connector emits most. A partial migration (SDK V2 for Dataset aspects, MCP
    for everything else) would add complexity with no functional gain. This
    decision should be revisited once SDK V2 gains coverage for those types.

    Note on dataPlatformInstance: this connector intentionally omits the
    dataPlatformInstance aspect unless platform_instance is explicitly configured
    per connection. Emitting it with a null instance in enrichment mode would
    UPSERT-overwrite the value already set by a native connector (e.g. Snowflake,
    BigQuery). This is a deliberate deviation from the standard api.md requirement.
    """

    config: BigIDSourceConfig
    report: BigIDSourceReport

    def __init__(self, config: BigIDSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.config = config
        self.report = BigIDSourceReport()
        self.client: Optional[BigIDClient] = None
        self.client = BigIDSource._build_client(config)
        # Populated during _load_registries()
        self._platform_map: dict[str, str] = {}       # connection_name → platform
        self._glossary_id_map: dict[str, str] = {}    # original_name → glossary_id
        self._friendly_name_map: dict[str, str] = {}  # glossary_id → friendly_name
        self._glossary_items: list[dict[str, Any]] = []
        # Track domain entities emitted this run (avoid duplicates)
        self._emitted_domain_urns: set[str] = set()
        # Friendly names for unlinked classifiers (original_name → friendly_name)
        self._classifier_friendly_names: dict[str, str] = {}
        # Track classifier-term URNs already emitted this run (demand-driven deduplication)
        self._emitted_classifier_term_urns: set[str] = set()
        # Within-run deduplication for create_datasets=True (prevents double-emitting
        # DatasetProperties/SchemaMetadata for the same URN in a single run)
        self._seen_dataset_urns: set[str] = set()
        # IDSoR attribute map: raw_name → (friendly_name, glossary_id_or_None)
        # Loaded at startup from results-tuning/attributes when sync_idsor=True.
        self._idsor_attr_map: dict[str, tuple[str, Optional[str]]] = {}
        # URNs of auto-generated IDSoR GlossaryTerms already emitted this run
        self._emitted_idsor_term_urns: set[str] = set()
        # URNs of Path-1 IDSoR linked terms (fn_item_* etc.) already emitted this run
        self._emitted_idsor_linked_term_urns: set[str] = set()

    @classmethod
    def create(cls, config_dict: dict[str, Any], ctx: PipelineContext) -> "BigIDSource":
        config = BigIDSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    # ------------------------------------------------------------------
    # TestableSource protocol
    # ------------------------------------------------------------------

    @staticmethod
    def _build_client(config: BigIDSourceConfig) -> BigIDClient:
        """Construct a BigIDClient from config. Extracted to avoid duplicate construction."""
        return BigIDClient(
            bigid_url=config.bigid_url,
            user_token=config.user_token.get_secret_value() if config.user_token else None,
            access_token=config.access_token.get_secret_value() if config.access_token else None,
            timeout=config.timeout,
            max_retries=config.max_retries,
        )

    @staticmethod
    def test_connection(config_dict: dict[str, Any]) -> TestConnectionReport:
        config = BigIDSourceConfig.model_validate(config_dict)
        try:
            client = BigIDSource._build_client(config)
            client.test_connection()
            client.close()
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(capable=True)
            )
        except Exception as exc:
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False, failure_reason=str(exc)
                )
            )

    # ------------------------------------------------------------------
    # Stateful ingestion wiring
    # ------------------------------------------------------------------

    def get_workunit_processors(self) -> list[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(self, self.config, self.ctx).workunit_processor,
        ]

    def get_report(self) -> BigIDSourceReport:
        return self.report

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        try:
            yield from self._load_and_emit()
        except BigIDAPIError as exc:
            self.report.failure("BigID API error", context=str(exc))
        except Exception as exc:
            self.report.failure("unexpected error", context=str(exc))
            logger.exception("Unexpected error in BigID connector: %s", exc)
        finally:
            if self.client is not None:
                self.client.close()

    def _load_and_emit(self) -> Iterator[MetadataWorkUnit]:
        # Step 0 — load supporting registries
        self._load_registries()
        if self.report.failures:
            return

        # Emit riskScore definition before any catalog passes so GMS commits it
        # before dataset structuredProperties usages arrive in later batches.
        yield from self._emit_risk_score_structured_property()

        # Step 1 — root GlossaryNode(s)
        yield from self._emit_root_glossary_node()
        if self.config.sync_idsor:
            yield from self._emit_idsor_glossary_node()

        # Step 2/3 — Domain entities (if mode != none)
        if self.config.domain_mode == "auto_namespaced":
            yield from self._emit_domain_entities()

        # Step 4 — GlossaryTerms
        yield from self._emit_glossary_terms()

        # Step 5 — Tag entities + dataset enrichment + column enrichment
        yield from self._process_catalog()

    # ------------------------------------------------------------------
    # Registry loading
    # ------------------------------------------------------------------

    def _load_registries(self) -> None:
        """Pre-load all supporting data needed for URN resolution and enrichment."""
        # Platform map from ds-connections
        try:
            connections = self.client.get_connections()
            for conn in connections:
                conn_name = conn.get("name", "")
                if not conn_name:
                    continue
                if conn_name in self.config.datasource_platform_mapping:
                    self._platform_map[conn_name] = self.config.datasource_platform_mapping[conn_name].platform
                else:
                    conn_type = conn.get("type", "")
                    platform = BIGID_TYPE_TO_PLATFORM.get(conn_type)
                    if platform:
                        self._platform_map[conn_name] = platform
                    else:
                        logger.warning(
                            "Unknown BigID connection type '%s' for connection '%s' — "
                            "no platform mapping; dataset URNs will use raw type.",
                            conn_type, conn_name,
                        )
                        self._platform_map[conn_name] = conn_type
                        self.report.report_connection_no_platform(conn_name)
        except BigIDAPIError as exc:
            self.report.warning("ds-connections unavailable", context=str(exc))

        if not self._platform_map and not self.config.datasource_platform_mapping:
            self.report.warning(
                "platform-map-empty",
                context="ds-connections API failed and no datasource_platform_mapping configured. "
                "All dataset URNs will use the raw BigID connection type as the platform.",
            )

        # all-classifications map: original_name → glossary_id + friendly_name
        try:
            classifications = self.client.get_all_classifications()
            for clf in classifications:
                orig = clf.get("original_name", "")
                g_id = clf.get("glossary_id")
                fname = clf.get("friendly_name")
                if orig and g_id:
                    self._glossary_id_map[orig] = g_id
                    if fname:
                        self._friendly_name_map[g_id] = fname
                elif orig and not g_id:
                    self.report.classifiers_without_glossary_id += 1
                    if fname:
                        self._classifier_friendly_names[orig] = fname
        except BigIDAPIError as exc:
            self.report.warning("all-classifications unavailable", context=str(exc))

        # business glossary items
        try:
            self._glossary_items = self.client.get_glossary_items()
        except BigIDAPIError as exc:
            self.report.warning("glossary-items unavailable", context=str(exc))

        # IDSoR attribute map from results-tuning/attributes
        if self.config.sync_idsor:
            try:
                self._idsor_attr_map = self.client.get_idsor_attribute_map()
            except BigIDAPIError as exc:
                self.report.warning("idsor-attributes unavailable", context=str(exc))

        # If both enrichment registries are empty, no enrichment will occur at all
        if not self._glossary_items and not self._glossary_id_map:
            self.report.failure(
                "registries-empty",
                context="Both glossary items and classification map failed to load. "
                "No enrichment will be applied. Check BigID API connectivity.",
            )

    # ------------------------------------------------------------------
    # Step 1 — Root GlossaryNode
    # ------------------------------------------------------------------

    def _emit_root_glossary_node(self) -> Iterator[MetadataWorkUnit]:
        node_urn = _BIGID_ROOT_GLOSSARY_NODE_URN
        node_info = GlossaryNodeInfoClass(
            name="BigID",
            definition="Business Glossary terms imported from BigID",
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=node_urn,
            aspect=node_info,
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=node_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()
        self.report.glossary_nodes_emitted += 1

    def _emit_idsor_glossary_node(self) -> Iterator[MetadataWorkUnit]:
        node_urn = _BIGID_IDSOR_GLOSSARY_NODE_URN
        yield MetadataChangeProposalWrapper(
            entityUrn=node_urn,
            aspect=GlossaryNodeInfoClass(
                name="IDSoR",
                definition=(
                    "Auto-generated GlossaryTerms from BigID Identity Source of Record (IDSoR) "
                    "correlation findings. IDSoR attributes are produced by BigID's correlation "
                    "engine when column values match rows in a configured Correlation Set."
                ),
                parentNode=_BIGID_ROOT_GLOSSARY_NODE_URN,
            ),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=node_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()
        self.report.glossary_nodes_emitted += 1

    # ------------------------------------------------------------------
    # Steps 2/3 — Domain entities
    # ------------------------------------------------------------------

    def _emit_domain_entities(self) -> Iterator[MetadataWorkUnit]:
        """Pre-emit all unique domain + sub_domain entities from glossary items."""
        seen_domains: set[str] = set()
        seen_sub_domains: dict[str, str] = {}  # sub_domain slug → parent domain slug

        for item in self._glossary_items:
            domain_val = item.get("domain", "")
            sub_domain_val = item.get("sub_domain", "")
            if domain_val:
                seen_domains.add(domain_val)
            if sub_domain_val and domain_val:
                seen_sub_domains[sub_domain_val] = domain_val

        # Top-level domains first
        for domain_val in seen_domains:
            domain_urn = f"urn:li:domain:bigid.{_slugify(domain_val)}"
            if domain_urn not in self._emitted_domain_urns:
                try:
                    yield MetadataChangeProposalWrapper(
                        entityUrn=domain_urn,
                        aspect=DomainPropertiesClass(name=domain_val, description=""),
                    ).as_workunit()
                except Exception as exc:
                    self.report.warning(
                        "domain-entity-failed",
                        context=f"domain={domain_val!r}: {exc}",
                    )
                    continue
                self._emitted_domain_urns.add(domain_urn)

        # Sub-domains with parent linkage
        for sub_domain_val, parent_domain_val in seen_sub_domains.items():
            sub_urn = f"urn:li:domain:bigid.{_slugify(sub_domain_val)}"
            parent_urn = f"urn:li:domain:bigid.{_slugify(parent_domain_val)}"
            if sub_urn not in self._emitted_domain_urns:
                try:
                    yield MetadataChangeProposalWrapper(
                        entityUrn=sub_urn,
                        aspect=DomainPropertiesClass(
                            name=sub_domain_val,
                            description="",
                            parentDomain=parent_urn,
                        ),
                    ).as_workunit()
                except Exception as exc:
                    self.report.warning(
                        "domain-entity-failed",
                        context=f"sub_domain={sub_domain_val!r}: {exc}",
                    )
                    continue
                self._emitted_domain_urns.add(sub_urn)

    # ------------------------------------------------------------------
    # Step 4 — GlossaryTerms
    # ------------------------------------------------------------------

    def _glossary_term_urn(self, item: dict[str, Any]) -> str:
        g_id = item.get("glossary_id", "")
        if g_id:
            return f"urn:li:glossaryTerm:bigid.{g_id}"
        return f"urn:li:glossaryTerm:bigid.{_slugify(item.get('name', 'unknown'))}"

    def _should_include_item(self, item: dict[str, Any]) -> bool:
        item_type = item.get("type", "")
        # OOTB Personal Data Items are always included (column enrichment references their URNs)
        if item.get("is_ootb") and item_type == "Personal Data Item":
            return True
        return item_type in self.config.item_types

    def _emit_glossary_terms(self) -> Iterator[MetadataWorkUnit]:
        root_node_urn = _BIGID_ROOT_GLOSSARY_NODE_URN

        for item in self._glossary_items:
            if not self._should_include_item(item):
                continue
            try:
                yield from self._emit_single_glossary_term(item, root_node_urn)
            except Exception as exc:
                self.report.warning(
                    "glossary-term-failed",
                    context=f"item_id={item.get('_id')!r} name={item.get('name')!r}: {exc}",
                )

    def _emit_single_glossary_term(
        self, item: dict[str, Any], root_node_urn: str
    ) -> Iterator[MetadataWorkUnit]:
        term_urn = self._glossary_term_urn(item)
        owner_val = item.get("owner", "")
        domain_val = item.get("domain", "")
        sub_domain_val = item.get("sub_domain", "")

        custom_props: dict[str, str] = {
            "bigid_type": item.get("type", ""),
            "bigid_is_ootb": str(item.get("is_ootb", False)).lower(),
            "bigid_glossary_id": item.get("glossary_id", ""),
            "bigid_id": item.get("_id", ""),
            "bigid_update_date": item.get("update_date", ""),
        }
        if owner_val:
            custom_props["bigid_owner"] = owner_val
        if domain_val:
            custom_props["bigid_domain"] = domain_val
        if sub_domain_val:
            custom_props["bigid_sub_domain"] = sub_domain_val

        term_info = GlossaryTermInfoClass(
            name=item.get("name", ""),
            definition=item.get("description") or "",
            termSource="EXTERNAL",
            parentNode=root_node_urn,
            customProperties=custom_props,
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=term_urn,
            aspect=term_info,
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=term_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # Ownership
        if owner_val and self.config.owner_type != "none":
            if self.config.owner_type == "user":
                owner_urn = builder.make_user_urn(owner_val)
            else:
                owner_urn = builder.make_group_urn(owner_val)
            yield MetadataChangeProposalWrapper(
                entityUrn=term_urn,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(
                            owner=owner_urn,
                            type=OwnershipTypeClass.DATAOWNER,
                        )
                    ]
                ),
            ).as_workunit()

        # Domains on the term
        if self.config.domain_mode in ("auto_namespaced", "config_map"):
            domain_urn = self._resolve_domain_urn(domain_val, sub_domain_val)
            if domain_urn:
                yield MetadataChangeProposalWrapper(
                    entityUrn=term_urn,
                    aspect=DomainsClass(domains=[domain_urn]),
                ).as_workunit()

        self.report.glossary_terms_emitted += 1

    def _resolve_domain_urn(self, domain_val: str, sub_domain_val: str) -> Optional[str]:
        if self.config.domain_mode == "auto_namespaced":
            if sub_domain_val:
                return f"urn:li:domain:bigid.{_slugify(sub_domain_val)}"
            if domain_val:
                return f"urn:li:domain:bigid.{_slugify(domain_val)}"
        elif self.config.domain_mode == "config_map":
            key = sub_domain_val or domain_val
            return self.config.domain_mapping.get(key)
        return None

    # ------------------------------------------------------------------
    # Step 5 — Catalog processing (tags + dataset enrichment + columns)
    # ------------------------------------------------------------------

    def _process_catalog(self) -> Iterator[MetadataWorkUnit]:
        # Build name-based glossary lookup for businessTerm.* prefix resolution
        name_to_glossary_id: dict[str, str] = {
            item.get("name", ""): item.get("glossary_id", "")
            for item in self._glossary_items
            if item.get("name") and item.get("glossary_id")
        }

        # Single-pass catalog design: buffer all objects (~2 KB each; ~5.6 MB for 2,800 objects)
        # to collect unique tag pairs and process objects in one API call rather than two.
        seen_tag_pairs: set[tuple[str, str, str]] = set()
        catalog_objects: list[dict[str, Any]] = []

        try:
            for obj in self.client.get_catalog_objects():
                catalog_objects.append(obj)
                if self.config.sync_tags:
                    for tag in obj.get("tags", []):
                        if tag.get("properties", {}).get("hidden"):
                            continue
                        app_type = tag.get("properties", {}).get("applicationType", "")
                        if app_type not in self.config.tag_application_types:
                            continue
                        tag_name = tag.get("tagName", "")
                        tag_value = tag.get("tagValue", "")
                        if tag_name and tag_value:
                            seen_tag_pairs.add((tag_name, tag_value, app_type))
        except BigIDAPIError as exc:
            self.report.warning(
                "catalog-objects-partial",
                context=f"Fetch interrupted after {len(catalog_objects)} objects: {exc}",
            )

        # Emit tag entities before any dataset enrichment so Tag entities exist first
        for tag_name, tag_value, app_type in seen_tag_pairs:
            if app_type == "risk" and _RISK_SCORE_TAG_NAME in tag_name:
                continue  # riskScore → StructuredProperty, not a Tag entity
            yield from self._emit_tag_entity(tag_name, tag_value)

        for obj in catalog_objects:
            try:
                yield from self._process_catalog_object(obj, name_to_glossary_id)
            except Exception as exc:
                self.report.warning(
                    "catalog-object-failed",
                    context=f"fqn={obj.get('fullyQualifiedName')!r}: {exc}",
                )

    @staticmethod
    def _tag_display_name(tag_name: str, tag_value: str) -> str:
        """Return a human-readable display name for a tag entity.

        Strips the BigID-internal `system.*` prefix so the meaningful segment
        is visible in the DataHub UI pill.  The URN is unaffected.
        Examples:
          system.sensitivityClassification.Sensitivity + Restricted -> Sensitivity : Restricted
          system.risk.riskGroup + high                              -> riskGroup : high
          Sen.Priority + P3                                         -> Sen.Priority : P3
        """
        segment = tag_name.rsplit(".", 1)[-1] if tag_name.startswith("system.") else tag_name
        return f"{segment} : {tag_value}"

    def _emit_tag_entity(self, tag_name: str, tag_value: str) -> Iterator[MetadataWorkUnit]:
        tag_urn = f"urn:li:tag:bigid.{_encode_urn_name(tag_name)}:{_encode_urn_name(tag_value)}"
        yield MetadataChangeProposalWrapper(
            entityUrn=tag_urn,
            aspect=TagPropertiesClass(
                name=self._tag_display_name(tag_name, tag_value),
                description=tag_name,
            ),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=tag_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()
        self.report.tag_entities_emitted += 1

    def _emit_risk_score_structured_property(self) -> Iterator[MetadataWorkUnit]:
        prop_urn = self.config.risk_score_structured_property_urn
        yield MetadataChangeProposalWrapper(
            entityUrn=prop_urn,
            aspect=StructuredPropertyDefinitionClass(
                qualifiedName="bigid.riskScore",
                displayName="BigID Risk Score",
                valueType="urn:li:dataType:datahub.number",
                cardinality="SINGLE",
                entityTypes=["urn:li:entityType:datahub.dataset"],
                description="Numeric risk score from BigID (0–100).",
            ),
        ).as_workunit()

    # ------------------------------------------------------------------
    # Per-catalog-object processing
    # ------------------------------------------------------------------

    def _process_catalog_object(
        self,
        obj: dict[str, Any],
        name_to_glossary_id: dict[str, str],
    ) -> Iterator[MetadataWorkUnit]:
        fqn = obj.get("fullyQualifiedName", "")
        source_name = obj.get("source", "")  # BigID connection name (first FQN segment)
        object_name = obj.get("objectName", "")
        scanner_type_group = obj.get("scanner_type_group", "")
        is_structured = scanner_type_group == "structured"

        dataset_urn = self._make_dataset_urn(fqn, source_name)
        if not dataset_urn:
            return

        now_ms = int(time.time() * 1000)
        scan_date_str = obj.get("scanDate") or obj.get("last_scanned", "")
        scan_ts_ms = _parse_iso_to_ms(scan_date_str) or now_ms

        tags = obj.get("tags", [])
        object_tags = [
            t for t in tags
            if not t.get("properties", {}).get("hidden")
            and t.get("tagType") == "OBJECT"
            and t.get("properties", {}).get("applicationType", "") in self.config.tag_application_types
        ]

        # --- Pre-fetch columns once (used by both creation and enrichment) ---
        columns: list[dict[str, Any]] = []
        if is_structured:
            if not object_name:
                self.report.warning(
                    "missing-object-name",
                    context=f"Skipping column fetch for {fqn!r} — objectName field is empty",
                )
            else:
                try:
                    columns = self.client.get_columns(object_name, source_name, fqn=fqn)
                except BigIDAPIError as exc:
                    self.report.warning(
                        "column-fetch-failed",
                        context=f"{source_name}/{object_name}: {exc}",
                    )

        # --- Dataset creation (opt-in) ---
        if self.config.create_datasets:
            yield from self._maybe_emit_dataset_creation(
                obj, dataset_urn, fqn, object_name, is_structured, columns
            )

        # --- GlobalTags (dataset-level) ---
        if self.config.sync_tags and object_tags:
            yield from self._emit_dataset_tags(dataset_urn, object_tags)

        self.report.datasets_enriched += 1

        # --- dataPlatformInstance (only when platform_instance is configured) ---
        yield from self._emit_platform_instance(dataset_urn, source_name)

        # --- Structured sources: column enrichment + DatasetProfile ---
        if is_structured:
            if columns:
                yield from self._emit_schema_field_enrichment(
                    dataset_urn, columns, source_name, name_to_glossary_id, now_ms
                )
                yield from self._emit_dataset_profile(dataset_urn, columns, scan_ts_ms)
                self.report.columns_enriched += len(columns)
        else:
            attr_details = obj.get("attribute_details", [])
            if self.config.sync_unstructured_enrichment and attr_details:
                yield from self._emit_unstructured_dataset_enrichment(
                    obj, dataset_urn, source_name, name_to_glossary_id, now_ms, scan_ts_ms
                )
            else:
                self.report.datasets_skipped_unstructured += 1

    def _emit_unstructured_dataset_enrichment(
        self,
        obj: dict[str, Any],
        dataset_urn: str,
        source_name: str,
        name_to_glossary_id: dict[str, str],
        now_ms: int,
        scan_ts_ms: int,
    ) -> Iterator[MetadataWorkUnit]:
        """Emit dataset-level GlossaryTerms and DatasetProfile for unstructured objects.

        Reads attribute_details from the catalog API response (already in memory) instead
        of calling the columns API.  Each attribute maps to a GlossaryTerm at the dataset
        level (not a schema field) because unstructured content has no column schema.
        """
        attr_details: list[dict[str, Any]] = obj.get("attribute_details", [])

        # Build clf_stats so value-type classifiers get their finding count in
        # MetadataAttribution sourceDetail, matching the structured path behaviour.
        clf_stats: dict[str, dict[str, str]] = {}
        for entry in attr_details:
            name = entry.get("name", "")
            count = entry.get("count")
            if name and count is not None:
                bare = name.replace("classifier.", "", 1) if name.startswith("classifier.") else name
                clf_stats[bare] = {"row_count": str(count), "distinct_count": ""}

        seen_term_urns: set[str] = set()
        term_assocs: list[GlossaryTermAssociationClass] = []
        pending_new_terms: list[tuple[str, str]] = []

        for attr in attr_details:
            result = self._resolve_attr_enrichment(
                attr, clf_stats, source_name, now_ms, name_to_glossary_id
            )
            if result is None:
                continue
            term_assoc, _tag_urns, classifier_to_emit = result
            # Confidence-level tags are per-field context; skip them for unstructured datasets.
            if term_assoc.urn not in seen_term_urns:
                seen_term_urns.add(term_assoc.urn)
                term_assocs.append(term_assoc)
            if classifier_to_emit is not None:
                pending_new_terms.append(classifier_to_emit)

        # Emit new term entities before the GlossaryTerms MCP so referenced entities exist.
        seen_pending: set[str] = set()
        for attr_name, term_urn in pending_new_terms:
            if term_urn in seen_pending:
                continue
            seen_pending.add(term_urn)
            if ":bigid.idsor." in term_urn:
                yield from self._emit_idsor_term(attr_name, term_urn)
            elif ":bigid.classifier." in term_urn:
                yield from self._emit_classifier_term(attr_name, term_urn)
            else:
                yield from self._emit_idsor_linked_term(attr_name, term_urn)

        if term_assocs:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=GlossaryTermsClass(
                    terms=term_assocs,
                    auditStamp=AuditStampClass(time=now_ms, actor="urn:li:corpuser:datahub"),
                ),
            ).as_workunit()

        # DatasetProfile: total_pii_count as rowCount, sizeInBytes if present.
        # columnCount=0 signals "no schema" in the DataHub UI.
        total_pii = obj.get("total_pii_count")
        size_bytes = obj.get("sizeInBytes")
        row_count: Optional[int] = None
        size_val: Optional[int] = None
        if total_pii:
            try:
                row_count = int(total_pii)
            except (ValueError, TypeError):
                pass
        if size_bytes:
            try:
                size_val = int(size_bytes)
            except (ValueError, TypeError):
                pass

        if row_count is not None or size_val is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetProfileClass(
                    timestampMillis=scan_ts_ms,
                    rowCount=row_count,
                    columnCount=0,
                    sizeInBytes=size_val,
                ),
            ).as_workunit()

        self.report.datasets_enriched_unstructured += 1

    def _extract_dataset_tag_associations(
        self,
        object_tags: list[dict[str, Any]],
        dataset_urn: str,
    ) -> tuple[list[TagAssociationClass], Optional[float]]:
        """Return (tag_assocs, risk_score_val) from a list of OBJECT-type tags."""
        tag_assocs: list[TagAssociationClass] = []
        risk_score_val: Optional[float] = None
        for tag in object_tags:
            tag_name = tag.get("tagName", "")
            tag_value = tag.get("tagValue", "")
            app_type = tag.get("properties", {}).get("applicationType", "")
            if not tag_name or not tag_value:
                continue
            if app_type == "risk" and _RISK_SCORE_TAG_NAME in tag_name:
                try:
                    risk_score_val = float(tag_value)
                except ValueError:
                    logger.warning(
                        "Non-numeric riskScore value %r for dataset %s -- skipping structured property.",
                        tag_value, dataset_urn,
                    )
                continue
            tag_urn = f"urn:li:tag:bigid.{_encode_urn_name(tag_name)}:{_encode_urn_name(tag_value)}"
            tag_assocs.append(TagAssociationClass(tag=tag_urn))
        return tag_assocs, risk_score_val

    def _emit_dataset_tags(
        self, dataset_urn: str, object_tags: list[dict[str, Any]]
    ) -> Iterator[MetadataWorkUnit]:
        """Emit GlobalTags and riskScore StructuredProperty for a dataset."""
        tag_assocs, risk_score_val = self._extract_dataset_tag_associations(
            object_tags, dataset_urn
        )
        if tag_assocs:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=GlobalTagsClass(tags=tag_assocs),
            ).as_workunit()
        if risk_score_val is not None:
            for mcp in (
                DatasetPatchBuilder(dataset_urn)
                .set_structured_property(
                    self.config.risk_score_structured_property_urn,
                    risk_score_val,
                )
                .build()
            ):
                yield MetadataWorkUnit(id=f"{dataset_urn}-riskScore-patch", mcp_raw=mcp)

    def _emit_platform_instance(
        self, dataset_urn: str, source_name: str
    ) -> Iterator[MetadataWorkUnit]:
        """Emit dataPlatformInstance only when a platform_instance is configured.

        Emitting with instance=None in enrichment mode would UPSERT-overwrite the
        value already set by a native connector (e.g. Snowflake, BigQuery).
        """
        platform_instance = self._get_platform_instance(source_name)
        if not platform_instance:
            return
        platform = self._get_platform(source_name)
        platform_urn = f"urn:li:dataPlatform:{platform}"
        instance_urn = f"urn:li:dataPlatformInstance:({platform_urn},{platform_instance})"
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DataPlatformInstanceClass(
                platform=platform_urn,
                instance=instance_urn,
            ),
        ).as_workunit()

    def _maybe_emit_dataset_creation(
        self,
        obj: dict[str, Any],
        dataset_urn: str,
        fqn: str,
        object_name: str,
        is_structured: bool,
        columns: list[dict[str, Any]],
    ) -> Iterator[MetadataWorkUnit]:
        # Check checkpoint — only create on first encounter
        if dataset_urn in self._seen_dataset_urns:
            return

        # Emit DatasetProperties
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(
                name=object_name,
                qualifiedName=fqn,
                customProperties={"bigid_source": "true"},
            ),
        ).as_workunit()

        # Emit SchemaMetadata for structured sources using pre-fetched columns
        if is_structured and columns:
            schema_fields = []
            for col in sorted(columns, key=lambda c: c.get("order", 9999)):
                schema_fields.append(
                    SchemaFieldClass(
                        fieldPath=col.get("columnName", ""),
                        type=_map_field_type(col.get("fieldType", "")),
                        nativeDataType=col.get("fieldType", ""),
                        nullable=col.get("nullable", True),
                        isPartOfKey=col.get("isPrimary", False),
                    )
                )
            platform = self._get_platform(obj.get("source", ""))
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SchemaMetadataClass(
                    schemaName=fqn,
                    platform=f"urn:li:dataPlatform:{platform}",
                    version=0,
                    hash="",
                    platformSchema=OtherSchemaClass(rawSchema=""),
                    fields=schema_fields,
                ),
            ).as_workunit()

        self._seen_dataset_urns.add(dataset_urn)
        self.report.datasets_created += 1

    # ------------------------------------------------------------------
    # Column enrichment
    # ------------------------------------------------------------------

    def _emit_schema_field_enrichment(
        self,
        dataset_urn: str,
        columns: list[dict[str, Any]],
        source_name: str,
        name_to_glossary_id: dict[str, str],
        now_ms: int,
    ) -> Iterator[MetadataWorkUnit]:
        # Keyed by fieldPath so columns from multiple BigID connections are merged.
        # GMS rejects editableSchemaMetadata with duplicate fieldPaths.
        editable_fields: dict[str, _FieldEnrichment] = {}
        # Term entities (classifier or IDSoR) that need to be emitted for the first time this
        # run. Collected here and emitted after the loop so entity emission is not a
        # mid-iteration side effect.
        pending_new_terms: list[tuple[str, str]] = []  # (attr_name, term_urn)

        for col in columns:
            field_path = col.get("columnName", "")
            if not field_path:
                continue

            clf_stats = self._build_clf_stats(col.get("fieldClassifications", []))

            for attr in col.get("attributeDetails", []):
                result = self._resolve_attr_enrichment(
                    attr, clf_stats, source_name, now_ms, name_to_glossary_id
                )
                if result is None:
                    continue
                term_assoc, tag_urns, classifier_to_emit = result

                if field_path not in editable_fields:
                    editable_fields[field_path] = _FieldEnrichment()
                editable_fields[field_path].terms.append(term_assoc)
                editable_fields[field_path].tag_urns.extend(tag_urns)

                if classifier_to_emit is not None:
                    pending_new_terms.append(classifier_to_emit)

        # Emit new term entities before the schema enrichment MCP so GlossaryTerm
        # entities always exist before they are referenced.
        # Each emit method updates the appropriate dedup set and counter on success.
        seen_pending: set[str] = set()
        for attr_name, term_urn in pending_new_terms:
            if term_urn in seen_pending:
                continue
            seen_pending.add(term_urn)
            if ":bigid.idsor." in term_urn:
                yield from self._emit_idsor_term(attr_name, term_urn)
            elif ":bigid.classifier." in term_urn:
                yield from self._emit_classifier_term(attr_name, term_urn)
            else:
                # IDSoR path 1 — linked term (fn_item_*, bt_*, etc.); ensure name is set
                yield from self._emit_idsor_linked_term(attr_name, term_urn)

        if not editable_fields:
            return

        field_info_list: list[EditableSchemaFieldInfoClass] = []
        for fp, enrichment in editable_fields.items():
            field_glossary_terms = GlossaryTermsClass(
                terms=enrichment.terms,
                auditStamp=AuditStampClass(time=now_ms, actor="urn:li:corpuser:datahub"),
            )
            field_tags = (
                GlobalTagsClass(tags=[TagAssociationClass(tag=u) for u in enrichment.tag_urns])
                if enrichment.tag_urns
                else None
            )
            field_info_list.append(
                EditableSchemaFieldInfoClass(
                    fieldPath=fp,
                    glossaryTerms=field_glossary_terms,
                    globalTags=field_tags,
                )
            )
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=EditableSchemaMetadataClass(editableSchemaFieldInfo=field_info_list),
        ).as_workunit()

    @staticmethod
    def _build_clf_stats(
        field_classifications: list[dict[str, Any]],
    ) -> dict[str, dict[str, str]]:
        """Build classificationName → {row_count, distinct_count} from fieldClassifications."""
        clf_stats: dict[str, dict[str, str]] = {}
        for fc in field_classifications:
            clf_name = fc.get("classificationName", "")
            if clf_name:
                clf_stats[clf_name] = {
                    "row_count": str(fc.get("rowsOrFieldsCounter", "")),
                    "distinct_count": str(fc.get("distinctValueCount", "")),
                }
        return clf_stats

    @staticmethod
    def _classifier_type(attr_name: str) -> str:
        """Return 'metadata', 'value', or 'business_term' based on attr_name prefix.

        MD:: must be checked before the generic classifier. prefix — the ordering
        here is a correctness invariant.
        """
        if attr_name.startswith("classifier.MD::"):
            return "metadata"
        if attr_name.startswith("classifier."):
            return "value"
        return "business_term"

    def _resolve_idsor_term(
        self, attr: dict[str, Any], attr_name: str
    ) -> Optional[tuple[str, Optional[str], Optional[tuple[str, str]]]]:
        """Resolve an IDSoR attribute to (term_urn, resolved_friendly, classifier_to_emit).

        Returns None if the attribute name produces an empty slug.
        classifier_to_emit is (attr_name, term_urn) when the term entity needs to be emitted
        for the first time; None when already emitted this run.
        """
        info = self._idsor_attr_map.get(attr_name)
        if info:
            friendly_name, g_id = info
            if g_id:
                # Path 1: linked to an existing BigID glossary item — reuse its URN.
                term_urn = f"urn:li:glossaryTerm:bigid.{g_id}"
                classifier_to_emit: Optional[tuple[str, str]] = (
                    (attr_name, term_urn) if term_urn not in self._emitted_idsor_linked_term_urns else None
                )
                return term_urn, friendly_name, classifier_to_emit
            else:
                # Path 2: in map but no glossaryId — auto-generate under bigid.idsor
                slug = _slugify(friendly_name) or _slugify(attr_name)
                if not slug:
                    logger.debug("IDSoR path 2: empty slug for attr %r — skipping", attr_name)
                    return None
                term_urn = f"urn:li:glossaryTerm:bigid.idsor.{slug}"
                classifier_to_emit = (
                    (attr_name, term_urn) if term_urn not in self._emitted_idsor_term_urns else None
                )
                return term_urn, friendly_name, classifier_to_emit
        else:
            # Path 3: not in map — auto-generate from the raw attribute name
            slug = _slugify(attr_name)
            if not slug:
                logger.debug("IDSoR path 3: empty slug for attr %r — skipping", attr_name)
                return None
            term_urn = f"urn:li:glossaryTerm:bigid.idsor.{slug}"
            resolved_friendly = attr_name.replace("_", " ").title()
            classifier_to_emit = (
                (attr_name, term_urn) if term_urn not in self._emitted_idsor_term_urns else None
            )
            return term_urn, resolved_friendly, classifier_to_emit

    def _resolve_term_urn_and_pending(
        self,
        attr: dict[str, Any],
        attr_name: str,
        name_to_glossary_id: dict[str, str],
    ) -> Optional[tuple[str, Optional[str], Optional[tuple[str, str]], str]]:
        """Resolve an attribute to (term_urn, resolved_friendly, classifier_to_emit, clf_type).

        Returns None if the attribute should be skipped (no mapping, disabled, empty slug).
        """
        glossary_id = self._resolve_attr_to_glossary_id(attr_name, name_to_glossary_id)

        if glossary_id:
            term_urn = f"urn:li:glossaryTerm:bigid.{glossary_id}"
            resolved_friendly = self._friendly_name_map.get(glossary_id)
            clf_type = self._classifier_type(attr_name)
            return term_urn, resolved_friendly, None, clf_type

        if self.config.sync_unlinked_classifiers and attr_name.startswith("classifier."):
            term_urn = self._classifier_term_urn(attr_name)
            if term_urn is None:
                return None
            resolved_friendly = self._classifier_friendly_names.get(attr_name)
            clf_type = self._classifier_type(attr_name)
            classifier_to_emit: Optional[tuple[str, str]] = (
                (attr_name, term_urn) if term_urn not in self._emitted_classifier_term_urns else None
            )
            return term_urn, resolved_friendly, classifier_to_emit, clf_type

        if self.config.sync_idsor and _is_idsor_attr(attr):
            result = self._resolve_idsor_term(attr, attr_name)
            if result is None:
                return None
            term_urn, resolved_friendly, classifier_to_emit = result
            return term_urn, resolved_friendly, classifier_to_emit, "idsor_attribute"

        return None

    def _resolve_attr_enrichment(
        self,
        attr: dict[str, Any],
        clf_stats: dict[str, dict[str, str]],
        source_name: str,
        now_ms: int,
        name_to_glossary_id: dict[str, str],
    ) -> Optional[tuple[GlossaryTermAssociationClass, list[str], Optional[tuple[str, str]]]]:
        """Resolve one attributeDetails entry to enrichment data.

        Returns (term_assoc, confidence_tag_urns, classifier_to_emit) or None if skipped.
        classifier_to_emit is (attr_name, term_urn) when a new term entity needs to be
        emitted; None when the term already exists this run or no new entity is needed.
        """
        attr_name = attr.get("name", "")
        ranks = attr.get("ranks", [])
        rank = str(ranks[0]) if ranks else "LOW"

        confidence_float = _rank_to_float(rank)
        if confidence_float < self.config.minimum_confidence_threshold:
            self.report.findings_below_threshold += 1
            return None

        resolved = self._resolve_term_urn_and_pending(attr, attr_name, name_to_glossary_id)
        if resolved is None:
            return None
        term_urn, resolved_friendly, classifier_to_emit, clf_type = resolved

        source_detail: dict[str, str] = {
            "classifier_name": attr_name,
            "classifier_type": clf_type,
            "confidence_level": rank.upper(),
            "bigid_connection": source_name,
        }

        if clf_type == "value":
            bare_name = attr_name.replace("classifier.", "", 1)
            stats = clf_stats.get(bare_name) or clf_stats.get(attr_name) or {}
            if stats.get("row_count"):
                source_detail["row_count"] = stats["row_count"]
            if stats.get("distinct_count"):
                source_detail["distinct_count"] = stats["distinct_count"]
        elif clf_type == "idsor_attribute":
            count = attr.get("count")
            if count is not None:
                source_detail["row_count"] = str(count)

        if rank.upper() == "HIGH" and resolved_friendly:
            source_detail["classifier_friendly_name"] = resolved_friendly

        attribution = MetadataAttributionClass(
            time=now_ms,
            actor="urn:li:corpuser:datahub",
            source="urn:li:dataPlatform:bigid",
            sourceDetail=source_detail,
        )
        term_assoc = GlossaryTermAssociationClass(urn=term_urn, attribution=attribution)

        tag_urns: list[str] = []
        if self.config.confidence_level_tag:
            tag_urns.append(f"urn:li:tag:bigid.confidence:{rank.upper()}")

        return term_assoc, tag_urns, classifier_to_emit

    def _resolve_attr_to_glossary_id(
        self, attr_name: str, name_to_glossary_id: dict[str, str]
    ) -> Optional[str]:
        """Resolve an attributeDetails name to a BigID glossary_id."""
        if attr_name.startswith("classifier."):
            # Both classifier.* and classifier.MD::* look up in all-classifications map
            return self._glossary_id_map.get(attr_name)
        if attr_name.startswith("businessTerm."):
            # Strip prefix → name lookup in business glossary items
            bare_name = attr_name[len("businessTerm."):]
            return name_to_glossary_id.get(bare_name)
        return None

    @staticmethod
    def _strip_classifier_prefix(attr_name: str) -> str:
        """Strip 'classifier.' and optional 'MD::' prefix, returning the bare name."""
        bare = attr_name[len("classifier."):]
        if bare.startswith("MD::"):
            bare = bare[len("MD::"):]
        return bare

    def _classifier_term_urn(self, attr_name: str) -> Optional[str]:
        """Build a deterministic GlossaryTerm URN for an unlinked BigID classifier.

        Returns None when the classifier name produces an empty slug (malformed input).
        """
        slug = _slugify(self._strip_classifier_prefix(attr_name))
        if not slug:
            return None
        return f"urn:li:glossaryTerm:bigid.classifier.{slug}"

    def _try_build_term_mcps(
        self,
        term_urn: str,
        term_info: GlossaryTermInfoClass,
        warning_key: str,
        attr_name: str,
    ) -> Optional[list[MetadataWorkUnit]]:
        """Build GlossaryTermInfo + Status MCPs; return None and warn on failure."""
        try:
            return [
                MetadataChangeProposalWrapper(entityUrn=term_urn, aspect=term_info).as_workunit(),
                MetadataChangeProposalWrapper(
                    entityUrn=term_urn, aspect=StatusClass(removed=False)
                ).as_workunit(),
            ]
        except Exception as e:
            self.report.warning(
                warning_key,
                context=f"attr_name={attr_name!r} term_urn={term_urn!r} error={e}",
            )
            return None

    def _emit_classifier_term(self, attr_name: str, term_urn: str) -> Iterator[MetadataWorkUnit]:
        """Emit GlossaryTermInfo + Status for an unlinked classifier (demand-driven)."""
        friendly = self._classifier_friendly_names.get(attr_name, "")
        bare = self._strip_classifier_prefix(attr_name)
        display_name = friendly if friendly else bare.replace("_", " ").title()
        term_info = GlossaryTermInfoClass(
            name=display_name,
            definition=f"Auto-generated from BigID classifier: {attr_name}",
            termSource="EXTERNAL",
            parentNode=_BIGID_ROOT_GLOSSARY_NODE_URN,
            customProperties={"bigid_type": "classifier", "bigid_classifier_name": attr_name},
        )
        mcps = self._try_build_term_mcps(term_urn, term_info, "classifier-term-build-failed", attr_name)
        if mcps is None:
            return
        yield from mcps
        self.report.classifier_terms_emitted += 1
        self._emitted_classifier_term_urns.add(term_urn)

    def _emit_idsor_linked_term(self, attr_name: str, term_urn: str) -> Iterator[MetadataWorkUnit]:
        """Emit GlossaryTermInfo + Status for an IDSoR Path 1 linked term (fn_item_* etc.).

        These terms are referenced by their BigID glossaryId and typically do not appear
        in the business_glossary_items API response, so no GlossaryTermInfo is emitted
        by the glossary sync step. Emitting it here ensures the term renders with a
        human-readable name in the DataHub UI rather than the raw fn_item_* ID.
        """
        info = self._idsor_attr_map.get(attr_name)
        if info:
            display_name, g_id = info
        else:
            display_name = attr_name.replace("_", " ").title()
            g_id = None
        custom_props: dict[str, str] = {
            "bigid_type": "idsor_attribute",
            "bigid_attribute_name": attr_name,
        }
        if g_id:
            custom_props["bigid_glossary_id"] = g_id
        term_info = GlossaryTermInfoClass(
            name=display_name,
            definition=f"Linked from BigID IDSoR attribute: {attr_name}",
            termSource="EXTERNAL",
            parentNode=_BIGID_IDSOR_GLOSSARY_NODE_URN,
            customProperties=custom_props,
        )
        mcps = self._try_build_term_mcps(term_urn, term_info, "idsor-linked-term-build-failed", attr_name)
        if mcps is None:
            return
        yield from mcps
        self.report.idsor_terms_emitted += 1
        self._emitted_idsor_linked_term_urns.add(term_urn)

    def _emit_idsor_term(self, attr_name: str, term_urn: str) -> Iterator[MetadataWorkUnit]:
        """Emit GlossaryTermInfo + Status for an auto-generated IDSoR term (paths 2 and 3)."""
        info = self._idsor_attr_map.get(attr_name)
        display_name = info[0] if info else attr_name.replace("_", " ").title()
        term_info = GlossaryTermInfoClass(
            name=display_name,
            definition=f"Auto-generated from BigID IDSoR attribute: {attr_name}",
            termSource="EXTERNAL",
            parentNode=_BIGID_IDSOR_GLOSSARY_NODE_URN,
            customProperties={"bigid_type": "idsor_attribute", "bigid_attribute_name": attr_name},
        )
        mcps = self._try_build_term_mcps(term_urn, term_info, "idsor-term-build-failed", attr_name)
        if mcps is None:
            return
        yield from mcps
        self.report.idsor_terms_emitted += 1
        self._emitted_idsor_term_urns.add(term_urn)

    # ------------------------------------------------------------------
    # DatasetProfile
    # ------------------------------------------------------------------

    def _emit_dataset_profile(
        self,
        dataset_urn: str,
        columns: list[dict[str, Any]],
        timestamp_ms: int,
    ) -> Iterator[MetadataWorkUnit]:
        if not columns:
            return

        # Dataset-level stats from first column's columnProfile
        first_profile = columns[0].get("columnProfile", {})
        row_count: Optional[int] = None
        if "fieldCount" in first_profile:
            try:
                row_count = int(first_profile["fieldCount"])
            except (ValueError, TypeError):
                pass

        field_profiles: list[DatasetFieldProfileClass] = []
        for col in columns:
            fp = _build_field_profile(col)
            if fp is not None:
                field_profiles.append(fp)

        profile = DatasetProfileClass(
            timestampMillis=timestamp_ms,
            rowCount=row_count,
            columnCount=len(columns),
            fieldProfiles=field_profiles if field_profiles else None,
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=profile,
        ).as_workunit()

    # ------------------------------------------------------------------
    # URN helpers
    # ------------------------------------------------------------------

    def _get_platform(self, source_name: str) -> str:
        if source_name in self.config.datasource_platform_mapping:
            return self.config.datasource_platform_mapping[source_name].platform
        return self._platform_map.get(source_name, source_name)

    def _get_env(self, source_name: str) -> str:
        if source_name in self.config.datasource_platform_mapping:
            override_env = self.config.datasource_platform_mapping[source_name].env
            if override_env:
                return override_env
        return self.config.env

    def _get_platform_instance(self, source_name: str) -> Optional[str]:
        if source_name in self.config.datasource_platform_mapping:
            override = self.config.datasource_platform_mapping[source_name].platform_instance
            if override is not None:
                return override
        return self.config.platform_instance  # global default from PlatformInstanceConfigMixin

    def _make_dataset_urn(self, fqn: str, source_name: str) -> Optional[str]:
        """
        Convert a BigID FQN to a DataHub dataset URN.

        BigID FQN: {connection_name}.{remaining parts} (2-4 dot-segments depending on source)
        DataHub URN: urn:li:dataset:(urn:li:dataPlatform:{platform},{remaining},{env})
        """
        if not fqn or not source_name:
            return None

        # Strip the first segment (connection name) — it may contain dots itself,
        # so we strip by prefix match rather than simple split.
        prefix = source_name + "."
        if fqn.startswith(prefix):
            remaining = fqn[len(prefix):]
        else:
            # Fallback: drop the first dot-delimited segment
            parts = fqn.split(".", 1)
            remaining = parts[1] if len(parts) > 1 else fqn

        platform = self._get_platform(source_name)
        env = self._get_env(source_name)
        platform_instance = self._get_platform_instance(source_name)

        if platform in LOWERCASE_PLATFORMS:
            remaining = remaining.lower()

        if platform_instance:
            dataset_name = f"{platform_instance}.{remaining}"
        else:
            dataset_name = remaining

        # Percent-encode reserved URN delimiter characters ( ) , in the name component
        safe_name = _encode_urn_name(dataset_name)
        return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{safe_name},{env})"

