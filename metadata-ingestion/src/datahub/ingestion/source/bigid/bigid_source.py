import logging
import time
from collections.abc import Iterable, Iterator
from typing import Any, Optional

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import DomainKey
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
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigid.bigid_api import BigIDAPIError, BigIDClient
from datahub.ingestion.source.bigid.bigid_report import BigIDSourceReport
from datahub.ingestion.source.bigid.bigid_utils import (
    _build_field_profile,
    _coerce_int,
    _encode_urn_name,
    _is_idsor_attr,
    _map_field_type,
    _parse_iso_to_ms,
    _rank_to_float,
    _slugify,
)
from datahub.ingestion.source.bigid.config import BigIDSourceConfig
from datahub.ingestion.source.bigid.constants import (
    APP_TYPE_RISK,
    BIGID_DATA_PLATFORM_URN,
    BIGID_IDSOR_GLOSSARY_NODE_URN,
    BIGID_PLATFORM_NAME,
    BIGID_ROOT_GLOSSARY_NODE_URN,
    BIGID_TYPE_TO_PLATFORM,
    BUSINESS_TERM_PREFIX,
    CLASSIFIER_MD_PREFIX,
    CLASSIFIER_PREFIX,
    CLF_TYPE_BUSINESS_TERM,
    CLF_TYPE_IDSOR,
    CLF_TYPE_METADATA,
    CLF_TYPE_VALUE,
    DATAHUB_ACTOR_URN,
    LOWERCASE_PLATFORMS,
    RISK_SCORE_TAG_NAME,
    SCANNER_TYPE_STRUCTURED,
    TAG_TYPE_OBJECT,
)
from datahub.ingestion.source.bigid.models import (
    AttrEnrichment,
    BigIDAttributeDetail,
    BigIDCatalogObject,
    BigIDColumn,
    BigIDFieldClassification,
    BigIDGlossaryItem,
    BigIDTag,
    ClassificationStats,
    DatasetTagExtract,
    FieldEnrichment,
    IDSoRAttributeInfo,
    IDSoRResolution,
    PendingTerm,
    TagPair,
    TermResolution,
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
    DomainPropertiesClass,
    DomainsClass,
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

logger = logging.getLogger(__name__)


@platform_name("BigID")
@config_class(BigIDSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(
    SourceCapability.GLOSSARY_TERMS,
    "BigID classification findings as GlossaryTerms on SchemaFields",
)
@capability(SourceCapability.TAGS, "BigID tags applied to datasets and columns")
@capability(
    SourceCapability.SCHEMA_METADATA,
    "Column schema from BigID columns API (requires create_datasets=True)",
)
@capability(
    SourceCapability.DATA_PROFILING,
    "Column-level profiles from BigID columnProfile data",
)
@capability(
    SourceCapability.OWNERSHIP,
    "Ownership on GlossaryTerms (not Datasets); controlled by owner_type config",
)
@capability(
    SourceCapability.DOMAINS,
    "Domain entities created when domain_mode is auto_namespaced or config_map",
)
@capability(
    SourceCapability.DELETION_DETECTION, "Stale entity removal via stateful ingestion"
)
@capability(
    SourceCapability.PLATFORM_INSTANCE,
    "Platform instance emitted per dataset when platform_instance is configured",
)
@capability(SourceCapability.LINEAGE_COARSE, "Not supported", supported=False)
class BigIDSource(StatefulIngestionSourceBase):
    # Enrichment connector: syncs BigID glossary items, tags, classification findings,
    # and profiles onto existing Dataset entities (Datasets are only created when
    # create_datasets=True). Emitted aspects use UPSERT, so tags or terms curated in the
    # DataHub UI are overwritten on the next run — see the connector docs for the
    # recommended deployment pattern.

    config: BigIDSourceConfig
    report: BigIDSourceReport

    def __init__(self, config: BigIDSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.config = config
        self.report = BigIDSourceReport()
        self.client = BigIDSource._build_client(config)
        # Lookup registries populated by _load_registries().
        self._platform_map: dict[str, str] = {}  # connection name → platform
        self._glossary_id_map: dict[str, str] = {}  # original name → glossary id
        self._friendly_name_map: dict[str, str] = {}  # glossary id → friendly name
        self._glossary_items: list[BigIDGlossaryItem] = []
        self._classifier_friendly_names: dict[str, str] = {}  # original name → friendly
        self._idsor_attr_map: dict[str, IDSoRAttributeInfo] = {}  # raw name → info
        # Within-run dedup sets so entities emitted on demand are not re-emitted and
        # create_datasets does not double-emit structural aspects for one URN.
        self._emitted_domain_urns: set[str] = set()
        self._emitted_classifier_term_urns: set[str] = set()
        self._seen_dataset_urns: set[str] = set()
        self._emitted_idsor_term_urns: set[str] = set()
        self._emitted_idsor_linked_term_urns: set[str] = set()

    @classmethod
    def create(cls, config_dict: dict[str, Any], ctx: PipelineContext) -> "BigIDSource":
        config = BigIDSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def _build_client(config: BigIDSourceConfig) -> BigIDClient:
        return BigIDClient(
            bigid_url=config.bigid_url,
            user_token=config.user_token.get_secret_value()
            if config.user_token
            else None,
            access_token=config.access_token.get_secret_value()
            if config.access_token
            else None,
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

    def get_report(self) -> BigIDSourceReport:
        return self.report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        try:
            yield from self._load_and_emit()
        except BigIDAPIError as exc:
            self.report.failure("BigID API error", context=str(exc), exc=exc)
            logger.exception("BigID API error in connector: %s", exc)
        except Exception as exc:
            self.report.failure("unexpected error", context=str(exc), exc=exc)
            logger.exception("Unexpected error in BigID connector: %s", exc)
        finally:
            self.client.close()

    def _load_and_emit(self) -> Iterator[MetadataWorkUnit]:
        self._load_registries()

        # Emit the riskScore definition before any catalog passes so GMS commits it
        # before dataset structuredProperties usages arrive in later batches.
        yield from self._emit_risk_score_structured_property()

        yield from self._emit_root_glossary_node()
        if self.config.sync_idsor:
            yield from self._emit_idsor_glossary_node()

        if self.config.domain_mode == "auto_namespaced":
            yield from self._emit_domain_entities()

        yield from self._emit_glossary_terms()
        yield from self._process_catalog()

    def _load_registries(self) -> None:
        try:
            for connection in self.client.get_connections():
                connection_name = connection.name
                if not connection_name:
                    continue
                if not self.config.connection_pattern.allowed(connection_name):
                    continue
                if connection_name in self.config.datasource_platform_mapping:
                    self._platform_map[connection_name] = (
                        self.config.datasource_platform_mapping[
                            connection_name
                        ].platform
                    )
                else:
                    connection_type = connection.conn_type
                    platform = BIGID_TYPE_TO_PLATFORM.get(connection_type)
                    if platform:
                        self._platform_map[connection_name] = platform
                    else:
                        self.report.warning(
                            "unknown-connection-type",
                            context=f"type={connection_type!r} connection={connection_name!r} — no platform mapping; URNs use raw type",
                        )
                        self._platform_map[connection_name] = connection_type
                        self.report.report_connection_no_platform(connection_name)
        except BigIDAPIError as exc:
            self.report.warning("ds-connections unavailable", context=str(exc), exc=exc)

        if not self._platform_map and not self.config.datasource_platform_mapping:
            self.report.warning(
                "platform-map-empty",
                context="ds-connections API failed and no datasource_platform_mapping configured. "
                "All dataset URNs will use the raw BigID connection type as the platform.",
            )

        try:
            for classification in self.client.get_all_classifications():
                original_name = classification.original_name
                glossary_id = classification.glossary_id
                friendly_name = classification.friendly_name
                if original_name and glossary_id:
                    self._glossary_id_map[original_name] = glossary_id
                    if friendly_name:
                        self._friendly_name_map[glossary_id] = friendly_name
                elif original_name and not glossary_id:
                    self.report.classifiers_without_glossary_id += 1
                    if friendly_name:
                        self._classifier_friendly_names[original_name] = friendly_name
        except BigIDAPIError as exc:
            self.report.warning(
                "all-classifications unavailable", context=str(exc), exc=exc
            )

        try:
            self._glossary_items = self.client.get_glossary_items()
        except BigIDAPIError as exc:
            self.report.warning("glossary-items unavailable", context=str(exc), exc=exc)

        if self.config.sync_idsor:
            try:
                self._idsor_attr_map = self.client.get_idsor_attribute_map()
            except BigIDAPIError as exc:
                self.report.warning(
                    "idsor-attributes unavailable", context=str(exc), exc=exc
                )

        # No glossary items and no classification map means nothing can be enriched.
        if not self._glossary_items and not self._glossary_id_map:
            self.report.failure(
                "registries-empty",
                context="Both glossary items and classification map failed to load. "
                "No enrichment will be applied. Check BigID API connectivity.",
            )

    def _emit_root_glossary_node(self) -> Iterator[MetadataWorkUnit]:
        node_urn = BIGID_ROOT_GLOSSARY_NODE_URN
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
        node_urn = BIGID_IDSOR_GLOSSARY_NODE_URN
        yield MetadataChangeProposalWrapper(
            entityUrn=node_urn,
            aspect=GlossaryNodeInfoClass(
                name="IDSoR",
                definition=(
                    "Auto-generated GlossaryTerms from BigID Identity Source of Record (IDSoR) "
                    "correlation findings. IDSoR attributes are produced by BigID's correlation "
                    "engine when column values match rows in a configured Correlation Set."
                ),
                parentNode=BIGID_ROOT_GLOSSARY_NODE_URN,
            ),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=node_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()
        self.report.glossary_nodes_emitted += 1

    def _domain_urn(self, value: str) -> str:
        # GUID-based domain URN via DomainKey (urn:li:domain:<guid>) rather than a
        # human-readable slug. platform=bigid namespaces the hash so BigID domains
        # never collide with same-named domains from other sources. The human-
        # readable label is carried separately on DomainPropertiesClass.name.
        return DomainKey(name=value, platform=BIGID_PLATFORM_NAME).as_urn()

    def _emit_domain_entities(self) -> Iterator[MetadataWorkUnit]:
        seen_domains: set[str] = set()
        seen_sub_domains: dict[str, str] = {}  # sub_domain value → parent domain value

        for item in self._glossary_items:
            domain_val = item.domain
            sub_domain_val = item.sub_domain
            if domain_val:
                seen_domains.add(domain_val)
            if sub_domain_val and domain_val:
                seen_sub_domains[sub_domain_val] = domain_val

        for domain_val in seen_domains:
            domain_urn = self._domain_urn(domain_val)
            if domain_urn not in self._emitted_domain_urns:
                try:
                    props_wu = MetadataChangeProposalWrapper(
                        entityUrn=domain_urn,
                        aspect=DomainPropertiesClass(name=domain_val, description=""),
                    ).as_workunit()
                    status_wu = MetadataChangeProposalWrapper(
                        entityUrn=domain_urn,
                        aspect=StatusClass(removed=False),
                    ).as_workunit()
                except Exception as exc:
                    self.report.warning(
                        "domain-entity-failed",
                        context=f"domain={domain_val!r}: {exc}",
                        exc=exc,
                    )
                    continue
                yield props_wu
                yield status_wu
                self._emitted_domain_urns.add(domain_urn)

        for sub_domain_val, parent_domain_val in seen_sub_domains.items():
            sub_urn = self._domain_urn(sub_domain_val)
            parent_urn = self._domain_urn(parent_domain_val)
            if sub_urn not in self._emitted_domain_urns:
                try:
                    props_wu = MetadataChangeProposalWrapper(
                        entityUrn=sub_urn,
                        aspect=DomainPropertiesClass(
                            name=sub_domain_val,
                            description="",
                            parentDomain=parent_urn,
                        ),
                    ).as_workunit()
                    status_wu = MetadataChangeProposalWrapper(
                        entityUrn=sub_urn,
                        aspect=StatusClass(removed=False),
                    ).as_workunit()
                except Exception as exc:
                    self.report.warning(
                        "domain-entity-failed",
                        context=f"sub_domain={sub_domain_val!r}: {exc}",
                        exc=exc,
                    )
                    continue
                yield props_wu
                yield status_wu
                self._emitted_domain_urns.add(sub_urn)

    def _glossary_term_urn(self, item: BigIDGlossaryItem) -> str:
        if item.glossary_id:
            return f"urn:li:glossaryTerm:bigid.{item.glossary_id}"
        return f"urn:li:glossaryTerm:bigid.{_slugify(item.name or 'unknown')}"

    def _should_include_item(self, item: BigIDGlossaryItem) -> bool:
        # OOTB Personal Data Items are always included: column enrichment references their URNs.
        if item.is_ootb and item.item_type == "Personal Data Item":
            return True
        return item.item_type in self.config.item_types

    def _emit_glossary_terms(self) -> Iterator[MetadataWorkUnit]:
        root_node_urn = BIGID_ROOT_GLOSSARY_NODE_URN

        for item in self._glossary_items:
            if not self._should_include_item(item):
                continue
            try:
                yield from self._emit_single_glossary_term(item, root_node_urn)
            except Exception as exc:
                self.report.warning(
                    "glossary-term-failed",
                    context=f"item_id={item.bigid_id!r} name={item.name!r}: {exc}",
                    exc=exc,
                )

    def _emit_single_glossary_term(
        self, item: BigIDGlossaryItem, root_node_urn: str
    ) -> Iterator[MetadataWorkUnit]:
        term_urn = self._glossary_term_urn(item)
        owner_val = item.owner
        domain_val = item.domain
        sub_domain_val = item.sub_domain

        custom_props: dict[str, str] = {
            "bigid_type": item.item_type,
            "bigid_is_ootb": str(item.is_ootb).lower(),
            "bigid_glossary_id": item.glossary_id,
            "bigid_id": item.bigid_id,
            "bigid_update_date": item.update_date,
        }
        if owner_val:
            custom_props["bigid_owner"] = owner_val
        if domain_val:
            custom_props["bigid_domain"] = domain_val
        if sub_domain_val:
            custom_props["bigid_sub_domain"] = sub_domain_val

        term_info = GlossaryTermInfoClass(
            name=item.name,
            definition=item.description or "",
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

        if self.config.domain_mode in ("auto_namespaced", "config_map"):
            domain_urn = self._resolve_domain_urn(domain_val, sub_domain_val)
            if domain_urn:
                yield MetadataChangeProposalWrapper(
                    entityUrn=term_urn,
                    aspect=DomainsClass(domains=[domain_urn]),
                ).as_workunit()

        self.report.glossary_terms_emitted += 1

    def _resolve_domain_urn(
        self, domain_val: str, sub_domain_val: str
    ) -> Optional[str]:
        if self.config.domain_mode == "auto_namespaced":
            if sub_domain_val:
                return self._domain_urn(sub_domain_val)
            if domain_val:
                return self._domain_urn(domain_val)
        elif self.config.domain_mode == "config_map":
            key = sub_domain_val or domain_val
            return self.config.domain_mapping.get(key)
        return None

    def _process_catalog(self) -> Iterator[MetadataWorkUnit]:
        # businessTerm.* attributes resolve by name rather than glossary id.
        name_to_glossary_id: dict[str, str] = {
            item.name: item.glossary_id
            for item in self._glossary_items
            if item.name and item.glossary_id
        }

        # Buffer the whole catalog so unique tag entities can be emitted before the
        # datasets that reference them, in a single pass over the catalog API.
        # ponytail: single global in-memory buffer, O(n) memory in catalog size (~2 KB
        # per object, so ~200 MB–1 GB at 100k–500k objects). Scope with connection_pattern
        # if that bites; the upgrade path is a streaming two-pass over the catalog API.
        seen_tag_pairs: set[TagPair] = set()
        catalog_objects: list[BigIDCatalogObject] = []

        try:
            for obj in self.client.get_catalog_objects():
                if not self.config.connection_pattern.allowed(obj.source):
                    self.report.report_connection_filtered(obj.source)
                    continue
                catalog_objects.append(obj)
                if self.config.sync_tags:
                    seen_tag_pairs.update(self._object_tag_pairs(obj))
        except BigIDAPIError as exc:
            self.report.warning(
                "catalog-objects-partial",
                context=f"Fetch interrupted after {len(catalog_objects)} objects: {exc}",
                exc=exc,
            )

        # Emit Tag entities before any dataset enrichment so they exist first.
        for tag_pair in seen_tag_pairs:
            if (
                tag_pair.application_type == APP_TYPE_RISK
                and RISK_SCORE_TAG_NAME in tag_pair.tag_name
            ):
                continue  # riskScore is a StructuredProperty, not a Tag entity
            yield from self._emit_tag_entity(tag_pair.tag_name, tag_pair.tag_value)

        for obj in catalog_objects:
            try:
                yield from self._process_catalog_object(obj, name_to_glossary_id)
            except Exception as exc:
                self.report.warning(
                    "catalog-object-failed",
                    context=f"fqn={obj.fully_qualified_name!r}: {exc}",
                    exc=exc,
                )

    def _object_tag_pairs(self, obj: BigIDCatalogObject) -> Iterator[TagPair]:
        # Only OBJECT-scoped tags become dataset-level Tag entities. FIELD-type tags are
        # not referenced by any emitted aspect (column enrichment uses attributeDetails),
        # so emitting them would create orphaned TagProperties entities.
        for tag in obj.tags:
            if tag.properties.hidden:
                continue
            if tag.tag_type != TAG_TYPE_OBJECT:
                continue
            application_type = tag.properties.application_type
            if application_type not in self.config.tag_application_types:
                continue
            if tag.tag_name and tag.tag_value:
                yield TagPair(
                    tag_name=tag.tag_name,
                    tag_value=tag.tag_value,
                    application_type=application_type,
                )

    @staticmethod
    def _tag_display_name(tag_name: str, tag_value: str) -> str:
        # Strip the BigID-internal `system.*` prefix so only the meaningful segment shows
        # in the UI pill (e.g. system.risk.riskGroup + high -> "riskGroup : high"). Names
        # without the prefix are kept verbatim. The URN is unaffected.
        segment = (
            tag_name.rsplit(".", 1)[-1] if tag_name.startswith("system.") else tag_name
        )
        return f"{segment} : {tag_value}"

    def _emit_tag_entity(
        self, tag_name: str, tag_value: str
    ) -> Iterator[MetadataWorkUnit]:
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

    def _emit_risk_score_structured_property(
        self,
    ) -> Iterator[MetadataWorkUnit]:
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

    def _process_catalog_object(
        self,
        obj: BigIDCatalogObject,
        name_to_glossary_id: dict[str, str],
    ) -> Iterator[MetadataWorkUnit]:
        fqn = obj.fully_qualified_name
        source_name = obj.source  # BigID connection name (first FQN segment)
        object_name = obj.object_name
        is_structured = obj.scanner_type_group == SCANNER_TYPE_STRUCTURED

        dataset_urn = self._make_dataset_urn(fqn, source_name)
        if not dataset_urn:
            return

        now_ms = int(time.time() * 1000)
        scan_date_str = obj.scan_date or obj.last_scanned or ""
        scan_ts_ms = _parse_iso_to_ms(scan_date_str) or now_ms

        object_tags = [
            tag
            for tag in obj.tags
            if not tag.properties.hidden
            and tag.tag_type == TAG_TYPE_OBJECT
            and tag.properties.application_type in self.config.tag_application_types
        ]

        # Columns are fetched once and reused by both creation and enrichment.
        columns: list[BigIDColumn] = []
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
                        exc=exc,
                    )

        if self.config.create_datasets:
            yield from self._maybe_emit_dataset_creation(
                obj, dataset_urn, fqn, object_name, is_structured, columns
            )

        if self.config.sync_tags and object_tags:
            yield from self._emit_dataset_tags(dataset_urn, object_tags)

        self.report.datasets_enriched += 1

        yield from self._emit_platform_instance(dataset_urn, source_name)

        if is_structured:
            if columns:
                yield from self._emit_schema_field_enrichment(
                    dataset_urn, columns, source_name, name_to_glossary_id, now_ms
                )
                yield from self._emit_dataset_profile(dataset_urn, columns, scan_ts_ms)
                self.report.columns_enriched += len(columns)
        else:
            attr_details = obj.attribute_details
            if self.config.sync_unstructured_enrichment and attr_details:
                yield from self._emit_unstructured_dataset_enrichment(
                    obj,
                    dataset_urn,
                    source_name,
                    name_to_glossary_id,
                    now_ms,
                    scan_ts_ms,
                )
            else:
                self.report.datasets_skipped_unstructured += 1

    def _emit_unstructured_dataset_enrichment(
        self,
        obj: BigIDCatalogObject,
        dataset_urn: str,
        source_name: str,
        name_to_glossary_id: dict[str, str],
        now_ms: int,
        scan_ts_ms: int,
    ) -> Iterator[MetadataWorkUnit]:
        # Unstructured content has no column schema, so each attribute maps to a
        # dataset-level GlossaryTerm read straight from the catalog response's
        # attribute_details rather than from the columns API.
        attr_details = obj.attribute_details

        # Finding counts so value-type classifiers carry a row_count in their
        # MetadataAttribution, matching the structured path.
        clf_stats: dict[str, ClassificationStats] = {}
        for entry in attr_details:
            if entry.name and entry.count is not None:
                bare_name = (
                    entry.name.replace(CLASSIFIER_PREFIX, "", 1)
                    if entry.name.startswith(CLASSIFIER_PREFIX)
                    else entry.name
                )
                clf_stats[bare_name] = ClassificationStats(row_count=str(entry.count))

        seen_term_urns: set[str] = set()
        term_assocs: list[GlossaryTermAssociationClass] = []
        pending_new_terms: list[PendingTerm] = []

        for attr in attr_details:
            enrichment = self._resolve_attr_enrichment(
                attr, clf_stats, source_name, now_ms, name_to_glossary_id
            )
            if enrichment is None:
                continue
            # Confidence-level tags are per-field context; skip them for unstructured datasets.
            if enrichment.term_assoc.urn not in seen_term_urns:
                seen_term_urns.add(enrichment.term_assoc.urn)
                term_assocs.append(enrichment.term_assoc)
            if enrichment.pending_term is not None:
                pending_new_terms.append(enrichment.pending_term)

        # Emit new term entities before the GlossaryTerms MCP so referenced entities exist.
        yield from self._emit_pending_terms(pending_new_terms)

        if term_assocs:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=GlossaryTermsClass(
                    terms=term_assocs,
                    auditStamp=AuditStampClass(time=now_ms, actor=DATAHUB_ACTOR_URN),
                ),
            ).as_workunit()

        # DatasetProfile: total_pii_count as rowCount, sizeInBytes when present.
        # columnCount=0 signals "no schema" in the DataHub UI.
        row_count = _coerce_int(obj.total_pii_count)
        size_val = _coerce_int(obj.size_in_bytes)

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
        object_tags: list[BigIDTag],
        dataset_urn: str,
    ) -> DatasetTagExtract:
        extract = DatasetTagExtract()
        for tag in object_tags:
            if not tag.tag_name or not tag.tag_value:
                continue
            if (
                tag.properties.application_type == APP_TYPE_RISK
                and RISK_SCORE_TAG_NAME in tag.tag_name
            ):
                try:
                    extract.risk_score = float(tag.tag_value)
                except ValueError:
                    self.report.warning(
                        "invalid-risk-score",
                        context=f"dataset={dataset_urn!r} value={tag.tag_value!r} — skipping structured property",
                    )
                continue
            tag_urn = f"urn:li:tag:bigid.{_encode_urn_name(tag.tag_name)}:{_encode_urn_name(tag.tag_value)}"
            extract.tag_assocs.append(TagAssociationClass(tag=tag_urn))
        return extract

    def _emit_dataset_tags(
        self, dataset_urn: str, object_tags: list[BigIDTag]
    ) -> Iterator[MetadataWorkUnit]:
        extract = self._extract_dataset_tag_associations(object_tags, dataset_urn)
        if extract.tag_assocs:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=GlobalTagsClass(tags=extract.tag_assocs),
            ).as_workunit()
        if extract.risk_score is not None:
            for mcp in (
                DatasetPatchBuilder(dataset_urn)
                .set_structured_property(
                    self.config.risk_score_structured_property_urn,
                    extract.risk_score,
                )
                .build()
            ):
                yield MetadataWorkUnit(id=f"{dataset_urn}-riskScore-patch", mcp_raw=mcp)

    def _emit_platform_instance(
        self, dataset_urn: str, source_name: str
    ) -> Iterator[MetadataWorkUnit]:
        # Only emit when a platform_instance is configured: emitting instance=None in
        # enrichment mode would UPSERT-overwrite the value a native connector already set.
        platform_instance = self._get_platform_instance(source_name)
        if not platform_instance:
            return
        platform = self._get_platform(source_name)
        platform_urn = builder.make_data_platform_urn(platform)
        instance_urn = builder.make_dataplatform_instance_urn(
            platform, platform_instance
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DataPlatformInstanceClass(
                platform=platform_urn,
                instance=instance_urn,
            ),
        ).as_workunit()

    def _maybe_emit_dataset_creation(
        self,
        obj: BigIDCatalogObject,
        dataset_urn: str,
        fqn: str,
        object_name: str,
        is_structured: bool,
        columns: list[BigIDColumn],
    ) -> Iterator[MetadataWorkUnit]:
        # Only create on first encounter of the URN this run.
        if dataset_urn in self._seen_dataset_urns:
            return

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(
                name=object_name,
                qualifiedName=fqn,
                customProperties={"bigid_source": "true"},
            ),
        ).as_workunit()

        if is_structured and columns:
            schema_fields = []
            for column in sorted(columns, key=lambda col: col.order):
                schema_fields.append(
                    SchemaFieldClass(
                        fieldPath=column.column_name,
                        type=_map_field_type(column.field_type),
                        nativeDataType=column.field_type,
                        nullable=column.nullable,
                        isPartOfKey=column.is_primary,
                    )
                )
            platform = self._get_platform(obj.source)
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SchemaMetadataClass(
                    schemaName=fqn,
                    platform=builder.make_data_platform_urn(platform),
                    version=0,
                    hash="",
                    platformSchema=OtherSchemaClass(rawSchema=""),
                    fields=schema_fields,
                ),
            ).as_workunit()

        self._seen_dataset_urns.add(dataset_urn)
        self.report.datasets_created += 1

    def _emit_schema_field_enrichment(
        self,
        dataset_urn: str,
        columns: list[BigIDColumn],
        source_name: str,
        name_to_glossary_id: dict[str, str],
        now_ms: int,
    ) -> Iterator[MetadataWorkUnit]:
        # Keyed by fieldPath so columns from multiple BigID connections merge: GMS rejects
        # editableSchemaMetadata with duplicate fieldPaths.
        editable_fields: dict[str, FieldEnrichment] = {}
        # Collected during the loop and emitted afterwards so entity emission is not a
        # mid-iteration side effect.
        pending_new_terms: list[PendingTerm] = []

        for column in columns:
            field_path = column.column_name
            if not field_path:
                continue

            clf_stats = self._build_clf_stats(column.field_classifications)

            for attr in column.attribute_details:
                enrichment_result = self._resolve_attr_enrichment(
                    attr, clf_stats, source_name, now_ms, name_to_glossary_id
                )
                if enrichment_result is None:
                    continue

                field = editable_fields.setdefault(field_path, FieldEnrichment())
                if enrichment_result.term_assoc.urn not in field.seen_urns:
                    field.seen_urns.add(enrichment_result.term_assoc.urn)
                    field.terms.append(enrichment_result.term_assoc)
                field.add_tag_urns(enrichment_result.confidence_tag_urns)

                if enrichment_result.pending_term is not None:
                    pending_new_terms.append(enrichment_result.pending_term)

        # Emit new term entities before the schema enrichment MCP so GlossaryTerm entities
        # always exist before they are referenced.
        yield from self._emit_pending_terms(pending_new_terms)

        if not editable_fields:
            return

        field_info_list: list[EditableSchemaFieldInfoClass] = []
        for field_path, field in editable_fields.items():
            field_glossary_terms = GlossaryTermsClass(
                terms=field.terms,
                auditStamp=AuditStampClass(time=now_ms, actor=DATAHUB_ACTOR_URN),
            )
            field_tags = (
                GlobalTagsClass(
                    tags=[
                        TagAssociationClass(tag=tag_urn) for tag_urn in field.tag_urns
                    ]
                )
                if field.tag_urns
                else None
            )
            field_info_list.append(
                EditableSchemaFieldInfoClass(
                    fieldPath=field_path,
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
        field_classifications: list[BigIDFieldClassification],
    ) -> dict[str, ClassificationStats]:
        clf_stats: dict[str, ClassificationStats] = {}
        for classification in field_classifications:
            if classification.classification_name:
                clf_stats[classification.classification_name] = ClassificationStats(
                    row_count=str(classification.rows_or_fields_counter),
                    distinct_count=str(classification.distinct_value_count),
                )
        return clf_stats

    @staticmethod
    def _classifier_type(attr_name: str) -> str:
        # MD:: must be tested before the bare classifier prefix (longer match on the same
        # string): this ordering is a correctness invariant.
        if attr_name.startswith(CLASSIFIER_MD_PREFIX):
            return CLF_TYPE_METADATA
        if attr_name.startswith(CLASSIFIER_PREFIX):
            return CLF_TYPE_VALUE
        return CLF_TYPE_BUSINESS_TERM

    def _resolve_idsor_term(
        self, attr: BigIDAttributeDetail, attr_name: str
    ) -> Optional[IDSoRResolution]:
        # Three resolution paths; each returns None when the attribute name produces an
        # empty slug. pending_term is set only when the term entity is new this run.
        info = self._idsor_attr_map.get(attr_name)
        if info and info.glossary_id:
            # Path 1: linked to an existing BigID glossary item — reuse its URN.
            term_urn = f"urn:li:glossaryTerm:bigid.{info.glossary_id}"
            return IDSoRResolution(
                term_urn=term_urn,
                resolved_friendly=info.friendly_name,
                pending_term=self._pending_term(
                    attr_name, term_urn, self._emitted_idsor_linked_term_urns
                ),
            )
        if info:
            # Path 2: in map but no glossaryId — auto-generate under bigid.idsor.
            slug = _slugify(info.friendly_name) or _slugify(attr_name)
            if not slug:
                logger.debug(
                    "IDSoR path 2: empty slug for attr %r — skipping", attr_name
                )
                return None
            term_urn = f"urn:li:glossaryTerm:bigid.idsor.{slug}"
            return IDSoRResolution(
                term_urn=term_urn,
                resolved_friendly=info.friendly_name,
                pending_term=self._pending_term(
                    attr_name, term_urn, self._emitted_idsor_term_urns
                ),
            )
        # Path 3: not in map — auto-generate from the raw attribute name.
        slug = _slugify(attr_name)
        if not slug:
            logger.debug("IDSoR path 3: empty slug for attr %r — skipping", attr_name)
            return None
        term_urn = f"urn:li:glossaryTerm:bigid.idsor.{slug}"
        return IDSoRResolution(
            term_urn=term_urn,
            resolved_friendly=attr_name.replace("_", " ").title(),
            pending_term=self._pending_term(
                attr_name, term_urn, self._emitted_idsor_term_urns
            ),
        )

    @staticmethod
    def _pending_term(
        attr_name: str, term_urn: str, already_emitted: set[str]
    ) -> Optional[PendingTerm]:
        if term_urn in already_emitted:
            return None
        return PendingTerm(attr_name=attr_name, term_urn=term_urn)

    def _resolve_term_urn_and_pending(
        self,
        attr: BigIDAttributeDetail,
        attr_name: str,
        name_to_glossary_id: dict[str, str],
    ) -> Optional[TermResolution]:
        # Returns None when the attribute should be skipped (no mapping, disabled, or an
        # empty slug).
        glossary_id = self._resolve_attr_to_glossary_id(attr_name, name_to_glossary_id)

        if glossary_id:
            return TermResolution(
                term_urn=f"urn:li:glossaryTerm:bigid.{glossary_id}",
                resolved_friendly=self._friendly_name_map.get(glossary_id),
                pending_term=None,
                clf_type=self._classifier_type(attr_name),
            )

        if self.config.sync_unlinked_classifiers and attr_name.startswith(
            CLASSIFIER_PREFIX
        ):
            term_urn = self._classifier_term_urn(attr_name)
            if term_urn is None:
                return None
            return TermResolution(
                term_urn=term_urn,
                resolved_friendly=self._classifier_friendly_names.get(attr_name),
                pending_term=self._pending_term(
                    attr_name, term_urn, self._emitted_classifier_term_urns
                ),
                clf_type=self._classifier_type(attr_name),
            )

        if self.config.sync_idsor and _is_idsor_attr(attr):
            idsor = self._resolve_idsor_term(attr, attr_name)
            if idsor is None:
                return None
            return TermResolution(
                term_urn=idsor.term_urn,
                resolved_friendly=idsor.resolved_friendly,
                pending_term=idsor.pending_term,
                clf_type=CLF_TYPE_IDSOR,
            )

        return None

    def _resolve_attr_enrichment(
        self,
        attr: BigIDAttributeDetail,
        clf_stats: dict[str, ClassificationStats],
        source_name: str,
        now_ms: int,
        name_to_glossary_id: dict[str, str],
    ) -> Optional[AttrEnrichment]:
        attr_name = attr.name
        rank = str(attr.ranks[0]) if attr.ranks else "LOW"

        confidence_float = _rank_to_float(rank)
        if confidence_float < self.config.minimum_confidence_threshold:
            self.report.findings_below_threshold += 1
            return None

        resolved = self._resolve_term_urn_and_pending(
            attr, attr_name, name_to_glossary_id
        )
        if resolved is None:
            return None

        # sourceDetail is the MetadataAttribution string map, so it stays a flat dict.
        source_detail: dict[str, str] = {
            "classifier_name": attr_name,
            "classifier_type": resolved.clf_type,
            "confidence_level": rank.upper(),
            "bigid_connection": source_name,
        }

        if resolved.clf_type == CLF_TYPE_VALUE:
            bare_name = attr_name.replace(CLASSIFIER_PREFIX, "", 1)
            stats = clf_stats.get(bare_name) or clf_stats.get(attr_name)
            if stats and stats.row_count:
                source_detail["row_count"] = stats.row_count
            if stats and stats.distinct_count:
                source_detail["distinct_count"] = stats.distinct_count
        elif resolved.clf_type == CLF_TYPE_IDSOR:
            if attr.count is not None:
                source_detail["row_count"] = str(attr.count)

        if rank.upper() == "HIGH" and resolved.resolved_friendly:
            source_detail["classifier_friendly_name"] = resolved.resolved_friendly

        term_assoc = GlossaryTermAssociationClass(
            urn=resolved.term_urn,
            attribution=MetadataAttributionClass(
                time=now_ms,
                actor=DATAHUB_ACTOR_URN,
                source=BIGID_DATA_PLATFORM_URN,
                sourceDetail=source_detail,
            ),
        )

        confidence_tag_urns: list[str] = []
        if self.config.confidence_level_tag:
            confidence_tag_urns.append(f"urn:li:tag:bigid.confidence:{rank.upper()}")

        return AttrEnrichment(
            term_assoc=term_assoc,
            confidence_tag_urns=confidence_tag_urns,
            pending_term=resolved.pending_term,
        )

    def _resolve_attr_to_glossary_id(
        self, attr_name: str, name_to_glossary_id: dict[str, str]
    ) -> Optional[str]:
        # classifier.* and classifier.MD::* both look up in the all-classifications map.
        if attr_name.startswith(CLASSIFIER_PREFIX):
            return self._glossary_id_map.get(attr_name)
        if attr_name.startswith(BUSINESS_TERM_PREFIX):
            bare_name = attr_name[len(BUSINESS_TERM_PREFIX) :]
            return name_to_glossary_id.get(bare_name)
        return None

    @staticmethod
    def _strip_classifier_prefix(attr_name: str) -> str:
        bare_name = attr_name[len(CLASSIFIER_PREFIX) :]
        if bare_name.startswith("MD::"):
            bare_name = bare_name[len("MD::") :]
        return bare_name

    def _classifier_term_urn(self, attr_name: str) -> Optional[str]:
        # Returns None when the classifier name produces an empty slug (malformed input).
        slug = _slugify(self._strip_classifier_prefix(attr_name))
        if not slug:
            return None
        return f"urn:li:glossaryTerm:bigid.classifier.{slug}"

    def _emit_pending_terms(
        self, pending_new_terms: list[PendingTerm]
    ) -> Iterator[MetadataWorkUnit]:
        # Deduplicate by term_urn within the call and route to the correct emit method
        # based on the URN namespace, so callers don't need their own seen-set.
        seen_pending: set[str] = set()
        for pending in pending_new_terms:
            if pending.term_urn in seen_pending:
                continue
            seen_pending.add(pending.term_urn)
            if ":bigid.idsor." in pending.term_urn:
                yield from self._emit_idsor_term(pending.attr_name, pending.term_urn)
            elif ":bigid.classifier." in pending.term_urn:
                yield from self._emit_classifier_term(
                    pending.attr_name, pending.term_urn
                )
            else:
                # IDSoR path 1 — linked term (fn_item_*, bt_*, etc.)
                yield from self._emit_idsor_linked_term(
                    pending.attr_name, pending.term_urn
                )

    def _try_build_term_mcps(
        self,
        term_urn: str,
        term_info: GlossaryTermInfoClass,
        warning_key: str,
        attr_name: str,
    ) -> Optional[list[MetadataWorkUnit]]:
        try:
            return [
                MetadataChangeProposalWrapper(
                    entityUrn=term_urn, aspect=term_info
                ).as_workunit(),
                MetadataChangeProposalWrapper(
                    entityUrn=term_urn, aspect=StatusClass(removed=False)
                ).as_workunit(),
            ]
        except Exception as exc:
            self.report.warning(
                warning_key,
                context=f"attr_name={attr_name!r} term_urn={term_urn!r} error={exc}",
                exc=exc,
            )
            return None

    def _emit_classifier_term(
        self, attr_name: str, term_urn: str
    ) -> Iterator[MetadataWorkUnit]:
        friendly = self._classifier_friendly_names.get(attr_name, "")
        bare_name = self._strip_classifier_prefix(attr_name)
        display_name = friendly if friendly else bare_name.replace("_", " ").title()
        term_info = GlossaryTermInfoClass(
            name=display_name,
            definition=f"Auto-generated from BigID classifier: {attr_name}",
            termSource="EXTERNAL",
            parentNode=BIGID_ROOT_GLOSSARY_NODE_URN,
            customProperties={
                "bigid_type": "classifier",
                "bigid_classifier_name": attr_name,
            },
        )
        mcps = self._try_build_term_mcps(
            term_urn, term_info, "classifier-term-build-failed", attr_name
        )
        if mcps is None:
            return
        yield from mcps
        self.report.classifier_terms_emitted += 1
        self._emitted_classifier_term_urns.add(term_urn)

    def _emit_idsor_linked_term(
        self, attr_name: str, term_urn: str
    ) -> Iterator[MetadataWorkUnit]:
        # Path 1 terms are referenced by their BigID glossaryId but typically do not appear
        # in the business_glossary_items response, so the glossary sync step never emits
        # their GlossaryTermInfo. Emitting it here gives the term a human-readable name in
        # the UI instead of the raw fn_item_* id.
        info = self._idsor_attr_map.get(attr_name)
        display_name = (
            info.friendly_name if info else attr_name.replace("_", " ").title()
        )
        custom_props: dict[str, str] = {
            "bigid_type": "idsor_attribute",
            "bigid_attribute_name": attr_name,
        }
        if info and info.glossary_id:
            custom_props["bigid_glossary_id"] = info.glossary_id
        term_info = GlossaryTermInfoClass(
            name=display_name,
            definition=f"Linked from BigID IDSoR attribute: {attr_name}",
            termSource="EXTERNAL",
            parentNode=BIGID_IDSOR_GLOSSARY_NODE_URN,
            customProperties=custom_props,
        )
        mcps = self._try_build_term_mcps(
            term_urn, term_info, "idsor-linked-term-build-failed", attr_name
        )
        if mcps is None:
            return
        yield from mcps
        self.report.idsor_terms_emitted += 1
        self._emitted_idsor_linked_term_urns.add(term_urn)

    def _emit_idsor_term(
        self, attr_name: str, term_urn: str
    ) -> Iterator[MetadataWorkUnit]:
        info = self._idsor_attr_map.get(attr_name)
        display_name = (
            info.friendly_name if info else attr_name.replace("_", " ").title()
        )
        term_info = GlossaryTermInfoClass(
            name=display_name,
            definition=f"Auto-generated from BigID IDSoR attribute: {attr_name}",
            termSource="EXTERNAL",
            parentNode=BIGID_IDSOR_GLOSSARY_NODE_URN,
            customProperties={
                "bigid_type": "idsor_attribute",
                "bigid_attribute_name": attr_name,
            },
        )
        mcps = self._try_build_term_mcps(
            term_urn, term_info, "idsor-term-build-failed", attr_name
        )
        if mcps is None:
            return
        yield from mcps
        self.report.idsor_terms_emitted += 1
        self._emitted_idsor_term_urns.add(term_urn)

    def _emit_dataset_profile(
        self,
        dataset_urn: str,
        columns: list[BigIDColumn],
        timestamp_ms: int,
    ) -> Iterator[MetadataWorkUnit]:
        if not columns:
            return

        field_profiles: list[DatasetFieldProfileClass] = []
        for column in columns:
            field_profile = _build_field_profile(column)
            if field_profile is not None:
                field_profiles.append(field_profile)

        # rowCount is intentionally omitted: BigID's fieldCount is a per-column scan sample
        # size, not a table row count, so deriving rowCount from it would be inaccurate.
        profile = DatasetProfileClass(
            timestampMillis=timestamp_ms,
            columnCount=len(columns),
            fieldProfiles=field_profiles if field_profiles else None,
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=profile,
        ).as_workunit()

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
            override = self.config.datasource_platform_mapping[
                source_name
            ].platform_instance
            if override is not None:
                return override
        # Global default from PlatformInstanceConfigMixin.
        return self.config.platform_instance

    def _make_dataset_urn(self, fqn: str, source_name: str) -> Optional[str]:
        # BigID FQN is {connection_name}.{remaining}; the DataHub URN drops the connection
        # segment and becomes urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env}).
        if not fqn or not source_name:
            return None

        # Strip by prefix rather than split: the connection name may itself contain dots.
        prefix = source_name + "."
        if fqn.startswith(prefix):
            remaining = fqn[len(prefix) :]
        else:
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

        safe_name = _encode_urn_name(dataset_name)
        return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{safe_name},{env})"
