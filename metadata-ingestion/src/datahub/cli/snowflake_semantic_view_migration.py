"""Bidirectional governance migration for Snowflake Semantic Views.

Copies entity-level governance and DataHub-only column tags/terms between
legacy dataset "Semantic View" URNs and semanticModel/metric (+ schemaField)
URNs. The source must exist; the destination may not yet (aspects are written
to the mapped URNs so ingest can fill structural aspects afterward). Typical
order: migrate governance, then Snowflake ingest with emit_semantic_model_entities.

Lineage, policies, data products, and soft-delete are out of scope.

URN helpers mirror ``SnowflakeIdentifierBuilder`` but use ``datahub.metadata.urns``
so the migrate CLI does not require the snowflake connector extra.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Set, Tuple

from datahub.cli.migration_utils import INBOUND_REFERENCE_RELATIONSHIP_TYPES
from datahub.emitter.aspect import ASPECT_MAP
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import RemovedStatusFilter, SearchFilterRule
from datahub.ingestion.graph.openapi import RelatedEntity, RelationshipDirection
from datahub.metadata.schema_classes import (
    DocumentationAssociationClass,
    DocumentationClass,
    EditableDatasetPropertiesClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    SemanticModelInfoClass,
    StatusClass,
    SubTypesClass,
    _Aspect,
)
from datahub.metadata.urns import DatasetUrn, MetricUrn, SemanticModelUrn
from datahub.utilities.str_enum import StrEnum
from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path

log = logging.getLogger(__name__)

SNOWFLAKE_PLATFORM = "snowflake"
SEMANTIC_VIEW_SUBTYPE = "Semantic View"

# Connector-synthesized column classification tags on legacy schemaMetadata.
# Not customer tags — remodeled as SemanticField.type / metric entities.
_SYNTHETIC_SUBTYPE_TAG_URNS: Set[str] = {
    make_tag_urn("DIMENSION"),
    make_tag_urn("FACT"),
    make_tag_urn("METRIC"),
    make_tag_urn("dimension"),
    make_tag_urn("fact"),
    make_tag_urn("metric"),
}

GOVERNANCE_ASPECTS: List[str] = [
    "ownership",
    "domains",
    "globalTags",
    "glossaryTerms",
    "institutionalMemory",
    "structuredProperties",
    "documentation",
    "deprecation",
    "applications",
]

# Not copied wholesale. schemaMetadata / editableSchemaMetadata are read only
# to extract column tags/terms for field fan-out (see migrate_field_governance).
SKIPPED_ASPECTS: List[str] = [
    "status",
    "subTypes",
    "browsePathsV2",
    "dataPlatformInstance",
    "upstreamLineage",
    "datasetUsageStatistics",
    "schemaMetadata",
    "datasetProperties",
    "viewProperties",
    "container",
    "editableSchemaMetadata",
    "siblings",
    "incidentsSummary",
    "testResults",
    "forms",
]


class MigrationDirection(StrEnum):
    DATASET_TO_SM = "dataset-to-sm"
    SM_TO_DATASET = "sm-to-dataset"


# --- URN identity: the (db, schema, view) triple shared by a dataset,
# semanticModel, and its metrics ---


@dataclass(frozen=True)
class SnowflakeViewIdentity:
    db: str
    schema: str
    view: str


def snowflake_identifier(identifier: str, convert_urns_to_lowercase: bool) -> str:
    """Match ``SnowflakeIdentifierBuilder.snowflake_identifier``: lowercase iff configured."""
    return identifier.lower() if convert_urns_to_lowercase else identifier


def _semantic_path(
    identity: SnowflakeViewIdentity,
    platform_instance: Optional[str],
    convert_urns_to_lowercase: bool,
) -> str:
    path = snowflake_identifier(
        f"{identity.db}.{identity.schema}", convert_urns_to_lowercase
    )
    return f"{platform_instance}.{path}" if platform_instance else path


def gen_semantic_model_urn(
    identity: SnowflakeViewIdentity,
    platform_instance: Optional[str],
    convert_urns_to_lowercase: bool,
) -> str:
    return str(
        SemanticModelUrn(
            platform=make_data_platform_urn(SNOWFLAKE_PLATFORM),
            path=_semantic_path(identity, platform_instance, convert_urns_to_lowercase),
            id=snowflake_identifier(identity.view, convert_urns_to_lowercase),
        )
    )


def gen_metric_urn(
    identity: SnowflakeViewIdentity,
    metric_name: str,
    platform_instance: Optional[str],
    convert_urns_to_lowercase: bool,
) -> str:
    view_id = snowflake_identifier(identity.view, convert_urns_to_lowercase)
    path = f"{_semantic_path(identity, platform_instance, convert_urns_to_lowercase)}.{view_id}"
    return str(
        MetricUrn(
            platform=make_data_platform_urn(SNOWFLAKE_PLATFORM),
            path=path,
            id=snowflake_identifier(metric_name, convert_urns_to_lowercase),
        )
    )


def gen_dataset_urn(
    identity: SnowflakeViewIdentity,
    platform_instance: Optional[str],
    env: str,
    convert_urns_to_lowercase: bool,
) -> str:
    name = snowflake_identifier(
        f"{identity.db}.{identity.schema}.{identity.view}", convert_urns_to_lowercase
    )
    return make_dataset_urn_with_platform_instance(
        platform=SNOWFLAKE_PLATFORM,
        name=name,
        platform_instance=platform_instance,
        env=env,
    )


def _strip_instance_prefix(value: str, platform_instance: Optional[str]) -> str:
    if not platform_instance:
        return value
    prefix = f"{platform_instance}."
    if not value.startswith(prefix):
        raise ValueError(
            f"'{value}' does not start with expected platform instance prefix '{prefix}'"
        )
    return value[len(prefix) :]


def parse_dataset_identity(
    dataset_urn: str, platform_instance: Optional[str]
) -> SnowflakeViewIdentity:
    """Recover the (db, schema, view) identity encoded in a Snowflake dataset urn's name."""
    urn = DatasetUrn.from_string(dataset_urn)
    name = _strip_instance_prefix(urn.name, platform_instance)
    parts = name.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"Dataset name '{name}' (from {dataset_urn}) does not resolve to exactly "
            "3 db.schema.view parts"
        )
    return SnowflakeViewIdentity(db=parts[0], schema=parts[1], view=parts[2])


def parse_semantic_model_identity(
    semantic_model_urn: str, platform_instance: Optional[str]
) -> SnowflakeViewIdentity:
    """Recover the (db, schema, view) identity encoded in a semanticModel urn's path/id."""
    urn = SemanticModelUrn.from_string(semantic_model_urn)
    path = _strip_instance_prefix(urn.path, platform_instance)
    parts = path.split(".")
    if len(parts) != 2:
        raise ValueError(
            f"SemanticModel path '{path}' (from {semantic_model_urn}) does not resolve "
            "to exactly 2 db.schema parts"
        )
    return SnowflakeViewIdentity(db=parts[0], schema=parts[1], view=str(urn.id))


def dataset_urn_to_semantic_model_urn(
    dataset_urn: str, platform_instance: Optional[str], convert_urns_to_lowercase: bool
) -> str:
    identity = parse_dataset_identity(dataset_urn, platform_instance)
    return gen_semantic_model_urn(
        identity, platform_instance, convert_urns_to_lowercase
    )


def semantic_model_urn_to_dataset_urn(
    semantic_model_urn: str,
    platform_instance: Optional[str],
    env: str,
    convert_urns_to_lowercase: bool,
) -> str:
    identity = parse_semantic_model_identity(semantic_model_urn, platform_instance)
    return gen_dataset_urn(identity, platform_instance, env, convert_urns_to_lowercase)


# --- Per-entity migration ---


@dataclass
class EntityMigrationResult:
    src_urn: str
    dst_urn: str
    aspects_copied: List[str] = field(default_factory=list)
    fields_migrated: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    inbound_refs: List[RelatedEntity] = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class FieldGovernance:
    column_name: str
    is_metric: bool
    global_tags: Optional[GlobalTagsClass] = None
    glossary_terms: Optional[GlossaryTermsClass] = None


def _collect_governance_aspects(
    graph: DataHubGraph, src_urn: str
) -> Dict[str, _Aspect]:
    aspect_types = [ASPECT_MAP[name] for name in GOVERNANCE_ASPECTS]
    result = graph.get_aspects_for_entity(
        entity_urn=src_urn, aspects=GOVERNANCE_ASPECTS, aspect_types=aspect_types
    )
    return {name: aspect for name, aspect in result.items() if aspect is not None}


def _simple_column_name(field_path: str) -> str:
    try:
        simple = get_simple_field_path_from_v2_field_path(field_path)
    except Exception:
        simple = field_path
    return simple.split(".")[-1]


def _strip_synthetic_subtype_tags(
    tags: Optional[GlobalTagsClass],
) -> Optional[GlobalTagsClass]:
    if tags is None or not tags.tags:
        return None
    kept = [t for t in tags.tags if t.tag not in _SYNTHETIC_SUBTYPE_TAG_URNS]
    if not kept:
        return None
    return GlobalTagsClass(tags=kept)


def _tags_indicate_metric(tags: Optional[GlobalTagsClass]) -> bool:
    if tags is None or not tags.tags:
        return False
    return any(
        tag.tag in (make_tag_urn("METRIC"), make_tag_urn("metric")) for tag in tags.tags
    )


def _schema_field_is_metric(schema_field: SchemaFieldClass) -> bool:
    if _tags_indicate_metric(schema_field.globalTags):
        return True
    if schema_field.jsonProps:
        try:
            props = json.loads(schema_field.jsonProps)
        except (TypeError, ValueError):
            return False
        subtype = props.get("columnSubType")
        if isinstance(subtype, str) and "METRIC" in {
            s.strip().upper() for s in subtype.split(",")
        }:
            return True
    return False


def _union_global_tags(
    left: Optional[GlobalTagsClass], right: Optional[GlobalTagsClass]
) -> Optional[GlobalTagsClass]:
    if left is None:
        return right
    if right is None:
        return left
    seen: Set[str] = set()
    tags = []
    for tag in list(left.tags or []) + list(right.tags or []):
        if tag.tag in seen:
            continue
        seen.add(tag.tag)
        tags.append(tag)
    return GlobalTagsClass(tags=tags) if tags else None


def _union_glossary_terms(
    left: Optional[GlossaryTermsClass], right: Optional[GlossaryTermsClass]
) -> Optional[GlossaryTermsClass]:
    if left is None:
        return right
    if right is None:
        return left
    seen: Set[str] = set()
    terms: List[GlossaryTermAssociationClass] = []
    for term in list(left.terms or []) + list(right.terms or []):
        if term.urn in seen:
            continue
        seen.add(term.urn)
        terms.append(term)
    if not terms:
        return None
    return GlossaryTermsClass(
        terms=terms, auditStamp=right.auditStamp or left.auditStamp
    )


def _merge_field_governance(
    by_column: Dict[str, FieldGovernance],
    column_name: str,
    is_metric: bool,
    tags: Optional[GlobalTagsClass],
    terms: Optional[GlossaryTermsClass],
) -> None:
    customer_tags = _strip_synthetic_subtype_tags(tags)
    # Always record METRIC classification even when the only tags were synthetic
    # subtype tags (stripped below) so a later editableSchemaMetadata merge can
    # still fan out onto the metric URN.
    if customer_tags is None and terms is None and not is_metric:
        return
    existing = by_column.get(column_name)
    if existing is None:
        by_column[column_name] = FieldGovernance(
            column_name=column_name,
            is_metric=is_metric,
            global_tags=customer_tags,
            glossary_terms=terms,
        )
        return
    existing.is_metric = existing.is_metric or is_metric
    if customer_tags is not None:
        existing.global_tags = _union_global_tags(existing.global_tags, customer_tags)
    if terms is not None:
        existing.glossary_terms = _union_glossary_terms(existing.glossary_terms, terms)


def _field_governance_for_emit(
    fields: List[FieldGovernance],
) -> List[FieldGovernance]:
    """Drop metric-classification-only rows that have nothing to copy."""
    return [
        f for f in fields if f.global_tags is not None or f.glossary_terms is not None
    ]


def collect_dataset_field_governance(
    graph: DataHubGraph, dataset_urn: str
) -> List[FieldGovernance]:
    """Read column tags/terms from schemaMetadata + editableSchemaMetadata.

    Synthetic DIMENSION/FACT/METRIC tags are dropped. Remaining tags may include
    Snowflake-sourced tags from ``schemaMetadata`` (the connector merges those
    into field ``globalTags``); the ON-mode connector may also re-emit them.
    UI / API edits typically live on ``editableSchemaMetadata`` and are unioned
    with schema-side tags. METRIC detection uses synthetic tags /
    ``columnSubType`` jsonProps so fan-out can target metric URNs.
    """
    aspects = graph.get_aspects_for_entity(
        entity_urn=dataset_urn,
        aspects=["schemaMetadata", "editableSchemaMetadata"],
        aspect_types=[SchemaMetadataClass, EditableSchemaMetadataClass],
    )
    by_column: Dict[str, FieldGovernance] = {}

    schema_metadata = aspects.get("schemaMetadata")
    if isinstance(schema_metadata, SchemaMetadataClass) and schema_metadata.fields:
        for schema_field in schema_metadata.fields:
            column_name = _simple_column_name(schema_field.fieldPath)
            _merge_field_governance(
                by_column,
                column_name,
                is_metric=_schema_field_is_metric(schema_field),
                tags=schema_field.globalTags,
                terms=schema_field.glossaryTerms,
            )

    editable = aspects.get("editableSchemaMetadata")
    if isinstance(editable, EditableSchemaMetadataClass):
        for field_info in editable.editableSchemaFieldInfo or []:
            column_name = _simple_column_name(field_info.fieldPath)
            existing_is_metric = (
                by_column[column_name].is_metric if column_name in by_column else False
            )
            _merge_field_governance(
                by_column,
                column_name,
                is_metric=existing_is_metric
                or _tags_indicate_metric(field_info.globalTags),
                tags=field_info.globalTags,
                terms=field_info.glossaryTerms,
            )

    return _field_governance_for_emit(list(by_column.values()))


def _semantic_model_field_path(
    column_name: str, convert_urns_to_lowercase: bool
) -> str:
    # Matches SnowflakeSemanticModelMapper SemanticField fieldPath construction
    # (identifier of the uppercased column name).
    return snowflake_identifier(column_name.upper(), convert_urns_to_lowercase)


def _dataset_schema_field_paths(
    graph: DataHubGraph, dataset_urn: str
) -> Dict[str, str]:
    """Map casefolded simple column name -> schemaMetadata fieldPath."""
    schema_metadata = graph.get_aspects_for_entity(
        entity_urn=dataset_urn,
        aspects=["schemaMetadata"],
        aspect_types=[SchemaMetadataClass],
    ).get("schemaMetadata")
    paths: Dict[str, str] = {}
    if (
        not isinstance(schema_metadata, SchemaMetadataClass)
        or not schema_metadata.fields
    ):
        return paths
    for schema_field in schema_metadata.fields:
        simple = _simple_column_name(schema_field.fieldPath)
        paths[simple.casefold()] = schema_field.fieldPath
    return paths


def _resolve_dataset_field_path(
    column_name: str,
    convert_urns_to_lowercase: bool,
    schema_paths: Dict[str, str],
) -> Tuple[str, Optional[str]]:
    """Pick a fieldPath that joins to the dataset schema (preserve original case).

    Unlike semanticModel fieldPaths (always upper-then-identifier), legacy dataset
    schemaMetadata uses snowflake_identifier(col.name) without forcing UPPER, so
    quoted mixed-case columns must keep their schema path when present.

    Returns (field_path, optional_note). When the destination has no matching
    schemaMetadata field, falls back to snowflake_identifier(column_name)
    (original case, not forced UPPER) and returns a note for the report.
    """
    matched = schema_paths.get(column_name.casefold())
    if matched is not None:
        return matched, None
    fallback = snowflake_identifier(column_name, convert_urns_to_lowercase)
    if not schema_paths:
        note = (
            f"no schemaMetadata on destination dataset; wrote editable fieldPath "
            f"'{fallback}' for {column_name} (may not join in UI until re-ingest)"
        )
    else:
        note = (
            f"column {column_name} not in destination schemaMetadata; "
            f"wrote editable fieldPath '{fallback}'"
        )
    return fallback, note


def _is_soft_deleted(graph: DataHubGraph, urn: str) -> bool:
    status = graph.get_aspects_for_entity(
        entity_urn=urn, aspects=["status"], aspect_types=[StatusClass]
    ).get("status")
    return isinstance(status, StatusClass) and bool(status.removed)


def _emit_aspect(
    graph: DataHubGraph, entity_urn: str, aspect: _Aspect, dry_run: bool
) -> None:
    if not dry_run:
        graph.emit_mcp(
            MetadataChangeProposalWrapper(entityUrn=entity_urn, aspect=aspect)
        )


def _ensure_src_exists(graph: DataHubGraph, src_urn: str) -> Optional[str]:
    """Return an error message if the source is missing; else None.

    Destinations may be absent: migrate-first writes governance aspects onto the
    mapped URNs, and a later Snowflake ingest supplies structural aspects.
    """
    if not graph.exists(src_urn):
        return f"source entity does not exist: {src_urn}"
    return None


def migrate_dataset_field_governance(
    graph: DataHubGraph,
    src_dataset_urn: str,
    semantic_model_urn: str,
    identity: SnowflakeViewIdentity,
    platform_instance: Optional[str],
    convert_urns_to_lowercase: bool,
    dry_run: bool,
) -> Tuple[List[str], List[str]]:
    """Fan out DataHub column tags/terms to metric entities or schemaField URNs.

    Returns (migrated entries, skip notes). Destination metric / schemaField URNs
    need not exist yet (migrate-before-ingest). Each field is migrated independently
    so one field's failure doesn't discard progress already made on the others.
    """
    migrated: List[str] = []
    skipped: List[str] = []
    for field_gov in collect_dataset_field_governance(graph, src_dataset_urn):
        try:
            if field_gov.is_metric:
                dst_urn = gen_metric_urn(
                    identity,
                    field_gov.column_name,
                    platform_instance,
                    convert_urns_to_lowercase,
                )
            else:
                field_path = _semantic_model_field_path(
                    field_gov.column_name, convert_urns_to_lowercase
                )
                dst_urn = make_schema_field_urn(semantic_model_urn, field_path)
            if field_gov.global_tags is not None:
                _emit_aspect(graph, dst_urn, field_gov.global_tags, dry_run)
                migrated.append(f"globalTags:{field_gov.column_name}->{dst_urn}")
            if field_gov.glossary_terms is not None:
                _emit_aspect(graph, dst_urn, field_gov.glossary_terms, dry_run)
                migrated.append(f"glossaryTerms:{field_gov.column_name}->{dst_urn}")
        except Exception as e:
            skipped.append(
                f"failed to migrate field governance for {field_gov.column_name}: {e}"
            )
    return migrated, skipped


def collect_semantic_model_field_governance(
    graph: DataHubGraph,
    semantic_model_urn: str,
    convert_urns_to_lowercase: bool,
) -> List[FieldGovernance]:
    """Gather column tags/terms from metric entities and schemaField URNs under the model."""
    by_column: Dict[str, FieldGovernance] = {}

    for related in graph.get_related_entities(
        entity_urn=semantic_model_urn,
        relationship_types=["ModeledBy"],
        direction=RelationshipDirection.INCOMING,
    ):
        if not related.urn.startswith("urn:li:metric:"):
            continue
        try:
            metric_urn_obj = MetricUrn.from_string(related.urn)
            column_name = str(metric_urn_obj.id)
        except Exception:
            continue
        aspects = graph.get_aspects_for_entity(
            entity_urn=related.urn,
            aspects=["globalTags", "glossaryTerms"],
            aspect_types=[GlobalTagsClass, GlossaryTermsClass],
        )
        tags = aspects.get("globalTags")
        terms = aspects.get("glossaryTerms")
        if tags is None and terms is None:
            continue
        by_column[column_name] = FieldGovernance(
            column_name=column_name,
            is_metric=True,
            global_tags=tags if isinstance(tags, GlobalTagsClass) else None,
            glossary_terms=terms if isinstance(terms, GlossaryTermsClass) else None,
        )

    model_info = graph.get_aspects_for_entity(
        entity_urn=semantic_model_urn,
        aspects=["semanticModelInfo"],
        aspect_types=[SemanticModelInfoClass],
    ).get("semanticModelInfo")
    if isinstance(model_info, SemanticModelInfoClass) and model_info.datasets:
        for model_dataset in model_info.datasets:
            for semantic_field in model_dataset.fields or []:
                schema_field = semantic_field.schemaField
                if schema_field is None:
                    continue
                column_name = _simple_column_name(schema_field.fieldPath)
                field_urn = make_schema_field_urn(
                    semantic_model_urn,
                    _semantic_model_field_path(column_name, convert_urns_to_lowercase),
                )
                aspects = graph.get_aspects_for_entity(
                    entity_urn=field_urn,
                    aspects=["globalTags", "glossaryTerms"],
                    aspect_types=[GlobalTagsClass, GlossaryTermsClass],
                )
                tags = aspects.get("globalTags")
                terms = aspects.get("glossaryTerms")
                # Prefer schemaField entity aspects (migration/UI); fall back to
                # tags embedded on the SemanticField from Snowflake ingest.
                if tags is None and schema_field.globalTags is not None:
                    tags = _strip_synthetic_subtype_tags(schema_field.globalTags)
                if terms is None:
                    terms = schema_field.glossaryTerms
                if tags is None and terms is None:
                    continue
                by_column[column_name] = FieldGovernance(
                    column_name=column_name,
                    is_metric=False,
                    global_tags=tags if isinstance(tags, GlobalTagsClass) else None,
                    glossary_terms=(
                        terms if isinstance(terms, GlossaryTermsClass) else None
                    ),
                )

    return list(by_column.values())


def migrate_semantic_model_field_governance(
    graph: DataHubGraph,
    semantic_model_urn: str,
    dataset_urn: str,
    convert_urns_to_lowercase: bool,
    dry_run: bool,
) -> Tuple[List[str], List[str]]:
    """Merge column tags/terms into the dataset's editableSchemaMetadata.

    Preserves existing per-field descriptions on fields the source also touches
    (and any fields not touched). Tags/terms are unioned by URN (deduped).
    Resolves fieldPath against the destination dataset schema so mixed-case
    columns join correctly when convert_urns_to_lowercase is false.

    Returns (migrated entries, notes).
    """
    fields = collect_semantic_model_field_governance(
        graph,
        semantic_model_urn,
        convert_urns_to_lowercase,
    )
    if not fields:
        return [], []

    existing = graph.get_aspects_for_entity(
        entity_urn=dataset_urn,
        aspects=["editableSchemaMetadata"],
        aspect_types=[EditableSchemaMetadataClass],
    ).get("editableSchemaMetadata")
    by_path: Dict[str, EditableSchemaFieldInfoClass] = {}
    if isinstance(existing, EditableSchemaMetadataClass):
        for field_info in existing.editableSchemaFieldInfo or []:
            by_path[field_info.fieldPath] = field_info

    schema_paths = _dataset_schema_field_paths(graph, dataset_urn)
    migrated: List[str] = []
    notes: List[str] = []
    for field_gov in fields:
        field_path, path_note = _resolve_dataset_field_path(
            field_gov.column_name, convert_urns_to_lowercase, schema_paths
        )
        if path_note is not None:
            notes.append(path_note)
        prior = by_path.get(field_path)
        by_path[field_path] = EditableSchemaFieldInfoClass(
            fieldPath=field_path,
            description=prior.description if prior is not None else None,
            globalTags=_union_global_tags(
                prior.globalTags if prior is not None else None,
                field_gov.global_tags,
            )
            if field_gov.global_tags is not None
            else (prior.globalTags if prior is not None else None),
            glossaryTerms=_union_glossary_terms(
                prior.glossaryTerms if prior is not None else None,
                field_gov.glossary_terms,
            )
            if field_gov.glossary_terms is not None
            else (prior.glossaryTerms if prior is not None else None),
        )
        if field_gov.global_tags is not None:
            migrated.append(f"globalTags:{field_gov.column_name}->{dataset_urn}")
        if field_gov.glossary_terms is not None:
            migrated.append(f"glossaryTerms:{field_gov.column_name}->{dataset_urn}")

    _emit_aspect(
        graph,
        dataset_urn,
        EditableSchemaMetadataClass(editableSchemaFieldInfo=list(by_path.values())),
        dry_run,
    )
    return migrated, notes


def _maybe_fold_editable_description(
    graph: DataHubGraph,
    src_dataset_urn: str,
    dst_urn: str,
    governance_aspects: Dict[str, _Aspect],
) -> Optional[str]:
    """Best-effort: fold a dataset's editableDatasetProperties.description into the
    destination's 'documentation' aspect, since ingestion owns semanticModelInfo and
    we must not overwrite it. Only applies when no explicit 'documentation' aspect was
    already found on the source, and the destination has none yet.

    Returns a short note for the report, or None if nothing was done.
    """
    if "documentation" in governance_aspects:
        return None

    editable = graph.get_aspects_for_entity(
        entity_urn=src_dataset_urn,
        aspects=["editableDatasetProperties"],
        aspect_types=[EditableDatasetPropertiesClass],
    ).get("editableDatasetProperties")
    description = getattr(editable, "description", None) if editable else None
    if not description:
        return None

    dst_documentation = graph.get_aspects_for_entity(
        entity_urn=dst_urn, aspects=["documentation"], aspect_types=[DocumentationClass]
    ).get("documentation")
    if dst_documentation is not None and dst_documentation.documentations:
        return "editableDatasetProperties.description present but destination already has documentation -- skipped (manual review)"

    governance_aspects["documentation"] = DocumentationClass(
        documentations=[DocumentationAssociationClass(documentation=description)]
    )
    return "copied editableDatasetProperties.description into documentation aspect"


def _maybe_fold_documentation_to_editable_dataset(
    graph: DataHubGraph,
    dst_dataset_urn: str,
    governance_aspects: Dict[str, _Aspect],
    dry_run: bool,
) -> Optional[str]:
    """sm→dataset: surface documentation on editableDatasetProperties.description.

    Dataset UI primarily renders editableDatasetProperties.description; copying
    only the documentation aspect can leave a round-tripped description invisible.
    """
    documentation = governance_aspects.get("documentation")
    if (
        not isinstance(documentation, DocumentationClass)
        or not documentation.documentations
    ):
        return None
    description = documentation.documentations[0].documentation
    if not description:
        return None

    existing = graph.get_aspects_for_entity(
        entity_urn=dst_dataset_urn,
        aspects=["editableDatasetProperties"],
        aspect_types=[EditableDatasetPropertiesClass],
    ).get("editableDatasetProperties")
    if isinstance(existing, EditableDatasetPropertiesClass) and existing.description:
        return (
            "documentation present but destination already has "
            "editableDatasetProperties.description -- skipped fold"
        )

    if isinstance(existing, EditableDatasetPropertiesClass):
        editable = EditableDatasetPropertiesClass(
            description=description,
            name=existing.name,
            created=existing.created,
            lastModified=existing.lastModified,
            deleted=existing.deleted,
        )
    else:
        editable = EditableDatasetPropertiesClass(description=description)
    _emit_aspect(graph, dst_dataset_urn, editable, dry_run)
    return "folded documentation into editableDatasetProperties.description"


def _fetch_inbound_refs(graph: DataHubGraph, urn: str) -> List[RelatedEntity]:
    """References into a dataset/semanticModel by urn. We never rewrite these --
    only report them so operators know what still points at the pre-migration urn.
    """
    return list(
        graph.get_related_entities(
            entity_urn=urn,
            relationship_types=INBOUND_REFERENCE_RELATIONSHIP_TYPES,
            direction=RelationshipDirection.INCOMING,
        )
    )


def migrate_entity(
    graph: DataHubGraph,
    src_urn: str,
    dst_urn: str,
    dry_run: bool,
    report_inbound_refs: bool,
    fold_editable_description_from: Optional[str] = None,
) -> EntityMigrationResult:
    """Copy the governance-aspect allowlist from src_urn to dst_urn.

    Source must exist; destination may be created by writing aspects (migrate
    before ingest). Always overwrites the destination's existing aspect values
    (last-write-wins); no merge/conflict strategy is offered here (unlike
    dataplatform2instance/instance2instance).
    """
    result = EntityMigrationResult(src_urn=src_urn, dst_urn=dst_urn)
    missing = _ensure_src_exists(graph, src_urn)
    if missing is not None:
        result.error = missing
        return result
    try:
        governance_aspects = _collect_governance_aspects(graph, src_urn)

        if fold_editable_description_from is not None:
            note = _maybe_fold_editable_description(
                graph, fold_editable_description_from, dst_urn, governance_aspects
            )
            if note is not None:
                result.notes.append(note)

        for aspect_name, aspect in governance_aspects.items():
            if not dry_run:
                graph.emit_mcp(
                    MetadataChangeProposalWrapper(entityUrn=dst_urn, aspect=aspect)
                )
            result.aspects_copied.append(aspect_name)

        if report_inbound_refs:
            result.inbound_refs = _fetch_inbound_refs(graph, src_urn)
    except Exception as e:
        log.warning(f"Failed to migrate {src_urn} -> {dst_urn}: {e}")
        result.error = str(e)
    return result


def migrate_dataset_to_semantic_model(
    graph: DataHubGraph,
    src_dataset_urn: str,
    platform_instance: Optional[str],
    convert_urns_to_lowercase: bool,
    dry_run: bool,
    report_inbound_refs: bool,
) -> EntityMigrationResult:
    try:
        identity = parse_dataset_identity(src_dataset_urn, platform_instance)
        dst_urn = gen_semantic_model_urn(
            identity, platform_instance, convert_urns_to_lowercase
        )
    except ValueError as e:
        return EntityMigrationResult(src_urn=src_dataset_urn, dst_urn="", error=str(e))

    result = migrate_entity(
        graph,
        src_dataset_urn,
        dst_urn,
        dry_run,
        report_inbound_refs,
        fold_editable_description_from=src_dataset_urn,
    )
    if result.error is not None:
        return result

    if _is_soft_deleted(graph, src_dataset_urn):
        result.notes.append("source is soft-deleted")

    try:
        migrated, skipped = migrate_dataset_field_governance(
            graph,
            src_dataset_urn,
            dst_urn,
            identity,
            platform_instance,
            convert_urns_to_lowercase,
            dry_run,
        )
        result.fields_migrated = migrated
        result.notes.extend(skipped)
    except Exception as e:
        log.warning(
            f"Field governance migration failed for {src_dataset_urn} -> {dst_urn}: {e}"
        )
        result.notes.append(f"field governance migration failed: {e}")
    return result


def migrate_semantic_model_to_dataset(
    graph: DataHubGraph,
    src_semantic_model_urn: str,
    platform_instance: Optional[str],
    convert_urns_to_lowercase: bool,
    env: str,
    dry_run: bool,
    report_inbound_refs: bool,
) -> EntityMigrationResult:
    try:
        identity = parse_semantic_model_identity(
            src_semantic_model_urn, platform_instance
        )
        dst_urn = gen_dataset_urn(
            identity, platform_instance, env, convert_urns_to_lowercase
        )
    except ValueError as e:
        return EntityMigrationResult(
            src_urn=src_semantic_model_urn, dst_urn="", error=str(e)
        )

    result = migrate_entity(
        graph, src_semantic_model_urn, dst_urn, dry_run, report_inbound_refs
    )
    if result.error is not None:
        return result

    if _is_soft_deleted(graph, src_semantic_model_urn):
        result.notes.append("source is soft-deleted")

    try:
        # Re-collect so we can fold documentation that was just selected for copy.
        governance_aspects = _collect_governance_aspects(graph, src_semantic_model_urn)
        note = _maybe_fold_documentation_to_editable_dataset(
            graph, dst_urn, governance_aspects, dry_run
        )
        if note is not None:
            result.notes.append(note)
    except Exception as e:
        log.warning(
            f"Documentation→editable fold failed for {src_semantic_model_urn} -> {dst_urn}: {e}"
        )
        result.notes.append(f"documentation→editable fold failed: {e}")

    try:
        migrated, field_notes = migrate_semantic_model_field_governance(
            graph,
            src_semantic_model_urn,
            dst_urn,
            convert_urns_to_lowercase,
            dry_run,
        )
        result.fields_migrated = migrated
        result.notes.extend(field_notes)
    except Exception as e:
        log.warning(
            f"Field governance migration failed for {src_semantic_model_urn} -> {dst_urn}: {e}"
        )
        result.notes.append(f"field governance migration failed: {e}")
    return result


# --- Discovery ---


def _status_filter(
    include_soft_deleted: bool, only_soft_deleted: bool
) -> RemovedStatusFilter:
    if only_soft_deleted:
        return RemovedStatusFilter.ONLY_SOFT_DELETED
    if include_soft_deleted:
        return RemovedStatusFilter.ALL
    return RemovedStatusFilter.NOT_SOFT_DELETED


def discover_semantic_view_dataset_urns(
    graph: DataHubGraph,
    env: Optional[str] = None,
    platform_instance: Optional[str] = None,
    include_soft_deleted: bool = False,
    *,
    only_soft_deleted: bool = False,
) -> List[str]:
    """Find Snowflake dataset urns carrying the "Semantic View" subtype.

    Defaults to live entities only. After flag-ON ingest, sources are often
    soft-deleted — pass ``include_soft_deleted=True`` (or use the CLI flag) once
    operators have confirmed that set is intentional. ``only_soft_deleted`` is
    for the empty-discovery hint probe.
    """
    status = _status_filter(include_soft_deleted, only_soft_deleted)
    return list(
        graph.get_urns_by_filter(
            entity_types=["dataset"],
            platform=SNOWFLAKE_PLATFORM,
            platform_instance=platform_instance,
            env=env,
            status=status,
            extraFilters=[
                SearchFilterRule(
                    field="typeNames", condition="EQUAL", values=[SEMANTIC_VIEW_SUBTYPE]
                ).to_raw()
            ],
        )
    )


def discover_semantic_model_urns(
    graph: DataHubGraph,
    platform_instance: Optional[str] = None,
    include_soft_deleted: bool = False,
    *,
    only_soft_deleted: bool = False,
) -> List[str]:
    """Find semanticModel urns on the Snowflake platform.

    Defaults to live entities only. Use ``include_soft_deleted=True`` for
    flag-OFF rollback after stateful ingest soft-deletes the models.
    """
    status = _status_filter(include_soft_deleted, only_soft_deleted)
    return list(
        graph.get_urns_by_filter(
            entity_types=["semanticModel"],
            platform=SNOWFLAKE_PLATFORM,
            platform_instance=platform_instance,
            status=status,
        )
    )


def filter_by_semantic_view_subtype(
    graph: DataHubGraph, urns: Sequence[str], force: bool
) -> Tuple[List[str], List[str]]:
    """Split urns into (valid, skipped) based on the "Semantic View" subtype.

    When force is set, subtype validation is bypassed entirely (all urns are valid).
    """
    if force:
        return list(urns), []

    valid: List[str] = []
    skipped: List[str] = []
    for urn in urns:
        subtypes = graph.get_aspects_for_entity(
            entity_urn=urn, aspects=["subTypes"], aspect_types=[SubTypesClass]
        ).get("subTypes")
        if subtypes is not None and SEMANTIC_VIEW_SUBTYPE in (subtypes.typeNames or []):
            valid.append(urn)
        else:
            skipped.append(urn)
    return valid, skipped


# --- Reporting ---


@dataclass
class SemanticViewMigrationReport:
    direction: MigrationDirection
    dry_run: bool
    results: List[EntityMigrationResult] = field(default_factory=list)
    subtype_skipped: List[str] = field(default_factory=list)

    def __repr__(self) -> str:
        prefix = "[Dry Run] " if self.dry_run else ""
        succeeded = [r for r in self.results if r.error is None]
        failed = [r for r in self.results if r.error is not None]
        lines = [
            f"{prefix}Snowflake Semantic View Migration Report ({self.direction.value}):",
            "--------------",
            f"{prefix}Entities migrated = {len(succeeded)}",
            f"{prefix}Entities errored = {len(failed)}",
        ]
        if self.subtype_skipped:
            lines.append(
                f"{prefix}Entities skipped (not '{SEMANTIC_VIEW_SUBTYPE}' subtype) = "
                f"{len(self.subtype_skipped)}"
            )
            for skipped_urn in self.subtype_skipped:
                lines.append(f"{prefix}  skipped: {skipped_urn}")
        lines.append(f"{prefix}Not migrated (by design): {', '.join(SKIPPED_ASPECTS)}")
        lines.append(f"{prefix}Details:")
        for r in succeeded:
            lines.append(f"{prefix}  {r.src_urn} -> {r.dst_urn}")
            lines.append(f"{prefix}    aspects copied: {r.aspects_copied or 'none'}")
            if r.fields_migrated:
                lines.append(
                    f"{prefix}    field tags/terms: {', '.join(r.fields_migrated)}"
                )
            for note in r.notes:
                lines.append(f"{prefix}    note: {note}")
            if r.inbound_refs:
                refs = ", ".join(
                    f"{ref.relationship_type}<-{ref.urn}" for ref in r.inbound_refs
                )
                lines.append(f"{prefix}    inbound refs not repointed: {refs}")
        for r in failed:
            lines.append(f"{prefix}  {r.src_urn}: ERROR: {r.error}")
            if r.aspects_copied:
                lines.append(
                    f"{prefix}    aspects copied before failure: {r.aspects_copied}"
                )
            if r.fields_migrated:
                lines.append(
                    f"{prefix}    field tags/terms copied before failure: "
                    f"{', '.join(r.fields_migrated)}"
                )
        return "\n".join(lines)


def run_migration(
    graph: DataHubGraph,
    direction: MigrationDirection,
    urns: Sequence[str],
    platform_instance: Optional[str],
    convert_urns_to_lowercase: bool,
    env: str,
    dry_run: bool,
    report_inbound_refs: bool,
    subtype_skipped: Optional[List[str]] = None,
) -> SemanticViewMigrationReport:
    report = SemanticViewMigrationReport(
        direction=direction,
        dry_run=dry_run,
        subtype_skipped=list(subtype_skipped or []),
    )
    for urn in urns:
        try:
            if direction == MigrationDirection.DATASET_TO_SM:
                result = migrate_dataset_to_semantic_model(
                    graph,
                    urn,
                    platform_instance,
                    convert_urns_to_lowercase,
                    dry_run,
                    report_inbound_refs,
                )
            else:
                result = migrate_semantic_model_to_dataset(
                    graph,
                    urn,
                    platform_instance,
                    convert_urns_to_lowercase,
                    env,
                    dry_run,
                    report_inbound_refs,
                )
        except Exception as e:
            log.warning(f"Unexpected error migrating {urn}: {e}")
            result = EntityMigrationResult(src_urn=urn, dst_urn="", error=str(e))
        report.results.append(result)
    return report
