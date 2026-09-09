"""Source-independent parts of the semantic-model governance migrations.

Shared by the Snowflake Semantic View and dbt semantic model migrations. What
lives here is what must not diverge between them: the governance aspect
allowlist, the tag/term union semantics, the editableSchemaMetadata merge, the
per-entity error isolation, and the report.

What deliberately does *not* live here is URN algebra and orchestration. Each
source encodes its own identity, and the destinations differ in kind: Snowflake
maps a legacy dataset onto a ``semanticModel``, while dbt maps it onto another
dataset (the Semantic Model Dataset), because a dbt ``semanticModel`` is
project-scoped and shared by every semantic model in the project.
"""

import logging
from dataclasses import dataclass, field
from typing import (
    AbstractSet,
    Callable,
    Dict,
    List,
    Optional,
    Protocol,
    Sequence,
    Set,
    Tuple,
)

from datahub.cli.migration_utils import INBOUND_REFERENCE_RELATIONSHIP_TYPES
from datahub.emitter.aspect import ASPECT_MAP
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import RemovedStatusFilter
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
    StatusClass,
    SubTypesClass,
    _Aspect,
)
from datahub.utilities.str_enum import StrEnum
from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path

log = logging.getLogger(__name__)

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
# to extract column tags/terms for field fan-out.
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
    # The values are CLI surface. "sm" reads as the new-entity side of the
    # mapping, which for dbt is a Semantic Model Dataset rather than a
    # semanticModel -- the per-command help spells that out.
    DATASET_TO_SM = "dataset-to-sm"
    SM_TO_DATASET = "sm-to-dataset"


def describe_exception(e: Exception) -> str:
    """str(e) alone loses the type -- a KeyError renders as a bare quoted key."""
    return f"{type(e).__name__}: {e}"


@dataclass
class EntityMigrationResult:
    src_urn: str
    dst_urn: str
    aspects_copied: List[str] = field(default_factory=list)
    fields_migrated: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    inbound_refs: List[RelatedEntity] = field(default_factory=list)
    error: Optional[str] = None
    # Column tags/terms that could not be migrated. Kept apart from `error`
    # so the entity-level copy still counts as done, but reported separately
    # rather than hidden in notes on an otherwise-successful entity.
    field_errors: List[str] = field(default_factory=list)


@dataclass
class FieldGovernance:
    column_name: str
    is_metric: bool
    global_tags: Optional[GlobalTagsClass] = None
    glossary_terms: Optional[GlossaryTermsClass] = None


def collect_governance_aspects(
    graph: DataHubGraph, src_urn: str, extra_aspects: Sequence[str] = ()
) -> Dict[str, _Aspect]:
    names = list(GOVERNANCE_ASPECTS) + [
        name for name in extra_aspects if name not in GOVERNANCE_ASPECTS
    ]
    aspect_types = [ASPECT_MAP[name] for name in names]
    result = graph.get_aspects_for_entity(
        entity_urn=src_urn, aspects=names, aspect_types=aspect_types
    )
    return {name: aspect for name, aspect in result.items() if aspect is not None}


def simple_column_name(field_path: str) -> str:
    try:
        simple = get_simple_field_path_from_v2_field_path(field_path)
    except (AttributeError, TypeError):
        # The helper indexes the path as a string, so a non-string stored path
        # is the realistic failure. The raw value is the best remaining guess,
        # but it will not join to a real column, so the governance write lands
        # on an orphan URN.
        log.warning(
            f"Could not simplify schema field path {field_path!r}; using it "
            "as-is, which may not match any column on the destination",
            exc_info=True,
        )
        simple = field_path
    return simple.split(".")[-1]


def strip_synthetic_subtype_tags(
    tags: Optional[GlobalTagsClass], synthetic_tag_urns: AbstractSet[str]
) -> Optional[GlobalTagsClass]:
    """Drop connector-synthesized classification tags, keeping customer tags.

    ``synthetic_tag_urns`` is empty for sources that never synthesize such tags
    (dbt encodes the classification in nativeDataType instead), in which case
    every tag is a customer tag and nothing is stripped.
    """
    if tags is None or not tags.tags:
        return None
    kept = [t for t in tags.tags if t.tag not in synthetic_tag_urns]
    if not kept:
        return None
    return GlobalTagsClass(tags=kept)


def union_global_tags(
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


def union_glossary_terms(
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


def merge_field_governance(
    by_column: Dict[str, FieldGovernance],
    column_name: str,
    is_metric: bool,
    tags: Optional[GlobalTagsClass],
    terms: Optional[GlossaryTermsClass],
    synthetic_tag_urns: AbstractSet[str],
) -> None:
    customer_tags = strip_synthetic_subtype_tags(tags, synthetic_tag_urns)
    # Always record METRIC classification even when the only tags were synthetic
    # subtype tags (stripped above) so a later editableSchemaMetadata merge can
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
        existing.global_tags = union_global_tags(existing.global_tags, customer_tags)
    if terms is not None:
        existing.glossary_terms = union_glossary_terms(existing.glossary_terms, terms)


def field_governance_for_emit(
    fields: List[FieldGovernance],
) -> List[FieldGovernance]:
    """Drop metric-classification-only rows that have nothing to copy."""
    return [
        f for f in fields if f.global_tags is not None or f.glossary_terms is not None
    ]


def collect_dataset_field_governance(
    graph: DataHubGraph,
    dataset_urn: str,
    synthetic_tag_urns: AbstractSet[str] = frozenset(),
    is_metric_column: Optional[Callable[[SchemaFieldClass], bool]] = None,
    tags_indicate_metric: Optional[Callable[[Optional[GlobalTagsClass]], bool]] = None,
) -> List[FieldGovernance]:
    """Read column tags/terms from schemaMetadata + editableSchemaMetadata.

    UI / API edits typically live on ``editableSchemaMetadata`` and are unioned
    with schema-side tags.

    The metric-classification callbacks default to "no column is a metric",
    which is right for any source that does not fan column governance out onto
    metric URNs.
    """
    is_metric = is_metric_column or (lambda schema_field: False)
    metric_tags = tags_indicate_metric or (lambda tags: False)
    aspects = graph.get_aspects_for_entity(
        entity_urn=dataset_urn,
        aspects=["schemaMetadata", "editableSchemaMetadata"],
        aspect_types=[SchemaMetadataClass, EditableSchemaMetadataClass],
    )
    by_column: Dict[str, FieldGovernance] = {}

    schema_metadata = aspects.get("schemaMetadata")
    if isinstance(schema_metadata, SchemaMetadataClass) and schema_metadata.fields:
        for schema_field in schema_metadata.fields:
            column_name = simple_column_name(schema_field.fieldPath)
            merge_field_governance(
                by_column,
                column_name,
                is_metric=is_metric(schema_field),
                tags=schema_field.globalTags,
                terms=schema_field.glossaryTerms,
                synthetic_tag_urns=synthetic_tag_urns,
            )

    editable = aspects.get("editableSchemaMetadata")
    if isinstance(editable, EditableSchemaMetadataClass):
        for field_info in editable.editableSchemaFieldInfo or []:
            column_name = simple_column_name(field_info.fieldPath)
            existing_is_metric = (
                by_column[column_name].is_metric if column_name in by_column else False
            )
            merge_field_governance(
                by_column,
                column_name,
                is_metric=existing_is_metric or metric_tags(field_info.globalTags),
                tags=field_info.globalTags,
                terms=field_info.glossaryTerms,
                synthetic_tag_urns=synthetic_tag_urns,
            )

    return field_governance_for_emit(list(by_column.values()))


def schema_metadata_fields(
    graph: DataHubGraph, dataset_urn: str
) -> List[SchemaFieldClass]:
    schema_metadata = graph.get_aspects_for_entity(
        entity_urn=dataset_urn,
        aspects=["schemaMetadata"],
        aspect_types=[SchemaMetadataClass],
    ).get("schemaMetadata")
    if not isinstance(schema_metadata, SchemaMetadataClass):
        return []
    return list(schema_metadata.fields or [])


def dataset_schema_field_paths(graph: DataHubGraph, dataset_urn: str) -> Dict[str, str]:
    """Map casefolded simple column name -> schemaMetadata fieldPath."""
    paths: Dict[str, str] = {}
    for schema_field in schema_metadata_fields(graph, dataset_urn):
        simple = simple_column_name(schema_field.fieldPath)
        paths[simple.casefold()] = schema_field.fieldPath
    return paths


def resolve_field_path(
    column_name: str,
    schema_paths: Dict[str, str],
    fallback: str,
) -> Tuple[str, Optional[str]]:
    """Pick a fieldPath that joins to the destination dataset's schema.

    Prefers the destination's own schemaMetadata path so mixed-case columns
    join correctly. ``fallback`` is the source-specific path to write when the
    destination has no matching field -- which is the migrate-before-ingest
    case, and returns a note for the report.
    """
    matched = schema_paths.get(column_name.casefold())
    if matched is not None:
        return matched, None
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


def is_soft_deleted(graph: DataHubGraph, urn: str) -> bool:
    status = graph.get_aspects_for_entity(
        entity_urn=urn, aspects=["status"], aspect_types=[StatusClass]
    ).get("status")
    return isinstance(status, StatusClass) and bool(status.removed)


def emit_aspect(
    graph: DataHubGraph, entity_urn: str, aspect: _Aspect, dry_run: bool
) -> None:
    if not dry_run:
        graph.emit_mcp(
            MetadataChangeProposalWrapper(entityUrn=entity_urn, aspect=aspect)
        )


def ensure_src_exists(graph: DataHubGraph, src_urn: str) -> Optional[str]:
    """Return an error message if the source is missing; else None.

    Destinations may be absent: migrate-first writes governance aspects onto the
    mapped URNs, and a later ingest supplies structural aspects.
    """
    if not graph.exists(src_urn):
        return f"source entity does not exist: {src_urn}"
    return None


def fetch_inbound_refs(graph: DataHubGraph, urn: str) -> List[RelatedEntity]:
    """References into an entity by urn. We never rewrite these -- only report
    them so operators know what still points at the pre-migration urn.
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
    extra_aspects: Sequence[str] = (),
) -> EntityMigrationResult:
    """Copy the governance-aspect allowlist from src_urn to dst_urn.

    Source must exist; destination may be created by writing aspects (migrate
    before ingest). Always overwrites the destination's existing aspect values
    (last-write-wins); no merge/conflict strategy is offered here.
    """
    result = EntityMigrationResult(src_urn=src_urn, dst_urn=dst_urn)
    missing = ensure_src_exists(graph, src_urn)
    if missing is not None:
        result.error = missing
        return result
    try:
        governance_aspects = collect_governance_aspects(graph, src_urn, extra_aspects)

        if fold_editable_description_from is not None:
            note = maybe_fold_editable_description(
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
            result.inbound_refs = fetch_inbound_refs(graph, src_urn)
    except Exception as e:
        log.warning(f"Failed to migrate {src_urn} -> {dst_urn}", exc_info=True)
        result.error = describe_exception(e)
    return result


def merge_field_governance_into_editable_schema(
    graph: DataHubGraph,
    dataset_urn: str,
    fields: List[FieldGovernance],
    resolve_field_path: Callable[[str, Dict[str, str]], Tuple[str, Optional[str]]],
    dry_run: bool,
) -> Tuple[List[str], List[str]]:
    """Merge column tags/terms into the dataset's editableSchemaMetadata.

    Preserves existing per-field descriptions on fields the source also touches
    (and any fields not touched). Tags/terms are unioned by URN (deduped).
    ``resolve_field_path`` maps a column name plus the destination's schema
    paths onto the fieldPath to write, and may return a note for the report.

    Returns (migrated entries, notes).
    """
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

    schema_paths = dataset_schema_field_paths(graph, dataset_urn)
    migrated: List[str] = []
    notes: List[str] = []
    for field_gov in fields:
        field_path, path_note = resolve_field_path(field_gov.column_name, schema_paths)
        if path_note is not None:
            notes.append(path_note)
        prior = by_path.get(field_path)
        prior_tags = prior.globalTags if prior is not None else None
        prior_terms = prior.glossaryTerms if prior is not None else None
        by_path[field_path] = EditableSchemaFieldInfoClass(
            fieldPath=field_path,
            description=prior.description if prior is not None else None,
            globalTags=union_global_tags(prior_tags, field_gov.global_tags),
            glossaryTerms=union_glossary_terms(prior_terms, field_gov.glossary_terms),
        )
        if field_gov.global_tags is not None:
            migrated.append(f"globalTags:{field_gov.column_name}->{dataset_urn}")
        if field_gov.glossary_terms is not None:
            migrated.append(f"glossaryTerms:{field_gov.column_name}->{dataset_urn}")

    emit_aspect(
        graph,
        dataset_urn,
        EditableSchemaMetadataClass(editableSchemaFieldInfo=list(by_path.values())),
        dry_run,
    )
    return migrated, notes


def maybe_fold_editable_description(
    graph: DataHubGraph,
    src_dataset_urn: str,
    dst_urn: str,
    governance_aspects: Dict[str, _Aspect],
) -> Optional[str]:
    """Best-effort: fold a dataset's editableDatasetProperties.description into the
    destination's 'documentation' aspect, for destinations whose descriptive
    aspect is owned by ingestion. Only applies when no explicit 'documentation'
    aspect was already found on the source, and the destination has none yet.

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


def maybe_fold_documentation_to_editable_dataset(
    graph: DataHubGraph,
    dst_dataset_urn: str,
    governance_aspects: Dict[str, _Aspect],
    dry_run: bool,
) -> Optional[str]:
    """Surface a documentation aspect on editableDatasetProperties.description.

    The dataset UI primarily renders editableDatasetProperties.description;
    copying only the documentation aspect can leave a round-tripped description
    invisible.
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
    emit_aspect(graph, dst_dataset_urn, editable, dry_run)
    return "folded documentation into editableDatasetProperties.description"


class DiscoverSources(Protocol):
    """Discovery callback contract for resolve_migration_sources.

    A Protocol rather than ``Callable[..., List[str]]``, which would disable
    checking that the callback accepts the keyword this helper passes.
    """

    def __call__(self, only_soft_deleted: bool) -> List[str]: ...


@dataclass
class ResolvedSources:
    """The source urns to migrate, or a terminal message explaining why none.

    Shared by every semantic-model migration command: the collect / dedupe /
    subtype-filter / discover / empty-result sequence is identical, and the
    empty-result messages are the operator-facing part most likely to drift.
    """

    urns: List[str] = field(default_factory=list)
    subtype_skipped: List[str] = field(default_factory=list)
    # Set when there is nothing to migrate; the caller echoes it and stops.
    message: Optional[str] = None


def resolve_migration_sources(
    *,
    explicit_urns: Sequence[str],
    discover: DiscoverSources,
    expected_subtype: str,
    soft_deleted_label: str,
    include_soft_deleted: bool,
    filter_subtype: Optional[
        Callable[[Sequence[str]], Tuple[List[str], List[str]]]
    ] = None,
) -> ResolvedSources:
    """Resolve which urns to migrate from explicit input or discovery.

    ``filter_subtype`` is None for directions that do not validate the
    source's subtype.
    """
    urns = list(dict.fromkeys(explicit_urns))
    used_discovery = not urns

    subtype_skipped: List[str] = []
    if urns:
        if filter_subtype is not None:
            urns, subtype_skipped = filter_subtype(urns)
    else:
        urns = discover(only_soft_deleted=False)

    if urns:
        return ResolvedSources(urns=urns, subtype_skipped=subtype_skipped)

    if subtype_skipped:
        return ResolvedSources(
            subtype_skipped=subtype_skipped,
            message=(
                f"No entities found to migrate: all {len(subtype_skipped)} "
                f"provided urn(s) lack the '{expected_subtype}' subtype. Pass "
                "--force to bypass this check."
            ),
        )
    if used_discovery and not include_soft_deleted:
        # After a flag flip, stateful ingestion soft-deletes the previous side,
        # so "nothing found" usually means "found, but soft-deleted".
        soft_only = discover(only_soft_deleted=True)
        if soft_only:
            return ResolvedSources(
                message=(
                    f"No live entities found to migrate, but found "
                    f"{len(soft_only)} soft-deleted {soft_deleted_label}"
                    f"{'s' if len(soft_only) != 1 else ''}. "
                    "Did ingest already run? Re-run with --include-soft-deleted "
                    "after reviewing that set."
                )
            )
    return ResolvedSources(message="No entities found to migrate.")


def status_filter(
    include_soft_deleted: bool, only_soft_deleted: bool
) -> RemovedStatusFilter:
    if only_soft_deleted:
        return RemovedStatusFilter.ONLY_SOFT_DELETED
    if include_soft_deleted:
        return RemovedStatusFilter.ALL
    return RemovedStatusFilter.NOT_SOFT_DELETED


def filter_by_subtype(
    graph: DataHubGraph, urns: Sequence[str], force: bool, subtype: str
) -> Tuple[List[str], List[str]]:
    """Split urns into (valid, skipped) based on an exact subtype match.

    The match must be exact: "Semantic Model" and "Semantic Model Dataset" are
    distinct subtypes, and a prefix match would let a migration target its own
    destinations. When force is set, validation is bypassed entirely.
    """
    if force:
        return list(urns), []

    valid: List[str] = []
    skipped: List[str] = []
    for urn in urns:
        subtypes = graph.get_aspects_for_entity(
            entity_urn=urn, aspects=["subTypes"], aspect_types=[SubTypesClass]
        ).get("subTypes")
        if subtypes is not None and subtype in (subtypes.typeNames or []):
            valid.append(urn)
        else:
            skipped.append(urn)
    return valid, skipped


@dataclass
class MigrationReport:
    direction: MigrationDirection
    dry_run: bool
    title: str = "Semantic Model Migration Report"
    legacy_subtype_label: str = "Semantic Model"
    results: List[EntityMigrationResult] = field(default_factory=list)
    subtype_skipped: List[str] = field(default_factory=list)

    def __repr__(self) -> str:
        prefix = "[Dry Run] " if self.dry_run else ""
        succeeded = [r for r in self.results if r.error is None]
        failed = [r for r in self.results if r.error is not None]
        field_failed = [r for r in succeeded if r.field_errors]
        lines = [
            f"{prefix}{self.title} ({self.direction.value}):",
            "--------------",
            f"{prefix}Entities migrated = {len(succeeded)}",
            f"{prefix}Entities errored = {len(failed)}",
        ]
        if field_failed:
            lines.append(
                f"{prefix}Entities with field-governance failures = {len(field_failed)}"
            )
        if self.subtype_skipped:
            lines.append(
                f"{prefix}Entities skipped (not '{self.legacy_subtype_label}' subtype) = "
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
            for field_error in r.field_errors:
                lines.append(f"{prefix}    field governance FAILED: {field_error}")
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


def run_migration_loop(
    urns: Sequence[str],
    migrate_one: Callable[[str], EntityMigrationResult],
    report: MigrationReport,
) -> MigrationReport:
    """Migrate each urn, isolating failures so one bad urn cannot abort the batch."""
    for urn in urns:
        try:
            result = migrate_one(urn)
        except Exception as e:
            log.warning(f"Unexpected error migrating {urn}", exc_info=True)
            result = EntityMigrationResult(
                src_urn=urn, dst_urn="", error=describe_exception(e)
            )
        report.results.append(result)
    return report
