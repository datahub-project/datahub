"""Re-anchor column metadata after a connector changes column field-path casing.

A casing change (Snowflake ``preserve_column_case``, or any connector that used
to lowercase everything) makes re-ingestion rewrite ``schemaMetadata`` with new
field paths, orphaning column-level metadata on the old paths: the dataset's
``editableSchemaMetadata`` entries and aspects on the ``schemaField`` entities
(whose urns embed the field path). This reconciles both onto the current schema
by a case-insensitive path match, making no assumption about the transform so it
works for any connector.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Set, Tuple, cast

import click

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import RemovedStatusFilter, SearchFilterRule
from datahub.metadata.schema_classes import (
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    SchemaMetadataClass,
    StructuredPropertiesClass,
    TagAssociationClass,
    _Aspect,
)
from datahub.metadata.urns import SchemaFieldUrn
from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path

log = logging.getLogger(__name__)

DATASET_ENTITY = "dataset"
SCHEMA_FIELD_ENTITY = "schemaField"

# Searchable field on schemaFieldKey pointing at the owning dataset.
SCHEMA_FIELD_PARENT_FIELD = "parent"

# The user/API-authored aspects on a schemaField entity, from the schemaField
# entity in entity-registry.yml. The rest of that entity's aspects are omitted on
# purpose: schemafieldInfo/subTypes/logicalParent/schemaFieldAliases are
# structural and re-emitted by ingestion, status is handled by the soft-delete of
# the old field, and testResults/incidentsSummary are runtime state.
MIGRATED_SCHEMA_FIELD_ASPECTS: List[str] = [
    "documentation",
    "structuredProperties",
    "businessAttributes",
    "globalTags",
    "glossaryTerms",
    "deprecation",
    "ownership",
    "domains",
    "forms",
    "semanticFieldAnnotation",
    "aiContext",
]


def _norm(field_path: str) -> str:
    # Reduce both v1 and v2 field paths to the simple dotted path, then casefold,
    # so a casing change matches regardless of encoding. The full current path is
    # still used when writing.
    return get_simple_field_path_from_v2_field_path(field_path).casefold()


@dataclass
class PathReconciler:
    """Maps a stranded (old) field path to the current schema's field path."""

    current_paths: Set[str]
    _norm_to_paths: Dict[str, List[str]]
    _ambiguous: Set[str]

    @classmethod
    def build(cls, paths: Sequence[str]) -> "PathReconciler":
        norm_to_paths: Dict[str, List[str]] = {}
        for path in paths:
            bucket = norm_to_paths.setdefault(_norm(path), [])
            if path not in bucket:
                bucket.append(path)
        ambiguous = {norm for norm, ps in norm_to_paths.items() if len(ps) > 1}
        return cls(set(paths), norm_to_paths, ambiguous)

    def resolve(self, old_path: str) -> Tuple[Optional[str], Optional[str]]:
        """Return (new_path, unresolved_reason). Exactly one is non-None.

        A path already present in the current schema resolves to itself (no-op).
        """
        if old_path in self.current_paths:
            return old_path, None
        norm = _norm(old_path)
        if norm in self._ambiguous:
            return (
                None,
                f"case-only collision: '{old_path}' matches multiple current "
                f"fields {sorted(self._norm_to_paths[norm])} — resolve manually",
            )
        targets = self._norm_to_paths.get(norm)
        if not targets:
            return (
                None,
                f"no current schema field matches '{old_path}' "
                "(column dropped or renamed beyond a casing change?)",
            )
        return targets[0], None

    def candidates(self, old_path: str) -> List[str]:
        """Current field paths an ambiguous ``old_path`` could map to (>1 means a
        case-only collision; empty means no match at all)."""
        return sorted(self._norm_to_paths.get(_norm(old_path), []))


@dataclass
class FieldRemap:
    old_path: str
    new_path: str
    schema_field_aspects: List[str] = field(default_factory=list)
    editable: bool = False


@dataclass
class DatasetReconcileResult:
    dataset_urn: str
    remaps: List[FieldRemap] = field(default_factory=list)
    editable_updated: bool = False
    skipped: List[str] = field(default_factory=list)
    error: Optional[str] = None


class ClashResolver:
    """Decides how to handle the two situations the reconciler cannot resolve on
    its own. The default is conservative: never guess — report and skip, so no
    metadata is lost or overwritten. ``--interactive`` swaps in a subclass that
    asks the operator instead.
    """

    def choose_target(
        self, old_path: str, candidates: List[str], what: str
    ) -> Optional[str]:
        """Pick which current field a stranded ``old_path`` maps to when it
        case-folds to more than one. ``None`` means skip."""
        return None

    def resolve_conflict(self, old_path: str, new_path: str, aspect_name: str) -> bool:
        """Whether to overwrite the destination's existing ``aspect_name`` with the
        stranded field's value. ``False`` keeps both for manual handling."""
        return False


class InteractiveClashResolver(ClashResolver):
    def choose_target(
        self, old_path: str, candidates: List[str], what: str
    ) -> Optional[str]:
        click.echo(f"\nAmbiguous: stranded '{old_path}' ({what}) could map to:")
        for i, candidate in enumerate(candidates, 1):
            click.echo(f"  [{i}] {candidate}")
        click.echo("  [s] skip (leave for manual handling)")
        while True:
            choice = click.prompt("Choose target", default="s").strip().lower()
            if choice == "s":
                return None
            if choice.isdigit() and 1 <= int(choice) <= len(candidates):
                return candidates[int(choice) - 1]
            click.echo("Invalid choice.")

    def resolve_conflict(self, old_path: str, new_path: str, aspect_name: str) -> bool:
        return click.confirm(
            f"\nConflict on '{new_path}': it already has a different "
            f"'{aspect_name}' than stranded '{old_path}'. Overwrite the "
            f"destination with the stranded value?",
            default=False,
        )


def _current_field_paths(graph: DataHubGraph, dataset_urn: str) -> List[str]:
    schema_metadata = graph.get_aspect(dataset_urn, SchemaMetadataClass)
    if schema_metadata is None or not schema_metadata.fields:
        return []
    return [f.fieldPath for f in schema_metadata.fields]


def discover_schema_field_urns(
    graph: DataHubGraph, dataset_urn: str, include_soft_deleted: bool
) -> List[str]:
    """All schemaField entities whose parent is this dataset.

    Uses the ``@Searchable`` ``parent`` field on ``schemaFieldKey`` rather than
    reconstructing urns from a guessed transform, so orphaned fields are found
    whatever the old casing was.
    """
    status = (
        RemovedStatusFilter.ALL
        if include_soft_deleted
        else RemovedStatusFilter.NOT_SOFT_DELETED
    )
    return list(
        graph.get_urns_by_filter(
            entity_types=[SCHEMA_FIELD_ENTITY],
            extraFilters=[
                SearchFilterRule(
                    field=SCHEMA_FIELD_PARENT_FIELD,
                    condition="EQUAL",
                    values=[dataset_urn],
                ).to_raw()
            ],
            status=status,
        )
    )


def _read_schema_field_aspects(
    graph: DataHubGraph, schema_field_urn: str
) -> Dict[str, _Aspect]:
    bag = cast(
        Dict[str, _Aspect],
        graph.get_entity_semityped(
            schema_field_urn, aspects=MIGRATED_SCHEMA_FIELD_ASPECTS
        ),
    )
    return {name: bag[name] for name in MIGRATED_SCHEMA_FIELD_ASPECTS if name in bag}


def _dedup_tags(tags: Sequence[TagAssociationClass]) -> List[TagAssociationClass]:
    # On a duplicate urn, keep the association carrying attribution (a
    # source-assigned / propagated / "immutable" tag) over a bare UI one, so the
    # attribution is not silently dropped when the same tag exists on both sides.
    by_urn: Dict[str, TagAssociationClass] = {}
    for tag in tags:
        prev = by_urn.get(tag.tag)
        if prev is None or (prev.attribution is None and tag.attribution is not None):
            by_urn[tag.tag] = tag
    return list(by_urn.values())


def _dedup_terms(
    terms: Sequence[GlossaryTermAssociationClass],
) -> List[GlossaryTermAssociationClass]:
    by_urn: Dict[str, GlossaryTermAssociationClass] = {}
    for term in terms:
        prev = by_urn.get(term.urn)
        if prev is None or (prev.attribution is None and term.attribution is not None):
            by_urn[term.urn] = term
    return list(by_urn.values())


def _union_association_aspect(
    name: str, dest: Optional[_Aspect], src: _Aspect
) -> _Aspect:
    """Union a globalTags/glossaryTerms aspect from the old field onto whatever the
    destination field already carries, deduping by urn and preserving attribution."""
    if name == "globalTags":
        dest_tags = dest.tags if isinstance(dest, GlobalTagsClass) else []
        src_tags = src.tags if isinstance(src, GlobalTagsClass) else []
        return GlobalTagsClass(tags=_dedup_tags([*dest_tags, *src_tags]))
    dest_terms = dest.terms if isinstance(dest, GlossaryTermsClass) else []
    src_terms = src.terms if isinstance(src, GlossaryTermsClass) else []
    if isinstance(dest, GlossaryTermsClass):
        audit_stamp = dest.auditStamp
    else:
        assert isinstance(src, GlossaryTermsClass)
        audit_stamp = src.auditStamp
    return GlossaryTermsClass(
        terms=_dedup_terms([*dest_terms, *src_terms]), auditStamp=audit_stamp
    )


def _merge_structured_properties(
    dest: Optional[_Aspect], src: _Aspect
) -> Tuple[Optional[_Aspect], bool]:
    """Union structured-property assignments by ``propertyUrn``. Disjoint or
    identical assignments merge additively; the same property carrying different
    values on each side is a genuine conflict (returns ``(None, True)``) — we do
    not guess which value wins."""
    dest_props = dest.properties if isinstance(dest, StructuredPropertiesClass) else []
    src_props = src.properties if isinstance(src, StructuredPropertiesClass) else []
    merged: Dict[str, object] = {}
    for prop in [*dest_props, *src_props]:
        prev = merged.get(prop.propertyUrn)
        if prev is None:
            merged[prop.propertyUrn] = prop
        elif prev.values != prop.values:  # type: ignore[attr-defined]
            return None, True
    return StructuredPropertiesClass(properties=list(merged.values())), False  # type: ignore[arg-type]


def _merge_aspect(
    name: str, dest: Optional[_Aspect], src: _Aspect
) -> Tuple[Optional[_Aspect], bool]:
    """Combine a stranded field's aspect with whatever the destination already has.

    Returns ``(aspect_to_emit, conflict)``. Tags, terms, and structured properties
    merge additively (a value already propagated onto the correctly-cased field
    survives). Any other aspect that already differs on the destination is a
    conflict the caller must resolve rather than silently clobber.
    """
    if dest is None:
        return src, False
    if name in ("globalTags", "glossaryTerms"):
        return _union_association_aspect(name, dest, src), False
    if name == "structuredProperties":
        return _merge_structured_properties(dest, src)
    if dest == src:
        return src, False
    return None, True


def _merge_editable_field_info(
    existing: Optional[EditableSchemaFieldInfoClass],
    incoming: EditableSchemaFieldInfoClass,
    new_path: str,
) -> EditableSchemaFieldInfoClass:
    """Fold ``incoming`` onto ``new_path``, unioning with any existing entry there.

    An existing entry already at the destination wins on description (a
    deliberate edit on the correctly-cased field is not clobbered by a stale
    one); tags and terms are unioned and de-duplicated.
    """
    if existing is None:
        return EditableSchemaFieldInfoClass(
            fieldPath=new_path,
            description=incoming.description,
            globalTags=incoming.globalTags,
            glossaryTerms=incoming.glossaryTerms,
        )

    existing_tags = existing.globalTags.tags if existing.globalTags else []
    incoming_tags = incoming.globalTags.tags if incoming.globalTags else []
    merged_tags = _dedup_tags([*existing_tags, *incoming_tags])

    existing_terms = existing.glossaryTerms.terms if existing.glossaryTerms else []
    incoming_terms = incoming.glossaryTerms.terms if incoming.glossaryTerms else []
    merged_terms = _dedup_terms([*existing_terms, *incoming_terms])

    return EditableSchemaFieldInfoClass(
        fieldPath=new_path,
        description=existing.description or incoming.description,
        globalTags=GlobalTagsClass(tags=merged_tags) if merged_tags else None,
        glossaryTerms=(
            GlossaryTermsClass(
                terms=merged_terms,
                auditStamp=(
                    existing.glossaryTerms.auditStamp
                    if existing.glossaryTerms
                    else incoming.glossaryTerms.auditStamp  # type: ignore[union-attr]
                ),
            )
            if merged_terms
            else None
        ),
    )


def _has_editable_content(info: EditableSchemaFieldInfoClass) -> bool:
    return bool(
        info.description
        or (info.globalTags and info.globalTags.tags)
        or (info.glossaryTerms and info.glossaryTerms.terms)
    )


def _get_or_add_remap(
    remaps: Dict[Tuple[str, str], FieldRemap], old_path: str, new_path: str
) -> FieldRemap:
    key = (old_path, new_path)
    remap = remaps.get(key)
    if remap is None:
        remap = FieldRemap(old_path=old_path, new_path=new_path)
        remaps[key] = remap
    return remap


def _reconcile_schema_field_entities(
    graph: DataHubGraph,
    dataset_urn: str,
    reconciler: PathReconciler,
    remaps: Dict[Tuple[str, str], FieldRemap],
    result: DatasetReconcileResult,
    resolver: ClashResolver,
    *,
    dry_run: bool,
    delete_source: bool,
    include_soft_deleted: bool,
) -> None:
    for schema_field_urn in discover_schema_field_urns(
        graph, dataset_urn, include_soft_deleted
    ):
        try:
            old_path = SchemaFieldUrn.from_string(schema_field_urn).field_path
        except Exception:
            continue

        new_path, reason = reconciler.resolve(old_path)
        if new_path == old_path:
            continue

        aspects = _read_schema_field_aspects(graph, schema_field_urn)
        if not aspects:
            # Nothing user-authored here; a stale key-only field entity is not
            # worth moving or reporting.
            continue

        if new_path is None:
            candidates = reconciler.candidates(old_path)
            new_path = (
                resolver.choose_target(
                    old_path, candidates, f"aspects {sorted(aspects)}"
                )
                if len(candidates) > 1
                else None
            )
            if new_path is None:
                result.skipped.append(
                    f"schemaField '{old_path}': {reason}; left in place: {sorted(aspects)}"
                )
                continue

        new_schema_field_urn = make_schema_field_urn(dataset_urn, new_path)
        # Read the destination so we never clobber metadata already sitting on the
        # correctly-cased field (e.g. propagated by an automation/transformer).
        existing_dest = _read_schema_field_aspects(graph, new_schema_field_urn)
        carried: List[str] = []
        left_behind = False
        for name, aspect in aspects.items():
            to_emit, conflict = _merge_aspect(name, existing_dest.get(name), aspect)
            if conflict:
                if resolver.resolve_conflict(old_path, new_path, name):
                    to_emit = aspect  # operator chose the stranded value
                else:
                    result.skipped.append(
                        f"schemaField '{old_path}' -> '{new_path}': destination "
                        f"already has a different '{name}'; left source copy in "
                        "place for review"
                    )
                    left_behind = True
                    continue
            assert to_emit is not None
            if not dry_run:
                graph.emit_mcp(
                    MetadataChangeProposalWrapper(
                        entityUrn=new_schema_field_urn, aspect=to_emit
                    )
                )
            carried.append(name)
        # Only record a remap when something actually moved — a field whose sole
        # aspect hit the conflict guard carried nothing and is not a re-anchoring.
        if carried:
            _get_or_add_remap(remaps, old_path, new_path).schema_field_aspects.extend(
                carried
            )
        # Keep the source field if anything could not be carried over, so the
        # un-migrated aspect is not lost behind a soft delete.
        if delete_source and not dry_run and not left_behind:
            graph.soft_delete_entity(schema_field_urn)


def _reconcile_editable_schema_metadata(
    graph: DataHubGraph,
    dataset_urn: str,
    reconciler: PathReconciler,
    remaps: Dict[Tuple[str, str], FieldRemap],
    result: DatasetReconcileResult,
    resolver: ClashResolver,
    *,
    dry_run: bool,
) -> None:
    editable = graph.get_aspect(dataset_urn, EditableSchemaMetadataClass)
    if editable is None:
        return
    entries = editable.editableSchemaFieldInfo or []
    if not entries:
        return

    by_path: Dict[str, EditableSchemaFieldInfoClass] = {}
    changed = False

    # Entries already on a current path are kept as-is (their own destination).
    for info in entries:
        if info.fieldPath in reconciler.current_paths:
            by_path[info.fieldPath] = _merge_editable_field_info(
                by_path.get(info.fieldPath), info, info.fieldPath
            )

    # Stranded entries are re-anchored onto the matching current path.
    for info in entries:
        if info.fieldPath in reconciler.current_paths:
            continue
        new_path, reason = reconciler.resolve(info.fieldPath)
        if new_path is None:
            candidates = reconciler.candidates(info.fieldPath)
            new_path = (
                resolver.choose_target(info.fieldPath, candidates, "editable entry")
                if len(candidates) > 1
                else None
            )
            if new_path is None:
                if _has_editable_content(info):
                    result.skipped.append(
                        f"editableSchemaMetadata '{info.fieldPath}': {reason}"
                    )
                    # Keep it at its own path: the aspect below is rewritten
                    # wholesale from ``by_path`` once any *other* entry resolves,
                    # and an unresolved entry omitted here would be deleted
                    # rather than left in place for manual review.
                    by_path[info.fieldPath] = _merge_editable_field_info(
                        by_path.get(info.fieldPath), info, info.fieldPath
                    )
                continue
        by_path[new_path] = _merge_editable_field_info(
            by_path.get(new_path), info, new_path
        )
        changed = True
        _get_or_add_remap(remaps, info.fieldPath, new_path).editable = True

    if not changed:
        return
    result.editable_updated = True
    if not dry_run:
        graph.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=EditableSchemaMetadataClass(
                    editableSchemaFieldInfo=list(by_path.values())
                ),
            )
        )


def reconcile_dataset(
    graph: DataHubGraph,
    dataset_urn: str,
    *,
    dry_run: bool,
    delete_source: bool,
    include_soft_deleted: bool,
    resolver: Optional[ClashResolver] = None,
) -> DatasetReconcileResult:
    resolver = resolver or ClashResolver()
    result = DatasetReconcileResult(dataset_urn=dataset_urn)
    current_paths = _current_field_paths(graph, dataset_urn)
    if not current_paths:
        result.error = (
            "no schemaMetadata found — re-ingest the source with the new casing "
            "before reconciling, so there is a schema to reconcile against"
        )
        return result

    reconciler = PathReconciler.build(current_paths)
    remaps: Dict[Tuple[str, str], FieldRemap] = {}

    try:
        _reconcile_schema_field_entities(
            graph,
            dataset_urn,
            reconciler,
            remaps,
            result,
            resolver,
            dry_run=dry_run,
            delete_source=delete_source,
            include_soft_deleted=include_soft_deleted,
        )
        _reconcile_editable_schema_metadata(
            graph,
            dataset_urn,
            reconciler,
            remaps,
            result,
            resolver,
            dry_run=dry_run,
        )
    except Exception as e:
        log.warning(f"Failed to reconcile {dataset_urn}: {e}")
        result.error = str(e)

    result.remaps = list(remaps.values())
    return result


def discover_dataset_urns(
    graph: DataHubGraph,
    platform: Optional[str],
    platform_instance: Optional[str],
    env: Optional[str],
) -> List[str]:
    return list(
        graph.get_urns_by_filter(
            entity_types=[DATASET_ENTITY],
            platform=platform,
            platform_instance=platform_instance,
            env=env,
            status=RemovedStatusFilter.NOT_SOFT_DELETED,
        )
    )


@dataclass
class SchemaFieldCaseMigrationReport:
    dry_run: bool
    results: List[DatasetReconcileResult] = field(default_factory=list)

    def __repr__(self) -> str:
        prefix = "[Dry Run] " if self.dry_run else ""
        touched = [r for r in self.results if r.remaps or r.editable_updated]
        errored = [r for r in self.results if r.error is not None]
        needs_review = [r for r in self.results if r.skipped]
        total_fields = sum(len(r.remaps) for r in self.results)
        lines = [
            f"{prefix}Schema Field Case Migration Report:",
            "--------------",
            f"{prefix}Datasets scanned = {len(self.results)}",
            f"{prefix}Datasets changed = {len(touched)}",
            f"{prefix}Fields re-anchored = {total_fields}",
            f"{prefix}Datasets errored = {len(errored)}",
            f"{prefix}Datasets needing manual review = {len(needs_review)}",
        ]
        for r in touched:
            lines.append(f"{prefix}  {r.dataset_urn}")
            for remap in r.remaps:
                where = []
                if remap.schema_field_aspects:
                    where.append(f"schemaField[{','.join(remap.schema_field_aspects)}]")
                if remap.editable:
                    where.append("editableSchemaMetadata")
                lines.append(
                    f"{prefix}    '{remap.old_path}' -> '{remap.new_path}' "
                    f"({'; '.join(where)})"
                )
        for r in needs_review:
            lines.append(f"{prefix}  REVIEW {r.dataset_urn}")
            for note in r.skipped:
                lines.append(f"{prefix}    {note}")
        for r in errored:
            lines.append(f"{prefix}  ERROR {r.dataset_urn}: {r.error}")
        return "\n".join(lines)


def run_migration(
    graph: DataHubGraph,
    dataset_urns: Sequence[str],
    *,
    dry_run: bool,
    delete_source: bool,
    include_soft_deleted: bool,
    resolver: Optional[ClashResolver] = None,
) -> SchemaFieldCaseMigrationReport:
    resolver = resolver or ClashResolver()
    report = SchemaFieldCaseMigrationReport(dry_run=dry_run)
    for dataset_urn in dataset_urns:
        try:
            result = reconcile_dataset(
                graph,
                dataset_urn,
                dry_run=dry_run,
                delete_source=delete_source,
                include_soft_deleted=include_soft_deleted,
                resolver=resolver,
            )
        except Exception as e:
            log.warning(f"Unexpected error reconciling {dataset_urn}: {e}")
            result = DatasetReconcileResult(dataset_urn=dataset_urn, error=str(e))
        report.results.append(result)
    return report
