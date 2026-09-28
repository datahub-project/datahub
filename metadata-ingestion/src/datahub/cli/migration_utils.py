"""Utilities for DataHub entity migration — aspect operations, merge logic, and URN rewriting."""

import logging
import uuid
from typing import Callable, Dict, Iterable, List, Optional, Set, TypeVar, Union

import click
from avrogen.dict_wrapper import DictWrapper

from datahub.cli import cli_utils
from datahub.emitter.aspect import TIMESERIES_ASPECT_MAP
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.ingestion.graph.openapi import RelatedEntity
from datahub.metadata.schema_classes import (
    ENTITY_TYPE_TO_ASPECT_NAMES,
    GlobalTagsClass,
    GlossaryTermsClass,
    MetadataChangeProposalClass,
    OwnershipClass,
    SchemaMetadataClass,
    StructuredPropertiesClass,
    SystemMetadataClass,
    UpstreamLineageClass,
    _Aspect,
)
from datahub.migration.models import ConflictStrategy, MergeResult
from datahub.specific.aspect_helpers.ownership import HasOwnershipPatch
from datahub.specific.aspect_helpers.structured_properties import (
    HasStructuredPropertiesPatch,
)
from datahub.specific.aspect_helpers.tags import HasTagsPatch
from datahub.specific.aspect_helpers.terms import HasTermsPatch
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.utilities.urns.urn import guess_entity_type
from datahub.utilities.urns.urn_iter import list_urns, transform_urns

log = logging.getLogger(__name__)

_T = TypeVar("_T")


# --- Constants ---

# Relationship types whose target aspects reference an entity by urn (lineage,
# foreign keys, containment). Used by commands that *report* — without repointing —
# inbound references to a migrated urn (e.g. `migrate snowflake-semantic-views
# --report-inbound-refs`). The generic engine repoints inbound references across
# all indexed relationship types via `get_incoming_relationships`, so it does not
# use this list.
INBOUND_REFERENCE_RELATIONSHIP_TYPES: List[str] = [
    "DownstreamOf",
    "Consumes",
    "Produces",
    "ForeignKeyToDataset",
    "DerivedFrom",
    "IsPartOf",
]

# Entity types supported by migration commands.
ALL_ENTITY_TYPES = ["dataset", "chart", "dashboard", "dataFlow", "dataJob"]

# Entity types that support env/origin filtering in ElasticSearch.
# Charts, dashboards, dataflows, and datajobs don't have env/origin fields.
ENV_ENTITY_TYPES = {"dataset"}

# Non-dataset types whose lineage and description live in a single non-additive
# *Info aspect (chartInfo, dashboardInfo, dataFlowInfo, dataJobInputOutput,
# dataProductProperties) that the entity-agnostic patch builder cannot union. An
# additive merge would keep the target's copy and strand the source's lineage
# before the source is deleted, so these keep the full overwrite under every
# conflict strategy. Other non-dataset types (schemaField, glossaryTerm,
# container, …) only carry union-able or safely-copyable aspects.
NON_ADDITIVE_MERGE_ENTITY_TYPES = frozenset(
    {"chart", "dashboard", "dataFlow", "dataJob", "dataProduct"}
)


# Aspects the dataset path unions rather than overwrites, so a target's existing
# values survive the merge. ownership/tags/terms/structuredProperties union by key
# on any entity; upstreamLineage unions too but only through DatasetPatchBuilder's
# dataset-only lineage template, so it is dataset-path-only (non-dataset carriers
# fall to conflict-aware copy — see _GENERIC_UNIONABLE_ASPECTS).
ADDITIVE_ASPECTS = {
    "ownership",
    "globalTags",
    "glossaryTerms",
    "structuredProperties",
    "upstreamLineage",
}

# The additive aspects the entity-agnostic patch builder can union. upstreamLineage
# is additive but only unionable through DatasetPatchBuilder's dataset-only lineage
# template, so on non-dataset entities it falls into the conflict-aware complement
# in _merge_generic_entity rather than being unioned here.
_GENERIC_UNIONABLE_ASPECTS = {
    "ownership",
    "globalTags",
    "glossaryTerms",
    "structuredProperties",
}

# Aspects where conflicts need resolution (non-list, scalar values)
NON_ADDITIVE_ASPECTS = {
    "schemaMetadata",
    "editableSchemaMetadata",
    "viewProperties",
}

# Aspects with mixed additive/non-additive fields
MIXED_ASPECTS = {
    "datasetProperties",
    "editableDatasetProperties",
}

# Aspects that are always overwritten during merge (target already exists).
# Note: dataPlatformInstance and key aspects (e.g. containerKey) are intentionally
# absent — they never reach src_aspect_map because get_migratable_aspect_names
# excludes system-managed aspects and key aspects are not in the entity registry.
ALWAYS_OVERWRITE_ASPECTS = {
    "containerProperties",
}

# Aspects excluded from the merge path (target exists) but still cloned when
# creating a new target.
#
# - ``status``: carries the soft-delete flag — overwriting a live target's
#   status with a soft-deleted source would silently remove the target.
#   The target's own status is authoritative.
#
# - ``container``: a reference to the parent container entity. For
#   ``urns-mapping`` containers are not migrated, so the source's container
#   URN may point at an entity about to be deleted. For ``p2i``/``i2i`` the
#   container migration step runs separately and its incoming-reference
#   rewriting updates any entity that references the old container URN.
MERGE_EXCLUDED_ASPECTS = {
    "status",
    "container",
}

# --- How each aspect is treated by a migration ---
#
# Every migratable aspect falls into exactly one of three buckets:
#
#  1. COPIED — cloned verbatim from the source to the new URN. This is the default
#     for everything NOT listed below (ownership, tags, schema, lineage, ...).
#
#  2. REGENERATED BY THE MIGRATION ITSELF — must NOT be copied (its value is bound
#     to the *old* instance), and GMS does not derive it, so each command that
#     changes the instance re-emits a fresh one for the *new* instance (entity
#     engine: ``engine._emit_target_instance``; containers: ``_migrate_containers``).
MIGRATION_REGENERATED_ASPECTS = {
    "dataPlatformInstance",
}
#
#  3. REGENERATED BY GMS — must NOT be copied; the platform recomputes them for the
#     new URN/instance on its own, so the migration neither clones nor re-emits them.
GMS_REGENERATED_ASPECTS = {
    "incidentsSummary",  # platform-computed rollup; a verbatim copy would be stale
    "browsePaths",  # encodes the old instance path; recomputed on read
    "browsePathsV2",
}
#
# Both buckets 2 and 3 are excluded from the clone. (Key aspects — datasetKey,
# containerKey, ... — are a further GMS-derived case: GMS derives them from the new
# URN's guid and they are not even present in ENTITY_TYPE_TO_ASPECT_NAMES, so they
# are neither listed nor cloned here.)
ASPECTS_EXCLUDED_FROM_CLONE = MIGRATION_REGENERATED_ASPECTS | GMS_REGENERATED_ASPECTS

_SCHEMA_FIELD_PREFIX = "urn:li:schemaField:"


def get_migratable_aspect_names(entity_type: str) -> List[str]:
    """Non-timeseries aspects that are COPIED verbatim to the migrated URN.

    Sourced from the generated entity registry (``ENTITY_TYPE_TO_ASPECT_NAMES``)
    instead of a hand-maintained list, so newly-modeled aspects
    (``structuredProperties``, ``forms``, ``documentation``, ...) are picked up
    automatically. Excludes timeseries aspects (append-only; migrated, if at all,
    via a separate path) and the aspects in ``ASPECTS_EXCLUDED_FROM_CLONE`` — those
    are regenerated either by the migration itself (``dataPlatformInstance``) or by
    GMS (browse paths, incidents summary), never copied.

    TODO: source this from the live server via
    ``DataHubGraph.get_entity_aspect_specs()`` (see
    ``ingestion/graph/client.py``) instead of the codegen constant, so the aspect
    list reflects the running server's registry — including custom aspects — rather
    than whatever the CLI happened to be built against.
    """
    return [
        a
        for a in ENTITY_TYPE_TO_ASPECT_NAMES.get(entity_type, [])
        if a not in TIMESERIES_ASPECT_MAP and a not in ASPECTS_EXCLUDED_FROM_CLONE
    ]


def require_migratable_aspect_names(entity_type: str) -> List[str]:
    """Like get_migratable_aspect_names, but refuses an entity type the registry
    doesn't model.

    An empty list is dangerous for every write path: cli_utils.get_aspects_for_entity
    treats aspects=[] as "no filter" and fetches *everything*, while clone_aspect's
    loop over an empty list yields nothing — so a migration would write zero aspects,
    delete the source, and report a clean success. Fail loudly instead.
    """
    aspect_names = get_migratable_aspect_names(entity_type)
    if not aspect_names:
        raise ValueError(
            f"Refusing to migrate entity type '{entity_type}': no migratable aspects "
            f"are known for it. The CLI's entity registry is likely older than this "
            f"entity type; upgrade acryl-datahub before migrating it."
        )
    return aspect_names


def make_self_urn_rewriter(old_urn: str, new_urn: str) -> Callable[[str], str]:
    """Rewrite references to a single migrated entity URN.

    Intended for use with ``urn_iter.transform_urns``, which walks an aspect
    using its ``@Relationship``/Urn field markers and applies this function to
    every URN it finds. Rewrites both the entity URN itself and ``schemaField``
    URNs that embed it (column-level lineage), so FineGrainedLineage is covered
    without any per-aspect special-casing. References to *other* entities are
    left untouched.
    """

    def rewrite(urn: str) -> str:
        if urn == old_urn:
            return new_urn
        # schemaField URNs are 'urn:li:schemaField:(<datasetUrn>,<field>)'.
        # Rewrite only when the embedded dataset is the one being migrated.
        if urn.startswith(f"{_SCHEMA_FIELD_PREFIX}({old_urn},"):
            return urn.replace(old_urn, new_urn, 1)
        return urn

    return rewrite


def make_batch_urn_rewriter(urn_map: Dict[str, str]) -> Callable[[str], str]:
    """Rewrite references to ANY migrated entity URN in the batch.

    Like ``make_self_urn_rewriter`` but aware of all source→target mappings in
    the current migration run, so cross-pair references (e.g. entity A's lineage
    pointing to entity B, where both are being migrated) are rewritten at clone
    time regardless of processing order.
    """

    def rewrite(urn: str) -> str:
        if urn in urn_map:
            return urn_map[urn]
        if urn.startswith(_SCHEMA_FIELD_PREFIX):
            for old, new in urn_map.items():
                if urn.startswith(f"{_SCHEMA_FIELD_PREFIX}({old},"):
                    return urn.replace(old, new, 1)
        return urn

    return rewrite


def rewrite_incoming_references(
    graph: DataHubGraph,
    target_urn: str,
    rewrite_urn: Callable[[str], str],
) -> List[MetadataChangeProposalWrapper]:
    """Rewrite references to a migrated URN across all aspects of one entity.

    Given an entity that references the migrated URN, fetch every (non-timeseries)
    aspect it has and rewrite each via ``transform_urns`` — driven by the aspect's
    relationship/Urn field markers. Unlike a hand-maintained relationship-to-aspect
    map, this rewrites the reference wherever it appears, in any aspect. Returns an
    MCP for each aspect that actually changed (the caller decides how to emit).

    TODO: this fetches all aspects for every referencing entity, one entity at a
    time. For a heavily-referenced entity (many downstreams) that's a full-entity
    read per reference and can get slow on large migrations — worth batching.
    """
    aspect_map = _get_aspects(graph, target_urn, [])
    changed: List[MetadataChangeProposalWrapper] = []
    for aspect in aspect_map.values():
        if not isinstance(aspect, DictWrapper):
            continue
        if not any(rewrite_urn(u) != u for u in list_urns(aspect)):
            continue
        transform_urns(aspect, rewrite_urn)
        changed.append(
            MetadataChangeProposalWrapper(entityUrn=target_urn, aspect=aspect)
        )
    return changed


# --- Aspect cloning and relationship fetching ---


def _get_aspects(
    graph: DataHubGraph, urn: str, aspects: List[str]
) -> Dict[str, Union[dict, _Aspect]]:
    return cli_utils.get_aspects_for_entity(
        graph._session, graph.config.server, urn, aspects=aspects, typed=True
    )


def clone_aspect(
    src_urn: str,
    aspect_names: List[str],
    dst_urn: str,
    run_id: Optional[str] = None,
    graph: Optional[DataHubGraph] = None,
) -> Iterable[MetadataChangeProposalWrapper]:
    # Generate per call rather than as a default arg: a default is evaluated once
    # at import time and would share a single run_id across every migration.
    run_id = run_id or str(uuid.uuid4())
    client = graph or get_default_graph(ClientMode.CLI)
    aspect_map = _get_aspects(client, src_urn, aspect_names)

    for a in aspect_names:
        if a in aspect_map:
            aspect_value = aspect_map[a]
            assert isinstance(aspect_value, DictWrapper)
            new_mcp = MetadataChangeProposalWrapper(
                entityUrn=dst_urn,
                aspect=aspect_value,
                systemMetadata=SystemMetadataClass(runId=run_id),
            )
            log.debug(f"Emitting mcp for {dst_urn} aspect {a}")
            yield new_mcp
        else:
            log.debug(f"did not find aspect {a} in response, continuing...")


def get_incoming_relationships(
    urn: str, graph: Optional[DataHubGraph] = None
) -> Iterable[RelatedEntity]:
    """Entities that hold an indexed reference to ``urn``.

    Queries the relationship graph across ALL relationship types rather than a
    hand-picked subset, so references such as ``Asserts`` (assertions → dataset)
    are included and get rewritten during migration instead of being left
    dangling. Edges are stored ``(source)-[type]->(destination)``; an entity that
    references ``urn`` has it as the *destination*, so we filter on
    ``destination_urns`` and return each edge's *source*.

    Only ``@Relationship``-annotated fields are graph-indexed; references held in
    plain URN fields (e.g. structured-property URN values) are not discovered here.
    """
    client = graph or get_default_graph(ClientMode.CLI)
    seen: Set[str] = set()
    scroll_id: Optional[str] = None
    while True:
        result = client.scroll_relationships(
            destination_urns=[urn],
            relationship_types=None,  # all indexed relationship types
            include_soft_delete=True,
            scroll_id=scroll_id,
        )
        for rel in result.relationships:
            if rel.source_urn in seen:
                continue
            seen.add(rel.source_urn)
            yield RelatedEntity(
                urn=rel.source_urn, relationship_type=rel.relationship_type
            )
        scroll_id = result.scroll_id
        if not scroll_id:
            break


# --- Merge logic (for instance2instance with overlapping entities) ---


class _AdditivePatchBuilder(
    HasOwnershipPatch,
    HasTagsPatch,
    HasTermsPatch,
    HasStructuredPropertiesPatch,
    MetadataPatchProposal,
):
    """Only the aspect mixins that set ``array_primary_keys`` on their patches.

    GMS routes any patch carrying non-empty ``arrayPrimaryKeys`` through
    ``applyGenericPatch``, which unions by those keys without needing a registered
    per-aspect template — so this is safe on any entity type (schemaField, chart,
    dashboard, ...), unlike DatasetPatchBuilder which also carries dataset-only
    schema/lineage/customProperties surface. A fifth mixin is only valid here if it
    likewise sets ``array_primary_keys``."""

    def build(self) -> List[MetadataChangeProposalClass]:
        # Enforce the mixin invariant at merge time, not GMS emit time. A mixin that
        # emitted a plain JSON patch (no arrayPrimaryKeys) would be silently dropped
        # by GMS on a non-dataset entity — surface it here as a hard failure instead.
        for aspect_name, patches in self.patches.items():
            for patch in patches:
                if not patch.array_primary_keys:
                    raise TypeError(
                        f"_AdditivePatchBuilder produced a non-generic patch for "
                        f"'{aspect_name}'; every mixin here must set array_primary_keys."
                    )
        return super().build()


def _add_each(
    dst_urn: str, kind: str, items: Optional[List[_T]], add: Callable[[_T], object]
) -> None:
    # Isolate per item: one malformed owner/tag/term/property must not void the
    # entity's other additive aspects (this codebase's don't-fail-the-batch rule).
    item_list = list(items or [])
    if not item_list:
        return
    failed = 0
    last_error: Optional[Exception] = None
    for item in item_list:
        try:
            add(item)
        except Exception as e:
            failed += 1
            last_error = e
            log.warning(f"Skipping a {kind} on {dst_urn} during additive merge: {e}")
    # Every item of a present aspect failing is diagnostic of a systemic bug (e.g. a
    # schema mismatch in the patch mixin), not bad data. Abort the pair so the caller
    # doesn't delete the source and report success with the aspect silently missing.
    if failed == len(item_list):
        raise RuntimeError(
            f"All {failed} {kind}(s) failed to merge onto {dst_urn}; aborting the "
            f"pair rather than dropping the aspect and deleting the source"
        ) from last_error


def _apply_union_patches(
    patch_builder: Union[DatasetPatchBuilder, _AdditivePatchBuilder],
    src_aspects: Dict[str, DictWrapper],
    dst_urn: str,
) -> None:
    if "ownership" in src_aspects:
        aspect = src_aspects["ownership"]
        assert isinstance(aspect, OwnershipClass)
        _add_each(dst_urn, "owner", aspect.owners, patch_builder.add_owner)

    if "globalTags" in src_aspects:
        aspect = src_aspects["globalTags"]
        assert isinstance(aspect, GlobalTagsClass)
        _add_each(dst_urn, "tag", aspect.tags, patch_builder.add_tag)

    if "glossaryTerms" in src_aspects:
        aspect = src_aspects["glossaryTerms"]
        assert isinstance(aspect, GlossaryTermsClass)
        _add_each(dst_urn, "term", aspect.terms, patch_builder.add_term)

    if "structuredProperties" in src_aspects:
        aspect = src_aspects["structuredProperties"]
        assert isinstance(aspect, StructuredPropertiesClass)
        _add_each(
            dst_urn,
            "structured property",
            aspect.properties,
            patch_builder.set_structured_property_manual,
        )


def merge_additive_aspects(
    src_aspects: Dict[str, DictWrapper],
    dst_urn: str,
    graph: DataHubGraph,
    dry_run: bool,
) -> List[str]:
    """Merge additive aspects from source into existing target via Patch API.

    Returns the aspect names that actually produced a write (JSON patches, plus a
    lineage UPSERT when the target has no ``upstreamLineage`` aspect yet). An
    empty source aspect (e.g. ``GlobalTagsClass(tags=[])``) yields no MCP and is
    therefore not reported as merged — keeping the count and the name list in sync.

    **Known limitation (multi-downstream FGL):** the Patch API keys
    fine-grained lineage entries on ``(transformOp, downstream, query)`` and
    requires exactly one downstream per entry. FGL entries with 0 or 2+
    downstreams will raise ``TypeError`` from the patch builder. This is a
    pre-existing constraint of :pymethod:`DatasetPatchBuilder.add_fine_grained_lineage`,
    not specific to migration. In practice field-level lineage is almost always
    N upstreams → 1 downstream, so this is unlikely to trigger. Use
    ``--on-conflict overwrite`` instead of ``patch`` if your data contains
    multi-downstream FGL entries.
    """
    patch_builder = DatasetPatchBuilder(dst_urn)
    emitted: List[str] = []

    _apply_union_patches(patch_builder, src_aspects, dst_urn)

    if "upstreamLineage" in src_aspects:
        aspect = src_aspects["upstreamLineage"]
        assert isinstance(aspect, UpstreamLineageClass)
        has_lineage = bool(aspect.upstreams) or bool(aspect.fineGrainedLineages)
        # DatasetPatchBuilder lineage PATCH is a GMS no-op when the target has
        # no upstreamLineage: it is a plain JSON Patch (no GenericJsonPatch /
        # arrayPrimaryKeys), and GMS never writes. Ownership, globalTags, and
        # glossaryTerms PATCH through GenericJsonPatch + aspect templates, which
        # create the aspect from a default. UPSERT is lineage-only. Empty source
        # lineage is a no-op; PATCH is only for unioning into existing lineage.
        if has_lineage:
            existing_lineage = graph.get_aspect(dst_urn, UpstreamLineageClass)
            if existing_lineage is None:
                if not dry_run:
                    graph.emit_mcp(
                        MetadataChangeProposalWrapper(
                            entityUrn=dst_urn,
                            aspect=aspect,
                        )
                    )
                emitted.append("upstreamLineage")
            else:
                # Unlike the per-item-isolated additive loops above, lineage edges
                # are NOT isolated per item: a malformed fine-grained entry
                # (0 or >1 downstreams) raises and aborts the aspect. Deliberate —
                # a lineage edge is structural, so silently dropping one would
                # corrupt the graph; surface it and let the operator use overwrite.
                for upstream in aspect.upstreams or []:
                    patch_builder.add_upstream_lineage(upstream)
                for fine_grained in aspect.fineGrainedLineages or []:
                    patch_builder.add_fine_grained_lineage(fine_grained)

    for mcp in patch_builder.build():
        if not dry_run:
            graph.emit(mcp)
        if mcp.aspectName:
            emitted.append(mcp.aspectName)
    return emitted


def _resolve_conflict(
    label: str,
    src_preview: str,
    dst_preview: str,
    src_urn: str,
    dst_urn: str,
    on_conflict: ConflictStrategy,
) -> bool:
    # True = take source (overwrite target); False = keep target.
    if on_conflict == ConflictStrategy.OVERWRITE:
        return True
    if on_conflict == ConflictStrategy.PATCH:
        log.info(
            f"Conflict on {label} for {dst_urn} — keeping target "
            f"(strategy: {on_conflict.value})"
        )
        return False
    if on_conflict == ConflictStrategy.PROMPT:
        click.echo(f"\nConflict on {label} for {dst_urn}")
        click.echo(f"  Source ({src_urn}): {src_preview}")
        click.echo(f"  Target ({dst_urn}): {dst_preview}")
        choice = click.prompt(
            "  Keep [s]ource or [t]arget?",
            type=click.Choice(["s", "t"]),
            default="t",
        )
        return choice == "s"
    return False


def _preview(value: str) -> str:
    return f'"{value[:80]}{"..." if len(value) > 80 else ""}"'


def should_overwrite_scalar(
    field_name: str,
    src_value: str,
    dst_value: str,
    src_urn: str,
    dst_urn: str,
    on_conflict: ConflictStrategy,
) -> bool:
    return _resolve_conflict(
        f"'{field_name}'",
        _preview(src_value),
        _preview(dst_value),
        src_urn,
        dst_urn,
        on_conflict,
    )


def should_overwrite_non_additive(
    aspect_name: str,
    src_aspect: DictWrapper,
    dst_aspect: DictWrapper,
    src_urn: str,
    dst_urn: str,
    on_conflict: ConflictStrategy,
) -> bool:
    if src_aspect.to_obj() == dst_aspect.to_obj():
        return True  # identical data, no conflict
    return _resolve_conflict(
        f"aspect '{aspect_name}'",
        _summarize_aspect(aspect_name, src_aspect),
        _summarize_aspect(aspect_name, dst_aspect),
        src_urn,
        dst_urn,
        on_conflict,
    )


def _summarize_aspect(aspect_name: str, aspect: DictWrapper) -> str:
    """Return a short human-readable summary of an aspect for conflict prompts."""
    if aspect_name in ("datasetProperties", "editableDatasetProperties"):
        desc = getattr(aspect, "description", None)
        if desc:
            return f'description="{desc[:80]}{"..." if len(desc) > 80 else ""}"'
    if aspect_name == "schemaMetadata":
        assert isinstance(aspect, SchemaMetadataClass)
        return f"{len(aspect.fields or [])} fields"
    return f"{aspect_name} (use --dry-run to inspect)"


def merge_mixed_aspects(
    src_aspects: Dict[str, DictWrapper],
    dst_urn: str,
    src_urn: str,
    graph: DataHubGraph,
    on_conflict: ConflictStrategy,
    dry_run: bool,
) -> MergeResult:
    patch_builder = DatasetPatchBuilder(dst_urn)
    has_patches = False
    conflicts_skipped = 0
    # An aspect can both write (e.g. customProperties) and skip (e.g. a conflicting
    # description) in one pass; track which aspects were skipped so the report lists
    # each aspect in exactly one bucket rather than in both.
    skipped_names: Set[str] = set()

    for aspect_name in MIXED_ASPECTS:
        if aspect_name not in src_aspects:
            continue
        src_aspect = src_aspects[aspect_name]

        # Fetch target aspect once for both customProperties and description
        dst_asp = _get_aspects(graph, dst_urn, [aspect_name]).get(aspect_name)

        src_custom_props: Dict[str, str] = {}
        if hasattr(src_aspect, "customProperties") and src_aspect.customProperties:
            src_custom_props = dict(src_aspect.customProperties)

        if src_custom_props:
            dst_custom_props: Dict[str, str] = {}
            if dst_asp is not None:
                if hasattr(dst_asp, "customProperties") and dst_asp.customProperties:
                    dst_custom_props = dict(dst_asp.customProperties)

            for key, value in src_custom_props.items():
                if key in dst_custom_props and dst_custom_props[key] != value:
                    if should_overwrite_scalar(
                        f"{aspect_name}.customProperties.{key}",
                        value,
                        dst_custom_props[key],
                        src_urn,
                        dst_urn,
                        on_conflict,
                    ):
                        patch_builder.add_custom_property(key, value)
                        has_patches = True
                    else:
                        conflicts_skipped += 1
                        skipped_names.add(aspect_name)
                else:
                    patch_builder.add_custom_property(key, value)
                    has_patches = True

        src_desc = getattr(src_aspect, "description", None)
        if src_desc:
            dst_desc = None
            if dst_asp is not None:
                dst_desc = getattr(dst_asp, "description", None)

            if dst_desc and dst_desc != src_desc:
                if should_overwrite_scalar(
                    f"{aspect_name}.description",
                    src_desc,
                    dst_desc,
                    src_urn,
                    dst_urn,
                    on_conflict,
                ):
                    editable = aspect_name == "editableDatasetProperties"
                    patch_builder.set_description(src_desc, editable=editable)
                    has_patches = True
                else:
                    conflicts_skipped += 1
                    skipped_names.add(aspect_name)
            elif not dst_desc:
                editable = aspect_name == "editableDatasetProperties"
                patch_builder.set_description(src_desc, editable=editable)
                has_patches = True

    emitted: List[str] = []
    if has_patches:
        for mcp in patch_builder.build():
            if not dry_run:
                graph.emit(mcp)
            if mcp.aspectName:
                emitted.append(mcp.aspectName)

    return MergeResult(
        merged=len(emitted),
        skipped=conflicts_skipped,
        merged_aspects=emitted,
        # An aspect that produced any write is reported as merged, not skipped.
        skipped_aspects=[a for a in skipped_names if a not in emitted],
    )


def _fetch_and_rewrite_source_aspects(
    src_urn: str,
    dst_urn: str,
    graph: DataHubGraph,
    rewrite_urn: Optional[Callable[[str], str]],
) -> Dict[str, Union[dict, _Aspect]]:
    """Fetch the source's migratable aspects and repoint their self-references at
    the target, so merged aspects never carry the old URN.

    Shared prologue for the dataset and generic merge paths. Raises if the registry
    models no aspects for the entity type (see require_migratable_aspect_names).
    """
    aspect_names = require_migratable_aspect_names(guess_entity_type(dst_urn))
    src_aspect_map = _get_aspects(graph, src_urn, aspect_names)
    if rewrite_urn is None:
        rewrite_urn = make_self_urn_rewriter(src_urn, dst_urn)
    for aspect in src_aspect_map.values():
        if isinstance(aspect, DictWrapper):
            transform_urns(aspect, rewrite_urn)
    return src_aspect_map


def _reseat_always_overwrite_aspects(
    src_aspect_map: Dict[str, Union[dict, _Aspect]],
    dst_urn: str,
    graph: DataHubGraph,
    dry_run: bool,
) -> MergeResult:
    """Emit ALWAYS_OVERWRITE_ASPECTS (e.g. containerProperties) verbatim.

    These carry instance-bound data (customProperties that moved with the GUID) and
    must land on the target rather than be skipped as a conflict.
    """
    result = MergeResult(merged=0, skipped=0)
    for aspect_name in ALWAYS_OVERWRITE_ASPECTS:
        src_val = src_aspect_map.get(aspect_name)
        if isinstance(src_val, DictWrapper):
            if not dry_run:
                graph.emit_mcp(
                    MetadataChangeProposalWrapper(entityUrn=dst_urn, aspect=src_val)
                )
            result.merged += 1
            result.merged_aspects.append(aspect_name)
    return result


def _overwrite_entity(
    src_urn: str,
    dst_urn: str,
    graph: DataHubGraph,
    dry_run: bool,
    rewrite_urn: Optional[Callable[[str], str]] = None,
) -> MergeResult:
    """Overwrite target entity with all aspects from source (no merge logic).

    Used as fallback for entity types that don't support Patch-based merge.
    Skips ``MERGE_EXCLUDED_ASPECTS`` (e.g. ``status``) — the target's own
    soft-delete state is authoritative when the target already exists.
    No conflicts are reported since we always overwrite.
    """
    if rewrite_urn is None:
        rewrite_urn = make_self_urn_rewriter(src_urn, dst_urn)
    aspect_names = [
        a
        for a in require_migratable_aspect_names(guess_entity_type(dst_urn))
        if a not in MERGE_EXCLUDED_ASPECTS
    ]
    aspects_written = 0
    written_names: List[str] = []
    for mcp in clone_aspect(
        src_urn,
        aspect_names=aspect_names,
        dst_urn=dst_urn,
        graph=graph,
    ):
        if mcp.aspect is not None:
            transform_urns(mcp.aspect, rewrite_urn)
        if not dry_run:
            graph.emit_mcp(mcp)
        aspects_written += 1
        if mcp.aspectName:
            written_names.append(mcp.aspectName)
    return MergeResult(merged=aspects_written, skipped=0, merged_aspects=written_names)


def _merge_additive_aspects_generic(
    src_aspects: Dict[str, DictWrapper],
    dst_urn: str,
    graph: DataHubGraph,
    dry_run: bool,
) -> List[str]:
    # Non-dataset counterpart to merge_additive_aspects: unions only the
    # entity-agnostic aspects (no dataset-only lineage template). Returns the
    # aspect names that actually produced a patch — an empty GlobalTagsClass(tags=[])
    # yields no MCP, so it must not be reported as merged.
    patch_builder = _AdditivePatchBuilder(dst_urn)
    _apply_union_patches(patch_builder, src_aspects, dst_urn)
    emitted: List[str] = []
    for mcp in patch_builder.build():
        if not dry_run:
            graph.emit(mcp)
        if mcp.aspectName:
            emitted.append(mcp.aspectName)
    return emitted


def _merge_generic_entity(
    src_urn: str,
    dst_urn: str,
    on_conflict: ConflictStrategy,
    graph: DataHubGraph,
    dry_run: bool,
    rewrite_urn: Optional[Callable[[str], str]] = None,
) -> MergeResult:
    # Additive union of the union-able aspects + conflict-aware handling of
    # everything else, so an existing non-dataset target's curated metadata is
    # never clobbered.
    src_aspect_map = _fetch_and_rewrite_source_aspects(
        src_urn, dst_urn, graph, rewrite_urn
    )
    result = MergeResult(merged=0, skipped=0)

    additive: Dict[str, DictWrapper] = {
        k: v
        for k, v in src_aspect_map.items()
        if k in _GENERIC_UNIONABLE_ASPECTS and isinstance(v, DictWrapper)
    }
    if additive:
        merged_names = _merge_additive_aspects_generic(
            additive, dst_urn, graph, dry_run
        )
        result = result + MergeResult(len(merged_names), 0, merged_names, [])

    # _merge_default_aspects skips ALWAYS_OVERWRITE_ASPECTS, so a container reached
    # via urns-mapping would otherwise silently drop containerProperties (whose
    # customProperties moved with the GUID). Reseat them here, like the dataset path.
    result = result + _reseat_always_overwrite_aspects(
        src_aspect_map, dst_urn, graph, dry_run
    )

    # Everything not handled above is copied conflict-aware. Taking the complement
    # (rather than enumerating buckets) means a NON_ADDITIVE/MIXED aspect that a
    # non-dataset entity happens to carry — e.g. schemaMetadata on glossaryTerm, or
    # upstreamLineage on a semanticModel — and any newly-modeled aspect can never
    # silently fall through and be lost when the source is deleted.
    handled_elsewhere = (
        _GENERIC_UNIONABLE_ASPECTS | ALWAYS_OVERWRITE_ASPECTS | MERGE_EXCLUDED_ASPECTS
    )
    remaining = [name for name in src_aspect_map if name not in handled_elsewhere]
    result = result + _copy_aspects_conflict_aware(
        remaining, src_aspect_map, dst_urn, src_urn, graph, on_conflict, dry_run
    )
    return result


def _copy_aspects_conflict_aware(
    aspect_names: Iterable[str],
    src_aspect_map: Dict[str, Union[dict, _Aspect]],
    dst_urn: str,
    src_urn: str,
    graph: DataHubGraph,
    on_conflict: ConflictStrategy,
    dry_run: bool,
) -> MergeResult:
    """Copy each named source aspect onto the target, honoring the conflict strategy.

    On a value conflict the target is kept unless the strategy says otherwise.
    """
    result = MergeResult(merged=0, skipped=0)
    for aspect_name in aspect_names:
        src_aspect = src_aspect_map.get(aspect_name)
        if not isinstance(src_aspect, DictWrapper):
            continue
        dst_aspect = _get_aspects(graph, dst_urn, [aspect_name]).get(aspect_name)
        if isinstance(dst_aspect, DictWrapper) and not should_overwrite_non_additive(
            aspect_name, src_aspect, dst_aspect, src_urn, dst_urn, on_conflict
        ):
            result.skipped += 1
            result.skipped_aspects.append(aspect_name)
            continue
        if not dry_run:
            graph.emit_mcp(
                MetadataChangeProposalWrapper(entityUrn=dst_urn, aspect=src_aspect)
            )
        result.merged += 1
        result.merged_aspects.append(aspect_name)
    return result


def _merge_non_additive_aspects(
    src_aspect_map: Dict[str, Union[dict, _Aspect]],
    dst_urn: str,
    src_urn: str,
    graph: DataHubGraph,
    on_conflict: ConflictStrategy,
    dry_run: bool,
) -> MergeResult:
    return _copy_aspects_conflict_aware(
        NON_ADDITIVE_ASPECTS,
        src_aspect_map,
        dst_urn,
        src_urn,
        graph,
        on_conflict,
        dry_run,
    )


def merge_entity(
    src_urn: str,
    dst_urn: str,
    on_conflict: ConflictStrategy,
    graph: DataHubGraph,
    dry_run: bool,
    rewrite_urn: Optional[Callable[[str], str]] = None,
) -> MergeResult:
    """Merge all aspects from source entity into existing target.

    Datasets run the full Patch pipeline. Non-datasets whose lineage lives in a
    non-unionable *Info aspect (NON_ADDITIVE_MERGE_ENTITY_TYPES) are fully
    overwritten; the rest get an additive union of the union-able aspects plus
    conflict-aware handling of the remainder. An explicit OVERWRITE always fully
    replaces the target.

    When ``rewrite_urn`` is provided (batch migration), it is used instead of a
    single-pair rewriter so that cross-pair references are rewritten correctly.
    """
    # PRESERVE leaves the existing target entirely untouched — no additive merge,
    # no scalar overwrite. Short-circuit here, before any merge work: the additive
    # path never consults on_conflict, so it would otherwise still mutate the target.
    if on_conflict == ConflictStrategy.PRESERVE:
        return MergeResult(merged=0, skipped=1, skipped_aspects=["*"])

    # NON_ADDITIVE_MERGE_ENTITY_TYPES (and any explicit OVERWRITE) fully overwrite
    # the target; other non-datasets union the union-able aspects and copy the
    # remainder conflict-aware.
    entity_type = guess_entity_type(dst_urn)
    if entity_type != "dataset":
        if (
            on_conflict == ConflictStrategy.OVERWRITE
            or entity_type in NON_ADDITIVE_MERGE_ENTITY_TYPES
        ):
            return _overwrite_entity(src_urn, dst_urn, graph, dry_run, rewrite_urn)
        return _merge_generic_entity(
            src_urn, dst_urn, on_conflict, graph, dry_run, rewrite_urn
        )

    src_aspect_map = _fetch_and_rewrite_source_aspects(
        src_urn, dst_urn, graph, rewrite_urn
    )
    result = MergeResult(merged=0, skipped=0)

    # Additive aspects via Patch API.
    additive: Dict[str, DictWrapper] = {
        k: v
        for k, v in src_aspect_map.items()
        if k in ADDITIVE_ASPECTS and isinstance(v, DictWrapper)
    }
    if additive:
        merged_names = merge_additive_aspects(additive, dst_urn, graph, dry_run)
        result = result + MergeResult(len(merged_names), 0, merged_names, [])

    # Mixed aspects (customProperties + description).
    mixed: Dict[str, DictWrapper] = {
        k: v
        for k, v in src_aspect_map.items()
        if k in MIXED_ASPECTS and isinstance(v, DictWrapper)
    }
    if mixed:
        result = result + merge_mixed_aspects(
            mixed, dst_urn, src_urn, graph, on_conflict, dry_run
        )

    result = result + _merge_non_additive_aspects(
        src_aspect_map, dst_urn, src_urn, graph, on_conflict, dry_run
    )
    result = result + _reseat_always_overwrite_aspects(
        src_aspect_map, dst_urn, graph, dry_run
    )
    # Default bucket: any registry aspect not explicitly classified above.
    result = result + _merge_default_aspects(
        src_aspect_map, dst_urn, src_urn, graph, on_conflict, dry_run
    )
    return result


def _merge_default_aspects(
    src_aspect_map: Dict[str, Union[dict, _Aspect]],
    dst_urn: str,
    src_urn: str,
    graph: DataHubGraph,
    on_conflict: ConflictStrategy,
    dry_run: bool,
) -> MergeResult:
    """Merge source aspects not handled by any explicit classification bucket.

    Because the aspect list is now sourced dynamically from the entity registry,
    newly-modeled aspects would otherwise be silently dropped in merge mode.
    They are treated conflict-aware, like the non-additive bucket, so nothing is
    lost.
    """
    classified = (
        ADDITIVE_ASPECTS
        | MIXED_ASPECTS
        | NON_ADDITIVE_ASPECTS
        | ALWAYS_OVERWRITE_ASPECTS
        | MERGE_EXCLUDED_ASPECTS
    )
    unclassified = [name for name in src_aspect_map if name not in classified]
    return _copy_aspects_conflict_aware(
        unclassified, src_aspect_map, dst_urn, src_urn, graph, on_conflict, dry_run
    )
