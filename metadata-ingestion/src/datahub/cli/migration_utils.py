"""Utilities for DataHub entity migration — aspect operations, merge logic, and URN rewriting."""

import logging
import uuid
from typing import Callable, Dict, Iterable, List, Optional, Protocol, Tuple, Union

import click
from avrogen.dict_wrapper import DictWrapper

from datahub.cli import cli_utils
from datahub.emitter.aspect import TIMESERIES_ASPECT_MAP
from datahub.emitter.mce_builder import (
    chart_urn_to_key,
    dashboard_urn_to_key,
    dataset_urn_to_key,
    make_chart_urn,
    make_dashboard_urn,
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.ingestion.graph.openapi import RelatedEntity, RelationshipDirection
from datahub.metadata.schema_classes import (
    ENTITY_TYPE_TO_ASPECT_NAMES,
    GlobalTagsClass,
    GlossaryTermsClass,
    OwnershipClass,
    SchemaMetadataClass,
    SystemMetadataClass,
    UpstreamLineageClass,
    _Aspect,
)
from datahub.metadata.urns import DataFlowUrn, DataJobUrn
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.utilities.str_enum import StrEnum
from datahub.utilities.urns.urn import guess_entity_type
from datahub.utilities.urns.urn_iter import (
    list_urns,
    lowercase_dataset_urn,
    transform_urns,
)

log = logging.getLogger(__name__)


# --- Constants ---

# Entity types supported by migration commands.
ALL_ENTITY_TYPES = ["dataset", "chart", "dashboard", "dataFlow", "dataJob"]

# Entity types that support env/origin filtering in ElasticSearch.
# Charts, dashboards, dataflows, and datajobs don't have env/origin fields.
ENV_ENTITY_TYPES = {"dataset"}


class ConflictStrategy(StrEnum):
    """How to handle aspect writes during migration.

    Aligns with DataHub's TransformerSemantics terminology:
    - OVERWRITE: Write aspects from source, replacing target values
    - PATCH: Merge with existing target values, only add new data
    - PROMPT: Ask user interactively for each conflict
    """

    OVERWRITE = "overwrite"
    PATCH = "patch"
    PROMPT = "prompt"


# Aspects that can be merged additively (lists of items, deduplicated)
ADDITIVE_ASPECTS = {
    "ownership",
    "globalTags",
    "glossaryTerms",
    "upstreamLineage",
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

# Aspects that are always overwritten during migration
ALWAYS_OVERWRITE_ASPECTS = {
    "dataPlatformInstance",
    "container",
    "status",
    "containerProperties",
    "containerKey",
}

# Aspects the migration must NOT clone verbatim from source to the new URN:
#  - dataPlatformInstance: re-emitted explicitly with the *new* instance
#  - incidentsSummary: a platform-computed rollup; a verbatim copy would carry a
#    stale summary and is re-derived by the platform anyway
#  - browsePaths/browsePathsV2: encode the *old* instance path; recomputed on read
# Key aspects (datasetKey, containerKey, ...) are intentionally absent from
# ENTITY_TYPE_TO_ASPECT_NAMES: their content is bound to the URN and GMS derives
# them on any write, so they are neither cloned nor listed here.
SYSTEM_MANAGED_ASPECTS = {
    "dataPlatformInstance",
    "incidentsSummary",
    "browsePaths",
    "browsePathsV2",
}

_SCHEMA_FIELD_PREFIX = "urn:li:schemaField:"


def get_migratable_aspect_names(entity_type: str) -> List[str]:
    """Non-timeseries, user-migratable aspects for an entity type.

    Sourced from the generated entity registry (``ENTITY_TYPE_TO_ASPECT_NAMES``)
    instead of a hand-maintained list, so newly-modeled aspects
    (``structuredProperties``, ``forms``, ``documentation``, ...) are picked up
    automatically. Timeseries aspects are excluded (append-only; migrated, if at
    all, via a separate path), as are system-managed aspects the migration
    re-derives.
    """
    return [
        a
        for a in ENTITY_TYPE_TO_ASPECT_NAMES.get(entity_type, [])
        if a not in TIMESERIES_ASPECT_MAP and a not in SYSTEM_MANAGED_ASPECTS
    ]


def make_batch_urn_rewriter(url_map: Dict[str, str]) -> Callable[[str], str]:
    """Rewrite references to any entity migrated in the same batch.

    Intended for use with ``urn_iter.transform_urns``, which walks an aspect
    using its ``@Relationship``/Urn field markers and applies this function to
    every URN it finds. Rewrites both entity URNs present in ``url_map`` and
    ``schemaField`` URNs that embed them (column-level lineage), so
    FineGrainedLineage is covered without any per-aspect special-casing.

    Unlike a self-only rewriter, this rewrites cross-references *between*
    co-migrated entities (e.g. a downstream's ``upstreamLineage`` pointer to a
    sibling being migrated in the same run) deterministically at clone time —
    independent of the eventually-consistent relationship index, which is stale
    for siblings created earlier in the same batch. References to entities
    outside the batch are left untouched.
    """

    def rewrite(urn: str) -> str:
        mapped = url_map.get(urn)
        if mapped is not None:
            return mapped
        # schemaField URNs are 'urn:li:schemaField:(<datasetUrn>,<field>)'.
        # Rewrite when the embedded dataset is any entity being migrated.
        if urn.startswith(f"{_SCHEMA_FIELD_PREFIX}("):
            for old_urn, new_urn in url_map.items():
                if urn.startswith(f"{_SCHEMA_FIELD_PREFIX}({old_urn},"):
                    return urn.replace(old_urn, new_urn, 1)
        return urn

    return rewrite


def make_self_urn_rewriter(old_urn: str, new_urn: str) -> Callable[[str], str]:
    """Rewrite references to a single migrated entity URN.

    A one-entry :func:`make_batch_urn_rewriter`; kept for the incoming-reference
    path, where an external referrer is rewritten against just the one URN being
    migrated.
    """
    return make_batch_urn_rewriter({old_urn: new_urn})


# --- URN converters (pluggable transforms for `migrate transform`) ---


class UrnConverter(Protocol):
    """A pluggable URN transform driving a generic (non-instance) migration."""

    name: str

    def should_convert(self, urn: str) -> bool:
        """Whether this URN needs converting (skip no-ops)."""
        ...

    def convert_urn(self, urn: str) -> str:
        """Return the new URN for a source URN."""
        ...


class LowercaseConverter:
    """Lowercase the name segment of dataset URNs (e.g. mixed-case -> lowercase)."""

    name = "lowercase"

    def should_convert(self, urn: str) -> bool:
        return urn != lowercase_dataset_urn(urn)

    def convert_urn(self, urn: str) -> str:
        return lowercase_dataset_urn(urn)


CONVERTERS: Dict[str, Callable[[], UrnConverter]] = {
    LowercaseConverter.name: LowercaseConverter,
}


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
    """
    aspect_map = cli_utils.get_aspects_for_entity(
        graph._session,
        graph.config.server,
        target_urn,
        aspects=[],
        typed=True,
    )
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


def clone_aspect(
    src_urn: str,
    aspect_names: List[str],
    dst_urn: str,
    run_id: str = str(uuid.uuid4()),
    graph: Optional[DataHubGraph] = None,
) -> Iterable[MetadataChangeProposalWrapper]:
    client = graph or get_default_graph(ClientMode.CLI)
    aspect_map = cli_utils.get_aspects_for_entity(
        client._session,
        client.config.server,
        entity_urn=src_urn,
        aspects=aspect_names,
        typed=True,
    )

    if aspect_names is not None:
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


def get_incoming_relationships(urn: str) -> Iterable[RelatedEntity]:
    client = get_default_graph(ClientMode.CLI)
    yield from client.get_related_entities(
        entity_urn=urn,
        relationship_types=[
            "DownstreamOf",
            "Consumes",
            "Produces",
            "ForeignKeyToDataset",
            "DerivedFrom",
            "IsPartOf",
        ],
        direction=RelationshipDirection.INCOMING,
    )


# --- Merge logic (for instance2instance with overlapping entities) ---


def merge_additive_aspects(
    src_aspects: Dict[str, DictWrapper],
    dst_urn: str,
    graph: DataHubGraph,
    dry_run: bool,
) -> int:
    """Merge additive aspects from source into existing target via Patch API.

    Returns the number of patch MCPs emitted.
    """
    patch_builder = DatasetPatchBuilder(dst_urn)

    if "ownership" in src_aspects:
        aspect = src_aspects["ownership"]
        assert isinstance(aspect, OwnershipClass)
        for owner in aspect.owners or []:
            patch_builder.add_owner(owner)

    if "globalTags" in src_aspects:
        aspect = src_aspects["globalTags"]
        assert isinstance(aspect, GlobalTagsClass)
        for tag in aspect.tags or []:
            patch_builder.add_tag(tag)

    if "glossaryTerms" in src_aspects:
        aspect = src_aspects["glossaryTerms"]
        assert isinstance(aspect, GlossaryTermsClass)
        for term in aspect.terms or []:
            patch_builder.add_term(term)

    if "upstreamLineage" in src_aspects:
        aspect = src_aspects["upstreamLineage"]
        assert isinstance(aspect, UpstreamLineageClass)
        for upstream in aspect.upstreams or []:
            patch_builder.add_upstream_lineage(upstream)
        for fine_grained in aspect.fineGrainedLineages or []:
            patch_builder.add_fine_grained_lineage(fine_grained)

    mcps = patch_builder.build()
    for mcp in mcps:
        if not dry_run:
            graph.emit(mcp)
    return len(mcps)


def should_overwrite_scalar(
    field_name: str,
    src_value: str,
    dst_value: str,
    src_urn: str,
    dst_urn: str,
    on_conflict: ConflictStrategy,
) -> bool:
    """Decide whether to overwrite a scalar field based on conflict strategy."""
    if on_conflict == ConflictStrategy.OVERWRITE:
        return True
    elif on_conflict == ConflictStrategy.PATCH:
        log.info(
            f"Conflict on {field_name} for {dst_urn} — keeping target "
            f"(strategy: {on_conflict.value})"
        )
        return False
    elif on_conflict == ConflictStrategy.PROMPT:
        click.echo(f"\nConflict on '{field_name}' for {dst_urn}")
        src_preview = src_value[:80] + ("..." if len(src_value) > 80 else "")
        dst_preview = dst_value[:80] + ("..." if len(dst_value) > 80 else "")
        click.echo(f'  Source ({src_urn}): "{src_preview}"')
        click.echo(f'  Target ({dst_urn}): "{dst_preview}"')
        choice = click.prompt(
            "  Keep [s]ource or [t]arget?",
            type=click.Choice(["s", "t"]),
            default="t",
        )
        return choice == "s"
    return False


def should_overwrite_non_additive(
    aspect_name: str,
    src_aspect: DictWrapper,
    dst_aspect: DictWrapper,
    src_urn: str,
    dst_urn: str,
    on_conflict: ConflictStrategy,
) -> bool:
    """Decide whether to overwrite a non-additive aspect based on conflict strategy."""
    if src_aspect.to_obj() == dst_aspect.to_obj():
        return True  # No conflict, same data

    if on_conflict == ConflictStrategy.OVERWRITE:
        return True
    elif on_conflict == ConflictStrategy.PATCH:
        log.info(
            f"Conflict on {aspect_name} for {dst_urn} — keeping target "
            f"(strategy: {on_conflict.value})"
        )
        return False
    elif on_conflict == ConflictStrategy.PROMPT:
        click.echo(f"\nConflict on aspect '{aspect_name}' for {dst_urn}")
        click.echo(
            f"  Source ({src_urn}): {_summarize_aspect(aspect_name, src_aspect)}"
        )
        click.echo(
            f"  Target ({dst_urn}): {_summarize_aspect(aspect_name, dst_aspect)}"
        )
        choice = click.prompt(
            "  Keep [s]ource or [t]arget?",
            type=click.Choice(["s", "t"]),
            default="t",
        )
        return choice == "s"
    return False


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
) -> tuple[int, int]:
    """Merge aspects with both additive and scalar fields.

    Returns (aspects_merged, conflicts_skipped).
    """
    patch_builder = DatasetPatchBuilder(dst_urn)
    has_patches = False
    conflicts_skipped = 0

    for aspect_name in MIXED_ASPECTS:
        if aspect_name not in src_aspects:
            continue
        src_aspect = src_aspects[aspect_name]

        # Fetch target aspect once for both customProperties and description
        dst_aspect_map = cli_utils.get_aspects_for_entity(
            graph._session,
            graph.config.server,
            dst_urn,
            aspects=[aspect_name],
            typed=True,
        )
        dst_asp = dst_aspect_map.get(aspect_name)

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
            elif not dst_desc:
                editable = aspect_name == "editableDatasetProperties"
                patch_builder.set_description(src_desc, editable=editable)
                has_patches = True

    aspects_merged = 0
    if has_patches:
        mcps = patch_builder.build()
        for mcp in mcps:
            if not dry_run:
                graph.emit(mcp)
            aspects_merged += 1

    return aspects_merged, conflicts_skipped


def _overwrite_entity(
    src_urn: str,
    dst_urn: str,
    graph: DataHubGraph,
    dry_run: bool,
) -> tuple[int, int]:
    """Overwrite target entity with all aspects from source (no merge logic).

    Used as fallback for entity types that don't support Patch-based merge.
    Returns (aspects_written, 0) — no conflicts since we always overwrite.
    """
    aspects_written = 0
    for mcp in clone_aspect(
        src_urn,
        aspect_names=get_migratable_aspect_names(guess_entity_type(dst_urn)),
        dst_urn=dst_urn,
        graph=graph,
    ):
        if not dry_run:
            graph.emit_mcp(mcp)
        aspects_written += 1
    return aspects_written, 0


def merge_entity(
    src_urn: str,
    dst_urn: str,
    on_conflict: ConflictStrategy,
    graph: DataHubGraph,
    dry_run: bool,
    url_map: Optional[Dict[str, str]] = None,
) -> tuple[int, int]:
    """Merge all aspects from source entity into existing target.

    Only dataset entities support full merge via the Patch API. For other entity
    types (chart, dashboard, dataFlow, dataJob), this falls back to overwrite.

    ``url_map`` is the full old->new map for the batch; when given, merged aspects
    also rewrite references to co-migrated siblings (not just self-references).

    Returns (aspects_merged, conflicts_skipped).
    """
    # Only datasets support Patch-based merge. Other entity types fall back to
    # overwrite because there's no ChartPatchBuilder/DashboardPatchBuilder etc.
    entity_type = guess_entity_type(dst_urn)
    if entity_type != "dataset":
        log.info(
            f"Entity type '{entity_type}' does not support merge — "
            f"falling back to overwrite for {dst_urn}"
        )
        return _overwrite_entity(src_urn, dst_urn, graph, dry_run)

    src_aspect_map = cli_utils.get_aspects_for_entity(
        graph._session,
        graph.config.server,
        src_urn,
        aspects=get_migratable_aspect_names(entity_type),
        typed=True,
    )

    # Rewrite the source's references (e.g. fineGrainedLineages schemaField URNs
    # and upstreamLineage pointers) before merging, so merged aspects don't carry
    # old URNs — mirroring the clone path. The batch map also moves references to
    # co-migrated siblings.
    rewrite_urn = make_batch_urn_rewriter(url_map or {src_urn: dst_urn})
    for aspect in src_aspect_map.values():
        if isinstance(aspect, DictWrapper):
            transform_urns(aspect, rewrite_urn)

    total_merged = 0
    total_skipped = 0

    # Additive aspects via Patch API
    additive: Dict[str, DictWrapper] = {}
    for k, v in src_aspect_map.items():
        if k in ADDITIVE_ASPECTS and isinstance(v, DictWrapper):
            additive[k] = v
    if additive:
        total_merged += merge_additive_aspects(additive, dst_urn, graph, dry_run)

    # Mixed aspects (customProperties + description)
    mixed: Dict[str, DictWrapper] = {}
    for k, v in src_aspect_map.items():
        if k in MIXED_ASPECTS and isinstance(v, DictWrapper):
            mixed[k] = v
    if mixed:
        merged, skipped = merge_mixed_aspects(
            mixed, dst_urn, src_urn, graph, on_conflict, dry_run
        )
        total_merged += merged
        total_skipped += skipped

    # Non-additive aspects with conflict strategy
    for aspect_name in NON_ADDITIVE_ASPECTS:
        if aspect_name not in src_aspect_map:
            continue
        src_aspect = src_aspect_map[aspect_name]
        if not isinstance(src_aspect, DictWrapper):
            continue
        dst_aspect_map = cli_utils.get_aspects_for_entity(
            graph._session,
            graph.config.server,
            dst_urn,
            aspects=[aspect_name],
            typed=True,
        )
        if aspect_name in dst_aspect_map and isinstance(
            dst_aspect_map[aspect_name], DictWrapper
        ):
            dst_aspect: DictWrapper = dst_aspect_map[aspect_name]  # type: ignore[assignment]
            assert isinstance(src_aspect, DictWrapper)
            if should_overwrite_non_additive(
                aspect_name, src_aspect, dst_aspect, src_urn, dst_urn, on_conflict
            ):
                assert isinstance(src_aspect, DictWrapper)
                mcp = MetadataChangeProposalWrapper(
                    entityUrn=dst_urn, aspect=src_aspect
                )
                if not dry_run:
                    graph.emit_mcp(mcp)
                total_merged += 1
            else:
                total_skipped += 1
        else:
            assert isinstance(src_aspect, DictWrapper)
            mcp = MetadataChangeProposalWrapper(entityUrn=dst_urn, aspect=src_aspect)
            if not dry_run:
                graph.emit_mcp(mcp)
            total_merged += 1

    # Always overwrite migration-specific aspects
    for aspect_name in ALWAYS_OVERWRITE_ASPECTS:
        src_val = src_aspect_map.get(aspect_name)
        if src_val is not None and isinstance(src_val, DictWrapper):
            mcp = MetadataChangeProposalWrapper(entityUrn=dst_urn, aspect=src_val)
            if not dry_run:
                graph.emit_mcp(mcp)
            total_merged += 1

    # Default bucket: any registry aspect not explicitly classified above.
    merged, skipped = _merge_default_aspects(
        src_aspect_map, dst_urn, src_urn, graph, on_conflict, dry_run
    )
    total_merged += merged
    total_skipped += skipped

    return total_merged, total_skipped


def _merge_default_aspects(
    src_aspect_map: Dict[str, Union[dict, _Aspect]],
    dst_urn: str,
    src_urn: str,
    graph: DataHubGraph,
    on_conflict: ConflictStrategy,
    dry_run: bool,
) -> Tuple[int, int]:
    """Merge source aspects not handled by any explicit classification bucket.

    Because the aspect list is now sourced dynamically from the entity registry,
    newly-modeled aspects would otherwise be silently dropped in merge mode.
    They are treated conflict-aware, like the non-additive bucket, so nothing is
    lost. Returns (aspects_merged, conflicts_skipped).
    """
    classified = (
        ADDITIVE_ASPECTS
        | MIXED_ASPECTS
        | NON_ADDITIVE_ASPECTS
        | ALWAYS_OVERWRITE_ASPECTS
    )
    merged = 0
    skipped = 0
    for aspect_name, src_aspect in src_aspect_map.items():
        if aspect_name in classified or not isinstance(src_aspect, DictWrapper):
            continue
        dst_aspect_map = cli_utils.get_aspects_for_entity(
            graph._session,
            graph.config.server,
            dst_urn,
            aspects=[aspect_name],
            typed=True,
        )
        dst_aspect = dst_aspect_map.get(aspect_name)
        if isinstance(dst_aspect, DictWrapper) and not should_overwrite_non_additive(
            aspect_name, src_aspect, dst_aspect, src_urn, dst_urn, on_conflict
        ):
            skipped += 1
            continue
        mcp = MetadataChangeProposalWrapper(entityUrn=dst_urn, aspect=src_aspect)
        if not dry_run:
            graph.emit_mcp(mcp)
        merged += 1
    return merged, skipped


# --- URN rewriting ---


def replace_instance_prefix(name: str, old_instance: str, new_instance: str) -> str:
    """Replace the old platform instance prefix in an entity name with the new one.

    Entity names with a platform instance are formatted as '{instance}.{name}'.
    Raises ValueError if the name doesn't start with the expected old instance prefix,
    as this indicates a data quality issue (entity doesn't belong to the old instance).
    """
    prefix = f"{old_instance}."
    if name.startswith(prefix):
        return f"{new_instance}.{name[len(prefix) :]}"
    raise ValueError(
        f"Entity name '{name}' does not start with expected instance prefix "
        f"'{old_instance}.'. This entity may not belong to the source instance. "
        f"Use --skip-on-error to skip such entities."
    )


# --- Generic URN builder ---

# Entity type → (key_parser, name_extractor, urn_constructor)
_UrnSpec = Tuple[Callable, Callable, Callable]

_ENTITY_URN_SPECS: Dict[str, _UrnSpec] = {
    "dataset": (
        dataset_urn_to_key,
        lambda key: key.name,
        # `name` already carries the new instance prefix (added by
        # _rewrite_name), so platform_instance must be None here — otherwise the
        # instance segment is prepended twice (e.g. new_inst.new_inst.db.table).
        lambda key, name, _inst: make_dataset_urn_with_platform_instance(
            platform=key.platform[len("urn:li:dataPlatform:") :],
            name=name,
            platform_instance=None,
            env=str(key.origin),
        ),
    ),
    "chart": (
        chart_urn_to_key,
        lambda key: key.chartId,
        lambda key, name, _inst: make_chart_urn(platform=key.dashboardTool, name=name),
    ),
    "dashboard": (
        dashboard_urn_to_key,
        lambda key: key.dashboardId,
        lambda key, name, _inst: make_dashboard_urn(
            platform=key.dashboardTool, name=name
        ),
    ),
}


def make_urn_builder(
    entity_type: str,
    new_instance: str,
    old_instance: Optional[str] = None,
) -> Callable[[str], str]:
    """Build a URN rewriter for any supported entity type.

    When *old_instance* is ``None`` (platform-to-instance migration) the
    new instance is simply prepended.  When *old_instance* is given
    (instance-to-instance) the old prefix is replaced.
    """

    def _rewrite_name(name: str) -> str:
        if old_instance is None:
            return f"{new_instance}.{name}"
        return replace_instance_prefix(name, old_instance, new_instance)

    if entity_type == "dataFlow":

        def _dataflow_urn(src_urn: str) -> str:
            parsed = DataFlowUrn.from_string(src_urn)
            return make_data_flow_urn(
                orchestrator=parsed.orchestrator,
                flow_id=_rewrite_name(parsed.flow_id),
                cluster=parsed.cluster,
            )

        return _dataflow_urn

    if entity_type == "dataJob":

        def _datajob_urn(src_urn: str) -> str:
            parsed = DataJobUrn.from_string(src_urn)
            flow = DataFlowUrn.from_string(parsed.flow)
            new_flow_urn = make_data_flow_urn(
                orchestrator=flow.orchestrator,
                flow_id=_rewrite_name(flow.flow_id),
                cluster=flow.cluster,
            )
            return make_data_job_urn_with_flow(new_flow_urn, parsed.job_id)

        return _datajob_urn

    spec = _ENTITY_URN_SPECS.get(entity_type)
    if spec is None:
        raise ValueError(f"Unsupported entity type for URN rewriting: {entity_type}")

    key_parser, name_extractor, urn_constructor = spec

    def _entity_urn(src_urn: str) -> str:
        key = key_parser(src_urn)
        assert key
        new_name = _rewrite_name(name_extractor(key))
        return urn_constructor(key, new_name, new_instance)

    return _entity_urn


# --- Backward-compatible convenience wrappers ---


def make_p2i_dataset_urn(instance: str) -> Callable[[str], str]:
    return make_urn_builder("dataset", new_instance=instance)


def make_p2i_chart_urn(instance: str) -> Callable[[str], str]:
    return make_urn_builder("chart", new_instance=instance)


def make_p2i_dashboard_urn(instance: str) -> Callable[[str], str]:
    return make_urn_builder("dashboard", new_instance=instance)


def make_p2i_dataflow_urn(instance: str) -> Callable[[str], str]:
    return make_urn_builder("dataFlow", new_instance=instance)


def make_p2i_datajob_urn(instance: str) -> Callable[[str], str]:
    return make_urn_builder("dataJob", new_instance=instance)


def make_i2i_dataset_urn(old_instance: str, new_instance: str) -> Callable[[str], str]:
    return make_urn_builder(
        "dataset", new_instance=new_instance, old_instance=old_instance
    )


def make_i2i_chart_urn(old_instance: str, new_instance: str) -> Callable[[str], str]:
    return make_urn_builder(
        "chart", new_instance=new_instance, old_instance=old_instance
    )


def make_i2i_dashboard_urn(
    old_instance: str, new_instance: str
) -> Callable[[str], str]:
    return make_urn_builder(
        "dashboard", new_instance=new_instance, old_instance=old_instance
    )


def make_i2i_dataflow_urn(old_instance: str, new_instance: str) -> Callable[[str], str]:
    return make_urn_builder(
        "dataFlow", new_instance=new_instance, old_instance=old_instance
    )


def make_i2i_datajob_urn(old_instance: str, new_instance: str) -> Callable[[str], str]:
    return make_urn_builder(
        "dataJob", new_instance=new_instance, old_instance=old_instance
    )
