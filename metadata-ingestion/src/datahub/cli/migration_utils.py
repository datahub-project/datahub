"""Utilities for DataHub entity migration — aspect operations, merge logic, and URN rewriting."""

import logging
import uuid
from typing import Callable, Dict, Iterable, List, Optional, Tuple

import click
from avrogen.dict_wrapper import DictWrapper

from datahub.cli import cli_utils
from datahub.emitter.mce_builder import (
    Aspect,
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
    ChartInfoClass,
    ContainerClass,
    DashboardInfoClass,
    DataJobInputOutputClass,
    DataProcessInfoClass,
    GlobalTagsClass,
    GlossaryTermsClass,
    MLFeaturePropertiesClass,
    MLPrimaryKeyPropertiesClass,
    OwnershipClass,
    SchemaMetadataClass,
    SystemMetadataClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import DataFlowUrn, DataJobUrn
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.utilities.str_enum import StrEnum
from datahub.utilities.urns.urn import guess_entity_type

log = logging.getLogger(__name__)


# --- Constants ---

# Entity types supported by migration commands.
ALL_ENTITY_TYPES = ["dataset", "chart", "dashboard", "dataFlow", "dataJob"]

# Entity types that support env/origin filtering in ElasticSearch.
# Charts, dashboards, dataflows, and datajobs don't have env/origin fields.
ENV_ENTITY_TYPES = {"dataset"}

# TODO: Make this dynamic based on the real aspect class map.
all_aspects = [
    "schemaMetadata",
    "datasetProperties",
    "viewProperties",
    "subTypes",
    "editableDatasetProperties",
    "ownership",
    "datasetDeprecation",
    "institutionalMemory",
    "editableSchemaMetadata",
    "globalTags",
    "glossaryTerms",
    "upstreamLineage",
    "datasetUpstreamLineage",
    "status",
    "containerProperties",
    "dataPlatformInstance",
    "containerKey",
    "container",
    "domains",
    "editableContainerProperties",
    # Non-dataset entity aspects
    "chartInfo",
    "dashboardInfo",
    "dataFlowInfo",
    "dataJobInfo",
    "dataJobInputOutput",
    "dataProcessInfo",
]


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


# --- Relationship-to-aspect mapping ---


def get_aspect_name_from_relationship(relationship_type: str, entity_type: str) -> str:
    aspect_map = {
        "Produces": {"datajob": "dataJobInputOutput"},
        "Consumes": {
            "chart": "chartInfo",
            "dashboard": "dashboardInfo",
            "datajob": "dataJobInputOutput",
            "dataProcess": "dataProcessInfo",
        },
        "DownstreamOf": {"dataset": "upstreamLineage"},
        "ForeignKeyToDataset": {"dataset": "schemaMetadata"},
        "DerivedFrom": {
            "mlfeature": "mlFeatureProperties",
            "mlprimarykey": "mlPrimaryKeyProperties",
        },
        "IsPartOf": {
            "container": "container",
            "dataset": "container",
            "dashboard": "container",
            "chart": "container",
        },
    }

    if (
        relationship_type in aspect_map
        and entity_type.lower() in aspect_map[relationship_type]
    ):
        return aspect_map[relationship_type][entity_type.lower()]

    raise Exception(
        f"Unable to map aspect name from relationship_type {relationship_type} "
        f"and entity_type {entity_type}"
    )


# --- URN list modifiers (rewrite URN references inside aspects) ---


class UrnListModifier:
    @staticmethod
    def dataJobInputOutput_modifier(
        aspect: DictWrapper,
        relationship_type: str,
        old_urn: str,
        new_urn: str,
    ) -> DictWrapper:
        assert isinstance(aspect, DataJobInputOutputClass)
        if relationship_type == "Produces":
            aspect.outputDatasets = [
                new_urn if d == old_urn else d for d in aspect.outputDatasets
            ]
            return aspect
        if relationship_type == "Consumes":
            aspect.inputDatasets = [
                new_urn if d == old_urn else d for d in aspect.inputDatasets
            ]
            return aspect
        raise Exception(
            f"Unable to map aspect_name: dataJobInputOutput, "
            f"relationship_type {relationship_type}"
        )

    @staticmethod
    def chartInfo_modifier(
        aspect: DictWrapper,
        relationship_type: str,
        old_urn: str,
        new_urn: str,
    ) -> DictWrapper:
        assert isinstance(aspect, ChartInfoClass)
        aspect.inputs = [new_urn if x == old_urn else x for x in aspect.inputs or []]
        return aspect

    @staticmethod
    def dashboardInfo_modifier(
        aspect: DictWrapper,
        relationship_type: str,
        old_urn: str,
        new_urn: str,
    ) -> DictWrapper:
        assert isinstance(aspect, DashboardInfoClass)
        aspect.datasets = [
            new_urn if x == old_urn else x for x in aspect.datasets or []
        ]
        if aspect.datasetEdges is not None:
            for edge in aspect.datasetEdges:
                if edge.destinationUrn == old_urn:
                    edge.destinationUrn = new_urn
        return aspect

    @staticmethod
    def dataProcessInfo_modifier(
        aspect: DictWrapper,
        relationship_type: str,
        old_urn: str,
        new_urn: str,
    ) -> DictWrapper:
        assert isinstance(aspect, DataProcessInfoClass)
        if aspect.inputs is not None:
            aspect.inputs = [new_urn if x == old_urn else x for x in aspect.inputs]
        return aspect

    @staticmethod
    def upstreamLineage_modifier(
        aspect: DictWrapper,
        relationship_type: str,
        old_urn: str,
        new_urn: str,
    ) -> DictWrapper:
        assert isinstance(aspect, UpstreamLineageClass)
        for upstream in aspect.upstreams:
            if upstream.dataset == old_urn:
                upstream.dataset = new_urn
        return aspect

    @staticmethod
    def schemaMetadata_modifier(
        aspect: DictWrapper,
        relationship_type: str,
        old_urn: str,
        new_urn: str,
    ) -> DictWrapper:
        assert isinstance(aspect, SchemaMetadataClass)
        for foreignKey in aspect.foreignKeys or []:
            foreignKey.foreignFields = [
                f.replace(old_urn, new_urn) for f in foreignKey.foreignFields
            ]
            if foreignKey.foreignDataset == old_urn:
                foreignKey.foreignDataset = new_urn
        return aspect

    @staticmethod
    def mlFeatureProperties_modifier(
        aspect: DictWrapper,
        relationship_type: str,
        old_urn: str,
        new_urn: str,
    ) -> DictWrapper:
        assert isinstance(aspect, MLFeaturePropertiesClass)
        aspect.sources = [new_urn if s == old_urn else s for s in aspect.sources or []]
        return aspect

    @staticmethod
    def mlPrimaryKeyProperties_modifier(
        aspect: DictWrapper,
        relationship_type: str,
        old_urn: str,
        new_urn: str,
    ) -> DictWrapper:
        assert isinstance(aspect, MLPrimaryKeyPropertiesClass)
        aspect.sources = [new_urn if s == old_urn else s for s in aspect.sources]
        return aspect

    @staticmethod
    def container_modifier(
        aspect: DictWrapper,
        relationship_type: str,
        old_urn: str,
        new_urn: str,
    ) -> DictWrapper:
        assert isinstance(aspect, ContainerClass)
        aspect.container = new_urn if aspect.container == old_urn else aspect.container
        return aspect


def modify_urn_list_for_aspect(
    aspect_name: str,
    aspect: Aspect,
    relationship_type: str,
    old_urn: str,
    new_urn: str,
) -> Aspect:
    if hasattr(UrnListModifier, f"{aspect_name}_modifier"):
        modifier = getattr(UrnListModifier, f"{aspect_name}_modifier")
        return modifier(
            aspect=aspect,
            relationship_type=relationship_type,
            old_urn=old_urn,
            new_urn=new_urn,
        )
    raise Exception(
        f"Unable to map aspect_name: {aspect_name}, "
        f"relationship_type {relationship_type}"
    )


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
        aspect_names=all_aspects,
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
) -> tuple[int, int]:
    """Merge all aspects from source entity into existing target.

    Only dataset entities support full merge via the Patch API. For other entity
    types (chart, dashboard, dataFlow, dataJob), this falls back to overwrite.

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
        aspects=all_aspects,
        typed=True,
    )

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

    return total_merged, total_skipped


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
        lambda key, name, inst: make_dataset_urn_with_platform_instance(
            platform=key.platform[len("urn:li:dataPlatform:") :],
            name=name,
            platform_instance=inst,
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
