"""Bidirectional governance migration for dbt semantic models.

Copies entity-level governance and column tags/terms between legacy dbt
"Semantic Model" dataset URNs and the URNs the first-class path emits under
``emit_semantic_model_entities``. Typical order: migrate governance, then
re-ingest with the flag on.

Unlike the Snowflake migration, both sides here are **datasets**. The dbt
``semanticModel`` is project-scoped and shared by every semantic model in the
project, and ``migrate_entity`` is last-write-wins, so copying N legacy
datasets onto it would silently keep only the last one's governance. The
destination is therefore the Semantic Model Dataset, and the project-level
``semanticModel`` entity is not a governance destination at all.

URN helpers mirror ``DbtSemanticModelMapper`` but use ``datahub.metadata.urns``
so this CLI does not require the dbt connector extra.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

from datahub.cli.semantic_model_migration_common import (
    EntityMigrationResult,
    MigrationDirection,
    MigrationReport,
    collect_dataset_field_governance,
    describe_exception,
    filter_by_subtype,
    is_soft_deleted,
    merge_field_governance_into_editable_schema,
    migrate_entity,
    resolve_field_path,
    run_migration_loop,
    status_filter,
)
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import SearchFilterRule
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.metadata.urns import DatasetUrn

log = logging.getLogger(__name__)

DBT_PLATFORM = "dbt"
LEGACY_SUBTYPE = DatasetSubTypes.SEMANTIC_MODEL
SEMANTIC_MODEL_DATASET_SUBTYPE = DatasetSubTypes.SEMANTIC_MODEL_DATASET

_MIGRATION_REPORT_TITLE = "dbt Semantic Model Migration Report"

# dbt never synthesizes column classification tags -- the entity/dimension/
# measure kind lives in nativeDataType ("entity:primary", "measure:sum") -- so
# every column tag on a legacy dbt semantic-model dataset is a customer tag and
# nothing is stripped, which is the shared core's default. A create_metric
# measure also stays a column of the logical dataset, so its tags land via the
# schema merge and never need fanning out onto a metric URN.

# The description is copied directly rather than folded into a documentation
# aspect: unlike a semanticModel, the destination is a dataset and owns the
# same editable aspect the source does.
_EXTRA_COPY_ASPECTS: Sequence[str] = ("editableDatasetProperties",)


@dataclass(frozen=True)
class DbtSemanticModelIdentity:
    """The one component both URN shapes share: the semantic model's name.

    The legacy URN encodes ``<database>.<schema>.<name>`` and the new one
    ``<project>.<name>``, so neither side's URN carries the other's prefix.
    """

    name: str


def legacy_dataset_name(dataset_urn: str, platform_instance: Optional[str]) -> str:
    urn = DatasetUrn.from_string(dataset_urn)
    name = urn.name
    if platform_instance:
        prefix = f"{platform_instance}."
        if not name.startswith(prefix):
            raise ValueError(
                f"'{name}' does not start with expected platform instance prefix "
                f"'{prefix}'"
            )
        name = name[len(prefix) :]
    return name


def parse_legacy_identity(
    dataset_urn: str, platform_instance: Optional[str]
) -> DbtSemanticModelIdentity:
    """Recover the semantic model name from a legacy dbt dataset urn.

    Accepts two or three parts: ``DBTNode.get_db_fqn`` drops falsy components,
    and a semantic model's database is genuinely optional (dbt Cloud resolves it
    from upstreams, and may not find one).
    """
    name = legacy_dataset_name(dataset_urn, platform_instance)
    parts = [part for part in name.split(".") if part]
    if len(parts) < 2 or len(parts) > 3:
        raise ValueError(
            f"Dataset name '{name}' (from {dataset_urn}) does not resolve to "
            "2 (schema.name) or 3 (database.schema.name) parts"
        )
    return DbtSemanticModelIdentity(name=parts[-1])


def parse_semantic_model_dataset_identity(
    dataset_urn: str, platform_instance: Optional[str]
) -> DbtSemanticModelIdentity:
    """Recover the semantic model name from a Semantic Model Dataset urn."""
    name = legacy_dataset_name(dataset_urn, platform_instance)
    parts = [part for part in name.split(".") if part]
    if len(parts) != 2:
        raise ValueError(
            f"Dataset name '{name}' (from {dataset_urn}) does not resolve to "
            "exactly 2 project.name parts"
        )
    return DbtSemanticModelIdentity(name=parts[-1])


def gen_semantic_model_dataset_urn(
    identity: DbtSemanticModelIdentity,
    project_name: str,
    platform_instance: Optional[str],
    env: str,
    convert_urns_to_lowercase: bool,
) -> str:
    """Match ``DbtSemanticModelMapper._logical_dataset_name``.

    ``platform_instance`` is applied by
    ``make_dataset_urn_with_platform_instance``, not baked into the identifier.
    """
    name = f"{project_name}.{identity.name}"
    if convert_urns_to_lowercase:
        name = name.lower()
    return make_dataset_urn_with_platform_instance(
        platform=DBT_PLATFORM,
        name=name,
        platform_instance=platform_instance,
        env=env,
    )


def migrate_field_governance(
    graph: DataHubGraph,
    src_dataset_urn: str,
    dst_dataset_urn: str,
    convert_column_urns_to_lowercase: bool,
    dry_run: bool,
) -> Tuple[List[str], List[str]]:
    """Merge the source's column tags/terms into the destination's editable schema.

    Both directions use the same mechanism, since both sides are datasets whose
    field paths agree.
    """
    fields = collect_dataset_field_governance(graph, src_dataset_urn)

    def resolve(
        column_name: str, schema_paths: Dict[str, str]
    ) -> Tuple[str, Optional[str]]:
        # Both sides' schemaMetadata comes from the same flattened
        # entity/dimension/measure names, so the translation is the identity --
        # but a not-yet-ingested destination has no schema to join against, and
        # the path must then be synthesized the way ingest will emit it.
        fallback = (
            column_name.lower() if convert_column_urns_to_lowercase else column_name
        )
        return resolve_field_path(column_name, schema_paths, fallback)

    return merge_field_governance_into_editable_schema(
        graph, dst_dataset_urn, fields, resolve, dry_run
    )


def migrate_one_dataset(
    graph: DataHubGraph,
    src_urn: str,
    dst_urn: str,
    convert_column_urns_to_lowercase: bool,
    dry_run: bool,
    report_inbound_refs: bool,
) -> EntityMigrationResult:
    result = migrate_entity(
        graph,
        src_urn,
        dst_urn,
        dry_run,
        report_inbound_refs,
        extra_aspects=_EXTRA_COPY_ASPECTS,
    )
    if result.error is not None:
        return result

    if is_soft_deleted(graph, src_urn):
        result.notes.append("source is soft-deleted")

    try:
        migrated, notes = migrate_field_governance(
            graph,
            src_urn,
            dst_urn,
            convert_column_urns_to_lowercase,
            dry_run,
        )
        result.fields_migrated = migrated
        result.notes.extend(notes)
    except Exception as e:
        log.warning(
            f"Field governance migration failed for {src_urn} -> {dst_urn}",
            exc_info=True,
        )
        # A field_error, not a note: every column tag and term on this entity
        # was lost, which must not read as a clean migration.
        result.field_errors.append(describe_exception(e))
    return result


# --- Discovery ---


def _discover_by_subtype(
    graph: DataHubGraph,
    subtype: str,
    env: Optional[str],
    platform_instance: Optional[str],
    include_soft_deleted: bool,
    only_soft_deleted: bool,
) -> List[str]:
    status = status_filter(include_soft_deleted, only_soft_deleted)
    return list(
        graph.get_urns_by_filter(
            entity_types=["dataset"],
            platform=DBT_PLATFORM,
            platform_instance=platform_instance,
            env=env,
            status=status,
            extraFilters=[
                SearchFilterRule(
                    field="typeNames", condition="EQUAL", values=[subtype]
                ).to_raw()
            ],
        )
    )


def discover_legacy_dataset_urns(
    graph: DataHubGraph,
    env: Optional[str] = None,
    platform_instance: Optional[str] = None,
    include_soft_deleted: bool = False,
    *,
    only_soft_deleted: bool = False,
) -> List[str]:
    """Find dbt dataset urns carrying the "Semantic Model" subtype.

    The subtype match is exact, so Semantic Model *Dataset* URNs -- this
    migration's own destinations -- are never returned as sources.
    """
    return _discover_by_subtype(
        graph,
        LEGACY_SUBTYPE,
        env,
        platform_instance,
        include_soft_deleted,
        only_soft_deleted,
    )


def discover_semantic_model_dataset_urns(
    graph: DataHubGraph,
    env: Optional[str] = None,
    platform_instance: Optional[str] = None,
    include_soft_deleted: bool = False,
    *,
    only_soft_deleted: bool = False,
) -> List[str]:
    """Find dbt dataset urns carrying the "Semantic Model Dataset" subtype."""
    return _discover_by_subtype(
        graph,
        SEMANTIC_MODEL_DATASET_SUBTYPE,
        env,
        platform_instance,
        include_soft_deleted,
        only_soft_deleted,
    )


def filter_by_expected_subtype(
    graph: DataHubGraph,
    urns: Sequence[str],
    force: bool,
    direction: MigrationDirection,
) -> Tuple[List[str], List[str]]:
    subtype = (
        LEGACY_SUBTYPE
        if direction == MigrationDirection.DATASET_TO_SM
        else SEMANTIC_MODEL_DATASET_SUBTYPE
    )
    return filter_by_subtype(graph, urns, force, subtype)


# --- Mapping resolution ---


@dataclass
class DbtUrnMapping:
    """Legacy <-> new URN pairs, plus why any source could not be paired.

    The dbt project name is in neither URN, and the legacy
    ``database.schema`` prefix is not in the new one, so the pairing cannot be
    computed from a single URN in either direction.
    """

    pairs: Dict[str, str] = field(default_factory=dict)
    # Source urn -> why it has no destination.
    unresolved: Dict[str, str] = field(default_factory=dict)
    # Candidates on the other side that could not be used, keyed by their urn.
    skipped_candidates: Dict[str, str] = field(default_factory=dict)


def build_mapping(
    graph: DataHubGraph,
    direction: MigrationDirection,
    src_urns: Sequence[str],
    *,
    project_name: Optional[str] = None,
    explicit_pairs: Optional[Dict[str, str]] = None,
    pair_by_name: bool = True,
    platform_instance: Optional[str] = None,
    env: str = "PROD",
    convert_urns_to_lowercase: bool = True,
) -> DbtUrnMapping:
    """Resolve destination urns, highest-precedence source first."""
    if explicit_pairs is not None:
        return DbtUrnMapping(
            pairs={
                urn: explicit_pairs[urn] for urn in src_urns if urn in explicit_pairs
            },
            unresolved={
                urn: "not present in the mapping file"
                for urn in src_urns
                if urn not in explicit_pairs
            },
        )

    if direction == MigrationDirection.DATASET_TO_SM and project_name:
        return _mapping_from_project_name(
            src_urns, project_name, platform_instance, env, convert_urns_to_lowercase
        )

    if pair_by_name:
        return _mapping_by_name(graph, direction, src_urns, platform_instance, env)

    return DbtUrnMapping(
        unresolved={
            urn: "cannot resolve a destination urn; pass --project-name (forward "
            "only) or --mapping-file, or leave --pair-by-name enabled"
            for urn in src_urns
        }
    )


def _mapping_from_project_name(
    src_urns: Sequence[str],
    project_name: str,
    platform_instance: Optional[str],
    env: str,
    convert_urns_to_lowercase: bool,
) -> DbtUrnMapping:
    """Synthesize destinations. The only option that works before ingest."""
    mapping = DbtUrnMapping()
    for urn in src_urns:
        try:
            identity = parse_legacy_identity(urn, platform_instance)
        except ValueError as e:
            mapping.unresolved[urn] = str(e)
            continue
        mapping.pairs[urn] = gen_semantic_model_dataset_urn(
            identity, project_name, platform_instance, env, convert_urns_to_lowercase
        )
    return mapping


def _mapping_by_name(
    graph: DataHubGraph,
    direction: MigrationDirection,
    src_urns: Sequence[str],
    platform_instance: Optional[str],
    env: str,
) -> DbtUrnMapping:
    """Join both sides on the semantic model name, the one shared component.

    Requires both sides to exist, so it only works after an ingest with the
    flag in its new state. An ambiguous name is reported and skipped rather
    than guessed at.
    """
    mapping = DbtUrnMapping()
    forward = direction == MigrationDirection.DATASET_TO_SM
    discover = (
        discover_semantic_model_dataset_urns
        if forward
        else discover_legacy_dataset_urns
    )
    parse_dst = (
        parse_semantic_model_dataset_identity if forward else parse_legacy_identity
    )
    parse_src = (
        parse_legacy_identity if forward else parse_semantic_model_dataset_identity
    )

    candidates_by_name: Dict[str, List[str]] = {}
    for candidate in discover(
        graph,
        env=env,
        platform_instance=platform_instance,
        # The other side is often soft-deleted right after a flag flip.
        include_soft_deleted=True,
    ):
        try:
            identity = parse_dst(candidate, platform_instance)
        except ValueError as e:
            # Without this the source below reports "no counterpart found",
            # which is wrong: the counterpart exists, its urn just did not
            # parse (a stale shape, or a --platform-instance mismatch).
            mapping.skipped_candidates[candidate] = (
                f"skipped as a mapping candidate: {e}"
            )
            continue
        candidates_by_name.setdefault(identity.name.casefold(), []).append(candidate)

    for urn in src_urns:
        try:
            identity = parse_src(urn, platform_instance)
        except ValueError as e:
            mapping.unresolved[urn] = str(e)
            continue
        matches = sorted(candidates_by_name.get(identity.name.casefold(), []))
        if not matches:
            hint = (
                " Some candidates were skipped as unparseable; check "
                "--platform-instance."
                if mapping.skipped_candidates
                else ""
            )
            mapping.unresolved[urn] = (
                f"no counterpart named '{identity.name}' found; ingest the other "
                f"side first, or pass --mapping-file.{hint}"
            )
            continue
        if len(matches) > 1:
            mapping.unresolved[urn] = (
                f"'{identity.name}' is ambiguous across {matches}; pass "
                "--mapping-file to disambiguate"
            )
            continue
        mapping.pairs[urn] = matches[0]
    return mapping


# --- Reporting ---


# repr=False so the parent's operator-facing __repr__ survives.
@dataclass(repr=False)
class DbtMigrationReport(MigrationReport):
    """MigrationReport with the dbt title and legacy subtype label."""

    title: str = _MIGRATION_REPORT_TITLE
    legacy_subtype_label: str = LEGACY_SUBTYPE


def run_migration(
    graph: DataHubGraph,
    direction: MigrationDirection,
    urns: Sequence[str],
    mapping: DbtUrnMapping,
    convert_column_urns_to_lowercase: bool,
    dry_run: bool,
    report_inbound_refs: bool,
    subtype_skipped: Optional[List[str]] = None,
) -> DbtMigrationReport:
    def migrate_one(urn: str) -> EntityMigrationResult:
        dst_urn = mapping.pairs.get(urn)
        if dst_urn is None:
            return EntityMigrationResult(
                src_urn=urn,
                dst_urn="",
                error=mapping.unresolved.get(
                    urn, "no destination urn could be resolved"
                ),
            )
        return migrate_one_dataset(
            graph,
            urn,
            dst_urn,
            convert_column_urns_to_lowercase,
            dry_run,
            report_inbound_refs,
        )

    report = DbtMigrationReport(
        direction=direction,
        dry_run=dry_run,
        subtype_skipped=list(subtype_skipped or []),
    )
    run_migration_loop(urns, migrate_one, report)
    return report


def semantic_model_not_a_destination_note() -> str:
    """The surprising part of this migration, worth stating up front."""
    return (
        "The project-level semanticModel entity is not a governance "
        "destination: it is shared by every semantic model in the project, so "
        "copying each legacy dataset onto it would keep only the last one's "
        "owners, tags and domain. Governance authored on a legacy semantic-model "
        "dataset moves to the corresponding Semantic Model Dataset instead."
    )
