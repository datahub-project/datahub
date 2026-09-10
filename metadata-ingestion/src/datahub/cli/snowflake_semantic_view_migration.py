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
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set, Tuple

from datahub.cli.semantic_model_migration_common import (
    GOVERNANCE_ASPECTS,
    SKIPPED_ASPECTS,
    EntityMigrationResult,
    FieldGovernance,
    MigrationDirection,
    MigrationReport,
    collect_dataset_field_governance as _collect_dataset_field_governance,
    collect_governance_aspects,
    emit_aspect,
    filter_by_subtype,
    is_soft_deleted,
    maybe_fold_documentation_to_editable_dataset,
    merge_field_governance,
    merge_field_governance_into_editable_schema,
    migrate_entity,
    resolve_field_path,
    run_migration_loop,
    schema_metadata_fields,
    simple_column_name,
    status_filter,
    strip_synthetic_subtype_tags,
)
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
    make_tag_urn,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import SearchFilterRule
from datahub.ingestion.graph.openapi import RelationshipDirection
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.snowflake.constants import SemanticViewColumnSubtype
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    GlobalTagsClass,
    GlossaryTermsClass,
    SchemaFieldClass,
    ViewPropertiesClass,
)
from datahub.metadata.urns import DatasetUrn, MetricUrn, SemanticModelUrn

log = logging.getLogger(__name__)

SNOWFLAKE_PLATFORM = "snowflake"
SEMANTIC_VIEW_SUBTYPE = DatasetSubTypes.SEMANTIC_VIEW

_MIGRATION_REPORT_TITLE = "Snowflake Semantic View Migration Report"

__all__ = [
    "GOVERNANCE_ASPECTS",
    "SKIPPED_ASPECTS",
    "EntityMigrationResult",
    "FieldGovernance",
    "MigrationDirection",
    "SemanticViewMigrationReport",
    "SnowflakeViewIdentity",
    "collect_dataset_field_governance",
    "collect_semantic_model_field_governance",
    "dataset_urn_to_semantic_model_urn",
    "discover_semantic_model_urns",
    "discover_semantic_view_dataset_urns",
    "filter_by_semantic_view_subtype",
    "gen_dataset_urn",
    "gen_metric_urn",
    "gen_semantic_model_dataset_urn",
    "gen_semantic_model_urn",
    "migrate_dataset_field_governance",
    "migrate_dataset_to_semantic_model",
    "migrate_entity",
    "migrate_semantic_model_field_governance",
    "migrate_semantic_model_to_dataset",
    "parse_dataset_identity",
    "parse_semantic_model_identity",
    "run_migration",
    "semantic_model_urn_to_dataset_urn",
    "snowflake_identifier",
]

# Connector-synthesized column classification tags on legacy schemaMetadata.
# Not customer tags — remodeled as semanticFieldAnnotation.type / metric entities.
# Lowercase variants have no connector-enum equivalent (SemanticViewColumnSubtype
# is uppercase-only), so those stay as literals.
_SYNTHETIC_SUBTYPE_TAG_URNS: Set[str] = {
    make_tag_urn(SemanticViewColumnSubtype.DIMENSION),
    make_tag_urn(SemanticViewColumnSubtype.FACT),
    make_tag_urn(SemanticViewColumnSubtype.METRIC),
    make_tag_urn("dimension"),
    make_tag_urn("fact"),
    make_tag_urn("metric"),
}


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


def gen_semantic_model_dataset_urn(
    identity: SnowflakeViewIdentity,
    logical_table: str,
    platform_instance: Optional[str],
    env: str,
    convert_urns_to_lowercase: bool,
) -> str:
    """Match ``SnowflakeIdentifierBuilder.gen_semantic_model_dataset_urn``.

    ``platform_instance`` is applied by ``make_dataset_urn_with_platform_instance``,
    not baked into the identifier (unlike semanticModel / metric URNs).
    """
    name = snowflake_identifier(
        f"{snowflake_identifier(f'{identity.db}.{identity.schema}', convert_urns_to_lowercase)}"
        f".{snowflake_identifier(identity.view, convert_urns_to_lowercase)}"
        f".{snowflake_identifier(logical_table, convert_urns_to_lowercase)}",
        convert_urns_to_lowercase,
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


def collect_dataset_field_governance(
    graph: DataHubGraph, dataset_urn: str
) -> List[FieldGovernance]:
    """Read column tags/terms, dropping the connector's synthetic subtype tags.

    METRIC detection uses those synthetic tags / ``columnSubType`` jsonProps so
    fan-out can target metric URNs.
    """
    return _collect_dataset_field_governance(
        graph,
        dataset_urn,
        synthetic_tag_urns=_SYNTHETIC_SUBTYPE_TAG_URNS,
        is_metric_column=_schema_field_is_metric,
        tags_indicate_metric=_tags_indicate_metric,
    )


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


def _semantic_model_field_path(
    column_name: str, convert_urns_to_lowercase: bool
) -> str:
    # Matches SnowflakeSemanticModelMapper semantic field fieldPath construction
    # (identifier of the uppercased column name).
    return snowflake_identifier(column_name.upper(), convert_urns_to_lowercase)


def _resolve_dataset_field_path(
    column_name: str,
    convert_urns_to_lowercase: bool,
    schema_paths: Dict[str, str],
) -> Tuple[str, Optional[str]]:
    """Pick a fieldPath that joins to the dataset schema (preserve original case).

    Unlike semanticModel fieldPaths (always upper-then-identifier), legacy dataset
    schemaMetadata uses snowflake_identifier(col.name) without forcing UPPER, so
    quoted mixed-case columns must keep their schema path when present.
    """
    return resolve_field_path(
        column_name,
        schema_paths,
        snowflake_identifier(column_name, convert_urns_to_lowercase),
    )


def _logical_table_names_from_source(
    graph: DataHubGraph, src_dataset_urn: str
) -> List[str]:
    """Best-effort logical table names for migrate-before-ingest SMD URN derivation.

    Unions ``TABLE_SYNONYM_<LOGICAL>`` keys on ``datasetProperties`` (present
    only for tables that have synonyms) with logical aliases parsed from the
    ``TABLES (...)`` clause of ``viewProperties.viewLogic``. Dedupes
    case-insensitively, synonym keys first.
    """
    names: List[str] = []
    seen: Set[str] = set()

    props = graph.get_aspects_for_entity(
        entity_urn=src_dataset_urn,
        aspects=["datasetProperties"],
        aspect_types=[DatasetPropertiesClass],
    ).get("datasetProperties")
    if isinstance(props, DatasetPropertiesClass) and props.customProperties:
        prefix = "TABLE_SYNONYM_"
        for key in props.customProperties:
            if not key.startswith(prefix):
                continue
            logical = key[len(prefix) :]
            if not logical:
                continue
            upper = logical.upper()
            if upper not in seen:
                seen.add(upper)
                names.append(logical)

    view_props = graph.get_aspects_for_entity(
        entity_urn=src_dataset_urn,
        aspects=["viewProperties"],
        aspect_types=[ViewPropertiesClass],
    ).get("viewProperties")
    if isinstance(view_props, ViewPropertiesClass) and view_props.viewLogic:
        for logical in _logical_table_names_from_view_logic(view_props.viewLogic):
            upper = logical.upper()
            if upper not in seen:
                seen.add(upper)
                names.append(logical)
    return names


def _logical_table_names_from_view_logic(view_logic: str) -> List[str]:
    """Extract logical aliases from a CREATE SEMANTIC VIEW ``TABLES (...)`` clause.

    Mirrors the Snowflake connector's DDL fallback (tokenize + walk) without
    importing the snowflake connector package.
    """
    try:
        import sqlglot
    except ImportError:
        return []

    try:
        dialect = sqlglot.Dialect.get_or_raise("snowflake")
        tokens = dialect.tokenize(view_logic)
    except Exception:
        return []

    entries = _tables_clause_entries(tokens)
    return [
        name
        for entry in entries
        for name in [_logical_name_from_tables_entry(entry)]
        if name is not None
    ]


def _tables_clause_entries(tokens: Sequence[object]) -> List[List[object]]:
    from sqlglot.tokens import TokenType

    block_start = None
    for idx in range(len(tokens) - 1):
        tok = tokens[idx]
        nxt = tokens[idx + 1]
        if (
            tok.token_type == TokenType.VAR  # type: ignore[attr-defined]
            and tok.text.upper() == "TABLES"  # type: ignore[attr-defined]
            and nxt.token_type == TokenType.L_PAREN  # type: ignore[attr-defined]
        ):
            block_start = idx + 2
            break
    if block_start is None:
        return []

    entries: List[List[object]] = []
    current: List[object] = []
    depth = 1
    for token in tokens[block_start:]:
        if token.token_type == TokenType.L_PAREN:  # type: ignore[attr-defined]
            depth += 1
        elif token.token_type == TokenType.R_PAREN:  # type: ignore[attr-defined]
            depth -= 1
            if depth == 0:
                break
        if depth == 1 and token.token_type == TokenType.COMMA:  # type: ignore[attr-defined]
            entries.append(current)
            current = []
        else:
            current.append(token)
    if current:
        entries.append(current)
    return entries


def _logical_name_from_tables_entry(entry_tokens: Sequence[object]) -> Optional[str]:
    from sqlglot.tokens import TokenType

    depth = 0
    for idx, token in enumerate(entry_tokens):
        if token.token_type == TokenType.L_PAREN:  # type: ignore[attr-defined]
            depth += 1
        elif token.token_type == TokenType.R_PAREN:  # type: ignore[attr-defined]
            depth -= 1
        elif depth == 0 and token.token_type == TokenType.ALIAS:  # type: ignore[attr-defined]
            nxt = entry_tokens[idx + 1] if idx + 1 < len(entry_tokens) else None
            # SQL-query logical tables (`alias AS (SELECT ...)`) have no SMD to
            # write governance onto; skip rather than minting an orphan URN.
            if nxt is not None and nxt.token_type == TokenType.L_PAREN:  # type: ignore[attr-defined]
                return None
            for prev in reversed(list(entry_tokens[:idx])):
                if prev.token_type in (TokenType.VAR, TokenType.IDENTIFIER):  # type: ignore[attr-defined]
                    return prev.text.strip('"')  # type: ignore[attr-defined]
            return None

    parts: List[str] = []
    for token in entry_tokens:
        if token.token_type in (TokenType.VAR, TokenType.IDENTIFIER):  # type: ignore[attr-defined]
            parts.append(token.text.strip('"'))  # type: ignore[attr-defined]
        elif token.token_type == TokenType.DOT:  # type: ignore[attr-defined]
            continue
        elif parts:
            break
    return parts[-1] if parts else None


def _logical_dataset_urns_for_model(
    graph: DataHubGraph,
    semantic_model_urn: str,
    src_dataset_urn: str,
    identity: SnowflakeViewIdentity,
    platform_instance: Optional[str],
    convert_urns_to_lowercase: bool,
) -> List[str]:
    """Resolve Semantic Model Dataset URNs for forward field-governance writes.

    Prefer live ``IsPartOf`` members when ingest already ran. Otherwise derive
    SMD URNs from logical tables on the legacy source (same shape as ingest) so
    migrate-before-ingest still lands tags where rollback will read them.
    """
    urns: List[str] = []
    for related in graph.get_related_entities(
        entity_urn=semantic_model_urn,
        relationship_types=["IsPartOf"],
        direction=RelationshipDirection.INCOMING,
    ):
        try:
            DatasetUrn.from_string(related.urn)
        except Exception:
            continue
        urns.append(related.urn)
    if urns:
        return urns

    env = str(DatasetUrn.from_string(src_dataset_urn).env)
    return [
        gen_semantic_model_dataset_urn(
            identity,
            logical_table,
            platform_instance,
            env,
            convert_urns_to_lowercase,
        )
        for logical_table in _logical_table_names_from_source(graph, src_dataset_urn)
    ]


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
    need not exist yet (migrate-before-ingest). Each destination is migrated
    independently so one SMD write failure doesn't skip the rest of the fan-out.

    Non-metric column tags/terms are written onto Semantic Model Dataset
    ``schemaField`` URNs (same anchor as ingest's ``semanticFieldAnnotation``),
    not the semanticModel URN.
    """
    migrated: List[str] = []
    skipped: List[str] = []
    logical_dataset_urns = _logical_dataset_urns_for_model(
        graph,
        semantic_model_urn,
        src_dataset_urn,
        identity,
        platform_instance,
        convert_urns_to_lowercase,
    )
    for field_gov in collect_dataset_field_governance(graph, src_dataset_urn):
        if field_gov.is_metric:
            dst_urns = [
                gen_metric_urn(
                    identity,
                    field_gov.column_name,
                    platform_instance,
                    convert_urns_to_lowercase,
                )
            ]
        else:
            if not logical_dataset_urns:
                skipped.append(
                    f"no logical datasets found for {field_gov.column_name}; "
                    "cannot derive SMD schemaField URN (need IsPartOf members, "
                    "TABLE_SYNONYM_* custom properties, or viewProperties.viewLogic)"
                )
                continue
            field_path = _semantic_model_field_path(
                field_gov.column_name, convert_urns_to_lowercase
            )
            dst_urns = [
                make_schema_field_urn(smd_urn, field_path)
                for smd_urn in logical_dataset_urns
            ]
        for dst_urn in dst_urns:
            try:
                if field_gov.global_tags is not None:
                    emit_aspect(graph, dst_urn, field_gov.global_tags, dry_run)
                    migrated.append(f"globalTags:{field_gov.column_name}->{dst_urn}")
                if field_gov.glossary_terms is not None:
                    emit_aspect(graph, dst_urn, field_gov.glossary_terms, dry_run)
                    migrated.append(f"glossaryTerms:{field_gov.column_name}->{dst_urn}")
            except Exception as e:
                skipped.append(
                    f"failed to migrate field governance for {field_gov.column_name} "
                    f"-> {dst_urn}: {e}"
                )
    return migrated, skipped


def collect_semantic_model_field_governance(
    graph: DataHubGraph,
    semantic_model_urn: str,
    convert_urns_to_lowercase: bool,
) -> List[FieldGovernance]:
    """Gather column tags/terms from metric entities and schemaField URNs under the model.

    Columns are enumerated from the ``schemaMetadata`` of each logical dataset
    linked via ``IsPartOf`` (``semanticModelProperties.semanticModel``).
    """
    by_column: Dict[str, FieldGovernance] = {}

    for related in graph.get_related_entities(
        entity_urn=semantic_model_urn,
        relationship_types=["ModeledBy"],
        direction=RelationshipDirection.INCOMING,
    ):
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

    for related in graph.get_related_entities(
        entity_urn=semantic_model_urn,
        relationship_types=["IsPartOf"],
        direction=RelationshipDirection.INCOMING,
    ):
        try:
            DatasetUrn.from_string(related.urn)
        except Exception:
            continue
        for schema_field in schema_metadata_fields(graph, related.urn):
            column_name = simple_column_name(schema_field.fieldPath)
            field_urn = make_schema_field_urn(
                related.urn,
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
            # what Snowflake ingest wrote onto the model dataset's schema.
            if tags is None and schema_field.globalTags is not None:
                tags = strip_synthetic_subtype_tags(
                    schema_field.globalTags, _SYNTHETIC_SUBTYPE_TAG_URNS
                )
            if terms is None:
                terms = schema_field.glossaryTerms
            if tags is None and terms is None:
                continue
            # Union across SMDs that share a column name (e.g. ID) so iteration
            # order does not drop governance from earlier datasets.
            merge_field_governance(
                by_column,
                column_name,
                is_metric=False,
                tags=tags if isinstance(tags, GlobalTagsClass) else None,
                terms=terms if isinstance(terms, GlossaryTermsClass) else None,
                synthetic_tag_urns=_SYNTHETIC_SUBTYPE_TAG_URNS,
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

    Resolves fieldPath against the destination dataset schema so mixed-case
    columns join correctly when convert_urns_to_lowercase is false.

    Returns (migrated entries, notes).
    """
    fields = collect_semantic_model_field_governance(
        graph,
        semantic_model_urn,
        convert_urns_to_lowercase,
    )
    return merge_field_governance_into_editable_schema(
        graph,
        dataset_urn,
        fields,
        lambda column_name, schema_paths: _resolve_dataset_field_path(
            column_name, convert_urns_to_lowercase, schema_paths
        ),
        dry_run,
    )


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

    if is_soft_deleted(graph, src_dataset_urn):
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

    if is_soft_deleted(graph, src_semantic_model_urn):
        result.notes.append("source is soft-deleted")

    try:
        # Re-collect so we can fold documentation that was just selected for copy.
        governance_aspects = collect_governance_aspects(graph, src_semantic_model_urn)
        note = maybe_fold_documentation_to_editable_dataset(
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
    status = status_filter(include_soft_deleted, only_soft_deleted)
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
    status = status_filter(include_soft_deleted, only_soft_deleted)
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
    """Split urns into (valid, skipped) based on the "Semantic View" subtype."""
    return filter_by_subtype(graph, urns, force, SEMANTIC_VIEW_SUBTYPE)


# --- Reporting ---


# repr=False so the parent's operator-facing __repr__ survives.
@dataclass(repr=False)
class SemanticViewMigrationReport(MigrationReport):
    """MigrationReport with the Snowflake title and legacy subtype label."""

    title: str = _MIGRATION_REPORT_TITLE
    legacy_subtype_label: str = SEMANTIC_VIEW_SUBTYPE


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
    def migrate_one(urn: str) -> EntityMigrationResult:
        if direction == MigrationDirection.DATASET_TO_SM:
            return migrate_dataset_to_semantic_model(
                graph,
                urn,
                platform_instance,
                convert_urns_to_lowercase,
                dry_run,
                report_inbound_refs,
            )
        return migrate_semantic_model_to_dataset(
            graph,
            urn,
            platform_instance,
            convert_urns_to_lowercase,
            env,
            dry_run,
            report_inbound_refs,
        )

    report = SemanticViewMigrationReport(
        direction=direction,
        dry_run=dry_run,
        subtype_skipped=list(subtype_skipped or []),
    )
    run_migration_loop(urns, migrate_one, report)
    return report
