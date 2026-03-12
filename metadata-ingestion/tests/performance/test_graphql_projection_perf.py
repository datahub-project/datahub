"""Performance benchmark for GraphQL query projection pipeline.

Measures the cost of each stage (parse, inline, visit, print_ast) on real
workloads so we can verify that _inline_fragments() is negligible.

Run with:
    cd metadata-ingestion && source venv/bin/activate

    # Mock-only (no server required):
    pytest tests/performance/test_graphql_projection_perf.py -s -k "not live"

    # Against a live DataHub instance:
    DATAHUB_PERF_GMS_URL=http://localhost:8080 \
        pytest tests/performance/test_graphql_projection_perf.py -s -k live
"""

import importlib.resources as pkg_resources
import os
import statistics
import time
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, List, NamedTuple, Optional, Tuple
from unittest.mock import Mock

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph

import pytest
from graphql import (
    GraphQLSchema,
    TypeInfo,
    TypeInfoVisitor,
    build_schema,
    parse,
    print_ast,
    visit,
)
from graphql.utilities import introspection_from_schema

from datahub.utilities.graphql_query_adapter import (
    QueryProjector,
    UnsupportedFieldRemover,
    _inline_fragments,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

N_ITERATIONS = 100

_AGENT_CONTEXT_GQL_DIR = (
    Path(__file__).resolve().parents[3]
    / "datahub-agent-context"
    / "src"
    / "datahub_agent_context"
    / "mcp_tools"
    / "gql"
)


class Workload(NamedTuple):
    name: str
    query: str
    schema: GraphQLSchema


class StageTimings(NamedTuple):
    parse_ms: Tuple[float, float]  # (median, p95)
    inline_ms: Tuple[float, float]
    visit_ms: Tuple[float, float]
    print_ms: Tuple[float, float]
    cold_ms: Tuple[float, float]


def _percentile(sorted_vals: List[float], pct: float) -> float:
    idx = int(len(sorted_vals) * pct / 100)
    return sorted_vals[min(idx, len(sorted_vals) - 1)]


def _time_n(fn: Callable[[], object], n: int = N_ITERATIONS) -> Tuple[float, float]:
    """Run *fn* n times, return (median_ms, p95_ms)."""
    times: List[float] = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1000)
    times.sort()
    return statistics.median(times), _percentile(times, 95)


def _make_mock_graph(
    schema: GraphQLSchema,
    commit_hash: str = "abc123",
) -> Mock:
    graph = Mock()

    def execute_graphql(query: str, strip_unsupported_fields: bool = False) -> dict:
        if "__schema" in query:
            return dict(introspection_from_schema(schema))
        return {}

    graph.execute_graphql = execute_graphql
    graph._gms_server = "http://localhost:8080"
    graph.server_config = Mock()
    graph.server_config.commit_hash = commit_hash
    return graph


# ---------------------------------------------------------------------------
# Query loaders
# ---------------------------------------------------------------------------


def _load_cli_search_query() -> str:
    fragments = (
        pkg_resources.files("datahub.cli.gql").joinpath("fragments.gql").read_text()
    )
    operation = (
        pkg_resources.files("datahub.cli.gql").joinpath("search.gql").read_text()
    )
    return fragments + "\n" + operation


def _load_cli_semantic_search_query() -> str:
    fragments = (
        pkg_resources.files("datahub.cli.gql").joinpath("fragments.gql").read_text()
    )
    operation = (
        pkg_resources.files("datahub.cli.gql")
        .joinpath("semantic_search.gql")
        .read_text()
    )
    return fragments + "\n" + operation


def _load_entity_details_query() -> Optional[str]:
    gql_file = _AGENT_CONTEXT_GQL_DIR / "entity_details.gql"
    if gql_file.exists():
        return gql_file.read_text()
    return None


def _load_document_search_query() -> Optional[str]:
    gql_file = _AGENT_CONTEXT_GQL_DIR / "document_search.gql"
    if gql_file.exists():
        return gql_file.read_text()
    return None


SIMPLE_QUERY = """\
query getMe {
  me {
    corpUser {
      urn
      username
      properties {
        displayName
        email
      }
    }
  }
}
"""

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

# Covers CLI search/semantic-search queries (all entity types except Document)
_CLI_SEARCH_SCHEMA_SDL = """
    type Query {
        searchAcrossEntities(input: SearchAcrossEntitiesInput!): SearchResultsWithFacets
        semanticSearchAcrossEntities(input: SearchAcrossEntitiesInput!): SearchResultsWithFacets
    }
    input SearchAcrossEntitiesInput {
        query: String!
        count: Int
        start: Int
        types: [EntityType!]
        orFilters: [AndFilterInput!]
        viewUrn: String
        sortInput: SearchSortInput
        searchFlags: SearchFlags
    }
    enum EntityType { DATASET CHART DASHBOARD DATA_JOB DATA_FLOW CORP_USER CORP_GROUP
        DOMAIN CONTAINER GLOSSARY_TERM TAG ML_MODEL ML_FEATURE ML_FEATURE_TABLE
        ML_PRIMARY_KEY ML_MODEL_GROUP DOCUMENT }
    input AndFilterInput { and: [FacetFilterInput!] }
    input FacetFilterInput { field: String! values: [String!] condition: FilterOperator }
    enum FilterOperator { EQUAL CONTAIN }
    input SearchSortInput { sortCriterion: SortCriterion }
    input SortCriterion { field: String! sortOrder: SortOrder }
    enum SortOrder { ASCENDING DESCENDING }
    input SearchFlags { skipHighlighting: Boolean maxAggValues: Int }

    type SearchResultsWithFacets {
        start: Int!
        count: Int!
        total: Int!
        searchResults: [SearchResult!]!
        facets: [FacetMetadata!]!
    }
    type SearchResult {
        entity: Entity
        matchedFields: [MatchedField!]
    }
    type MatchedField { name: String value: String }
    type FacetMetadata {
        field: String!
        displayName: String
        aggregations: [AggregationMetadata!]!
    }
    type AggregationMetadata {
        value: String!
        count: Int!
        displayName: String
        entity: Entity
    }

    union Entity = Dataset | Chart | Dashboard | DataJob | DataFlow
        | CorpUser | CorpGroup | Domain | Container | GlossaryTerm | Tag
        | MLModel | MLFeature | MLFeatureTable | MLPrimaryKey | MLModelGroup
        | Document | DataPlatform

    type DataPlatform {
        urn: String!
        name: String!
        properties: DataPlatformProperties
    }
    type DataPlatformProperties { displayName: String logoUrl: String }

    type Dataset {
        urn: String!
        type: EntityType!
        name: String
        origin: String
        properties: DatasetProperties
        platform: DataPlatform
        tags: GlobalTags
        glossaryTerms: GlossaryTermAssociation
        ownership: Ownership
        domain: DomainAssociation
        container: Container
    }
    type DatasetProperties {
        name: String
        description: String
        customProperties: [CustomPropertiesEntry!]
    }
    type CustomPropertiesEntry { key: String! value: String }

    type Chart {
        urn: String!
        type: EntityType!
        properties: ChartProperties
        platform: DataPlatform
        tags: GlobalTags
        glossaryTerms: GlossaryTermAssociation
        ownership: Ownership
        domain: DomainAssociation
    }
    type ChartProperties { name: String description: String }

    type Dashboard {
        urn: String!
        type: EntityType!
        properties: DashboardProperties
        platform: DataPlatform
        tags: GlobalTags
        glossaryTerms: GlossaryTermAssociation
        ownership: Ownership
        domain: DomainAssociation
    }
    type DashboardProperties { name: String description: String }

    type DataJob {
        urn: String!
        type: EntityType!
        dataFlow: DataFlow
        jobId: String
        properties: DataJobProperties
        tags: GlobalTags
        glossaryTerms: GlossaryTermAssociation
        ownership: Ownership
        domain: DomainAssociation
    }
    type DataJobProperties { name: String description: String }

    type DataFlow {
        urn: String!
        type: EntityType!
        flowId: String
        cluster: String
        properties: DataFlowProperties
        platform: DataPlatform
        tags: GlobalTags
        glossaryTerms: GlossaryTermAssociation
        ownership: Ownership
        domain: DomainAssociation
    }
    type DataFlowProperties { name: String description: String }

    type CorpUser {
        urn: String!
        type: EntityType!
        username: String!
        properties: CorpUserProperties
    }
    type CorpUserProperties { displayName: String email: String }

    type CorpGroup {
        urn: String!
        type: EntityType!
        name: String
        properties: CorpGroupProperties
    }
    type CorpGroupProperties { displayName: String email: String }

    type Domain {
        urn: String!
        type: EntityType!
        properties: DomainProperties
    }
    type DomainProperties { name: String description: String }

    type Container {
        urn: String!
        type: EntityType!
        properties: ContainerProperties
        platform: DataPlatform
        tags: GlobalTags
        glossaryTerms: GlossaryTermAssociation
        ownership: Ownership
        domain: DomainAssociation
    }
    type ContainerProperties { name: String description: String }

    type GlossaryTerm {
        urn: String!
        type: EntityType!
        name: String
        properties: GlossaryTermProperties
    }
    type GlossaryTermProperties { name: String description: String }

    type Tag {
        urn: String!
        type: EntityType!
        name: String
        properties: TagProperties
    }
    type TagProperties { name: String description: String colorHex: String }

    type MLModel {
        urn: String!
        type: EntityType!
        name: String
        properties: MLModelProperties
        platform: DataPlatform
        tags: GlobalTags
        glossaryTerms: GlossaryTermAssociation
        ownership: Ownership
        domain: DomainAssociation
    }
    type MLModelProperties { description: String }

    type MLFeature {
        urn: String!
        type: EntityType!
        name: String
        properties: MLFeatureProperties
        tags: GlobalTags
        glossaryTerms: GlossaryTermAssociation
        ownership: Ownership
        domain: DomainAssociation
    }
    type MLFeatureProperties { description: String }

    type MLFeatureTable {
        urn: String!
        type: EntityType!
        name: String
        properties: MLFeatureTableProperties
        platform: DataPlatform
        tags: GlobalTags
        glossaryTerms: GlossaryTermAssociation
        ownership: Ownership
        domain: DomainAssociation
    }
    type MLFeatureTableProperties { description: String }

    type MLPrimaryKey {
        urn: String!
        type: EntityType!
        name: String
        properties: MLPrimaryKeyProperties
        tags: GlobalTags
        glossaryTerms: GlossaryTermAssociation
        ownership: Ownership
        domain: DomainAssociation
    }
    type MLPrimaryKeyProperties { description: String }

    type MLModelGroup {
        urn: String!
        type: EntityType!
        name: String
        properties: MLModelGroupProperties
        platform: DataPlatform
        tags: GlobalTags
        glossaryTerms: GlossaryTermAssociation
        ownership: Ownership
        domain: DomainAssociation
    }
    type MLModelGroupProperties { description: String }

    type Document {
        urn: String!
        type: EntityType!
        subType: String
        platform: DataPlatform
        info: DocumentInfo
        domain: DomainAssociation
        tags: GlobalTags
        glossaryTerms: GlossaryTermAssociation
    }
    type DocumentInfo {
        title: String
        source: DocumentSource
        lastModified: AuditStamp
        created: AuditStamp
    }
    type DocumentSource { sourceType: String externalUrl: String }
    type AuditStamp { time: Long actor: CorpUser }
    scalar Long

    type GlobalTags { tags: [TagAssociation!] }
    type TagAssociation { tag: Tag }
    type GlossaryTermAssociation { terms: [GlossaryTermAssociationEntry!] }
    type GlossaryTermAssociationEntry { term: GlossaryTerm }
    type Ownership { owners: [Owner!] }
    type Owner { owner: OwnerEntity }
    union OwnerEntity = CorpUser | CorpGroup
    type DomainAssociation { domain: Domain }
"""

# Entity details schema — covers the main query types used in entity_details.gql.
# Missing types simply get stripped by the visitor, which is part of what we measure.
_ENTITY_DETAILS_SCHEMA_SDL = """
    type Query {
        entity(urn: String!): Entity
        searchAcrossLineage(input: SearchAcrossLineageInput!): SearchAcrossLineageResults
    }
    input SearchAcrossLineageInput {
        urn: String!
        query: String
        count: Int
        start: Int
        direction: LineageDirection
        orFilters: [AndFilterInput!]
        types: [EntityType!]
    }
    enum LineageDirection { UPSTREAM DOWNSTREAM }
    input AndFilterInput { and: [FacetFilterInput!] }
    input FacetFilterInput { field: String! values: [String!] condition: FilterOperator }
    enum FilterOperator { EQUAL CONTAIN }

    type SearchAcrossLineageResults {
        start: Int!
        count: Int!
        total: Int!
        searchResults: [SearchAcrossLineageResult!]!
    }
    type SearchAcrossLineageResult { entity: Entity degree: Int }

    enum EntityType { DATASET CHART DASHBOARD DATA_JOB DATA_FLOW CORP_USER CORP_GROUP
        DOMAIN CONTAINER GLOSSARY_TERM TAG ML_MODEL ML_FEATURE ML_FEATURE_TABLE
        ML_PRIMARY_KEY ML_MODEL_GROUP DOCUMENT DATA_PRODUCT }

    union Entity = Dataset | Chart | Dashboard | DataJob | DataFlow
        | CorpUser | CorpGroup | Domain | Container | GlossaryTerm | Tag
        | MLModel | MLFeature | MLFeatureTable | MLPrimaryKey | MLModelGroup
        | Document | DataProduct | DataPlatform

    type DataPlatform { urn: String! name: String! properties: DataPlatformProperties }
    type DataPlatformProperties { displayName: String logoUrl: String }

    type Dataset {
        urn: String!
        type: EntityType!
        name: String
        origin: String
        properties: DatasetProperties
        editableProperties: EditableDatasetProperties
        platform: DataPlatform
        tags: GlobalTags
        glossaryTerms: GlossaryTermAssociation
        ownership: Ownership
        domain: DomainAssociation
        container: Container
        schemaMetadata: SchemaMetadata
        deprecation: Deprecation
        subTypes: SubTypes
    }
    type DatasetProperties { name: String description: String qualifiedName: String customProperties: [CustomPropertiesEntry!] }
    type EditableDatasetProperties { description: String }
    type CustomPropertiesEntry { key: String! value: String }
    type SchemaMetadata { fields: [SchemaField!] }
    type SchemaField { fieldPath: String! nativeDataType: String description: String type: SchemaFieldDataType }
    type SchemaFieldDataType { type: String }
    type Deprecation { deprecated: Boolean! note: String actor: String }
    type SubTypes { typeNames: [String!] }

    type Chart { urn: String! type: EntityType! properties: ChartProperties platform: DataPlatform tags: GlobalTags glossaryTerms: GlossaryTermAssociation ownership: Ownership domain: DomainAssociation }
    type ChartProperties { name: String description: String }

    type Dashboard { urn: String! type: EntityType! properties: DashboardProperties platform: DataPlatform tags: GlobalTags glossaryTerms: GlossaryTermAssociation ownership: Ownership domain: DomainAssociation }
    type DashboardProperties { name: String description: String }

    type DataJob { urn: String! type: EntityType! dataFlow: DataFlow jobId: String properties: DataJobProperties tags: GlobalTags glossaryTerms: GlossaryTermAssociation ownership: Ownership domain: DomainAssociation }
    type DataJobProperties { name: String description: String }

    type DataFlow { urn: String! type: EntityType! flowId: String cluster: String properties: DataFlowProperties platform: DataPlatform tags: GlobalTags glossaryTerms: GlossaryTermAssociation ownership: Ownership domain: DomainAssociation }
    type DataFlowProperties { name: String description: String }

    type CorpUser { urn: String! type: EntityType! username: String! properties: CorpUserProperties editableProperties: EditableCorpUserProperties }
    type CorpUserProperties { active: Boolean displayName: String title: String email: String }
    type EditableCorpUserProperties { displayName: String title: String email: String }

    type CorpGroup { urn: String! type: EntityType! name: String properties: CorpGroupProperties }
    type CorpGroupProperties { displayName: String email: String }

    type Domain { urn: String! type: EntityType! properties: DomainProperties }
    type DomainProperties { name: String description: String }

    type Container { urn: String! type: EntityType! properties: ContainerProperties platform: DataPlatform tags: GlobalTags glossaryTerms: GlossaryTermAssociation ownership: Ownership domain: DomainAssociation }
    type ContainerProperties { name: String description: String }

    type GlossaryTerm { urn: String! type: EntityType! name: String properties: GlossaryTermProperties }
    type GlossaryTermProperties { name: String description: String }

    type Tag { urn: String! type: EntityType! name: String properties: TagProperties }
    type TagProperties { name: String description: String colorHex: String }

    type MLModel { urn: String! type: EntityType! name: String properties: MLModelProperties platform: DataPlatform tags: GlobalTags glossaryTerms: GlossaryTermAssociation ownership: Ownership domain: DomainAssociation }
    type MLModelProperties { description: String }
    type MLFeature { urn: String! type: EntityType! name: String properties: MLFeatureProperties tags: GlobalTags glossaryTerms: GlossaryTermAssociation ownership: Ownership domain: DomainAssociation }
    type MLFeatureProperties { description: String }
    type MLFeatureTable { urn: String! type: EntityType! name: String properties: MLFeatureTableProperties platform: DataPlatform tags: GlobalTags glossaryTerms: GlossaryTermAssociation ownership: Ownership domain: DomainAssociation }
    type MLFeatureTableProperties { description: String }
    type MLPrimaryKey { urn: String! type: EntityType! name: String properties: MLPrimaryKeyProperties tags: GlobalTags glossaryTerms: GlossaryTermAssociation ownership: Ownership domain: DomainAssociation }
    type MLPrimaryKeyProperties { description: String }
    type MLModelGroup { urn: String! type: EntityType! name: String properties: MLModelGroupProperties platform: DataPlatform tags: GlobalTags glossaryTerms: GlossaryTermAssociation ownership: Ownership domain: DomainAssociation }
    type MLModelGroupProperties { description: String }

    type Document { urn: String! type: EntityType! subType: String platform: DataPlatform info: DocumentInfo domain: DomainAssociation tags: GlobalTags glossaryTerms: GlossaryTermAssociation }
    type DocumentInfo { title: String source: DocumentSource lastModified: AuditStamp created: AuditStamp }
    type DocumentSource { sourceType: String externalUrl: String }
    type AuditStamp { time: Long actor: CorpUser }
    scalar Long

    type DataProduct { urn: String! type: EntityType! properties: DataProductProperties }
    type DataProductProperties { name: String description: String }

    type GlobalTags { tags: [TagAssociation!] }
    type TagAssociation { tag: Tag }
    type GlossaryTermAssociation { terms: [GlossaryTermAssociationEntry!] }
    type GlossaryTermAssociationEntry { term: GlossaryTerm }
    type Ownership { owners: [Owner!] }
    type Owner { owner: OwnerEntity }
    union OwnerEntity = CorpUser | CorpGroup
    type DomainAssociation { domain: Domain }
"""

_SIMPLE_SCHEMA_SDL = """
    type Query { me: AuthenticatedUser }
    type AuthenticatedUser { corpUser: CorpUser }
    type CorpUser { urn: String! username: String! properties: CorpUserProperties }
    type CorpUserProperties { displayName: String email: String }
"""


def _build_workloads() -> List[Workload]:
    cli_schema = build_schema(_CLI_SEARCH_SCHEMA_SDL)
    entity_schema = build_schema(_ENTITY_DETAILS_SCHEMA_SDL)
    simple_schema = build_schema(_SIMPLE_SCHEMA_SDL)

    workloads: List[Workload] = []

    # CLI search (735 lines, 14 named spreads, 2-level nesting)
    workloads.append(
        Workload(
            "CLI search (735 lines, 14 spreads)", _load_cli_search_query(), cli_schema
        )
    )

    # CLI semantic search (737 lines, same fragments)
    workloads.append(
        Workload(
            "CLI semantic (737 lines, 14 spreads)",
            _load_cli_semantic_search_query(),
            cli_schema,
        )
    )

    # MCP entity_details (1735 lines, 28 fragment defs, 93 named spreads)
    entity_details = _load_entity_details_query()
    if entity_details:
        workloads.append(
            Workload(
                "entity_details (1735 lines, 93 spreads)", entity_details, entity_schema
            )
        )

    # MCP document_search (114 lines, 2 named spreads)
    doc_search = _load_document_search_query()
    if doc_search:
        workloads.append(
            Workload("doc_search (114 lines, 2 spreads)", doc_search, cli_schema)
        )

    # Simple inline-only query (~15 lines, 0 spreads)
    workloads.append(
        Workload("simple (15 lines, 0 spreads)", SIMPLE_QUERY, simple_schema)
    )

    return workloads


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------


def _benchmark_stages(workload: Workload) -> StageTimings:
    """Time each pipeline stage independently for a single workload."""
    query_text = workload.query
    schema = workload.schema

    # Stage 1: parse
    parse_stats = _time_n(lambda: parse(query_text))

    # Pre-compute parsed doc for subsequent stages
    document = parse(query_text)

    # Stage 2: inline
    inline_stats = _time_n(lambda: _inline_fragments(document))

    # Pre-compute inlined doc for subsequent stages
    inlined = _inline_fragments(document)

    # Stage 3: visit (UnsupportedFieldRemover)
    def do_visit():
        ti = TypeInfo(schema)
        v = UnsupportedFieldRemover(schema, ti)
        visit(inlined, TypeInfoVisitor(ti, v))

    visit_stats = _time_n(do_visit)

    # Pre-compute visited AST for print stage
    ti = TypeInfo(schema)
    v = UnsupportedFieldRemover(schema, ti)
    visited = visit(inlined, TypeInfoVisitor(ti, v))

    # Stage 4: print_ast
    print_stats = _time_n(lambda: print_ast(visited))

    # Stage 5: cold adapt_query (full pipeline, fresh projector each time)
    graph = _make_mock_graph(schema)

    def do_cold():
        p = QueryProjector()
        p.adapt_query(query_text, graph)

    cold_stats = _time_n(do_cold)

    return StageTimings(
        parse_ms=parse_stats,
        inline_ms=inline_stats,
        visit_ms=visit_stats,
        print_ms=print_stats,
        cold_ms=cold_stats,
    )


def _benchmark_cache(workload: Workload) -> Dict[str, Tuple[float, float]]:
    """Measure cache hit paths."""
    query_text = workload.query
    schema = workload.schema
    graph = _make_mock_graph(schema)

    # Warm up the projector (populates both caches)
    projector = QueryProjector()
    projector.adapt_query(query_text, graph)

    # Tier 2 hit: same query + same schema generation (dict lookup)
    tier2_stats = _time_n(lambda: projector.adapt_query(query_text, graph))

    # Tier 1 hit, Tier 2 miss: invalidate projection cache but keep inline cache.
    # We directly manipulate the projector's internal state to isolate the
    # visitor re-run cost from the schema introspection cost.
    def do_tier1_hit():
        # Evict projection cache by bumping generation
        projector._schema_generation += 1
        projector._query_cache.clear()
        projector.adapt_query(query_text, graph)
        # Restore so next iteration starts clean
        projector._query_cache.clear()

    tier1_stats = _time_n(do_tier1_hit, n=50)

    return {
        "tier2_hit": tier2_stats,
        "tier1_hit_tier2_miss": tier1_stats,
    }


def _fmt(stats: Tuple[float, float]) -> str:
    """Format (median, p95) as 'median / p95' in ms."""
    return f"{stats[0]:7.3f} / {stats[1]:7.3f}"


@pytest.mark.perf
def test_graphql_projection_benchmark() -> None:
    """Benchmark all stages of the GraphQL query projection pipeline."""
    workloads = _build_workloads()

    print(f"\n{'=' * 100}")
    print(f"GraphQL Query Projection Benchmark (N={N_ITERATIONS}, median / p95 in ms)")
    print(f"{'=' * 100}")

    header = (
        f"{'Workload':<42} {'parse':>15} {'inline':>15} "
        f"{'visit':>15} {'print_ast':>15} {'cold_total':>15}"
    )
    print(header)
    print("-" * len(header))

    for wl in workloads:
        timings = _benchmark_stages(wl)
        print(
            f"{wl.name:<42} {_fmt(timings.parse_ms):>15} {_fmt(timings.inline_ms):>15} "
            f"{_fmt(timings.visit_ms):>15} {_fmt(timings.print_ms):>15} {_fmt(timings.cold_ms):>15}"
        )

    # Cache benchmarks — use the first workload (CLI search, heaviest inlining)
    print(f"\n{'Cache paths (CLI search workload):'}")
    print("-" * 60)
    cache_stats = _benchmark_cache(workloads[0])
    print(f"  Tier 2 hit (dict lookup):          {_fmt(cache_stats['tier2_hit'])} ms")
    print(
        f"  Tier 1 hit + visitor re-run (N=20): {_fmt(cache_stats['tier1_hit_tier2_miss'])} ms"
    )
    print()


# ---------------------------------------------------------------------------
# Live-server benchmark
# ---------------------------------------------------------------------------

_LIVE_GMS_URL = os.environ.get("DATAHUB_PERF_GMS_URL")
_LIVE_GMS_TOKEN = os.environ.get("DATAHUB_PERF_GMS_TOKEN")

_LIVE_N_ITERATIONS = 20


def _get_live_graph():
    """Create a real DataHubGraph connected to the live server."""
    from datahub.ingestion.graph.client import DataHubGraph
    from datahub.ingestion.graph.config import DatahubClientConfig

    config = DatahubClientConfig(
        server=_LIVE_GMS_URL,
        token=_LIVE_GMS_TOKEN,
    )
    return DataHubGraph(config)


def _time_live_introspection(graph: "DataHubGraph") -> Tuple[float, float]:
    """Time the raw introspection query against the live server."""
    from graphql import get_introspection_query

    introspection_query = get_introspection_query(descriptions=True)

    def do_introspection():
        graph.execute_graphql(introspection_query)

    return _time_n(do_introspection, n=_LIVE_N_ITERATIONS)


@pytest.mark.perf
@pytest.mark.skipif(
    not _LIVE_GMS_URL,
    reason="Set DATAHUB_PERF_GMS_URL to run live-server benchmarks",
)
def test_graphql_projection_benchmark_live() -> None:
    """Benchmark projection pipeline against a live DataHub server.

    Measures introspection query latency and end-to-end adapt_query cost
    including real network round-trips.

    Run with:
        DATAHUB_PERF_GMS_URL=http://localhost:8080 \
            pytest tests/performance/test_graphql_projection_perf.py::test_graphql_projection_benchmark_live -s
    """
    graph = _get_live_graph()

    print(f"\n{'=' * 90}")
    print(
        f"Live-Server Projection Benchmark "
        f"(N={_LIVE_N_ITERATIONS}, server={_LIVE_GMS_URL})"
    )
    print(f"{'=' * 90}")

    # 1. Raw introspection query latency
    print("\nIntrospection query (raw GraphQL round-trip):")
    print("-" * 60)
    introspect_stats = _time_live_introspection(graph)
    print(f"  median / p95: {_fmt(introspect_stats)} ms")

    # 2. Cold adapt_query (includes introspection + parse + inline + visit + print)
    queries = {
        "CLI search": _load_cli_search_query(),
        "CLI semantic": _load_cli_semantic_search_query(),
        "simple (getMe)": SIMPLE_QUERY,
    }
    entity_details = _load_entity_details_query()
    if entity_details:
        queries["entity_details"] = entity_details
    doc_search = _load_document_search_query()
    if doc_search:
        queries["doc_search"] = doc_search

    # Cold: fresh projector each time (introspection + full pipeline)
    print(f"\n{'Workload':<30} {'cold (w/ introspection)':>25} {'warm (cached)':>25}")
    print("-" * 82)

    for name, query_text in queries.items():
        # Cold: every call creates a new projector → forces introspection
        def do_cold(q=query_text):
            p = QueryProjector()
            p.adapt_query(q, graph)

        cold_stats = _time_n(do_cold, n=_LIVE_N_ITERATIONS)

        # Warm: reuse projector with populated caches (tier 2 hit)
        projector = QueryProjector()
        projector.adapt_query(query_text, graph)  # warm up

        def do_warm(q: str = query_text, p: QueryProjector = projector) -> None:
            p.adapt_query(q, graph)

        warm_stats = _time_n(do_warm, n=_LIVE_N_ITERATIONS)

        print(f"  {name:<28} {_fmt(cold_stats):>25} {_fmt(warm_stats):>25}")

    # 3. Schema fetch with TTL invalidation (force re-introspection)
    print("\nSchema re-fetch (forced TTL expiry):")
    print("-" * 60)
    projector = QueryProjector()
    query_text = _load_cli_search_query()
    projector.adapt_query(query_text, graph)  # warm up

    def do_refetch():
        # Force schema refetch by expiring the TTL
        projector._schema_fetched_at = 0.0
        projector._schema_commit_hash = None
        projector._query_cache.clear()
        projector.adapt_query(query_text, graph)

    refetch_stats = _time_n(do_refetch, n=_LIVE_N_ITERATIONS)
    print(f"  median / p95: {_fmt(refetch_stats)} ms")
    print()
