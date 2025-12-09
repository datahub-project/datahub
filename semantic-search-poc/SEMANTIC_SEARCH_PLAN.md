# Semantic Search Integration Plan

## Semantic Search POC → Slack + MCP integration plan

### Goals

- **Add** semantic search powered by OpenSearch kNN and embeddings.
- **Integrate** into Slack bot and MCP server, gated to a per-user allowlist.
- **Reuse** existing DataHub GraphQL search shape (start/count), adding a mode to switch to semantic ranking.
- **Avoid** leaking model/index knobs in the public API; keep them server-configured.

### High-level architecture

> **⚠️ Architecture Update (2025-08-27)**: Implementation revised to use dedicated semantic search endpoints instead of searchMode routing.

#### ~~Original Design (Obsolete)~~

- ~~Clients call DataHub GraphQL `searchAcrossEntities` with `searchFlags: { searchMode: SEMANTIC }`~~
- ~~Single endpoint with mode switching between KEYWORD and SEMANTIC~~

#### **Current Implementation**

- Query enters via Slack or MCP.
- Clients call dedicated GraphQL semantic endpoints:
  - `semanticSearch` for single entity type search
  - `semanticSearchAcrossEntities` for multi-entity search
- Server generates embedding, queries semantic indices via nested kNN, maps hits to URNs, then fetches entities via GraphQL for auth and shape; returns results with start/count pagination.
- No searchMode field needed - endpoint selection determines search type
- Optional snippet and score exposed as additional fields later.

### Components overview

> **Updated for dedicated endpoints architecture (2025-08-27)**

- **Slack bot (datahub-integrations-service)**: User-facing entrypoint. Routes `/datahub` commands. ~~For semantic mode, calls GraphQL with `searchFlags: { searchMode: SEMANTIC }`~~ **Now calls dedicated `semanticSearchAcrossEntities` endpoint**. Applies UX-side gating and fallbacks.
- **MCP server (mcp-server-datahub)**: Provides tools to LLM clients. Adds `semantic_search` tool that ~~calls GraphQL with `searchFlags: { searchMode: SEMANTIC }`~~ **now calls dedicated `semanticSearchAcrossEntities` endpoint**. Applies tool-level gating and surfacing of errors.
- **DataHub GraphQL API (backend)**: Source of truth. ~~Augmented resolver supports semantic mode~~ **New dedicated semantic search resolvers (`SemanticSearchResolver`, `SemanticSearchAcrossEntitiesResolver`)**. Enforces allowlist gating, produces embeddings, queries OpenSearch, permission-filters, paginates, and returns results.
- Supporting infra: **OpenSearch semantic indices** and **Embedding provider** (Cohere/Bedrock) configured via env.

### Component details

#### Slack bot

- **Function**: Parse `/datahub` commands, construct `SearchAcrossEntitiesInput`, and call GraphQL. For semantic mode, set `mode: SEMANTIC`.
- **Gating**: Check allowlist (Slack user → DataHub `corpuser` URN). If not allowlisted or feature disabled, fall back to keyword search and optionally inform the user.
- **Config**:
  - `SEMANTIC_SEARCH_ENABLED`
  - `SEMANTIC_SEARCH_ALLOWED_USERS` (CSV of `corpuser` URNs)
- **Fallbacks**:
  - GraphQL returns error/denial → revert to KEYWORD path.
  - Timeouts/errors → revert to KEYWORD path with a brief notice.
- **Observability**: Log command, mode, user, duration; emit metrics for semantic usage, fallbacks.

#### MCP server

- **Function**: Expose a `semantic_search` tool that accepts `query`, optional `types`, and `size` and calls GraphQL `searchAcrossEntities` with `mode: SEMANTIC`, mapping `size → count` and using `start=0` (or cursor in future).
- **Gating**: Check allowlist before calling GraphQL to provide clear tool errors; server still enforces gating.
- **Config**:
  - `SEMANTIC_SEARCH_ENABLED`
  - `SEMANTIC_SEARCH_ALLOWED_USERS`
- **Fallbacks**:
  - If gated off or GraphQL returns denial → return a friendly tool error suggesting KEYWORD search.
  - If transient error → optionally retry once; else return error message.
- **Observability**: Record tool usage, durations, and error counts; include user identity when available.

#### DataHub GraphQL API (backend)

- **Function**: Extend `searchAcrossEntities` to accept `mode: SEMANTIC`. Default remains `KEYWORD`. When semantic, generate embeddings and perform OpenSearch kNN, then post-filter and paginate via start/count.
- **Gating**: Primary enforcement. If `SEMANTIC_SEARCH_ENABLED` is false or actor not in `SEMANTIC_SEARCH_ALLOWED_USERS`, either:
  - Treat as `KEYWORD` (silent fallback), or
  - Return an authorization error (configurable behavior).
- **OpenSearch integration**:
  - Map `types` to indices with full parity: for each searchable entity index, append `_semantic` (e.g., `datasetindex_v2` → `datasetindex_v2_semantic`).
  - Oversampling default (v1): keep minimal headroom for ranking robustness (ANN + post-filter/auth drops), no rerank.
    - Default `k_internal`: `min(k_max, max(start + count, ceil(1.2 × (start + count)) + 5))`, with `k_max = 500`.
    - Breadth knob: we do NOT set `num_candidates` per request today; rely on index-level `knn.algo_param.ef_search` (tune ~128–256 for POC). If per-request `num_candidates` is supported in our OpenSearch version, we may add it later.
  - Compute internal k with headroom as above; avoid chasing long tails.
  - Use nested kNN on `embeddings.cohere_embed_v3.chunks.vector`; pre-filter simple terms when safe.
  - `_source` includes `urn`, `name`, `platform`, `qualifiedName`, `typeNames`, and top chunk `text` for optional snippets.
- **Authorization + filtering**:
  - Convert hits → URNs; dedupe; fetch entities through existing resolvers to apply auth and complex filters.
  - Slice to `start/count`; maintain stable tie-breakers.
- **Failure handling**:
  - Timeouts/provider issues → configurable fallback to KEYWORD; emit telemetry.
- **Result field evolution**:
  - **Original plan**: `semanticSimilarity: Float` in range 0–1. Present only when `mode=SEMANTIC`, omitted in `KEYWORD`.
  - **Current implementation**: Using existing `score: Float` field for all search modes:
    - KEYWORD mode: BM25 score (unbounded, can be > 1)
    - SEMANTIC mode: Cosine similarity score (normalized to [0, 1])
  - **Rationale for change**: Simpler API surface, single field for all modes, clients already know their requested mode
  - **Note**: We retain the option to revert to `semanticSimilarity` if distinct fields prove necessary for client clarity

### Current GraphQL search capabilities (and what MCP/Slack use)

- **Shared input type**: Both `searchAcrossEntities` and `scrollAcrossEntities` accept the same `SearchAcrossEntitiesInput` (naming may vary slightly by schema version). Fields observed in client queries:
  - **query: String!**
  - **types: [EntityType!]**
  - **orFilters: [AndFilterInput!]** (compiled DSL; clients pass nested AND blocks inside an OR array)
  - **count: Int!**
  - **start: Int** (used by Slack’s non-scroll query)
  - **scrollId: String** (used by the scroll variant; MCP currently does not pass it)
    - Semantic mode guidance:
      - v1: Not required for MCP. Return null/empty cursor; support only top-N (e.g., count ≤ 20–50).
      - If deeper pagination is needed later, use a server-side cursor that caches the semantic candidate set with a TTL and deterministic tie-breakers (e.g., search_after), not OpenSearch scroll.
  - **searchFlags**: object with fields like `skipHighlighting`, `maxAggValues`
  - **highlighting**: Supported in `KEYWORD` mode; the server can return matched-field highlights. Clients often set `skipHighlighting: true` to reduce overhead. In `SEMANTIC` mode, highlighting is not supported and `skipHighlighting` is effectively a no-op.
  - **sortInput: SearchSortInput** to specify custom sorting (field + order).
    - Keyword: Applied server-side on supported fields.
    - Semantic v1: Ignored. Results are ranked strictly by semantic score.
- **Response fields used by clients**:
  - **searchResults[].entity**: typed union; at minimum `urn`; sometimes name via entity-specific properties (Dataset/Chart/Dashboard/Container).
  - **count, total**: both Slack and MCP expect counts; Slack also expects `start` for pagination.
  - **facets[]**: both clients include facets in their result shape. MCP surfaces them to the LLM client for iterative filtering; Slack may render them.

#### MCP server usage (from queries)

- Uses `scrollAcrossEntities` with `query`, `count`, optional `types`, `orFilters`, and `searchFlags { skipHighlighting, maxAggValues }`.
- Does not pass `scrollId` today (first page only). Expects `count`, `total`, `searchResults[].entity{ urn, name (entity-specific) }`, and `facets`.
- Also relies on `GetEntity` for rich details and `searchAcrossLineage` for lineage (unchanged by semantic mode).

#### Slack usage (from queries)

- Uses `searchAcrossEntities` (non-scroll) with `start`, `count`, `types`, `orFilters`.
- Expects `start`, `count`, `total`, `searchResults[].entity{ ...entityFields }`, and `facets`.

### Support in semantic mode (required vs optional)

- **Must support (v1)**:
  - Inputs: `query`, `types`, `orFilters`, `count`; `start` for non-scroll; `sortInput` accepted but ignored in semantic v1; `searchFlags` accepted (highlighting is a no-op in semantic).
  - Outputs: `count`, `total`, `searchResults[].entity{ urn (and minimal name fields via existing resolvers) }`, `facets`, and `score` (present for all modes - BM25 for KEYWORD, cosine similarity for SEMANTIC).
  - Behavior: honor `types` and `orFilters` (pre-filter simple terms when safe; post-filter otherwise). Preserve pagination semantics (`start/count`).
- **Nice to have (later)**:
  - `scrollId`/cursor-based pagination for semantic mode. MCP doesn’t use it yet; we can return null/ignore initially.
  - Additional semantic metadata (e.g., snippets) if we choose to add later.
  - Highlighting (available in `KEYWORD`; ignored in `SEMANTIC` v1 even if clients pass `skipHighlighting`).

Notes:

- MCP depends on `facets` being present in the response contract.
- Facet strategy (v1 default): compute facets via the keyword path in parallel with semantic results.
  - Implementation: call `aggregateAcrossEntities` (preferred) or `searchAcrossEntities` with `count: 0`, using `query: "*"`, same `types` and `orFilters`, and `searchFlags: { skipHighlighting: true, maxAggValues: N }`.
  - Response: return semantic results and totals from the semantic query; return `facets` from the keyword aggregation.
  - Trade-off: facets reflect the full lexical match set, not the semantic candidate pool; acceptable for v1 and stable for MCP.
- Highlighting: Not supported in `SEMANTIC` mode. The `skipHighlighting` flag is accepted for compatibility but ignored; only `KEYWORD` mode may return highlights.

Future (nice-to-have): adaptive oversampling

- Increase `k_internal` in small steps (e.g., +100) until a similarity knee/plateau is detected, additional batches yield < X above-threshold hits, or the latency budget is reached. Optionally cache the candidate set behind a server-side cursor for deep paging.
- Purpose: improve ranking quality only. Facets in v1 remain keyword-derived regardless of oversampling.
- Cap: keep maximum `k_internal` in the low hundreds to bound cost (e.g., ≤ 500).
- Fallback: if no plateau within the cap, proceed with current candidates for ranking and still compute facets via `aggregateAcrossEntities`.

### Prerequisites

- OpenSearch indices exist with kNN mapping:
  - `_semantic` variants exist for all searchable entity indices (e.g., `datasetindex_v2_semantic`, `chartindex_v2_semantic`, `dashboardindex_v2_semantic`, ...).
  - See: `semantic-search-poc/create_semantic_indices.py` and `OPENSEARCH_SCHEMA.md`.
- Documents backfilled with embeddings:
  - Start with datasets; extend to charts/dashboards.
  - See: `semantic-search-poc/backfill_dataset_embeddings.py`.
- Embedding provider configured via env (Cohere or Bedrock).
- Services can reach OpenSearch with appropriate auth (basic or SigV4).

### Semantic index parity (required)

- For every searchable entity type (source of truth: `SEARCHABLE_ENTITY_TYPES`), create a `_semantic` counterpart of its base search index.
  - Compute base names via `IndexConvention.getEntityIndexName(entity)` (respects env-specific index prefixes), then append `_semantic`.
  - Example: `datasetindex_v2 → datasetindex_v2_semantic`, `chartindex_v2 → chartindex_v2_semantic`, `dashboardindex_v2 → dashboardindex_v2_semantic`, etc.
- This guarantees a uniform resolver path (no runtime existence checks) and simplifies failover. Empty indices are acceptable until backfill completes.
- Reference list of base indices: see `SEMANTIC_SEARCH_PROJECT.md` → “Searchable Indices Coverage”.

### API design (GraphQL)

- Reuse existing endpoints and pagination semantics.
- Mode selection uses the existing `searchFlags.searchMode` field (no new top-level input field required).
  - Enum: `SearchMode { KEYWORD, SEMANTIC }` (server default: KEYWORD when unset).
  - Clients set `searchFlags: { searchMode: SEMANTIC }` to request semantic; omit to use keyword.
- Do not expose `k`, model keys, or index suffixes.
- Internals compute an appropriate internal k based on `start + count` with headroom.

#### Search mode integration (routing and behavior)

- Resolver routing:
  - Read `opContext.getSearchContext().getSearchFlags().getSearchMode()`.
  - Route to keyword path when null or `KEYWORD`; route to semantic service when `SEMANTIC`.
- Defaults and fallbacks:
  - If semantic gating denies access or errors occur, fall back to keyword and return keyword results (or configurable auth error).
  - When in semantic mode, facets are sourced via parallel keyword aggregation as defined above.
- Clients:
  - MCP: pass `searchFlags: { searchMode: SEMANTIC, skipHighlighting: true, maxAggValues: N }` when semantic is desired; otherwise omit `searchMode`.
  - Slack: same pattern; if user not allowlisted, omit `searchMode` to use keyword.
- Observability: log selected mode, gating decision, and fallback reason when applicable.

Example (client):

```graphql
query Search($input: SearchAcrossEntitiesInput!) {
  scrollAcrossEntities(input: $input) {
    start
    count
    searchResults { entity { urn, ... } }
  }
}

// Variables
{
  "input": {
    "query": "customer churn",
    "start": 0,
    "count": 10,
    "types": [DATASET, CHART, DASHBOARD],
    "orFilters": [],
    "searchFlags": { "searchMode": "SEMANTIC" }
  }
}
```

### Resolver behavior (mode = SEMANTIC)

- Enforce allowlist gating using authenticated actor; if not allowed, fall back to KEYWORD or return an auth error (configurable).
- Generate query embedding via env-configured provider (with optional local cache).
- Map `types` to semantic indices; support multi-index queries for "ALL".
- Compute `internal_k = clamp(max(start+count+headroom, count*factor), k_max)`.
- Build nested kNN query on `embeddings.cohere_embed_v3.chunks.vector` with minimal pre-filters (e.g., platform) when safe.
- Collect hits → URNs; dedupe; post-filter via existing GraphQL layer for auth and complex filters; slice by start/count.
- Optionally enrich each result with `snippet` (top chunk text) and `score` in a nullable extension field (can be added later without breaking clients).
- Timeouts and graceful fallback to KEYWORD path on provider/OS errors.

### Feature gating (per-user)

- Config:
  - `SEMANTIC_SEARCH_ENABLED=true|false`
  - `SEMANTIC_SEARCH_ALLOWED_USERS=urn:li:corpuser:alice,urn:li:corpuser:bob`
- Enforce in GraphQL resolver (primary), and redundantly in Slack/MCP (UX).
- Log and emit telemetry for usage and denials.

### OpenSearch details (server-owned)

- Indices: `_semantic` variants for all searchable entity indices (index parity with base indices), e.g., `datasetindex_v2_semantic`, `chartindex_v2_semantic`, `dashboardindex_v2_semantic`.
- Fields required in `_source` for mapping back: `urn`, `name`, `platform`, `qualifiedName`, `typeNames`, and `embeddings.cohere_embed_v3.chunks.text`.
- Query pattern mirrors `interactive_search.py` (nested kNN over chunks; score_mode max).

### Documentation updates

- Audit and update any docs or sections that list only a subset of semantic indices to reflect full index parity (all searchable entity types have `_semantic` variants).

### Client changes

- Slack bot (in `datahub-integrations-service`):
  - Routing: add a subcommand or flag to request semantic mode (e.g., `/datahub search --semantic <q>` or `/datahub semantic <q>`). If user is not allowlisted, fall back to keyword search.
  - Handler: call GraphQL with `searchFlags: { searchMode: SEMANTIC }`; reuse existing renderers. `semanticSimilarity` is available to the LLM/tooling; do not display it in Slack messages by default.
- MCP server:
  - Add a new tool `semantic_search(query, types?, size?)` that delegates to GraphQL with `searchFlags: { searchMode: SEMANTIC }`. Keep existing `search` unchanged.
  - Gate in-tool for clear error messaging; rely on server gating for enforcement.

### Configuration/env

- Embeddings:
  - `EMBED_PROVIDER=cohere|bedrock`
  - If `cohere`: `COHERE_API_KEY`, optional `COHERE_MODEL` (default: embed-english-v3.0)
  - If `bedrock`: AWS creds, `AWS_REGION`, optional `BEDROCK_MODEL` (default: cohere.embed-english-v3)
- OpenSearch:
  - `OPENSEARCH_HOST`, `OPENSEARCH_PORT`, and optional basic auth; or
  - `OPENSEARCH_ENDPOINT`, `OPENSEARCH_AUTH_MODE=basic|sigv4` (AWS SigV4 where applicable)
- Feature flags:
  - `SEMANTIC_SEARCH_ENABLED`, `SEMANTIC_SEARCH_ALLOWED_USERS`

### Backfill pipeline

1. Create semantic indices (one-time per env).
2. Backfill datasets (batch), then charts and dashboards; monitor rate limits and index health.
3. Schedule periodic refresh jobs for new/updated entities (phase 2).

### Rollout plan

1. Enable feature flags in staging; allowlist internal POC users.
2. Validate end-to-end: indexing, resolver gating, Slack/MCP flows, latency.
3. Gradually expand allowlist; monitor usage and errors.
4. Consider reranking post-POC.

### Testing

- Unit tests for resolver paths (KEYWORD/SEMANTIC), allowlist enforcement, fallback behavior.
- Integration tests with mocked embeddings/OpenSearch; golden tests for Slack render.
- Diagnostics with queries from `OPENSEARCH_SCHEMA.md`.

### Telemetry & SLOs

- Metrics: time to embed, OpenSearch latency, total request latency, hit counts, fallback rate, auth drop rate.
- Logs: sampled queries and top-k counts (no PII), allowlist denials.
- SLO: 95th percentile < 2.0s end-to-end for count ≤ 10.

### Risks & mitigations

- Index gaps → fallback to KEYWORD; track fallback rate.
- Provider outages → short-circuit to KEYWORD; alert.
- Auth drop reduces result set → use internal headroom and post-filter.
- Pagination instability → deterministic tie-break (score desc, then \_id).

### Milestones (suggested)

- M1: Indices + dataset backfill + resolver (SEMANTIC) + MCP tool behind allowlist.
- M2: Slack command + snippets + basic telemetry.
- M3: Charts/dashboards backfill + expanded allowlist + perf tuning.
- M4: UI integration discussion.

### Open questions

- Do we expose snippet/score in v1 GraphQL response, or add later?
- Do we pre-filter more facets in OS query vs post-filter in GraphQL for consistency?
- Long-term: real-time embedding updates vs batch cadence.

### POC Progress

**Done**

- Backend semantic service: Implemented `SemanticEntitySearchService` using OpenSearch nested kNN with pre-filtering and `_semantic` index suffix resolution. Preserves pagination via oversampling and slicing.
- Result parity: Populates `entity`, `score`, and adds `features` (SEARCH_BACKEND_SCORE, optional QUERY_COUNT) and `extraFields` (stringified `_source`). `matchedFields` intentionally omitted in SEMANTIC v1.
- Shared utilities: Added `SearchResultUtils.buildBaseFeatures` and `toExtraFields`, adopted by both keyword and semantic paths for consistency.
- DI wiring: Added factories for the semantic service and an `EmbeddingProvider` (default `DisabledEmbeddingProvider`). Injected into `SearchService` immutably.
- Mode routing: `SearchService` now routes on `searchFlags.searchMode` for `search(...)` and `searchAcrossEntities(...)`. SEMANTIC branch preserves kNN order via `SimpleRanker` only.
- Facets in SEMANTIC: Compute via keyword path (aggregations-only) in parallel and attach to semantic results; skip facet fetch when `skipAggregates=true` or facet list is empty. Also apply legacy `withAdditionalAggregates`.
- Parallelization: Keyword facet aggregation is fired in parallel with the semantic query using a `CompletableFuture` and joined afterward.
- Tests: Updated fixtures to mock the new semantic dependency; added unit tests for `SearchResultUtils`.
- Documentation: Added method-level docstrings to `SemanticEntitySearchService` and interface Javadoc to `SemanticEntitySearch`.
- GraphQL resolver integration: `SearchFlagsInputMapper` now correctly maps `searchMode` from GraphQL to backend. Added tests in `SearchAcrossEntitiesResolverSemanticTest` to verify routing.
- Score field exposure: Exposed existing `score` field in GraphQL `SearchResult` for all search modes (BM25 for KEYWORD, cosine similarity for SEMANTIC). Added `MapperUtils` logic to map backend Double to GraphQL Float with tests.
- Search metadata fields: Added `searchType` and `scoringMethod` to `SearchResultMetadata` to indicate which search algorithm was actually used (may differ from requested due to fallbacks) and how scores were calculated. Set to "KEYWORD"/"BM25" for keyword search and "SEMANTIC"/"cosine_similarity" for semantic search.
- Embedding providers: Wired real Bedrock Cohere provider via integrations service API; Java `IntegrationsServiceEmbeddingProvider` delegates to `IntegrationsService`; manual 2048-char truncation for Bedrock Cohere; embeddings are L2-normalized; integration test passes.

- Gating: Environment-level enable/disable implemented. When disabled, backend throws `SemanticSearchDisabledException`; GraphQL resolvers map to `BAD_REQUEST` with a clear message (no fallback). Configured via `searchService.semanticSearchEnabled` (env var `SEARCH_SERVICE_SEMANTIC_SEARCH_ENABLED`) and enabled in quickstart compose. HYBRID mode removed from enums/schema and clients.

**TODO (for POC)**

- Dynamic switch: Discuss wiring the semantic search enable/disable to GlobalSettings (DB) or adding a hot-reloadable configuration (e.g., Spring Cloud @RefreshScope, GlobalSettings, or similar) to avoid GMS restarts when toggling.
- Observability: Add metrics (embed time, OS latency, total latency, fallback rate) and logs per plan.
- Tests: Add more unit/integration tests for SEMANTIC routing, allowlist enforcement, facet parity, and error/fallback paths.
- Scroll: Keep keyword-only in v1 (as implemented). Revisit later for server-side cursor/search_after approach if needed.
- Optional enhancements: snippets, better executor control for parallel facet fetch, and addressing remaining linter warnings around raw generics/deprecated APIs.
- Score denormalization: Current semantic `score` is normalized to [0,1]. Denormalize to true cosine similarity in [-1,1] (e.g., `cos = 2*score - 1`) before exposing/using it for comparisons.
