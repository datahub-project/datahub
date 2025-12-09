### Tech Spec — DataHub Semantic Search

- Author: Alex Skurikhin
- Stakeholders/Reviewers: GMS, Search, Frontend, Infra, Docs
- Status: ~~Draft~~ **Implementation Phase 1 Complete - Now Available in OSS**
- Last Updated: ~~2025-08-08~~ **2025-12-08**
- Template reference: "Tech Spec — Billing Metrics" on Notion ([Notion template link](https://www.notion.so/acryldata/Tech-Spec-Billing-Metrics-22cfc6a642778025a872fbbd2093ffa1))

> **🎉 OSS Availability (2025-12-08)**
>
> Semantic search has been ported to open-source DataHub! This feature is now available in both DataHub Cloud and OSS.
> OSS users can enable semantic search with OpenSearch 2.17+ (k-NN plugin) and a configured embedding provider.

> **📢 Important Architecture Update (2025-08-27)**
>
> The semantic search implementation has been revised from the original searchMode routing approach to **dedicated semantic search endpoints**. This change provides:
>
> - Complete separation between keyword and semantic search
> - Zero merge conflicts with upstream DataHub
> - Cleaner architecture and maintainability
> - Independent evolution of search types
>
> See [APIs & Integration](#apis--integration) section for details on the new endpoint structure.

### Summary

- Add semantic search to DataHub by storing embeddings in OpenSearch and running semantic vector search with existing filters; true hybrid (lexical + neural) is optional/TBD.
- MVP uses template-generated natural language descriptions embedded via AWS Bedrock models — Cohere Embed v3 (primary) and Amazon Titan Embeddings v2 (secondary); multi-chunk with overlap from day one.
- Phase 1: Offline embedding generation, OpenSearch index extension, filtered neural query path (hybrid optional, behind a flag), evaluation and Slack UX validation.
- Phase 2: Real-time updates, cost controls, monitoring, and SLOs.

### Background & Motivation

- Keyword-only search struggles with intent; users want natural language discovery.
- Opportunity to improve discoverability, reduce time-to-insight, and increase search success/adoption.
- Sources: [SEMANTIC_SEARCH_PROJECT.md](https://github.com/acryldata/datahub-fork/blob/5df3dd7a8e0618c660f7a6a49df3b252de3a14c9/SEMANTIC_SEARCH_PROJECT.md), [semantic-search-poc/semantic search POC.md](https://docs.google.com/document/d/1k135S77MHM5Jy3aA17WXpdsbC6KMs5U1gnRHQJRlaoc/edit?tab=t.0#heading=h.us4taqnovgog).

### Goals

- Integrate semantic search across all `SEARCHABLE_ENTITY_TYPES` indices without breaking existing filters.
- Provide filtered neural search with optional re-ranking and progressive relaxation (hybrid lexical+neural is TBD; see Problems, Challenges, and Questions).
- Deliver an MVP that is fast to implement, testable with real users, and cost-conscious.
- Maintain flexibility for multi-model embeddings, chunking, and evolution paths.

### Non-Goals

- Replacing keyword search.
- Full lineage-aware reasoning or field-by-field embeddings in Phase 1.

### Requirements

- Functional
  - Filtered neural search endpoint(s) combining semantic similarity with existing filters. Optional hybrid (lexical + neural) evaluation behind a flag.
  - Extend all entity indices (e.g., `datasetindex_v2`, `chartindex_v2`, `dashboardindex_v2`, etc.) with an embedded vector field.
  - Pre-filter via facets: owner(s), domain, platform, env, tags, origin, etc.
  - Multi-model-ready schema to support future embeddings.
- Non-Functional
  - Backward compatible with keyword-only search; no downtime during rollout.
  - Performance target: p95 < 500 ms for search (Phase 2 target).
  - Observability: log model version, generation time, and chunk metadata.
  - Cost tracking and token accounting.

### Users & Use Cases

- Data Scientists, Product Managers, Marketing; see personas and NL questions in [SEMANTIC_SEARCH_PROJECT.md](https://github.com/acryldata/datahub-fork/blob/5df3dd7a8e0618c660f7a6a49df3b252de3a14c9/SEMANTIC_SEARCH_PROJECT.md).
- Slack bot A/B validation of semantic vs keyword relevance during Phase 1.

### High-Level Design

- Embedding Generation
  - MVP: Template-based natural descriptions combining name, description, limited schema roles, domain vocabulary, and usage context when present.
  - Exclude URNs/UUIDs/emails, platform/env boilerplate, long enumerations, timestamps/flags from embedding text.
  - Models: AWS Bedrock Cohere Embed v3 (preferred; 1024 dims) and Amazon Titan Embeddings v2 (fallback; 1024 dims).
  - Chunking: Required from day one. Use adaptive chunking with overlap to respect model input limits and maintain semantic continuity.
- Indexing
  - Extend existing OpenSearch indices with a nested `embeddings` object that contains a `cohere_embed_v3` sub-object with `chunks` (and optionally `titan_embed_v2`).
  - Phase 1: Offline backfill pipeline; no real-time updates.
  - Phase 2: Real-time updates triggered by entity lifecycle changes.
  - Note: See Problems, Challenges, and Questions — OpenSearch cluster strategy.
- Querying
  - Decompose user query into filters + semantic text.
  - Apply filters to narrow candidates; perform nested knn on chunk vectors, with score_mode=max.
  - Optional reranking via Cohere Rerank (AWS Bedrock) on top-M candidates; fusion/RRF out-of-scope for V1. Result size and rerank budget TBD (see Problems, Challenges, and Questions).
  - Note on terminology: filters (bool filter clauses) are always included; “hybrid” refers to combining lexical (BM25) and neural (vector) scores, not merely applying filters. V1 assumes filters + neural only.
  - Dynamic cutoff of tail: Rather than a fixed top-K, examine the shape of similarity scores and cut at the plateau/elbow. Use simple knee-detection or derivative-based heuristics; optionally combine with absolute/relative floors. If few items remain above the cutoff, return only those; optionally augment with graph neighbors (see below).
  - Optional neighbor augmentation: If above-threshold results are sparse, include 1-hop graph neighbors (e.g., upstream/downstream datasets, linked dashboards/charts, owners) for context, labeled as related results.

### Detailed Design

#### Embedding Text Strategy (MVP)

Intent: Generate natural-language descriptions for each entity rather than embedding raw key–value pairs (JSON or delimiter-concatenated). Natural text aligns with how embedding models are trained, captures relationships and intent across fields, and yields higher-quality semantic retrieval than flat KVP formats.

- Include in generated text

  - Natural description (purpose and what it’s about)
  - Human-readable domain vocabulary
  - A few representative field roles (identifier, metric, date)
  - Data grain/time coverage if present
  - Usage context when evidenced (analytics, reporting, segmentation)

- Exclude from generated text
  - IDs/URNs/UUIDs, account/project IDs
  - Platform/env boilerplate and full qualified names
  - Long field enumerations and exhaustive schemas
  - System timestamps/flags and other boilerplate metadata

Note: PII/email inclusion policy is a product decision; see Problems, Challenges, and Questions.

#### Chunking Strategy (Day 1)

- Adaptive segmentation by semantic boundaries (paragraphs/sections), with safety fallback to token-length splitting.
- Default parameters (tunable): target ~400 tokens per chunk with ~80-token overlap; hard max 450 tokens per chunk to respect Cohere’s smaller context window. Apply same defaults to Titan v2 for MVP to keep behavior consistent; revisit after profiling.
- Always store all chunks for each entity; do not collapse to a single chunk even for short content.
- Query text is typically short and embedded as a single vector; no query chunking initially.

#### Index Settings (enable k-NN)

Enable k-NN at index creation time.

```json
{
  "settings": {
    "index.knn": true
  }
}
```

#### OpenSearch Mapping (per index)

```json
GET datasetindex_v2/_search
{
  "_source": ["urn","name","description","platform","tags"],
  "query": {
    "nested": {
      "path": "embeddings.cohere_embed_v3.chunks",
      "query": {
        "knn": {
          "embeddings.cohere_embed_v3.chunks.vector": {
            "vector": [ /* query embedding */ ],
            "k": 200,
            "filter": {
              "bool": {
                "filter": [
                  { "term": { "platform": "snowflake" } },
                  { "terms": { "tags": ["business_critical"] } }
                ]
              }
            }
          }
        }
      },
      "score_mode": "max",
      "inner_hits": {
        "_source": ["embeddings.cohere_embed_v3.chunks.text","embeddings.cohere_embed_v3.chunks.position"],
        "size": 1
      }
    }
  },
  "size": 10
}
```

#### Sample Filtered k-NN Query (Nested multi-chunk with inner_hits)

```json
{
  "size": 10,
  "_source": ["urn", "name", "description", "platform", "tags"],
  "query": {
    "nested": {
      "path": "embeddings.cohere_embed_v3.chunks",
      "score_mode": "max",
      "inner_hits": {
        "size": 1,
        "_source": [
          "embeddings.cohere_embed_v3.chunks.text",
          "embeddings.cohere_embed_v3.chunks.position",
          "embeddings.cohere_embed_v3.chunks.character_offset",
          "embeddings.cohere_embed_v3.chunks.character_length"
        ],
        "sort": [{ "_score": "desc" }]
      },
      "query": {
        "knn": {
          "field": "embeddings.cohere_embed_v3.chunks.vector",
          "query_vector": [
            /* query embedding */
          ],
          "k": 50,
          "num_candidates": 200,
          "filter": {
            "bool": {
              "filter": [
                { "term": { "platform": "snowflake" } },
                { "terms": { "tags": ["business_critical"] } }
              ]
            }
          }
        }
      }
    }
  }
}
```

#### Data Flows

- Backfill: GraphQL fetch (or OpenSearch read) -> transform -> embed -> index (offline pipeline). See Problems, Challenges, and Questions — Backfill data source. If GraphQL is chosen, see GraphQL Backfill Implementation.
- Search: Query -> extract filters + semantic text -> get query embedding -> filtered vector search -> optional re-rank -> results.

#### APIs & Integration

> **⚠️ Architecture Update (2025-08-27)**: The implementation approach has been revised from the original searchMode routing to dedicated semantic search endpoints for cleaner separation of concerns and to avoid upstream merge conflicts.

##### ~~Original Approach (Obsolete)~~

- ~~New semantic search path using searchMode routing parameter within existing search endpoints~~
- ~~SearchFlags.searchMode = SEMANTIC to trigger semantic search path~~
- ~~Single endpoint handling both keyword and semantic search based on mode~~

##### **Current Approach (Implemented)**

- **Dedicated semantic search endpoints** completely separate from keyword search
- **GraphQL Endpoints:**
  - `semanticSearch(input: SearchInput!): SearchResults` - single entity type semantic search
  - `semanticSearchAcrossEntities(input: SearchAcrossEntitiesInput!): SearchResults` - multi-entity semantic search
- **Clean architectural separation:**
  - `SemanticSearchService` - dedicated service for semantic search operations
  - `SearchService` - handles keyword search only (semantic logic removed)
  - No searchMode field or routing logic required
- **Benefits of dedicated endpoints:**
  - Zero merge conflicts with upstream DataHub
  - Cleaner code separation and maintenance
  - Independent evolution of semantic and keyword search
  - Simpler testing and debugging
  - No performance overhead from routing logic
- Optional Slack bot endpoint for A/B preference logging

#### GraphQL Backfill Implementation (if chosen)

- Endpoint & Auth: Use the DataHub GraphQL endpoint with a service account token; apply least-privilege scopes.
- Entity coverage: Iterate over `SEARCHABLE_ENTITY_TYPES`; for each type, paginate through all entities.
- Pagination & batching: Cursor-based pagination; batch size 100–500 per page; parallelize pages per entity type with a global QPS cap.
- Selected fields (per entity): `urn`, `name`, `description`, key schema fields (name, description, role), `platform`, `domain`, `tags`, `owners`, and lightweight usage/lineage counts if cheaply available.
- Transformation & idempotency: Build canonical natural text per entity; compute a stable hash; skip embedding if hash unchanged (embedding cache).
- Error handling: Exponential backoff, bounded retries, dead-letter queue for persistent failures; resumable checkpoints per entity type and page cursor.
- Rate limits & load: Apply conservative QPS and concurrency to avoid impacting GMS; schedule backfill during off-peak windows when possible.
- Observability: Log processed counts, tokens, cost, and per-model summaries; emit metrics and alerts on failure rates.

#### Real-Time Updates (Phase 2)

- Change detection on semantically relevant fields (name, description, tags, schema).
- Incremental regeneration with caching keyed on canonical text; batch reprocessing for strategy/model changes.

### Dependencies

- OpenSearch cluster with k-NN enabled and support for `knn_vector` (e.g., HNSW/Lucene with `space_type: "cosinesimil"`).
- AWS Bedrock access (IAM permissions, model enablement for Cohere Embed v3 and Amazon Titan Embeddings v2), secret/IAM role management.
- DataHub GraphQL endpoint to fetch entities.
- Slack bot (optional) for Phase 1 validation.

### Monitoring & Observability

- Metrics: embedding generation success rate, search p95 latency, daily cost, usage counts, Slack user preference rate.
- Logs: model_version, generated_at, chunk counts, token counts.
- Alerts: failures in backfill/updates, cost threshold breaches.

### Risks & Mitigations

- Search quality uncertainty: run offline experiments, Slack A/B testing, multiple content strategies.
- Integration complexity: offline-first Phase 1; real-time in Phase 2.
- Cost risk: caching, batching, selective embedding, model right-sizing.
- Mapping lock-in: start with multi-model-ready nested schema; commit to multi-chunk from day one to avoid schema churn later.

### Problems, Challenges, and Questions

- OpenSearch cluster strategy (existing vs separate)

  - Long-term target: integrate semantic vectors into existing indices/cluster for unified operations and simpler product experience.
  - Proposal: use the existing OpenSearch cluster. Validate on a staging environment with real data before enabling in production; gate with feature flags and circuit breakers.
  - Trade-offs:
    - Pros (separate): isolation, safer experimentation, independent scaling/tuning, easier rollback.
    - Cons (separate): additional ops cost, cross-cluster networking, eventual migration complexity.
  - Decision: TBD. Proposal is to use the existing cluster; test extensively on staging with real data; enable behind a flag; monitor; if issues arise, disable the flag and revert to keyword-only.
  - Action items: validate knn/neural search compatibility with current cluster version/plugins; confirm mapping constraints and resource sizing for vector fields; run load tests in staging; prepare backout plan.

- Multilingual support (English-only vs multilingual)

  - Context: Most metadata and queries are expected to be in English.
  - Option A: English-only models (default); simpler, lower cost, better quality for English.
  - Option B: Multilingual embedding model (e.g., Cohere multilingual) if non-English content/queries emerge.
  - Decision: TBD. Working assumption: English-only for MVP; revisit if non-English usage is observed.

- Do we need true hybrid (lexical + neural), or just filters + neural?

  - Clarification: "Filters + neural" = apply facet filters (non-scoring) and then kNN vector search. From OpenSearch's perspective, this is not hybrid; it's filtered vector search.
  - "Hybrid" here means combining lexical BM25 scoring with neural vector scoring for the same query, typically via score fusion (weighted sum), Reciprocal Rank Fusion (RRF), or a cross-encoder re-ranker.
  - Pros (true hybrid): better robustness for short queries, exact term matching, improved precision in top-k.
  - Cons: extra complexity, potential latency/cost (especially with cross-encoder re-rankers), more tuning.
  - ~~Decision: TBD. Proposed path: MVP ships with filters + neural; evaluate true hybrid as an experiment (behind a flag) and adopt if it demonstrably improves precision@k within SLO.~~
  - **Decision (2025-08-27)**: With dedicated semantic search endpoints, hybrid becomes a potential future enhancement to the semantic endpoints only. The clean separation allows:
    - Keyword search remains pure BM25 through existing endpoints
    - Semantic search uses pure neural (with filters) through dedicated endpoints
    - Future hybrid could be added to semantic endpoints without affecting keyword search
    - Users explicitly choose search type via endpoint selection rather than mode flags

- Re-ranking: do we need a second-stage reranker?

  - Definition: two-stage ranking where we first retrieve candidates and then reorder them with a stronger scorer.
  - Options:
    - Cross-encoder re-ranker (Cohere Rerank via Bedrock): apply to top M (e.g., 50–100) candidates to improve precision at top-k.
  - Guardrails: enable only when latency budget allows; gate behind a flag; log impact.
  - Decision: Use Cohere Rerank for V1; exclude fusion/RRF from scope. Revisit fusion/RRF only if reranker is insufficient or too costly.
  - Open question: How many results should we return and rerank? Returning 50–100 entities may be preferable given small entity size. Cohere Rerank may cap input list size around 100 per call (to confirm with Bedrock limits). Decide on M (rerank candidates) and N (final returned results).

- Progressive relaxation: do we broaden constraints when results are sparse?

  - Definition: a controlled fallback that gradually relaxes constraints to reach a minimum number of results.
  - Suggested sequence (stop when ≥ target results, e.g., 5):
    1. Strict filters + kNN over chunks.
    2. Increase k (e.g., 100 → 300).
    3. Relax least-critical filters in order: owners → tags → domain; keep platform/env last.
    4. Expand entity scope (e.g., include dashboards/charts if starting with datasets).
    5. Lower score threshold slightly.
    6. Fallback to keyword-only if still sparse.
  - Guardrails: cap relax levels, enforce latency budget, clearly indicate “broadened your search.”
  - Decision: TBD. Suggested default: trigger when < target results above threshold (not just raw count); max 3 relax levels.

- Result cutoff policy and neighbor augmentation

  - Question: How should we cut off low-quality tails? OpenSearch kNN returns top-K, but we should only surface items above quality thresholds.
  - Cutoff heuristics: elbow/plateau detection on the score curve (primary), optionally combined with absolute cosine floor (t_abs) and/or relative-to-top (within p%). Prefer the simplest robust rule for V1.
  - Target counts: Decide min (N_min) and max (N_max) results to display after cutoff. If fewer than N_min above threshold, consider progressive relaxation or neighbor augmentation.
  - Neighbor augmentation options: include 1-hop lineage (upstream/downstream), related dashboards/charts, same-owner items; must be clearly labeled and ranked after primary matches.

- Backfill data source (GraphQL vs OpenSearch)
  - Context: Much of the entity data already exists in OpenSearch; embeddings need a consolidated, human-readable text. We can fetch via GraphQL (canonical GMS view) or read directly from OpenSearch indices.
  - OpenSearch read — Pros: faster bulk scroll/scan, less load on GMS, simpler infra for Phase 1 if using a separate cluster; Cons: may miss fields not indexed (if any), potential staleness/denormalization differences, per-index schema drift, pagination/scroll complexity, permissions parity.
  - GraphQL fetch — Pros: canonical schema, easier to compose cross-aspect fields for natural text, respects product semantics/permissions, stable contracts; Cons: adds load to GMS, rate limits, slower for large backfills.
  - Decision: TBD. Proposal: prefer OpenSearch read for Phase 1 to reduce complexity and load; use GraphQL when it provides materially better fields/joins for higher-quality natural text or where required fields are not indexed.

### Alternatives Considered

- Raw combined-field concatenations vs natural-language templates (choose templates for quality).
- Single flat vector field vs nested chunk structure (choose nested for future readiness).
- OpenSearch specifics (confirm cluster/version and kNN settings).

### Rollout Plan

> **Implementation Status (2025-08-27)**: Phase 1 architecture completed with dedicated semantic search endpoints approach.

- ~~Phase 1 (4–6 weeks)~~ **Phase 1 (Completed)**
  - ~~Extend indices with `embeddings` nested object.~~ ✅ Indices extended
  - ~~Offline backfill pipeline and semantic filtered-neural query path (hybrid optional behind a flag).~~
  - **Implemented instead:** Dedicated semantic search endpoints with clean separation:
    - ✅ `SemanticSearchService` created with full semantic search logic
    - ✅ `semanticSearch` and `semanticSearchAcrossEntities` GraphQL endpoints added
    - ✅ Removed searchMode routing - no flags or mode switching needed
    - ✅ SearchService cleaned of all semantic logic
  - ✅ Content strategy experiments and integration testing completed
  - ✅ Local deployment and end-to-end validation successful
  - Go/No-Go for Phase 2 based on success metrics.
- Phase 2 (6–8 weeks, if Go)
  - Real-time embedding updates and caching.
  - Monitoring, SLOs, cost optimization.
  - Deeper UI integration and documentation.

### Success Metrics

- Relevance: top-3 relevance > 75% vs keyword-only.
- User adoption: increased searches and engagement.
- Slack A/B: > 60% preference for semantic/hybrid results.
- Slack A/B: > 60% preference for semantic (filtered neural) results.
- Coverage: support all `SEARCHABLE_ENTITY_TYPES` indices.

### Open Questions / Inconsistencies to Resolve

- OpenSearch target: Confirm cluster/version and kNN plugin/settings; update any legacy references from “Elasticsearch.”
- Model selection: Cohere Embed v3 English vs Multilingual? When to use Titan v2 as fallback or for specific languages/cost profiles?
- UI: Any frontend exposure beyond Slack in Phase 1 - likely not?
- Default chunking parameters: confirm target tokens per chunk and overlap for Cohere/Titan in the MVP.

### Testing Strategy

- Unit: transformation functions, embedding orchestrators, query builders.
- Integration: backfill dry runs; search results vs baselines.
- Validation: human relevance judgments; Slack preference logging.
- Load: knn under filtered candidate sizes; hybrid re-ranking performance.

### Timeline & Resourcing

- Phase 1: 1 backend engineer (3–4 weeks); minimal infra; optional Slack help.
- Phase 2: 1 backend (6–8 weeks) + 0.5 devops (2–3 weeks) for monitoring/deployment.

### Appendix

- See [SEMANTIC_SEARCH_PROJECT.md](https://github.com/acryldata/datahub-fork/blob/5df3dd7a8e0618c660f7a6a49df3b252de3a14c9/SEMANTIC_SEARCH_PROJECT.md) for sample mapping/query JSON and entity coverage details.
- See [semantic-search-poc/semantic search POC.md](https://docs.google.com/document/d/1k135S77MHM5Jy3aA17WXpdsbC6KMs5U1gnRHQJRlaoc/edit?tab=t.0#heading=h.us4taqnovgog) for risk-optimized slicing and validation plan variants.

### Feedback from GPT‑5 Pro

# TL;DR (what to change now)

1. **Cohere Embed specifics**

   - v3 models cap at **~512 tokens per input**; your 400‑token target with ~80 overlap is perfect.
   - Always set `input_type`: use `search_document` for documents and `search_query` for queries; it materially affects embedding quality.

2. **Titan v2 knobs**

   - Titan Embeddings v2 emits **1024‑dim** vectors and also supports **smaller dims (256/512)**—worth testing to cut cost/storage with minimal quality loss.

3. **Reranker envelope**

   - Cohere Rerank 3/3.5 can handle large lists, but **don’t send huge candidate sets**—100–200 is plenty. (Cohere docs recommend ≤1,000 docs per call; you’ll want far less for latency.) Put it behind a flag as you noted.

4. **Cost control via quantization**
   - Plan for **byte (int8) or scalar quantization** of vectors in OpenSearch and/or request **compressed embeddings** from Cohere (int8) when you’re ready. This slashes RAM/disk ~4× vs float32 with small recall impact.

---

# Detailed feedback (by area)

## Index + Query design

- **Mapping tweak (OpenSearch)**  
  Replace `dense_vector` with `knn_vector` and add index‑level `knn: true`. For nested chunks, keep your structure but push vectors under a nested path. You can also tune HNSW (`m`, `ef_construction`) per index.

- **Filters + vector search**  
  Your “filters + neural” plan is great for MVP. In OpenSearch, prefer **putting filters inside the `knn` clause** to pre‑filter candidates rather than only at the outer bool. This prevents wasted work in the ANN stage. Use `inner_hits` to return the best‑matching chunk and snippet.

- **Hybrid (optional)**  
  If you experiment with hybrid later, you can implement it via **Search Pipelines** with a **Rerank processor** (and an initial BM25 pass), keeping complexity out of the app layer.

## Embeddings & chunking

- **Cohere v3 limits**: ~512 tokens per input; 1024‑dim output (English & Multilingual v3). Your 400/80 split is safe. Batch up to ~96 inputs per call for throughput.
- **Set `input_type`**: `search_document` (index) vs `search_query` (query). This is a must; quality dips if you forget.
- **Titan v2**: defaults to 1024 dims, but supports **256/512/1024**—try 512 if storage becomes a problem.
- **What to embed**: Your “natural text with semantically useful bits only” approach is exactly right. I’d also:
  - Normalize acronyms (e.g., “PII (personally identifiable information)”) once per entity.
  - Keep a **stable canonical text hash** (you already plan this) to skip re‑embeds.

## Latency budget (p95 target 500 ms)

A realistic split (rough order‑of‑magnitude; verify in staging):

- **Query embed** (Cohere v3 via Bedrock): ~40–120 ms regional.
- **kNN** (filtered, nested, HNSW @k=100): ~50–200 ms depending on shard size and filter selectivity.
- **Rerank** (top M=50–100): ~80–250 ms.  
  If you need to live under **500 ms p95**, gate rerank by: (a) a high cosine threshold, (b) number of hits above threshold, or (c) skip when the query is clearly navigational (“table abc_user”). (Exact times depend on your cluster and quotas—measure before promising.)  
  Tip: Stick a **query‑embed cache** for frequent short queries.

## Storage & cost math (quick sanity checks)

Raw float32 vector size = `dims * 4`. For 1024‑dim that’s **4 KB / chunk**.  
Examples (raw vectors only, not counting graph/segment overhead):

- 500k docs × 2 chunks ≈ **4.10 GB** (float32) → **~2.05 GB (fp16)** → **~1.02 GB (int8)**.
- 1M docs × 4 chunks ≈ **16.38 GB** (float32) → **~8.19 GB (fp16)** → **~4.10 GB (int8)**.  
  OpenSearch HNSW/segment overhead can 2–3× these numbers depending on `m`, `ef_construction`, engine, and quantization—so **quantization is your best lever**.

## Result cutoff & progressive relaxation

- **Elbow/knee** detection on cosine scores is a good start. Also enforce a **min absolute floor** (e.g., cos ≥ 0.25–0.35 for Cohere v3 in many corpora—tune per model).
- If below N_min: raise `k`, then relax least‑critical filters (owners → tags → domain) exactly as you propose. Make the UI say **“broadened your search”** and show what changed.

## Re‑ranking

- Keep it behind a flag and log **hit@k and MRR** impact + latency deltas. Cohere Rerank 3.5 via Bedrock is a good default; cap M at **≤100** unless the query is rare and latency isn’t critical.

## OpenSearch knobs you’ll likely tune

- **HNSW** (`m`, `ef_construction`, `ef_search`) per index/type. Start with `m=16`, `ef_construction=128–256`, raise `ef_search` for recall when needed. (Measure per entity type.)
- **Engines**: Lucene HNSW is simple; Faiss can be faster at scale.
- **Quantization**: Try **byte vectors** (int8) or **scalar quantization** once you have a quality baseline.
- **Pre‑filtering**: Use the `filter` parameter _inside_ `knn` for faster, safer filtering.

## DataHub integration notes

- **Real‑time (Phase 2)**: Hook into GMS change events for the aspects that move the needle (name/description/tags/schema). Feed a debounce window (e.g., 1–5 minutes) to batch churn.
- **Permissions**: Make sure **authorization filters hit before kNN** (pre‑filter) to avoid scoring docs a user can’t see.
- **Backfill source**:
  - **OpenSearch read** wins for Phase 1 speed and load (agree).
  - **GraphQL** for entities where OS lacks necessary fields to craft high‑quality natural text (owners/domain/schema roles).

## Observability (a few adds)

- Log **embedding params** too: `model_id`, `input_type`, `embedding_type` (float vs int8), and `dims`.
- Capture **kNN timings and candidate counts** (pre‑ and post‑filter) plus **rerank timings** to see where p95 is going.
- Emit **score histograms** so you can tune cutoffs per entity type.

---

# Snippets (drop‑in)

## Mapping (nested chunks, OpenSearch)

```json
PUT datasetindex_v2
{
  "settings": { "index": { "knn": true }},
  "mappings": {
    "properties": {
      "platform": { "type": "keyword" },
      "tags": { "type": "keyword" },
      "embeddings": {
        "properties": {
          "cohere_embed_v3": {
            "properties": {
              "chunks": {
                "type": "nested",
                "properties": {
                  "vector": {
                    "type": "knn_vector",
                    "dimension": 1024,
                    "space_type": "cosinesimil",
                    "method": {
                      "name": "hnsw",
                      "engine": "lucene",
                      "parameters": { "m": 16, "ef_construction": 128 }
                    }
                  },
                  "text": { "type": "text", "index": false },
                  "position": { "type": "integer" },
                  "character_offset": { "type": "integer" },
                  "character_length": { "type": "integer" },
                  "token_count": { "type": "integer" }
                }
              },
              "total_chunks": { "type": "integer" },
              "total_tokens": { "type": "integer" },
              "model_version": { "type": "keyword" },
              "generated_at": { "type": "date" },
              "chunking_strategy": { "type": "keyword" }
            }
          }
        }
      }
    }
  }
}
```

(Use the same shape for Titan; just store under `embeddings.titan_embed_v2` with `dimension` aligned to your Titan choice.)

## Filtered nested kNN query with inner_hits

```json
GET datasetindex_v2/_search
{
  "_source": ["urn","name","description","platform","tags"],
  "query": {
    "nested": {
      "path": "embeddings.cohere_embed_v3.chunks",
      "query": {
        "knn": {
          "embeddings.cohere_embed_v3.chunks.vector": {
            "vector": [ /* query embedding */ ],
            "k": 200,
            "filter": {
              "bool": {
                "filter": [
                  { "term": { "platform": "snowflake" } },
                  { "terms": { "tags": ["business_critical"] } }
                ]
              }
            }
          }
        }
      },
      "score_mode": "max",
      "inner_hits": {
        "_source": ["embeddings.cohere_embed_v3.chunks.text","embeddings.cohere_embed_v3.chunks.position"],
        "size": 1
      }
    }
  },
  "size": 10
}
```

(Notice `filter` lives _inside_ `knn` and we return the best chunk via `inner_hits`.)

---

# Open questions you raised (quick takes)

- **Separate vs existing cluster**: Start on **existing** with feature flags + backout, but do a **staging load test** focusing on: index RAM growth, kNN cache behavior, and latency at peak QPS. If you expect heavy rerank + high k, a **separate vector tier** can simplify tuning and rollback later.

- **English‑only vs multilingual**: If your corpus and queries are mostly English, ship English for MVP. Add **multilingual** where logs show need (you can run a sidecar pipeline to embed non‑English with the multilingual model only).

- **Do you need “true hybrid”?**  
  Try **filtered neural + rerank** first. If top‑k precision is still shaky on short queries, pilot **hybrid** via Search Pipelines so you can roll it back easily.

- **How many docs to rerank / return?**  
  Start with **M = 50–100** candidates to rerank; **N = 10–20** final results. Cohere’s API comfortably handles this; bigger lists just cost you latency.

---

# Final nits & nice‑to‑haves

- Add a **query rewriter** (LLM or rules) to normalize “PII” ↔ “personally identifiable information”, “dashboards about churn” → “customer churn metrics dashboard” (kept behind a flag).
- Log **query class** (navigational vs informational) to decide when to skip rerank and how aggressive to be with progressive relaxation.
- Consider **byte/int8 embeddings from Cohere** end‑to‑end once your baseline is set; it plays very nicely with OpenSearch byte vectors.

If you want, I can help you turn this into a 1‑pager “**go/no‑go checklist**” for Phase 1 (with exact thresholds for latency, precision@k, and cost).
