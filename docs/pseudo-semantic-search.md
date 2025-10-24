# `semantic_search` — A Practical “Semantic” Search Without Vectors

_A candidate-generation + rerank approach for DataHub using LLM-assisted keyword expansion, soft facets, and negative handling._

**Status:** draft v1.0
**Owner:** (your team)
**Last updated:** 2025‑10‑20

---

## Table of Contents

1. [Why this exists](#why-this-exists)
2. [High-level flow](#high-level-flow)
3. [Agent vs. Tool: who generates keywords?](#agent-vs-tool-who-generates-keywords)
4. [API surface (service + SDK)](#api-surface-service--sdk)
5. [LLM contract (structured output)](#llm-contract-structured-output)
6. [Query planning](#query-planning)
7. [Retrieval against DataHub](#retrieval-against-datahub)
8. [Negatives (hard exclude vs. soft bury)](#negatives-hard-exclude-vs-soft-bury)
9. [Reranking + final scoring](#reranking--final-scoring)
10. [Observability & evaluation](#observability--evaluation)
11. [Performance, limits, and cost](#performance-limits-and-cost)
12. [Security & privacy](#security--privacy)
13. [Configuration reference](#configuration-reference)
14. [Pseudocode (end-to-end)](#pseudocode-end-to-end)
15. [Examples](#examples)
16. [FAQ](#faq)
17. [Roadmap](#roadmap)

---

## Why this exists

Full semantic search via vector stores is powerful but heavy to operate. For a DataHub-style catalog, **BM25 + good analyzers + disciplined expansion + cross-encoder rerank** captures most of the perceived “semantic” quality with far less complexity.

Key ideas:

- Use an LLM **as a constrained suggester** to extract _anchors_, _phrases_, _synonyms_, and _negatives_.
- Keep retrieval **recall-friendly** with OR queries and **conservative facets** (no hard filters by default).
- **Deboost synonyms** by _budgeting_ (fewer candidates from synonym-only query), not by fragile term weights.
- **Handle negatives** with two tiers: hard exclude (safe patterns) and soft bury (small penalties with exceptions).
- Let a **reranker** (Cohere Rerank / BGE / Jina) pick the true top-5–10 from ~200 candidates.

---

## High-level flow

1. **Classify query** (navigational vs. NL intent).
2. **LLM expansion** → JSON: `anchors`, `phrases`, `synonyms`, `negatives.hard`, `negatives.soft`, `exceptions`.
3. **Gate terms** (stoplist, df-ratio band, length caps).
4. **Two-pass retrieval** via DataHub GraphQL:
   - **Pass A (anchors+phrases)** → take **N₁** (e.g., 120–150).
   - **Pass B (synonyms only)** → take **N₂** (e.g., 50–80).
   - Apply **negatives** to both result sets (drop hard, tag soft).
5. **Merge + dedupe** by URN → ~200 candidates.
6. **Shape docs** (title, path, desc, top columns/tags/owners) → **Rerank** with original user prompt.
7. **Blend tiny lexical signals & penalties** → return top‑K (default 5).

---

## Agent vs. Tool: who generates keywords?

**Recommendation:** keep expansion **inside the `semantic_search` tool** with an _optional override_.
Rationale:

- Expansion quality depends on **index statistics** (document frequencies, stoplists) and **catalog enums** (platform, domain, tags). Housing it with the retrieval code enables tighter validation and adaptation (e.g., df-based gating, dynamic candidate budgets).
- Callers get a **simple API** (`query`, optional `hints`) without worrying about internal heuristics.
- For advanced users or experimentation, allow **optional injected terms** to override or augment the plan (e.g., `injected.anchors`, `injected.synonyms`, `injected.negatives`).

**Alternative (agent does expansion):** works, but you must re-implement df-gating and stoplists in the agent or accept lower precision. Use only if you already have a central “query understanding” agent shared by many tools.

**Hybrid:** default = tool-driven; when the caller provides valid injected terms, the tool **merges** them (after gating) and logs provenance (`source: caller|llm`).

---

## API surface (service + SDK)

### Service (HTTP)

```
POST /semantic_search/v1/search
Content-Type: application/json
Authorization: Bearer <token>
```

**Request JSON**

```json
{
  "user_query": "find pii tables with emails and phone numbers in snowflake marketing, not test",
  "types": ["DATASET", "DASHBOARD", "DATAJOB"],
  "top_k": 5,
  "max_candidates": 200,
  "budgets": { "anchors": 140, "synonyms": 60 },
  "mode": "auto", // auto | anchors_only | anchors_plus_synonyms | raw
  "llm": { "enabled": true }, // set false to skip LLM expansion (raw passthrough)
  "datahub": {
    "host": "https://datahub.mycorp",
    "token": "DATABEARER..."
  },
  "injected": {
    "anchors": ["pii", "email"],
    "synonyms": ["phone number", "phone", "gdpr"],
    "negatives": {
      "hard": ["tmp"],
      "soft": ["dev", "test"],
      "exceptions": [{ "negative": "test", "allow": ["ab test"] }]
    }
  },
  "flags": {
    "return_explain": true,
    "return_candidates": false
  }
}
```

**Response JSON (success)**

```json
{
  "query": "find pii tables with emails ...",
  "results": [
    {
      "rank": 1,
      "score": 0.87,
      "urn": "urn:li:dataset:(...)",
      "name": "marketing.customers",
      "platform": "snowflake",
      "path": "snowflake/db.schema.customers",
      "highlights": ["columns.name: email", "tags: pii"],
      "why": "Matches anchors (pii,email), phone in columns, certified, recent high usage"
    }
  ],
  "explain": {
    "plan": {
      "anchors": ["pii", "email"],
      "phrases": ["phone number"],
      "synonyms": ["phone", "phone_number", "gdpr"],
      "negatives": {
        "hard": ["tmp", "sandbox"],
        "soft": ["dev", "test"],
        "exceptions": [{ "negative": "test", "allow": ["ab test"] }]
      }
    },
    "gating": { "dropped_synonyms": ["contact"], "df_ratio_band": [0.3, 3.0] },
    "budgets": { "anchors": 140, "synonyms": 60 },
    "graphql": {
      "passA_query": "\"pii email\"~2 OR pii OR email OR \"phone number\"",
      "passB_query": "\"phone number\" OR phone OR phone_number OR gdpr",
      "flags": { "skipHighlighting": true, "skipAggregates": true }
    },
    "counts": {
      "passA": 134,
      "passB": 71,
      "merged": 200,
      "after_negatives": 188
    }
  }
}
```

### Python SDK (concept)

```python
from semantic_search import SemanticSearch, Config

cfg = Config(datahub_host="...", datahub_token="...", reranker="cohere:r3")
engine = SemanticSearch(cfg)
res = engine.search("pii tables with emails, not test", top_k=5, explain=True)
```

---

## LLM contract (structured output)

Ask for a **strict JSON** object; temperature=0. Provide **allowed enums** if you want optional soft facets later.

```json
{
  "anchors": ["customer 360", "email"], // 1–3, from the prompt
  "phrases": ["phone number"], // multi-word; keep short
  "synonyms": ["pii", "gdpr", "phone", "phone_number", "contact"],
  "negatives": {
    "hard": [
      "_tmp",
      "tmp",
      "scratch",
      "sandbox",
      "deprecated",
      "sample_data",
      "staging"
    ],
    "soft": ["dev", "test", "sample"]
  },
  "exceptions": [
    {
      "negative": "test",
      "allow": ["ab test", "a/b test", "t-test", "hypothesis test", "test_id"]
    },
    { "negative": "sample", "allow": ["sample size", "sample_rate"] }
  ]
}
```

**Model-side instructions (summary):**

- Return JSON only. No proper nouns or invented identifiers.
- Remove English stop words and generic analytics words (data, dataset, table, report, field, column, business, team).
- Keep `synonyms` ≤ 12 tokens, 2–24 characters, `[a-z0-9_.-]`.
- Negatives: “hard” = safe to exclude; “soft” = ambiguous (prefer penalties).
- Provide exceptions for common false-positives.

---

## Query planning

### Gating & normalization

- Lowercase, ASCII fold; split camel/snake.
- Drop from **stoplist**; enforce token length bounds.
- **DF gating:** remove synonyms whose document frequency is beyond a ratio band relative to anchors (default `[0.3×, 3.0×]`).
- Cap `synonyms` to ≤ 12 after gating.

### Building queries (no wildcards)

- **Pass A (anchors + phrases):**
  - Plain query with **OR + proximity**: `"<a1 a2>"~2 OR a1 OR a2` plus any `phrases` as `"..."~2`.
  - OR field-aimed via `/q` without wildcards: `name:(a1 OR a2) OR description:(a1 OR a2) OR fieldPaths:(a1 OR a2)`.
- **Pass B (synonyms only):**
  - `syn1 OR syn2 OR "syn phrase"`.

**Why avoid `*k*` wildcards?** They flatten scoring (constant-score) and can be expensive. Keep analyzed terms for BM25 + phrase scoring.

### Candidate budgets (de-boost synonyms)

- Default **N₁:N₂ = 140:60** (tune with logs).
- If Pass A returns < N₁, top-up from Pass B. If Pass A >> target, trim Pass B further.

---

## Retrieval against DataHub

**GraphQL (sketch)**

```graphql
query SearchAcross($types: [EntityType!], $q: String!, $count: Int!) {
  searchAcrossEntities(
    input: {
      types: $types
      query: $q
      count: $count
      searchFlags: { skipHighlighting: true, skipAggregates: true }
    }
  ) {
    searchResults {
      entity {
        urn
        ... on Dataset {
          name
          platform {
            name
          }
          properties {
            description
          }
          editableProperties {
            description
          }
          tags {
            tags {
              tag {
                name
              }
            }
          }
          glossaryTerms {
            terms {
              term {
                name
              }
            }
          }
          editableSchemaMetadata {
            editableSchemaFieldInfo {
              fieldPath
              description
            }
          }
          schemaMetadata {
            fields {
              fieldPath
              description
            }
          }
          ownership {
            owners {
              owner {
                __typename
                ... on CorpUser {
                  username
                }
              }
            }
          }
        }
      }
    }
  }
}
```

- Use the **plain** query for proximity scoring: `"<a1 a2>"~2 OR a1 OR a2`.
- Or the **field-aimed** `/q` variant without wildcards.
- Keep `count` equal to each budget (N₁ or N₂).
- Request only fields needed for **shaping** and **negatives** evaluation.

---

## Negatives (hard exclude vs. soft bury)

### Hard exclude (safe patterns)

- Name/path/browse path regex: `(^|[_-])(tmp|temp|scratch|sandbox|bak|legacy|staging)([_-]|$)`
- Tags/terms: `deprecated`, `do-not-use`
- Environment facets: `DEV`, `STAGING` (if present)
- Apply server-side in `/q` with `NOT` if confident, otherwise **client-side drop**.

### Soft bury (keep with small penalty)

- Tokens: `dev`, `test`, `sample` (with **exceptions** like “A/B test”, “t-test”, “sample size”).
- Penalty per token up to a cap (e.g., 0.05–0.10 of final score before normalization).

---

## Reranking + final scoring

- Use a **cross-encoder reranker** (`cohere.rerank-*`, `bge-reranker-v2-m3`, `jina-reranker-v2`).
- **Shape each candidate** into a compact text block:

```
TITLE: <name/title>  TYPE: <dataset|dashboard|...>  PLATFORM: <...>
PATH: <platform>/<db>/<schema>/<name>
DESC: <short>
COLUMNS: <name1> - <short>; <name2> - <short>; ...
TAGS: <t1, t2, ...>  OWNERS: <o1, o2>
USAGE_30D: <n>  CERTIFIED: <bool>
```

**Final blend (tie-breakers only):**

```
final = 0.85 * normalize(rerank_score)
      + 0.10 * normalize(anchor_hits)     # # of anchor terms or phrases matched in high-signal fields
      - 0.05 * soft_negative_penalty
```

Keep the lexical adjustments small; the reranker should dominate.

---

## Observability & evaluation

**Log per query:**

- LLM JSON, gating decisions (dropped synonyms), candidate budgets, GraphQL strings.
- Counts: passA, passB, merged, after negatives, reranked top‑K.
- Latencies per stage (LLM, passA, passB, rerank).
- Clicks / opens / dwell time (if available).

**Offline metrics:** nDCG@5, MRR@10, Recall@50 (pre-rerank), P50/P95 latency.
**A/B toggles:** synonyms on/off, budgets, negatives penalties, phrase slop (1–3), LLM vs. deterministic PRF.

---

## Performance, limits, and cost

- **Disable highlighting & aggregates** for candidate harvest.
- Cap GraphQL `count` (e.g., 150 + 80) and truncate entity shapes deterministically for the reranker (token budget).
- Cache LLM outputs for frequent queries; also cache Pass A/B GraphQL results for 30–120s.
- Parallelize Pass A/B when backend permits (they are independent).

---

## Security & privacy

- Never send catalog contents to the LLM; only the **user query** and **enum lists** (platforms/domains/tags) and your stoplists.
- Prefer an LLM vendor or deployment with **no-training** guarantees; consider on-prem for strict environments.
- Treat DataHub tokens as secrets; store in a secure vault; rotate regularly.

---

## Configuration reference

```yaml
semantic_search:
  budgets:
    anchors: 140
    synonyms: 60
  df_ratio_band: [0.3, 3.0]
  max_synonyms: 12
  stoplist:
    - data
    - dataset
    - table
    - report
    - field
    - column
    - business
    - team
  negatives:
    hard_patterns:
      - "(^|[_-])(tmp|temp|scratch|sandbox|bak|legacy|staging)([_-]|$)"
    soft_terms:
      - dev
      - test
      - sample
    exceptions:
      - {
          negative: "test",
          allow:
            ["ab test", "a/b test", "t-test", "hypothesis test", "test_id"],
        }
      - { negative: "sample", allow: ["sample size", "sample_rate"] }
  reranker:
    provider: "cohere"
    model: "rerank-3"
    blend: { rerank: 0.85, anchors: 0.10, negatives: 0.05 }
  datahub:
    host: "https://datahub.mycorp"
    token_env: "DATAHUB_TOKEN"
```

---

## Pseudocode (end-to-end)

```python
def search(user_query, cfg, injected=None, explain=False):
    # 1) Classify navigational vs NL
    if is_navigational(user_query):
        q_plain = navigational_query(user_query)  # exact path/name dis_max
        return dh_search(q_plain, count=cfg.budgets.anchors, flags=FAST).top_k(5)

    # 2) LLM expansion (or use injected terms)
    plan = injected or call_llm(user_query)
    plan = gate_terms(plan, cfg.stoplist, cfg.df_ratio_band, cfg.max_synonyms)

    # 3) Build queries
    qA = build_plain_or_query(plan.anchors, plan.phrases)  # "\"a1 a2\"~2 OR a1 OR a2 ..."
    qB = build_plain_or_query(plan.synonyms, [])           # "syn1 OR syn2 ..."

    # 4) Retrieve
    resA = dh_search(qA, count=cfg.budgets.anchors, flags=FAST)
    resB = dh_search(qB, count=cfg.budgets.synonyms, flags=FAST)

    # 5) Apply negatives
    resA2 = apply_negatives(resA, plan.negatives, plan.exceptions)
    resB2 = apply_negatives(resB, plan.negatives, plan.exceptions)

    # 6) Merge + dedupe
    candidates = dedupe_by_urn(resA2 + resB2)[:cfg.max_candidates]

    # 7) Shape, rerank, blend
    docs = [shape_for_rerank(e) for e in candidates]
    r = rerank(cfg.reranker, user_query, docs)
    final = blend_with_lexical_signals(r, anchors=plan.anchors, negatives=plan.negatives)

    return build_response(final, explain, plan, qA, qB, counts={...})
```

---

## Examples

**User query:**

> “find pii tables with emails and phone numbers in snowflake marketing, not test”

**LLM JSON (after gating):**

```json
{
  "anchors": ["pii", "email"],
  "phrases": ["phone number"],
  "synonyms": ["phone", "phone_number", "gdpr"],
  "negatives": {
    "hard": ["tmp", "sandbox"],
    "soft": ["dev", "test"],
    "exceptions": [{ "negative": "test", "allow": ["ab test"] }]
  }
}
```

**Pass A query:**
`"pii email"~2 OR pii OR email OR "phone number"`

**Pass B query:**
`"phone number" OR phone OR phone_number OR gdpr`

**Outcome:**
Top‑200 include high‑usage, certified Snowflake tables in the marketing domain with columns `email`/`phone`, plus a few GDPR-tagged assets; test/dev/tmp items are either dropped or softly buried. Reranker then elevates the most relevant 5.

---

## FAQ

**Q: Why not put synonym weights (`^2`, `^3`) in the query text?**
A: DataHub’s public API doesn’t guarantee per-term boosts in user-provided query text and `/q` is not designed for term weighting. Budget the candidate pools instead, then let the reranker decide; add a tiny client-side blend for anchors as a tie-breaker.

**Q: Do we ever hard-filter facets?**
A: Default is **no** (use soft facet boosts if you control server scoring). If a facet is explicit (user selected platform=Snowflake), it’s safe to filter via `orFilters` or `/q`—but remember to keep at least one keyword match (`minimum_should_match: 1` behavior).

**Q: Why avoid wildcards like `*k*`?**
A: They flatten scoring (constant-score) and can be expensive. Using analyzed terms preserves BM25 and phrase/proximity scoring.

**Q: What about column-level matches?**
A: Include `fieldPaths` and field descriptions in shaping and/or in the `/q` query (without wildcards).

---

## Roadmap

- Optional **PRF (pseudo-relevance feedback)** stage to harvest high-signal terms from a small initial set.
- Reranker A/B: Cohere vs. BGE/Jina; latency vs. quality curves.
- Field Configuration profiles in DataHub (`search_config.yml`) selectable per mode.
- Query understanding for **question answering** (route to RAG/notebooks).
- UI: “Why this result?” with anchors matched, synonyms used, negatives applied, and reranker evidence snippets.

---
