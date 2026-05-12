# Design Review: ONNX In-Process Embedding Provider [PFP-3657]

**Date:** 2026-05-06
**Branch:** `pfp-3657-onnx-embedding-spike`
**Reviewers:** Deployment Architecture, Threat Model, Operational Design, Separation of Concerns

## Context

This branch adds an in-process ONNX embedding provider to DataHub's semantic search subsystem. Instead of calling an external service (OpenAI, Ollama, etc.), the GMS JVM runs ONNX Runtime inference directly, using DJL's HuggingFace tokenizer for tokenization.

**User requirements:**

1. ONNX must be opt-in, not default (separate Docker image variant)
2. Must be production-safe with loud failures on mismatches
3. Model baked into Docker image at build time

---

## Findings

### Redesign

#### R1. `modelId` returns a filesystem path — broken ES field names and silent search failure

**Flagged by:** Ops (C1), Separation (D1), Deployment (A4)

`EmbeddingProviderConfiguration.getModelId()` returns `onnx.getModelDir()` — a filesystem path like `/models/arctic-embed-s`. This feeds into `deriveModelEmbeddingKeyFromModelId()`, which replaces `-`, `.`, `:` with underscores but NOT `/`. The resulting ES field name contains slashes, won't match any key in the `models` map, and kNN queries hit a non-existent nested path. Semantic search returns zero results with no error.

**Evidence:**

- `EmbeddingProviderConfiguration.java:74` — returns `onnx.getModelDir()`
- `SemanticEntitySearchServiceFactory.java:121` — derivation doesn't handle `/`
- `AppConfigResolver.java:397` — duplicated derivation with different special cases

**Action:** Add a `modelName` field to `OnnxConfig` (e.g., `arctic_embed_s`) that serves as the logical model identity. Return this from `getModelId()`. Require it matches a key in the `models` map. Validate at startup.

#### R2. No dimension mismatch guard — ONNX produces 384/768/1024 dims, default is 3072

**Flagged by:** Deployment (A4), Ops (C2), Separation (D3), Threat (B2)

Default `ModelEmbeddingConfig.vectorDimension` is 3072 (OpenAI). ONNX models produce 384/768/1024. If an operator sets `type=onnx` without updating `vectorDimension`, the ES index expects 3072-dim vectors but gets 384-dim vectors. The error surfaces as a cryptic ES exception deep in the search pipeline, not a startup failure.

The Vertex AI provider already has this guard at `EmbeddingProviderFactory.java:189-199`.

**Evidence:**

- `ModelEmbeddingConfig.java:17` — default vectorDimension = 3072
- `EmbeddingProviderFactory.java:155-168` — no dimension validation for ONNX
- `EmbeddingProviderFactory.java:189-199` — Vertex AI has this validation

**Action:** At startup, run a probe embedding, compare output dimension to configured `vectorDimension`, fail fast if they differ.

#### R3. ONNX dependencies ship in all GMS images — violates opt-in requirement

**Flagged by:** Deployment (A1), Separation (D6)

ONNX Runtime (~60MB native) and DJL tokenizer are `implementation` in `metadata-io/build.gradle`. They ship in every GMS Docker image. This adds ~80MB to all images even when ONNX is never used.

**Evidence:**

- `metadata-io/build.gradle:145-147` — `implementation` scope
- `build.gradle:355-357` — version declarations

**Action:** For production, move to a two-image strategy. Make ONNX deps `compileOnly` in base build, create a `datahub-gms-onnx` Dockerfile that includes the ONNX JARs and baked-in model.

#### R4. No model bake-in mechanism in Docker

**Flagged by:** Deployment (A2)

No Dockerfile, build script, or documentation for baking an ONNX model into the Docker image. The current design only takes a `modelDir` path. Operators must create their own derived Dockerfile or use volume mounts.

**Action:** Create `docker/datahub-gms-onnx/Dockerfile` with a build arg for model selection and a `COPY` of pre-validated models.

### Improve

#### I1. No input length bounds — DoS vector via native memory

**Flagged by:** Threat (B2)

`embed()` passes raw text to the tokenizer with no character-length limit. Other providers (e.g., AwsBedrock) use `maxCharacterLength`. ONNX allocates native memory proportional to token count, evading JVM heap limits.

**Evidence:** `OnnxEmbeddingProvider.java:130` — `tokenizer.encode(text)` with no bounds
**Action:** Wire `maxCharacterLength` from config. Add a hard token-count cap after tokenization.

#### I2. `intraOpThreads` not exposed through configuration

**Flagged by:** Deployment (A6), Separation (D7), Ops (C4)

Constructor accepts `intraOpThreads` but factory hardcodes default (0 = all cores). In containers with CPU limits, this causes excessive thread creation and contention.

**Evidence:**

- `OnnxEmbeddingProvider.java:63` — accepts `intraOpThreads`
- `EmbeddingProviderFactory.java:167` — uses 0-arg constructor
- `OnnxConfig` — no `intraOpThreads` field

**Action:** Add `intraOpThreads` to `OnnxConfig` with env var `ONNX_EMBEDDING_INTRA_OP_THREADS`, default 4.

#### I3. AutoCloseable not wired to Spring destroy lifecycle

**Flagged by:** Threat (B4), Ops (C5), Separation (D5)

`OnnxEmbeddingProvider` implements `AutoCloseable` but the `@Bean` return type is `EmbeddingProvider` (which doesn't extend `AutoCloseable`). Spring may not auto-detect `close()`.

**Action:** Add `destroyMethod = "close"` to the `@Bean` annotation.

#### I4. No performance instrumentation on embed()

**Flagged by:** Ops (C4)

No timing metrics or latency logging on the embed call. Operators can't distinguish slow ONNX inference from slow ES kNN queries.

**Action:** Add timing at the `SemanticEntitySearchService.search()` call site (benefits all providers).

#### I5. JVM native memory unaccounted for

**Flagged by:** Deployment (A3), Ops (C7)

GMS runs with 1GB heap, no `MaxDirectMemorySize`, no container memory limit. ONNX model weights are allocated in native memory (invisible to `-Xmx`). A 130MB model needs ~300MB native; 550MB model needs ~800MB.

**Action:** Document memory requirements per model. Log model file size and estimated memory at startup. For the ONNX image variant, set container memory limits.

#### I6. Health check timing may be insufficient for large models

**Flagged by:** Deployment (A7), Ops (C3)

ONNX model loading is synchronous in the Spring constructor. Large models (550MB) can take 30-60s. Combined with normal GMS startup (~60s), total may exceed the 90s health check start period.

**Action:** Log start/end of model loading with elapsed time. Document increased health check start period for ONNX variant. Consider lazy initialization.

### Document

#### D1. Model upgrade requires reindex — not communicated

**Flagged by:** Ops (C8)

Switching ONNX models (different dimensions) requires recreating semantic indices and re-embedding all documents. No startup warning or documentation communicates this.

**Action:** Add startup log warning when ONNX provider is active about model switching implications.

#### D2. Thread safety relies on library guarantees

**Flagged by:** Threat (B7)

`OrtSession.run()` and `HuggingFaceTokenizer.encode()` are documented as thread-safe, but this is an implicit contract.

**Action:** Add a class-level comment noting the thread-safety assumption with library doc references.

### Accept

#### A1. No model file integrity check (baked images)

**Flagged by:** Threat (B1)

For the baked-in-image model (build time), the container image IS the integrity boundary. Volume-mount deployments would need checksum validation, but that's not the current design.

**Rationale:** Build-time model inclusion means the model is part of the signed container image. Accept for this deployment model.

#### A2. Path traversal via configuration

**Flagged by:** Threat (B5)

`modelDir` comes from env var without path validation. However, config access already implies full admin access.

**Rationale:** Accept-risk. The threat requires a privilege level that already implies full control.

#### A3. `model` parameter in embed() is ignored

**Flagged by:** Separation (D4)

ONNX ignores the `model` parameter. All callers currently pass `null`. Low practical risk.

**Rationale:** Accept for now. Add a log warning if `model` is non-null as part of I4 instrumentation work.

#### A4. Duplicated `deriveModelEmbeddingKey` logic

**Flagged by:** Separation (D8)

Two implementations with divergent special cases. Pre-existing issue, not introduced by ONNX.

**Rationale:** Accept — worth fixing but out of scope for this branch.

---

## Self-Check

### Coverage scan

All major components reviewed: provider implementation, config model, factory wiring, Docker/deployment, ES index mappings, query-time consumption, error propagation path.

### Contradiction check

No contradictions between reviewers. All four flagged the modelId/dimension mismatch from different angles.

### Blind spots

- **Ingestion-side embedding**: The Python ingestion framework also generates embeddings. If ingestion uses OpenAI and query uses ONNX with different dimensions, kNN breaks. This is covered by R2 (dimension guard), but a full production rollout needs ingestion-side ONNX support too — out of scope for this spike.
- **Multi-model coexistence**: The `models` map supports multiple models simultaneously. This review focused on single-model ONNX deployment. Multi-model scenarios (e.g., ONNX for query + OpenAI for ingestion) are not addressed.
