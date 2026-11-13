# The semanticContent Model

`semanticContent` is the aspect that stores embedding vectors for an entity. One aspect holds any number of embedding profiles, so a document can carry vectors from several models, or from several kinds of content, at once. This page describes the model, the ways it is written, and how the server keeps each profile consistent with the content it was generated from.

For how vectors are generated and searched, see [Architecture](ARCHITECTURE.md). For configuration keys, see [Configuration](CONFIGURATION.md).

## Purpose

The model is designed around four goals:

- **Independent profiles.** Each profile is written on its own. A write of one profile leaves every other profile in place.
- **Explicit state.** A profile records what it stores (vectors, an exclusion, or a skip) and what work is outstanding, so readers can tell a deliberate gap from lag or failure.
- **Staleness from content.** A profile records which version of its bound content its vectors represent. A content edit marks only the profiles bound to the changed fields as out of date.
- **Profile-scoped consistency.** A write that depends on what it read checks that profile's own state, and leaves writes to sibling profiles free to proceed.

## Records

### SemanticContent

| Field           | Type                              | Meaning                                                                                                                    |
| --------------- | --------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `embeddings`    | `map[string, EmbeddingModelData]` | One entry per profile. The key is the profile key.                                                                         |
| `refreshMarker` | optional `SemanticRefreshMarker`  | The latest audit-stamp time of each watched path that a profile is bound to, keyed by watch key. Maintained by the server. |
| `skipReason`    | optional string                   | Legacy entity-wide skip. Set with an empty `embeddings` map. A reader that sees any profile key ignores it.                |
| `skippedAt`     | optional time                     | Legacy time of the entity-wide skip.                                                                                       |

New writers record a skip on the profile itself and leave `skipReason` and `skippedAt` unset.

The aspect is schema version 2. Version 2 adds the optional fields described below. Readers that ignore unknown fields keep working, and no data migration is needed. A client that requires schema version 1 must accept version 2.

### EmbeddingModelData

Each profile has the original generation fields and a set of optional state fields.

| Field              | Type                             | Meaning                                                                                                                 |
| ------------------ | -------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `modelVersion`     | string                           | Full model identifier, including the provider.                                                                          |
| `generatedAt`      | long                             | Time the profile was last written, in milliseconds.                                                                     |
| `sourceTextSha256` | optional string                  | SHA-256 of the resolved text the vectors were generated from.                                                           |
| `chunkingStrategy` | optional string                  | Description of the chunking strategy.                                                                                   |
| `totalChunks`      | int                              | Number of chunks. Zero for an excluded or skipped profile.                                                              |
| `totalTokens`      | optional int                     | Estimated token count across chunks.                                                                                    |
| `chunks`           | `array[EmbeddingChunk]`          | Chunks and their vectors. Empty for an excluded or skipped profile.                                                     |
| `vectorState`      | optional `SemanticVectorState`   | What the profile stores. Legacy data with chunks and no value reads as `READY`.                                         |
| `workState`        | optional `SemanticWorkState`     | Outstanding work for the profile, independent of `vectorState`.                                                         |
| `stateReason`      | optional string                  | Why the profile is excluded, skipped, deferred, or quarantined.                                                         |
| `policyVersion`    | optional string                  | Version of the policy that produced the current vector or work state.                                                   |
| `current`          | optional boolean                 | True when the stored vectors match the profile's bound content. Unset on legacy data.                                   |
| `refreshMarker`    | optional `SemanticRefreshMarker` | The part of the entity marker these vectors represent: the watch keys of the profile's binding.                         |
| `dirty`            | optional boolean                 | Set when a content write could not name a time. Writes through the metadata service always have one, so it stays unset. |
| `partial`          | optional boolean                 | True when a chunk cap cut the profile before hashing.                                                                   |
| `contentHash`      | optional string                  | Hash of the capped chunk payload that was embedded.                                                                     |
| `vectorDimension`  | optional int                     | Dimension of the stored vectors.                                                                                        |

`sourceTextSha256` covers the resolved source text. `contentHash` covers the chunk payload that was sent for embedding, after chunking and capping, together with `sourceTextSha256`. It excludes `generatedAt`, `current`, and the work state, so writing the same chunks again produces the same hash.

### EmbeddingChunk

| Field             | Type            | Meaning                                                   |
| ----------------- | --------------- | --------------------------------------------------------- |
| `position`        | int             | Zero-based position of the chunk.                         |
| `vector`          | `array[float]`  | The embedding vector.                                     |
| `characterOffset` | optional int    | Character offset in the source text.                      |
| `characterLength` | optional int    | Character length in the source text.                      |
| `tokenCount`      | optional int    | Estimated token count.                                    |
| `text`            | optional string | Chunk text. Omit it to share vectors without source text. |

### SemanticRefreshMarker

`sources: map[string, long]`, keyed by watch key. A watch key is the aspect name when a profile watches the whole aspect, or `aspect#path` when it watches one path, for example `documentInfo#contents.text` or `documentInfo#title`. Each value is the audit-stamp time of the aspect version that last changed that watched path. The value moves forward only: a write with an older audit stamp keeps the stored value.

### SemanticVectorState

| Value      | Meaning                                                    |
| ---------- | ---------------------------------------------------------- |
| `READY`    | The profile stores vectors.                                |
| `EXCLUDED` | A policy keeps this entity out of the profile. No vectors. |
| `SKIPPED`  | There was nothing to embed, for example empty text.        |

A missing profile key means the profile is absent, so absent is not an enum value.

### SemanticWorkState

| Value             | Meaning                                |
| ----------------- | -------------------------------------- |
| `NONE`            | No outstanding work.                   |
| `PENDING`         | Work is queued.                        |
| `BUDGET_DEFERRED` | Work is postponed by a resource limit. |
| `QUARANTINED`     | Work is held after repeated failures.  |

## Profile keys

The key of the `embeddings` map is the profile key. It matches a model entry key under `elasticsearch.entityIndex.semanticSearch.models` (for example `cohere_embed_v3`). Every write names exactly one key, and the server applies it to that key only.

## Bindings and current vectors

A binding lists the aspect fields that make a profile stale. Bindings are declared under `semanticSearch.profiles.bindings`. A model entry selects one with `binding`, and a key with no model entry uses `semanticSearch.profiles.defaultBinding`. A path selects part of a source aspect:

- `contents.text` selects a record or map field.
- `items[0].text` selects one array element. An index past the end selects nothing.
- `items[*].text` selects `text` on every element. A sibling field does not count. Removing an element counts, because later indexes move.
- `texts[*].text` selects `text` on every map value.
- `texts['my.key'].text` selects a map key that contains a dot.
- A leading `$` is ignored. `..`, filters, slices, unions, and scripts are rejected when the server loads the binding.

An empty path list means any change to the aspect. A binding may list several paths of one aspect. A change to any of them marks that profile not current and moves the tracked time of that path. A change outside the listed paths does neither. A leading `$.` is dropped when the watch key is built, so `$.contents.text` and `contents.text` are the same watch key, `documentInfo#contents.text`.

The source aspect's audit time and the tracked time are different clocks. The audit time increases on every write to the aspect. The tracked time in `refreshMarker.sources` increases only when a watched path changes, and only forward: the max of the previous tracked time and the audit time of the write that changed the path. Each watched path has its own tracked time, keyed by its watch key. An embedding records the tracked times it read, one per watch key of its binding. `current` compares that snapshot with the tracked times in `semanticContent`. It does not compare the source aspect's audit time. A read can include newer content than the tracked time it records, including a watched path whose marker update is not visible yet. The next advance of that path's tracked time makes that snapshot older and the profile not current.

Profiles that watch different paths of one aspect keep separate tracked times. A write that changes `title` moves only `documentInfo#title`, so a profile that watches only `contents.text` stays current. A profile that stores its own `refreshMarker` keeps only the tracked times of its own watch keys.

- The shipped `documentText` binding watches `documentInfo.contents.text` and `semanticText.text`.
- The shipped `external` binding watches nothing. A model entry selects it with `binding: external`. It is declared as `external: {}` under `semanticSearch.profiles.bindings`.

An external profile is owned by the connector that writes it.

- A change to any other aspect does not clear `current` on an external profile. It also does not advance the entity refresh marker because of that profile.
- The connector decides whether the vectors are current. It may hash its own source to skip a write. The server does not read or hash the document for this binding.
- When the written payload changes (`contentHash`, `chunks`, `sourceTextSha256`, or the profile `refreshMarker`), a profile with no `refreshMarker` keeps the `current` value the connector sent. A profile that carries `refreshMarker` is current: the watch set is empty, so the clock comparison has nothing to fail.
- Connectors write one profile with a JSON patch when the server reports `semanticContentPatchSupported` on the semantic search config. An upsert remains a plain replace of the whole aspect, including other profiles and the entity marker.

When a bound field changes, the server:

1. Commits the source write without touching `semanticContent`.
2. Compares the committed source aspect with its previous version on the watched paths of every stored profile. When a path changed, it emits one `semanticContent` JSON patch that sets `/refreshMarker/sources/<watch key>` to the audit time of that write for each changed watch key. An edit that changes no watched path emits nothing. The patch is ingested on its own after the source commit, so the marker and `current` update shortly after the edit, not in the same transaction. A failure in that step does not fail the source write.
3. Applies the patch to the stored `semanticContent` and reconciles it like any other write, as described below. Profiles bound to other content are unchanged.

Every `semanticContent` write keeps each `refreshMarker.sources` entry at the max of the stored and written time, so a full-aspect write that carries an older or missing marker cannot move the clock back. The server then sets `current` on each profile whose payload changed or whose bound watch key's tracked time moved forward on this write. A change to only the profile `refreshMarker` is a new payload, so `current` is recomputed even when the vectors are unchanged. Apply writes keep the `current` they computed.

- A profile that carries `refreshMarker` is current when that marker is at least as new as every bound source on the entity marker. The comparison uses the entity marker after this write. The server does not read or hash the document for this decision.
- A profile with no `refreshMarker` is legacy. When a bound watch key other than `documentInfo#contents.text` or `semanticText#text` moved forward, the profile is not current, including a binding that also watches document text. Otherwise, for a binding that watches `documentInfo#contents.text` or `semanticText#text`, `current` is true only when `sourceTextSha256` equals the hash of the resolved text. A binding that watches a whole aspect, such as `documentInfo` with no paths, has no document-text key, so it is not hashed and follows the rule for other non-external bindings. The resolved text is `semanticText.text` when set, else `documentInfo.contents.text`, read from the stored aspects. A mismatch sets `current` false. Any other non-external binding with no marker is current when its payload changes.
- An external profile keeps the `current` it was written with.

## Write paths

### Connector patch

The ingestion chunking source writes one profile as a JSON Patch at `/embeddings/<key>`. `SemanticContentPatchBuilder` emits it in both Java and Python. A write that stores vectors also removes `/skipReason` and `/skippedAt`. The server drops either remove when that field is already absent, so a document that was never skipped still accepts the profile. A skip writes a complete profile: `modelVersion`, `generatedAt`, `totalChunks` of 0, empty `chunks`, `vectorState` `SKIPPED`, and `stateReason`. It leaves an existing entity skip marker in place.

Connector patches carry no preconditions. The connector owns its key, so the last write wins.

### Legacy upsert

Servers that accept profile patches report `semanticContentPatchSupported` on the semantic search config. The field is set whether or not an embedding provider is configured. A client that reads an older server, which does not send the field, falls back to `embeddingConfig.semanticContentPatchSupported`. When neither field is true, the chunking source falls back to an upsert of the whole aspect. An upsert replaces every profile, so it is used only against older servers.

### Apply

`SemanticContentApplyService` stores the outcome of server-side work for one profile:

- **Result.** Stores vectors. `vectorState` becomes `READY` and `workState` becomes `NONE`. The profile records the part of the entity marker its binding watches. `current` is true when that marker is at least as new as every watch key of the binding that the entity marker has recorded. A watch key with no marker entry has not had a watched-path change, so it does not make the result stale. A write that leaves the watched fields unchanged does not record a marker entry. When the represented marker is older than a recorded source, the vectors are stored with `current` false. A result also removes the legacy entity skip fields `/skipReason` and `/skippedAt`.
- **Removal.** Excludes or skips the profile, or drops the key. Exclusion and skip require a reason. They clear the chunks, set `workState` to `NONE`, and clear the result fields (`current`, `contentHash`, `partial`, `vectorDimension`, and the profile `refreshMarker`). Dropping a key that is already absent does not write.
- **State only.** Records `workState` and `stateReason` without touching vectors. A retry does not replace a `workState`, a `stateReason` this write sets, or a `generatedAt` that changed after the read.

Apply writes carry the `semanticApply` system-metadata property and run as the system actor. The property is removed from writes by any other actor. `SemanticProfileEligibility` decides whether a result may be stored; the default bean accepts every result.

### Server marker updates

A write that changes a watched path adds a semanticContent patch that moves that path's entity marker key forward and clears `current` on the profiles that watch it. These updates are forward-only, so they carry no preconditions.

## Profile-scoped preconditions

One aspect holds every profile, so its version changes on each write to any profile and on each marker update. A check on the aspect version would treat those unrelated writes as conflicts. Apply writes instead check the state of their own profile.

`SemanticProfilePatchBuilder` extends `SemanticContentPatchBuilder` with expectations. Each expectation is a JSON Patch `test` op:

| Method                              | Path                                 |
| ----------------------------------- | ------------------------------------ |
| `expectProfileField(key, field, v)` | `/embeddings/<key>/<field>`          |
| `expectProfileAbsent(key)`          | `/embeddings/<key>`                  |
| `expectSource(watchKey, time)`      | `/refreshMarker/sources/<watch key>` |
| `expectSourceAbsent(watchKey)`      | `/refreshMarker/sources/<watch key>` |

On `semanticContent`, a `test` value of `null` means the path must be absent. `expectProfileState` and `expectBoundSources` build expectations from the values that were read.

The server evaluates these ops against the stored aspect, which is read for update inside the write transaction, before it applies the rest of the patch. If any expectation fails, nothing is written. Apply then reads again and retries once.

| Apply operation | Expectations                                                                                                                                                      |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Result          | The key's `contentHash`, `refreshMarker`, and `generatedAt`, plus the entity marker entries for the watch keys in the key's binding.                              |
| Removal         | The key's `contentHash`, `vectorState`, and `generatedAt`, or that the key is absent.                                                                             |
| State only      | The key's `modelVersion`, `contentHash`, `policyVersion`, `workState`, and `generatedAt`, plus `stateReason` when this write sets one, or that the key is absent. |

A state-only write on an existing key sets `workState` and `stateReason` field by field, so it keeps vectors that a concurrent result stored for the same payload. A retry drops the write when `workState`, `generatedAt`, or `stateReason` when this write sets one, changed after the read.

## Service-owned profiles

A model entry with `serviceOwned: true` is protected while `semanticSearch.profiles.generationEnabled` is true. Adding, removing, or changing that profile's vectors, `workState`, `stateReason`, or `policyVersion` then requires an apply write. Changes to `current`, `refreshMarker`, and `dirty` stay open to every writer, so a content edit can record that the vectors are stale. The check runs in an aspect validator, so it covers every API.

`SemanticProfileRegistry` answers which profiles are service-owned and which binding each key uses. The default implementation reads the configuration above, and another implementation replaces it when one is present.

## Search index projection

- **V3 entity index.** Mappings declare the profile state fields on every configured model entry. `refreshMarker` is stored and not indexed. The indexer copies `embeddings` and the entity `refreshMarker` to the root of the search document.
- **V2 semantic index.** The sidecar document carries the vectors and omits the state fields.

Query uses the one configured query profile.
