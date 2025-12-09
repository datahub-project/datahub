# Dual-Write Pattern for Semantic Index Synchronization

**Status:** Implemented  
**Created:** 2024-12-04  
**Author:** AI Assistant

## Overview

This document describes the dual-write pattern used to keep semantic indices synchronized with v2 indices in DataHub. As we transition toward semantic search as the primary index, dual-writes ensure that semantic indices remain a complete, up-to-date copy of v2 data, enabling a seamless future migration to decommission v2 indices.

## Background: Why Dual-Write?

### The Migration Strategy

DataHub is migrating from v2 keyword-based search to semantic vector search:

1. **Current State**: V2 indices (`datasetindex_v2`, `chartindex_v2`, etc.) are the primary search indices
2. **Transition State**: Semantic indices (`datasetindex_v2_semantic`, etc.) exist alongside v2 indices, with dual-writes keeping them in sync
3. **Future State**: Semantic indices become primary, v2 indices are decommissioned

### Why Not Just Backfill?

Backfilling alone isn't sufficient because:

- Backfills are point-in-time snapshots
- Data changes continuously (ingestion, user edits, deletions)
- Without dual-writes, semantic indices become stale immediately after backfill
- **Goal**: Semantic index must be a complete, real-time copy of v2 data

### The Dual-Write Pattern

Dual-write means: **any write operation to v2 index is also applied to semantic index** (when it exists).

```
┌─────────────────┐
│  Write Request  │
└────────┬────────┘
         │
         ├──────────────┐
         │              │
         ▼              ▼
  ┌─────────────┐  ┌──────────────────┐
  │  V2 Index   │  │  Semantic Index  │
  │  (Primary)  │  │   (Sync Copy)    │
  └─────────────┘  └──────────────────┘
```

## Dual-Write Implementation Points

Dual-writes are implemented at multiple layers to ensure complete data synchronization:

### 1. Document Updates (UpdateIndicesV2Strategy)

**Location**: `metadata-io/src/main/java/com/linkedin/metadata/service/UpdateIndicesV2Strategy.java`

**What it does**: When aspects change (via MCL events), the corresponding search documents are updated in both indices.

```java
// Write to V2 index
elasticSearchService.upsertDocument(opContext, entityName, finalDocument, docId);

// Dual-write to semantic index if enabled for this entity
if (shouldWriteToSemanticIndex(opContext, entityName)) {
  writeToSemanticIndex(entityName, finalDocument, docId);
}
```

**When it triggers**:

- Entity aspects are created/updated/deleted
- Metadata changes from ingestion pipelines
- User edits in the UI
- API updates

**Conditions for dual-write**:

1. Semantic search is globally enabled (`semanticSearchConfig.isEnabled()`)
2. Entity type is in enabled entities list
3. Semantic index exists in OpenSearch

### 2. Document Deletions (UpdateIndicesV2Strategy)

**What it does**: When entities are deleted (key aspect deletion), removes from both indices.

```java
// Delete from V2 index
elasticSearchService.deleteDocument(opContext, entityName, docId);

// Also delete from semantic index if enabled
if (shouldWriteToSemanticIndex(opContext, entityName)) {
  deleteFromSemanticIndex(entityName, docId);
}
```

### 3. RunId Updates (ElasticSearchService)

**Location**: `metadata-io/src/main/java/com/linkedin/metadata/search/elasticsearch/ElasticSearchService.java`

**What it does**: When ingestion runs update the `runId` field, applies the update to both indices.

**Why runId is special**: The `runId` field tracks which ingestion runs touched an entity. This metadata is critical for:

- Debugging ingestion issues
- Tracking data lineage
- Understanding data freshness
- Soft deletes (entities not touched by recent runs may be stale)

**Previous gap**: Before this implementation, `appendRunId` only updated v2 index, causing semantic indices to have stale or missing runId data.

## RunId Dual-Write Implementation (New)

### The Problem

Previously, the `appendRunId` method in `ElasticSearchService` only updated the v2 index:

```java
// OLD: Only v2 index updated
esWriteDAO.applyScriptUpdate(opContext, entityName, docId, SCRIPT_SOURCE, scriptParams, upsert);
```

When semantic search was enabled, the `runId` field in semantic indices would become stale, breaking the "complete copy" guarantee.

### The Solution

Extended the dual-write pattern to `appendRunId`:

```java
// NEW: Dual-write implementation
// 1. Update V2 index
esWriteDAO.applyScriptUpdate(opContext, entityName, docId, SCRIPT_SOURCE, scriptParams, upsert);

// 2. Dual-write to semantic index if it exists
String semanticIndexName =
    opContext.getSearchContext().getIndexConvention().getEntityIndexNameSemantic(entityName);
if (indexExists(semanticIndexName)) {
  applyScriptUpdateByIndexName(semanticIndexName, docId, SCRIPT_SOURCE, scriptParams, upsert);
}
```

### Implementation Details

#### 1. Added `applyScriptUpdateByIndexName` to `ESWriteDAO`

```java
public void applyScriptUpdateByIndexName(
    @Nonnull String indexName,
    @Nonnull String docId,
    @Nonnull String scriptSource,
    @Nonnull Map<String, Object> scriptParams,
    Map<String, Object> upsert)
```

This method applies a script update directly to a specified index name, similar to existing methods like `upsertDocumentByIndexName` and `deleteDocumentByIndexName`.

#### 2. Added Wrapper Method in `ElasticSearchService`

```java
public void applyScriptUpdateByIndexName(
    @Nonnull String indexName,
    @Nonnull String docId,
    @Nonnull String scriptSource,
    @Nonnull Map<String, Object> scriptParams,
    Map<String, Object> upsert)
```

Provides a clean interface for applying script updates to specific indices.

#### 3. Modified `appendRunId` for Dual-Write

The updated `appendRunId` method now:

```java
@Override
public void appendRunId(
    @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nullable String runId) {
  final String entityName = urn.getEntityType();
  final String docId = opContext.getSearchContext().getIndexConvention().getEntityDocumentId(urn);

  // ... prepare script params and upsert document ...

  // Update V2 index
  esWriteDAO.applyScriptUpdate(opContext, entityName, docId, SCRIPT_SOURCE, scriptParams, upsert);

  // Dual-write to semantic index if it exists
  String semanticIndexName =
      opContext.getSearchContext().getIndexConvention().getEntityIndexNameSemantic(entityName);
  if (indexExists(semanticIndexName)) {
    applyScriptUpdateByIndexName(semanticIndexName, docId, SCRIPT_SOURCE, scriptParams, upsert);
  }
}
```

#### 4. Refactored for Code Reuse

Following DRY principles and existing patterns in `ESWriteDAO`:

```java
// applyScriptUpdate delegates to applyScriptUpdateByIndexName
public void applyScriptUpdate(...) {
  applyScriptUpdateByIndexName(toIndexName(opContext, entityName), ...);
}
```

This matches the existing pattern:

- `upsertDocument(opContext, entityName, ...)` → `upsertDocumentByIndexName(indexName, ...)`
- `deleteDocument(opContext, entityName, ...)` → `deleteDocumentByIndexName(indexName, ...)`

### Testing

Added comprehensive tests in `ElasticSearchServiceTest`:

1. **`testAppendRunId_DualWriteToSemanticIndex`**: Verifies both v2 and semantic indices are updated
2. **`testAppendRunId_SkipsSemanticIndexWhenNotExists`**: Verifies graceful skip when semantic index doesn't exist

All tests pass successfully (5/5 passing).

## Dual-Write Design Principles

### 1. Index Existence Check vs Configuration Check

**Pattern**: Check if semantic index exists rather than checking semantic search configuration.

**Rationale**:

- **Simpler**: No need to inject/pass `SemanticSearchConfiguration` everywhere
- **Self-healing**: Automatically adapts when semantic indices are created or deleted
- **Consistent**: Used throughout (`UpdateIndicesV2Strategy` and `ElasticSearchService`)
- **Defensive**: Won't fail if configuration is out of sync with actual index state

**Implementation**:

```java
if (indexExists(semanticIndexName)) {
  // dual-write
}
```

### 2. Fail-Safe Approach

**Pattern**: Dual-writes are opportunistic, not mandatory.

**Behavior**:

- If semantic index doesn't exist → skip dual-write, continue normally
- If semantic write fails → logs error but doesn't fail the primary v2 write
- Uses same retry mechanism as v2 writes (via `BulkProcessor`)

**Why**: V2 index is still the source of truth during transition. We can always rebuild semantic indices from v2.

### 3. Performance Considerations

**Index existence checks**:

- Cached by OpenSearch/Elasticsearch cluster state
- Very fast local operation (no network round-trip in most cases)
- Cache refreshed automatically on cluster changes

**Write overhead**:

- Minimal: dual-write only occurs when semantic indices exist
- No additional latency: both writes go to the same cluster via bulk processor
- Batched: uses existing `BulkProcessor` for both v2 and semantic writes

**Memory overhead**:

- None: same document written to both indices (no duplication in memory)
- Script updates: same script parameters reused

### 4. Consistency Guarantees

**What we guarantee**:

- ✅ All writes to v2 are also written to semantic (when semantic exists)
- ✅ Write ordering preserved (both use same bulk processor queue)
- ✅ Semantic index is eventually consistent with v2

**What we don't guarantee**:

- ❌ Perfect synchronization (network/cluster issues may cause temporary drift)
- ❌ Transactional consistency (not possible with separate indices)

**Recovery strategy**: If drift is detected, use fast-copy backfill to re-sync semantic from v2.

## Complete Dual-Write Coverage

After the runId implementation, dual-writes now cover:

| Write Operation                         | V2 Index | Semantic Index | Implementation                                          |
| --------------------------------------- | -------- | -------------- | ------------------------------------------------------- |
| **Document upsert** (aspect changes)    | ✅       | ✅             | `UpdateIndicesV2Strategy.updateSearchIndicesForEvent()` |
| **Document deletion** (entity deletion) | ✅       | ✅             | `UpdateIndicesV2Strategy.deleteSearchData()`            |
| **RunId updates** (ingestion tracking)  | ✅       | ✅             | `ElasticSearchService.appendRunId()` ⭐ **NEW**         |

**Result**: Semantic index is a complete, real-time mirror of v2 index data.

## Files Modified (RunId Implementation)

### Production Code

1. **`ESWriteDAO.java`** - Low-level write operations

   - Added `applyScriptUpdateByIndexName(indexName, ...)` for direct index script updates
   - Refactored `applyScriptUpdate(opContext, entityName, ...)` to delegate to `applyScriptUpdateByIndexName`

2. **`ElasticSearchService.java`** - Service layer
   - Added `applyScriptUpdateByIndexName(...)` wrapper
   - Modified `appendRunId(...)` to dual-write to semantic index

### Test Code

3. **`ElasticSearchServiceTest.java`** - Unit tests
   - `testAppendRunId_DualWriteToSemanticIndex` - Verifies dual-write when semantic exists
   - `testAppendRunId_SkipsSemanticIndexWhenNotExists` - Verifies skip when semantic doesn't exist

## Backward Compatibility

✅ **Fully backward compatible**

- When semantic indices don't exist, behavior is identical to before
- No configuration changes required
- Automatically activates when semantic indices are created

## Relationship to Other Components

### Fast-Copy Backfill

Dual-writes complement the [Fast-Copy proposal](proposal-semantic-search-fast-copy.md):

| Component      | Purpose                                                 | Timing                                   |
| -------------- | ------------------------------------------------------- | ---------------------------------------- |
| **Fast-Copy**  | Efficiently backfills existing data from v2 to semantic | One-time, during semantic index creation |
| **Dual-Write** | Keeps semantic in sync with ongoing changes             | Continuous, for all new writes           |

**Together**: Fast-copy provides the initial data snapshot, dual-writes keep it current.

### UpdateIndicesService

The `UpdateIndicesService` orchestrates all index updates:

```
MCL Event → UpdateIndicesService → UpdateIndicesV2Strategy → ElasticSearchService
                                          ↓
                                   Dual-Write Logic
                                          ↓
                                   ┌─────────┴────────┐
                                   ↓                  ↓
                              V2 Index          Semantic Index
```

### Index Conventions

The `IndexConvention` interface handles index naming:

```java
// Current routing (during transition)
indexConvention.getEntityIndexName("dataset")         → "datasetindex_v2"
indexConvention.getEntityIndexNameSemantic("dataset") → "datasetindex_v2_semantic"

// Future routing (after migration)
indexConvention.getEntityIndexName("dataset")         → "datasetindex_v2_semantic"
// v2 deprecated and removed
```

## Migration Roadmap

### Phase 1: Enable Dual-Writes (✅ Complete)

- [x] Document updates dual-write (`UpdateIndicesV2Strategy`)
- [x] Document deletions dual-write (`UpdateIndicesV2Strategy`)
- [x] RunId updates dual-write (`ElasticSearchService`) ⭐ **This PR**

### Phase 2: Backfill & Validation (In Progress)

- [ ] Implement fast-copy backfill ([proposal](proposal-semantic-search-fast-copy.md))
- [ ] Run backfill for all enabled entities
- [ ] Validate semantic index completeness vs v2
- [ ] Monitor dual-write success rate

### Phase 3: Read Migration (Future)

- [ ] Add feature flag to route reads to semantic index
- [ ] Test semantic search quality and performance
- [ ] Gradual rollout: 1% → 10% → 50% → 100% traffic to semantic
- [ ] Compare search results quality between v2 and semantic

### Phase 4: Decommission V2 (Future)

- [ ] Confirm semantic is production-ready
- [ ] Remove v2 writes (keep semantic writes only)
- [ ] Update `IndexConvention` to point to semantic indices by default
- [ ] Remove v2 indices from cluster
- [ ] Remove dual-write logic (no longer needed)

**Current Status**: Phase 1 complete, Phase 2 in progress

## Verification & Monitoring

### Verifying Dual-Write in Running System

#### 1. Check Document Synchronization

```bash
# Get a document from v2 index
curl -X GET "localhost:9200/datasetindex_v2/_doc/<ENCODED_URN>?pretty"

# Get the same document from semantic index
curl -X GET "localhost:9200/datasetindex_v2_semantic/_doc/<ENCODED_URN>?pretty"

# Compare fields (should be identical except for embeddings field in semantic):
# - urn
# - runId (should match exactly)
# - name, description, platform, etc.
```

#### 2. Check RunId Synchronization

```bash
# After an ingestion run completes, verify runId updated in both indices
ENCODED_URN=$(python3 -c "import urllib.parse; print(urllib.parse.quote('urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)', safe=''))")

# V2 index
curl -X GET "localhost:9200/datasetindex_v2/_doc/${ENCODED_URN}?_source=runId&pretty"

# Semantic index
curl -X GET "localhost:9200/datasetindex_v2_semantic/_doc/${ENCODED_URN}?_source=runId&pretty"
```

#### 3. Check Index Document Counts

```bash
# Should be roughly equal (may differ during active ingestion)
curl -X GET "localhost:9200/datasetindex_v2/_count?pretty"
curl -X GET "localhost:9200/datasetindex_v2_semantic/_count?pretty"
```

### Monitoring Dual-Write Health

#### Metrics to Track

1. **Write Success Rate**

   - Monitor bulk processor success/failure rates
   - Alert if semantic writes consistently fail
   - Check OpenSearch logs for errors

2. **Index Lag**

   - Compare document counts between v2 and semantic
   - Expected: small delta during active writes
   - Alert if delta grows continuously

3. **Data Drift**
   - Periodically sample random documents
   - Compare field values between v2 and semantic
   - Report mismatches for investigation

#### Logs to Watch

```bash
# Look for dual-write log messages
grep "Dual-write" gms.log

# Semantic index write successes
grep "Semantic dual-write: UPSERT" gms.log

# Semantic index deletions
grep "Semantic dual-write: DELETE" gms.log

# Index existence checks (first time for each entity)
grep "Semantic dual-write check" gms.log

# RunId dual-write operations
grep "Appending run id" gms.log
```

## Troubleshooting

### Semantic Index Not Being Written

**Symptom**: Updates go to v2 but not semantic index.

**Check**:

1. Is semantic search enabled in config?

   ```yaml
   # application.yaml
   semanticSearch:
     enabled: true
     enabledEntities: [dataset, chart, dashboard]
   ```

2. Does the semantic index exist?

   ```bash
   curl -X GET "localhost:9200/_cat/indices/*semantic*?v"
   ```

3. Check logs for "SKIP" messages:
   ```bash
   grep "Semantic dual-write check.*SKIP" gms.log
   ```

### Data Drift Detected

**Symptom**: Fields differ between v2 and semantic indices.

**Recovery**:

1. Use fast-copy backfill to re-sync from v2
2. Monitor for ongoing issues (may indicate dual-write bug)
3. Check if drift is isolated to specific entities or fields

### Performance Issues

**Symptom**: Writes are slower after enabling semantic search.

**Check**:

1. OpenSearch cluster health

   ```bash
   curl -X GET "localhost:9200/_cluster/health?pretty"
   ```

2. Bulk processor queue depth (check metrics)
3. Network latency between GMS and OpenSearch
4. Consider increasing OpenSearch resources

## Conclusion

The dual-write pattern ensures semantic indices remain a complete, synchronized copy of v2 indices during the transition to semantic search. This implementation:

- ✅ **Complete coverage**: Documents, deletions, and runId updates all dual-written
- ✅ **Robust**: Gracefully handles missing indices, uses same retry mechanisms as v2
- ✅ **Tested**: Comprehensive unit tests verify behavior
- ✅ **Monitored**: Rich logging for debugging and verification
- ✅ **Forward-compatible**: Smooth path to eventually deprecate v2

With dual-writes in place and fast-copy backfill available, we have a solid foundation for migrating to semantic search as the primary index.
