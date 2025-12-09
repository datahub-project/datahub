# Proposal: Fast-Pass Mode for Semantic Index Backfill

**Status:** Proposal  
**Created:** 2024-12-04  
**Author:** Engineering Team

## Problem Statement

The current `CopyDocumentsToSemanticIndex` upgrade step reads documents from primary storage (Cassandra/MySQL), transforms them through `SearchDocumentTransformer`, and writes to the semantic index. This approach is slow and resource-intensive for large installations.

**Performance characteristics:**

- **Current approach**: ~30-60 seconds for 1,000 documents
- **Database load**: Reads from primary storage for every document
- **Memory overhead**: Full transformation pipeline in GMS
- **Network hops**: DB → GMS → OpenSearch

## Proposed Solution

Add a "fast-pass" mode that uses OpenSearch's native `_reindex` API to copy documents directly from V2 index to semantic index, bypassing primary storage and transformation.

**Performance characteristics:**

- **Fast-pass approach**: ~1-5 seconds for 1,000 documents
- **Database load**: None (OpenSearch-internal operation)
- **Memory overhead**: Minimal (no GMS involvement)
- **Network hops**: OpenSearch internal (same cluster)

## Design

### Configuration

Add a configurable flag to control copy strategy:

```yaml
# application.yaml
systemUpdate:
  copyDocumentsToSemanticIndex:
    enabled: ${SYSTEM_UPDATE_COPY_DOCUMENTS_TO_SEMANTIC_INDEX_ENABLED:false}
    useFastCopy: ${SYSTEM_UPDATE_SEMANTIC_FAST_COPY:true} # Default to fast mode
```

### Implementation

Modify `CopyDocumentsToSemanticIndexStep.java`:

```java
public class CopyDocumentsToSemanticIndexStep implements UpgradeStep {

  private final boolean useFastCopy;

  @Override
  public void execute() throws Exception {
    String sourceIndex = indexConvention.getEntityIndexName(entityName);  // e.g., documentindex_v2
    String targetIndex = indexConvention.getEntityIndexNameSemantic(entityName);  // e.g., documentindex_v2_semantic

    if (useFastCopy) {
      executeFastCopy(sourceIndex, targetIndex);
    } else {
      executeFullCopy(sourceIndex, targetIndex);
    }
  }

  /**
   * Fast copy using OpenSearch reindex API.
   * Copies documents directly from V2 index to semantic index within OpenSearch.
   */
  private void executeFastCopy(String sourceIndex, String targetIndex) throws IOException {
    log.info("Starting fast copy for entity '{}': {} -> {}",
             entityName, sourceIndex, targetIndex);

    // Ensure target index exists
    if (!elasticSearchService.indexExists(targetIndex)) {
      throw new IllegalStateException(
          String.format("Target semantic index %s does not exist. " +
                       "Run BuildIndices upgrade step first.", targetIndex));
    }

    // Build reindex request
    ReindexRequest reindexRequest = new ReindexRequest()
        .setSourceIndices(sourceIndex)
        .setDestIndex(targetIndex)
        .setRefresh(true)  // Refresh after reindex for immediate visibility
        .setSlices(4);     // Parallel slices for faster processing

    // Execute reindex
    BulkByScrollResponse response =
        elasticSearchService.reindex(reindexRequest);

    log.info("Fast copy completed for entity '{}': copied {} documents in {}ms",
             entityName,
             response.getCreated(),
             response.getTook().millis());
  }

  /**
   * Full copy using primary storage and transformation.
   * Used for recovery scenarios or when V2 index might be stale.
   */
  private void executeFullCopy(String sourceIndex, String targetIndex) {
    // Existing implementation - read from primary storage
    log.info("Starting full copy for entity '{}': reading from primary storage", entityName);

    // ... existing code ...
  }
}
```

### Required Changes

#### 1. Add reindex method to ElasticSearchService

```java
// metadata-io/src/main/java/com/linkedin/metadata/search/elasticsearch/ElasticSearchService.java

public BulkByScrollResponse reindex(@Nonnull ReindexRequest request) {
  return esWriteDAO.reindex(request);
}
```

#### 2. Add reindex method to ESWriteDAO

```java
// metadata-io/src/main/java/com/linkedin/metadata/search/elasticsearch/update/ESWriteDAO.java

public BulkByScrollResponse reindex(@Nonnull ReindexRequest request) throws IOException {
  log.info("Executing reindex from {} to {}",
           request.getSourceIndices(),
           request.getDestIndex());

  return searchClient.reindex(request, RequestOptions.DEFAULT);
}
```

#### 3. Update configuration classes

```java
// metadata-service/configuration/src/main/java/com/linkedin/metadata/config/SystemUpdateConfiguration.java

@Data
public class SystemUpdateConfiguration {
  private boolean waitForSystemUpdate = true;
  private CopyDocumentsConfig copyDocumentsToSemanticIndex;

  @Data
  public static class CopyDocumentsConfig {
    private boolean enabled = false;
    private boolean useFastCopy = true;  // Default to fast mode
  }
}
```

#### 4. Inject configuration into upgrade step

```java
// datahub-upgrade/src/main/java/com/linkedin/datahub/upgrade/system/SystemUpdate.java

private List<UpgradeStep> buildSteps() {
  List<UpgradeStep> steps = new ArrayList<>();

  // ... other steps ...

  if (semanticSearchConfig.isEnabled()) {
    for (String entityName : semanticSearchConfig.getEnabledEntities()) {
      boolean useFastCopy =
          systemUpdateConfig.getCopyDocumentsToSemanticIndex().isUseFastCopy();

      steps.add(new CopyDocumentsToSemanticIndexStep(
          elasticSearchService,
          entityName,
          indexConvention,
          useFastCopy));  // Pass flag
    }
  }

  return steps;
}
```

## Trade-offs

### When to Use Fast-Pass (Default)

✅ **Use fast-pass when:**

- V2 indices are current (normal operations)
- Enabling semantic search for the first time
- Re-syncing after temporary issues
- Time-sensitive deployments

### When to Use Full Copy

✅ **Use full copy when:**

- Recovering from V2 index corruption
- V2 indices were rebuilt from scratch
- Suspecting data inconsistencies
- Need guaranteed source-of-truth rebuild

## Rollout Plan

### Phase 1: Implementation

1. Add configuration flag (default `true`)
2. Implement fast-pass logic using OpenSearch `_reindex` API
3. Keep existing full-copy logic as fallback
4. Add unit tests for both modes

### Phase 2: Testing

1. Test with small dataset (100 docs)
2. Test with medium dataset (10K docs)
3. Verify data integrity after fast-pass
4. Compare performance metrics

### Phase 3: Documentation

1. Update SystemUpdate docs
2. Add runbook for choosing copy mode
3. Document troubleshooting steps

## Performance Expectations

### Benchmark (1,000 documents) - estimation - not real data

| Metric              | Current (Full Copy) | Proposed (Fast-Pass) | Improvement        |
| ------------------- | ------------------- | -------------------- | ------------------ |
| Duration            | 30-60s              | 1-5s                 | **6-30x faster**   |
| Database queries    | ~1,000              | 0                    | ✅ Zero DB load    |
| Network round-trips | ~2,000              | 1                    | ✅ Minimal network |
| GMS memory          | ~500MB peak         | ~50MB                | ✅ 90% reduction   |

### Scaling (projected)

| Document Count | Current   | Fast-Pass | Savings |
| -------------- | --------- | --------- | ------- |
| 1K             | ~45s      | ~3s       | 42s     |
| 10K            | ~7 min    | ~15s      | 6m 45s  |
| 100K           | ~70 min   | ~90s      | 68 min  |
| 1M             | ~12 hours | ~15 min   | 11h 45m |

## Risk Mitigation

### Data Consistency Risk

**Risk:** V2 index might be stale or corrupted.

**Mitigation:**

1. Fast-pass is opt-in via config flag
2. Default to `true` for most users (V2 is typically current)
3. Document when to use full copy
4. Add validation step to compare sample documents

### Validation Step (Optional Enhancement)

```java
private void validateSample(String sourceIndex, String targetIndex) {
  // After reindex, compare 100 random documents
  SearchRequest sampleRequest = new SearchRequest(sourceIndex)
      .source(new SearchSourceBuilder().size(100));

  SearchResponse sourceResults = searchClient.search(sampleRequest);

  for (SearchHit hit : sourceResults.getHits()) {
    String docId = hit.getId();
    GetResponse targetDoc = searchClient.get(
        new GetRequest(targetIndex, docId));

    if (!targetDoc.isExists()) {
      log.warn("Document {} missing in target index after reindex", docId);
    }
  }
}
```

## Alternative Considered: Hybrid Approach

**Idea:** Use fast-pass by default, but verify via sampling.

```java
if (useFastCopy) {
  executeFastCopy(sourceIndex, targetIndex);

  // Sample 1% of documents to verify
  if (shouldValidate) {
    validateSample(sourceIndex, targetIndex);
  }
}
```

**Verdict:** Adds complexity. Better to keep it simple with a config flag.

## Security Considerations

- No security implications - operation is read-only from source, write-only to target
- Both indices are in the same OpenSearch cluster
- No data leaves the cluster boundary

## Backward Compatibility

- ✅ Fully backward compatible
- ✅ Existing behavior preserved when `useFastCopy=false`
- ✅ Default to fast mode for better out-of-box experience
- ✅ No API changes

## Open Questions

1. **Should we add progress logging during reindex?**

   - OpenSearch reindex task can be monitored via task API
   - Could poll and log progress: "Copied 5,000/10,000 documents (50%)"

2. **Should we support incremental fast-copy?**

   - Only copy documents modified since last run
   - Would require tracking last sync timestamp
   - Adds complexity - probably not worth it

3. **Should we validate after fast-copy?**
   - Sample validation to detect issues
   - Trade-off: adds time but increases confidence
   - Recommendation: Make it optional

## Conclusion

Fast-pass mode provides significant performance improvements (6-30x faster) with minimal risk. The implementation is straightforward using OpenSearch's native `_reindex` API. Recommend proceeding with implementation with the following defaults:

- `useFastCopy: true` (fast by default)
- Full copy available via config flag for recovery scenarios
- Optional validation for extra safety

## Example Usage

### Default (Fast Mode)

```bash
# Just run SystemUpdate - uses fast-pass by default
./gradlew :datahub-upgrade:bootRun --args='-u SystemUpdate'
```

### Recovery Mode (Full Copy)

```bash
# When V2 might be stale or corrupted
export SYSTEM_UPDATE_SEMANTIC_FAST_COPY=false
./gradlew :datahub-upgrade:bootRun --args='-u SystemUpdate'
```

### With Validation

```bash
export SYSTEM_UPDATE_SEMANTIC_FAST_COPY=true
export SYSTEM_UPDATE_SEMANTIC_VALIDATE=true
./gradlew :datahub-upgrade:bootRun --args='-u SystemUpdate'
```
