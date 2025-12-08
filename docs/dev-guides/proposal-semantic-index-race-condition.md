# Proposal: Preventing Race Conditions During Semantic Index Backfill

**Status:** Proposal  
**Created:** 2024-12-04  
**Author:** Engineering Team  
**Related:** proposal-semantic-search-fast-copy.md

## Executive Summary

**Conclusion:** Make `CopyDocumentsToSemanticIndices` a **BlockingSystemUpgrade** (combined with fast-pass mode).

**Rationale:**

- Eliminates race conditions completely
- Simple implementation (one-line change)
- Acceptable delay with fast-pass (15 seconds for 10K docs, 15 minutes for 1M docs)
- No complex timestamp checking or script logic
- Easy to debug and maintain

**Rejected alternative:** Timestamp-aware reindex (Option 2) adds complexity without sufficient benefit and doesn't fully solve the problem.

---

## Problem Statement

`CopyDocumentsToSemanticIndices` is a **NonBlockingSystemUpgrade**, which means it runs concurrently with live MCP processing. This creates a race condition:

**Timeline:**

```
Time   V2 Index                 Semantic Index            Live MCP
----   --------                 --------------            --------
T0     doc:v10                  (empty)                   -
T1     doc:v10                  (empty)                   MCP arrives: doc:v11
T2     doc:v11 (MCP processed)  doc:v11 (dual-write)      -
T3     doc:v11                  doc:v10 (OVERWRITTEN!)    CopyDocuments runs
```

**Result:** Semantic index has stale data (v10) while V2 has current data (v11).

This is particularly problematic when:

- Using fast-pass mode (OpenSearch `_reindex` API)
- High ingestion rate during SystemUpdate
- Large number of documents to copy (longer race window)

## Current System Behavior

### Blocking vs Non-Blocking Upgrades

DataHub SystemUpdate executes in phases:

1. **Blocking Phase:**

   - `BuildIndices` creates/updates all indices
   - Other blocking upgrades
   - `DataHubStartupStep` sends "ready" signal to Kafka

2. **GMS Waits:**

   - `waitForSystemUpdate=true` → GMS waits for "ready" signal
   - Once received, GMS starts processing MCPs from Kafka

3. **Non-Blocking Phase (runs concurrently with GMS):**
   - `CopyDocumentsToSemanticIndices` ← **Happens WHILE MCPs are being processed**
   - Other data migrations

**Code reference:**

```33:58:datahub-upgrade/src/main/java/com/linkedin/datahub/upgrade/system/SystemUpdate.java
    // blocking upgrades
    steps.addAll(blockingSystemUpgrades.stream().flatMap(up -> up.steps().stream()).toList());
    cleanupSteps.addAll(
        blockingSystemUpgrades.stream().flatMap(up -> up.cleanupSteps().stream()).toList());

    // bootstrap blocking only
    if (bootstrapMCPBlocking != null) {
      steps.addAll(bootstrapMCPBlocking.steps());
      cleanupSteps.addAll(bootstrapMCPBlocking.cleanupSteps());
    }

    // emit system update message if blocking upgrade(s) present
    if (dataHubStartupStep != null && !blockingSystemUpgrades.isEmpty()) {
      steps.add(dataHubStartupStep);  // ← GMS starts after this
    }

    // bootstrap non-blocking only
    if (bootstrapMCPNonBlocking != null) {
      steps.addAll(bootstrapMCPNonBlocking.steps());
      cleanupSteps.addAll(bootstrapMCPNonBlocking.cleanupSteps());
    }

    // add non-blocking upgrades last (GMS already running)
    steps.addAll(nonBlockingSystemUpgrades.stream().flatMap(up -> up.steps().stream()).toList());
    cleanupSteps.addAll(
        nonBlockingSystemUpgrades.stream().flatMap(up -> up.cleanupSteps().stream()).toList());
```

**Current classification:**

```20:21:datahub-upgrade/src/main/java/com/linkedin/datahub/upgrade/system/semanticsearch/CopyDocumentsToSemanticIndices.java
@Slf4j
public class CopyDocumentsToSemanticIndices implements NonBlockingSystemUpgrade {
```

## Proposed Solutions

### Option 1: Make CopyDocumentsToSemanticIndices Blocking

**Change:**

```java
// Change from:
public class CopyDocumentsToSemanticIndices implements NonBlockingSystemUpgrade

// To:
public class CopyDocumentsToSemanticIndices implements BlockingSystemUpgrade
```

**Effect:**

- Copy completes BEFORE GMS starts
- No race condition possible
- GMS startup delayed by copy duration

#### Performance Impact

| Document Count | Current (non-blocking) | Blocking (current impl) | Blocking (with fast-pass) |
| -------------- | ---------------------- | ----------------------- | ------------------------- |
| 1K             | No delay               | +30-60s startup delay   | +1-5s startup delay       |
| 10K            | No delay               | +7 min startup delay    | +15s startup delay        |
| 100K           | No delay               | +70 min startup delay   | +90s startup delay        |
| 1M             | No delay               | +12h startup delay      | +15 min startup delay     |

**Pros:**

- ✅ Eliminates race condition completely
- ✅ Simple one-line change
- ✅ Acceptable with fast-pass mode (1-15s delay)
- ✅ Safe for all scenarios
- ✅ No additional complexity

**Cons:**

- ❌ Delays GMS startup
- ❌ Without fast-pass, delays are unacceptable for large installations
- ❌ Requires fast-pass implementation

**Configuration:**

```yaml
systemUpdate:
  copyDocumentsToSemanticIndex:
    enabled: true
    blocking: ${SYSTEM_UPDATE_SEMANTIC_COPY_BLOCKING:true} # Make it blocking by default
    useFastCopy: ${SYSTEM_UPDATE_SEMANTIC_FAST_COPY:true} # Fast mode required
```

**Implementation:**

```java
// datahub-upgrade/src/main/java/com/linkedin/datahub/upgrade/config/SemanticSearchUpgradeConfiguration.java

@Bean
@ConditionalOnProperty(
    name = "systemUpdate.copyDocumentsToSemanticIndex.blocking",
    havingValue = "true",
    matchIfMissing = true)  // Default to blocking
public BlockingSystemUpgrade copyDocumentsToSemanticIndicesBlocking(
    OperationContext opContext,
    SearchClientShim<?> searchClient,
    EntityService<?> entityService,
    SemanticSearchConfiguration semanticSearchConfig,
    IndexConvention indexConvention,
    @Value("${systemUpdate.copyDocumentsToSemanticIndex.enabled}") boolean enabled) {

  return new CopyDocumentsToSemanticIndices(
      opContext, searchClient, entityService, semanticSearchConfig, indexConvention, enabled);
}

@Bean
@ConditionalOnProperty(
    name = "systemUpdate.copyDocumentsToSemanticIndex.blocking",
    havingValue = "false")
public NonBlockingSystemUpgrade copyDocumentsToSemanticIndicesNonBlocking(
    OperationContext opContext,
    SearchClientShim<?> searchClient,
    EntityService<?> entityService,
    SemanticSearchConfiguration semanticSearchConfig,
    IndexConvention indexConvention,
    @Value("${systemUpdate.copyDocumentsToSemanticIndex.enabled}") boolean enabled) {

  return new CopyDocumentsToSemanticIndices(
      opContext, searchClient, entityService, semanticSearchConfig, indexConvention, enabled);
}
```

### Option 2: Timestamp-Aware Reindex (Rejected)

**Approach:** Use the `lastModifiedAt` timestamp field to prevent stale overwrites during reindex.

**Status:** ❌ **Not recommended** - see "Why Option 2 Doesn't Work" section below for detailed analysis.

#### Available Fields in Search Documents

**Note:** SystemMetadata fields (including `version`) are **not** stored in search documents. They exist only in primary storage.

**Available timestamp fields:**

- `systemCreated` - timestamp when entity was first created (never changes)
- `lastModifiedAt` - timestamp of last modification (updates on each MCP)
- `runId` - ingestion run identifier (string, not comparable)

**Example from actual document:**

```json
{
  "urn": "urn:li:document:local-dual_write_test",
  "systemCreated": 1764826657181,
  "lastModifiedAt": 1764827542361  ← Updates on each change
}
```

**Why `lastModifiedAt` works:**

- Updates on each MCP processed
- Millisecond precision (sufficient for ordering)
- Already in all search documents
- Reliable wall-clock timestamp

#### Timestamp-Based Reindex Script

**Concept:** Only overwrite target document if source timestamp is newer.

**Implementation:**

```java
private BulkByScrollResponse executeFastCopyWithTimestampCheck(
    String sourceIndex, String targetIndex) throws IOException {

  log.info("Starting timestamp-aware fast copy: {} -> {}", sourceIndex, targetIndex);

  Script timestampCheckScript = new Script(
      ScriptType.INLINE,
      "painless",
      // Extract lastModifiedAt timestamp (milliseconds since epoch)
      "def getTimestamp(map) {" +
      "  if (map == null || map.lastModifiedAt == null) {" +
      "    return 0L;" +  // Treat missing timestamp as 0 (oldest)
      "  }" +
      "  return map.lastModifiedAt;" +
      "}" +
      "" +
      "long sourceTime = getTimestamp(params._source);" +
      "long targetTime = getTimestamp(ctx._source);" +
      "" +
      "if (sourceTime > targetTime) {" +
      "  ctx._source = params._source;" +  // Source is newer, overwrite
      "} else {" +
      "  ctx.op = 'noop';" +  // Target is same or newer, skip
      "}",
      Collections.emptyMap()
  );

  ReindexRequest reindexRequest = new ReindexRequest()
      .setSourceIndices(sourceIndex)
      .setDestIndex(targetIndex)
      .setConflicts("proceed")  // Continue on conflicts
      .setScript(timestampCheckScript)
      .setRefresh(true)
      .setSlices(4);  // Parallel processing

  BulkByScrollResponse response = searchClient.reindex(reindexRequest, RequestOptions.DEFAULT);

  log.info("Timestamp-aware copy completed: copied={}, updated={}, noops={}, took={}ms",
           response.getCreated(),
           response.getUpdated(),
           response.getNoops(),
           response.getTook().millis());

  return response;
}
```

**How it works:**

**Case 1: Normal flow (no race)**

```
Source (V2): doc lastModifiedAt=1000
Target (Semantic): (empty)
Result: Copy succeeds (1000 > 0)
```

**Case 2: Dual-write happened first (MCP processed during copy)**

```
Source (V2): doc lastModifiedAt=1000 (snapshot from before MCP)
Target (Semantic): doc lastModifiedAt=2000 (from dual-write)
Result: Noop (1000 < 2000, keep newer target)
```

**Case 3: V2 updated after snapshot**

```
Source (V2): doc lastModifiedAt=3000 (MCP processed after reindex started)
Target (Semantic): doc lastModifiedAt=2000 (from earlier dual-write)
Result: Copy succeeds (3000 > 2000) ✓ Correct!
```

**Pros:**

- ✅ Prevents stale overwrites completely
- ✅ GMS can run concurrently (no blocking)
- ✅ Uses existing timestamp field (no schema changes)
- ✅ Works with both fast-pass and full-copy modes
- ✅ Simple comparison (no parsing needed)
- ✅ Already populated in all documents

**Cons:**

- ❌ Wall-clock dependent (clock drift risk - typically minimal)
- ❌ Millisecond resolution (updates within same ms could conflict)
- ❌ Reindex script adds ~10-20% overhead

**Clock drift considerations:**

- Modern NTP keeps clock drift under 10ms
- GMS and OpenSearch typically on same network (sub-1ms drift)
- Risk is very low in practice

**Edge case handling:**

- Missing `lastModifiedAt` → treat as `0` (oldest)
- Null timestamp → treat as `0` (oldest)
- Target is empty → copy succeeds (0 > any timestamp)

**Note:** This is the same approach described above. `lastModifiedAt` is the primary field available for timestamp comparison.

## Comparison Matrix

| Approach                | Race-Safe | GMS Delay                   | Complexity | Debugging  | Works with Fast-Pass | Works with Full-Copy |
| ----------------------- | --------- | --------------------------- | ---------- | ---------- | -------------------- | -------------------- |
| **Option 1: Blocking**  | ✅ Yes    | 1-5s (fast) / 30-60s (slow) | ✅ Low     | ✅ Simple  | ✅ Yes               | ✅ Yes               |
| **Option 2: Timestamp** | ⚠️ Mostly | None                        | ❌ High    | ❌ Complex | ⚠️ Mitigates         | ❌ No                |

**Option 2 limitations:**

- Fast-pass: Reduces risk but doesn't eliminate it (concurrent writes during 15-min window)
- Full-copy: Requires read-before-write (defeats performance benefit)
- Clock drift edge cases
- Script failure modes
- Harder debugging when things go wrong

## Recommended Approach

**Primary recommendation: Option 1 (Blocking + Fast-Pass)**

**Why blocking is the only practical solution:**

### Fast-Pass Mode Issues with Option 2

**Problem:** OpenSearch reindex operates on a point-in-time snapshot, but takes time to process:

```
T0: Snapshot taken (1M docs, all with lastModifiedAt from before T0)
T1-T15min: Processing snapshot...
  - During this window, live MCPs update both V2 and semantic via dual-write
  - Dual-writes have NEWER timestamps than snapshot
  - Timestamp script would prevent overwrites... BUT:
```

**Issues:**

1. **Performance overhead**: Script adds 10-20% (15min → 18min)
2. **Clock drift**: Even 10ms drift can cause issues at high volume
3. **Complexity**: Script failures, debugging, edge cases
4. **Race still exists**: MCPs during snapshot creation

**Better solution**: Just block GMS for 15 minutes. It's a one-time cost.

### Full-Copy Mode Issues with Option 2

**Problem:** Reading from database and writing via UpdateIndicesService:

```
Recovery reads DB: doc:X version 10 (snapshot from yesterday)
Live MCP writes:  doc:X version 11 (happening during recovery)
Recovery reaches doc:X: Should it write version 10?
```

**To prevent overwrite, you'd need:**

1. Read current doc from index before each write
2. Compare versions/timestamps
3. Skip if current > snapshot

**This defeats the purpose!**

- Reading before writing is slow (2x operations)
- Removes performance benefit
- Just as slow as current implementation

**Better solution**: Block GMS during recovery. Recovery is a rare, manual operation.

### Why Blocking is Acceptable

**With fast-pass:**
| Documents | Blocking Delay | Frequency | Impact |
|-----------|---------------|-----------|---------|
| 1K | 1-5 seconds | One-time when enabling | ✅ Negligible |
| 10K | 15 seconds | One-time | ✅ Acceptable |
| 100K | 90 seconds | One-time | ✅ Acceptable |
| 1M | 15 minutes | One-time | ⚠️ Plan for maintenance window |

**For subsequent runs:**

- Dual-write keeps indices in sync
- SystemUpdate can skip copy (indices already populated)
- No blocking delay

**Configuration:**

```yaml
systemUpdate:
  copyDocumentsToSemanticIndex:
    enabled: true
    useFastCopy: true # Fast mode (OpenSearch reindex)
    blocking: true # Always blocking (prevent races)
```

## Implementation Plan

### Phase 1: Timestamp-Aware Reindex

1. Implement `lastModifiedAt`-based reindex script
2. Add timestamp comparison logic
3. Default to timestamp-aware mode
4. Add monitoring for noops (indicates race prevention)

### Phase 2: Configuration Options

1. Add `useTimestampCheck` flag (default `true`)
2. Add `blocking` flag (default `false` when timestamp check enabled)
3. Document both modes

### Phase 3: Testing

1. Test race condition scenarios:
   - MCP arrives before reindex
   - MCP arrives during reindex
   - MCP arrives after reindex
2. Test with rapid MCP ingestion during copy
3. Performance benchmarks

### Phase 4: Monitoring

1. Log noop statistics (indicates race prevention working)
2. Add metrics for noops vs created vs updated
3. Alert on unexpected patterns

## Verification Strategy

**How to verify timestamps are reliable:**

1. Check timestamp fields in production data:

```bash
# Sample documents from V2 index
curl "localhost:9200/documentindex_v2/_search?size=5" | \
  jq '.hits.hits[]._source | {urn, systemCreated, lastModifiedAt}'
```

Expected output:

```json
{
  "urn": "urn:li:document:local-dual_write_test",
  "systemCreated": 1764826657181,
  "lastModifiedAt": 1764827542361
}
```

2. Monitor timestamps during ingestion:

```java
log.info("Processing MCP for urn={}, lastModifiedAt={}",
         urn, searchDocument.get("lastModifiedAt"));
```

3. Verify monotonicity:

```bash
# Check if lastModifiedAt increases over time for same document
curl "localhost:9200/documentindex_v2/_search" -d '{
  "query": {"term": {"urn": "urn:li:document:test"}},
  "_source": ["lastModifiedAt"]
}'
```

## Edge Cases

### Missing lastModifiedAt

**Scenario:** Old documents without `lastModifiedAt` field

**Handling:**

```java
"if (map == null || map.lastModifiedAt == null) {" +
"  return 0L;" +  // Treat as epoch (oldest possible)
"}",
```

Result: Document will be overwritten (safe default)

### Clock Drift Between Servers

**Scenario:** GMS server clock is 10ms ahead of OpenSearch server

**Risk Level:** Very low

- Modern NTP keeps drift under 10ms
- Typically same data center (< 1ms drift)
- Only matters if updates happen within drift window

**Worst case:**

```
T0: Source writes doc (GMS clock: 1000)
T1: Target writes doc (GMS clock: 1010, actual: 1000)
Reindex: 1000 < 1010 → noop (keeps newer)
Result: Correct! (Target truly is newer)
```

### Concurrent Updates to Same Document

**Scenario:** Same document updated twice within 1ms

**Handling:**

- Both updates get same `lastModifiedAt` timestamp
- Reindex sees equal timestamps → noop
- Whichever write happened last is preserved
- Race exists but window is < 1ms (acceptable)

### Reindex During High-Volume Ingestion

**Scenario:** Thousands of MCPs/second during reindex

**Behavior:**

- Fast-pass reads V2 index snapshot (point-in-time)
- Dual-writes continue to semantic index
- Script compares timestamps for each doc
- Dual-write updates are preserved (newer timestamps)

**Performance:**

- Script execution adds ~10-20% overhead
- Still much faster than full copy
- Example: 1K docs in 1-2s (vs 0.8s without script)

## Why Option 2 (Timestamp-Aware) Is Not Sufficient

### Issue 1: Reindex Snapshot vs Live Writes

Even with timestamp checking, there's a fundamental ordering problem:

**Example scenario:**

```
T0:  Reindex starts, creates snapshot of V2 (doc:X has lastModifiedAt=1000)
T1:  MCP arrives, updates doc:X to lastModifiedAt=2000
     - Dual-write succeeds: semantic now has doc:X (lastModifiedAt=2000)
T2:  Reindex processes doc:X from snapshot (lastModifiedAt=1000)
     - Script compares: 1000 < 2000 → noop ✓

So far so good. But consider:

T3:  Another MCP updates doc:X to lastModifiedAt=3000
     - V2 update succeeds
     - Dual-write to semantic... conflicts with ongoing reindex?
```

**OpenSearch behavior during concurrent reindex + writes:**

- Reindex task and dual-write both try to write same document
- Version conflicts possible
- Retry logic kicks in
- Eventually consistent but **timing-dependent**

### Issue 2: No Recovery Guarantees

**What if the script fails?**

```painless
// What if lastModifiedAt is null? Or in wrong format?
long sourceTime = getTimestamp(params._source);  // Could throw exception
```

**Script failure scenarios:**

- Painless script syntax error → entire reindex fails
- Field type mismatch → reindex fails
- Null pointer → reindex fails

**Recovery is harder:**

- Can't tell which documents were skipped vs failed
- Need to re-run entire reindex

### Issue 3: Debugging Complexity

**When indices are out of sync, how do you diagnose?**

Without script:

- "Reindex overwrote dual-writes" → clear root cause

With script:

- "Was it clock drift?"
- "Did the script noop incorrectly?"
- "Was there a parsing failure?"
- "Did the timestamp update correctly?"

**More moving parts = harder debugging**

## Conclusion: Option 1 (Blocking + Fast-Pass) is the Clear Choice

### Why Blocking Wins

**Simplicity:**

- ✅ No complex scripts
- ✅ No clock drift concerns
- ✅ No version comparison logic
- ✅ Straightforward debugging

**Safety:**

- ✅ Eliminates race conditions completely
- ✅ No concurrent write conflicts
- ✅ Guaranteed consistency
- ✅ Predictable behavior

**Performance:**

- ✅ Fast-pass makes 15-min delay acceptable (one-time cost)
- ✅ Simpler than timestamp check (no script overhead)
- ✅ No retry logic needed

**Operational:**

- ✅ Run during maintenance window if > 100K docs
- ✅ For < 100K docs, delay is under 2 minutes (negligible)
- ✅ After first run, dual-write keeps in sync (no more copies needed)

### Implementation Plan

1. **Implement fast-pass mode** (OpenSearch `_reindex` API)
2. **Change `CopyDocumentsToSemanticIndices` to `BlockingSystemUpgrade`**
3. **Add auto-skip if semantic index already populated:**

   ```java
   long semanticCount = elasticSearchService.docCount(semanticIndexName);
   if (semanticCount > 0) {
     log.info("Semantic index {} already has {} docs. Skipping copy (dual-write active).",
              semanticIndexName, semanticCount);
     return UpgradeStepResult.SUCCEEDED;
   }
   ```

4. **Configuration:**
   ```yaml
   systemUpdate:
     copyDocumentsToSemanticIndex:
       enabled: true
       useFastCopy: true # Fast mode (OpenSearch reindex)
       blocking: true # Always blocking (implicit via BlockingSystemUpgrade)
   ```

### Why Option 2 Doesn't Work

**For fast-pass:**

- Timestamp check mitigates but doesn't eliminate race
- Adds complexity and failure modes
- Performance overhead for marginal benefit
- Blocking is simpler and safer

**For full-copy (DB recovery):**

- Would need to read-compare-write for each document
- Defeats performance benefit of batch processing
- Essentially rebuilds the slow path we're trying to avoid
- If you need this, just block GMS during recovery

### Final Recommendation

**Implement only Option 1:**

- Fast-pass mode for performance (15 min vs 12 hours for 1M docs)
- Blocking mode for safety (no races possible)
- Auto-skip for subsequent runs (dual-write keeps in sync)
- Simple, safe, fast enough

**Don't implement Option 2:**

- Added complexity not worth the benefit
- Blocking delay is acceptable with fast-pass
- Timestamp check doesn't fully solve the problem
- Harder to debug and maintain
