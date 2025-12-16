# Snowplow Connector - Performance & Scaling Review

**Reviewed by**: Claude (Python Expert)
**Date**: 2025-12-16
**Focus**: Scaling bottlenecks, performance issues, batch processing opportunities

---

## Executive Summary

The Snowplow connector has **4 CRITICAL scaling issues** that will cause severe performance degradation with large schemas or many enrichments:

1. ⚠️ **N+1 Query Problem**: Fetches deployments individually for each schema
2. ⚠️ **Huge DataJobInputOutput Aspects**: Creates massive lineage aspects for enrichments
3. ⚠️ **Redundant Data Fetching**: Re-fetches all schemas multiple times
4. ⚠️ **No Parallel Processing**: All operations are sequential

**Estimated impact at scale:**
- 1000 schemas + field version tracking: ~1000+ API calls, 10-20 minutes
- 50 enrichments × 500 event schemas: 25,000 lineage edges per enrichment (DataHub limitation risk)
- 100 schemas with 50 fields each: 5,000 fine-grained lineages per schema

---

## 🔴 CRITICAL ISSUES

### 1. N+1 Query Problem in Deployment Fetching

**Location**: `snowplow.py:598-619`

**Problem**:
```python
# Lines 598-619
if self.config.field_tagging.track_field_versions:
    for ds in data_structures:  # ← Loop through ALL schemas
        if not ds.hash:
            continue
        try:
            # Individual API call per schema ← N+1 PROBLEM!
            deployments = self.bdp_client.get_data_structure_deployments(
                ds.hash
            )
            ds.deployments = deployments
```

**Impact**:
- **1000 schemas** = **1000 API calls** (sequential)
- Each call: ~200-500ms
- **Total time**: 200-500 seconds (3-8 minutes) just for deployments
- Network latency multiplies this

**Solution**:
```python
# Option A: Batch API (if BDP supports it)
all_hashes = [ds.hash for ds in data_structures if ds.hash]
deployments_map = self.bdp_client.get_data_structure_deployments_batch(all_hashes)
for ds in data_structures:
    ds.deployments = deployments_map.get(ds.hash, [])

# Option B: Parallel fetching with ThreadPoolExecutor
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_deployments(ds):
    if ds.hash:
        ds.deployments = self.bdp_client.get_data_structure_deployments(ds.hash)
    return ds

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(fetch_deployments, ds) for ds in data_structures]
    data_structures = [f.result() for f in as_completed(futures)]
```

**Recommendation**:
- ✅ Implement **Option B (parallel fetching)** immediately (10-20x speedup)
- ✅ Add config: `max_concurrent_api_calls: int = 10` (with rate limiting)
- ⚠️ Request batch API from Snowplow team for future

---

### 2. Massive DataJobInputOutput Aspects for Enrichments

**Location**: `snowplow.py:2221-2372`

**Problem**:
```python
# Line 2221: Fetch ALL event schemas
event_schema_urns = self._get_event_schema_urns()  # Could be 500+ URNs

# Lines 2362-2372: Create aspect with ALL schemas as inputs
for enrichment in enrichments:  # 50 enrichments
    datajob_input_output = DataJobInputOutputClass(
        inputDatasets=event_schema_urns,  # ← ALL 500 event schemas
        outputDatasets=output_datasets,
        fineGrainedLineages=fine_grained_lineages  # ← Could be 1000s
    )
```

**Impact**:
- **50 enrichments × 500 event schemas** = **25,000 input edges per enrichment**
- **DataHub aspect size**: Could exceed 10MB per enrichment
- **DataHub GMS performance**: Huge aspects slow down entity loading
- **UI performance**: Entity page takes 5-10 seconds to load
- **Storage**: Massive aspect storage requirements

**Real-world scenario**:
```
Organization: 500 event schemas, 50 enrichments
Each enrichment gets: 500 inputs + 1 output + 100 fine-grained lineages
Total lineage edges: 50 × 501 = 25,050 edges
Aspect size per enrichment: ~5-10 MB
Total aspect storage: 250-500 MB just for enrichment lineage
```

**Solution**:

**Option A: Sample Inputs (Recommended)**
```python
# Only link representative event schemas (not all)
if len(event_schema_urns) > MAX_ENRICHMENT_INPUTS:
    logger.warning(
        f"Too many event schemas ({len(event_schema_urns)}), "
        f"sampling {MAX_ENRICHMENT_INPUTS} representative schemas"
    )
    # Sample: first N + last N + random middle
    event_schema_urns = (
        event_schema_urns[:10] +
        event_schema_urns[-10:] +
        random.sample(event_schema_urns[10:-10], 30)
    )
```

**Option B: Group by Schema Type**
```python
# Instead of linking individual schemas, link "event_schema" container
# Only show fine-grained lineages, not dataset-level lineage
datajob_input_output = DataJobInputOutputClass(
    inputDatasets=[],  # Empty - use fine-grained only
    outputDatasets=[warehouse_table_urn],
    fineGrainedLineages=fine_grained_lineages  # Show field-level only
)
```

**Option C: Config-Driven Sampling**
```python
class EnrichmentConfig(BaseModel):
    max_input_schemas_per_enrichment: int = 50
    link_all_schemas: bool = False  # If True, link all (risky at scale)
    link_representative_schemas: bool = True  # Sample approach
```

**Recommendation**:
- ✅ **Implement Option A + Option C** (configurable sampling)
- ✅ Add warning when `len(event_schema_urns) > 100`
- ⚠️ Consider not emitting dataset-level lineage at all (fine-grained is sufficient)

---

### 3. Redundant Data Fetching

**Location**: `snowplow.py:1763` in `_get_event_schema_urns()`

**Problem**:
```python
def _get_event_schema_urns(self) -> List[str]:
    # Line 1763: Fetches ALL data structures AGAIN!
    data_structures = self._get_data_structures_filtered()

    for data_structure in data_structures:
        # Process schemas...
```

**Called from**:
1. `_extract_enrichments()` - Line 2221
2. `_emit_collector_datajob()` - Called from enrichments
3. Potentially other places

**Impact**:
- **Fetches all schemas 2-3 times** during single ingestion run
- **1000 schemas × 3 fetches** = 3000 unnecessary API calls
- **Wasted time**: 5-10 minutes of redundant work

**Solution**:
```python
# Add caching at class level
class SnowplowSource(StatefulIngestionSourceBase):
    def __init__(self, ...):
        self._cached_data_structures: Optional[List[DataStructure]] = None
        self._cached_event_schema_urns: Optional[List[str]] = None

    def _get_data_structures_filtered(self) -> List[DataStructure]:
        if self._cached_data_structures is None:
            # Fetch and cache
            self._cached_data_structures = self.bdp_client.get_data_structures(...)
            # Apply filtering...
        return self._cached_data_structures

    def _get_event_schema_urns(self) -> List[str]:
        if self._cached_event_schema_urns is None:
            data_structures = self._get_data_structures_filtered()
            self._cached_event_schema_urns = [
                # Build URNs...
            ]
        return self._cached_event_schema_urns
```

**Recommendation**:
- ✅ **Implement caching immediately** (3x speedup)
- ✅ Cache at instance level (cleared between runs)
- ✅ Add `_clear_caches()` method for testing

---

### 4. No Parallel Processing

**Location**: Throughout `snowplow.py`

**Problem**:
```python
# Everything is sequential
for data_structure in data_structures:  # ← One by one
    yield from self._process_data_structure(data_structure)

for enrichment in enrichments:  # ← One by one
    yield from self._process_enrichment(enrichment)
```

**Impact**:
- **1000 schemas** processed sequentially
- **Each schema**: 500-1000ms processing time
- **Total**: 8-16 minutes (could be 2-3 minutes with parallelization)

**Solution**:
```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
    # Parallel schema processing
    data_structures = self._get_data_structures_filtered()

    max_workers = self.config.max_parallel_workers  # Config: default 5

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(self._process_data_structure_to_list, ds)
            for ds in data_structures
        ]

        for future in as_completed(futures):
            workunits = future.result()
            for wu in workunits:
                yield wu

def _process_data_structure_to_list(self, ds: DataStructure) -> List[MetadataWorkUnit]:
    """Thread-safe version that returns list instead of yielding."""
    workunits = []
    for wu in self._process_data_structure(ds):
        workunits.append(wu)
    return workunits
```

**Challenges**:
- ⚠️ Generator-based design makes parallelization complex
- ⚠️ Need thread-safe reporting
- ⚠️ Memory usage increases (all workunits in memory)

**Recommendation**:
- ✅ Implement for **deployment fetching** (easy win)
- ⚠️ Consider for schema processing (requires refactoring)
- ✅ Make parallelization **configurable** (some users prefer sequential for debugging)

---

## ⚠️ MODERATE CONCERNS

### 5. Column Lineage Can Generate Huge Aspects

**Location**: `snowplow.py:2462-2546`

**Problem**:
```python
def _emit_column_lineage(self, ...):
    # Line 2509-2512: Create URNs for ALL fields
    iglu_field_urns = [
        make_schema_field_urn(dataset_urn, field.fieldPath)
        for field in schema_metadata.fields  # ← Could be 100+ fields
    ]

    # Line 2520-2525: Single lineage with ALL fields
    fine_grained_lineage = FineGrainedLineageClass(
        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        upstreams=iglu_field_urns,  # ← All fields
        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
        downstreams=[snowflake_field_urn],
    )
```

**Impact**:
- **100 schemas × 50 fields each** = **5000 fine-grained lineages**
- Each lineage: ~500 bytes
- **Total**: ~2.5 MB of lineage data
- Not critical, but worth monitoring

**When it becomes a problem**:
- Schemas with **500+ fields** (rare but possible)
- **Deep nested structures** (array of objects with many properties)

**Recommendation**:
- ✅ Add config: `max_fields_per_lineage: int = 200`
- ✅ Log warning if field count exceeds threshold
- ⏸️ No immediate action needed (monitor in production)

---

### 6. Sequential API Calls in Client

**Location**: `snowplow_client.py:303-341`

**Current implementation**:
```python
while True:  # Pagination loop
    response_data = self._request("GET", endpoint, params=params)
    # Process page...
    offset += len(page_structures)
```

**Good**: Already implements pagination ✅
**Problem**: Fetches pages sequentially

**Optimization opportunity**:
```python
# Parallel page fetching (if API supports concurrent requests)
def get_data_structures_parallel(self, page_size=100):
    # 1. Fetch first page to get total count
    first_page = self._fetch_page(0, page_size)

    # 2. Calculate total pages
    total_pages = (len(first_page) // page_size) + 1

    # 3. Fetch remaining pages in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(self._fetch_page, i * page_size, page_size)
            for i in range(1, total_pages)
        ]
        all_results = [first_page]
        for future in as_completed(futures):
            all_results.extend(future.result())

    return all_results
```

**Recommendation**:
- ⏸️ **Low priority** (pagination already working)
- ✅ Implement only if API rate limits allow
- ⚠️ Risk of rate limiting (needs careful throttling)

---

## 📊 Scaling Estimates

### Current Performance (1000 schemas scenario)

| Operation | Time (Sequential) | API Calls |
|-----------|------------------|-----------|
| Fetch schemas | 30s | 10 (paginated) |
| Fetch deployments | 500s (8.3 min) | 1000 |
| Process schemas | 600s (10 min) | 0 |
| Fetch enrichments | 20s | 50 |
| Build enrichment lineage | 10s | 0 |
| **TOTAL** | **~19 minutes** | **~1060** |

### Optimized Performance (with recommendations)

| Operation | Time (Optimized) | API Calls | Improvement |
|-----------|-----------------|-----------|-------------|
| Fetch schemas | 30s | 10 | - |
| Fetch deployments **(parallel)** | 50s | 1000 | **10x faster** |
| Process schemas **(cached)** | 400s (6.7 min) | 0 | **1.5x faster** |
| Fetch enrichments | 20s | 50 | - |
| Build enrichment lineage **(sampled)** | 5s | 0 | **2x faster** |
| **TOTAL** | **~8.5 minutes** | **~1060** | **2.2x faster** |

**Memory usage**:
- Current: ~100 MB (streaming workunits)
- With caching: ~150 MB (cached structures)
- With parallel: ~200 MB (workunit buffering)

---

## 🎯 Recommended Implementation Priority

### Phase 1: Quick Wins (Immediate - 1-2 days)

1. ✅ **Add deployment fetching parallelization** (10x speedup)
   - File: `snowplow.py:598-619`
   - Impact: High
   - Risk: Low
   - Config: `max_concurrent_deployment_fetches: int = 10`

2. ✅ **Add data structure caching** (3x reduction in API calls)
   - File: `snowplow.py:571-694`
   - Impact: Medium
   - Risk: Very low

3. ✅ **Add enrichment input sampling** (prevents huge aspects)
   - File: `snowplow.py:2221-2372`
   - Impact: Critical for large orgs
   - Risk: Low (configurable)
   - Config: `max_enrichment_inputs: int = 50`

### Phase 2: Performance Improvements (Medium-term - 1 week)

4. ⏸️ **Refactor for parallel schema processing** (2-3x speedup)
   - File: `snowplow.py:696-706`
   - Impact: High
   - Risk: Medium (requires refactoring)
   - Complexity: High

5. ⏸️ **Add field lineage limits** (prevent huge lineage aspects)
   - File: `snowplow.py:2462-2546`
   - Impact: Medium
   - Risk: Low
   - Config: `max_fields_per_lineage: int = 200`

### Phase 3: Advanced Optimizations (Long-term - 2-4 weeks)

6. ⏸️ **Request batch deployment API from Snowplow**
   - Requires: Snowplow team coordination
   - Impact: Very high (eliminates N+1)
   - Risk: External dependency

7. ⏸️ **Implement connection pooling**
   - File: `snowplow_client.py`
   - Impact: Medium
   - Risk: Low

---

## 🔧 Configuration Recommendations

Add these to `SnowplowSourceConfig`:

```python
class PerformanceConfig(BaseModel):
    """Performance and scaling configuration."""

    # API concurrency
    max_concurrent_api_calls: int = Field(
        default=10,
        description="Maximum concurrent API calls for deployment fetching"
    )

    # Enrichment lineage
    max_enrichment_inputs: int = Field(
        default=50,
        description="Maximum input schemas per enrichment (prevents huge aspects)"
    )
    sample_enrichment_inputs: bool = Field(
        default=True,
        description="Sample event schemas for enrichments instead of linking all"
    )

    # Column lineage
    max_fields_per_lineage: int = Field(
        default=200,
        description="Maximum fields per fine-grained lineage"
    )

    # Processing
    enable_parallel_processing: bool = Field(
        default=True,
        description="Enable parallel processing (disable for debugging)"
    )
    max_parallel_workers: int = Field(
        default=5,
        description="Maximum parallel workers for schema processing"
    )

    # Caching
    enable_caching: bool = Field(
        default=True,
        description="Cache data structures and URNs"
    )
```

---

## 🧪 Testing Recommendations

### Load Testing Scenarios

1. **Small org**: 50 schemas, 5 enrichments
   - Expected time: <2 minutes
   - Memory: <100 MB

2. **Medium org**: 500 schemas, 25 enrichments
   - Expected time: <10 minutes
   - Memory: <250 MB

3. **Large org**: 2000 schemas, 100 enrichments
   - Expected time: <30 minutes
   - Memory: <500 MB

### Metrics to Track

```python
# Add to SnowplowSourceReport
class SnowplowSourceReport(SourceReport):
    # Performance metrics
    api_call_count: int = 0
    api_total_time_seconds: float = 0.0
    parallel_fetch_count: int = 0
    cache_hit_count: int = 0
    cache_miss_count: int = 0

    # Aspect size tracking
    max_aspect_size_bytes: int = 0
    aspects_over_1mb: int = 0
    aspects_over_10mb: int = 0
```

---

## 🚨 Known Limitations

### DataHub Platform Limits

1. **Aspect size limit**: ~100 MB per aspect (soft limit)
2. **Fine-grained lineage**: Becomes slow with >1000 edges
3. **UI performance**: Entity pages slow with >10,000 lineage edges

### Snowplow API Limits

1. **Rate limiting**: Unknown (not documented)
2. **No batch APIs**: Must fetch individually
3. **Pagination only**: No concurrent page fetching documented

---

## 📝 Summary

**Critical actions**:
1. ✅ Implement parallel deployment fetching (Phase 1.1)
2. ✅ Add data structure caching (Phase 1.2)
3. ✅ Add enrichment input sampling (Phase 1.3)

**Expected improvements**:
- ⚡ **2-3x faster** for typical workloads
- ⚡ **5-10x faster** for large organizations with field tracking
- 💾 **Prevents DataHub performance issues** with huge aspects
- 📊 **Better monitoring** with performance metrics

**Risk assessment**: Low - All changes are backwards compatible and configurable.
