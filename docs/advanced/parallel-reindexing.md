# Parallel ElasticSearch Reindexing

## Overview

DataHub's parallel reindexing feature optimizes ElasticSearch index rebuild operations during system upgrades by allowing multiple indices to be reindexed concurrently while maintaining cluster stability through adaptive health monitoring and cost-based tier classification. This significantly reduces upgrade downtime for large deployments.

**Key Benefits:**

- ⚡ **Faster upgrades** - Reindex multiple indices simultaneously (2-4 concurrent NORMAL tier indices, 1-2 LARGE tier)
- 🛡️ **Intelligent throttling** - Adaptive request rate limiting based on real-time cluster health
- 🎯 **Cost-aware execution** - Classify indices by computational cost, serialize expensive operations
- 🚨 **Safety guarantees** - Document count verification, automatic cleanup, transactional settings management
- 📊 **Real-time monitoring** - Circuit breaker with hysteresis prevents cluster oscillation

## Architecture

### System Overview

The parallel reindex system consists of three major components:

1. **IndexCostEstimator** - Classifies indices into NORMAL/LARGE tiers based on computational cost
2. **CircuitBreakerState** - Tracks cluster health with hysteresis to prevent flapping
3. **ParallelReindexOrchestrator** - Schedules and monitors concurrent reindex tasks with adaptive throttling

### How It Works

**Phase 1: Index Classification**

```
Cost Formula: (documentCount × primaryShards) / dataNodeCount

Example: 1M docs, 5 shards, 3 data nodes
Cost = (1,000,000 × 5) / 3 = 1,666,667

Classification:
- NORMAL tier (cost < threshold): Can run in parallel (2-4 concurrent)
- LARGE tier (cost ≥ threshold): Must run serially (1-2 concurrent)
```

**Phase 2: Tier-Based Execution**

The orchestrator runs tiers sequentially:

1. **LARGE indices execute first** with strict concurrency limits (1-2 at a time)
2. **NORMAL indices execute next** with relaxed limits (2-4 at a time)

```
Queue (99 system indices)
    ↓
LARGE TIER (40-50 indices)     NORMAL TIER (50 indices)
├─ Index 1 (serial)             ├─ Index 1  ─┐
├─ Index 2 (serial) ────────→   ├─ Index 2  ─┤ Parallel (max 4)
├─ Index 3 (serial)             ├─ Index 3  ─┤
                                 └─ Index 4  ─┘
```

**Phase 3: Adaptive Health Monitoring**

Every 10-30 seconds, the circuit breaker evaluates cluster health:

```
Health Check
    ↓
┌─────────────────────────────────┐
│ RED State?                      │
│ - ES status RED, OR             │
│ - Heap ≥ 90%, OR                │
│ - Write rejections ≥ 50%        │
└─────────────────────────────────┘
    │ YES          │ NO
    ↓              ↓
PAUSE             Check YELLOW
submissions
    │              ├─ ES YELLOW, OR
    │              ├─ Heap 75-90%, OR
    │              ├─ Write rejections elevated
    │              │
    │              ├─ YES → YELLOW (throttle)
    │              │        RPS: 500 req/s
    │              │        Refresh: 60s
    │              │
    │              └─ NO → GREEN (full speed)
    │                      RPS: unlimited
    │                      Refresh: disabled (-1)
    │
    └─→ Rethrottle all active tasks
        Update refresh_interval
        Apply new RPS to ES
```

**Phase 4: Dynamic Rethrottling**

When health state changes, ALL active tasks are immediately rethrottled in parallel without restarting:

```
Current State: GREEN → Health degrades → YELLOW

Action:
1. Calculate new RPS: 500 req/s (vs unlimited before)
2. Parallel rethrottle 4 active tasks via ES API:
   POST /_reindex/{task1}/_rethrottle?requests_per_second=500
   POST /_reindex/{task2}/_rethrottle?requests_per_second=500
   ... (all 4 in parallel)
3. Update destination index refresh_interval: 60s
4. Continue monitoring, no task restart needed
```

### Execution Flow

```
┌─────────────────────────────────────────────────────────┐
│          DataHub System Upgrade Triggered               │
└─────────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│ Step 1: Index Classification (IndexCostEstimator)       │
│ - Calculate cost for each of 99 system indices          │
│ - Split into LARGE (serial) and NORMAL (parallel)       │
│ - Separate queues by tier                               │
└─────────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│ Step 2: Execute LARGE Tier Sequentially                 │
│ - Submit up to 2 indices at a time                      │
│ - Monitor health every 30 seconds                       │
│ - Rethrottle active tasks if health changes            │
│ - Pause submissions if cluster enters RED state         │
└─────────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│ Step 3: Execute NORMAL Tier in Parallel                 │
│ - Submit up to 4 indices at a time                      │
│ - Monitor health every 30 seconds                       │
│ - Rethrottle active tasks if health changes            │
│ - Pause submissions if cluster enters RED state         │
└─────────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│ Step 4: Finalization for Each Completed Task           │
│ - Verify document count matches source                  │
│ - Swap alias to new index                               │
│ - Restore settings (replicas, refresh_interval)         │
│ - Delete old temporary index                            │
└─────────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│ Reindex Complete: All 99 indices ready for production   │
└─────────────────────────────────────────────────────────┘
```

### Circuit Breaker: Hysteresis & Stability Windows

The circuit breaker prevents rapid state oscillations by requiring stability before transitioning:

```
Current State: GREEN
Stability required: 30 seconds

Timeline:
t=0s:    Health degrades → YELLOW signal detected
         State remains GREEN (waiting for stability)

t=15s:   Cluster recovers → GREEN signal
         Timer resets (no state change yet)

t=30s:   Cluster still degrading → YELLOW signal
         Stability window: 30s - 15s = 15s (not met)

t=45s:   Cluster continues degrading
         Stability window: 45s - 0s = 45s (MET)
         State transitions: GREEN → YELLOW (finalize)
         Rethrottle all active tasks to YELLOW RPS
```

**Stability Windows (configurable):**

- `yellowStabilitySeconds`: 30s - Wait 30s before transitioning to YELLOW
- `greenStabilitySeconds`: 30s - Wait 30s before transitioning back to GREEN
- `redRecoverySeconds`: 30s - Wait 30s in RED before attempting to return to YELLOW

This prevents flapping when cluster is on the boundary between health states.

### Finalization Process

When a reindex task completes, it enters the finalization phase:

**SUCCESS PATH (Document count matches):**

1. ✅ Verify document count within tolerance (default 0.1%)
2. ✅ Get old index name (current alias destination)
3. ✅ Swap alias to new index
4. ✅ Delete old index (free space)
5. ✅ Restore settings to new index:
   - Enable `refresh_interval` (60s in normal ops)
   - Enable `number_of_replicas` (1+)
   - Restore merge scheduler settings
6. ✅ Return `REINDEXED`

Result: New destination ready for queries/ingestion immediately

**FAILURE PATH (Document count mismatch or exception):**

1. ❌ Document count outside tolerance, OR
2. ❌ Exception during alias swap/cleanup
3. ✅ Get old index name (still the alias destination)
4. ✅ Restore settings to old index:
   - Enable `refresh_interval`
   - Enable `number_of_replicas`
   - Restore merge scheduler settings
5. ✅ Delete temporary index (cleanup)
6. ❌ Return `FAILED_DOC_COUNT_MISMATCH` or error status

Result: Old destination with normal settings ready for retry

**Key Guarantee:** Regardless of success or failure, the active destination index always has normal (non-optimized) settings after finalization.

## Configuration

### Configuration Parameters

All parameters are configured via environment variables or `application.yml` under `elasticSearch.buildIndices`.

#### Core Execution Settings

| Parameter                  | Env Variable                                              | Default | Description                                                 |
| -------------------------- | --------------------------------------------------------- | ------- | ----------------------------------------------------------- |
| `enableParallelReindex`    | `ELASTICSEARCH_BUILD_INDICES_ENABLE_PARALLEL_REINDEX`     | `false` | Enable/disable parallel reindexing (set `true` to activate) |
| `taskCheckIntervalSeconds` | `ELASTICSEARCH_BUILD_INDICES_TASK_CHECK_INTERVAL_SECONDS` | `15`    | How often to check task status and cluster health (seconds) |
| `maxReindexHours`          | `ELASTICSEARCH_INDEX_BUILDER_MAX_REINDEX_HOURS`           | `12`    | Maximum total reindex time before timeout (hours)           |

#### Index Cost & Tier Classification

| Parameter                    | Env Variable                                               | Default   | Description                                                                  |
| ---------------------------- | ---------------------------------------------------------- | --------- | ---------------------------------------------------------------------------- |
| `normalIndexCostThreshold`   | `ELASTICSEARCH_NORMAL_INDEX_COST_THRESHOLD`                | `500,000` | Cost threshold for NORMAL vs LARGE tier (formula: docCount × shards / nodes) |
| `maxConcurrentNormalReindex` | `ELASTICSEARCH_INDEX_BUILDER_MAX_REINDEX_HOURS`            | `4`       | Max concurrent reindex operations for NORMAL tier indices                    |
| `maxConcurrentLargeReindex`  | `ELASTICSEARCH_BUILD_INDICES_MAX_CONCURRENT_LARGE_REINDEX` | `2`       | Max concurrent reindex operations for LARGE tier indices                     |

**Tier Classification Examples:**

```
Scenario: 3-node cluster, 500K documents, 2 primary shards

Cost = (500,000 × 2) / 3 = 333,333
Classification: NORMAL tier (< 500K threshold)
Execution: Can run up to 4 concurrent with other NORMAL indices

---

Scenario: 3-node cluster, 1.5M documents, 5 primary shards

Cost = (1,500,000 × 5) / 3 = 2,500,000
Classification: LARGE tier (≥ 500K threshold)
Execution: Runs serially (max 1-2 concurrent)
```

#### Cluster Health Monitoring

| Parameter                           | Env Variable                                                        | Default | Description                                 |
| ----------------------------------- | ------------------------------------------------------------------- | ------- | ------------------------------------------- |
| `clusterHealthCheckIntervalSeconds` | `ELASTICSEARCH_BUILD_INDICES_CLUSTER_HEALTH_CHECK_INTERVAL_SECONDS` | `30`    | How often to check cluster health (seconds) |
| `clusterHeapThresholdPercent`       | `ELASTICSEARCH_BUILD_INDICES_CLUSTER_HEAP_THRESHOLD_PERCENT`        | `90`    | Heap usage threshold for RED state          |
| `clusterHeapYellowThresholdPercent` | `ELASTICSEARCH_BUILD_INDICES_CLUSTER_HEAP_YELLOW_THRESHOLD_PERCENT` | `75`    | Heap usage threshold for YELLOW state       |
| `writeRejectionRedThreshold`        | `ELASTICSEARCH_BUILD_INDICES_WRITE_REJECTION_RED_THRESHOLD`         | `50`    | Write rejection % threshold for RED state   |

**Health State Thresholds:**

```
RED State triggered by:
- ES cluster status RED, OR
- Heap usage ≥ 90%, OR
- Write rejections ≥ 50%

Action: Pause new task submissions, rethrottle to 100 req/s, refresh: 30s

YELLOW State triggered by:
- ES cluster status YELLOW (with unassigned replicas), OR
- Heap usage 75-90%, OR
- Write rejections elevated (< 50%)

Action: Continue submissions with standard rate, rethrottle to 500 req/s, refresh: 60s

GREEN State:
- ES cluster status GREEN, AND
- Heap usage < 75%, AND
- Write rejections normal

Action: Full speed, unlimited req/s, refresh: disabled (-1)
```

#### Adaptive Throttling

| Parameter                        | Env Variable                                                     | Default | Description                                   |
| -------------------------------- | ---------------------------------------------------------------- | ------- | --------------------------------------------- |
| `normalTierRequestsPerSecond`    | `ELASTICSEARCH_BUILD_INDICES_NORMAL_TIER_REQUESTS_PER_SECOND`    | `500`   | Request rate limit during YELLOW health state |
| `throttledTierRequestsPerSecond` | `ELASTICSEARCH_BUILD_INDICES_THROTTLED_TIER_REQUESTS_PER_SECOND` | `100`   | Request rate limit during RED health state    |
| `normalTierRefreshInterval`      | `ELASTICSEARCH_BUILD_INDICES_NORMAL_TIER_REFRESH_INTERVAL`       | `60s`   | Refresh interval during YELLOW state          |
| `throttledTierRefreshInterval`   | `ELASTICSEARCH_BUILD_INDICES_THROTTLED_TIER_REFRESH_INTERVAL`    | `30s`   | Refresh interval during RED state             |
| `rethrottleExecutorPoolSize`     | `ELASTICSEARCH_BUILD_INDICES_RETHROTTLE_EXECUTOR_POOL_SIZE`      | `8`     | Max parallel rethrottle operations            |

**RPS and Refresh Behavior:**

```
GREEN state (healthy cluster):
- RPS: -1 (unlimited) - submit requests as fast as possible
- Refresh: -1 (disabled) - maximize throughput, no memory overhead
- Usage: Normal operation with high load tolerance

YELLOW state (elevated load):
- RPS: 500 req/s - moderate throttling
- Refresh: 60s - periodic segment flushes reduce memory
- Usage: Some cluster pressure, balance throughput/stability

RED state (critical):
- RPS: 100 req/s - aggressive throttling
- Refresh: 30s - aggressive flushes for immediate heap relief
- Usage: Cluster near limits, pause submissions, stabilize
```

#### Stability Windows (Circuit Breaker Hysteresis)

| Parameter                | Env Variable                                           | Default | Description                                               |
| ------------------------ | ------------------------------------------------------ | ------- | --------------------------------------------------------- |
| `yellowStabilitySeconds` | `ELASTICSEARCH_BUILD_INDICES_YELLOW_STABILITY_SECONDS` | `30`    | Seconds cluster must be in YELLOW before state transition |
| `greenStabilitySeconds`  | `ELASTICSEARCH_BUILD_INDICES_GREEN_STABILITY_SECONDS`  | `30`    | Seconds cluster must be healthy before returning to GREEN |
| `redRecoverySeconds`     | `ELASTICSEARCH_BUILD_INDICES_RED_RECOVERY_SECONDS`     | `30`    | Seconds in RED before attempting recovery to YELLOW       |

#### Document Validation

| Parameter                        | Env Variable                                                      | Default | Description                                        |
| -------------------------------- | ----------------------------------------------------------------- | ------- | -------------------------------------------------- |
| `docCountValidationRetryCount`   | `ELASTICSEARCH_BUILD_INDICES_DOC_COUNT_VALIDATION_RETRY_COUNT`    | `10`    | Retries for document count validation post-reindex |
| `docCountValidationRetrySleepMs` | `ELASTICSEARCH_BUILD_INDICES_DOC_COUNT_VALIDATION_RETRY_SLEEP_MS` | `2000`  | Sleep between validation retries (ms)              |

#### Other Configuration

| Parameter                     | Env Variable                                                 | Default | Description                                                |
| ----------------------------- | ------------------------------------------------------------ | ------- | ---------------------------------------------------------- |
| `maxConcurrentFinalizations`  | `ELASTICSEARCH_BUILD_INDICES_MAX_CONCURRENT_FINALIZATIONS`   | `5`     | Max concurrent alias swap/finalization operations          |
| `replicaSyncTimeoutMinutes`   | `ELASTICSEARCH_BUILD_INDICES_REPLICA_SYNC_TIMEOUT_MINUTES`   | `1`     | Max time to wait for replica sync before promoting primary |
| `minimumReplicasForPromotion` | `ELASTICSEARCH_BUILD_INDICES_MINIMUM_REPLICAS_FOR_PROMOTION` | `1`     | Minimum replica count required before index promotion      |

### YAML Configuration Example

```yaml
elasticSearch:
  buildIndices:
    # Core execution
    enableParallelReindex: ${ELASTICSEARCH_BUILD_INDICES_ENABLE_PARALLEL_REINDEX:false}
    taskCheckIntervalSeconds: ${ELASTICSEARCH_BUILD_INDICES_TASK_CHECK_INTERVAL_SECONDS:15}
    maxReindexHours: ${ELASTICSEARCH_INDEX_BUILDER_MAX_REINDEX_HOURS:12}

    # Index tier classification
    normalIndexCostThreshold: ${ELASTICSEARCH_NORMAL_INDEX_COST_THRESHOLD:500000}
    maxConcurrentNormalReindex: ${ELASTICSEARCH_INDEX_BUILDER_MAX_REINDEX_HOURS:4}
    maxConcurrentLargeReindex: ${ELASTICSEARCH_BUILD_INDICES_MAX_CONCURRENT_LARGE_REINDEX:2}

    # Cluster health monitoring
    clusterHealthCheckIntervalSeconds: ${ELASTICSEARCH_BUILD_INDICES_CLUSTER_HEALTH_CHECK_INTERVAL_SECONDS:30}
    clusterHeapThresholdPercent: ${ELASTICSEARCH_BUILD_INDICES_CLUSTER_HEAP_THRESHOLD_PERCENT:90}
    clusterHeapYellowThresholdPercent: ${ELASTICSEARCH_BUILD_INDICES_CLUSTER_HEAP_YELLOW_THRESHOLD_PERCENT:75}
    writeRejectionRedThreshold: ${ELASTICSEARCH_BUILD_INDICES_WRITE_REJECTION_RED_THRESHOLD:50}

    # Adaptive throttling
    normalTierRequestsPerSecond: ${ELASTICSEARCH_BUILD_INDICES_NORMAL_TIER_REQUESTS_PER_SECOND:500}
    throttledTierRequestsPerSecond: ${ELASTICSEARCH_BUILD_INDICES_THROTTLED_TIER_REQUESTS_PER_SECOND:100}
    normalTierRefreshInterval: ${ELASTICSEARCH_BUILD_INDICES_NORMAL_TIER_REFRESH_INTERVAL:60s}
    throttledTierRefreshInterval: ${ELASTICSEARCH_BUILD_INDICES_THROTTLED_TIER_REFRESH_INTERVAL:30s}
    rethrottleExecutorPoolSize: ${ELASTICSEARCH_BUILD_INDICES_RETHROTTLE_EXECUTOR_POOL_SIZE:8}

    # Circuit breaker stability
    yellowStabilitySeconds: ${ELASTICSEARCH_BUILD_INDICES_YELLOW_STABILITY_SECONDS:30}
    greenStabilitySeconds: ${ELASTICSEARCH_BUILD_INDICES_GREEN_STABILITY_SECONDS:30}
    redRecoverySeconds: ${ELASTICSEARCH_BUILD_INDICES_RED_RECOVERY_SECONDS:30}

    # Document validation
    docCountValidationRetryCount: ${ELASTICSEARCH_BUILD_INDICES_DOC_COUNT_VALIDATION_RETRY_COUNT:10}
    docCountValidationRetrySleepMs: ${ELASTICSEARCH_BUILD_INDICES_DOC_COUNT_VALIDATION_RETRY_SLEEP_MS:2000}

    # Other
    maxConcurrentFinalizations: ${ELASTICSEARCH_BUILD_INDICES_MAX_CONCURRENT_FINALIZATIONS:5}
    replicaSyncTimeoutMinutes: ${ELASTICSEARCH_BUILD_INDICES_REPLICA_SYNC_TIMEOUT_MINUTES:1}
    minimumReplicasForPromotion: ${ELASTICSEARCH_BUILD_INDICES_MINIMUM_REPLICAS_FOR_PROMOTION:1}
```

### Environment Variables Example

```bash
# Enable parallel reindex
ELASTICSEARCH_BUILD_INDICES_ENABLE_PARALLEL_REINDEX=true

# Core execution settings
ELASTICSEARCH_BUILD_INDICES_TASK_CHECK_INTERVAL_SECONDS=15
ELASTICSEARCH_INDEX_BUILDER_MAX_REINDEX_HOURS=12

# Tier classification
ELASTICSEARCH_NORMAL_INDEX_COST_THRESHOLD=500000
ELASTICSEARCH_INDEX_BUILDER_MAX_REINDEX_HOURS=4
ELASTICSEARCH_BUILD_INDICES_MAX_CONCURRENT_LARGE_REINDEX=2

# Cluster health
ELASTICSEARCH_BUILD_INDICES_CLUSTER_HEALTH_CHECK_INTERVAL_SECONDS=30
ELASTICSEARCH_BUILD_INDICES_CLUSTER_HEAP_THRESHOLD_PERCENT=90
ELASTICSEARCH_BUILD_INDICES_CLUSTER_HEAP_YELLOW_THRESHOLD_PERCENT=75
ELASTICSEARCH_BUILD_INDICES_WRITE_REJECTION_RED_THRESHOLD=50

# Adaptive throttling
ELASTICSEARCH_BUILD_INDICES_NORMAL_TIER_REQUESTS_PER_SECOND=500
ELASTICSEARCH_BUILD_INDICES_THROTTLED_TIER_REQUESTS_PER_SECOND=100
ELASTICSEARCH_BUILD_INDICES_NORMAL_TIER_REFRESH_INTERVAL=60s
ELASTICSEARCH_BUILD_INDICES_THROTTLED_TIER_REFRESH_INTERVAL=30s

# Circuit breaker
ELASTICSEARCH_BUILD_INDICES_YELLOW_STABILITY_SECONDS=30
ELASTICSEARCH_BUILD_INDICES_GREEN_STABILITY_SECONDS=30
ELASTICSEARCH_BUILD_INDICES_RED_RECOVERY_SECONDS=30
```

### Tuning Guidelines

#### Single-Node Cluster (Testing/Development)

```bash
# Conservative concurrency
ELASTICSEARCH_INDEX_BUILDER_MAX_REINDEX_HOURS=1
ELASTICSEARCH_BUILD_INDICES_MAX_CONCURRENT_LARGE_REINDEX=1

# Strict health thresholds
ELASTICSEARCH_BUILD_INDICES_CLUSTER_HEAP_THRESHOLD_PERCENT=75
ELASTICSEARCH_BUILD_INDICES_CLUSTER_HEAP_YELLOW_THRESHOLD_PERCENT=60

# Fast health checks
ELASTICSEARCH_BUILD_INDICES_CLUSTER_HEALTH_CHECK_INTERVAL_SECONDS=10
ELASTICSEARCH_BUILD_INDICES_TASK_CHECK_INTERVAL_SECONDS=10

# Short timeout
ELASTICSEARCH_INDEX_BUILDER_MAX_REINDEX_HOURS=4

# Slower throttling for single-node
ELASTICSEARCH_BUILD_INDICES_NORMAL_TIER_REQUESTS_PER_SECOND=200
ELASTICSEARCH_BUILD_INDICES_THROTTLED_TIER_REQUESTS_PER_SECOND=50
```

**Why:** Single-node clusters have minimal resources. Strict health thresholds and low concurrency prevent circuit breaker trips and OOM errors.

#### Small Production Cluster (3-5 nodes)

```bash
# Balanced concurrency
ELASTICSEARCH_INDEX_BUILDER_MAX_REINDEX_HOURS=2
ELASTICSEARCH_BUILD_INDICES_MAX_CONCURRENT_LARGE_REINDEX=1

# Moderate health thresholds
ELASTICSEARCH_BUILD_INDICES_CLUSTER_HEAP_THRESHOLD_PERCENT=85
ELASTICSEARCH_BUILD_INDICES_CLUSTER_HEAP_YELLOW_THRESHOLD_PERCENT=70

# Standard health checks
ELASTICSEARCH_BUILD_INDICES_CLUSTER_HEALTH_CHECK_INTERVAL_SECONDS=30
ELASTICSEARCH_BUILD_INDICES_TASK_CHECK_INTERVAL_SECONDS=30

# Standard timeout
ELASTICSEARCH_INDEX_BUILDER_MAX_REINDEX_HOURS=12
```

**Why:** Small clusters need balanced parallelism. 2 concurrent NORMAL indices with moderate health thresholds prevents resource saturation while still gaining reindex speedup.

#### Large Production Cluster (10+ nodes, 100GB+ data)

```bash
# Aggressive concurrency
ELASTICSEARCH_INDEX_BUILDER_MAX_REINDEX_HOURS=4
ELASTICSEARCH_BUILD_INDICES_MAX_CONCURRENT_LARGE_REINDEX=2

# Relaxed health thresholds
ELASTICSEARCH_BUILD_INDICES_CLUSTER_HEAP_THRESHOLD_PERCENT=90
ELASTICSEARCH_BUILD_INDICES_CLUSTER_HEAP_YELLOW_THRESHOLD_PERCENT=80

# Standard health checks (less frequent)
ELASTICSEARCH_BUILD_INDICES_CLUSTER_HEALTH_CHECK_INTERVAL_SECONDS=60
ELASTICSEARCH_BUILD_INDICES_TASK_CHECK_INTERVAL_SECONDS=60

# Long timeout for large datasets
ELASTICSEARCH_INDEX_BUILDER_MAX_REINDEX_HOURS=24

# Higher throttling for stable clusters
ELASTICSEARCH_BUILD_INDICES_NORMAL_TIER_REQUESTS_PER_SECOND=1000
ELASTICSEARCH_BUILD_INDICES_THROTTLED_TIER_REQUESTS_PER_SECOND=200
```

**Why:** Large clusters benefit from high parallelism. Relaxed health thresholds account for legitimate background activity. Higher RPS limits take advantage of cluster capacity.

### Disabling Parallel Mode

To fall back to sequential reindexing:

```bash
ELASTICSEARCH_BUILD_INDICES_ENABLE_PARALLEL_REINDEX=false
```

**When to disable:**

- Single-node development environments
- Troubleshooting reindex failures in isolation
- ES cluster experiencing instability
- Validating a problematic upgrade with sequential execution

## Performance Benchmarks

### Example: 50 System Indices, 20M Documents Total

| Configuration                            | Mode       | Time    | Improvement    |
| ---------------------------------------- | ---------- | ------- | -------------- |
| Default (4 NORMAL, 2 LARGE)              | Parallel   | 27m 29s | **baseline**   |
| Disabled (sequential)                    | Sequential | 52m 17s | 90% slower     |
| Aggressive (4 NORMAL, 2 LARGE, 1000 RPS) | Parallel   | 22m 15s | **19% faster** |

_Note: Actual performance depends heavily on cluster capacity, hardware, data complexity, and concurrent load._

### Factors Affecting Performance

- **Index size** - Larger indices take longer but benefit more from parallelism
- **Cluster capacity** - More ES nodes = higher safe concurrency and faster per-index reindex
- **Document complexity** - Complex mappings and custom analyzers slow reindex operations
- **Concurrent workload** - Other active queries/ingestion compete for resources; circuit breaker will throttle
- **Network bandwidth** - Large bulk operations can saturate network; RPS limits provide flow control
- **Disk I/O** - Reindex involves heavy disk reads; SSD clusters perform significantly better

## Monitoring & Troubleshooting

### Key Metrics to Watch

**Cluster-Level Metrics:**

- **Heap usage** - Should stay below 85% (YELLOW threshold 75%)
- **Write rejections** - % of writes rejected by ES (RED threshold 50%)
- **Node count** - Affects cost tier classification
- **Cluster status** - RED triggers immediate pause, YELLOW throttles

**Per-Task Metrics:**

- **Task duration** - How long each reindex takes (compare to baseline)
- **Documents reindexed** - Should match source count (within tolerance)
- **Request rate** - Should match applied RPS limit

**Reindex Orchestrator Metrics:**

- **Active task count** - Should not exceed configured concurrency
- **Health state transitions** - Indicates cluster is hitting resource limits
- **Rethrottle frequency** - How often health changes trigger rethrottling

### Log Messages

**Normal operation:**

```
INFO  ParallelReindexOrchestrator - Starting parallel reindex for 99 indices
INFO  ParallelReindexOrchestrator - Cost classification: 50 LARGE tier, 49 NORMAL tier
INFO  ParallelReindexOrchestrator - Executing LARGE tier indices with max concurrency 2
INFO  ParallelReindexOrchestrator - Submitted reindex for datasetindex_v2 (task: abc123) - 1/2 active
INFO  ParallelReindexOrchestrator - Monitoring progress: 2 active, 48 pending, 0 completed, 0 failed
INFO  CircuitBreakerState - Health check: GREEN (heap=62%, rejections=0%)
INFO  ParallelReindexOrchestrator - Reindex completed for datasetindex_v2 (1.2M docs, result: REINDEXED)
INFO  ParallelReindexOrchestrator - Executing NORMAL tier indices with max concurrency 4
INFO  ParallelReindexOrchestrator - Parallel reindex complete: 50 succeeded, 0 failed
```

**Health state changes:**

```
WARN  CircuitBreakerState - Health degraded: GREEN → YELLOW (heap=78%, rejections=5%)
INFO  ParallelReindexOrchestrator - Rethrottling 4 active tasks from unlimited to 500 req/s
INFO  ParallelReindexOrchestrator - Updated refresh_interval to 60s on 4 destination indices
WARN  CircuitBreakerState - Health critical: YELLOW → RED (heap=92%, rejections=48%)
INFO  ParallelReindexOrchestrator - Pausing submissions (cluster in RED state)
INFO  ParallelReindexOrchestrator - Rethrottling 3 active tasks from 500 to 100 req/s
INFO  CircuitBreakerState - Health recovered: RED → YELLOW (heap=81%, rejections=8%)
```

**Failures:**

```
ERROR ParallelReindexOrchestrator - Document count mismatch for chartindex_v2: expected=500000, actual=499800, diff=200
ERROR ParallelReindexOrchestrator - Reindex timeout! 2 tasks still active after 12 hours
WARN  ParallelReindexOrchestrator - Failed to cleanup temp index datasetindex_v2_1234567890
WARN  HealthCheckPoller - Cluster health check failed, circuit breaker defaulting to RED (safe mode)
```

### Common Issues & Solutions

#### Issue: Reindex tasks timing out

**Symptoms:**

```
ERROR Reindex timeout! 3 tasks still active after 12 hours
```

**Diagnosis:**

1. Check if cluster is in RED state frequently (should be brief)
2. Check individual task RPS - if < 100 docs/s, task is extremely slow
3. Check ES disk I/O and memory usage

**Solutions:**

- Increase `maxReindexHours` for very large datasets (100M+ docs)
- Reduce `maxConcurrentNormalReindex` and `maxConcurrentLargeReindex` to give each task more resources
- Add more ES nodes to improve cost tier classification
- Check ES cluster health: `GET /_cluster/health`

#### Issue: Frequent health state oscillation

**Symptoms:**

```
WARN  CircuitBreakerState - Health degraded: GREEN → YELLOW
... (5 seconds later)
WARN  CircuitBreakerState - Health recovered: YELLOW → GREEN
... (repeats every few seconds)
```

**Diagnosis:**
Cluster is hovering right around a threshold (e.g., heap at 75%).

**Solutions:**

- Increase stability windows: `ELASTICSEARCH_BUILD_INDICES_YELLOW_STABILITY_SECONDS=60`
- Reduce concurrency to lower resource usage
- Add more memory/nodes to move away from the threshold

#### Issue: Document count mismatches

**Symptoms:**

```
ERROR Document count mismatch for datasetindex_v2: expected=1000000, actual=999800
ERROR Reindex failed: FAILED_DOC_COUNT_MISMATCH
```

**Understanding Tolerance:**

- Default tolerance: 0.1% of source document count
- Example: 1M docs → allows ±1,000 docs variance (999K-1.001M OK)
- Tolerance prevents false failures from timing edge cases

**Solutions:**

1. **Verify no concurrent writes** - Stop ingestion during upgrade (recommended)
2. **Check ES cluster stability** - Look for node failures or GC pauses during reindex
3. **Retry the reindex** - Transient network issues often resolve on retry
4. **Increase tolerance only if necessary** - Use `ELASTICSEARCH_BUILD_INDICES_DOC_COUNT_TOLERANCE_PERCENT=0.5` only for known concurrent write scenarios

#### Issue: ES circuit breaker trips

**Symptoms:**

```
ERROR Failed to submit reindex for chartindex_v2: CircuitBreakingException: [parent] Data too large
```

**Solutions:**

- Reduce `maxConcurrentNormalReindex` (start with 2, increase gradually)
- Increase ES JVM heap size if cluster has available memory
- Check ES field data cache: `GET /_stats/fielddata`
- Increase stability window so cluster has time to recover: `ELASTICSEARCH_BUILD_INDICES_YELLOW_STABILITY_SECONDS=60`

#### Issue: Write rejections causing RED state

**Symptoms:**

```
WARN  CircuitBreakerState - Health degraded to RED (write rejections: 52% > 50% threshold)
INFO  ParallelReindexOrchestrator - Pausing submissions
```

**Solutions:**

- Reduce concurrency limits to decrease ES load
- Increase `ELASTICSEARCH_BUILD_INDICES_WRITE_REJECTION_RED_THRESHOLD` (e.g., 70%) if rejections are expected
- Check for long-running queries blocking writes
- Monitor ES thread pool queue sizes: `GET /_nodes/stats/thread_pool`

#### Issue: Temp index cleanup failures

**Symptoms:**

```
WARN Failed to cleanup temp index datasetindex_v2_1234567890 after retries
```

**Solutions:**

- Temp indices are named like `{index}_v2_{timestamp}`
- Manual cleanup: `DELETE /datasetindex_v2_1234567890`
- Check ES cluster health before deleting
- Cleanup is automatically retried with exponential backoff

### ElasticSearch Queries for Monitoring

```bash
# Check active reindex tasks with progress
GET /_tasks?actions=*reindex&detailed=true

# Check cluster health
GET /_cluster/health

# Check node stats (heap, GC, rejections)
GET /_nodes/stats

# Check specific index stats
GET /datasetindex_v2/_stats

# Check write rejection rates
GET /_nodes/stats/indices/indexing

# Monitor circuit breaker limits
GET /_nodes/stats/breaker
```

## System Indices (99 Total)

DataHub maintains 99 system-managed indices across multiple index services:

- **EntitySearchService** (~62 indices) - Full-text search indices for entities (datasetindex_v2, chartindex_v2, etc.)
- **GraphService** (1 index) - Relationship graph: graph_service_v1
- **SystemMetadataService** (1 index) - Aspect metadata: system_metadata_service_v1
- **TimeseriesAspectService** (~35 indices) - Timeseries data (dataset usage, assertions, profiles, etc.)

All 99 indices are reindexed as part of the system upgrade process.

## Deployment Considerations

### Upgrade Context

Parallel reindexing is designed specifically for **controlled system upgrade scenarios** where:

- Reindex runs in a maintenance window with a dedicated upgrade pod/job
- Typically one upgrade pod running at a time (not multi-pod runtime)
- DataHub is offline or in read-only mode during the upgrade

**Important:** The implementation uses JVM-local locks (not distributed locks). This is safe for single-pod upgrade jobs but **NOT recommended for multi-pod runtime scenarios**.

### Production Rollout

**Recommended approach:**

1. **Test in dev/staging first** - Validate with your data volume and cluster size
2. **Start conservative** - Use defaults (enableParallelReindex=false) for first few upgrades
3. **Enable on non-critical environment** - Enable `enableParallelReindex=true` in staging
4. **Monitor closely** - Watch ES metrics (heap, rejections, GC) during upgrade
5. **Measure improvement** - Compare upgrade time before/after enabling
6. **Tune gradually** - Increase concurrency limits based on cluster stability
7. **Document your config** - Record what works for your deployment

### Rollback Procedure

If parallel reindexing causes issues:

1. **Mid-upgrade:** Press Ctrl+C to interrupt

   - In-flight tasks are cancelled via ES task cancellation API
   - Temporary indices are cleaned up automatically
   - Settings are restored to pre-reindex state

2. **Post-upgrade:** If reindex succeeded but caused subsequent issues:

   - Revert to previous DataHub version
   - Set `enableParallelReindex: false` for next upgrade attempt
   - Sequential reindex is slower but safer for troubleshooting

3. **Orphaned indices:** Look for `{index}_v2_{timestamp}` patterns
   - Delete via: `DELETE /{pattern}*`
   - These are safe to delete (not part of active aliases)

## Technical Details

### Implementation Classes

**ParallelReindexOrchestrator**

- Location: `metadata-io/src/main/java/com/linkedin/metadata/search/elasticsearch/indexbuilder/ParallelReindexOrchestrator.java`
- Orchestrates concurrent reindex tasks with health monitoring and rethrottling

**IndexCostEstimator**

- Location: `metadata-io/src/main/java/com/linkedin/metadata/search/elasticsearch/indexbuilder/IndexCostEstimator.java`
- Classifies indices into NORMAL/LARGE tiers based on computational cost

**CircuitBreakerState**

- Location: `metadata-io/src/main/java/com/linkedin/metadata/search/elasticsearch/indexbuilder/CircuitBreakerState.java`
- Tracks cluster health with hysteresis and stability windows

**HealthCheckPoller**

- Location: `metadata-io/src/main/java/com/linkedin/metadata/search/elasticsearch/indexbuilder/HealthCheckPoller.java`
- Polls ES cluster health and determines health state (GREEN/YELLOW/RED)

**BuildIndicesConfiguration**

- Location: `metadata-service/configuration/src/main/java/com/linkedin/metadata/config/search/BuildIndicesConfiguration.java`
- Configuration model with all tunable parameters

### Reindex Result Types

The system tracks reindex outcomes with these result types:

- `REINDEXED` - Successful reindex with alias swap
- `REINDEXED_SKIPPED_0DOCS` - Skipped empty index (0 documents, no reindex needed)
- `FAILED_TIMEOUT` - Task exceeded `maxReindexHours`
- `FAILED_DOC_COUNT_MISMATCH` - Document count outside tolerance after reindex
- `FAILED_SUBMISSION` - Failed to submit reindex task to ES
- `FAILED_SUBMISSION_IO` - I/O error during task submission
- `FAILED_MONITORING_ERROR` - Error during task monitoring/status check

### Document Count Validation

Post-reindex validation ensures data integrity:

- **Tolerance:** Default 0.1% of source document count (minimum 1 document)
- **Example:** 10,000 docs → tolerance of 10 docs → accept 9,990-10,010 docs
- **Retries:** Up to 10 retries with 2-second sleep between attempts
- **Rationale:** Accounts for ES refresh delays and segment merging transients

## FAQ

**Q: Will parallel reindexing speed up my upgrades?**

A: Yes, typically 40-70% faster for large deployments. Actual speedup depends on cluster capacity, index distribution across NORMAL/LARGE tiers, and current cluster load. A 50-index reindex we tested showed ~27 min (parallel) vs ~52 min (sequential).

**Q: What happens if I set concurrency too high?**

A: ES cluster may experience high memory pressure, circuit breaker trips, or temporary slowness. The system includes adaptive throttling that automatically pauses submissions when cluster health degrades. Start conservative (default 2-4 NORMAL) and increase gradually based on monitoring.

**Q: Can I use this for runtime reindexing outside upgrades?**

A: Not recommended. The current implementation uses JVM-local locks suitable for single-pod upgrade jobs. For multi-pod runtime scenarios, you would need distributed locking (not currently implemented).

**Q: What if a reindex task hangs indefinitely?**

A: The orchestrator has a `maxReindexHours` timeout (default 12 hours) that will abort stuck tasks via ES task cancellation API and clean up temporary indices automatically.

**Q: How do I know if my ES cluster can handle more concurrency?**

A: Monitor these metrics during an upgrade:

- Heap usage: Should stay < 85% (YELLOW threshold)
- Write rejections: Should stay < 50% (RED threshold)
- Garbage collection: Should not see long GC pauses (>1s)

If healthy, try increasing `maxConcurrentNormalReindex` by 1-2 for next upgrade. If health degrades, reduce concurrency.

**Q: Does this work with OpenSearch?**

A: Yes! The implementation uses the standard OpenSearch client and is compatible with both ElasticSearch 7.x+ and OpenSearch 1.x+.

**Q: What's the difference between NORMAL and LARGE tier indices?**

A: Cost = (docCount × primaryShards) / dataNodeCount. NORMAL tier indices (cost < 500K) can run multiple in parallel. LARGE tier indices (cost ≥ 500K) run serially to avoid cluster overload. A 50M-document index with 5 shards in a 3-node cluster would be LARGE and reindex serially.

**Q: Can I change cost thresholds mid-upgrade?**

A: You can change `normalIndexCostThreshold` before upgrade, but it will reclassify all indices and could change the execution plan. Recommended: finalize tuning in staging before production upgrade.

## See Also

- [ElasticSearch Reindex API](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-reindex.html)
- [DataHub System Upgrade Guide](../how/updating-datahub.md)
- [Circuit Breaker Pattern](https://martinfowler.com/bliki/CircuitBreaker.html)
