# Monitoring DataHub

## Overview

Monitoring DataHub's system components is essential for maintaining operational excellence, troubleshooting performance issues,
and ensuring system reliability. This comprehensive guide covers how to implement observability in DataHub through tracing and metrics,
and how to extract valuable insights from your running instances.

## Why Monitor DataHub?

Effective monitoring enables you to:

- Identify Performance Bottlenecks: Pinpoint slow queries or API endpoints
- Debug Issues Faster: Trace requests across distributed components to locate failures
- Meet SLAs: Track and alert on key performance indicators

## Observability Components

DataHub's observability strategy consists of two complementary approaches:

1. Metrics Collection

   **Purpose:** Aggregate statistical data about system behavior over time
   **Technology:** Transitioning from DropWizard/JMX to Micrometer

   **Current State:** Micrometer is primary for new metrics (Actuator `/actuator/prometheus`); legacy Dropwizard-style meters may still appear for older dashboards
   **Direction:** Align alerts and Grafana with the [canonical metric taxonomy](#canonical-metric-taxonomy-prometheus); reduce reliance on JMX-only names
   **Naming stability:** Actuator metric names and tags are still **evolving** and are not a compatibility guarantee across releases until called out explicitly in docs; legacy JMX-style series are the main backward-compatibility concern for existing panels.
   **Compatibility:** Prometheus-compatible format with support for other metrics backends

   Key Metrics Categories:

   - Performance Metrics: Request latency, throughput, error rates
   - Resource Metrics: CPU, memory utilization
   - Application Metrics: Cache hit rates, queue depths, processing times
   - Business Metrics: Entity counts, ingestion rates, search performance

2. Distributed Tracing

   **Purpose:** Track individual requests as they flow through multiple services and components
   **Technology:** OpenTelemetry-based instrumentation

   - Provides end-to-end visibility of request lifecycles
   - Automatically instruments popular libraries (Kafka, JDBC, Elasticsearch)
   - Supports multiple backend systems (Jaeger, Zipkin, etc.)
   - Enables custom span creation with minimal code changes

   Key Benefits:

   - Visualize request flow across microservices
   - Identify latency hotspots
   - Understand service dependencies
   - Debug complex distributed transactions

## GraphQL Instrumentation (Micrometer)

### Overview

DataHub provides comprehensive instrumentation for its GraphQL API through Micrometer metrics, enabling detailed performance
monitoring and debugging capabilities. The instrumentation system offers flexible configuration options to balance between
observability depth and performance overhead.

### Why Path-Level GraphQL Instrumentation Matters

Traditional GraphQL monitoring only tells you "the search query is slow" but not **why**. Without path-level instrumentation,
you're blind to which specific fields are causing performance bottlenecks in complex nested queries.

### Real-World Example

Consider this GraphQL query:

```graphql
query getSearchResults {
  search(input: { query: "sales data" }) {
    searchResults {
      entity {
        ... on Dataset {
          name
          owner {
            # Path: /search/searchResults/entity/owner
            corpUser {
              displayName
            }
          }
          lineage {
            # Path: /search/searchResults/entity/lineage
            upstreamCount
            downstreamCount
            upstreamEntities {
              urn
              name
            }
          }
          schemaMetadata {
            # Path: /search/searchResults/entity/schemaMetadata
            fields {
              fieldPath
              description
            }
          }
        }
      }
    }
  }
}
```

### What Path-Level Instrumentation Reveals

With path-level metrics, you discover:

- `/search/searchResults/entity/owner` - 50ms (fast, well-cached)
- `/search/searchResults/entity/lineage` - 2500ms (SLOW! hitting graph database)
- `/search/searchResults/entity/schemaMetadata` - 150ms (acceptable)

**Without path metrics**: "Search query takes 3 seconds"  
**With path metrics**: "Lineage resolution is the bottleneck"

### Key Benefits

#### 1. **Surgical Optimization**

Instead of guessing, you know exactly which resolver needs optimization. Maybe lineage needs better caching or pagination.

#### 2. **Smart Query Patterns**

Identify expensive patterns like:

```yaml
# These paths consistently slow:
/*/lineage/upstreamEntities/*
/*/siblings/*/platform
# Action: Add field-level caching or lazy loading
```

#### 3. **Client-Specific Debugging**

Different clients request different fields. Path instrumentation shows:

- Web UI requests are slow (requesting everything)
- API integrations timeout (requesting deep lineage)

#### 4. **N+1 Query Detection**

Spot resolver patterns that indicate N+1 problems:

```
/users/0/permissions - 10ms
/users/1/permissions - 10ms
/users/2/permissions - 10ms
... (100 more times)
```

### Configuration Strategy

Start targeted to minimize overhead:

```yaml
# Focus on known slow operations
fieldLevelOperations: "searchAcrossEntities,getDataset"

# Target expensive resolver paths
fieldLevelPaths: "/**/lineage/**,/**/relationships/**,/**/privileges"
```

### Architecture

The GraphQL instrumentation is implemented through DataHub’s [`GraphQLTimingInstrumentation`](../../metadata-io/src/main/java/com/linkedin/metadata/system_telemetry/GraphQLTimingInstrumentation.java) (`com.linkedin.metadata.system_telemetry`), which extends GraphQL Java’s `SimplePerformantInstrumentation` and records Micrometer meters—it is **not** a Spring Framework class. It provides:

- **Request-level metrics**: Overall query performance and error tracking
- **Field-level metrics**: Detailed timing for individual field resolvers
- **Smart filtering**: Configurable targeting of specific operations or field paths
- **Low overhead**: Minimal performance impact through efficient instrumentation

### Metrics Collected

#### Request-Level Metrics

**Metric: `graphql.request.duration`**

- **Type**: Timer with percentiles (p50, p95, p99)
- **Tags**:
  - `operation`: Operation name (e.g., "getSearchResultsForMultiple")
  - `operation.type`: Query, mutation, or subscription
  - `success`: true/false based on error presence
  - `field.filtering`: Filtering mode applied (DISABLED, ALL_FIELDS, BY_OPERATION, BY_PATH, BY_BOTH)
- **Use Case**: Monitor overall GraphQL performance, identify slow operations

**Metric: `graphql.request.errors`**

- **Type**: Counter
- **Tags**:
  - `operation`: Operation name
  - `operation.type`: Query, mutation, or subscription
- **Use Case**: Track error rates by operation

#### Field-Level Metrics

**Metric: `graphql.field.duration`**

- **Type**: Timer with percentiles (p50, p95, p99)
- **Tags**:
  - `parent.type`: GraphQL parent type (e.g., "Dataset", "User")
  - `field`: Field name being resolved
  - `operation`: Operation name context
  - `success`: true/false
  - `path`: Field path (optional, controlled by `fieldLevelPathEnabled`)
- **Use Case**: Identify slow field resolvers, optimize data fetching

**Metric: `graphql.field.errors`**

- **Type**: Counter
- **Tags**: Same as field duration (minus success tag)
- **Use Case**: Track field-specific error patterns

**Metric: `graphql.fields.instrumented`**

- **Type**: Counter
- **Tags**:
  - `operation`: Operation name
  - `filtering.mode`: Active filtering mode
- **Use Case**: Monitor instrumentation coverage and overhead

### Configuration Guide

#### Master Controls

```yaml
graphQL:
  metrics:
    # Master switch for all GraphQL metrics
    enabled: ${GRAPHQL_METRICS_ENABLED:true}

    # Enable field-level resolver metrics
    fieldLevelEnabled: ${GRAPHQL_METRICS_FIELD_LEVEL_ENABLED:false}
```

#### Selective Field Instrumentation

Field-level metrics can add significant overhead for complex queries. DataHub provides multiple strategies to control which fields are instrumented:

##### 1. **Operation-Based Filtering**

Target specific GraphQL operations known to be slow or critical:

```yaml
fieldLevelOperations: "getSearchResultsForMultiple,searchAcrossLineageStructure"
```

##### 2. **Path-Based Filtering**

Use path patterns to instrument specific parts of your schema:

```yaml
fieldLevelPaths: "/search/results/**,/user/*/permissions,/**/lineage/*"
```

**Path Pattern Syntax**:

- `/user` - Exact match for the user field
- `/user/*` - Direct children of user (e.g., `/user/name`, `/user/email`)
- `/user/**` - User field and all descendants at any depth
- `/*/comments/*` - Comments field under any parent

##### 3. **Combined Filtering**

When both operation and path filters are configured, only fields matching BOTH criteria are instrumented:

```yaml
# Only instrument search results within specific operations
fieldLevelOperations: "searchAcrossEntities"
fieldLevelPaths: "/searchResults/**"
```

#### Advanced Options

```yaml
# Include field paths as metric tags (WARNING: high cardinality risk)
fieldLevelPathEnabled: false

# Include metrics for trivial property access
trivialDataFetchersEnabled: false
```

### Filtering Modes Explained

The instrumentation automatically determines the most efficient filtering mode:

1. **DISABLED**: Field-level metrics completely disabled
2. **ALL_FIELDS**: No filtering, all fields instrumented (highest overhead)
3. **BY_OPERATION**: Only instrument fields within specified operations
4. **BY_PATH**: Only instrument fields matching path patterns
5. **BY_BOTH**: Most restrictive - both operation and path must match

### Performance Considerations

#### Impact Assessment

Field-level instrumentation overhead varies by:

- **Query complexity**: More fields = more overhead
- **Resolver performance**: Fast resolvers have higher relative overhead
- **Filtering effectiveness**: Better targeting = less overhead

#### Best Practices

1. **Start Conservative**: Begin with field-level metrics disabled

   ```yaml
   fieldLevelEnabled: false
   ```

2. **Target Known Issues**: Enable selectively for problematic operations

   ```yaml
   fieldLevelEnabled: true
   fieldLevelOperations: "slowSearchQuery,complexLineageQuery"
   ```

3. **Use Path Patterns Wisely**: Focus on expensive resolver paths

   ```yaml
   fieldLevelPaths: "/search/**,/**/lineage/**"
   ```

4. **Avoid Path Tags in Production**: High cardinality risk

   ```yaml
   fieldLevelPathEnabled: false # Keep this false
   ```

5. **Monitor Instrumentation Overhead**: Track the `graphql.fields.instrumented` metric

### Example Configurations

#### Development Environment (Full Visibility)

```yaml
graphQL:
  metrics:
    enabled: true
    fieldLevelEnabled: true
    fieldLevelOperations: "" # All operations
    fieldLevelPathEnabled: true # Include paths for debugging
    trivialDataFetchersEnabled: true
```

#### Production - Targeted Monitoring

```yaml
graphQL:
  metrics:
    enabled: true
    fieldLevelEnabled: true
    fieldLevelOperations: "getSearchResultsForMultiple,searchAcrossLineage"
    fieldLevelPaths: "/search/results/*,/lineage/upstream/**,/lineage/downstream/**"
    fieldLevelPathEnabled: false
    trivialDataFetchersEnabled: false
```

#### Production - Minimal Overhead

```yaml
graphQL:
  metrics:
    enabled: true
    fieldLevelEnabled: false # Only request-level metrics
```

### Debugging Slow Queries

When investigating GraphQL performance issues:

1. **Enable request-level metrics first** to identify slow operations
2. **Temporarily enable field-level metrics** for the slow operation:
   ```yaml
   fieldLevelOperations: "problematicQuery"
   ```
3. **Analyze field duration metrics** to find bottlenecks
4. **Optionally enable path tags** (briefly) for precise identification:
   ```yaml
   fieldLevelPathEnabled: true # Temporary only!
   ```
5. **Optimize identified resolvers** and disable detailed instrumentation

### Integration with Monitoring Stack

The GraphQL metrics integrate seamlessly with DataHub's monitoring infrastructure:

- **Prometheus**: Metrics exposed at `/actuator/prometheus`
- **Grafana**: Create dashboards showing:
  - Request rates and latencies by operation
  - Error rates and types
  - Field resolver performance heatmaps
  - Top slow operations and fields

Example Prometheus queries:

```promql
# Average request duration by operation
rate(graphql_request_duration_seconds_sum[5m])
/ rate(graphql_request_duration_seconds_count[5m])

# Field resolver p99 latency
histogram_quantile(0.99,
  rate(graphql_field_duration_seconds_bucket[5m])
)

# Error rate by operation
rate(graphql_request_errors_total[5m])
```

## Kafka Consumer Instrumentation (Micrometer)

### Overview

DataHub provides comprehensive instrumentation for Kafka message consumption through Micrometer metrics, enabling
real-time monitoring of message queue latency and consumer performance. This instrumentation is critical for
maintaining data freshness SLAs and identifying processing bottlenecks across DataHub's event-driven architecture.

### Why Kafka Queue Time Monitoring Matters

Traditional Kafka lag monitoring only tells you "we're behind by 10,000 messages"
Without queue time metrics, you can't answer critical questions like "are we meeting our 5-minute data freshness SLA?"
or "which consumer groups are experiencing delays?"

#### Real-World Impact

Consider these scenarios:

Variable Production Rate:

- Morning: 100 messages/second → 1000 message lag = 10 seconds old
- Evening: 10 messages/second → 1000 message lag = 100 seconds old
- Same lag count, vastly different business impact!

Burst Traffic Patterns:

- Bulk ingestion creates 1M message backlog
- Are these messages from the last hour (recoverable) or last 24 hours (SLA breach)?

Consumer Group Performance:

- Real-time processors need < 1 minute latency
- Analytics consumers can tolerate 1 hour latency
- Different groups require different monitoring thresholds

### Architecture

Kafka queue time instrumentation is implemented across all DataHub consumers:

- MetadataChangeProposals (MCP) Processor - SQL entity updates
  - BatchMetadataChangeProposals (MCP) Processor - Bulk SQL entity updates
- MetadataChangeLog (MCL) Processor & Hooks - Elasticsearch & downstream aspect operations
- DataHubUsageEventsProcessor - Usage analytics events
- PlatformEventProcessor - Platform operations & external consumers

Each consumer records **record age** at processing time: `now - ConsumerRecord.timestamp()` (Micrometer name `datahub.kafka.consumer.record_age`).

### Metrics Collected

#### Core Metric

Metric: `datahub.kafka.consumer.record_age`

- Type: Timer with configurable percentiles and SLO buckets
- Unit: Seconds on Prometheus (`_seconds` suffix); source measurement is milliseconds
- Tags:
  - topic: Kafka topic name (e.g., "MetadataChangeProposal_v1")
  - consumer.group: Consumer group ID (e.g., "generic-mce-consumer")
- Use Case: Consumer lag / record age from the Kafka record timestamp to when the consumer handles the message

#### Statistical Distribution

The timer automatically tracks:

- Count: Total messages processed
- Sum: Cumulative queue time
- Max: Highest queue time observed
- Percentiles: p50, p95, p99, p99.9 (configurable)
- SLO Buckets: Percentage of messages meeting latency targets

#### Configuration Guide

Default Configuration:

```yaml
kafka:
  consumer:
    metrics:
      # Percentiles to calculate
      percentiles: "0.5,0.95,0.99,0.999"

      # Service Level Objective buckets (seconds)
      slo: "300,1800,3600,10800,21600,43200" # 5m,30m,1h,3h,6h,12h

      # Maximum expected queue time
      maxExpectedValue: 86400 # 24 hours (seconds)
```

#### Key Monitoring Patterns

SLA Compliance Monitoring:

```promql
# Percentage of messages processed within 5-minute SLA
sum(rate(datahub_kafka_consumer_record_age_seconds_bucket{le="300"}[5m])) by (topic)
/ sum(rate(datahub_kafka_consumer_record_age_seconds_count[5m])) by (topic) * 100
```

Consumer Group Comparison:

```promql
# P99 queue time by consumer group
histogram_quantile(0.99,
  sum by (consumer_group, le) (
    rate(datahub_kafka_consumer_record_age_seconds_bucket[5m])
  )
)
```

#### Performance Considerations

Metric Cardinality:

The instrumentation is designed for low cardinality:

- Only two tags: `topic` and `consumer.group`
- No partition-level tags (avoiding explosion with high partition counts)
- No message-specific tags

Overhead Assessment:

- CPU Impact: Minimal - single timestamp calculation per message
- Memory Impact: ~5KB per topic/consumer-group combination
- Network Impact: Negligible - metrics aggregated before export

#### Relation to legacy `kafkaLag`

The Micrometer timer `datahub.kafka.consumer.record_age` is the **preferred** signal for consumer queue delay on the Prometheus path. A Dropwizard-style `kafkaLag` histogram may still be emitted for older JMX-oriented scrapes. Operators moving to `/actuator/prometheus` should use the Micrometer timer; **its name and tags are not locked** until observability is declared stable (see [Canonical metric taxonomy](#canonical-metric-taxonomy-prometheus)).

Micrometer queue-time metrics provide:

- Better percentile accuracy
- SLO bucket tracking
- Multi-backend support
- Dimensional tagging

## DataHub Request Hook Latency Instrumentation (Micrometer)

### Overview

DataHub provides comprehensive instrumentation for measuring the latency from initial request submission to post-MCL
(Metadata Change Log) hook execution. This metric is crucial for understanding the end-to-end processing time of metadata
changes, including both the time spent in Kafka queues and the time taken to process through the system to the final hooks.

### Why Hook Latency Monitoring Matters

Traditional metrics only show individual component performance. Request hook latency provides the complete picture of how long
it takes for a metadata change to be fully processed through DataHub's pipeline:

- Request Submission: When a metadata change request is initially submitted
- Queue Time: Time spent in Kafka topics waiting to be consumed
- Processing Time: Time for the change to be persisted and processed
- Hook Execution: Final execution of MCL hooks

This end-to-end view is essential for:

- Meeting data freshness SLAs
- Identifying bottlenecks in the metadata pipeline
- Understanding the impact of system load on processing times
- Ensuring timely updates to downstream systems

### Configuration

Hook latency metrics are configured separately from Kafka consumer metrics to allow fine-tuning based on your specific requirements:

```yaml
datahub:
  metrics:
    # MCL hook trace lag: trace timestamp in system metadata to successful hook completion
    hookLatency:
      # Percentiles to calculate for latency distribution
      percentiles: "0.5,0.95,0.99,0.999"

      # Service Level Objective buckets (seconds)
      # These define the latency targets you want to track
      slo: "300,1800,3000,10800,21600,43200" # 5m, 30m, 1h, 3h, 6h, 12h

      # Maximum expected latency (seconds)
      # Values above this are considered outliers
      maxExpectedValue: 86000 # 24 hours
```

### Metrics Collected

#### Core Metric

Metric: `datahub.mcl.hook.trace_lag`

- Type: Timer with configurable percentiles and SLO buckets
- Unit: Seconds in Prometheus (`_seconds` suffix); recorded from wall-clock lag in the consumer
- Tags:
  - `hook`: Name of the MCL hook being executed (e.g., "IngestionSchedulerHook", "SiblingsHook")
- Use Case: Lag from the DataHub trace timestamp embedded in MCL system metadata to successful hook completion (not HTTP request latency)

#### Key Monitoring Patterns

SLA Compliance by Hook:

Monitor which hooks are meeting their latency SLAs:

```promql
# Percentage of hook completions within 5-minute trace-lag SLA per hook
sum(rate(datahub_mcl_hook_trace_lag_seconds_bucket{le="300"}[5m])) by (hook)
/ sum(rate(datahub_mcl_hook_trace_lag_seconds_count[5m])) by (hook) * 100
```

Hook Performance Comparison:

Identify which hooks have the highest latency:

```promql
# P99 latency by hook
histogram_quantile(0.99,
  sum by (hook, le) (
    rate(datahub_mcl_hook_trace_lag_seconds_bucket[5m])
  )
)
```

Latency Trends:

Track how hook latency changes over time:

```promql
# Average hook latency trend
avg by (hook) (
  rate(datahub_mcl_hook_trace_lag_seconds_sum[5m])
  / rate(datahub_mcl_hook_trace_lag_seconds_count[5m])
)
```

#### Implementation Details

The hook trace-lag metric uses the trace ID embedded in MCL system metadata:

1. Trace ID generation: A DataHub trace ID encodes a timestamp when the change is initiated
1. Propagation: The trace ID flows through the pipeline via system metadata on the MCL
1. Measurement: After a hook succeeds, the consumer records `now - trace_timestamp`
1. Recording: The value is stored as a timer with the hook simple class name as tag `hook`

#### Performance Considerations

- Overhead: Minimal - only requires trace ID extraction and time calculation per hook execution
- Cardinality: Low - only one tag (hook name) with typically < 20 unique values
- Accuracy: High for DataHub trace IDs; omitted when trace ID is missing or not DataHub-shaped (e.g. some W3C-only IDs)

#### Relationship to Kafka Queue Time Metrics

While Kafka record-age metrics (`datahub.kafka.consumer.record_age`) measure wall-clock lag from the record timestamp to consumption, MCL hook
trace lag measures end-to-end pipeline delay from the trace timestamp:

- Kafka queue time: Time from message production to consumption
- MCL hook trace lag: Time from embedded trace timestamp to successful hook execution

Together, these metrics help identify where delays occur:

- High Kafka queue time but low MCL trace lag: Bottleneck in Kafka consumption
- Low Kafka queue time but high MCL trace lag: Bottleneck in processing or persistence
- Both high: System-wide performance issues

## Aspect Size Validation Metrics

Emitted on all aspect writes (REST, GraphQL, MCP) to track sizes and detect oversized aspects.

**Metrics** (constants in [`MetricUtils`](../../metadata-utils/src/main/java/com/linkedin/metadata/utils/metrics/MetricUtils.java)):

- `datahub.validation.aspect_size.pre_patch.size_distribution` — size distribution of existing aspects (tags: `aspectName`, `sizeBucket`)
- `datahub.validation.aspect_size.post_patch.size_distribution` — size distribution of aspects being written (tags: `aspectName`, `sizeBucket`)
- `datahub.validation.aspect_size.pre_patch.oversized` — oversized aspects found in database (tags: `aspectName`, `remediation`)
- `datahub.validation.aspect_size.post_patch.oversized` — oversized aspects rejected during writes (tags: `aspectName`, `remediation`)
- `datahub.validation.aspect_size.pre_patch.warning` — aspects approaching limit in database (tags: `aspectName`)
- `datahub.validation.aspect_size.post_patch.warning` — aspects approaching limit during writes (tags: `aspectName`)
- `datahub.validation.aspect_size.remediation_deletion.success` / `.failure` — DELETE remediation outcomes (EntityService)

**Configuration:**

See [Aspect Size Validation](mcp-mcl.md#aspect-size-validation) for details.

```yaml
datahub:
  validation:
    aspectSize:
      metrics:
        sizeBuckets: [1048576, 5242880, 10485760, 15728640]
```

Default buckets (1MB, 5MB, 10MB, 15MB) create ranges: 0-1MB, 1MB-5MB, 5MB-10MB, 10MB-15MB, 15MB+

## Cache Monitoring (Micrometer)

### Overview

Micrometer provides automatic instrumentation for cache implementations, offering deep insights into cache performance
and efficiency. This instrumentation is crucial for DataHub, where caching significantly impacts query performance and system load.

### Automatic Cache Metrics

When caches are registered with Micrometer, comprehensive metrics are automatically collected without code changes:

#### Core Metrics

- **`cache.size`** (Gauge) - Current number of entries in the cache
- **`cache.gets`** (Counter) - Cache access attempts, tagged with:
  - `result=hit` - Successful cache hits
  - `result=miss` - Cache misses requiring backend fetch
- **`cache.puts`** (Counter) - Number of entries added to cache
- **`cache.evictions`** (Counter) - Number of entries evicted
- **`cache.eviction.weight`** (Counter) - Total weight of evicted entries (for size-based eviction)

#### Derived Metrics

Calculate key performance indicators using Prometheus queries:

```promql
# Cache hit rate (should be >80% for hot caches)
sum(rate(cache_gets_total{result="hit"}[5m])) by (cache) /
sum(rate(cache_gets_total[5m])) by (cache)

# Cache miss rate
1 - (cache_hit_rate)

# Eviction rate (indicates cache pressure)
rate(cache_evictions_total[5m])
```

### DataHub Cache Configuration

DataHub uses multiple cache layers, each automatically instrumented:

#### 1. Entity Client Cache

```yaml
cache.client.entityClient:
  enabled: true
  maxBytes: 104857600 # 100MB
  entityAspectTTLSeconds:
    corpuser:
      corpUserInfo: 20 # Short TTL for frequently changing data
      corpUserKey: 300 # Longer TTL for stable data
    structuredProperty:
      propertyDefinition: 300
      structuredPropertyKey: 86400 # 1 day for very stable data
```

#### 2. Usage Statistics Cache

```yaml
cache.client.usageClient:
  enabled: true
  maxBytes: 52428800 # 50MB
  defaultTTLSeconds: 86400 # 1 day
  # Caches expensive usage calculations
```

#### 3. Search & Lineage Cache

```yaml
cache.search.lineage:
  ttlSeconds: 86400 # 1 day
```

### Monitoring Best Practices

#### Key Indicators to Watch

1. **Hit Rate by Cache Type**

   ```promql
   # Alert if hit rate drops below 70%
   cache_hit_rate < 0.7
   ```

2. **Memory Pressure**
   ```promql
   # High eviction rate relative to puts
   rate(cache_evictions_total[5m]) / rate(cache_puts_total[5m]) > 0.1
   ```

## Thread Pool Executor Monitoring (Micrometer)

### Overview

Micrometer automatically instruments Java `ThreadPoolExecutor` instances, providing crucial visibility into concurrency
bottlenecks and resource utilization. For DataHub's concurrent operations, this monitoring is essential for maintaining
performance under load.

### Automatic Executor Metrics

#### Pool State Metrics

- **`executor.pool.size`** (Gauge) - Current number of threads in pool
- **`executor.pool.core`** (Gauge) - Core (minimum) pool size
- **`executor.pool.max`** (Gauge) - Maximum allowed pool size
- **`executor.active`** (Gauge) - Threads actively executing tasks

#### Queue Metrics

- **`executor.queued`** (Gauge) - Tasks waiting in queue
- **`executor.queue.remaining`** (Gauge) - Available queue capacity

#### Performance Metrics

- **`executor.completed`** (Counter) - Total completed tasks
- **`executor.seconds`** (Timer) - Task execution time distribution
- **`executor.rejected`** (Counter) - Tasks rejected due to saturation

### DataHub Executor Configurations

#### 1. GraphQL Query Executor

```yaml
graphQL.concurrency:
  separateThreadPool: true
  corePoolSize: 20 # Base threads
  maxPoolSize: 200 # Scale under load
  keepAlive: 60 # Seconds before idle thread removal
  # Handles complex GraphQL query resolution
```

#### 2. Batch Processing Executors

```yaml
entityClient.restli:
  get:
    batchConcurrency: 2 # Parallel batch processors
    batchQueueSize: 500 # Task buffer
    batchThreadKeepAlive: 60
  ingest:
    batchConcurrency: 2
    batchQueueSize: 500
```

#### 3. Search & Analytics Executors

```yaml
timeseriesAspectService.query:
  concurrency: 10 # Parallel query threads
  queueSize: 500 # Buffered queries
```

### Critical Monitoring Patterns

#### Saturation Detection

```promql
# Thread pool utilization (>0.8 indicates pressure)
executor_active / executor_pool_size > 0.8

# Queue filling up (>0.7 indicates backpressure)
executor_queued / (executor_queued + executor_queue_remaining) > 0.7
```

#### Rejection & Starvation

```promql
# Task rejections (should be zero)
rate(executor_rejected_total[1m]) > 0

# Thread starvation (all threads busy for extended period)
avg_over_time(executor_active[5m]) >= executor_pool_core
```

#### Performance Analysis

```promql
# Average task execution time
rate(executor_seconds_sum[5m]) / rate(executor_seconds_count[5m])

# Task throughput by executor
rate(executor_completed_total[5m])
```

### Tuning Guidelines

#### Symptoms & Solutions

| Symptom         | Metric Pattern           | Solution                        |
| --------------- | ------------------------ | ------------------------------- |
| High latency    | `executor_queued` rising | Increase pool size              |
| Rejections      | `executor_rejected` > 0  | Increase queue size or pool max |
| Memory pressure | Many idle threads        | Reduce `keepAlive` time         |
| CPU waste       | Low `executor_active`    | Reduce core pool size           |

#### Capacity Planning

1. **Measure baseline**: Monitor under normal load
2. **Stress test**: Identify saturation points
3. **Set alerts**:
   - Warning at 70% utilization
   - Critical at 90% utilization
4. **Auto-scale**: Consider dynamic pool sizing based on queue depth

## Distributed Tracing

Traces let us track the life of a request across multiple components. Each trace is consisted of multiple spans, which
are units of work, containing various context about the work being done as well as time taken to finish the work. By
looking at the trace, we can more easily identify performance bottlenecks.

We enable tracing by using the [OpenTelemetry java instrumentation library](https://github.com/open-telemetry/opentelemetry-java-instrumentation).
This project provides a Java agent JAR that is attached to java applications. The agent injects bytecode to capture
telemetry from popular libraries.

Using the agent we are able to

1. Plug and play different tracing tools based on the user's setup: Jaeger, Zipkin, or other tools
2. Get traces for Kafka, JDBC, and Elasticsearch without any additional code
3. Track traces of any function with a simple `@WithSpan` annotation

You can enable the agent by setting env variable `ENABLE_OTEL` to `true` for GMS and MAE/MCE consumers. In our
example [docker-compose](../../docker/monitoring/docker-compose.monitoring.yml), we export metrics to a local Jaeger
instance by setting env variable `OTEL_TRACES_EXPORTER` to `jaeger`
and `OTEL_EXPORTER_JAEGER_ENDPOINT` to `http://jaeger-all-in-one:14250`, but you can easily change this behavior by
setting the correct env variables. Refer to
this [doc](https://github.com/open-telemetry/opentelemetry-java/blob/main/sdk-extensions/autoconfigure/README.md) for
all configs.

Once the above is set up, you should be able to see a detailed trace as a request is sent to GMS. We added
the `@WithSpan` annotation in various places to make the trace more readable. You should start to see traces in the
tracing collector of choice. Our example [docker-compose](../../docker/monitoring/docker-compose.monitoring.yml) deploys
an instance of Jaeger with port 16686. The traces should be available at http://localhost:16686.

### Configuration Note

We recommend using either `grpc` or `http/protobuf`, configured using `OTEL_EXPORTER_OTLP_PROTOCOL`. Avoid using `http` will not work as expected due to the size of
the generated spans.

## Micrometer

DataHub is transitioning to Micrometer as its primary metrics framework, representing a significant upgrade in observability
capabilities. Micrometer is a vendor-neutral application metrics facade that provides a simple, consistent API for the most
popular monitoring systems, allowing you to instrument your JVM-based application code without vendor lock-in.

### Why Micrometer?

1. Native Spring Integration

   As DataHub uses Spring Boot, Micrometer provides seamless integration with:

   - Auto-configuration of common metrics
   - Built-in metrics for HTTP requests, JVM, caches, and more
   - Spring Boot Actuator endpoints for metrics exposure
   - Automatic instrumentation of Spring components

2. Multi-Backend Support

   Unlike the legacy DropWizard approach that primarily targets JMX, Micrometer natively supports:

   - Prometheus (recommended for cloud-native deployments)
   - JMX (for backward compatibility)
   - StatsD
   - CloudWatch
   - Datadog
   - New Relic
   - And many more...

3. Dimensional Metrics

   Micrometer embraces modern dimensional metrics with **labels/tags**, enabling:

   - Rich querying and aggregation capabilities
   - Better cardinality control
   - More flexible dashboards and alerts
   - Natural integration with cloud-native monitoring systems

## Canonical metric taxonomy (Prometheus)

This section is the **operator-facing catalog** for Micrometer meters that DataHub emits (or expects from Spring / JVM). Implementation work should match these names and tags; **do not add new meter names or labels** without updating this document and reviewing cardinality.

**Stability:** Micrometer and Prometheus-exposed names/tags in this catalog are **not yet treated as a frozen public API**. DataHub can rename or retag them release-to-release for clarity, cardinality, or alignment with Spring defaults until observability is broadly finalized. Plan on checking [Updating DataHub](../how/updating-datahub.md) and this page when upgrading. **Backward-compatibility pressure applies mainly to legacy JMX / `metrics_com_*` scrape names** and existing dashboards that still target those—not to preserving every early Micrometer string forever.

### Metric domains (hierarchy)

Logical grouping for dashboards and alert routing:

| Domain                    | Prefix / family                     | Components                    | Notes                                                                                                                                                                                                                                                                                                                                                                                                             |
| ------------------------- | ----------------------------------- | ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **GraphQL**               | `graphql.*`                         | GMS                           | DataHub’s GMS GraphQL API (`POST/GET /graphql`), instrumented by [`GraphQLTimingInstrumentation`](../../metadata-io/src/main/java/com/linkedin/metadata/system_telemetry/GraphQLTimingInstrumentation.java) (GraphQL Java + Micrometer). Meter **names** use the conventional `graphql.*` prefix (not `datahub.graphql.*`). See [Normalizing HTTP API metrics](#normalizing-http-api-metrics-graphql-vs-openapi). |
| **OpenAPI (REST)**        | `http.server.requests`              | GMS                           | DataHub’s [OpenAPI v3](../api/openapi/openapi-usage-guide.md) tier (`/openapi/v3/**`, related Spring MVC routes). Uses Spring Boot **HTTP server observation**—no separate `openapi.*` prefix. See [Normalizing HTTP API metrics](#normalizing-http-api-metrics-graphql-vs-openapi).                                                                                                                              |
| **Kafka consumer**        | `datahub.kafka.consumer.record_age` | GMS, MCE, MAE, …              | Timer; tags `topic`, `consumer.group` only.                                                                                                                                                                                                                                                                                                                                                                       |
| **Pipeline hooks**        | `datahub.mcl.hook.trace_lag`        | GMS, MAE, …                   | Timer; tag `hook` (simple class name).                                                                                                                                                                                                                                                                                                                                                                            |
| **Hook failures**         | `datahub.mcl.hook.failures`         | MAE (MCL listeners)           | Counter; tags `hook`, `consumer.group`. Class-scoped `*_failure` meters may still appear on the JMX/legacy path; prefer this family for Prometheus.                                                                                                                                                                                                                                                               |
| **Auth / login**          | `datahub.auth.login_outcomes`       | GMS (`AuthServiceController`) | Counter; see auth row below.                                                                                                                                                                                                                                                                                                                                                                                      |
| **API traffic**           | `datahub.api.traffic`               | GMS                           | Counter; tags `user_category`, `agent_class`, `request_api`, optional `agent_skill` / `agent_caller` from `X-DataHub-Context`. Actuator: `datahub_api_traffic_total`; legacy `requestContext_*` remains on JMX path.                                                                                                                                                                                              |
| **Aspect validation**     | `datahub.validation.aspect_size.*`  | GMS                           | Optional aspect size checks; counters under `datahub.validation.aspect_size.*` (see [Aspect Size Validation Metrics](#aspect-size-validation-metrics)). High tag cardinality if misconfigured.                                                                                                                                                                                                                    |
| **Storage (metadata)**    | `datahub.metadata_store`            | GMS (entity store)            | Ebean (SQL-backed) and Cassandra (CQL) access latency; same meter name—tags `subsystem` (`ebean`, `cassandra`) and `operation` (`get`, `batch_get`, `save`, …).                                                                                                                                                                                                                                                   |
| **Graph (relationships)** | `datahub.graph`                     | GMS                           | Graph / lineage query latency; tags `subsystem` (`elasticsearch` today; `neo4j` when instrumented) and `operation` (`search`, `search_pit`). Same meter name for all graph backends.                                                                                                                                                                                                                              |
| **Timeseries aspects**    | `datahub.timeseries_aspect`         | GMS                           | Timeseries-aspect read/write latency; tags `subsystem` (`elasticsearch` today) and `operation`.                                                                                                                                                                                                                                                                                                                   |
| **Entity search + usage** | `datahub.search`                    | GMS                           | Search / usage-index latency (Elasticsearch today); tags `subsystem` (`elasticsearch`) and `operation` (entity paths use values such as `search`, `autocomplete`; usage index uses `usage_search`).                                                                                                                                                                                                               |
| **Caches**                | `cache.*`                           | GMS, consumers                | Opt-in via `management.metrics.cache.enabled`.                                                                                                                                                                                                                                                                                                                                                                    |
| **Platform**              | JVM, HTTP, Actuator                 | All JVM services              | Standard Spring Boot / Micrometer; scrape `/actuator/prometheus` where enabled.                                                                                                                                                                                                                                                                                                                                   |

### Normalizing HTTP API metrics (GraphQL vs OpenAPI)

GraphQL and OpenAPI are both HTTP APIs on GMS, but Micrometer exposes them through **different instrumentations**:

- **GraphQL** — Operation-level timers and error counters (`graphql.request.*`, `graphql.field.*` if enabled). The same traffic also produces **one** `http.server.requests` series for the servlet path **`/graphql`**, optionally prefixed by GMS base path (see [Base path configuration](../deploy/BASE_PATH_CONFIGURATION.md)).
- **OpenAPI** — **`http.server.requests`** only (per route pattern on `/openapi/...`).

Do **not** treat `graphql.request.*` and `http.server.requests` for `/graphql` as interchangeable counts—they measure different layers (operation vs single HTTP exchange). For OpenAPI there is no separate `openapi.*` family today; **`http.server.requests` is the source of truth** for latency and status codes.

**Recommended “normalized” view (query-time, low cardinality)**

Use a **small, fixed set of surface labels** in Prometheus recording rules or dashboard queries—derive them from path prefixes and meter family, not from raw URNs or unbounded paths:

| API surface | Primary SLO signals                                                  | Optional coarse HTTP view (`http.server.requests`)                              |
| ----------- | -------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| GraphQL     | `graphql_request_duration_seconds_*`, `graphql_request_errors_total` | Match `uri` to `**/graphql` (include context path in the regex you deploy with) |
| OpenAPI     | `http_server_requests_seconds_*` with `uri` matching `**/openapi/**` | Same series                                                                     |

Example idea for a **unified error-pressure panel** (illustrative—adjust labels and regexes to your scrape):

```promql
# GraphQL: application-level errors (not HTTP 4xx/5xx on the wire only)
sum(rate(graphql_request_errors_total[5m]))

# OpenAPI: HTTP 5xx rate (narrow uri pattern to your deployment)
sum(rate(http_server_requests_seconds_count{status=~"5..",uri=~".*/openapi/.*"}[5m]))
```

To **sum into one chart**, use `label_replace` or recording rules that attach a shared label such as `datahub_api_surface="graphql"` vs `openapi`—keep **only those two values** (plus `other` if you add more rules later) so cardinality stays bounded.

**Optional future: normalize in the application**

If query-time joins are too awkward, a follow-up can add a **single bounded tag** (e.g. `datahub.api.surface` with values `graphql` \| `openapi` \| `other`) on observations—design it explicitly so `uri` is never copied verbatim into new tags.

**Prefix consistency:** Names are **not** all under `datahub.*` on purpose.

- **Spring / Micrometer instrumentation names** — DataHub runs Spring Boot and enables standard instrumentation where the framework defines meters (e.g. **`http.server.requests`**, `cache.*`, JVM). GraphQL request/field timers use the conventional **`graphql.*`** names emitted by DataHub’s own [`GraphQLTimingInstrumentation`](../../metadata-io/src/main/java/com/linkedin/metadata/system_telemetry/GraphQLTimingInstrumentation.java), which aligns with common Micrometer GraphQL examples even though the class lives in this repo. Do **not** duplicate the same signals under `datahub.*`.
- **DataHub-prefixed application metrics** — Use `datahub.*` for meters defined in DataHub code and listed here (e.g. `datahub.mcl.hook.trace_lag`, `datahub.mcl.hook.failures`, `datahub.auth.login_outcomes`, `datahub.validation.aspect_size.*`). **New** custom application meters should use this prefix unless they are clearly part of a Spring-managed family above.

**Opt-in instrumentation:** Feature flags and environment variables turn on extra Micrometer meters (GraphQL field-level timers, cache stats, optional validation counters, Kafka hook failures, and similar). **Only code paths that actually run** register series—enabling a flag does not create a useful time series until traffic hits that instrumentation. After you opt in, scrape `/actuator/prometheus` and confirm the **catalog names** you expect (for example `graphql_request_duration_seconds_*` for GraphQL latency, not a parallel `datahub.graphql.*` duplicate).

**Label-based routing:** Many deployments scrape **both** a JMX-exporter target (long `metrics_com_*` / Dropwizard-style names) **and** Spring Actuator on the same logical service. Prometheus `metric_relabel_configs`, recording rules, Grafana variables, or per-panel datasource routing then **choose which registry** backs a chart. When you move a panel to Micrometer, point that route at the Actuator scrape and rewrite PromQL to use **low-cardinality labels** (`operation`, `success`, `hook`, `topic`, …) on a **small set of families** (`graphql_request_duration_seconds_*`, `datahub_mcl_hook_failures_total`, …). Avoid perpetuating legacy patterns that regex-match dozens of `__name__` values (`*_Mean`, `*_95thPercentile`, typo’d percentile spellings, and so on) unless you still depend on the JMX path.

**Prometheus mapping:** Java/Micrometer uses **dot-separated** logical names. Prometheus exposition typically maps dots to underscores (counters gain `_total`, e.g. `datahub_auth_login_outcomes_total`).

### Domain meter names vs `subsystem` tag {#domain-meter-names-vs-subsystem-tags}

Use **stable, domain-focused** meter names (`datahub.graph`, `datahub.search`, `datahub.timeseries_aspect`, …). Use **`subsystem` only for the backing technology** (e.g. `elasticsearch`, `neo4j`, `cassandra`, `ebean`)—the meter name already states the domain, so do not repeat it inside `subsystem` (avoid values like `graph_elasticsearch`). Add **`operation`** for coarse steps within that meter. **Do not** add a parallel `implementation` label on these timers.

| Concept                | How it appears | Examples / notes                                                                                                                                                                     |
| ---------------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Backend technology** | `subsystem`    | `elasticsearch` (graph, timeseries, and search meters today), `neo4j` (graph when instrumented), `cassandra`, `ebean` (metadata entity store via Ebean on `datahub.metadata_store`). |
| **Operation / role**   | `operation`    | Coarse step name inside the domain (not index names, not SQL text): `search`, `batch_get`, `search_pit`, `usage_search` (usage index on `datahub.search`).                           |

**Legitimate split across multiple meter names:** When the **functional domain** differs—not only the backing vendor—separate meters are OK. Example: `datahub.graph`, `datahub.timeseries_aspect`, and `datahub.search` are different domains even when all use Elasticsearch today; each keeps its own meter name while **`subsystem`** carries the technology (`elasticsearch`, and later `neo4j` for graph if instrumented). For the metadata entity store, **one** meter (`datahub.metadata_store`) covers both Ebean and Cassandra; use **`subsystem`** (`ebean` vs `cassandra`) plus **`operation`**.

**Spring-managed meters** (e.g. `http.server.requests`, `graphql.request.*`, `cache.*`) follow upstream Micrometer naming; this convention applies to **DataHub-defined** `datahub.*` application meters in [`MetricUtils`](../../metadata-utils/src/main/java/com/linkedin/metadata/utils/metrics/MetricUtils.java).

### Cardinality rules

- **Allowed tag keys** for new application meters: `hook`, `topic`, `consumer.group`, coarse `operation` / `resource`, `result`, `login_source`, `denial_reason` (enum-valued), for API traffic `user_category`, `agent_class`, `request_api`, and optional `agent_skill` / `agent_caller` (sanitized, allowlisted-header keys only—see [`DataHubContextParser`](../../metadata-operation-context/src/main/java/io/datahubproject/metadata/context/request/DataHubContextParser.java)), and for `datahub.metadata_store`, `datahub.graph`, `datahub.timeseries_aspect`, and `datahub.search` use **`subsystem`** (bounded **technology** values such as `elasticsearch`, `neo4j`, `ebean`, `cassandra`) plus coarse **`operation`**—**not** a separate `implementation` tag.
- **Do not** use entity URNs, raw search text, SQL, exception messages, or unbounded strings as label values.
- **GraphQL field-level metrics** multiply series; keep `GRAPHQL_METRICS_FIELD_LEVEL_ENABLED=false` in production unless you explicitly accept the cost (see [Environment variables – GraphQL](../deploy/environment-vars.md#graphql-configuration)).
- **HTTP `uri` (or route) tags** on `http.server.requests` can explode cardinality if raw paths are used. Prefer Spring’s templated patterns where possible; scope dashboards and alerts with conservative matchers (e.g. prefix on `/openapi/`).

### Metric catalog

| Meter (logical name)                              | Type            | Component       | Tags (allowed values)                                                        | Cardinality note                                                      | Typical consumer                                                                                                                                                                                                 |
| ------------------------------------------------- | --------------- | --------------- | ---------------------------------------------------------------------------- | --------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `graphql.request.duration`                        | Timer           | GMS             | `operation`, `operation.type`, `success`, `field.filtering`                  | Per-operation; bounded by registered operations                       | Dashboards, latency SLOs                                                                                                                                                                                         |
| `graphql.request.errors`                          | Counter         | GMS             | `operation`, `operation.type`                                                | Same                                                                  | **GraphQL error-rate alerts** (replaces legacy `GraphQLController_error_Count` when queries are migrated)                                                                                                        |
| `http.server.requests`                            | Timer           | GMS             | `method`, `status`, `uri` (and other Boot defaults)                          | Grows with distinct route patterns; watch `uri`                       | **OpenAPI parity:** latency and status for `/openapi/**` alongside GraphQL; example: `sum(rate(http_server_requests_seconds_count{uri=~"/openapi.*"}[5m])) by (status, method)`                                  |
| `graphql.field.duration` / `graphql.field.errors` | Timer / counter | GMS             | Field-level tags (if enabled)                                                | **High** when field-level enabled                                     | Debugging only                                                                                                                                                                                                   |
| `datahub.kafka.consumer.record_age`               | Timer           | Kafka consumers | `topic`, `consumer.group` only                                               | Topics × groups × histogram buckets                                   | Lag / queue delay vs legacy `kafkaLag`                                                                                                                                                                           |
| `datahub.mcl.hook.trace_lag`                      | Timer           | GMS, MAE, …     | `hook`                                                                       | Bounded hook set × buckets                                            | Trace timestamp → successful MCL hook completion                                                                                                                                                                 |
| `datahub.mcl.hook.failures`                       | Counter         | MAE             | `hook`, `consumer.group`                                                     | Hooks × groups                                                        | MCL hook invoke failures (`sum by (hook) (rate(...))`)                                                                                                                                                           |
| `datahub.auth.login_outcomes`                     | Counter         | GMS             | `operation`, `result`, `login_source`, `denial_reason`                       | Fixed enums                                                           | Login success/failure dashboards; aligns with `LoginDenialReason` / `LoginSource`                                                                                                                                |
| `datahub.api.traffic`                             | Counter         | GMS             | `user_category`, `agent_class`, `request_api`, `agent_skill`, `agent_caller` | Bounded agent dimension tags × tier × HTTP client class × API surface | **API usage on Actuator:** `datahub_api_traffic_total`; optional `X-DataHub-Context` header (`skill` / `caller` only). Legacy JMX path still emits `requestContext_*` without context dimensions.                |
| `datahub.validation.aspect_size.*`                | Counter         | GMS             | `aspectName`, `sizeBucket`, `remediation` (varies by meter)                  | Can grow if enabled broadly                                           | Tier 3 / troubleshooting; see [Aspect Size Validation Metrics](#aspect-size-validation-metrics)                                                                                                                  |
| `datahub.metadata_store`                          | Timer           | GMS             | `subsystem`, `operation`                                                     | `ebean` or `cassandra` × bounded op names                             | Metadata entity store: `EbeanAspectDao` (`subsystem=ebean`) and `CassandraAspectDao` (`subsystem=cassandra`)                                                                                                     |
| `datahub.graph`                                   | Timer           | GMS             | `subsystem`, `operation`                                                     | `elasticsearch` today × bounded op names                              | Relationship / graph queries (ES graph index today; same meter for other backends)                                                                                                                               |
| `datahub.timeseries_aspect`                       | Timer           | GMS             | `subsystem`, `operation`                                                     | `elasticsearch` today × bounded op names                              | Timeseries-aspect storage (`ElasticSearchTimeseriesAspectService` today)                                                                                                                                         |
| `datahub.search`                                  | Timer           | GMS             | `subsystem`, `operation`                                                     | `elasticsearch` × bounded op names (usage index uses `usage_search`)  | Entity search (`ESSearchDAO`) and usage-event search (`DataHubUsageServiceImpl`); not graph index (see [`MetricUtils`](../../metadata-utils/src/main/java/com/linkedin/metadata/utils/metrics/MetricUtils.java)) |
| `cache.*`                                         | Various         | GMS, clients    | Micrometer cache tags                                                        | Bounded                                                               | Cache hit/miss                                                                                                                                                                                                   |

**`datahub.auth.login_outcomes` tags (intended contract):**

- `operation`: `generate_session_token` \| `verify_native_user_credentials` (extend only when new instrumented endpoints ship).
- `result`: `success` \| `failure`.
- `login_source`: Value from [`LoginSource`](../../metadata-service/services/src/main/java/com/linkedin/metadata/datahubusage/event/LoginSource.java) (`passwordLogin`, `ssoLogin`, …), or `unknown` if missing / unrecognized header `X-DataHubLoginSource`.
- `denial_reason`: `LoginDenialReason.name()` on failures; on success use `none` so PromQL can use a stable label set.

**`datahub.api.traffic` tags (intended contract):**

- `user_category`: `system` \| `admin` \| `service` \| `regular` — from actor URN in `RequestContext` (`system` / `admin` are exact matches; `service` when the URN has prefix `urn:li:corpuser:service:` for DataHub service accounts; otherwise `regular`).
- `agent_skill`, `agent_caller`: from optional header `X-DataHub-Context` (`skill=...;caller=...`). Only allowlisted keys are recorded; values are normalized (length-capped, Prometheus-safe). Absent or invalid → `unspecified`. OpenTelemetry uses `agent.skill` and `agent.caller`.
- `agent_class`: Lowercased agent classification from the user-agent parser (e.g. `browser`, `robot`, `sdk`, `unknown`).
- `request_api`: `restli` \| `openapi` \| `graphql` (`test` traffic is excluded).

### Remaining major observability surfaces (not fully covered above)

Use this as a backlog for dashboards and future instrumentation:

- **Graph backend (non-GraphQL)** — Lineage/relationship storage may be **Elasticsearch/OpenSearch** (graph index) or **Neo4j** when `graphService.type=neo4j`. Elasticsearch graph queries record **`datahub.graph`** with `subsystem=elasticsearch`; Neo4j sessions are not yet wrapped—when added, use the same meter with `subsystem=neo4j` (and coarse `operation`), not a new `datahub.neo4j.*` family.
- **Elasticsearch writes / bulk indexing** — Entity and graph **write** paths (`ESWriteDAO`, bulk processors, `BulkListener_*` on the legacy JMX path) are not yet mirrored as **`datahub.search`** / **`datahub.graph`** timers for bulk flush latency or failure rates; hook failures and consumer record-age timers cover part of the pipeline but not every bulk round-trip.
- **Rest.li entity APIs** — Ingest and entity resources often appear as legacy `metrics_com_*` timers; coarse coverage is available via `http.server.requests` and `datahub.api.traffic`, but per-resource Micrometer timers are not standardized yet.
- **Kafka producers and non-consumer hooks** — Producer-side latency and MCP/MAE **write** paths to search indices are separate from consumer `datahub.kafka.consumer.record_age`.
- **Secondary / semantic search indices** — Semantic or auxiliary indices share the same client bean; today’s **`datahub.search`** with `subsystem=elasticsearch` covers [`ESSearchDAO`](../../metadata-io/src/main/java/com/linkedin/metadata/search/elasticsearch/query/ESSearchDAO.java) operations regardless of index name—split further only if you add explicit `operation` values or a dedicated meter.
- **System metadata, browse, and scroll-heavy jobs** — Long-running scroll or reindex flows may need additional `operation` values or dedicated meters.
- **Infrastructure outside the JVM** — MySQL/Ebean pool sizing (`metrics_ebeans_*`), Elasticsearch exporter series, and gateway recording rules remain operator-owned.

### Legacy `metrics_com_*` → migration targets

JMX-exporter-style names often look like `metrics_com_<class>_<metric>_...`. Prefer **PromQL/dashboard changes** to the **`graphql.*`** and other Micrometer families below instead of re-implementing those shapes in application code. **Column “Target” shows current Micrometer/Prometheus names**; they may change while the surface is still evolving (see **Stability** above)—always verify against your release’s scrape output.

| Tier  | Legacy / dashboard signal                                                                 | Target                                                                                                                  |
| ----- | ----------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| **1** | `metrics_com_datahub_graphql_GraphQLController_error_Count` (e.g. GraphQLErrorRate alert) | `graphql_request_errors_total` with existing scrape labels (`namespace`, `container`, …)                                |
| **2** | `metrics_com_*_kafkaLag_*` / per-topic lag panels                                         | `datahub_kafka_consumer_record_age_seconds_*` and/or Kafka exporter consumer lag; drop app `kafkaLag` after panels move |
| **2** | Per-hook `*_failure_Count` on consumers                                                   | `datahub_mcl_hook_failures_total` with `sum by (hook)`                                                                  |
| **2** | `GraphQLController_*` latency percentiles                                                 | `graphql_request_duration_seconds_*` by `operation` / `success`                                                         |
| **2** | `metrics_requestContext_*` / `requestContext_*` usage counters (per combination in name)  | `datahub_api_traffic_total` with labels `user_category`, `agent_class`, `request_api`                                   |
| **3** | `CacheableSearcher_*`, incident generator hooks, Avro conversion, Ebean                   | Defer or recording rules unless a panel is required                                                                     |

### Staging checklist (operators)

After upgrading GMS/consumers:

1. Scrape `/actuator/prometheus` (or your equivalent) and confirm new series: `datahub_auth_login_outcomes_total`, `datahub_mcl_hook_failures_total`, `datahub_api_traffic_total` (after GMS has handled non-test API traffic).
2. Estimate **extra** time series (especially histogram buckets) before enabling optional timers.
3. If you scrape **both** JMX and Actuator for GMS/consumers, align **recording rules and dashboard datasources** so each panel (and alert) reads from the registry you intend after cutover—do not mix two names for the same SLO without an explicit bridge rule.
4. Update your Prometheus alert rules, recording rules (if any), and Grafana dashboards to match the new series; re-run your usual rule/dashboard tests in CI or staging. Treat Alertmanager receiver URLs and other routing secrets as sensitive—do not paste them into tickets or public documentation.

### Example Grafana dashboard patterns (legacy → Micrometer)

Typical operator exports (for example **DataHub Overview**, **Lineage Performance**, **[WIP] API Usage**, **Aspects Dashboard**, **MySQL Health**) often still query JMX-style series. When Micrometer is enabled and you migrate, prefer the catalog families above and **labels instead of `__name__` gymnastics**:

- **Overview-style boards** — Panels on `metrics_com_datahub_graphql_GraphQLController_*` percentiles, Rest.li `AspectResource` / `EntityResource` counters, per-hook `*_failure_Count`, app-registered `kafkaLag`, and `errorCode_*` counters should move to `graphql_request_duration_seconds_*` / `graphql_request_errors_total`, `datahub_mcl_hook_failures_total`, `datahub.kafka.consumer.record_age` (or Kafka-exporter lag), and coarse `http.server.requests` where that is the supported surface. Keep JVM (`jvm_*`), Elasticsearch, MySQL `information_schema`, and gateway recording rules (`api_gateway_*`, if you maintain them) as **infrastructure** signals—they are outside the `datahub.*` / `graphql.*` application catalog but still need consistent scrape labels (`namespace`, `job`, …).

- **Lineage performance** — Heavy `label_replace` across `metrics_com_datahub_graphql_GraphQLController_searchAcrossLineage_*`, related scroll/download/aggregate operations, `ESGraphQueryDAO_*`, and `Relationships_getLineage_*` timeline names maps naturally to **one** histogram family with an `operation` (and related) tag on the Micrometer path; rebuild panels with `sum by (operation, …)(histogram_quantile(...))` rather than rediscovering each legacy suffix.

- **API usage by client context** — On Actuator, use **`datahub_api_traffic_total`** with labels `user_category`, `agent_class`, `request_api` (logical meter `datahub.api.traffic`). Example: `sum by (user_category, agent_class, request_api) (rate(datahub_api_traffic_total[5m]))`. Legacy panels that match `metrics_requestContext_*` or `requestContext_*` per combination can be migrated without `label_replace` on `__name__`. Confirm GMS is receiving real API traffic (not only `RequestAPI.TEST`) so series exist.

- **Aspect write rates** — `metrics_com_linkedin_metadata_entity_ebean_EbeanAspectDao_aspectWriteCount_<aspect>_Count` (often unpacked with `label_replace` to `aspectName`) is **tier 3** in the migration table unless a tagged Micrometer replacement exists in your release.

- **MySQL / persistence dashboards** — Panels on `metrics_ebeans_connection_pool_size_main` and Elasticsearch `BulkListener_*` counters are **not** the same subsystem; pool metrics should align with whatever datasource pool Micrometer exposes for your config, while bulk indexing stays on ES exporter or legacy series until deliberately unified.

## Micrometer Transition Plan

DataHub is undertaking a strategic transition from DropWizard metrics (exposed via JMX) to Micrometer, a modern vendor-neutral metrics facade.
This transition aims to provide better cloud-native monitoring capabilities. **Existing JMX- and `metrics_com_*`-based dashboards** may need updates over time; **Micrometer meter names on the Prometheus endpoint are still evolving** and are not held to the same compatibility bar until the catalog is explicitly stabilized.

### Current State

What we have **today** (many deployments):

- **Prometheus-first path:** Spring Boot Actuator exposes Micrometer metrics at `/actuator/prometheus` (GraphQL, Kafka queue time, hook latency, auth outcomes, hook failures, etc.).
- **Legacy path:** Dropwizard-style meters still registered with tag `dwizMetric=true` and long class-derived names for backward compatibility with older Grafana panels.
- **Collection:** Prometheus scrapes either the Actuator endpoint, the JMX exporter, or both depending on your Helm/docker configuration.
- **Code:** [`MetricUtils`](../../metadata-utils/src/main/java/com/linkedin/metadata/utils/metrics/MetricUtils.java) bridges legacy and Micrometer APIs.

Alerts and dashboards should migrate from `metrics_com_*` toward the **canonical catalog** above.

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/0f6ae5ae889ee4e780504ca566670867acf975ff/imgs/advanced/monitoring/monitoring_current.svg"/>
</p>

Limitations:

- JMX-centric approach limits monitoring backend options
- No unified observability (separate instrumentation for metrics and traces)
- No support for dimensional metrics and tags
- Manual instrumentation required for most components
- Legacy naming conventions without proper tagging

### Transition State

What We're Building:

- Primary System: Micrometer with native Prometheus support
- Collection Method: Direct Prometheus scraping via /actuator/prometheus
- Unified Telemetry: Single instrumentation point for both metrics and traces
- Modern Patterns: Dimensional metrics with rich tagging
- Multi-Backend: Support for Prometheus, StatsD, CloudWatch, Datadog, etc.
- Auto-Instrumentation: Automatic metrics for Spring components

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/0f6ae5ae889ee4e780504ca566670867acf975ff/imgs/advanced/monitoring/monitoring_transition.svg"/>
</p>

Key Decisions and Rationale:

1. Dual Registry Approach

   **Decision:** Run both systems in parallel with tag-based routing (legacy Dropwizard/JMX vs Micrometer on Actuator).

   **Rationale:**

   - Lets deployments that still scrape JMX keep old series while others move to Prometheus
   - Gradual cutover of dashboards and alerts
   - Micrometer naming can still be refined in application releases without treating early names as immutable
   - Cutover PromQL should prefer **dimensional labels** on Micrometer families (see **Label-based routing** and **Example Grafana dashboard patterns** above) instead of replicating legacy `__name__` regex bridges

2. Prometheus as Primary Target

   **Decision:** Focus on Prometheus for new metrics

   **Rationale:**

   - Industry standard for cloud-native applications
   - Rich query language and ecosystem
   - Better suited for dimensional metrics

3. Observation API Adoption

   **Decision:** Promote Observation API for new instrumentation

   **Rationale:**

   - Single instrumentation for metrics + traces
   - Reduced code complexity
   - Consistent naming across telemetry types

### Future State

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/0f6ae5ae889ee4e780504ca566670867acf975ff/imgs/advanced/monitoring/monitoring_future.svg"/>
</p>

Once fully adopted, Micrometer will transform DataHub's observability from a collection of separate tools into a unified platform.
This means developers can focus on building features while getting comprehensive telemetry "for free."

Intelligent and Adaptive Monitoring

- Dynamic Instrumentation: Enable detailed metrics for specific entities or operations on-demand without code changes
- Environment-Aware Metrics: Automatically route metrics to Prometheus in Kubernetes, CloudWatch in AWS, or Azure Monitor in Azure
- Built-in SLO Tracking: Define Service Level Objectives declaratively and automatically track error budgets

Developer and Operator Experience

- Adding @Observed to a method automatically generates latency percentiles, error rates, and distributed trace spans
- Every service exposes golden signals (latency, traffic, errors, saturation) out-of-the-box
- Business metrics (entity ingestion rates, search performance) seamlessly correlate with system metrics
- Self-documenting telemetry where metrics, traces, and logs tell a coherent operational story

## DropWizard & JMX

We originally decided to use [Dropwizard Metrics](https://metrics.dropwizard.io/4.2.0/) to export custom metrics to JMX,
and then use [Prometheus-JMX exporter](https://github.com/prometheus/jmx_exporter) to export all JMX metrics to
Prometheus. This allows our code base to be independent of the metrics collection tool, making it easy for people to use
their tool of choice. You can enable the agent by setting env variable `ENABLE_PROMETHEUS` to `true` for GMS and MAE/MCE
consumers. Refer to this example [docker-compose](../../docker/monitoring/docker-compose.monitoring.yml) for setting the
variables.

In our example [docker-compose](../../docker/monitoring/docker-compose.monitoring.yml), we have configured prometheus to
scrape from 4318 ports of each container used by the JMX exporter to export metrics. We also configured grafana to
listen to prometheus and create useful dashboards. By default, we provide two
dashboards: [JVM dashboard](https://grafana.com/grafana/dashboards/14845) and DataHub dashboard.

**Micrometer (Spring Actuator / Play):** Docker images for GMS, MAE consumer, MCE consumer, and the Play frontend set
`MANAGEMENT_SERVER_PORT` to **4319** by default (see container `start.sh`). Micrometer Prometheus text is served at
`http://<container>:4319/actuator/prometheus` on a separate HTTP listener (reachable on the Docker network; quickstart compose **`expose`s** this port rather than publishing it to the host). JVM/JMX metrics remain on **4318** when
`ENABLE_PROMETHEUS=true`. The example [prometheus.yaml](../../docker/monitoring/prometheus.yaml) includes a `micrometer`
scrape job for port **4319**. Outside Docker, leave `MANAGEMENT_SERVER_PORT` unset so Actuator stays on the main
application port; set it when you want a separate management listener (Spring maps the env var to
`management.server.port`).

In the JVM dashboard, you can find detailed charts based on JVM metrics like CPU/memory/disk usage. In the DataHub
dashboard, you can find charts to monitor each endpoint and the kafka topics. Using the example implementation, go
to http://localhost:3001 to find the grafana dashboards! (Username: admin, PW: admin)

To make it easy to track various metrics within the code base, we created MetricUtils class. This util class creates a
central metric registry, sets up the JMX reporter, and provides convenient functions for setting up counters and timers.
You can run the following to create a counter and increment.

```java
metricUtils.counter(this.getClass(),"metricName").increment();
```

You can run the following to time a block of code.

```java
try(Timer.Context ignored=metricUtils.timer(this.getClass(),"timerName").timer()){
    ...block of code
    }
```

#### Enable monitoring through docker-compose

We provide some example configuration for enabling monitoring in
this [directory](https://github.com/datahub-project/datahub/tree/master/docker/monitoring). Take a look at the docker-compose
files, which adds necessary env variables to existing containers, and spawns new containers (Jaeger, Prometheus,
Grafana).

You can add in the above docker-compose using the `-f <<path-to-compose-file>>` when running docker-compose commands.
For instance,

```shell
docker-compose \
  -f quickstart/docker-compose.quickstart.yml \
  -f monitoring/docker-compose.monitoring.yml \
  pull && \
docker-compose -p datahub \
  -f quickstart/docker-compose.quickstart.yml \
  -f monitoring/docker-compose.monitoring.yml \
  up
```

We set up quickstart.sh, dev.sh, and dev-without-neo4j.sh to add the above docker-compose when MONITORING=true. For
instance `MONITORING=true ./docker/quickstart.sh` will add the correct env variables to start collecting traces and
metrics, and also deploy Jaeger, Prometheus, and Grafana. We will soon support this as a flag during quickstart.

## Health check endpoint

For monitoring healthiness of your DataHub service, `/admin` endpoint can be used.
