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

   **Current State:** DropWizard metrics exposed via JMX, collected by Prometheus
   **Future Direction:** Native Micrometer integration for Spring-based metrics
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

The GraphQL instrumentation is implemented through `GraphQLTimingInstrumentation`, which extends GraphQL Java's instrumentation framework. It provides:

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

## Micrometer Transition Plan

DataHub is undertaking a strategic transition from DropWizard metrics (exposed via JMX) to Micrometer, a modern vendor-neutral metrics facade.
This transition aims to provide better cloud-native monitoring capabilities while maintaining backward compatibility for existing
monitoring infrastructure.

### Current State

What We Have Now:

- Primary System: DropWizard metrics exposed through JMX
- Collection Method: Prometheus-JMX exporter scrapes JMX metrics
- Dashboards: Grafana dashboards consuming JMX-sourced metrics
- Code Pattern: MetricUtils class for creating counters and timers
- Integration: Basic Spring integration with manual metric creation

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

   **Decision:** Run both systems in parallel with tag-based routing

   **Rationale:**

   - Zero downtime or disruption
   - Gradual migration at component level
   - Easy rollback if issues arise

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
