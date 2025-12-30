# OpenTelemetry Monitoring for datahub-integrations-service

**Status**: Draft
**Author**: Claude Code
**Date**: 2025-12-22
**Scope**: datahub-integrations-service ONLY (Python FastAPI microservice)
**Out of Scope**: Other DataHub services (metadata-service, frontend, etc.) - those already have monitoring

---

## Executive Summary

This design document outlines the implementation of OpenTelemetry (OTEL) observability **exclusively for the `datahub-integrations-service`**, a Python FastAPI microservice.

**Important**: This does NOT cover other DataHub services:

- ✅ **In Scope**: datahub-integrations-service (Python, port 9003)
- ❌ **Out of Scope**: metadata-service (Java) - already has Micrometer + OTEL
- ❌ **Out of Scope**: datahub-frontend (React) - different monitoring approach
- ❌ **Out of Scope**: Other Java services - already instrumented

The implementation will follow the patterns established in the Java `metadata-service` which already uses Micrometer with OTEL bridges, ensuring consistency across the DataHub platform.

### Goals

1. **Expose infrastructure metrics** via Prometheus-compatible `/metrics` endpoint
2. **Enable multi-backend support** via OpenTelemetry Protocol (OTLP)
3. **Maintain consistency** with existing Java service monitoring patterns
4. **Provide distributed tracing** capabilities for future integration
5. **Support major observability platforms** (Datadog, Grafana, Observe.ai, CloudWatch, etc.)

### Non-Goals

- **Modifying other DataHub services**: metadata-service, datahub-frontend, etc. (already have monitoring)
- Replacing existing business telemetry (Mixpanel/DataHub API events)
- Modifying Loguru structured logging setup
- Changing Sentry error tracking integration
- Platform-wide observability strategy (this is service-specific)

---

## Background

### Current State

**datahub-integrations-service**:

- Python 3.11+ FastAPI application (port 9003)
- No infrastructure metrics (only logs and business telemetry)
- Key components: Actions system, Slack/Teams integration, Gen AI, Analytics engine

**metadata-service** (Java):

- Comprehensive Micrometer + OpenTelemetry setup
- Dual registry strategy (JMX legacy + Prometheus modern)
- Exposed at `/actuator/prometheus` (unauthenticated)
- OTLP exporter support with tracing bridge

### Architecture Alignment

The Python implementation will mirror the Java service patterns:

- **Metrics endpoint**: Public, unauthenticated metrics exposure
- **Tag-based dimensionality**: Similar to Java's tagged metrics
- **SLO-based bucketing**: Percentiles + custom histogram buckets
- **High cardinality prevention**: Wildcard patterns for dynamic paths

---

## Architecture

### Component Diagram

```
┌─────────────────────────────────────────────────────────────┐
│          datahub-integrations-service (FastAPI)             │
│                                                             │
│  ┌────────────────────────────────────────────────────┐    │
│  │  Application Code (server.py, routers, etc.)      │    │
│  │                                                    │    │
│  │  ┌──────────────────────────────────────────┐     │    │
│  │  │  OpenTelemetry Instrumentation          │     │    │
│  │  │  - FastAPI auto-instrumentation         │     │    │
│  │  │  - Custom metrics (actions, slack, ai)  │     │    │
│  │  │  - HTTP client instrumentation          │     │    │
│  │  └──────────────────────────────────────────┘     │    │
│  └────────────────────────────────────────────────────┘    │
│                                                             │
│  ┌────────────────────────────────────────────────────┐    │
│  │  OpenTelemetry SDK (Python)                       │    │
│  │                                                    │    │
│  │  ┌─────────────┐  ┌──────────────┐               │    │
│  │  │ MeterProvider│  │TracerProvider│               │    │
│  │  └──────┬───────┘  └──────┬───────┘               │    │
│  │         │                 │                        │    │
│  │  ┌──────▼─────────────────▼─────┐                 │    │
│  │  │   Resource (service.name,    │                 │    │
│  │  │   service.version, etc.)     │                 │    │
│  │  └──────────────────────────────┘                 │    │
│  └────────────────────────────────────────────────────┘    │
│                                                             │
│  ┌────────────────────────────────────────────────────┐    │
│  │  Metric Readers & Exporters                        │    │
│  │                                                     │    │
│  │  ┌──────────────────┐  ┌──────────────────────┐   │    │
│  │  │ PrometheusMetric │  │ PeriodicExporting    │   │    │
│  │  │ Reader           │  │ MetricReader         │   │    │
│  │  │ (pull-based)     │  │ + OTLPMetricExporter │   │    │
│  │  │                  │  │ (push-based)         │   │    │
│  │  └────────┬─────────┘  └──────────┬───────────┘   │    │
│  └───────────┼─────────────────────────┼──────────────┘    │
└──────────────┼─────────────────────────┼───────────────────┘
               │                         │
               │                         │
    ┌──────────▼───────────┐   ┌─────────▼──────────────┐
    │  GET /metrics        │   │  OTLP Collector        │
    │  (Prometheus format) │   │  (configurable)        │
    │  - Scrape endpoint   │   │  - gRPC/HTTP push      │
    │  - No auth required  │   │  - Batched exports     │
    └──────────┬───────────┘   └─────────┬──────────────┘
               │                         │
               └────────┬────────────────┘
                        │
         ┌──────────────▼──────────────────────────┐
         │  Observability Backends                 │
         │  - Prometheus/Grafana                   │
         │  - Datadog (via agent or OTLP)          │
         │  - Observe.ai (OTLP)                    │
         │  - AWS CloudWatch                       │
         │  - Splunk, New Relic, Honeycomb, etc.   │
         └─────────────────────────────────────────┘
```

### Technology Stack

| Component                   | Library                                  | Version           | Purpose                        |
| --------------------------- | ---------------------------------------- | ----------------- | ------------------------------ |
| **OTEL SDK**                | `opentelemetry-sdk`                      | Latest            | Core SDK for metrics/traces    |
| **OTEL API**                | `opentelemetry-api`                      | Latest            | Public API for instrumentation |
| **FastAPI Instrumentation** | `opentelemetry-instrumentation-fastapi`  | Latest            | Auto-instrument HTTP endpoints |
| **HTTPX Instrumentation**   | `opentelemetry-instrumentation-httpx`    | Latest            | Outbound HTTP client metrics   |
| **Prometheus Exporter**     | `opentelemetry-exporter-prometheus`      | Latest            | Expose `/metrics` endpoint     |
| **OTLP Exporter**           | `opentelemetry-exporter-otlp-proto-grpc` | Latest            | Push to OTLP collectors        |
| **Resource Detector**       | `opentelemetry-sdk-extension-aws`        | Latest (optional) | AWS environment detection      |

---

## Metrics to Collect

### 1. HTTP API Metrics (Auto-instrumented)

Via `opentelemetry-instrumentation-fastapi`:

| Metric Name                    | Type          | Labels                                          | Description                   |
| ------------------------------ | ------------- | ----------------------------------------------- | ----------------------------- |
| `http_server_request_duration` | Histogram     | `http.method`, `http.route`, `http.status_code` | Request latency distribution  |
| `http_server_active_requests`  | UpDownCounter | `http.method`, `http.route`                     | Concurrent requests in flight |
| `http_server_request_size`     | Histogram     | `http.method`, `http.route`                     | Request body size             |
| `http_server_response_size`    | Histogram     | `http.method`, `http.route`, `http.status_code` | Response body size            |

**Buckets**: Custom SLO-based (50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s)

### 2. Actions System Metrics (Custom)

Location: `src/datahub_integrations/actions/actions_manager.py`

| Metric Name                       | Type      | Labels                  | Description                                               |
| --------------------------------- | --------- | ----------------------- | --------------------------------------------------------- |
| `actions_execution_duration`      | Histogram | `action_type`, `status` | Time to execute action                                    |
| `actions_execution_total`         | Counter   | `action_type`, `status` | Total action executions                                   |
| `actions_queue_depth`             | Gauge     | `stage`                 | Actions waiting in queue                                  |
| `actions_pipeline_status`         | Gauge     | `action_id`, `status`   | Current pipeline state (0=failed, 1=running, 2=succeeded) |
| `actions_venv_creation_duration`  | Histogram | -                       | Virtual environment setup time                            |
| `actions_subprocess_spawns_total` | Counter   | `action_type`           | Subprocess spawn count                                    |

**Action Types**: `webhook`, `slack`, `teams`, `email`, `transform`, `metadata_change`
**Statuses**: `init`, `running`, `succeeded`, `failed`
**Stages**: `scheduled`, `executing`, `completed`

### 3. Slack Integration Metrics (Custom)

Location: `src/datahub_integrations/slack/`

| Metric Name                    | Type      | Labels                   | Description             |
| ------------------------------ | --------- | ------------------------ | ----------------------- |
| `slack_command_duration`       | Histogram | `command`, `success`     | Command execution time  |
| `slack_command_total`          | Counter   | `command`, `status_code` | Total commands executed |
| `slack_oauth_flow_duration`    | Histogram | `step`                   | OAuth flow latency      |
| `slack_oauth_flow_total`       | Counter   | `success`                | OAuth attempts          |
| `slack_entity_render_duration` | Histogram | `entity_type`            | Entity rendering time   |
| `slack_search_duration`        | Histogram | `query_type`             | Search query latency    |

**Commands**: `search`, `get`, `ask`
**OAuth Steps**: `initiate`, `callback`, `token_exchange`
**Entity Types**: `dataset`, `dashboard`, `chart`, `user`, `group`, `term`

### 4. Gen AI Metrics (Custom)

Location: `src/datahub_integrations/gen_ai/`

| Metric Name                             | Type      | Labels                           | Description                         |
| --------------------------------------- | --------- | -------------------------------- | ----------------------------------- |
| `genai_llm_call_duration`               | Histogram | `model`, `provider`, `operation` | LLM API call latency                |
| `genai_llm_call_total`                  | Counter   | `model`, `provider`, `status`    | Total LLM API calls                 |
| `genai_llm_tokens_total`                | Counter   | `model`, `token_type`            | Tokens consumed (prompt/completion) |
| `genai_cache_hit_total`                 | Counter   | `cache_type`                     | Cache hits                          |
| `genai_cache_miss_total`                | Counter   | `cache_type`                     | Cache misses                        |
| `genai_description_generation_duration` | Histogram | `entity_type`, `success`         | Description generation time         |

**Models**: `gpt-4`, `claude-3-5-sonnet`, `bedrock/anthropic.claude-v2`, etc.
**Providers**: `openai`, `anthropic`, `bedrock`, `azure`
**Operations**: `describe`, `suggest_terms`, `infer_query_description`
**Token Types**: `prompt`, `completion`

### 5. Analytics Engine Metrics (Custom)

Location: `src/datahub_integrations/analytics/`

| Metric Name                           | Type      | Labels                | Description              |
| ------------------------------------- | --------- | --------------------- | ------------------------ |
| `analytics_query_duration`            | Histogram | `platform`, `success` | Query execution time     |
| `analytics_query_total`               | Counter   | `platform`, `status`  | Total queries executed   |
| `analytics_cache_resolution_duration` | Histogram | `platform`            | S3 URI cache lookup time |
| `analytics_preview_rows_total`        | Counter   | `platform`            | Total rows returned      |

**Platforms**: `bigquery`, `snowflake`, `duckdb`, `s3`

### 6. External Dependency Metrics (Custom)

| Metric Name                      | Type      | Labels                              | Description             |
| -------------------------------- | --------- | ----------------------------------- | ----------------------- |
| `datahub_gms_request_duration`   | Histogram | `endpoint`, `method`, `status_code` | GMS API latency         |
| `datahub_gms_request_total`      | Counter   | `endpoint`, `method`, `status_code` | Total GMS requests      |
| `kafka_message_produce_duration` | Histogram | `topic`                             | Kafka produce latency   |
| `kafka_message_produce_total`    | Counter   | `topic`, `success`                  | Total messages produced |

### 7. System Resource Metrics (Auto-instrumented)

Via `opentelemetry-instrumentation-system-metrics`:

| Metric Name                    | Type    | Description          |
| ------------------------------ | ------- | -------------------- |
| `system_cpu_utilization`       | Gauge   | CPU usage percentage |
| `system_memory_usage`          | Gauge   | Memory usage bytes   |
| `process_runtime_memory_usage` | Gauge   | Process memory (RSS) |
| `process_runtime_cpu_time`     | Counter | Process CPU time     |

---

## Implementation Plan

### Phase 1: Foundation (Week 1)

**Goal**: Basic OTEL setup with HTTP metrics

1. **Add Dependencies** (`pyproject.toml`)

   ```toml
   [project.dependencies]
   opentelemetry-api = "^1.22.0"
   opentelemetry-sdk = "^1.22.0"
   opentelemetry-instrumentation-fastapi = "^0.43b0"
   opentelemetry-instrumentation-httpx = "^0.43b0"
   opentelemetry-exporter-prometheus = "^1.22.0"
   opentelemetry-exporter-otlp-proto-grpc = "^1.22.0"
   ```

2. **Create OTEL Configuration Module** (`src/datahub_integrations/observability/otel_config.py`)

   - Initialize `MeterProvider` with resource attributes
   - Configure Prometheus exporter (pull-based)
   - Configure OTLP exporter (push-based, optional)
   - Register instrumentation for FastAPI and HTTPX

3. **Expose `/metrics` Endpoint** (`src/datahub_integrations/server.py`)

   - Add route handler using Prometheus exporter's WSGI app
   - Exclude from authentication (similar to Java's `/actuator/prometheus`)
   - Add to health check documentation

4. **Environment Configuration**

   - `OTEL_SERVICE_NAME` (default: `datahub-integrations-service`)
   - `OTEL_SERVICE_VERSION` (from package version)
   - `OTEL_EXPORTER_OTLP_ENDPOINT` (optional, for push mode)
   - `OTEL_EXPORTER_OTLP_PROTOCOL` (grpc/http)
   - `OTEL_METRICS_EXEMPLARS_ENABLED` (link metrics to traces)

5. **Testing**
   - Unit tests for OTEL initialization
   - Integration test hitting `/metrics` endpoint
   - Validate Prometheus text format output

**Success Criteria**:

- `/metrics` endpoint returns valid Prometheus format
- HTTP request metrics auto-collected
- No performance degradation (< 1ms overhead per request)

### Phase 2: Custom Metrics (Week 2)

**Goal**: Instrument high-priority components

1. **Actions System Instrumentation**

   - Wrap `run_action()` in `ActionsManager`
   - Record execution duration and status
   - Track queue depth on schedule/complete
   - Monitor venv creation in `setup_venv()`

2. **Slack Integration Instrumentation**

   - Wrap command handlers in `slack/router.py`
   - Track OAuth flow in `slack/oauth.py`
   - Monitor search/get/ask latency
   - Entity rendering metrics in `slack/render/`

3. **Gen AI Instrumentation**

   - Wrap LLM calls in `gen_ai/description_generator.py`
   - Track token usage via LiteLLM callbacks
   - Monitor cache hit rates
   - Track Bedrock invocations

4. **Testing**
   - Unit tests for each instrumented component
   - Mock metric collection in tests
   - Validate metric labels and values

**Success Criteria**:

- Actions execution metrics visible in `/metrics`
- Slack command metrics captured
- Gen AI token usage tracked
- All metrics follow naming conventions

### Phase 3: Distributed Tracing (Week 3)

**Goal**: Enable end-to-end trace correlation

1. **Add Tracing Dependencies**

   ```toml
   opentelemetry-instrumentation-logging = "^0.43b0"
   ```

2. **Configure TracerProvider**

   - Initialize alongside MeterProvider
   - Configure OTLP trace exporter
   - Enable W3C Trace Context propagation

3. **Integrate with Loguru**

   - Add trace_id/span_id to log context
   - Configure sampler (head-based, 10% default)

4. **Add Trace Spans**
   - Wrap critical operations (actions, AI calls)
   - Add span attributes (action_type, entity_urn, etc.)
   - Link exceptions to spans

**Success Criteria**:

- Traces exported via OTLP
- Logs contain trace/span IDs
- Spans visible in Grafana/Jaeger

### Phase 4: Advanced Features (Week 4)

**Goal**: Production-ready hardening

1. **High Cardinality Prevention**

   - Implement path wildcard replacement (e.g., `/users/123` → `/users/*`)
   - Limit label value cardinality (top N + "other")
   - Add metric filtering configuration

2. **SLO-Based Bucketing**

   - Define custom histogram buckets per metric type
   - Configure percentiles (p50, p95, p99, p99.9)
   - Align with Java service SLOs

3. **Graceful Degradation**

   - Handle OTLP collector unavailability
   - Circuit breaker for metric collection overhead
   - Fallback to logging on export failures

4. **Performance Optimization**

   - Batch metric exports
   - Async export to avoid blocking
   - Memory-efficient aggregation

5. **Documentation**
   - Update README with metrics documentation
   - Add Grafana dashboard JSON
   - Document environment variables

**Success Criteria**:

- No service disruption if OTLP collector down
- < 1% CPU overhead from metrics collection
- Production-ready documentation

---

## Configuration

### Environment Variables

```bash
# Service Identification
OTEL_SERVICE_NAME=datahub-integrations-service
OTEL_SERVICE_VERSION=auto  # Read from package
OTEL_SERVICE_NAMESPACE=datahub

# Resource Attributes
OTEL_RESOURCE_ATTRIBUTES="deployment.environment=production,service.instance.id=pod-123"

# Prometheus Exporter (Pull-based)
OTEL_PROMETHEUS_EXPORTER_ENABLED=true
OTEL_PROMETHEUS_EXPORTER_PORT=9003  # Share with FastAPI port, different route

# OTLP Exporter (Push-based)
OTEL_EXPORTER_OTLP_ENABLED=false  # Optional
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
OTEL_EXPORTER_OTLP_PROTOCOL=grpc
OTEL_EXPORTER_OTLP_TIMEOUT=10000  # ms
OTEL_EXPORTER_OTLP_HEADERS="api-key=secret"

# Metrics Configuration
OTEL_METRICS_EXEMPLARS_ENABLED=true  # Link metrics to traces
OTEL_METRIC_EXPORT_INTERVAL=60000    # ms (1 minute)
OTEL_METRIC_EXPORT_TIMEOUT=30000     # ms

# Tracing Configuration
OTEL_TRACES_SAMPLER=parentbased_traceidratio
OTEL_TRACES_SAMPLER_ARG=0.1  # 10% sampling
OTEL_PROPAGATORS=tracecontext,baggage

# Custom Configuration
DATAHUB_METRICS_HIGH_CARDINALITY_LIMIT=100
DATAHUB_METRICS_PERCENTILES=0.5,0.95,0.99,0.999
DATAHUB_METRICS_SLO_BUCKETS_MS=50,100,250,500,1000,2500,5000,10000
```

### Configuration File (`src/datahub_integrations/observability/config.py`)

```python
from pydantic_settings import BaseSettings

class ObservabilityConfig(BaseSettings):
    """OTEL configuration matching Java metadata-service patterns."""

    # Service identification
    service_name: str = "datahub-integrations-service"
    service_version: str = "auto"
    service_namespace: str = "datahub"

    # Prometheus exporter
    prometheus_enabled: bool = True
    prometheus_port: int | None = None  # Share with FastAPI

    # OTLP exporter
    otlp_enabled: bool = False
    otlp_endpoint: str = "http://localhost:4317"
    otlp_protocol: str = "grpc"
    otlp_timeout_ms: int = 10000

    # Metrics configuration
    metrics_export_interval_ms: int = 60000
    high_cardinality_limit: int = 100
    percentiles: list[float] = [0.5, 0.95, 0.99, 0.999]
    slo_buckets_ms: list[int] = [50, 100, 250, 500, 1000, 2500, 5000, 10000]

    # Tracing configuration
    tracing_enabled: bool = True
    traces_sample_rate: float = 0.1  # 10%

    class Config:
        env_prefix = "OTEL_"
```

---

## Integration Points

### 1. Authentication Exclusion

Update `src/datahub_integrations/server.py` to exclude `/metrics`:

```python
# Similar to Java's application.yaml:
# authentication:
#   excludedPaths: /actuator/prometheus

# In FastAPI middleware or auth decorator
EXCLUDED_PATHS = [
    "/ping",
    "/docs",
    "/openapi.json",
    "/metrics",  # Add this
]
```

### 2. Docker Configuration

Update `Dockerfile` to expose metrics (if needed):

```dockerfile
# Metrics endpoint shares port 9003 with main app
EXPOSE 9003

# Optional: Separate metrics port
# EXPOSE 9090
```

### 3. Kubernetes Configuration

Update Kubernetes manifests for Prometheus scraping:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: datahub-integrations-service
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/path: "/metrics"
    prometheus.io/port: "9003"
spec:
  ports:
    - name: http
      port: 9003
      targetPort: 9003
```

### 4. Datadog Integration

**Option A: Prometheus Scraping**

```yaml
# datadog-agent ConfigMap
apiVersion: v1
kind: ConfigMap
metadata:
  name: datadog-config
data:
  prometheus.yaml: |
    init_config:
    instances:
      - prometheus_url: http://datahub-integrations-service:9003/metrics
        namespace: datahub
        metrics:
          - "http_server_*"
          - "actions_*"
          - "slack_*"
          - "genai_*"
```

**Option B: OTLP Push to Datadog Agent**

```bash
# Environment variables for OTLP
OTEL_EXPORTER_OTLP_ENABLED=true
OTEL_EXPORTER_OTLP_ENDPOINT=http://datadog-agent:4317
OTEL_RESOURCE_ATTRIBUTES="service.name=datahub-integrations-service"
```

---

## Testing Strategy

### Unit Tests

**File**: `tests/unit/observability/test_otel_config.py`

```python
def test_meter_provider_initialization():
    """Verify MeterProvider created with correct resource."""
    config = ObservabilityConfig(service_name="test-service")
    provider = setup_meter_provider(config)

    resource = provider.resource
    assert resource.attributes["service.name"] == "test-service"
    assert "service.version" in resource.attributes

def test_prometheus_exporter_enabled():
    """Verify Prometheus exporter registered when enabled."""
    config = ObservabilityConfig(prometheus_enabled=True)
    provider = setup_meter_provider(config)

    # Check PrometheusMetricReader in readers
    readers = provider._readers
    assert any(isinstance(r, PrometheusMetricReader) for r in readers)

def test_custom_histogram_buckets():
    """Verify SLO-based buckets applied to histograms."""
    config = ObservabilityConfig(slo_buckets_ms=[50, 100, 500])
    view = create_duration_view("http_server_duration", config)

    # Verify explicit bucket boundaries
    assert view._aggregation._boundaries == [0.05, 0.1, 0.5]  # seconds
```

### Integration Tests

**File**: `tests/integration/test_metrics_endpoint.py`

```python
@pytest.mark.asyncio
async def test_metrics_endpoint_exposed(test_client):
    """Verify /metrics returns Prometheus format."""
    response = await test_client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/plain; charset=utf-8"

    # Check for expected metrics
    body = response.text
    assert "http_server_request_duration" in body
    assert "TYPE" in body  # Prometheus metadata

@pytest.mark.asyncio
async def test_http_metrics_collected(test_client, meter_provider):
    """Verify HTTP request metrics recorded."""
    # Make test request
    response = await test_client.get("/ping")
    assert response.status_code == 200

    # Force metric collection
    meter_provider.force_flush()

    # Read metrics
    metrics_response = await test_client.get("/metrics")
    body = metrics_response.text

    # Verify metric exists with correct labels
    assert 'http_server_request_duration{http_method="GET",http_route="/ping"' in body
```

### Load Testing

**File**: `tests/load/test_metrics_overhead.py`

```python
def test_metrics_overhead_acceptable():
    """Verify metrics add < 1% latency overhead."""

    # Baseline: No metrics
    with disable_metrics():
        baseline_p99 = run_load_test(requests_per_sec=1000, duration_sec=60)

    # With metrics enabled
    with_metrics_p99 = run_load_test(requests_per_sec=1000, duration_sec=60)

    overhead_percent = ((with_metrics_p99 - baseline_p99) / baseline_p99) * 100
    assert overhead_percent < 1.0, f"Overhead {overhead_percent:.2f}% exceeds 1%"
```

---

## Monitoring and Alerting

### Key Metrics to Alert On

| Alert                    | Condition                                                                | Severity | Action                                |
| ------------------------ | ------------------------------------------------------------------------ | -------- | ------------------------------------- |
| **High Error Rate**      | `rate(http_server_request_duration{http_status_code=~"5.."}[5m]) > 0.05` | Critical | Check logs, rollback if recent deploy |
| **Action Failure Spike** | `rate(actions_execution_total{status="failed"}[5m]) > 0.1`               | High     | Check action logs, investigate errors |
| **LLM Latency High**     | `histogram_quantile(0.99, genai_llm_call_duration) > 10s`                | Medium   | Check LLM provider status             |
| **GMS Unavailable**      | `rate(datahub_gms_request_total{status_code=~"5.."}[5m]) > 0.5`          | Critical | Check GMS service health              |
| **Memory Leak**          | `process_runtime_memory_usage > 2GB` (increasing over time)              | High     | Restart pod, investigate memory leak  |

### Sample Prometheus Alert Rules

```yaml
groups:
  - name: datahub-integrations-service
    interval: 30s
    rules:
      - alert: HighErrorRate
        expr: |
          rate(http_server_request_duration_count{http_status_code=~"5..", service_name="datahub-integrations-service"}[5m])
          / rate(http_server_request_duration_count{service_name="datahub-integrations-service"}[5m]) > 0.05
        for: 5m
        labels:
          severity: critical
          service: datahub-integrations-service
        annotations:
          summary: "High HTTP error rate detected"
          description: "{{ $value | humanizePercentage }} of requests are failing"

      - alert: ActionExecutionFailures
        expr: |
          rate(actions_execution_total{status="failed", service_name="datahub-integrations-service"}[10m]) > 0.1
        for: 10m
        labels:
          severity: high
          service: datahub-integrations-service
        annotations:
          summary: "Frequent action execution failures"
          description: "Action failure rate: {{ $value }} failures/sec"

      - alert: LLMHighLatency
        expr: |
          histogram_quantile(0.99, rate(genai_llm_call_duration_bucket{service_name="datahub-integrations-service"}[5m])) > 10
        for: 15m
        labels:
          severity: medium
          service: datahub-integrations-service
        annotations:
          summary: "LLM API calls are slow"
          description: "P99 latency: {{ $value }}s"
```

---

## Grafana Dashboard

### Dashboard Structure

1. **Overview Panel**

   - Request rate (req/sec)
   - Error rate (%)
   - P50/P95/P99 latency
   - Active connections

2. **HTTP API Panel**

   - Requests by endpoint (top 10)
   - Status code distribution
   - Request/response size histograms
   - Slowest endpoints (P99)

3. **Actions System Panel**

   - Actions execution rate by type
   - Success/failure ratio
   - Queue depth over time
   - Execution duration heatmap

4. **Slack Integration Panel**

   - Commands per minute
   - Command success rate
   - OAuth flow success rate
   - Search latency distribution

5. **Gen AI Panel**

   - LLM calls by provider/model
   - Token usage rate
   - Cache hit ratio
   - Cost estimation (tokens × pricing)

6. **External Dependencies Panel**

   - GMS API latency (P50/P95/P99)
   - GMS error rate
   - Kafka produce latency

7. **System Resources Panel**
   - CPU utilization
   - Memory usage (RSS)
   - Python GC stats

**Sample Queries**:

```promql
# Request rate
rate(http_server_request_duration_count{service_name="datahub-integrations-service"}[5m])

# Error percentage
sum(rate(http_server_request_duration_count{http_status_code=~"5..",service_name="datahub-integrations-service"}[5m]))
/ sum(rate(http_server_request_duration_count{service_name="datahub-integrations-service"}[5m])) * 100

# P99 latency by endpoint
histogram_quantile(0.99, sum by(http_route, le) (rate(http_server_request_duration_bucket{service_name="datahub-integrations-service"}[5m])))

# Action execution rate by type and status
sum by(action_type, status) (rate(actions_execution_total{service_name="datahub-integrations-service"}[5m]))

# LLM token usage rate
rate(genai_llm_tokens_total{service_name="datahub-integrations-service"}[5m])
```

---

## Security Considerations

### 1. Metrics Endpoint Security

**Current Decision**: Follow Java service pattern (unauthenticated `/metrics`)

**Rationale**:

- Prometheus scraping requires unauthenticated access
- Metrics don't contain sensitive data (aggregate stats only)
- Network security (VPC, security groups) provides isolation

**Alternative**: If sensitive labels needed (e.g., user_id, customer_id):

- Use metric relabeling to drop sensitive labels
- Implement separate authenticated metrics endpoint
- Use Prometheus remote write with auth

### 2. High Cardinality Prevention

**Risk**: Label explosion causes memory/performance issues

**Mitigations**:

- **Path normalization**: Replace IDs with wildcards (`/users/123` → `/users/*`)
- **Label allowlists**: Only permit known values (e.g., 20 action types)
- **Cardinality limits**: Drop metrics exceeding threshold (100 unique values)
- **Aggregation**: Use coarser labels (e.g., `status_class="5xx"` vs `status_code="503"`)

### 3. Sensitive Data in Traces

**Risk**: Span attributes may capture PII (URNs, usernames)

**Mitigations**:

- **Span processors**: Redact sensitive attributes before export
- **Sampling**: Only trace 10% of requests (head-based sampling)
- **Attribute filtering**: Blocklist patterns (email, SSN, API keys)

---

## Rollout Plan

### Stage 1: Canary Deployment (Week 1)

- Deploy to single pod in staging environment
- Enable Prometheus exporter only (no OTLP)
- Monitor for errors/performance degradation
- Validate metrics via `curl http://localhost:9003/metrics`

**Success Criteria**:

- No increase in error rate or latency
- Metrics endpoint responds < 100ms
- Memory usage increase < 50MB

### Stage 2: Staging Full Rollout (Week 2)

- Deploy to all staging pods
- Configure Prometheus scraping
- Create Grafana dashboards
- Run load tests (1000 req/sec for 1 hour)

**Success Criteria**:

- All pods healthy
- Dashboards show expected metrics
- No performance regression

### Stage 3: Production Gradual Rollout (Week 3)

- Deploy to 10% of production pods (canary)
- Monitor for 48 hours
- Gradually increase to 50%, then 100%
- Enable OTLP exporter (push to DataDog/Observe.ai)

**Success Criteria**:

- No increase in P99 latency
- No OOM errors
- Observability platform shows metrics

### Stage 4: Advanced Features (Week 4)

- Enable distributed tracing (10% sampling)
- Configure alerting rules
- Tune SLO buckets based on production data
- Document runbooks

**Success Criteria**:

- Traces visible in observability platform
- Alerts firing correctly
- Documentation complete

---

## Maintenance and Operations

### Routine Tasks

1. **Weekly**: Review high cardinality metrics, adjust filters
2. **Monthly**: Tune SLO buckets based on actual latency distribution
3. **Quarterly**: Review and archive obsolete metrics

### Troubleshooting Guide

| Issue                     | Diagnosis                          | Solution                                            |
| ------------------------- | ---------------------------------- | --------------------------------------------------- |
| **Metrics not appearing** | Check `/metrics` endpoint directly | Verify OTEL initialization, check logs for errors   |
| **High memory usage**     | Profile with `tracemalloc`         | Reduce metric cardinality, increase export interval |
| **OTLP export failures**  | Check collector connectivity       | Verify endpoint URL, check network policies         |
| **Missing labels**        | Check instrumenter code            | Ensure labels added before metric recorded          |

---

## Cost Analysis

### Resource Overhead

| Component                     | CPU Overhead       | Memory Overhead      | Network Bandwidth   |
| ----------------------------- | ------------------ | -------------------- | ------------------- |
| **Prometheus Exporter**       | < 0.5%             | ~20MB                | 0 (pull-based)      |
| **OTLP Exporter**             | < 1%               | ~50MB                | ~10KB/sec (batched) |
| **FastAPI Instrumentation**   | < 0.2% per request | ~10MB                | 0                   |
| **Custom Metrics**            | < 0.1% per metric  | ~1MB per 1000 series | 0                   |
| **Total (Prometheus only)**   | ~1%                | ~30MB                | Negligible          |
| **Total (Prometheus + OTLP)** | ~2%                | ~80MB                | ~10KB/sec           |

### Observability Platform Costs (Estimated)

Assuming 1000 req/sec, 50 actions/min, 100 LLM calls/min:

| Platform                     | Monthly Cost        | Notes                 |
| ---------------------------- | ------------------- | --------------------- |
| **Grafana Cloud (Free)**     | $0                  | 10K series, 50GB logs |
| **Datadog**                  | ~$30-50/host        | APM + metrics + logs  |
| **Observe.ai**               | Custom              | OTLP native           |
| **Prometheus (self-hosted)** | Infrastructure only | ~$20/month (storage)  |

---

## Success Metrics

### Technical Metrics

- **Performance**: < 2% CPU overhead, < 100MB memory overhead
- **Reliability**: 99.9% metrics export success rate
- **Coverage**: 80% of critical code paths instrumented

### Operational Metrics

- **MTTR (Mean Time to Resolution)**: Reduce by 50% with metrics-driven debugging
- **Incident Detection**: 90% of incidents detected via alerts (vs manual discovery)
- **Debugging Efficiency**: Reduce investigation time from hours to minutes

### Business Metrics

- **Cost Optimization**: Identify and optimize expensive LLM calls (> $X/month)
- **SLA Compliance**: Achieve 99.5% uptime with SLO monitoring
- **User Satisfaction**: Improve perceived performance via latency optimization

---

## References

### Documentation

- [OpenTelemetry Python Docs](https://opentelemetry.io/docs/instrumentation/python/)
- [Prometheus Best Practices](https://prometheus.io/docs/practices/)
- [FastAPI OpenTelemetry Guide](https://opentelemetry.io/docs/instrumentation/python/automatic/example/)
- [DataHub Metadata Service OTEL Implementation](../metadata-service/factories/src/main/java/com/linkedin/gms/factory/system_telemetry/)

### Related Issues/PRs

- [TODO: Link to implementation PR]
- [TODO: Link to Grafana dashboard JSON]

### Standards and Conventions

- [OpenTelemetry Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/)
- [Prometheus Naming Best Practices](https://prometheus.io/docs/practices/naming/)
- [W3C Trace Context Specification](https://www.w3.org/TR/trace-context/)

---

## Appendix A: Code Snippets

### OTEL Initialization

```python
# src/datahub_integrations/observability/otel_config.py

from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource

def setup_meter_provider(config: ObservabilityConfig) -> MeterProvider:
    """Initialize MeterProvider with Prometheus exporter."""

    # Create resource with service metadata
    resource = Resource.create({
        "service.name": config.service_name,
        "service.version": config.service_version,
        "service.namespace": config.service_namespace,
        "telemetry.sdk.language": "python",
        "telemetry.sdk.name": "opentelemetry",
    })

    # Create metric readers
    readers = []

    if config.prometheus_enabled:
        readers.append(PrometheusMetricReader())

    if config.otlp_enabled:
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

        otlp_exporter = OTLPMetricExporter(
            endpoint=config.otlp_endpoint,
            timeout=config.otlp_timeout_ms // 1000,
        )
        readers.append(PeriodicExportingMetricReader(
            otlp_exporter,
            export_interval_millis=config.metrics_export_interval_ms,
        ))

    # Create provider
    provider = MeterProvider(resource=resource, metric_readers=readers)
    metrics.set_meter_provider(provider)

    return provider
```

### FastAPI Integration

```python
# src/datahub_integrations/server.py

from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from prometheus_client import make_asgi_app

# Initialize OTEL before FastAPI app creation
setup_meter_provider(observability_config)

app = FastAPI(title="DataHub Integrations Service")

# Auto-instrument FastAPI
FastAPIInstrumentor.instrument_app(app)

# Add /metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)
```

### Custom Metrics

```python
# src/datahub_integrations/actions/metrics.py

from opentelemetry import metrics

meter = metrics.get_meter(__name__)

# Define metrics
actions_duration = meter.create_histogram(
    name="actions_execution_duration",
    description="Action execution duration in seconds",
    unit="s",
)

actions_total = meter.create_counter(
    name="actions_execution_total",
    description="Total action executions",
)

# Usage in ActionsManager
def run_action(self, action_spec: LiveActionSpec) -> ActionRun:
    start_time = time.time()

    try:
        result = self._execute_action(action_spec)
        status = "succeeded"
    except Exception as e:
        status = "failed"
        raise
    finally:
        duration = time.time() - start_time

        # Record metrics
        actions_duration.record(
            duration,
            {"action_type": action_spec.action_type, "status": status}
        )
        actions_total.add(
            1,
            {"action_type": action_spec.action_type, "status": status}
        )
```

---

## Appendix B: Comparison with Java Implementation

| Aspect                   | Java (metadata-service)                         | Python (datahub-integrations-service) |
| ------------------------ | ----------------------------------------------- | ------------------------------------- |
| **Framework**            | Micrometer + OTEL bridge                        | OpenTelemetry SDK native              |
| **Metrics Library**      | Micrometer (abstraction)                        | OTEL Metrics API (direct)             |
| **Endpoint**             | `/actuator/prometheus` (Spring Actuator)        | `/metrics` (Prometheus convention)    |
| **Export Strategy**      | Dual registry (JMX + Prometheus)                | Single registry (Prometheus + OTLP)   |
| **Configuration**        | `application.yaml` + `@ConfigurationProperties` | Environment variables + Pydantic      |
| **Auto-instrumentation** | Spring Boot Actuator + Micrometer               | `opentelemetry-instrumentation-*`     |
| **Tracing Bridge**       | Micrometer Observation API → OTEL               | Native OTEL TracerProvider            |
| **Legacy Support**       | Dropwizard metrics (JMX)                        | None (greenfield)                     |

**Key Difference**: Python implementation is simpler due to:

1. No legacy metrics to support
2. Native OTEL (no abstraction layer like Micrometer)
3. Smaller codebase with fewer integrations

**Advantage**: Cleaner, more modern implementation aligned with OTEL standards.

---

## Appendix C: Metric Naming Conventions

Follow Prometheus naming best practices:

### Rules

1. **Use snake_case**: `http_server_request_duration` (not camelCase)
2. **Include unit suffix**: `_seconds`, `_bytes`, `_total`, `_ratio`
3. **Namespace prefix**: `datahub_` for custom metrics (auto-added by resource)
4. **Verb last**: `http_requests_total` (not `total_http_requests`)

### Examples

| Metric Type   | Good                            | Bad                           |
| ------------- | ------------------------------- | ----------------------------- |
| **Counter**   | `actions_execution_total`       | `action_count` (missing unit) |
| **Gauge**     | `actions_queue_depth`           | `queue` (ambiguous)           |
| **Histogram** | `http_request_duration_seconds` | `request_time` (missing unit) |
| **Summary**   | `genai_tokens_total`            | `tokens` (missing \_total)    |

### Label Naming

1. **Lowercase with underscores**: `http_method`, `action_type`
2. **Avoid high cardinality**: No user IDs, URNs, timestamps
3. **Consistent naming**: `status_code` (not `code` or `http_status`)

---

**End of Design Document**
