# Monitoring DataHub

## Overview

DataHub uses **Micrometer** (Spring Boot Actuator) as the primary metrics path, **OpenTelemetry** for traces, and optional **Dropwizard/JMX** for legacy dashboards. This page summarizes **where** telemetry is exposed, the **canonical meter names and tags** for Prometheus, and **how** to configure deeper instrumentation (GraphQL, Kafka, hooks).

**Endpoints (typical Docker):**

| Signal                  | Where                                      | Port (default)                      |
| ----------------------- | ------------------------------------------ | ----------------------------------- |
| Prometheus scrape       | `http://<host>:<port>/actuator/prometheus` | **4319** (`MANAGEMENT_SERVER_PORT`) |
| JMX exporter (optional) | Prometheus scrapes JVM agent               | **4318** (`ENABLE_PROMETHEUS=true`) |
| Traces                  | OTLP / Jaeger per `OTEL_*` env             | varies                              |

GMS, MAE/MCE consumers, and Play set **`MANAGEMENT_SERVER_PORT=4319`** in container `start.sh`. Quickstart may **expose** 4319 on the Docker network without publishing to the host. See [Dropwizard & JMX](#dropwizard--jmx) and [example compose](../../docker/monitoring/docker-compose.monitoring.yml).

---

## Canonical metric taxonomy (Prometheus) {#canonical-metric-taxonomy-prometheus}

**Operator contract:** Prefer these logical names and tags for new dashboards and alerts. **Names and tags are not yet frozen** across releases; verify after upgrades ([Updating DataHub](../how/updating-datahub.md)). Legacy **`metrics_com_*`** JMX names remain the main backward-compatibility concern.

**Prometheus mapping:** Micrometer uses dots in code (e.g. `datahub.metadata_store`); Prometheus usually shows **underscores**, timer suffixes **`_count` / `_sum` / `_max` / `_bucket`**, and counters ending in **`_total`**. Full GMS scrape shapes and platform families: [Prometheus scrape reference (GMS Actuator)](#prometheus-scrape-reference-gms).

### Domain families (logical meters) {#metric-catalog}

| Domain                | Logical prefix / meter                            | Component        | Tags (high level)                                                                     | Notes                                                                                                                                                                               |
| --------------------- | ------------------------------------------------- | ---------------- | ------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| GraphQL               | `graphql.request.*`, `graphql.field.*` (optional) | GMS              | `operation`, `operation.type`, `success`, …                                           | Implemented by [`GraphQLTimingInstrumentation`](../../metadata-io/src/main/java/com/linkedin/metadata/system_telemetry/GraphQLTimingInstrumentation.java); not `datahub.graphql.*`. |
| OpenAPI / HTTP        | `http.server.requests`                            | GMS              | `method`, `status`, `uri`, …                                                          | OpenAPI is Spring MVC routes under `/openapi/**`—no separate `openapi.*` meter.                                                                                                     |
| Kafka consumer age    | `datahub.kafka.consumer.record_age`               | GMS, MCE, MAE, … | `topic`, `consumer.group`                                                             | `now - ConsumerRecord.timestamp()` at processing start.                                                                                                                             |
| MCL hook lag          | `datahub.mcl.hook.trace_lag`                      | GMS, MAE, …      | `hook`                                                                                | Trace timestamp in MCL system metadata → hook success.                                                                                                                              |
| Hook failures         | `datahub.mcl.hook.failures`                       | MAE              | `hook`, `consumer.group`                                                              | Prefer over per-class `*_failure` on JMX.                                                                                                                                           |
| Auth                  | `datahub.auth.login_outcomes`                     | GMS              | `operation`, `result`, `login_source`, `denial_reason`                                | Session / native login outcomes.                                                                                                                                                    |
| API traffic           | `datahub.api.traffic`                             | GMS              | `user_category`, `agent_class`, `request_api`, optional `agent_skill`, `agent_caller` | Actuator: `datahub_api_traffic_total`; optional `X-DataHub-Context`.                                                                                                                |
| Aspect validation     | `datahub.validation.aspect_size.*`                | GMS              | `aspectName`, `sizeBucket`, …                                                         | Optional; see [Aspect size validation](#aspect-size-validation-metrics).                                                                                                            |
| Metadata store        | `datahub.metadata_store`                          | GMS              | **`subsystem`**, **`operation`**                                                      | `subsystem=ebean` or `cassandra`.                                                                                                                                                   |
| Graph                 | `datahub.graph`                                   | GMS              | **`subsystem`**, **`operation`**                                                      | Today `subsystem=elasticsearch`; later `neo4j`.                                                                                                                                     |
| Timeseries aspects    | `datahub.timeseries_aspect`                       | GMS              | **`subsystem`**, **`operation`**                                                      | Elasticsearch-backed today.                                                                                                                                                         |
| Entity search + usage | `datahub.search`                                  | GMS              | **`subsystem`**, **`operation`**                                                      | Usage index uses `operation=usage_search`.                                                                                                                                          |
| Caches                | `cache.*`                                         | GMS, consumers   | `cache`, `name`, `result`, …                                                          | Opt-in via Spring metrics.                                                                                                                                                          |
| Platform              | JVM, Jetty, executors, …                          | JVM services     | See [scrape reference](#prometheus-scrape-reference-gms)                              | Standard Boot/Micrometer.                                                                                                                                                           |

### GraphQL vs OpenAPI on the wire

- **GraphQL:** Operation-level `graphql.request.*` (and optional `graphql.field.*`). The same requests also create **`http.server.requests`** for the `/graphql` route—do not double-count as the same signal.
- **OpenAPI:** Use **`http.server.requests`** with `uri` matching `**/openapi/**`.

Unified error pressure (illustrative PromQL):

```promql
sum(rate(graphql_request_errors_total[5m]))
sum(rate(http_server_requests_seconds_count{status=~"5..",uri=~".*/openapi/.*"}[5m]))
```

For one chart across surfaces, use **recording rules** with a bounded label such as `datahub_api_surface=graphql|openapi`—do not copy raw URIs into new tags.

**Prefix policy:** Spring-owned names stay **`graphql.*`**, **`http.server.*`**, **`jvm.*`**, etc. DataHub-defined application meters use the **`datahub.*`** prefix. Opt-in series (field-level GraphQL, validation, …) appear only after traffic hits those paths—scrape `/actuator/prometheus` to confirm.

**Dual scrape:** Many sites scrape **both** JMX and Actuator. Use **`metric_relabel_configs`** / datasource routing so each panel reads one registry; on cutover, prefer **labels** on Micrometer families, not regex bridges across dozens of `__name__` values.

### Domain meter names vs `subsystem` tag {#domain-meter-names-vs-subsystem-tags}

- **Meter name** = functional domain (`datahub.graph`, `datahub.search`, `datahub.metadata_store`, …).
- **`subsystem`** = backing **technology** only: `elasticsearch`, `neo4j`, `ebean`, `cassandra`—not `graph_elasticsearch`.
- **`operation`** = coarse step (`search`, `batch_get`, `search_pit`, `usage_search`, …)—not index names or SQL.

One meter **`datahub.metadata_store`** covers both Ebean and Cassandra; distinguish with **`subsystem`**.

### Cardinality rules

- **Allowed** tag keys for new app meters: `hook`, `topic`, `consumer.group`, coarse `operation` / `resource`, `result`, `login_source`, `denial_reason`, API traffic tags above, and for storage/search meters **`subsystem`** + **`operation`** (see [`DataHubContextParser`](../../metadata-operation-context/src/main/java/io/datahubproject/metadata/context/request/DataHubContextParser.java) for header-derived tags).
- **Do not** label with entity URNs, raw queries, SQL, or exception messages.
- **GraphQL field-level** metrics: keep `GRAPHQL_METRICS_FIELD_LEVEL_ENABLED=false` in production unless you accept cost ([GraphQL env vars](../deploy/environment-vars.md#graphql-configuration)).
- **HTTP `uri`:** Prefer templated routes; raw paths can explode series.

### Tag contracts (auth and API traffic)

**`datahub.auth.login_outcomes`:** `operation` (`generate_session_token` \| `verify_native_user_credentials`), `result` (`success` \| `failure`), `login_source` from [`LoginSource`](../../metadata-service/services/src/main/java/com/linkedin/metadata/datahubusage/event/LoginSource.java) or `unknown`, `denial_reason` = `LoginDenialReason.name()` or `none` on success.

**`datahub.api.traffic`:** `user_category` (`system` \| `admin` \| `service` \| `regular`), `request_api` (`restli` \| `openapi` \| `graphql`; test excluded), `agent_class` (e.g. `browser`, `robot`), `agent_skill` / `agent_caller` from allowlisted `X-DataHub-Context` or `unspecified`.

### Prometheus scrape reference (GMS Actuator) {#prometheus-scrape-reference-gms}

GMS exposes **`/actuator/prometheus`** on the management port (**4319** in default images). Optional meters may be absent until code paths run.

#### Conventions

| Topic         | Detail                                                         |
| ------------- | -------------------------------------------------------------- |
| Timers        | `*_count`, `*_sum`, `*_max`; histograms add `*_bucket` + `le`. |
| Counters      | Often `*_total`.                                               |
| Spring meters | Usually include `application` (e.g. `datahub-gms`).            |

#### Storage and search (domain timers)

| Logical meter               | Prometheus stem                     | Label keys                              | Example `subsystem` / `operation`                                                        |
| --------------------------- | ----------------------------------- | --------------------------------------- | ---------------------------------------------------------------------------------------- |
| `datahub.metadata_store`    | `datahub_metadata_store_seconds`    | `subsystem`, `operation`, `application` | `ebean`: `batch_get`, `get`, `get_latest_aspects`, `save`; `cassandra` when used.        |
| `datahub.search`            | `datahub_search_seconds`            | `subsystem`, `operation`, `application` | `elasticsearch`: `search`, `search_scroll`, `count`, `aggregate`; usage: `usage_search`. |
| `datahub.graph`             | `datahub_graph_seconds`             | `subsystem`, `operation`, `application` | `elasticsearch`: `search`, `search_pit`.                                                 |
| `datahub.timeseries_aspect` | `datahub_timeseries_aspect_seconds` | `subsystem`, `operation`, `application` | e.g. `search_aspect_values`.                                                             |

#### API traffic and validation

| Logical meter                             | Prometheus name                                                                      | Type    | Label keys                                                                                  |
| ----------------------------------------- | ------------------------------------------------------------------------------------ | ------- | ------------------------------------------------------------------------------------------- |
| `datahub.api.traffic`                     | `datahub_api_traffic_total`                                                          | Counter | `user_category`, `agent_class`, `request_api`, `agent_skill`, `agent_caller`, `application` |
| Aspect size (pre/post patch distribution) | `datahub_validation_aspect_size_pre_patch_size_distribution_total`, `…_post_patch_…` | Counter | `aspectName`, `sizeBucket`, `application`                                                   |

#### Kafka and hooks

| Logical meter                       | Prometheus stem                             | Label keys                                                            |
| ----------------------------------- | ------------------------------------------- | --------------------------------------------------------------------- |
| `datahub.kafka.consumer.record_age` | `datahub_kafka_consumer_record_age_seconds` | `topic`, `consumer_group`, `application`; `le` on buckets             |
| `datahub.mcl.hook.trace_lag`        | `datahub_mcl_hook_trace_lag_seconds`        | `hook`, `application`; `le` on buckets                                |
| `datahub.mcl.hook.failures`         | `datahub_mcl_hook_failures_total`           | `hook`, `consumer_group`, `application`                               |
| `datahub.auth.login_outcomes`       | `datahub_auth_login_outcomes_total`         | `operation`, `result`, `login_source`, `denial_reason`, `application` |

#### GraphQL, HTTP, Spring Kafka

| Prometheus stem                       | Series                   | Label keys                                                                 |
| ------------------------------------- | ------------------------ | -------------------------------------------------------------------------- |
| `graphql_request_duration_seconds`    | `_count`, `_sum`, `_max` | `operation`, `operation_type`, `success`, `field_filtering`, `application` |
| `graphql_request_errors_total`        | `_total`                 | `operation`, `operation_type`, `application`                               |
| `http_server_requests_seconds`        | `_count`, `_sum`, `_max` | `method`, `status`, `uri`, `outcome`, `error`, `exception`, `application`  |
| `http_server_requests_active_seconds` | `_count`, `_sum`, `_max` | Same style as HTTP server requests                                         |
| `spring_kafka_listener_seconds`       | `_count`, `_sum`, `_max` | `name`, `result`, `exception`, `application`                               |
| `spring_kafka_template_seconds`       | `_count`, `_sum`, `_max` | `name`, `result`, `exception`, `application`                               |

Some builds also emit **`kafka_message_queue_time_seconds_*`** with `topic` and `consumer_group`—confirm on your scrape.

#### JVM, process, Jetty, executors, cache, disk, logs

| Area            | Example names                                   | Main labels                                                              |
| --------------- | ----------------------------------------------- | ------------------------------------------------------------------------ |
| JVM memory      | `jvm_memory_used_bytes`, …                      | `area`, `id`, `application`                                              |
| JVM GC          | `jvm_gc_pause_seconds_*`, …                     | `gc`, `action`, `cause`, `application`                                   |
| JVM threads     | `jvm_threads_*`                                 | `state` (where applicable), `application`                                |
| Process / OS    | `process_uptime_seconds`, `system_cpu_usage`, … | `application`                                                            |
| Jetty           | `jetty_threads_*`, `jetty_connections_*`        | `application` (+ binder tags)                                            |
| Executors       | `executor_*`, `executor_seconds_*`              | `name`, `application`                                                    |
| Scheduled tasks | `tasks_scheduled_execution_seconds_*`           | `code_namespace`, `code_function`, `outcome`, `exception`, `application` |
| Cache           | `cache_gets_total`, …                           | `cache`, `name`, `result`, `application`                                 |
| Disk            | `disk_free_bytes`, `disk_total_bytes`           | `path`, `application`                                                    |
| Logback         | `logback_events_total`                          | `level`, `application`                                                   |
| Boot lifecycle  | `application_started_time_seconds`, …           | `main_application_class`, `application`                                  |

**Cardinality:** templated `uri`, bounded GraphQL `operation`, careful use of `aspectName` on validation metrics.

### Gaps and backlog

- **Neo4j graph:** When instrumented, use **`datahub.graph`** with `subsystem=neo4j`, not a new top-level family.
- **ES bulk writes:** Not fully mirrored as `datahub.search` / `datahub.graph` timers; legacy JMX may still show bulk listeners.
- **Rest.li:** Often `metrics_com_*`; coarse coverage via `http.server.requests` and `datahub.api.traffic`.
- **Infra:** Pool metrics, Elasticsearch exporter, gateways—outside this app catalog.

### Legacy `metrics_com_*` → Micrometer targets

| Tier | Legacy signal                                   | Target                                                              |
| ---- | ----------------------------------------------- | ------------------------------------------------------------------- |
| 1    | `GraphQLController_error_Count`                 | `graphql_request_errors_total`                                      |
| 2    | `kafkaLag` / topic lag panels                   | `datahub_kafka_consumer_record_age_seconds_*` or Kafka exporter lag |
| 2    | Per-hook `*_failure_Count`                      | `datahub_mcl_hook_failures_total`                                   |
| 2    | GraphQL controller latency                      | `graphql_request_duration_seconds_*`                                |
| 2    | `requestContext_*` name-per-combo               | `datahub_api_traffic_total` with labels                             |
| 3    | CacheableSearcher, Avro, old Ebean class meters | Defer or recording rules                                            |

### Staging checklist

1. Scrape `/actuator/prometheus` and confirm expected `datahub_*` / `graphql_*` series after real traffic.
2. Estimate histogram **bucket** cardinality before enabling optional timers.
3. If scraping JMX and Actuator, align **datasources** per panel so SLOs do not mix registries silently.
4. Update alerts/dashboards; keep secrets out of tickets.

### Grafana migration hints

- **Overview boards:** Move `metrics_com_*` GraphQL and hook failures to `graphql_*`, `datahub_mcl_hook_failures_total`, `datahub_kafka_consumer_record_age_seconds_*`, and coarse `http.server.requests`; keep `jvm_*` / infra as separate layers.
- **Lineage:** Replace many legacy timeline names with **one** timer family + `operation` (or domain tags).
- **API usage:** `sum by (user_category, agent_class, request_api) (rate(datahub_api_traffic_total[5m]))`.

---

## Instrumentation guides

### GraphQL (Micrometer)

Path-level and field-level metrics help isolate slow resolvers (e.g. lineage vs schema) instead of only seeing a slow top-level operation. Implemented in [`GraphQLTimingInstrumentation`](../../metadata-io/src/main/java/com/linkedin/metadata/system_telemetry/GraphQLTimingInstrumentation.java) (GraphQL Java + Micrometer).

**Request-level:** `graphql.request.duration` — tags `operation`, `operation.type`, `success`, `field.filtering`. **`graphql.request.errors`** — counter by operation.

**Field-level (optional, high cardinality):** `graphql.field.duration` / `graphql.field.errors` — tags include `parent.type`, `field`, `operation`, `path` (if enabled). Keep **`fieldLevelPathEnabled: false`** in production.

**Master switches:**

```yaml
graphQL:
  metrics:
    enabled: ${GRAPHQL_METRICS_ENABLED:true}
    fieldLevelEnabled: ${GRAPHQL_METRICS_FIELD_LEVEL_ENABLED:false}
```

**Targeting:** `fieldLevelOperations` (comma-separated operation names), `fieldLevelPaths` (e.g. `/search/**`, `/**/lineage/**`). When both are set, fields must match **both**. Filtering modes: `DISABLED`, `ALL_FIELDS`, `BY_OPERATION`, `BY_PATH`, `BY_BOTH`.

**Examples:**

```promql
rate(graphql_request_duration_seconds_sum[5m]) / rate(graphql_request_duration_seconds_count[5m])
rate(graphql_request_errors_total[5m])
```

Details: [Environment variables – GraphQL](../deploy/environment-vars.md#graphql-configuration).

### Kafka consumer record age and MCL hook trace lag

**Record age** — `datahub.kafka.consumer.record_age`: timer at processing time = `now - ConsumerRecord.timestamp()`. Tags **`topic`**, **`consumer.group`** only (low cardinality). Covers MCP/MCL/MCE/MAE usage/platform consumers, etc.

```yaml
kafka:
  consumer:
    metrics:
      percentiles: "0.5,0.95,0.99,0.999"
      slo: "300,1800,3600,10800,21600,43200"
      maxExpectedValue: 86400
```

**Hook trace lag** — `datahub.mcl.hook.trace_lag`: time from DataHub trace timestamp in MCL system metadata to **successful hook completion**; tag **`hook`** (simple class name). Complements record age: high Kafka age + low hook lag suggests consumption bottleneck; the opposite suggests processing/hook cost.

```yaml
datahub:
  metrics:
    hookLatency:
      percentiles: "0.5,0.95,0.99,0.999"
      slo: "300,1800,3000,10800,21600,43200"
      maxExpectedValue: 86000
```

**Prometheus (examples):**

```promql
sum(rate(datahub_kafka_consumer_record_age_seconds_bucket{le="300"}[5m])) by (topic)
  / sum(rate(datahub_kafka_consumer_record_age_seconds_count[5m])) by (topic)
histogram_quantile(0.99, sum by (hook, le)(rate(datahub_mcl_hook_trace_lag_seconds_bucket[5m])))
```

Prefer **`datahub.kafka.consumer.record_age`** on Actuator over legacy **`kafkaLag`** where possible ([taxonomy](#canonical-metric-taxonomy-prometheus)).

### Aspect size validation metrics {#aspect-size-validation-metrics}

Emitted on aspect writes when validation is enabled. Constants in [`MetricUtils`](../../metadata-utils/src/main/java/com/linkedin/metadata/utils/metrics/MetricUtils.java):

- Distributions: `datahub.validation.aspect_size.pre_patch.size_distribution`, `post_patch.size_distribution` — `aspectName`, `sizeBucket`
- Oversized / warning / remediation counters — see same class for exact meter names and tags

Configuration: [Aspect size validation](mcp-mcl.md#aspect-size-validation).

```yaml
datahub:
  validation:
    aspectSize:
      metrics:
        sizeBuckets: [1048576, 5242880, 10485760, 15728640]
```

### Caches, executors, and JVM (Spring Micrometer)

Boot registers **cache** meters (`cache_gets_total` with `result=hit|miss`, `cache_size`, evictions, …), **executor** meters (`executor_pool_size_threads`, `executor_queued_tasks`, `executor_seconds_*`, …), and standard **JVM / process / Jetty** series when enabled. Label patterns are summarized in the [scrape reference](#prometheus-scrape-reference-gms).

**Useful PromQL:**

```promql
sum(rate(cache_gets_total{result="hit"}[5m])) by (cache) / sum(rate(cache_gets_total[5m])) by (cache)
rate(executor_seconds_sum[5m]) / rate(executor_seconds_count[5m])
```

---

## Distributed tracing

Traces group **spans** of work across services. DataHub uses the [OpenTelemetry Java agent](https://github.com/open-telemetry/opentelemetry-java-instrumentation) (`ENABLE_OTEL=true` on GMS and consumers). The agent instruments Kafka, JDBC, Elasticsearch, and more; use `@WithSpan` for custom spans.

Configure export via `OTEL_*` (example: Jaeger in [docker-compose monitoring](../../docker/monitoring/docker-compose.monitoring.yml)). Prefer **`grpc`** or **`http/protobuf`** for `OTEL_EXPORTER_OTLP_PROTOCOL`; plain **`http`** is a poor fit for large span payloads.

---

## Micrometer transition plan

DataHub is moving from **Dropwizard/JMX** to **Micrometer** as the primary facade: `/actuator/prometheus`, dimensional tags, and (longer term) Observation API for metrics + traces together. **Micrometer names on Actuator are still refined release-to-release** until the catalog is explicitly frozen; plan dashboard updates with upgrades.

**Today:** Actuator on **4319** for Micrometer; optional JMX agent on **4318**; [`MetricUtils`](../../metadata-utils/src/main/java/com/linkedin/metadata/utils/metrics/MetricUtils.java) bridges legacy `time()`/`increment()` and Micrometer.

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/0f6ae5ae889ee4e780504ca566670867acf975ff/imgs/advanced/monitoring/monitoring_current.svg"/>
</p>

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/0f6ae5ae889ee4e780504ca566670867acf975ff/imgs/advanced/monitoring/monitoring_transition.svg"/>
</p>

<p align="center">
  <img width="80%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/0f6ae5ae889ee4e780504ca566670867acf975ff/imgs/advanced/monitoring/monitoring_future.svg"/>
</p>

---

## Dropwizard & JMX {#dropwizard--jmx}

Historically, [Dropwizard Metrics](https://metrics.dropwizard.io/4.2.0/) reported to JMX and [jmx_exporter](https://github.com/prometheus/jmx_exporter) exposed Prometheus. Set **`ENABLE_PROMETHEUS=true`** for GMS and MAE/MCE to use the agent; [example compose](../../docker/monitoring/docker-compose.monitoring.yml) scrapes port **4318**.

**Actuator:** **`MANAGEMENT_SERVER_PORT=4319`** serves **`/actuator/prometheus`** on a separate listener (see container `start.sh`). [prometheus.yaml](../../docker/monitoring/prometheus.yaml) includes a **micrometer** job for 4319. Without `MANAGEMENT_SERVER_PORT`, Actuator often shares the main app port.

Example **add-on** compose:

```shell
docker-compose -f quickstart/docker-compose.quickstart.yml -f monitoring/docker-compose.monitoring.yml up
```

`MONITORING=true` with quickstart/dev scripts can attach the monitoring stack. Grafana example: `http://localhost:3001` (default admin/admin).

Legacy **`MetricUtils`** JMX-style helpers still exist; new code should use Micrometer APIs and the [taxonomy](#canonical-metric-taxonomy-prometheus).

---

## Health check endpoint

Use **`/admin`** to probe service health.
