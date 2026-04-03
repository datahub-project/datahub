package com.linkedin.metadata.utils.metrics;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.AtomicDouble;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
public class MetricUtils {
  /* Shared OpenTelemetry & Micrometer */
  public static final String DROPWIZARD_METRIC = "dwizMetric";
  public static final String DROPWIZARD_NAME = "dwizName";

  /* Micrometer. See https://prometheus.io/docs/practices/naming/ */
  /**
   * Age of consumed Kafka records when processing starts: {@code now - ConsumerRecord.timestamp()}
   * (ms). Prometheus: {@code datahub_kafka_consumer_record_age_seconds}.
   */
  public static final String DATAHUB_KAFKA_CONSUMER_RECORD_AGE =
      "datahub.kafka.consumer.record_age";

  /**
   * Time from the DataHub trace timestamp embedded in MCL system metadata to successful hook
   * completion (per hook). Prometheus: {@code datahub_mcl_hook_trace_lag_seconds} (+ histogram
   * suffixes).
   */
  public static final String DATAHUB_MCL_HOOK_TRACE_LAG = "datahub.mcl.hook.trace_lag";

  /**
   * Count of GMS API requests by coarse client segment. Prometheus: {@code
   * datahub_api_traffic_total} with tags {@value #TAG_REQUEST_USER_CATEGORY}, {@value
   * #TAG_REQUEST_AGENT_CLASS}, {@value #TAG_REQUEST_API}, plus {@code agent_*} tags from optional
   * {@code X-DataHub-Context} (keys, order, allowlists, and {@code other} bucket are configured via
   * {@code datahub.requestContext.contextHeader} in {@code application.yaml} (JSON {@code
   * valueAllowlistsJson} array; metric tag order is lexicographic by key, not header segment
   * order); defaults include {@value #TAG_AGENT_CALLER} and {@value #TAG_AGENT_SKILL}).
   */
  public static final String DATAHUB_API_TRAFFIC = "datahub.api.traffic";

  public static final String TAG_REQUEST_USER_CATEGORY = "user_category";
  public static final String TAG_REQUEST_AGENT_CLASS = "agent_class";
  public static final String TAG_REQUEST_API = "request_api";

  /**
   * Default Micrometer tag for the {@code skill} dimension from {@code X-DataHub-Context} (see
   * {@code datahub.requestContext.contextHeader.valueAllowlistsJson}).
   */
  public static final String TAG_AGENT_SKILL = "agent_skill";

  /** Default Micrometer tag for the {@code caller} dimension from {@code X-DataHub-Context}. */
  public static final String TAG_AGENT_CALLER = "agent_caller";

  /**
   * GMS auth servlet login / token outcomes. Prometheus: {@code datahub_auth_login_outcomes_total}.
   */
  public static final String DATAHUB_AUTH_LOGIN_OUTCOMES = "datahub.auth.login_outcomes";

  /**
   * Failed {@code MetadataChangeLog} hook invocations (MAE). Prometheus: {@code
   * datahub_mcl_hook_failures_total}.
   */
  public static final String DATAHUB_MCL_HOOK_FAILURES = "datahub.mcl.hook.failures";

  /**
   * Aspect size validation (pre-patch / post-patch); tags {@code aspectName}, {@code sizeBucket},
   * etc.
   */
  public static final String DATAHUB_VALIDATION_ASPECT_SIZE_PRE_PATCH_SIZE_DISTRIBUTION =
      "datahub.validation.aspect_size.pre_patch.size_distribution";

  public static final String DATAHUB_VALIDATION_ASPECT_SIZE_PRE_PATCH_OVERSIZED =
      "datahub.validation.aspect_size.pre_patch.oversized";

  public static final String DATAHUB_VALIDATION_ASPECT_SIZE_PRE_PATCH_WARNING =
      "datahub.validation.aspect_size.pre_patch.warning";

  public static final String DATAHUB_VALIDATION_ASPECT_SIZE_POST_PATCH_SIZE_DISTRIBUTION =
      "datahub.validation.aspect_size.post_patch.size_distribution";

  public static final String DATAHUB_VALIDATION_ASPECT_SIZE_POST_PATCH_OVERSIZED =
      "datahub.validation.aspect_size.post_patch.oversized";

  public static final String DATAHUB_VALIDATION_ASPECT_SIZE_POST_PATCH_WARNING =
      "datahub.validation.aspect_size.post_patch.warning";

  public static final String DATAHUB_VALIDATION_ASPECT_SIZE_REMEDIATION_DELETION_SUCCESS =
      "datahub.validation.aspect_size.remediation_deletion.success";

  public static final String DATAHUB_VALIDATION_ASPECT_SIZE_REMEDIATION_DELETION_FAILURE =
      "datahub.validation.aspect_size.remediation_deletion.failure";

  public static final String TAG_AUTH_OPERATION = "operation";
  public static final String TAG_AUTH_RESULT = "result";
  public static final String TAG_AUTH_LOGIN_SOURCE = "login_source";
  public static final String TAG_AUTH_DENIAL_REASON = "denial_reason";

  public static final String AUTH_OPERATION_GENERATE_SESSION_TOKEN = "generate_session_token";
  public static final String AUTH_OPERATION_VERIFY_NATIVE_USER_CREDENTIALS =
      "verify_native_user_credentials";

  public static final String AUTH_RESULT_SUCCESS = "success";
  public static final String AUTH_RESULT_FAILURE = "failure";
  public static final String AUTH_DENIAL_REASON_NONE = "none";

  /**
   * Backing technology for storage / domain timers (e.g. {@code elasticsearch}, {@code neo4j},
   * {@code cassandra}, {@code ebean}). The <strong>meter name</strong> carries the functional
   * domain ({@code datahub.graph}, {@code datahub.search}, …); do not repeat that domain in {@value
   * #TAG_SUBSYSTEM}. Do not add a parallel {@code implementation} tag—use only {@value
   * #TAG_SUBSYSTEM} plus {@value #TAG_STORAGE_OPERATION} where applicable.
   */
  public static final String TAG_SUBSYSTEM = "subsystem";

  /**
   * Metadata entity-store latency (Ebean-backed relational access or Cassandra CQL). Tag {@value
   * #TAG_SUBSYSTEM} is {@value #SUBSYSTEM_METADATA_SQL_EBEAN} or {@value
   * #SUBSYSTEM_METADATA_CASSANDRA}; tag {@value #TAG_STORAGE_OPERATION} names the coarse operation.
   * Prometheus: {@code datahub_metadata_store_seconds}.
   */
  public static final String DATAHUB_METADATA_STORE = "datahub.metadata_store";

  /**
   * Relationship / graph query latency. Tag {@value #TAG_SUBSYSTEM} is {@value
   * #SUBSYSTEM_ELASTICSEARCH} for the Elasticsearch graph index today; use {@value
   * #SUBSYSTEM_NEO4J} when the Neo4j graph service is instrumented. Prometheus: {@code
   * datahub_graph_seconds}.
   */
  public static final String DATAHUB_GRAPH = "datahub.graph";

  /**
   * Timeseries-aspect storage latency. Tag {@value #TAG_SUBSYSTEM} is {@value
   * #SUBSYSTEM_ELASTICSEARCH} today. Prometheus: {@code datahub_timeseries_aspect_seconds}.
   */
  public static final String DATAHUB_TIMESERIES_ASPECT = "datahub.timeseries_aspect";

  /** Coarse operation name on storage / domain timers (not high-cardinality index names). */
  public static final String TAG_STORAGE_OPERATION = "operation";

  /**
   * Elasticsearch or OpenSearch as the backing store. Used for {@link #DATAHUB_GRAPH}, {@link
   * #DATAHUB_TIMESERIES_ASPECT}, and {@link #DATAHUB_SEARCH}; the meter name distinguishes domain.
   */
  public static final String SUBSYSTEM_ELASTICSEARCH = "elasticsearch";

  /** Neo4j graph service when instrumented (same {@link #DATAHUB_GRAPH} meter as Elasticsearch). */
  public static final String SUBSYSTEM_NEO4J = "neo4j";

  /**
   * {@link #DATAHUB_METADATA_STORE}: ORM access layer (SQL-backed key-value table). Tag value
   * {@value #SUBSYSTEM_METADATA_SQL_EBEAN}.
   */
  public static final String SUBSYSTEM_METADATA_SQL_EBEAN = "ebean";

  /** {@link #DATAHUB_METADATA_STORE}: Cassandra CQL client. */
  public static final String SUBSYSTEM_METADATA_CASSANDRA = "cassandra";

  /**
   * {@link #DATAHUB_SEARCH} operation for usage-event index queries (must differ from entity {@code
   * search} when both use {@link #SUBSYSTEM_ELASTICSEARCH}).
   */
  public static final String SEARCH_OPERATION_USAGE_SEARCH = "usage_search";

  /**
   * Entity search and usage-index request latency only (not graph index). Tags: {@value
   * #TAG_SUBSYSTEM} is {@link #SUBSYSTEM_ELASTICSEARCH}; distinguish usage vs entity traffic with
   * {@value #TAG_STORAGE_OPERATION} (entity code paths use values such as {@code search}, {@code
   * autocomplete}; usage paths use {@link #SEARCH_OPERATION_USAGE_SEARCH}). Prometheus: {@code
   * datahub_search_seconds}.
   */
  public static final String DATAHUB_SEARCH = "datahub.search";

  /* OpenTelemetry */
  public static final String CACHE_HIT_ATTR = "cache.hit";
  public static final String BATCH_SIZE_ATTR = "batch.size";
  public static final String QUEUE_ENQUEUED_AT_ATTR = "queue.enqueued_at";
  public static final String QUEUE_DURATION_MS_ATTR = "queue.duration_ms";
  public static final String MESSAGING_SYSTEM = "messaging.system";
  public static final String MESSAGING_DESTINATION = "messaging.destination";
  public static final String MESSAGING_DESTINATION_KIND = "messaging.destination_kind";
  public static final String MESSAGING_OPERATION = "messaging.operation";
  public static final String ERROR_TYPE = "error.type";
  public static final String CHANGE_TYPE = "aspect.change_type";
  public static final String ENTITY_TYPE = "aspect.entity_type";
  public static final String ASPECT_NAME = "aspect.name";

  @Deprecated public static final String DELIMITER = "_";

  @Builder.Default @NonNull private final MeterRegistry registry = new CompositeMeterRegistry();
  private static final Map<String, Timer> legacyTimeCache = new ConcurrentHashMap<>();
  private static final Map<String, Counter> legacyCounterCache = new ConcurrentHashMap<>();
  private static final Map<String, DistributionSummary> legacyHistogramCache =
      new ConcurrentHashMap<>();
  private static final Map<String, Gauge> legacyGaugeCache = new ConcurrentHashMap<>();
  private static final Map<String, Counter> micrometerCounterCache = new ConcurrentHashMap<>();
  private static final Map<String, Timer> micrometerTimerCache = new ConcurrentHashMap<>();
  private static final Map<String, DistributionSummary> micrometerDistributionCache =
      new ConcurrentHashMap<>();
  // For state-based gauges (like throttled state)
  private static final Map<String, AtomicDouble> gaugeStates = new ConcurrentHashMap<>();

  public MeterRegistry getRegistry() {
    return registry;
  }

  @Deprecated
  public void time(String dropWizardMetricName, long durationNanos) {
    Timer timer =
        legacyTimeCache.computeIfAbsent(
            dropWizardMetricName,
            name -> Timer.builder(name).tags(DROPWIZARD_METRIC, "true").register(registry));
    timer.record(durationNanos, TimeUnit.NANOSECONDS);
  }

  @Deprecated
  public void increment(Class<?> klass, String metricName, double increment) {
    String name = MetricRegistry.name(klass, metricName);
    increment(name, increment);
  }

  @Deprecated
  public void exceptionIncrement(Class<?> klass, String metricName, Throwable t) {
    String[] splitClassName = t.getClass().getName().split("[.]");
    String snakeCase =
        splitClassName[splitClassName.length - 1].replaceAll("([A-Z][a-z])", DELIMITER + "$1");

    increment(klass, metricName, 1);
    increment(klass, metricName + DELIMITER + snakeCase, 1);
  }

  @Deprecated
  public void increment(String metricName, double increment) {
    Counter counter =
        legacyCounterCache.computeIfAbsent(
            metricName,
            name ->
                Counter.builder(MetricRegistry.name(name))
                    .tag(DROPWIZARD_METRIC, "true")
                    .register(registry));
    counter.increment(increment);
  }

  /**
   * Increment a counter using Micrometer metrics library.
   *
   * @param metricName The name of the metric
   * @param increment The value to increment by
   * @param tags The tags to associate with the metric (can be empty)
   */
  public void incrementMicrometer(String metricName, double increment, String... tags) {
    // Create a cache key that includes both metric name and tags
    String cacheKey = createCacheKey(metricName, tags);
    Counter counter =
        micrometerCounterCache.computeIfAbsent(cacheKey, key -> registry.counter(metricName, tags));
    counter.increment(increment);
  }

  /**
   * Record a timer measurement using Micrometer metrics library.
   *
   * @param metricName The name of the metric
   * @param durationNanos The duration in nanoseconds
   * @param tags The tags to associate with the metric (can be empty)
   */
  public void recordTimer(String metricName, long durationNanos, String... tags) {
    String cacheKey = createCacheKey(metricName, tags);
    Timer timer =
        micrometerTimerCache.computeIfAbsent(
            cacheKey, key -> Timer.builder(metricName).tags(tags).register(registry));
    timer.record(durationNanos, TimeUnit.NANOSECONDS);
  }

  public void recordEbean(long durationNanos, String operation) {
    recordTimer(
        DATAHUB_METADATA_STORE,
        durationNanos,
        TAG_SUBSYSTEM,
        SUBSYSTEM_METADATA_SQL_EBEAN,
        TAG_STORAGE_OPERATION,
        operation);
  }

  public void recordCassandra(long durationNanos, String operation) {
    recordTimer(
        DATAHUB_METADATA_STORE,
        durationNanos,
        TAG_SUBSYSTEM,
        SUBSYSTEM_METADATA_CASSANDRA,
        TAG_STORAGE_OPERATION,
        operation);
  }

  public void recordGraph(long durationNanos, String operation) {
    recordTimer(
        DATAHUB_GRAPH,
        durationNanos,
        TAG_SUBSYSTEM,
        SUBSYSTEM_ELASTICSEARCH,
        TAG_STORAGE_OPERATION,
        operation);
  }

  public void recordTimeseries(long durationNanos, String operation) {
    recordTimer(
        DATAHUB_TIMESERIES_ASPECT,
        durationNanos,
        TAG_SUBSYSTEM,
        SUBSYSTEM_ELASTICSEARCH,
        TAG_STORAGE_OPERATION,
        operation);
  }

  /**
   * @param subsystem typically {@link #SUBSYSTEM_ELASTICSEARCH} for current Elasticsearch-backed
   *     search
   * @param operation coarse step; use {@link #SEARCH_OPERATION_USAGE_SEARCH} for usage-index
   *     queries so series do not collide with entity {@code search}
   */
  public void recordElasticsearch(long durationNanos, String subsystem, String operation) {
    recordTimer(
        DATAHUB_SEARCH, durationNanos, TAG_SUBSYSTEM, subsystem, TAG_STORAGE_OPERATION, operation);
  }

  /**
   * Record a distribution summary (histogram) using Micrometer metrics library.
   *
   * @param metricName The name of the metric
   * @param value The value to record
   * @param tags The tags to associate with the metric (can be empty)
   */
  public void recordDistribution(String metricName, long value, String... tags) {
    String cacheKey = createCacheKey(metricName, tags);
    DistributionSummary summary =
        micrometerDistributionCache.computeIfAbsent(
            cacheKey, key -> DistributionSummary.builder(metricName).tags(tags).register(registry));
    summary.record(value);
  }

  /**
   * Increments the canonical hook failure counter with bounded tags ({@code hook} simple class
   * name, {@code consumer.group}).
   */
  public void incrementHookFailure(String hookSimpleName, String consumerGroupId) {
    incrementHookFailure(hookSimpleName, consumerGroupId, 1);
  }

  /**
   * Increments the canonical hook failure counter by {@code amount} (e.g. batch size on batch hook
   * failure).
   */
  public void incrementHookFailure(String hookSimpleName, String consumerGroupId, double amount) {
    incrementMicrometer(
        DATAHUB_MCL_HOOK_FAILURES,
        amount,
        "hook",
        hookSimpleName,
        "consumer.group",
        consumerGroupId);
  }

  /**
   * Creates a cache key for a metric with its tags.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>No tags: {@code createCacheKey("datahub.api.traffic")} returns {@code
   *       "datahub.api.traffic"}
   *   <li>With tags: {@code createCacheKey("datahub.api.traffic", "user_category", "regular",
   *       "agent_class", "browser")} returns {@code
   *       "datahub.api.traffic|user_category=regular|agent_class=browser"}
   * </ul>
   *
   * @param metricName the name of the metric
   * @param tags the tags to associate with the metric (key-value pairs)
   * @return a string key that uniquely identifies this metric+tags combination
   */
  private String createCacheKey(String metricName, String... tags) {
    if (tags.length == 0) {
      return metricName;
    }

    StringBuilder keyBuilder = new StringBuilder(metricName);
    for (int i = 0; i < tags.length; i += 2) {
      if (i + 1 < tags.length) {
        keyBuilder.append("|").append(tags[i]).append("=").append(tags[i + 1]);
      }
    }
    return keyBuilder.toString();
  }

  /**
   * Set a state-based gauge value (e.g., for binary states like throttled/not throttled). This is
   * more efficient than repeatedly calling gauge() with different suppliers.
   *
   * @param clazz The class for namespacing
   * @param metricName The metric name
   * @param value The gauge value to set
   */
  @Deprecated
  public void setGaugeValue(Class<?> clazz, String metricName, double value) {
    String name = MetricRegistry.name(clazz, metricName);

    // Get or create the state holder
    AtomicDouble state = gaugeStates.computeIfAbsent(name, k -> new AtomicDouble(0));

    // Register the gauge if not already registered
    legacyGaugeCache.computeIfAbsent(
        name,
        key ->
            Gauge.builder(key, state, AtomicDouble::get)
                .tag(DROPWIZARD_METRIC, "true")
                .register(registry));

    // Update the value
    state.set(value);
  }

  @Deprecated
  public void histogram(Class<?> clazz, String metricName, long value) {
    String name = MetricRegistry.name(clazz, metricName);
    DistributionSummary summary =
        legacyHistogramCache.computeIfAbsent(
            name,
            key ->
                DistributionSummary.builder(key).tag(DROPWIZARD_METRIC, "true").register(registry));
    summary.record(value);
  }

  @Deprecated
  public static String name(String name, String... names) {
    return MetricRegistry.name(name, names);
  }

  @Deprecated
  public static String name(Class<?> clazz, String... names) {
    return MetricRegistry.name(clazz.getName(), names);
  }

  public static double[] parsePercentiles(String percentilesConfig) {
    if (percentilesConfig == null || percentilesConfig.trim().isEmpty()) {
      // Default percentiles
      return new double[] {0.5, 0.95, 0.99};
    }

    return commaDelimitedDoubles(percentilesConfig);
  }

  public static double[] parseSLOSeconds(String sloConfig) {
    if (sloConfig == null || sloConfig.trim().isEmpty()) {
      // Default SLO seconds
      return new double[] {60, 300, 900, 1800, 3600};
    }

    return commaDelimitedDoubles(sloConfig);
  }

  private static double[] commaDelimitedDoubles(String value) {
    String[] parts = value.split(",");
    double[] result = new double[parts.length];
    for (int i = 0; i < parts.length; i++) {
      result[i] = Double.parseDouble(parts[i].trim());
    }
    return result;
  }
}
