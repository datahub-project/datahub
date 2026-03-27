package com.linkedin.metadata.kafka.hook.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.execution.StructuredExecutionReport;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * This hook listens for {@link ExecutionRequestResult} MCL events from ingestion runs and emits
 * Micrometer metrics for ingestion observability. Metrics are exposed via the /actuator/prometheus
 * endpoint.
 *
 * <p>The hook processes execution results that contain structured ingestion reports including:
 *
 * <ul>
 *   <li>Run metadata (pipeline name, platform, status)
 *   <li>Volume metrics (records written, events produced)
 *   <li>Quality metrics (errors, warnings, failures)
 *   <li>Performance metrics (duration)
 * </ul>
 */
@Slf4j
@Component
public class IngestionMetricsHook implements MetadataChangeLogHook {

  private static final String ASPECT_NAME = "dataHubExecutionRequestResult";
  private static final String INPUT_ASPECT_NAME = "dataHubExecutionRequestInput";
  private static final String GLOBAL_TAGS_ASPECT_NAME = "globalTags";
  private static final String INGESTION_SOURCE_INFO_ASPECT_NAME = "dataHubIngestionSourceInfo";
  private static final String METRIC_PREFIX = "com.datahub.ingest.";
  // CLI_INGEST: from CLI ingestion (datahub ingest command)
  // RUN_INGEST: from executor-based ingestion (scheduled, manual via UI)
  private static final Set<String> SUPPORTED_REPORT_TYPES =
      ImmutableSet.of("CLI_INGEST", "RUN_INGEST");
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Set<ChangeType> SUPPORTED_CHANGE_TYPES =
      ImmutableSet.of(ChangeType.UPSERT, ChangeType.CREATE);

  // Criticality tag URN prefixes (e.g., urn:li:tag:critical -> "critical")
  private static final String CRITICALITY_CRITICAL = "critical";
  private static final String CRITICALITY_WARNING = "warning";
  private static final String CRITICALITY_ALERT_DISABLED = "alert-disabled";
  private static final String CRITICALITY_DEFAULT = "warning"; // Default if no tag present

  // Metric tag names (pipeline is intentionally NOT a metric tag — it's per-run and would
  // cause unbounded cardinality. Per-execution detail is emitted as a structured log event.)
  private static final String TAG_PLATFORM = "platform";
  private static final String TAG_STATUS = "status";
  private static final String TAG_INGESTION_SOURCE = "ingestion_source";
  private static final String TAG_CRITICALITY = "criticality";
  private static final String TAG_SINK_HOST = "sink_host";
  private static final String TAG_VERSION = "version";

  @VisibleForTesting static final long CRITICALITY_CACHE_TTL_MS = 5 * 60 * 1000L; // 5 minutes

  private final MeterRegistry meterRegistry;
  private final SystemEntityClient entityClient;
  private final boolean isEnabled;
  @Getter private final String consumerGroupSuffix;
  private OperationContext systemOpContext;

  private final ConcurrentHashMap<String, String> criticalityCache = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, String> sinkHostCache = new ConcurrentHashMap<>();
  private final AtomicLong ingestionSourceCacheRefreshedAt =
      new AtomicLong(System.currentTimeMillis());

  @Autowired
  public IngestionMetricsHook(
      @Nonnull MeterRegistry meterRegistry,
      @Nonnull SystemEntityClient entityClient,
      @Nonnull @Value("${ingestionMetrics.hook.enabled:true}") Boolean isEnabled,
      @Nonnull @Value("${ingestionMetrics.hook.consumerGroupSuffix:}") String consumerGroupSuffix) {
    this.meterRegistry = meterRegistry;
    this.entityClient = entityClient;
    this.isEnabled = isEnabled;
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @VisibleForTesting
  public IngestionMetricsHook(
      @Nonnull MeterRegistry meterRegistry,
      @Nullable SystemEntityClient entityClient,
      @Nonnull Boolean isEnabled) {
    this.meterRegistry = meterRegistry;
    this.entityClient = entityClient;
    this.isEnabled = isEnabled;
    this.consumerGroupSuffix = "";
  }

  @Override
  public boolean isEnabled() {
    return isEnabled;
  }

  @Override
  public IngestionMetricsHook init(@Nonnull OperationContext systemOperationContext) {
    this.systemOpContext = systemOperationContext;
    log.info("Initialized the ingestion metrics hook");
    return this;
  }

  @Override
  public void invoke(@Nonnull MetadataChangeLog event) throws Exception {
    if (!isEnabled || !isEligibleForProcessing(event)) {
      return;
    }

    try {
      ExecutionRequestResult result = extractResult(event);

      // Only process ingestion reports (CLI_INGEST or RUN_INGEST)
      if (!isIngestionReport(result)) {
        return;
      }

      recordMetrics(result, event.getEntityUrn().toString());
    } catch (Exception e) {
      log.error("Failed to process ingestion metrics for event: {}", event.getEntityUrn(), e);
    }
  }

  /** Returns true if this event should be processed by this hook. */
  private boolean isEligibleForProcessing(@Nonnull MetadataChangeLog event) {
    return ASPECT_NAME.equals(event.getAspectName())
        && SUPPORTED_CHANGE_TYPES.contains(event.getChangeType())
        && event.getAspect() != null;
  }

  /** Checks if this is an ingestion report (CLI_INGEST or RUN_INGEST). */
  private boolean isIngestionReport(@Nonnull ExecutionRequestResult result) {
    if (!result.hasStructuredReport()) {
      return false;
    }
    StructuredExecutionReport report = result.getStructuredReport();
    return SUPPORTED_REPORT_TYPES.contains(report.getType());
  }

  /** Extracts the ExecutionRequestResult from the MCL event. */
  @Nonnull
  private ExecutionRequestResult extractResult(@Nonnull MetadataChangeLog event) {
    return GenericRecordUtils.deserializeAspect(
        event.getAspect().getValue(),
        event.getAspect().getContentType(),
        ExecutionRequestResult.class);
  }

  /** Records metrics from the execution result and emits a structured log event. */
  private void recordMetrics(
      @Nonnull ExecutionRequestResult result, @Nonnull String executionRequestUrn) {
    try {
      JsonNode reportJson = parseStructuredReport(result);
      if (reportJson == null) {
        log.warn("Could not parse structured report for {}", executionRequestUrn);
        return;
      }

      // Find source/sink reports once (used for platform extraction, metrics, and log)
      JsonNode sourceReport = findSourceReport(reportJson);
      JsonNode sinkReport = findSinkReport(reportJson);

      String pipelineName = extractPipelineName(reportJson, executionRequestUrn);
      String platform = extractPlatform(reportJson, sourceReport);
      String status = result.getStatus();
      String version = extractVersion(reportJson);

      String ingestionSourceUrn = extractIngestionSourceUrn(executionRequestUrn);
      String ingestionSource =
          ingestionSourceUrn != null ? extractIdFromUrn(ingestionSourceUrn) : "unknown";
      String criticality = extractCriticality(ingestionSourceUrn);
      String sinkHost = extractSinkHost(ingestionSourceUrn);

      // Stable tags only — no per-run pipeline (avoids unbounded cardinality)
      Tags tags =
          Tags.of(
              TAG_PLATFORM, sanitizeTagValue(platform),
              TAG_STATUS, sanitizeTagValue(status),
              TAG_INGESTION_SOURCE, sanitizeTagValue(ingestionSource),
              TAG_CRITICALITY, sanitizeTagValue(criticality),
              TAG_SINK_HOST, sanitizeTagValue(sinkHost),
              TAG_VERSION, sanitizeTagValue(version));

      // Extract numeric values (used for both metrics and structured log)
      long eventsProduced = extractLongField(sourceReport, "events_produced");
      int warningsCount = extractArraySize(sourceReport, "warnings");
      int failuresCount = extractArraySize(sourceReport, "failures");
      long recordsWritten = extractLongField(sinkReport, "total_records_written");
      int sinkFailuresCount = extractArraySize(sinkReport, "failures");

      Counter.builder(METRIC_PREFIX + "runs")
          .tags(tags)
          .description("Total number of ingestion runs")
          .register(meterRegistry)
          .increment();

      if (result.hasDurationMs()) {
        DistributionSummary.builder(METRIC_PREFIX + "duration_ms")
            .tags(tags)
            .description("Duration of ingestion runs in milliseconds")
            .register(meterRegistry)
            .record(result.getDurationMs());
      }

      if (eventsProduced > 0) {
        Counter.builder(METRIC_PREFIX + "events_produced")
            .tags(tags)
            .register(meterRegistry)
            .increment(eventsProduced);
      }
      if (warningsCount > 0) {
        Counter.builder(METRIC_PREFIX + "warnings")
            .tags(tags)
            .register(meterRegistry)
            .increment(warningsCount);
      }
      if (failuresCount > 0) {
        Counter.builder(METRIC_PREFIX + "failures")
            .tags(tags)
            .register(meterRegistry)
            .increment(failuresCount);
      }
      if (recordsWritten > 0) {
        Counter.builder(METRIC_PREFIX + "records_written")
            .tags(tags)
            .register(meterRegistry)
            .increment(recordsWritten);
      }
      if (sinkFailuresCount > 0) {
        Counter.builder(METRIC_PREFIX + "sink_failures")
            .tags(tags)
            .register(meterRegistry)
            .increment(sinkFailuresCount);
      }

      // Per-execution detail as structured log (Observe ingests this for drill-down)
      emitRunEvent(
          pipelineName,
          executionRequestUrn,
          ingestionSource,
          platform,
          status,
          criticality,
          result.hasDurationMs() ? result.getDurationMs() : null,
          eventsProduced,
          recordsWritten,
          warningsCount,
          failuresCount,
          sinkFailuresCount);

    } catch (Exception e) {
      log.error("Error recording metrics for {}: {}", executionRequestUrn, e.getMessage(), e);
    }
  }

  /** Parses the structured report JSON. */
  @Nullable
  private JsonNode parseStructuredReport(@Nonnull ExecutionRequestResult result) {
    try {
      if (!result.hasStructuredReport()) {
        return null;
      }
      String serializedValue = result.getStructuredReport().getSerializedValue();
      return OBJECT_MAPPER.readTree(serializedValue);
    } catch (Exception e) {
      log.warn("Failed to parse structured report JSON: {}", e.getMessage());
      return null;
    }
  }

  /** Extracts pipeline name from the report. */
  @Nonnull
  private String extractPipelineName(@Nonnull JsonNode reportJson, @Nonnull String urn) {
    // Try to get from run_id which often contains pipeline info
    JsonNode runId = reportJson.path("runId");
    if (!runId.isMissingNode() && runId.isTextual()) {
      return runId.asText();
    }
    // Fall back to execution request URN
    return extractIdFromUrn(urn);
  }

  /** Extracts platform from the source report (uses pre-found sourceReport to avoid re-lookup). */
  @Nonnull
  private String extractPlatform(@Nonnull JsonNode reportJson, @Nonnull JsonNode sourceReport) {
    // Try executor format first: source.type
    JsonNode sourceType = reportJson.path("source").path("type");
    if (!sourceType.isMissingNode() && sourceType.isTextual()) {
      return sourceType.asText();
    }

    // Fall back to platform field in source report (works for both formats)
    if (!sourceReport.isMissingNode()) {
      JsonNode platform = sourceReport.path("platform");
      if (!platform.isMissingNode() && platform.isTextual()) {
        return platform.asText();
      }
    }
    return "unknown";
  }

  /**
   * Extracts the sink host from the ingestion source's recipe sink server URL. Fetches {@code
   * dataHubIngestionSourceInfo} aspect, parses the recipe JSON, and extracts the hostname from
   * {@code sink.config.server} (e.g., "customer1.example.com" from
   * "https://customer1.example.com"). Results are cached alongside criticality with the same TTL.
   */
  @Nonnull
  private String extractSinkHost(@Nullable String ingestionSourceUrn) {
    if (ingestionSourceUrn == null || entityClient == null || systemOpContext == null) {
      return "unknown";
    }

    evictIngestionSourceCacheIfExpired();

    return sinkHostCache.computeIfAbsent(ingestionSourceUrn, this::fetchSinkHost);
  }

  /** Fetches sink host from ingestion source info recipe (called on cache miss). */
  @Nonnull
  private String fetchSinkHost(@Nonnull String ingestionSourceUrn) {
    try {
      Urn urn = UrnUtils.getUrn(ingestionSourceUrn);
      EntityResponse response =
          entityClient.getV2(
              systemOpContext,
              urn.getEntityType(),
              urn,
              ImmutableSet.of(INGESTION_SOURCE_INFO_ASPECT_NAME));

      if (response != null
          && response.getAspects() != null
          && response.getAspects().containsKey(INGESTION_SOURCE_INFO_ASPECT_NAME)) {
        EnvelopedAspect enveloped = response.getAspects().get(INGESTION_SOURCE_INFO_ASPECT_NAME);
        DataHubIngestionSourceInfo sourceInfo =
            new DataHubIngestionSourceInfo(enveloped.getValue().data());

        if (sourceInfo.hasConfig() && sourceInfo.getConfig().hasRecipe()) {
          return extractSinkHostFromRecipe(sourceInfo.getConfig().getRecipe());
        }
      }

      log.debug("No ingestion source info found for {}", ingestionSourceUrn);
      return "unknown";
    } catch (Exception e) {
      log.warn(
          "Failed to fetch ingestion source info for {}: {}", ingestionSourceUrn, e.getMessage());
      return "unknown";
    }
  }

  /** Parses recipe JSON and extracts hostname from sink server URL. */
  @Nonnull
  private String extractSinkHostFromRecipe(@Nonnull String recipeJson) {
    try {
      JsonNode recipe = OBJECT_MAPPER.readTree(recipeJson);
      JsonNode serverNode = recipe.path("sink").path("config").path("server");
      if (serverNode.isMissingNode() || !serverNode.isTextual()) {
        return "unknown";
      }
      return extractHostname(serverNode.asText());
    } catch (Exception e) {
      log.debug("Failed to parse recipe for sink host extraction: {}", e.getMessage());
      return "unknown";
    }
  }

  /**
   * Extracts hostname from URL (e.g., "customer1.example.com" from
   * "https://customer1.example.com").
   */
  @Nonnull
  private String extractHostname(@Nonnull String url) {
    try {
      java.net.URI uri = new java.net.URI(url);
      String host = uri.getHost();
      return (host != null && !host.isEmpty()) ? host : "unknown";
    } catch (Exception e) {
      return "unknown";
    }
  }

  /** Extracts CLI version from the report (e.g., "1.3.1" from cli.cli_version). */
  @Nonnull
  private String extractVersion(@Nonnull JsonNode reportJson) {
    JsonNode cliVersion = reportJson.path("cli").path("cli_version");
    if (!cliVersion.isMissingNode() && cliVersion.isTextual()) {
      return cliVersion.asText();
    }
    return "unknown";
  }

  /**
   * Extracts the stable ingestion source URN from the execution request.
   *
   * <p>This fetches the ExecutionRequestInput aspect to get the ingestionSource URN, which is
   * stable across runs for the same logical ingestion source. This allows proper grouping of
   * metrics for consecutive failure detection and baseline calculations.
   *
   * @param executionRequestUrn the URN of the execution request
   * @return the full ingestion source URN if available, or null if not found
   */
  @Nullable
  private String extractIngestionSourceUrn(@Nonnull String executionRequestUrn) {
    if (entityClient == null || systemOpContext == null) {
      log.debug(
          "EntityClient or OperationContext not available, cannot fetch ingestion source for {}",
          executionRequestUrn);
      return null;
    }

    try {
      Urn urn = UrnUtils.getUrn(executionRequestUrn);
      EntityResponse response =
          entityClient.getV2(
              systemOpContext, urn.getEntityType(), urn, ImmutableSet.of(INPUT_ASPECT_NAME));

      if (response != null
          && response.getAspects() != null
          && response.getAspects().containsKey(INPUT_ASPECT_NAME)) {
        ExecutionRequestInput input =
            new ExecutionRequestInput(
                response.getAspects().get(INPUT_ASPECT_NAME).getValue().data());

        if (input.hasSource() && input.getSource().hasIngestionSource()) {
          String ingestionSourceUrn = input.getSource().getIngestionSource().toString();
          log.debug(
              "Found ingestion source {} for execution request {}",
              ingestionSourceUrn,
              executionRequestUrn);
          return ingestionSourceUrn;
        }
      }

      log.debug("No ingestion source found for execution request {}", executionRequestUrn);
      return null;

    } catch (Exception e) {
      log.warn("Failed to fetch ingestion source for {}: {}", executionRequestUrn, e.getMessage());
      return null;
    }
  }

  /**
   * Extracts the criticality level from the ingestion source's global tags, with caching.
   *
   * <p>Results are cached by ingestion source URN for {@link #CRITICALITY_CACHE_TTL_MS} to avoid
   * repeated RPCs for the same source across consecutive runs. The cache is cleared in bulk when
   * the TTL expires, so tag changes are picked up within the TTL window.
   *
   * @param ingestionSourceUrn the URN of the ingestion source (may be null)
   * @return the criticality level: 'critical', 'warning', 'alert-disabled', or default 'warning'
   */
  @Nonnull
  private String extractCriticality(@Nullable String ingestionSourceUrn) {
    if (ingestionSourceUrn == null || entityClient == null || systemOpContext == null) {
      return CRITICALITY_DEFAULT;
    }

    evictIngestionSourceCacheIfExpired();

    return criticalityCache.computeIfAbsent(ingestionSourceUrn, this::fetchCriticality);
  }

  /** Evicts both criticality and sink host caches if the TTL has expired. */
  private void evictIngestionSourceCacheIfExpired() {
    long lastRefresh = ingestionSourceCacheRefreshedAt.get();
    long now = System.currentTimeMillis();
    if (now - lastRefresh > CRITICALITY_CACHE_TTL_MS
        && ingestionSourceCacheRefreshedAt.compareAndSet(lastRefresh, now)) {
      criticalityCache.clear();
      sinkHostCache.clear();
    }
  }

  /** Fetches criticality from entity client (called on cache miss). */
  @Nonnull
  private String fetchCriticality(@Nonnull String ingestionSourceUrn) {
    try {
      Urn urn = UrnUtils.getUrn(ingestionSourceUrn);
      EntityResponse response =
          entityClient.getV2(
              systemOpContext, urn.getEntityType(), urn, ImmutableSet.of(GLOBAL_TAGS_ASPECT_NAME));

      if (response != null
          && response.getAspects() != null
          && response.getAspects().containsKey(GLOBAL_TAGS_ASPECT_NAME)) {
        GlobalTags globalTags =
            new GlobalTags(response.getAspects().get(GLOBAL_TAGS_ASPECT_NAME).getValue().data());

        for (TagAssociation tagAssociation : globalTags.getTags()) {
          String tagName = extractIdFromUrn(tagAssociation.getTag().toString());
          if (CRITICALITY_CRITICAL.equalsIgnoreCase(tagName)) {
            return CRITICALITY_CRITICAL;
          } else if (CRITICALITY_WARNING.equalsIgnoreCase(tagName)) {
            return CRITICALITY_WARNING;
          } else if (CRITICALITY_ALERT_DISABLED.equalsIgnoreCase(tagName)) {
            return CRITICALITY_ALERT_DISABLED;
          }
        }
      }

      log.debug(
          "No criticality tag found for ingestion source {}, using default: {}",
          ingestionSourceUrn,
          CRITICALITY_DEFAULT);
      return CRITICALITY_DEFAULT;

    } catch (Exception e) {
      log.warn("Failed to fetch criticality tags for {}: {}", ingestionSourceUrn, e.getMessage());
      return CRITICALITY_DEFAULT;
    }
  }

  /**
   * Finds the source report in the JSON (handles multiple formats).
   *
   * <p>Executor format (RUN_INGEST): {"source": {"type": "...", "report": {...}}}
   *
   * <p>CLI format: {"Source (type)": {...}} or keys starting with "Source ("
   */
  @Nonnull
  private JsonNode findSourceReport(@Nonnull JsonNode reportJson) {
    // Try executor format first: source.report
    JsonNode source = reportJson.path("source");
    if (!source.isMissingNode()) {
      JsonNode report = source.path("report");
      if (!report.isMissingNode()) {
        return report;
      }
      // If no nested report, maybe it's directly in source
      return source;
    }

    // Fall back to CLI format: "Source (type)" keys
    var fields = reportJson.fields();
    while (fields.hasNext()) {
      var entry = fields.next();
      if (entry.getKey().startsWith("Source (")) {
        return entry.getValue();
      }
    }
    return OBJECT_MAPPER.missingNode();
  }

  /**
   * Finds the sink report in the JSON (handles multiple formats).
   *
   * <p>Executor format (RUN_INGEST): {"sink": {"type": "...", "report": {...}}}
   *
   * <p>CLI format: {"Sink (type)": {...}} or keys starting with "Sink ("
   */
  @Nonnull
  private JsonNode findSinkReport(@Nonnull JsonNode reportJson) {
    // Try executor format first: sink.report
    JsonNode sink = reportJson.path("sink");
    if (!sink.isMissingNode()) {
      JsonNode report = sink.path("report");
      if (!report.isMissingNode()) {
        return report;
      }
      // If no nested report, maybe it's directly in sink
      return sink;
    }

    // Fall back to CLI format: "Sink (type)" keys
    var fields = reportJson.fields();
    while (fields.hasNext()) {
      var entry = fields.next();
      if (entry.getKey().startsWith("Sink (")) {
        return entry.getValue();
      }
    }
    return OBJECT_MAPPER.missingNode();
  }

  /** Extracts ID from URN. */
  @Nonnull
  private String extractIdFromUrn(@Nonnull String urn) {
    // URN format: urn:li:dataHubExecutionRequest:xxxxx
    int lastColon = urn.lastIndexOf(':');
    if (lastColon > 0 && lastColon < urn.length() - 1) {
      return urn.substring(lastColon + 1);
    }
    return urn;
  }

  /** Extracts a long value from a JSON field, returning 0 if absent or non-numeric. */
  private long extractLongField(@Nonnull JsonNode parent, @Nonnull String fieldName) {
    JsonNode value = parent.path(fieldName);
    return (!value.isMissingNode() && value.isNumber()) ? value.asLong() : 0;
  }

  /** Returns the size of a JSON array field, or 0 if absent or not an array. */
  private int extractArraySize(@Nonnull JsonNode parent, @Nonnull String fieldName) {
    JsonNode array = parent.path(fieldName);
    return (!array.isMissingNode() && array.isArray()) ? array.size() : 0;
  }

  /**
   * Emits a structured JSON log event with full per-execution detail. Observe (or any log
   * aggregator) ingests this for per-run drill-down, while Prometheus metrics use only stable
   * labels for aggregation.
   */
  private void emitRunEvent(
      @Nonnull String pipeline,
      @Nonnull String executionRequestUrn,
      @Nonnull String ingestionSource,
      @Nonnull String platform,
      @Nonnull String status,
      @Nonnull String criticality,
      @Nullable Long durationMs,
      long eventsProduced,
      long recordsWritten,
      int warningsCount,
      int failuresCount,
      int sinkFailuresCount) {
    try {
      Map<String, Object> event = new LinkedHashMap<>();
      event.put("pipeline", pipeline);
      event.put("execution_request_urn", executionRequestUrn);
      event.put("ingestion_source", ingestionSource);
      event.put("platform", platform);
      event.put("status", status);
      event.put("criticality", criticality);
      if (durationMs != null) {
        event.put("duration_ms", durationMs);
      }
      event.put("events_produced", eventsProduced);
      event.put("records_written", recordsWritten);
      event.put("warnings_count", warningsCount);
      event.put("failures_count", failuresCount);
      event.put("sink_failures_count", sinkFailuresCount);

      log.info("[INGESTION_RUN_EVENT] {}", OBJECT_MAPPER.writeValueAsString(event));
    } catch (Exception e) {
      log.warn("Failed to emit structured run event: {}", e.getMessage());
    }
  }

  /** Sanitizes tag values to ensure they are valid for Prometheus. */
  @Nonnull
  private String sanitizeTagValue(@Nullable String value) {
    if (value == null || value.isEmpty()) {
      return "unknown";
    }
    // Replace non-alphanumeric characters (except underscore, dash, dot) with underscore
    return value.replaceAll("[^a-zA-Z0-9_.-]", "_");
  }

  @VisibleForTesting
  MeterRegistry getMeterRegistry() {
    return meterRegistry;
  }
}
