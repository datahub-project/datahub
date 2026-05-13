package com.linkedin.metadata.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.execution.StructuredExecutionReport;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MCPObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

/**
 * MCPObserver that emits Micrometer metrics and structured log events for ingestion runs. Triggered
 * by the plugin framework before the DB write, so metrics emit even if the DB transaction fails,
 * giving visibility into runs that were attempted.
 *
 * <p>Design decisions:
 *
 * <ul>
 *   <li>Change type filtering (UPSERT/CREATE only, excluding RESTATE) is configured via {@link
 *       AspectPluginConfig} — RESTATE is excluded because reindex operations replay historical MCLs
 *       and would double-count all counters.
 *   <li>Metric labels: connector, status, cli_version (3 labels). Per-run identifiers and other
 *       high-cardinality fields are emitted in the {@code [INGESTION_RUN_EVENT]} structured log
 *       only.
 *   <li>warnings/failures counters reflect the JSON array size, which the ingestion framework caps
 *       at 10 via LossyList. A run with 200 failures shows failures_count=10. No total count field
 *       exists in the structured report.
 *   <li>No entity client RPCs are made — all data comes from the proposal itself.
 * </ul>
 */
@Slf4j
@Getter
@Setter
@Accessors(chain = true)
public class IngestionMetricsEmitter extends MCPObserver {

  // JMX-style prefix intentionally chosen: Micrometer converts dots to underscores in Prometheus,
  // producing com_datahub_ingest_* which matches the Observe Agent allowlist filter regex.
  private static final String METRIC_PREFIX = "com.datahub.ingest.";
  private static final Set<String> SUPPORTED_REPORT_TYPES =
      ImmutableSet.of("CLI_INGEST", "RUN_INGEST");

  private static final String TAG_CONNECTOR = "connector";
  private static final String TAG_STATUS = "status";
  private static final String TAG_CLI_VERSION = "cli_version";

  private static final String LOG_ENTRY_TITLE = "title";
  private static final String LOG_ENTRY_MESSAGE = "message";
  private static final String LOG_ENTRY_CONTEXT = "context";
  private static final String LOG_ENTRY_CATEGORY = "category";

  private static final String MDC_MESSAGE_TYPE_KEY = "messageType";
  private static final String MDC_MESSAGE_TYPE_VALUE = "ingestionRunReport";

  @Nonnull private AspectPluginConfig config;

  private final MeterRegistry meterRegistry;
  private final ObjectMapper objectMapper;

  public IngestionMetricsEmitter(
      @Nonnull MeterRegistry meterRegistry, @Nonnull ObjectMapper objectMapper) {
    this.meterRegistry = meterRegistry;
    this.objectMapper = objectMapper;
    log.info("IngestionMetricsEmitter initialized");
  }

  @Override
  protected void observeMCPs(
      Collection<? extends BatchItem> items, @Nonnull RetrieverContext retrieverContext) {
    for (BatchItem item : items) {
      try {
        ExecutionRequestResult result = item.getAspect(ExecutionRequestResult.class);
        if (result == null || !isIngestionReport(result)) {
          continue;
        }
        recordMetrics(result, item.getUrn());
      } catch (Exception e) {
        log.error("Failed to process ingestion metrics for item: {}", e.getMessage(), e);
      }
    }
  }

  private boolean isIngestionReport(@Nonnull ExecutionRequestResult result) {
    if (!result.hasStructuredReport()) {
      return false;
    }
    StructuredExecutionReport report = result.getStructuredReport();
    return SUPPORTED_REPORT_TYPES.contains(report.getType());
  }

  private void recordMetrics(
      @Nonnull ExecutionRequestResult result, @Nonnull Urn executionRequestUrn) {
    try {
      StructuredExecutionReport structuredReport = result.getStructuredReport();
      if (!"application/json".equals(structuredReport.getContentType())) {
        log.warn(
            "Unsupported content type '{}' for {}",
            structuredReport.getContentType(),
            executionRequestUrn);
        return;
      }

      String serializedValue = structuredReport.getSerializedValue();
      IngestionRunReport.Report report =
          objectMapper.readValue(serializedValue, IngestionRunReport.Report.class);

      String connector = extractConnector(report);
      String status = result.getStatus() != null ? result.getStatus() : "unknown";
      String cliVersion = extractCliVersion(report);

      Tags tags =
          Tags.of(
              TAG_CONNECTOR, sanitizeTagValue(connector),
              TAG_STATUS, sanitizeTagValue(status),
              TAG_CLI_VERSION, sanitizeTagValue(cliVersion));

      IngestionRunReport.SourceReport sourceReport =
          report.getSource() != null ? report.getSource().getReport() : null;
      IngestionRunReport.SinkReport sinkReport =
          report.getSink() != null ? report.getSink().getReport() : null;

      long eventsProduced = sourceReport != null ? sourceReport.getEventsProduced() : 0;
      // NOTE: warnings and failures are counted by list size, but the ingestion framework's
      // LossyList caps these lists at 10 entries. A run with 200 failures emits failures_count=10.
      // There is no separate total count field in the structured report — this is a known
      // limitation.
      int warningsCount = sourceReport != null ? sourceReport.getWarnings().size() : 0;
      int failuresCount = sourceReport != null ? sourceReport.getFailures().size() : 0;
      long recordsWritten = sinkReport != null ? sinkReport.getTotalRecordsWritten() : 0;
      int sinkFailuresCount = sinkReport != null ? sinkReport.getFailures().size() : 0;

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

      // Register all counters unconditionally so Prometheus has the series even for zero-value
      // runs. Without this, rate() returns no data instead of 0 for label combinations that
      // haven't crossed the zero threshold, causing dashboard holes.
      Counter.builder(METRIC_PREFIX + "events_produced")
          .tags(tags)
          .description("Metadata records extracted from source")
          .register(meterRegistry)
          .increment(eventsProduced);
      Counter.builder(METRIC_PREFIX + "warnings")
          .tags(tags)
          .description("Source warning count per ingestion run")
          .register(meterRegistry)
          .increment(warningsCount);
      Counter.builder(METRIC_PREFIX + "failures")
          .tags(tags)
          .description("Source extraction error count per ingestion run")
          .register(meterRegistry)
          .increment(failuresCount);
      Counter.builder(METRIC_PREFIX + "records_written")
          .tags(tags)
          .description("Records successfully written to DataHub")
          .register(meterRegistry)
          .increment(recordsWritten);
      Counter.builder(METRIC_PREFIX + "sink_failures")
          .tags(tags)
          .description("Sink write failures — indicates data loss")
          .register(meterRegistry)
          .increment(sinkFailuresCount);

      // Per-execution detail as structured log (Observe ingests this for drill-down)
      // SQL-specific fields (tables_scanned, view_parse_failures) are in the log only.
      emitRunEvent(
          executionRequestUrn,
          connector,
          status,
          result.hasDurationMs() ? result.getDurationMs() : null,
          eventsProduced,
          recordsWritten,
          warningsCount,
          failuresCount,
          sinkFailuresCount,
          report);

    } catch (Exception e) {
      log.error("Error recording metrics for {}: {}", executionRequestUrn, e.getMessage(), e);
    }
  }

  @Nonnull
  private String extractConnector(@Nonnull IngestionRunReport.Report report) {
    if (report.getSource() != null && report.getSource().getType() != null) {
      return report.getSource().getType();
    }
    if (report.getSource() != null
        && report.getSource().getReport() != null
        && report.getSource().getReport().getPlatform() != null) {
      return report.getSource().getReport().getPlatform();
    }
    return "unknown";
  }

  @Nonnull
  private String extractCliVersion(@Nonnull IngestionRunReport.Report report) {
    if (report.getCli() != null && report.getCli().getCliVersion() != null) {
      return report.getCli().getCliVersion();
    }
    return "unknown";
  }

  private void emitRunEvent(
      @Nonnull Urn executionRequestUrn,
      @Nonnull String connector,
      @Nonnull String status,
      @Nullable Long durationMs,
      long eventsProduced,
      long recordsWritten,
      int warningsCount,
      int failuresCount,
      int sinkFailuresCount,
      @Nonnull IngestionRunReport.Report report) {
    try {
      MDC.put(MDC_MESSAGE_TYPE_KEY, MDC_MESSAGE_TYPE_VALUE);
      String json =
          objectMapper.writeValueAsString(
              buildRunEventMap(
                  executionRequestUrn,
                  connector,
                  status,
                  durationMs,
                  eventsProduced,
                  recordsWritten,
                  warningsCount,
                  failuresCount,
                  sinkFailuresCount,
                  report));
      if (failuresCount > 0 || sinkFailuresCount > 0 || warningsCount > 0) {
        log.warn("{}", json);
      } else {
        log.info("{}", json);
      }
    } catch (Exception e) {
      log.warn("Failed to emit structured run event: {}", e.getMessage());
    } finally {
      MDC.remove(MDC_MESSAGE_TYPE_KEY);
    }
  }

  @VisibleForTesting
  Map<String, Object> buildRunEventMap(
      @Nonnull Urn executionRequestUrn,
      @Nonnull String connector,
      @Nonnull String status,
      @Nullable Long durationMs,
      long eventsProduced,
      long recordsWritten,
      int warningsCount,
      int failuresCount,
      int sinkFailuresCount,
      @Nonnull IngestionRunReport.Report report) {
    Map<String, Object> event = new LinkedHashMap<>();

    event.put(Constants.EXECUTION_ID, executionRequestUrn.getId());
    event.put(Constants.CONNECTOR, connector);
    event.put(Constants.STATUS, status);
    if (durationMs != null) {
      event.put(Constants.DURATION_MS, durationMs);
    }

    IngestionRunReport.CliInfo cli = report.getCli();
    if (cli != null) {
      if (cli.getCliVersion() != null) {
        event.put(Constants.CLI_VERSION, cli.getCliVersion());
      }
      if (cli.getModelsVersion() != null) {
        event.put(Constants.MODELS_VERSION, cli.getModelsVersion());
      }
      if (cli.getPyVersion() != null) {
        event.put(Constants.PY_VERSION, cli.getPyVersion());
      }
      if (cli.getMemInfo() != null) {
        event.put(Constants.MEM_INFO, cli.getMemInfo());
      }
      if (cli.getPeakMemoryUsage() != null) {
        event.put(Constants.PEAK_MEMORY_USAGE, cli.getPeakMemoryUsage());
      }
      if (cli.getPeakDiskUsage() != null) {
        event.put(Constants.PEAK_DISK_USAGE, cli.getPeakDiskUsage());
      }
      if (cli.getThreadCount() != null) {
        event.put(Constants.THREAD_COUNT, cli.getThreadCount());
      }
      if (cli.getPeakThreadCount() != null) {
        event.put(Constants.PEAK_THREAD_COUNT, cli.getPeakThreadCount());
      }
    }

    IngestionRunReport.SourceReport sourceReport =
        report.getSource() != null ? report.getSource().getReport() : null;
    IngestionRunReport.SinkReport sinkReport =
        report.getSink() != null ? report.getSink().getReport() : null;

    event.put(Constants.EVENTS_PRODUCED, eventsProduced);

    if (sourceReport != null) {
      if (sourceReport.getTablesScanned() > 0) {
        event.put(Constants.TABLES_SCANNED, sourceReport.getTablesScanned());
      }
      if (sourceReport.getViewsScanned() > 0) {
        event.put(Constants.VIEWS_SCANNED, sourceReport.getViewsScanned());
      }
      if (sourceReport.getSchemasScanned() > 0) {
        event.put(Constants.SCHEMAS_SCANNED, sourceReport.getSchemasScanned());
      }
      if (sourceReport.getDatabasesScanned() > 0) {
        event.put(Constants.DATABASES_SCANNED, sourceReport.getDatabasesScanned());
      }
      if (sourceReport.getEntitiesProfiled() > 0) {
        event.put(Constants.ENTITIES_PROFILED, sourceReport.getEntitiesProfiled());
      }
      if (sourceReport.getNumViewDefinitionsFailedParsing() > 0) {
        event.put(
            Constants.NUM_VIEW_DEFINITIONS_FAILED_PARSING,
            sourceReport.getNumViewDefinitionsFailedParsing());
      }

      if (sourceReport.getIngestionStageDurations() != null) {
        event.put(Constants.INGESTION_STAGE_DURATIONS, sourceReport.getIngestionStageDurations());
      }

      if (sourceReport.getIngestionHighStageSeconds() != null) {
        // Remap Python enum names to human-readable values.
        // Source: metadata-ingestion/src/datahub/ingestion/source_report/ingestion_stage.py
        // IngestionHighStage._UNDEFINED = "Ingestion", IngestionHighStage.PROFILING = "Profiling"
        // If the Python enum changes, these keys will silently become stale in Observe logs.
        Map<String, Double> highStageSeconds = new LinkedHashMap<>();
        for (Map.Entry<String, Double> entry :
            sourceReport.getIngestionHighStageSeconds().entrySet()) {
          String key = entry.getKey();
          if ("_UNDEFINED".equals(key)) {
            key = "Ingestion";
          } else if ("PROFILING".equals(key)) {
            key = "Profiling";
          }
          highStageSeconds.put(key, entry.getValue());
        }
        event.put(Constants.INGESTION_HIGH_STAGE_SECONDS, highStageSeconds);
      }
    }

    event.put(Constants.WARNINGS_COUNT, warningsCount);
    event.put(Constants.FAILURES_COUNT, failuresCount);
    event.put(
        Constants.FAILURES,
        toLogEntryMaps(sourceReport != null ? sourceReport.getFailures() : List.of()));
    event.put(
        Constants.WARNINGS,
        toLogEntryMaps(sourceReport != null ? sourceReport.getWarnings() : List.of()));
    event.put(
        Constants.INFOS,
        toLogEntryMaps(sourceReport != null ? sourceReport.getInfos() : List.of()));

    event.put(Constants.RECORDS_WRITTEN, recordsWritten);
    event.put(Constants.SINK_FAILURES_COUNT, sinkFailuresCount);
    event.put(
        Constants.SINK_FAILURES,
        toLogEntryMaps(sinkReport != null ? sinkReport.getFailures() : List.of()));

    if (sinkReport != null) {
      if (sinkReport.getRecordsWrittenPerSecond() != null) {
        event.put(Constants.RECORDS_WRITTEN_PER_SECOND, sinkReport.getRecordsWrittenPerSecond());
      }
      if (sinkReport.getPendingRequests() != null) {
        event.put(Constants.PENDING_REQUESTS, sinkReport.getPendingRequests());
      }
      if (sinkReport.getGmsVersion() != null) {
        event.put(Constants.GMS_VERSION, sinkReport.getGmsVersion());
      }
      if (sinkReport.getMode() != null) {
        event.put(Constants.SINK_MODE, sinkReport.getMode());
      }
    }

    return event;
  }

  /**
   * Converts structured log entries to a list of maps for JSON serialization. The ingestion
   * framework caps entries at 10 per level via LossyDict, so no additional cap is needed here.
   */
  @Nonnull
  private List<Map<String, Object>> toLogEntryMaps(
      @Nonnull List<IngestionRunReport.LogEntry> entries) {
    List<Map<String, Object>> result = new ArrayList<>();
    for (IngestionRunReport.LogEntry entry : entries) {
      Map<String, Object> map = new LinkedHashMap<>();
      if (entry.getTitle() != null) {
        map.put(LOG_ENTRY_TITLE, entry.getTitle());
      }
      if (entry.getMessage() != null) {
        map.put(LOG_ENTRY_MESSAGE, entry.getMessage());
      }
      if (entry.getContext() != null && !entry.getContext().isEmpty()) {
        map.put(LOG_ENTRY_CONTEXT, entry.getContext());
      }
      if (entry.getLogCategory() != null) {
        map.put(LOG_ENTRY_CATEGORY, entry.getLogCategory());
      }
      if (!map.isEmpty()) {
        result.add(map);
      }
    }
    return result;
  }

  /**
   * Sanitizes tag values for Prometheus. Note: different inputs may map to the same output (e.g.,
   * "my:platform" and "my_platform" both become "my_platform"). This is acceptable since tag values
   * in practice (connector names) do not contain special characters.
   */
  @Nonnull
  private String sanitizeTagValue(@Nullable String value) {
    if (value == null || value.isEmpty()) {
      return "unknown";
    }
    return value.replaceAll("[^a-zA-Z0-9_.-]", "_");
  }

  @VisibleForTesting
  MeterRegistry getMeterRegistry() {
    return meterRegistry;
  }
}
