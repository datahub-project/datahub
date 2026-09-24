package com.linkedin.metadata.ingestion;

import static com.linkedin.metadata.ingestion.Constants.*;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Data;
import lombok.NoArgsConstructor;

/** POJOs for deserializing the ingestion structured execution report JSON. */
public class IngestionRunReport {

  @Data
  @NoArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Report {
    @Nullable private CliInfo cli;

    @Nullable private SourceSection source;

    @Nullable private SinkSection sink;
  }

  @Data
  @NoArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class CliInfo {
    @JsonProperty(CLI_VERSION)
    @Nullable
    private String cliVersion;

    @JsonProperty(MODELS_VERSION)
    @Nullable
    private String modelsVersion;

    @JsonProperty(PY_VERSION)
    @Nullable
    private String pyVersion;

    @JsonProperty(MEM_INFO)
    @Nullable
    private String memInfo;

    @JsonProperty(PEAK_MEMORY_USAGE)
    @Nullable
    private String peakMemoryUsage;

    @JsonProperty(PEAK_DISK_USAGE)
    @Nullable
    private String peakDiskUsage;

    @JsonProperty(THREAD_COUNT)
    @Nullable
    private Long threadCount;

    @JsonProperty(PEAK_THREAD_COUNT)
    @Nullable
    private Long peakThreadCount;
  }

  @Data
  @NoArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SourceSection {
    @Nullable private String type;

    @Nullable private SourceReport report;
  }

  @Data
  @NoArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SourceReport {
    @Nullable private String platform;

    @JsonProperty(EVENTS_PRODUCED)
    private long eventsProduced;

    @JsonProperty(TABLES_SCANNED)
    private long tablesScanned;

    @JsonProperty(VIEWS_SCANNED)
    private long viewsScanned;

    @JsonProperty(SCHEMAS_SCANNED)
    private long schemasScanned;

    @JsonProperty(DATABASES_SCANNED)
    private long databasesScanned;

    @JsonProperty(ENTITIES_PROFILED)
    private long entitiesProfiled;

    @JsonProperty(NUM_VIEW_DEFINITIONS_FAILED_PARSING)
    private long numViewDefinitionsFailedParsing;

    @JsonProperty(INGESTION_STAGE_DURATIONS)
    @Nullable
    private Map<String, Double> ingestionStageDurations;

    @JsonProperty(INGESTION_HIGH_STAGE_SECONDS)
    @Nullable
    private Map<String, Double> ingestionHighStageSeconds;

    private List<LogEntry> warnings = Collections.emptyList();

    private List<LogEntry> failures = Collections.emptyList();

    private List<LogEntry> infos = Collections.emptyList();
  }

  @Data
  @NoArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SinkSection {
    @Nullable private String type;

    @Nullable private SinkReport report;
  }

  @Data
  @NoArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SinkReport {
    @JsonProperty(TOTAL_RECORDS_WRITTEN)
    private long totalRecordsWritten;

    @JsonProperty(RECORDS_WRITTEN_PER_SECOND)
    @Nullable
    private Double recordsWrittenPerSecond;

    @JsonProperty(PENDING_REQUESTS)
    @Nullable
    private Long pendingRequests;

    @JsonProperty(GMS_VERSION)
    @Nullable
    private String gmsVersion;

    @Nullable private String mode;

    private List<LogEntry> failures = Collections.emptyList();
  }

  @Data
  @NoArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class LogEntry {
    @Nullable private String title;

    @Nullable private String message;

    private List<String> context = Collections.emptyList();

    @JsonProperty(LOG_CATEGORY)
    @Nullable
    private String logCategory;
  }
}
