package com.linkedin.metadata.ingestion;

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
    @JsonProperty("cli_version")
    @Nullable
    private String cliVersion;

    @JsonProperty("models_version")
    @Nullable
    private String modelsVersion;

    @JsonProperty("py_version")
    @Nullable
    private String pyVersion;

    @JsonProperty("mem_info")
    @Nullable
    private String memInfo;

    @JsonProperty("peak_memory_usage")
    @Nullable
    private String peakMemoryUsage;

    @JsonProperty("peak_disk_usage")
    @Nullable
    private String peakDiskUsage;

    @JsonProperty("thread_count")
    @Nullable
    private Long threadCount;

    @JsonProperty("peak_thread_count")
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

    @JsonProperty("events_produced")
    private long eventsProduced;

    @JsonProperty("tables_scanned")
    private long tablesScanned;

    @JsonProperty("views_scanned")
    private long viewsScanned;

    @JsonProperty("schemas_scanned")
    private long schemasScanned;

    @JsonProperty("databases_scanned")
    private long databasesScanned;

    @JsonProperty("entities_profiled")
    private long entitiesProfiled;

    @JsonProperty("num_view_definitions_failed_parsing")
    private long numViewDefinitionsFailedParsing;

    @JsonProperty("ingestion_stage_durations")
    @Nullable
    private Map<String, Double> ingestionStageDurations;

    @JsonProperty("ingestion_high_stage_seconds")
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
    @JsonProperty("total_records_written")
    private long totalRecordsWritten;

    @JsonProperty("records_written_per_second")
    @Nullable
    private Double recordsWrittenPerSecond;

    @JsonProperty("pending_requests")
    @Nullable
    private Long pendingRequests;

    @JsonProperty("gms_version")
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

    @JsonProperty("log_category")
    @Nullable
    private String logCategory;
  }
}
