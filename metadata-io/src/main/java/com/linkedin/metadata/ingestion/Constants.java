package com.linkedin.metadata.ingestion;

/** Field name constants for ingestion structured execution report JSON. */
public class Constants {

  private Constants() {}

  // Top-level event fields
  public static final String EXECUTION_ID = "execution_id";
  public static final String CONNECTOR = "connector";
  public static final String STATUS = "status";
  public static final String DURATION_MS = "duration_ms";

  // CLI info fields
  public static final String CLI_VERSION = "cli_version";
  public static final String MODELS_VERSION = "models_version";
  public static final String PY_VERSION = "py_version";
  public static final String MEM_INFO = "mem_info";
  public static final String PEAK_MEMORY_USAGE = "peak_memory_usage";
  public static final String PEAK_DISK_USAGE = "peak_disk_usage";
  public static final String THREAD_COUNT = "thread_count";
  public static final String PEAK_THREAD_COUNT = "peak_thread_count";

  // Source report fields
  public static final String EVENTS_PRODUCED = "events_produced";
  public static final String TABLES_SCANNED = "tables_scanned";
  public static final String VIEWS_SCANNED = "views_scanned";
  public static final String SCHEMAS_SCANNED = "schemas_scanned";
  public static final String DATABASES_SCANNED = "databases_scanned";
  public static final String ENTITIES_PROFILED = "entities_profiled";
  public static final String NUM_VIEW_DEFINITIONS_FAILED_PARSING =
      "num_view_definitions_failed_parsing";
  public static final String INGESTION_STAGE_DURATIONS = "ingestion_stage_durations";
  public static final String INGESTION_HIGH_STAGE_SECONDS = "ingestion_high_stage_seconds";

  // Warning/failure/info fields
  public static final String WARNINGS = "warnings";
  public static final String FAILURES = "failures";
  public static final String INFOS = "infos";
  public static final String WARNINGS_COUNT = "warnings_count";
  public static final String FAILURES_COUNT = "failures_count";

  // Sink report fields
  public static final String RECORDS_WRITTEN = "records_written";
  public static final String TOTAL_RECORDS_WRITTEN = "total_records_written";
  public static final String RECORDS_WRITTEN_PER_SECOND = "records_written_per_second";
  public static final String PENDING_REQUESTS = "pending_requests";
  public static final String GMS_VERSION = "gms_version";
  public static final String SINK_MODE = "sink_mode";
  public static final String SINK_FAILURES = "sink_failures";
  public static final String SINK_FAILURES_COUNT = "sink_failures_count";

  // Log entry fields
  public static final String LOG_CATEGORY = "log_category";
}
