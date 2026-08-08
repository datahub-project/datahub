package com.linkedin.metadata.analytics.postgres;

public final class AnalyticsMetricFamilies {
  public static final String DATAHUB_USAGE = "datahub_usage";
  public static final String API_USAGE = "api_usage";
  public static final String SYSTEM_USAGE = "system_usage";

  public static final String GRAIN_HOUR = "hour";
  public static final String GRAIN_DAY = "day";
  public static final String GRAIN_MONTH = "month";

  public static final String MERGE_ADDITIVE = "additive";
  public static final String MERGE_DISTINCT = "distinct";
  public static final String MERGE_LATEST = "latest";

  public static final String LAYER_HOUR = "hour";
  public static final String LAYER_DAY = "day";
  public static final String LAYER_MONTH = "month";

  private AnalyticsMetricFamilies() {}
}
