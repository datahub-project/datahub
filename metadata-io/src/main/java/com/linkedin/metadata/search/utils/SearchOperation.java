package com.linkedin.metadata.search.utils;

import lombok.Getter;

/** Values of the {@code operation} tag on {@link SearchTookMetrics}. */
public enum SearchOperation {
  BROWSE("browse"),
  DATAHUB_USAGE("dataHubUsage"),
  ENTITY_AGGREGATE_BY_VALUE("entityAggregateByValue"),
  ENTITY_AUTOCOMPLETE("entityAutoComplete"),
  ENTITY_SCROLL("entityScroll"),
  ENTITY_SEARCH("entitySearch"),
  GRAPH("graph"),
  GRAPH_QUERY_PIT("graphQueryPit"),
  INCIDENT_STATS("incidentStats"),
  PLATFORM_ENTITY_COUNTS("platformEntityCounts"),
  SYSTEM_METADATA("systemMetadata"),
  TIMESERIES("timeseries");

  @Getter private final String tag;

  SearchOperation(String tag) {
    this.tag = tag;
  }
}
