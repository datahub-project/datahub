package com.linkedin.metadata.config.search;

/**
 * Subsystems that own search indices and can each be routed to a named cluster via {@code
 * elasticsearch.componentCluster}.
 *
 * <p>Search V2 and Search V3 are separate components rather than one "entity search" component:
 * during a dual-write migration they may legitimately live on different clusters, which is the
 * whole reason a second connection exists.
 */
public enum SearchComponent {
  SEARCH_V2("searchV2"),
  SEARCH_V3("searchV3"),
  SEMANTIC("semantic"),
  GRAPH("graph"),
  TIMESERIES("timeseries"),
  SYSTEM_METADATA("systemMetadata"),
  USAGE("usage");

  private final String configKey;

  SearchComponent(String configKey) {
    this.configKey = configKey;
  }

  /** The {@code elasticsearch.componentCluster.*} property name for this component. */
  public String getConfigKey() {
    return configKey;
  }
}
