package com.linkedin.metadata.config.graphql;

import lombok.Data;

@Data
public class GraphQLMetricsConfiguration {
  private boolean enabled;
  private String percentiles;
  private boolean fieldLevelEnabled;
  private String fieldLevelOperations;
  private boolean fieldLevelPathEnabled;
  private String fieldLevelPaths;
  private boolean trivialDataFetchersEnabled;
}
