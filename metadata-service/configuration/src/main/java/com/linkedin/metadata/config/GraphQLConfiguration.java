package com.linkedin.metadata.config;

import com.linkedin.metadata.config.graphql.GraphQLConcurrencyConfiguration;
import com.linkedin.metadata.config.graphql.GraphQLMetricsConfiguration;
import com.linkedin.metadata.config.graphql.GraphQLQueryConfiguration;
import lombok.Data;

@Data
public class GraphQLConfiguration {
  private GraphQLQueryConfiguration query;
  private GraphQLConcurrencyConfiguration concurrency;
  private GraphQLMetricsConfiguration metrics;
}
