package com.linkedin.metadata.config;

import lombok.Data;

@Data
public class GraphQLConfiguration {
  private GraphQLQueryConfiguration query;
  private GraphQLConcurrencyConfiguration concurrency;
}
