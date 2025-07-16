package com.linkedin.metadata.config.graphql;

import lombok.Data;

@Data
public class GraphQLQueryConfiguration {
  private int complexityLimit;
  private int depthLimit;
  private boolean introspectionEnabled;
}
