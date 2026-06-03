package com.linkedin.metadata.config.graphql;

import lombok.Data;

@Data
public class GraphQLOtelConfiguration {
  private boolean enableOtelGraphqlTraces;
}
