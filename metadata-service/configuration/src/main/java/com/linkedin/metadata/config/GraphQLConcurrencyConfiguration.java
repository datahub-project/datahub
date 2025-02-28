package com.linkedin.metadata.config;

import lombok.Data;

@Data
public class GraphQLConcurrencyConfiguration {
  boolean separateThreadPool;
  long stackSize;
  int corePoolSize;
  int maxPoolSize;
  int keepAlive;
}
