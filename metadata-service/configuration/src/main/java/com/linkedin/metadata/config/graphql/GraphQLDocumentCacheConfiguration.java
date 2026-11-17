package com.linkedin.metadata.config.graphql;

import lombok.Data;

@Data
public class GraphQLDocumentCacheConfiguration {
  // Defaults true so a hand-built config (e.g. a test) is on by default too, not just when this
  // whole block is left null -- see GraphQLEngine.buildDocumentCache's null-config fallback.
  private boolean enabled = true;
  private long maximumWeightBytes;
}
