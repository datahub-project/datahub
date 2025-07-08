package com.linkedin.metadata.config.search;

import lombok.Builder;
import lombok.Data;

@Data
@Builder(toBuilder = true)
public class BuildIndicesConfiguration {

  private boolean cloneIndices;
  private boolean allowDocCountMismatch;
  private String retentionUnit;
  private Long retentionValue;
  private boolean zoneAwarenessEnabled;
}
