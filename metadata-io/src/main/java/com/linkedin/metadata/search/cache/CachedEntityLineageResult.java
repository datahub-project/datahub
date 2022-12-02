package com.linkedin.metadata.search.cache;

import lombok.Data;


@Data
public class CachedEntityLineageResult {
  private final String entityLineageResult;
  private final long timestamp;
}
