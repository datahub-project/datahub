package com.linkedin.metadata.search.cache;

import com.linkedin.metadata.graph.EntityLineageResult;
import lombok.Data;


@Data
public class CachedEntityLineageResult {
  private final EntityLineageResult entityLineageResult;
  private final long timestamp;
}
