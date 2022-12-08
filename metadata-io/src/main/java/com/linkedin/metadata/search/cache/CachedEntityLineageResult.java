package com.linkedin.metadata.search.cache;

import java.io.Serializable;
import lombok.Data;


@Data
public class CachedEntityLineageResult implements Serializable {
  private final byte[] entityLineageResult;
  private final long timestamp;
}
