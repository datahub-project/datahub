package com.linkedin.metadata.entity.retention;

import java.io.Serializable;
import lombok.Value;

/** Hazelcast-safe wrapper for a resolved {@link com.linkedin.retention.Retention} JSON payload. */
@Value
public class CachedRetention implements Serializable {
  private static final long serialVersionUID = 1L;

  String retentionJson;
  long cachedAtMillis;
}
