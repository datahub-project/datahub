package com.linkedin.metadata.entity.retention.buffer;

import java.io.Serializable;
import java.util.Objects;

/**
 * Coalescing key for the retention buffer: identifies a single (urn, aspect) pair whose pending
 * retention requests should be collapsed to a single "keep max version" entry.
 */
public record RetentionKey(String urn, String aspectName) implements Serializable {

  private static final long serialVersionUID = 1L;

  public RetentionKey {
    Objects.requireNonNull(urn, "urn must not be null");
    Objects.requireNonNull(aspectName, "aspectName must not be null");
  }
}
