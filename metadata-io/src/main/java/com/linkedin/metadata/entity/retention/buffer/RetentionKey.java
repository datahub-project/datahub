package com.linkedin.metadata.entity.retention.buffer;

import java.io.Serializable;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Coalescing key for the retention buffer: identifies a single (urn, aspect) pair whose pending
 * retention requests should be collapsed to a single "keep max version" entry.
 */
public final class RetentionKey implements Serializable {
  private static final long serialVersionUID = 1L;

  @Nonnull private final String urn;
  @Nonnull private final String aspectName;

  public RetentionKey(@Nonnull String urn, @Nonnull String aspectName) {
    this.urn = Objects.requireNonNull(urn, "urn must not be null");
    this.aspectName = Objects.requireNonNull(aspectName, "aspectName must not be null");
  }

  @Nonnull
  public String getUrn() {
    return urn;
  }

  @Nonnull
  public String getAspectName() {
    return aspectName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RetentionKey)) {
      return false;
    }
    RetentionKey that = (RetentionKey) o;
    return urn.equals(that.urn) && aspectName.equals(that.aspectName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(urn, aspectName);
  }

  @Override
  public String toString() {
    return "RetentionKey{urn=" + urn + ", aspectName=" + aspectName + '}';
  }
}
