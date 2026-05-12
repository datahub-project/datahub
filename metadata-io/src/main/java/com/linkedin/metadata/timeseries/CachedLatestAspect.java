package com.linkedin.metadata.timeseries;

import java.io.Serializable;
import javax.annotation.Nonnull;

/**
 * Cache value wrapper that pairs a serialized {@link com.linkedin.metadata.aspect.EnvelopedAspect}
 * with the event-time timestamp it represents. Carrying the timestamp as a primitive lets conflict
 * resolution on write (put-if-newer) be O(1) and atomic without re-parsing the serialized payload.
 *
 * <p>Must be {@link Serializable} so Hazelcast {@code IMap} entry processors can ship it across the
 * cluster.
 */
public final class CachedLatestAspect implements Serializable {
  private static final long serialVersionUID = 1L;

  private final long timestampMillis;
  @Nonnull private final String serializedAspect;

  public CachedLatestAspect(long timestampMillis, @Nonnull String serializedAspect) {
    this.timestampMillis = timestampMillis;
    this.serializedAspect = serializedAspect;
  }

  public long getTimestampMillis() {
    return timestampMillis;
  }

  @Nonnull
  public String getSerializedAspect() {
    return serializedAspect;
  }
}
