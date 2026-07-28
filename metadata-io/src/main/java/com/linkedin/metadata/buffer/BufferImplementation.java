package com.linkedin.metadata.buffer;

/**
 * Backend implementations for {@link CoalesceBuffer}, selected via {@code
 * datahub.buffer.implementation}.
 *
 * <p>{@code REDIS} is intentionally not listed yet — reserved for a future Redis/Dragonfly-backed
 * implementation (see the coalesce buffer design doc); not implemented in this change.
 */
public enum BufferImplementation {
  CAFFEINE,
  HAZELCAST
}
