package io.datahubproject.metadata.context;

import java.util.Optional;

public interface ContextInterface {
  /**
   * Caching layers must take into account the operation's context to avoid returning incorrect or
   * restricted results.
   *
   * <p>A consistent hash must be produced in a distributed cache so that multiple jvms produce the
   * same keys for the same objects within the same context. This generally rules out hashCode()
   * since those are not guaranteed properties.
   *
   * <p>We are however leveraging the special case of hashCode() for String which should be
   * consistent across jvms and executions.
   *
   * <p>The overall context must produce a unique id to be included with cache keys. Each component
   * of the OperationContext must produce a unique identifier to be used for this purpose.
   */
  Optional<Integer> getCacheKeyComponent();
}
