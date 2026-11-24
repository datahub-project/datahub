package com.linkedin.metadata.config.hazelcast;

import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Adds entries to the allow-list that the embedded Hazelcast member uses when it reads
 * Java-serialized values ({@code Serializable} / {@code Externalizable}) from peers.
 *
 * <p>Hazelcast rejects any Java-serialized class that is not on the allow-list. A module that
 * stores its own {@code Serializable} types in Hazelcast (map values, keys, entry processors)
 * registers a Spring bean implementing this interface instead of editing the core list. Types with
 * a Hazelcast {@code StreamSerializer}, {@code IdentifiedDataSerializable} or Compact serializer do
 * not go through Java serialization and need no entry.
 *
 * <p>Keep entries narrow. A prefix admits every class under it, including {@code readObject}
 * gadgets from libraries that bundle their own dependencies.
 */
public interface HazelcastSerializationAllowListContributor {

  /** Fully qualified class names, e.g. {@code com.example.MyCacheKey}. */
  @Nonnull
  default Set<String> classNames() {
    return Set.of();
  }

  /** Class-name prefixes, e.g. {@code com.example.cache.}. Include the trailing dot. */
  @Nonnull
  default Set<String> packagePrefixes() {
    return Set.of();
  }
}
