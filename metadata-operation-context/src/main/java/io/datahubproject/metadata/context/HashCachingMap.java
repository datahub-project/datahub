package io.datahubproject.metadata.context;

import java.util.Map;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.Delegate;

/**
 * {@link Map} wrapper that computes {@link #hashCode()} once. Used for values built once per
 * session but hashed on every authorization cache lookup, where a deep hash of the contents would
 * otherwise be repeated per call. Mutability is that of the wrapped map, and the cached hash
 * assumes it is not mutated after construction. Equality and hash code follow the {@link Map}
 * contract of the wrapped map.
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
@ToString
final class HashCachingMap<K, V> implements Map<K, V> {
  @Delegate private final Map<K, V> map;

  private int hash;
  private volatile boolean hashComputed;

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    return map.equals(o instanceof HashCachingMap ? ((HashCachingMap<?, ?>) o).map : o);
  }

  @Override
  public int hashCode() {
    if (!hashComputed) {
      hash = map.hashCode();
      hashComputed = true;
    }
    return hash;
  }
}
