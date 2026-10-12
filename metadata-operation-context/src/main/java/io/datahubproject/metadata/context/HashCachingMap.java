package io.datahubproject.metadata.context;

import java.util.Map;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.Delegate;

/**
 * {@link Map} wrapper that computes {@link #hashCode()} once. Used for values built once per
 * session but hashed on every authorization cache lookup, where a deep hash of the contents would
 * otherwise be repeated per call. Mutability is that of the wrapped map, and the cached hash
 * assumes it is not mutated after construction. Equality is only defined between instances of this
 * class.
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
@EqualsAndHashCode(cacheStrategy = EqualsAndHashCode.CacheStrategy.LAZY)
@ToString
final class HashCachingMap<K, V> implements Map<K, V> {
  @Delegate private final Map<K, V> map;
}
