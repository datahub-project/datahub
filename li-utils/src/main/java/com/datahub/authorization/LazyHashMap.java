package com.datahub.authorization;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class LazyHashMap<K, V> extends ConcurrentHashMap<K, V> {
  private final Function<K, V> computeFn;

  @Override
  public V get(Object key) {
    V result = super.get(key);
    if (result != null) {
      return result;
    }

    K typedKey = (K) key;
    V computedValue = computeFn.apply(typedKey);
    putIfAbsent(typedKey, computedValue);
    return computedValue;
  }
}
