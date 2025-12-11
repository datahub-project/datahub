package com.datahub.authorization;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class LazyAuthorizationResultMap extends ConcurrentHashMap<String, AuthorizationResult> {
  private final Function<String, AuthorizationResult> computeFn;

  @Override
  public AuthorizationResult get(Object key) {
    AuthorizationResult result = super.get(key);
    if (result != null) {
      return result;
    }

    String typedKey = (String) key;
    AuthorizationResult computedValue = computeFn.apply(typedKey);
    putIfAbsent(typedKey, computedValue);
    return computedValue;
  }
}
