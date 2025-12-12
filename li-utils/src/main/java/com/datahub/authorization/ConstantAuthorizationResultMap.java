package com.datahub.authorization;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

/**
 * Custom implementation of the {@link Map} for authorization results, that always returns constant
 * decision
 */
@RequiredArgsConstructor
public class ConstantAuthorizationResultMap extends AbstractMap<String, AuthorizationResult> {
  private final AuthorizationResult.Type result;

  @Override
  public AuthorizationResult get(Object key) {
    return new AuthorizationResult(null, result, "constant");
  }

  @Override
  @Nonnull
  public Set<Entry<String, AuthorizationResult>> entrySet() {
    return Set.of();
  }

  @Override
  public boolean containsKey(Object key) {
    // for any requested privilege we will return the value
    return true;
  }
}
