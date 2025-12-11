package com.datahub.authorization;

import java.util.AbstractMap;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ConstantAuthorizationResultMap extends AbstractMap<String, AuthorizationResult> {
  private final AuthorizationResult.Type result;

  @Override
  public AuthorizationResult get(Object key) {
    return new AuthorizationResult(null, result, "");
  }

  @Override
  @Nonnull
  public Set<Entry<String, AuthorizationResult>> entrySet() {
    return Set.of();
  }
}
