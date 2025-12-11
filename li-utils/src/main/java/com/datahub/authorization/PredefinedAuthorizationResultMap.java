package com.datahub.authorization;

import java.util.AbstractMap;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class PredefinedAuthorizationResultMap extends AbstractMap<String, AuthorizationResult> {
  private final Set<String> allowedPrivileges;

  @SuppressWarnings("SuspiciousMethodCalls")
  @Override
  public AuthorizationResult get(Object key) {
    if (allowedPrivileges.contains(key)) {
      return new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, "");
    }
    return new AuthorizationResult(null, AuthorizationResult.Type.DENY, "");
  }

  @Override
  @Nonnull
  public Set<Entry<String, AuthorizationResult>> entrySet() {
    return Set.of();
  }
}
