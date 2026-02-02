package com.datahub.test.authorization;

import com.datahub.authorization.AuthorizationResult;
import com.google.common.annotations.VisibleForTesting;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

/**
 * Custom implementation of the {@link Map} for authorization results, that allows only {@link
 * #allowedPrivileges specified} privileges and denies all others
 */
@VisibleForTesting
@RequiredArgsConstructor
public class PredefinedAuthorizationResultMap extends AbstractMap<String, AuthorizationResult> {
  private final Set<String> allowedPrivileges;

  @SuppressWarnings("SuspiciousMethodCalls")
  @Override
  public AuthorizationResult get(Object privilege) {
    var type =
        allowedPrivileges.contains(privilege)
            ? AuthorizationResult.Type.ALLOW
            : AuthorizationResult.Type.DENY;
    return new AuthorizationResult(null, type, "");
  }

  @Override
  public boolean containsKey(Object privilege) {
    // for any requested privilege we will return the value
    return true;
  }

  @Override
  @Nonnull
  public Set<Entry<String, AuthorizationResult>> entrySet() {
    return Set.of();
  }
}
