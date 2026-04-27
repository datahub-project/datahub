package com.datahub.test.authorization;

import com.datahub.authorization.AuthorizationResult;
import com.google.common.annotations.VisibleForTesting;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Custom implementation of the {@link Map} for authorization results, that always returns constant
 * decision
 */
@VisibleForTesting
public class ConstantAuthorizationResultMap extends AbstractMap<String, AuthorizationResult> {
  private final AuthorizationResult result;

  public ConstantAuthorizationResultMap(AuthorizationResult.Type type) {
    this.result = new AuthorizationResult(null, type, "constant");
  }

  @Override
  public AuthorizationResult get(Object privilege) {
    return result;
  }

  @Override
  @Nonnull
  public Set<Entry<String, AuthorizationResult>> entrySet() {
    return Set.of();
  }

  @Override
  public boolean containsKey(Object privilege) {
    // for any requested privilege we will return the constant value
    return true;
  }
}
