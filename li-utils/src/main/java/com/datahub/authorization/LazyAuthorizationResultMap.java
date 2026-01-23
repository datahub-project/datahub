package com.datahub.authorization;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

/**
 * Custom implementation of the {@link Map} for authorization results, that computes result for
 * specific privilege on demand
 */
@RequiredArgsConstructor
public class LazyAuthorizationResultMap extends ConcurrentHashMap<String, AuthorizationResult> {
  private final Set<String> availablePrivileges;
  private final Function<String, AuthorizationResult> computeFn;

  /**
   * Return the {@link AuthorizationResult} for specific privilege. Actor, {@link EntitySpec} and
   * sub-resources are within {@link #computeFn} closure
   *
   * @param privilege the privilege for whose result is fetched
   * @return result for specified privilege
   */
  @Override
  public AuthorizationResult get(Object privilege) {
    if (!availablePrivileges.contains(privilege)) {
      return null;
    }
    AuthorizationResult result = super.get(privilege);
    if (result != null) {
      return result;
    }

    String typedKey = (String) privilege;
    AuthorizationResult computedValue = computeFn.apply(typedKey);
    putIfAbsent(typedKey, computedValue);
    return computedValue;
  }

  /**
   * @param privilege a privilege to check whether it's supported by this map
   * @return true, if this map is able to return authorization result for that privilege, and false
   *     otherwise
   */
  @Override
  public boolean containsKey(Object privilege) {
    return availablePrivileges.contains(privilege);
  }
}
