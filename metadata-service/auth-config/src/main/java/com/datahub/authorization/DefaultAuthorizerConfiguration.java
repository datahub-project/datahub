package com.datahub.authorization;

import lombok.Data;

@Data
public class DefaultAuthorizerConfiguration {
  /** Whether authorization via DataHub policies is enabled. */
  private boolean enabled;

  /** The duration between policies cache refreshes. */
  private int cacheRefreshIntervalSecs;

  /**
   * Whether domain-based authorization is enabled. When enabled, policies can filter by entity
   * domains.
   */
  private boolean domainBasedAuthorizationEnabled;
}
