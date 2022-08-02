package com.datahub.authorization;

import lombok.Data;


@Data
public class DefaultAuthorizerConfiguration {
  /**
   * Whether authorization via DataHub policies is enabled.
   */
  private Boolean enabled;
  /**
   * The duration between policies cache refreshes.
   */
  private int cacheRefreshIntervalSecs;
}
