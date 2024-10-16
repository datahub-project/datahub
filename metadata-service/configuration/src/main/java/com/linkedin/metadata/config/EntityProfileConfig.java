package com.linkedin.metadata.config;

import lombok.Data;

@Data
public class EntityProfileConfig {
  /**
   * The default tab to show first on a Domain entity profile. Defaults to React code sorting if not
   * present.
   */
  public String domainDefaultTab;
}
