package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the ChromeExtension configuration block in application.yaml */
@Data
public class ChromeExtensionConfiguration {
  /** Whether the extension is enabled */
  public boolean enabled;

  /** Whether lineage is enabled for the extension */
  public boolean lineageEnabled;
}
