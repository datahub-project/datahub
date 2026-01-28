package com.linkedin.metadata.config;

import lombok.Data;

@Data
public class ApplicationConfig {
  /** DEPRECATED: This is now controlled via the UI settings. */
  public boolean showSidebarSectionWhenEmpty;
}
