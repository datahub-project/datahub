package com.linkedin.metadata.config;

import lombok.Data;

@Data
public class ApplicationConfig {
  /**
   * Whether to show the application sidebar section even when empty - will add noise to the UI for
   * teams that don't use applications.
   */
  public boolean showSidebarSectionWhenEmpty;
}
