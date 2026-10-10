package com.linkedin.metadata.config;

import lombok.Data;
import lombok.experimental.Accessors;

/** Startup configuration for aspect/path semantic no-op comparison. */
@Data
@Accessors(chain = true)
public class SemanticNoOpConfiguration {
  /** When false, rules are validated but not applied. */
  private boolean enabled = false;

  /**
   * JSON array of rules. Each rule names an aspect, a slash-delimited path ({@code *} is one array
   * element), and a strategy of {@code IGNORE} or {@code TIMESTAMP_WINDOW}.
   */
  private String rulesJson = "[]";
}
