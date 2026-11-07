package com.linkedin.metadata.config;

import lombok.Data;
import lombok.experimental.Accessors;

/** Startup configuration for aspect/path semantic no-op comparison. */
@Data
@Accessors(chain = true)
public class SemanticNoOpConfiguration {
  /** When false, rules are validated but not applied. Stock default is set in application.yaml. */
  private boolean enabled = false;

  /**
   * JSON array of rules. Each rule names an aspect, a slash-delimited path ({@code *} is one array
   * element), and a strategy of {@code IGNORE}, {@code TIMESTAMP_WINDOW}, or {@code UNORDERED}.
   * {@code UNORDERED} applies to an array and pairs elements without regard to position. A rule
   * whose aspect is not in the registry is skipped. The stock list is {@code
   * entityService.semanticNoOp.defaultRulesJson} in application.yaml. {@code []} compares strictly.
   */
  private String rulesJson = "[]";
}
