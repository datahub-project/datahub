package com.linkedin.metadata.test.config;

public interface TestRule {
  /**
   * Any test rule can be negated. Function to indicate whether the rule should be negated or not
   */
  boolean isNegated();
}
