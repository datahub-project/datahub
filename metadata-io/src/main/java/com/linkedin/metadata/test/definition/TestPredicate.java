package com.linkedin.metadata.test.definition;

public interface TestPredicate {
  /**
   * Any test rule can be negated. Function to indicate whether the rule should be negated or not
   */
  boolean isNegated();
}
