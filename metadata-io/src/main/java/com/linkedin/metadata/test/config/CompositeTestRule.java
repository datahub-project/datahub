package com.linkedin.metadata.test.config;

import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;


/**
 * DataHub Test Rule that combines multiple test rules using an AND or OR operation
 */
@EqualsAndHashCode
@Getter
public class CompositeTestRule implements TestRule {
  private final List<TestRule> or;
  private final List<TestRule> and;
  private final boolean negated;

  public enum CompositionOperation {
    OR, AND
  }

  public CompositeTestRule(CompositionOperation operation, List<TestRule> testRules) {
    this(operation, testRules, false);
  }

  public CompositeTestRule(CompositionOperation operation, List<TestRule> testRules, boolean negated) {
    if (operation == CompositionOperation.OR) {
      or = testRules;
      and = Collections.emptyList();
    } else {
      or = Collections.emptyList();
      and = testRules;
    }
    this.negated = negated;
  }
}
