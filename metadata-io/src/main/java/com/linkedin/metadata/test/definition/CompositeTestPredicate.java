package com.linkedin.metadata.test.definition;

import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;


/**
 * DataHub Test Rule that combines multiple test predicates using an AND or OR operation
 */
@EqualsAndHashCode
@Getter
public class CompositeTestPredicate implements TestPredicate {
  private final List<TestPredicate> or;
  private final List<TestPredicate> and;
  private final boolean negated;

  public enum CompositionOperation {
    OR, AND
  }

  public CompositeTestPredicate(CompositionOperation operation, List<TestPredicate> testPredicates) {
    this(operation, testPredicates, false);
  }

  public CompositeTestPredicate(CompositionOperation operation, List<TestPredicate> testPredicates, boolean negated) {
    if (operation == CompositionOperation.OR) {
      or = testPredicates;
      and = Collections.emptyList();
    } else {
      or = Collections.emptyList();
      and = testPredicates;
    }
    this.negated = negated;
  }
}
