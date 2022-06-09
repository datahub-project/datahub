package com.linkedin.metadata.test.eval;

import com.linkedin.metadata.test.definition.CompositeTestPredicate;
import com.linkedin.metadata.test.definition.LeafTestPredicate;
import com.linkedin.metadata.test.definition.TestPredicate;
import com.linkedin.metadata.test.query.TestQuery;
import com.linkedin.metadata.test.query.TestQueryResponse;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * Class that evaluates test predicates given the query evaluation results
 */
@Slf4j
@RequiredArgsConstructor
public class TestPredicateEvaluator {
  private final LeafTestPredicateEvaluator _leafTestPredicateEvaluator;

  private boolean evaluateCompositePredicate(Map<TestQuery, TestQueryResponse> batchQueryResponse,
      CompositeTestPredicate compositeTestPredicate) {
    if (!compositeTestPredicate.getOr().isEmpty()) {
      return compositeTestPredicate.getOr().stream().anyMatch(rule -> evaluate(batchQueryResponse, rule));
    }
    if (!compositeTestPredicate.getAnd().isEmpty()) {
      return compositeTestPredicate.getAnd().stream().allMatch(rule -> evaluate(batchQueryResponse, rule));
    }
    // Both or and and clauses are empty -> always true
    return true;
  }

  private boolean evaluateLeafPredicate(Map<TestQuery, TestQueryResponse> batchQueryResponse,
      LeafTestPredicate leafTestPredicate) {
    TestQuery query = new TestQuery(leafTestPredicate.getQuery());
    return _leafTestPredicateEvaluator.evaluate(batchQueryResponse.getOrDefault(query, TestQueryResponse.empty()),
        leafTestPredicate);
  }

  /**
   * Evaluate the test predicate given the batched query evaluation results
   *
   * @param batchQueryResponse Batched query evaluation results
   * @param testPredicate Test predicate being evaluated
   * @return Whether or not the predicate passes given the query evaluation results
   */
  public boolean evaluate(Map<TestQuery, TestQueryResponse> batchQueryResponse, TestPredicate testPredicate) {
    boolean result;
    if (testPredicate instanceof CompositeTestPredicate) {
      result = evaluateCompositePredicate(batchQueryResponse, (CompositeTestPredicate) testPredicate);
    } else if (testPredicate instanceof LeafTestPredicate) {
      result = evaluateLeafPredicate(batchQueryResponse, (LeafTestPredicate) testPredicate);
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported test rule type %s", testPredicate.getClass().getSimpleName()));
    }

    if (testPredicate.isNegated()) {
      return !result;
    }
    return result;
  }
}
