package com.linkedin.metadata.test.eval;

import com.linkedin.metadata.test.config.CompositeTestRule;
import com.linkedin.metadata.test.config.TestRule;
import com.linkedin.metadata.test.config.UnitTestRule;
import com.linkedin.metadata.test.query.TestQuery;
import com.linkedin.metadata.test.query.TestQueryResponse;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RequiredArgsConstructor
public class TestRuleEvaluator {
  private final UnitTestRuleEvaluator _unitTestRuleEvaluator;

  public boolean evaluate(Map<TestQuery, TestQueryResponse> batchQueryResponse, TestRule testRule) {
    boolean result;
    if (testRule instanceof CompositeTestRule) {
      CompositeTestRule compositeTestRule = (CompositeTestRule) testRule;
      if (!compositeTestRule.getOr().isEmpty()) {
        result = compositeTestRule.getOr().stream().anyMatch(rule -> evaluate(batchQueryResponse, rule));
      } else if (!compositeTestRule.getAnd().isEmpty()) {
        result = compositeTestRule.getAnd().stream().allMatch(rule -> evaluate(batchQueryResponse, rule));
      } else {
        // Both or and and clauses are empty -> always true
        result = true;
      }
    } else if (testRule instanceof UnitTestRule) {
      UnitTestRule unitTestRule = (UnitTestRule) testRule;
      TestQuery query = new TestQuery(unitTestRule.getQuery());
      result = _unitTestRuleEvaluator.evaluate(batchQueryResponse.getOrDefault(query, TestQueryResponse.empty()),
          unitTestRule);
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported test rule type %s", testRule.getClass().getSimpleName()));
    }

    if (testRule.isNegated()) {
      return !result;
    }
    return result;
  }
}
