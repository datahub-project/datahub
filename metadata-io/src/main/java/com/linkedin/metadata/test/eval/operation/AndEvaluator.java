package com.linkedin.metadata.test.eval.operation;

import com.linkedin.metadata.test.definition.TestPredicate;
import com.linkedin.metadata.test.definition.TestQuery;
import com.linkedin.metadata.test.query.TestQueryResponse;
import java.util.List;
import java.util.Map;


/**
 * and operation evaluator. Checks whether any of input predicates returns true
 */
public class AndEvaluator extends CompositeOperationEvaluator {
  @Override
  public String getOperation() {
    return "and";
  }

  @Override
  protected boolean combinePredicates(Map<TestQuery, TestQueryResponse> batchedQueryResponse,
      List<TestPredicate> childPredicates) {
    return childPredicates.stream()
        .allMatch(childPredicate -> getTestPredicateEvaluator().evaluate(batchedQueryResponse, childPredicate));
  }
}
