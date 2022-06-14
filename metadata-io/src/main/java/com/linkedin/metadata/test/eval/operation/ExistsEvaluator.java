package com.linkedin.metadata.test.eval.operation;

import com.linkedin.metadata.test.definition.TestPredicate;
import com.linkedin.metadata.test.definition.TestQuery;
import com.linkedin.metadata.test.definition.operation.OperationParams;
import com.linkedin.metadata.test.exception.OperationParamsInvalidException;
import com.linkedin.metadata.test.query.TestQueryResponse;
import java.util.Collections;
import java.util.Map;
import java.util.Set;


/**
 * Exists operation evaluator. Checks whether the query response exists
 */
public class ExistsEvaluator extends BaseOperationEvaluator {
  @Override
  public String getOperation() {
    return "exists";
  }

  @Override
  public void validate(OperationParams params) throws OperationParamsInvalidException {
  }

  @Override
  public boolean evaluate(Map<TestQuery, TestQueryResponse> batchedQueryResponse, TestPredicate testPredicate) {
    return !batchedQueryResponse.getOrDefault(testPredicate.getQuery(), TestQueryResponse.empty())
        .getValues()
        .isEmpty();
  }

  @Override
  public Set<TestQuery> getRequiredQueries(TestPredicate testPredicate) {
    return Collections.singleton(testPredicate.getQuery());
  }
}
