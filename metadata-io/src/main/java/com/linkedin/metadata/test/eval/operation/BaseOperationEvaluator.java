package com.linkedin.metadata.test.eval.operation;

import com.linkedin.metadata.test.definition.TestPredicate;
import com.linkedin.metadata.test.definition.TestQuery;
import com.linkedin.metadata.test.definition.operation.OperationParams;
import com.linkedin.metadata.test.eval.TestPredicateEvaluator;
import com.linkedin.metadata.test.exception.OperationParamsInvalidException;
import com.linkedin.metadata.test.query.TestQueryResponse;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;


public abstract class BaseOperationEvaluator {
  @Getter
  @Setter
  private TestPredicateEvaluator testPredicateEvaluator;

  /**
   * Operation being evaluated
   */
  public abstract String getOperation();

  /**
   * Validate params for the given operation
   *
   * @param params Parameters for evaluating operation
   * @throws IllegalArgumentException if parameters are not sufficient to evaluate the operation
   */
  public abstract void validate(OperationParams params) throws OperationParamsInvalidException;

  /**
   * Evaluate whether the operation passes given the batched query response and predicate
   */
  public abstract boolean evaluate(Map<TestQuery, TestQueryResponse> batchedQueryResponse, TestPredicate predicate)
      throws OperationParamsInvalidException;

  /**
   * Set of queries to evaluate to evaluate input predicate
   */
  public abstract Set<TestQuery> getRequiredQueries(TestPredicate testPredicate);
}
