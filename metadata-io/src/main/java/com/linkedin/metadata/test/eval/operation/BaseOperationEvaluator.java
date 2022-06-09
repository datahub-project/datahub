package com.linkedin.metadata.test.eval.operation;

import com.linkedin.metadata.test.query.TestQueryResponse;


public interface BaseOperationEvaluator {
  /**
   * Operation being evaluated
   */
  String getOperation();

  /**
   * Validate params for the given operation
   *
   * @param params Parameters for evaluating operation
   * @throws IllegalArgumentException if parameters are not sufficient to evaluate the operation
   */
  void validate(OperationParams params) throws IllegalArgumentException;

  /**
   * Evaluate whether the operation passes given the query response and parameters
   */
  boolean evaluate(TestQueryResponse queryResponse, OperationParams params);
}
