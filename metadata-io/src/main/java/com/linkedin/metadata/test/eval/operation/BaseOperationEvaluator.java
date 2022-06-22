package com.linkedin.metadata.test.eval.operation;

import com.linkedin.metadata.test.definition.operation.OperationParams;
import com.linkedin.metadata.test.eval.ResolvedParams;
import com.linkedin.metadata.test.exception.OperationParamsInvalidException;


public abstract class BaseOperationEvaluator {
  /**
   * Operation being evaluated
   */
  public abstract String getOperation();

  /**
   * Validate params for the given operation
   *
   * @param params Parameters for evaluating operation
   * @throws OperationParamsInvalidException if parameters are not sufficient to evaluate the operation
   */
  public abstract void validate(OperationParams params) throws OperationParamsInvalidException;

  /**
   * Evaluate whether the operation passes given the resolved parameter values
   */
  public abstract boolean evaluate(ResolvedParams resolvedParams) throws OperationParamsInvalidException;
}
