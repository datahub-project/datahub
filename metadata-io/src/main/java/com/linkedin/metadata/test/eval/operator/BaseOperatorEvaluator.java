package com.linkedin.metadata.test.eval.operator;

import com.linkedin.metadata.test.definition.operator.Operands;
import com.linkedin.metadata.test.definition.operator.OperatorType;
import com.linkedin.metadata.test.eval.ResolvedOperands;
import com.linkedin.metadata.test.exception.InvalidOperandException;

public abstract class BaseOperatorEvaluator {
  /** Operation being evaluated */
  public abstract OperatorType getOperatorType();

  /**
   * Validate operands for the given operation
   *
   * @param operands Parameters for evaluating operation
   * @throws InvalidOperandException if parameters are not sufficient to evaluate the operation
   */
  public abstract void validate(Operands operands) throws InvalidOperandException;

  /** Evaluate whether the operation passes given the resolved parameter values */
  public abstract Object evaluate(ResolvedOperands resolvedOperands) throws InvalidOperandException;
}
