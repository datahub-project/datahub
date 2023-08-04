package io.datahub.test.eval.operator;

import io.datahub.test.definition.operator.Operands;
import io.datahub.test.definition.operator.OperatorType;
import io.datahub.test.eval.ResolvedOperands;
import io.datahub.test.exception.InvalidOperandException;


public abstract class BaseOperatorEvaluator {
  /**
   * Operation being evaluated
   */
  public abstract OperatorType getOperatorType();

  /**
   * Validate operands for the given operation
   *
   * @param operands Parameters for evaluating operation
   * @throws InvalidOperandException if parameters are not sufficient to evaluate the operation
   */
  public abstract void validate(Operands operands) throws InvalidOperandException;

  /**
   * Evaluate whether the operation passes given the resolved parameter values
   */
  public abstract Object evaluate(ResolvedOperands resolvedOperands) throws InvalidOperandException;
}
