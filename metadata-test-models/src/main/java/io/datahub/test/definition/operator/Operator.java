package io.datahub.test.definition.operator;

import io.datahub.test.definition.expression.Expression;


/**
 * An operator is an {@link Expression} which is resolved by performing
 * an operation against a set of input operands.
 */
public interface Operator extends Expression {
  /**
   * Operation type to evaluate the rule e.g. equals
   */
  OperatorType operatorType();

  /**
   * The operands required for the operator
   */
  Operands operands();
}
