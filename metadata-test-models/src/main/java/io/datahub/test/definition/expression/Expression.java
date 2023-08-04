package io.datahub.test.definition.expression;

import io.datahub.test.definition.value.ValueType;


/**
 * An expression resolvers to some value.
 */
public interface Expression {
  /**
   * Return the expression type associated with the expression.
   */
  ExpressionType expressionType();

  /**
   * Return the value type associated with the expression.
   */
  ValueType valueType();
}
