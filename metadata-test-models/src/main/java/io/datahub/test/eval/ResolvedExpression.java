package io.datahub.test.eval;

import io.datahub.test.definition.expression.Expression;
import io.datahub.test.definition.expression.ExpressionType;
import lombok.Getter;


/**
 * A wrapper around {@link Expression} which contains its resolved value, represented
 * as an {@link Object}.
 */
public class ResolvedExpression {
  @Getter
  private final Expression expression; // The original expression.

  /**
   * The resolved value
   */
  private Object resolvedValue;

  // Constructor for predicate param
  public ResolvedExpression(Expression exp, Object value) {
    expression = exp;
    resolvedValue = value;
  }

  public ExpressionType getExpressionType() {
    return expression.expressionType();
  }

  public Object getValue() {
    return this.resolvedValue;
  }
}
