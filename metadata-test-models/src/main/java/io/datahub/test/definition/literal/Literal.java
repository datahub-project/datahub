package io.datahub.test.definition.literal;

import io.datahub.test.definition.expression.Expression;
import io.datahub.test.definition.expression.ExpressionType;


/**
 * A literal is an {@link Expression} that is directly convertible to a literal value.
 */
public interface Literal extends Expression {
  /**
   * The literal value
   */
  Object value();

  @Override
  default ExpressionType expressionType() {
    return ExpressionType.LITERAL;
  }
}
