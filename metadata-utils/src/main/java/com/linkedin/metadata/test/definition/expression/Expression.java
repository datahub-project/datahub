package com.linkedin.metadata.test.definition.expression;

import com.linkedin.metadata.test.definition.value.ValueType;

/** An expression resolvers to some value. */
public interface Expression {
  /** Return the expression type associated with the expression. */
  ExpressionType expressionType();

  /** Return the value type associated with the expression. */
  ValueType valueType();
}
