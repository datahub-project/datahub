package com.linkedin.metadata.test.definition.expression;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.linkedin.metadata.test.definition.literal.DateLiteral;
import com.linkedin.metadata.test.definition.literal.StringListLiteral;
import com.linkedin.metadata.test.definition.operator.Predicate;
import com.linkedin.metadata.test.definition.value.ValueType;

/** An expression resolvers to some value. */
@JsonSubTypes({
  @JsonSubTypes.Type(value = Predicate.class, name = "Predicate"),
  @JsonSubTypes.Type(value = StringListLiteral.class, name = "StringListLiteral"),
  @JsonSubTypes.Type(value = DateLiteral.class, name = "DateLiteral"),
  @JsonSubTypes.Type(value = Query.class, name = "Query")
})
@JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
public interface Expression {
  /** Return the expression type associated with the expression. */
  ExpressionType expressionType();

  /** Return the value type associated with the expression. */
  ValueType valueType();
}
