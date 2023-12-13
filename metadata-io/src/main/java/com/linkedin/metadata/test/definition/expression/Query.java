package com.linkedin.metadata.test.definition.expression;

import com.linkedin.metadata.test.definition.operator.Operands;
import com.linkedin.metadata.test.definition.operator.Operator;
import com.linkedin.metadata.test.definition.operator.OperatorType;
import com.linkedin.metadata.test.definition.value.ListType;
import com.linkedin.metadata.test.definition.value.StringType;
import com.linkedin.metadata.test.definition.value.ValueType;
import com.linkedin.metadata.test.query.TestQuery;
import lombok.ToString;
import lombok.Value;

/**
 * Parameter with a query that is resolved to return a query response.
 *
 * <p>It always resolves to a string list of values.
 */
@Value
@ToString
public class Query implements Operator {
  TestQuery query;

  public Query(String queryString) {
    query = new TestQuery(queryString);
  }

  @Override
  public ExpressionType expressionType() {
    return ExpressionType.QUERY;
  }

  @Override
  public ValueType valueType() {
    return new ListType(new StringType());
  }

  @Override
  public OperatorType operatorType() {
    return OperatorType.QUERY;
  }

  @Override
  public Operands operands() {
    return null;
  }
}
