package io.datahub.test.definition.expression;

import io.datahub.test.definition.operator.Operands;
import io.datahub.test.definition.operator.Operator;
import io.datahub.test.definition.operator.OperatorType;
import io.datahub.test.definition.value.ListType;
import io.datahub.test.definition.value.StringType;
import io.datahub.test.definition.value.ValueType;
import io.datahub.test.query.TestQuery;
import lombok.ToString;
import lombok.Value;


/**
 * Parameter with a query that is resolved to return a query response.
 *
 * It always resolves to a string list of values.
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
