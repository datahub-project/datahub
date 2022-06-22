package com.linkedin.metadata.test.definition.operation;

import com.linkedin.metadata.test.definition.TestQuery;
import lombok.Value;


/**
 * Parameter with a query that is resolved to return a query response
 */
@Value
public class QueryParam implements OperationParam {
  TestQuery query;

  public QueryParam(String queryString) {
    query = new TestQuery(queryString);
  }

  @Override
  public Type getType() {
    return Type.QUERY;
  }
}
