package com.linkedin.metadata.test.eval;

import com.linkedin.metadata.test.definition.operation.OperationParam;
import com.linkedin.metadata.test.definition.operation.PredicateParam;
import com.linkedin.metadata.test.definition.operation.QueryParam;
import com.linkedin.metadata.test.definition.operation.StringParam;
import com.linkedin.metadata.test.query.TestQueryResponse;
import java.util.List;
import lombok.Getter;


public class ResolvedParam {
  @Getter
  private final OperationParam operationParam;

  /**
   * Union of the resolved values of each operation param
   */
  private List<Boolean> resolvedPredicateParam;
  private List<String> resolvedStringParam;
  private TestQueryResponse resolvedQueryParam;

  // Constructor for predicate param
  public ResolvedParam(PredicateParam predicateParam, List<Boolean> evaluatedPredicates) {
    operationParam = predicateParam;
    resolvedPredicateParam = evaluatedPredicates;
  }

  // Constructor for query param
  public ResolvedParam(QueryParam queryParam, TestQueryResponse queryResponse) {
    operationParam = queryParam;
    resolvedQueryParam = queryResponse;
  }

  // Constructor for string param. Note, string param does not require resolution
  public ResolvedParam(StringParam stringParam) {
    operationParam = stringParam;
    resolvedStringParam = stringParam.getValues();
  }

  public OperationParam.Type getType() {
    return operationParam.getType();
  }

  public List<Boolean> getResolvedPredicateParam() {
    if (getType() != OperationParam.Type.PREDICATE) {
      throw new IllegalArgumentException(
          "Cannot retrieve resolved predicate parameters from a non-predicate operation param");
    }
    return resolvedPredicateParam;
  }

  public TestQueryResponse getResolvedQueryParam() {
    if (getType() != OperationParam.Type.QUERY) {
      throw new IllegalArgumentException("Cannot retrieve resolved query parameters from a non-query operation param");
    }
    return resolvedQueryParam;
  }

  public List<String> getResolvedStringParam() {
    if (getType() != OperationParam.Type.STRING) {
      throw new IllegalArgumentException(
          "Cannot retrieve resolved string parameters from a non-string operation param");
    }
    return resolvedStringParam;
  }
}
