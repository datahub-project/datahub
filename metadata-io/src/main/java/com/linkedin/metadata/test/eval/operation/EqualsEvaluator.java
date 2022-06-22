package com.linkedin.metadata.test.eval.operation;

import com.linkedin.metadata.test.definition.operation.OperationParam;
import com.linkedin.metadata.test.definition.operation.OperationParams;
import com.linkedin.metadata.test.eval.ResolvedParam;
import com.linkedin.metadata.test.eval.ResolvedParams;
import com.linkedin.metadata.test.exception.OperationParamsInvalidException;
import com.linkedin.metadata.test.query.TestQueryResponse;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.linkedin.metadata.test.definition.operation.ParamKeyConstants.QUERY;
import static com.linkedin.metadata.test.definition.operation.ParamKeyConstants.VALUE;
import static com.linkedin.metadata.test.definition.operation.ParamKeyConstants.VALUES;


/**
 * Equals operation evaluator. Checks whether the query response is equal to any of the input values
 */
public class EqualsEvaluator extends BaseOperationEvaluator {

  @Override
  public String getOperation() {
    return "equals";
  }

  @Override
  public void validate(OperationParams params) throws OperationParamsInvalidException {
    if (!params.hasKeyOfType(QUERY, OperationParam.Type.QUERY)) {
      throw new OperationParamsInvalidException(
          "Invalid params for the equals operation: Need to have query field set");
    }
    if (!params.hasKeyOfType(VALUES, OperationParam.Type.STRING) && !params.hasKeyOfType(VALUE,
        OperationParam.Type.STRING)) {
      throw new OperationParamsInvalidException(
          "Invalid params for the equals operation: Need to have either \"values\" or \"value\" fields set");
    }
  }

  @Override
  public boolean evaluate(ResolvedParams resolvedParams) throws OperationParamsInvalidException {
    if (!resolvedParams.hasKeyOfType(QUERY, OperationParam.Type.QUERY)) {
      throw new OperationParamsInvalidException(
          "Invalid params for the equals operation: Need to have query field set");
    }
    if (!resolvedParams.hasKeyOfType(VALUES, OperationParam.Type.STRING) && !resolvedParams.hasKeyOfType(VALUE,
        OperationParam.Type.STRING)) {
      throw new OperationParamsInvalidException(
          "Invalid params for the equals operation: Need to have either \"values\" or \"value\" fields set");
    }

    TestQueryResponse queryResponse = resolvedParams.getResolvedParam(QUERY).getResolvedQueryParam();
    // If query response is empty, return false
    if (queryResponse == null) {
      return false;
    }
    List<String> queryValues = queryResponse.getValues();

    // Get values to compare
    ResolvedParam valuesParam = resolvedParams.getResolvedParam(VALUES);
    if (valuesParam == null) {
      valuesParam = resolvedParams.getResolvedParam(VALUE);
    }
    Set<String> valuesToCompare = new HashSet<>(valuesParam.getResolvedStringParam());

    return queryValues.stream().anyMatch(valuesToCompare::contains);
  }
}
