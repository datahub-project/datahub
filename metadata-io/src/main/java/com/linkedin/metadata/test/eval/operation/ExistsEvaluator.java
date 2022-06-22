package com.linkedin.metadata.test.eval.operation;

import com.linkedin.metadata.test.definition.operation.OperationParam;
import com.linkedin.metadata.test.definition.operation.OperationParams;
import com.linkedin.metadata.test.eval.ResolvedParams;
import com.linkedin.metadata.test.exception.OperationParamsInvalidException;
import com.linkedin.metadata.test.query.TestQueryResponse;
import org.apache.commons.lang3.StringUtils;

import static com.linkedin.metadata.test.definition.operation.ParamKeyConstants.QUERY;


/**
 * Exists operation evaluator. Checks whether the query response exists
 */
public class ExistsEvaluator extends BaseOperationEvaluator {
  @Override
  public String getOperation() {
    return "exists";
  }

  @Override
  public void validate(OperationParams params) throws OperationParamsInvalidException {
    if (!params.hasKeyOfType(QUERY, OperationParam.Type.QUERY)) {
      throw new OperationParamsInvalidException(
          "Invalid params for the exists operation: Need to have query field set");
    }
  }

  @Override
  public boolean evaluate(ResolvedParams resolvedParams) throws OperationParamsInvalidException {
    if (!resolvedParams.hasKeyOfType(QUERY, OperationParam.Type.QUERY)) {
      throw new OperationParamsInvalidException(
          "Invalid params for the exists operation: Need to have query field set");
    }
    TestQueryResponse queryResponse = resolvedParams.getResolvedParam(QUERY).getResolvedQueryParam();
    // If query response is empty, return false
    if (queryResponse == null) {
      return false;
    }
    // Return true only if there is a non-empty value
    return queryResponse.getValues().stream().anyMatch(StringUtils::isNotEmpty);
  }
}
