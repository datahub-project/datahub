package com.linkedin.metadata.test.eval.operation;

import com.linkedin.metadata.test.definition.TestPredicate;
import com.linkedin.metadata.test.definition.TestQuery;
import com.linkedin.metadata.test.definition.operation.OperationParam;
import com.linkedin.metadata.test.definition.operation.OperationParams;
import com.linkedin.metadata.test.definition.operation.StringListParam;
import com.linkedin.metadata.test.definition.operation.StringParam;
import com.linkedin.metadata.test.exception.OperationParamsInvalidException;
import com.linkedin.metadata.test.query.TestQueryResponse;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
    if (params.hasKeyOfType(VALUES, OperationParam.Type.STRING_LIST) || params.hasKeyOfType(VALUE,
        OperationParam.Type.STRING)) {
      return;
    }
    throw new OperationParamsInvalidException(
        "Invalid params for the EQUALS operation: Need to have either values or value fields set");
  }

  @Override
  public boolean evaluate(Map<TestQuery, TestQueryResponse> batchedQueryResponse, TestPredicate testPredicate)
      throws OperationParamsInvalidException {
    Set<String> compareAgainst;
    Optional<StringListParam> valuesParam = testPredicate.getParams().getParamOfType(VALUES, StringListParam.class);
    Optional<StringParam> valueParam = testPredicate.getParams().getParamOfType(VALUE, StringParam.class);
    if (valuesParam.isPresent()) {
      compareAgainst = new HashSet<>(valuesParam.get().getValues());
    } else if (valueParam.isPresent()) {
      compareAgainst = Collections.singleton(valueParam.get().getValue());
    } else {
      throw new OperationParamsInvalidException(
          "Invalid params for the EQUALS operation: Need to have either values or value fields set");
    }

    // If query response is empty, return false
    if (!batchedQueryResponse.containsKey(testPredicate.getQuery())) {
      return false;
    }

    return batchedQueryResponse.get(testPredicate.getQuery()).getValues().stream().anyMatch(compareAgainst::contains);
  }

  @Override
  public Set<TestQuery> getRequiredQueries(TestPredicate testPredicate) {
    return Collections.singleton(testPredicate.getQuery());
  }
}
