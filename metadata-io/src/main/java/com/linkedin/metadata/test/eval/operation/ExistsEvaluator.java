package com.linkedin.metadata.test.eval.operation;

import com.linkedin.metadata.test.query.TestQueryResponse;
import java.util.Map;


public class ExistsEvaluator implements BaseOperationEvaluator {
  @Override
  public String getOperation() {
    return "exists";
  }

  @Override
  public void validate(Map<String, Object> params) throws IllegalArgumentException {
  }

  @Override
  public boolean evaluate(TestQueryResponse queryResponse, Map<String, Object> params) {
    return !queryResponse.getValues().isEmpty();
  }
}
