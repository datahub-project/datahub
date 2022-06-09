package com.linkedin.metadata.test.eval.operation;

import com.linkedin.metadata.test.query.TestQueryResponse;


public class ExistsEvaluator implements BaseOperationEvaluator {
  @Override
  public String getOperation() {
    return "exists";
  }

  @Override
  public void validate(OperationParams params) throws IllegalArgumentException {
  }

  @Override
  public boolean evaluate(TestQueryResponse queryResponse, OperationParams params) {
    return !queryResponse.getValues().isEmpty();
  }
}
