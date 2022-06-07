package com.linkedin.metadata.test.eval;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.test.config.UnitTestRule;
import com.linkedin.metadata.test.eval.operation.BaseOperationEvaluator;
import com.linkedin.metadata.test.eval.operation.EqualsEvaluator;
import com.linkedin.metadata.test.eval.operation.ExistsEvaluator;
import com.linkedin.metadata.test.query.TestQueryResponse;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


public class UnitTestRuleEvaluator {

  private final Map<String, BaseOperationEvaluator> operationEvaluators;
  private static final UnitTestRuleEvaluator INSTANCE = new UnitTestRuleEvaluator();

  public UnitTestRuleEvaluator() {
    this(ImmutableList.of(new EqualsEvaluator(), new ExistsEvaluator()));
  }

  public UnitTestRuleEvaluator(List<BaseOperationEvaluator> operationEvaluators) {
    this.operationEvaluators = operationEvaluators.stream()
        .collect(Collectors.toMap(BaseOperationEvaluator::getOperation, Function.identity()));
  }

  public static UnitTestRuleEvaluator getInstance() {
    return INSTANCE;
  }

  public void validate(UnitTestRule testRule) throws IllegalArgumentException {
    if (!operationEvaluators.containsKey(testRule.getOperation())) {
      throw new IllegalArgumentException(String.format("Unsupported test operation %s", testRule.getOperation()));
    }
    operationEvaluators.get(testRule.getOperation()).validate(testRule.getParams());
  }

  public boolean evaluate(TestQueryResponse queryResponse, UnitTestRule testRule) {
    if (!operationEvaluators.containsKey(testRule.getOperation())) {
      throw new IllegalArgumentException(String.format("Unsupported test operation %s", testRule.getOperation()));
    }
    return operationEvaluators.get(testRule.getOperation()).evaluate(queryResponse, testRule.getParams());
  }

  public boolean isOperationValid(String operation) {
    return operationEvaluators.containsKey(operation);
  }
}
