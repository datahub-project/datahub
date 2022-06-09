package com.linkedin.metadata.test.eval;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.test.definition.LeafTestPredicate;
import com.linkedin.metadata.test.eval.operation.BaseOperationEvaluator;
import com.linkedin.metadata.test.eval.operation.EqualsEvaluator;
import com.linkedin.metadata.test.eval.operation.ExistsEvaluator;
import com.linkedin.metadata.test.eval.operation.OperationParams;
import com.linkedin.metadata.test.query.TestQueryResponse;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


public class LeafTestPredicateEvaluator {

  private final Map<String, BaseOperationEvaluator> operationEvaluators;
  private static final LeafTestPredicateEvaluator INSTANCE = new LeafTestPredicateEvaluator();

  public LeafTestPredicateEvaluator() {
    this(ImmutableList.of(new EqualsEvaluator(), new ExistsEvaluator()));
  }

  public LeafTestPredicateEvaluator(List<BaseOperationEvaluator> operationEvaluators) {
    this.operationEvaluators = operationEvaluators.stream()
        .collect(Collectors.toMap(BaseOperationEvaluator::getOperation, Function.identity()));
  }

  public static LeafTestPredicateEvaluator getInstance() {
    return INSTANCE;
  }

  public void validate(LeafTestPredicate testRule) throws IllegalArgumentException {
    if (!operationEvaluators.containsKey(testRule.getOperation())) {
      throw new IllegalArgumentException(String.format("Unsupported test operation %s", testRule.getOperation()));
    }
    operationEvaluators.get(testRule.getOperation()).validate(new OperationParams(testRule.getParams()));
  }

  public boolean evaluate(TestQueryResponse queryResponse, LeafTestPredicate testRule) {
    if (!operationEvaluators.containsKey(testRule.getOperation())) {
      throw new IllegalArgumentException(String.format("Unsupported test operation %s", testRule.getOperation()));
    }
    return operationEvaluators.get(testRule.getOperation())
        .evaluate(queryResponse, new OperationParams(testRule.getParams()));
  }

  public boolean isOperationValid(String operation) {
    return operationEvaluators.containsKey(operation);
  }
}
