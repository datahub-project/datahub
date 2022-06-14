package com.linkedin.metadata.test.eval;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.test.definition.TestPredicate;
import com.linkedin.metadata.test.definition.TestQuery;
import com.linkedin.metadata.test.eval.operation.BaseOperationEvaluator;
import com.linkedin.metadata.test.eval.operation.EqualsEvaluator;
import com.linkedin.metadata.test.eval.operation.ExistsEvaluator;
import com.linkedin.metadata.test.query.TestQueryResponse;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;


public class TestPredicateEvaluator {

  private final Map<String, BaseOperationEvaluator> operationEvaluators;
  private static final TestPredicateEvaluator INSTANCE = new TestPredicateEvaluator();

  public TestPredicateEvaluator() {
    this(ImmutableList.of(new EqualsEvaluator(), new ExistsEvaluator()));
  }

  public TestPredicateEvaluator(List<BaseOperationEvaluator> operationEvaluators) {
    this.operationEvaluators = operationEvaluators.stream()
        .collect(Collectors.toMap(BaseOperationEvaluator::getOperation, Function.identity()));
    operationEvaluators.forEach(evaluator -> evaluator.setTestPredicateEvaluator(this));
  }

  public static TestPredicateEvaluator getInstance() {
    return INSTANCE;
  }

  private BaseOperationEvaluator getOperationEvaluator(String operation) {
    if (!operationEvaluators.containsKey(operation)) {
      throw new IllegalArgumentException(String.format("Unsupported test operation %s", operation));
    }
    return operationEvaluators.get(operation);
  }

  public void validate(TestPredicate testPredicate) throws IllegalArgumentException {
    getOperationEvaluator(testPredicate.getOperation()).validate(testPredicate.getParams());
  }

  public boolean evaluate(Map<TestQuery, TestQueryResponse> batchedQueryResponse, TestPredicate testPredicate) {
    return getOperationEvaluator(testPredicate.getOperation()).evaluate(batchedQueryResponse, testPredicate);
  }

  public Set<TestQuery> getRequiredQueries(TestPredicate testPredicate) {
    return getOperationEvaluator(testPredicate.getOperation()).getRequiredQueries(testPredicate);
  }

  public boolean isOperationValid(String operation) {
    return operationEvaluators.containsKey(operation);
  }
}
