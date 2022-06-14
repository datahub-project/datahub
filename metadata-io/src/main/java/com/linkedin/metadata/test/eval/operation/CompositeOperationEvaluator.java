package com.linkedin.metadata.test.eval.operation;

import com.linkedin.metadata.test.definition.TestPredicate;
import com.linkedin.metadata.test.definition.TestQuery;
import com.linkedin.metadata.test.definition.operation.OperationParam;
import com.linkedin.metadata.test.definition.operation.OperationParams;
import com.linkedin.metadata.test.definition.operation.PredicateParam;
import com.linkedin.metadata.test.exception.OperationParamsInvalidException;
import com.linkedin.metadata.test.query.TestQueryResponse;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.linkedin.metadata.test.definition.operation.ParamKeyConstants.PREDICATES;


public abstract class CompositeOperationEvaluator extends BaseOperationEvaluator {

  @Override
  public void validate(OperationParams params) throws OperationParamsInvalidException {
    if (params.hasKeyOfType(PREDICATES, OperationParam.Type.PREDICATE)) {
      return;
    }
    throw new OperationParamsInvalidException(
        String.format("%s operation requires param predicates containing the list of predicates to compose",
            getOperation()));
  }

  /**
   * Return the combined result of evaluating child predicates
   */
  protected abstract boolean combinePredicates(Map<TestQuery, TestQueryResponse> batchedQueryResponse,
      List<TestPredicate> childPredicates);

  @Override
  public final boolean evaluate(Map<TestQuery, TestQueryResponse> batchedQueryResponse, TestPredicate testPredicate)
      throws OperationParamsInvalidException {
    if (getTestPredicateEvaluator() == null) {
      throw new IllegalStateException(
          String.format("Test predicate evaluator is not set before evaluating %s operation", getOperation()));
    }

    Optional<PredicateParam> param = testPredicate.getParams().getParamOfType(PREDICATES, PredicateParam.class);
    if (!param.isPresent()) {
      throw new OperationParamsInvalidException(
          String.format("%s operation requires param predicates containing the list of predicates to compose",
              getOperation()));
    }
    List<TestPredicate> childPredicates = param.get().getPredicates();
    return combinePredicates(batchedQueryResponse, childPredicates);
  }

  @Override
  public final Set<TestQuery> getRequiredQueries(TestPredicate testPredicate) {
    if (getTestPredicateEvaluator() == null) {
      throw new IllegalStateException(
          String.format("Test predicate evaluator is not set before evaluating %s operation", getOperation()));
    }

    return testPredicate.getParams()
        .getParamOfType(PREDICATES, PredicateParam.class)
        .map(PredicateParam::getPredicates)
        .orElse(Collections.emptyList())
        .stream()
        .flatMap(childPredicate -> getTestPredicateEvaluator().getRequiredQueries(childPredicate).stream())
        .collect(Collectors.toSet());
  }
}
