package com.linkedin.metadata.test.eval;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.test.definition.TestPredicate;
import com.linkedin.metadata.test.definition.TestQuery;
import com.linkedin.metadata.test.definition.operation.OperationParam;
import com.linkedin.metadata.test.definition.operation.OperationParams;
import com.linkedin.metadata.test.definition.operation.PredicateParam;
import com.linkedin.metadata.test.definition.operation.QueryParam;
import com.linkedin.metadata.test.definition.operation.StringParam;
import com.linkedin.metadata.test.eval.operation.AndEvaluator;
import com.linkedin.metadata.test.eval.operation.BaseOperationEvaluator;
import com.linkedin.metadata.test.eval.operation.EqualsEvaluator;
import com.linkedin.metadata.test.eval.operation.ExistsEvaluator;
import com.linkedin.metadata.test.eval.operation.OrEvaluator;
import com.linkedin.metadata.test.exception.OperationParamsInvalidException;
import com.linkedin.metadata.test.query.TestQueryResponse;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Class that evaluates all test predicates given the batched query responses
 */
public class TestPredicateEvaluator {

  private final Map<String, BaseOperationEvaluator> operationEvaluators;
  private static final TestPredicateEvaluator INSTANCE = new TestPredicateEvaluator();

  public TestPredicateEvaluator() {
    this(ImmutableList.of(new EqualsEvaluator(), new ExistsEvaluator(), new OrEvaluator(), new AndEvaluator()));
  }

  public TestPredicateEvaluator(List<BaseOperationEvaluator> operationEvaluators) {
    this.operationEvaluators = operationEvaluators.stream()
        .collect(Collectors.toMap(BaseOperationEvaluator::getOperation, Function.identity()));
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

  /**
   * Validate the test predicate. Make sure it has the set of parameters required to evaluate the operator
   *
   * @param testPredicate Test predicate to validate
   * @throws OperationParamsInvalidException if parameters are not sufficient to evaluate the operation
   */
  public void validate(TestPredicate testPredicate) throws OperationParamsInvalidException {
    getOperationEvaluator(testPredicate.getOperation()).validate(testPredicate.getParams());
  }

  /**
   * Evaluate the input test predicate given the batched query responses
   *
   * @param testPredicate Test predicate to evaluate
   * @param batchedQueryResponse Batched query responses containing the query responses of all queries
   *                             required by the predicate
   * @return whether or not the predicate passed
   */
  public boolean evaluate(TestPredicate testPredicate, Map<TestQuery, TestQueryResponse> batchedQueryResponse) {
    // 1. Resolve all parameter values.
    // i.e. get correct query response for query parameter, evaluate predicate for predicate parameter
    ResolvedParams resolvedParams = resolveParams(testPredicate.getParams(), batchedQueryResponse);
    // 2. Evaluate predicate by passing in the resolved params
    boolean evaluatedResult = getOperationEvaluator(testPredicate.getOperation()).evaluate(resolvedParams);
    if (testPredicate.isNegated()) {
      return !evaluatedResult;
    }
    return evaluatedResult;
  }

  private ResolvedParams resolveParams(OperationParams operationParams,
      Map<TestQuery, TestQueryResponse> batchedQueryResponse) {
    return new ResolvedParams(operationParams.getParams()
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> resolveParam(entry.getValue(), batchedQueryResponse))));
  }

  private ResolvedParam resolveParam(OperationParam operationParam,
      Map<TestQuery, TestQueryResponse> batchedQueryResponse) {
    if (operationParam instanceof PredicateParam) {
      // For a predicate param, recursively evaluate the child predicates
      PredicateParam predicateParam = (PredicateParam) operationParam;
      return new ResolvedParam(predicateParam, predicateParam.getPredicates()
          .stream()
          .map(childPredicate -> evaluate(childPredicate, batchedQueryResponse))
          .collect(Collectors.toList()));
    }
    if (operationParam instanceof QueryParam) {
      // For a query param, fetch the query response from the batched responses
      QueryParam queryParam = (QueryParam) operationParam;
      return new ResolvedParam(queryParam,
          batchedQueryResponse.getOrDefault(queryParam.getQuery(), TestQueryResponse.empty()));
    }
    if (operationParam instanceof StringParam) {
      return new ResolvedParam((StringParam) operationParam);
    }
    throw new IllegalArgumentException(
        String.format("Unsupported operation param type: %s", operationParam.getClass().getSimpleName()));
  }

  /**
   * Get all required queries to be resolved by the query engine for the input test predicate
   */
  public Set<TestQuery> getRequiredQueries(TestPredicate testPredicate) {
    // If the predicate has any query params, return the queries in the params
    List<QueryParam> queryParams = testPredicate.getParams().getAllParamsOfType(QueryParam.class);
    if (!queryParams.isEmpty()) {
      return queryParams.stream().map(QueryParam::getQuery).collect(Collectors.toSet());
    }

    // If the predicate has any predicate params, recursively call function
    // to fetch the required queries of each predicate param
    List<PredicateParam> predicateParams = testPredicate.getParams().getAllParamsOfType(PredicateParam.class);
    if (!predicateParams.isEmpty()) {
      return predicateParams.stream()
          .flatMap(param -> param.getPredicates()
              .stream()
              .flatMap(childPredicate -> getRequiredQueries(childPredicate).stream()))
          .collect(Collectors.toSet());
    }

    // Otherwise, there are no required queries to be resolved
    return Collections.emptySet();
  }

  public boolean isOperationValid(String operation) {
    return operationEvaluators.containsKey(operation);
  }
}
