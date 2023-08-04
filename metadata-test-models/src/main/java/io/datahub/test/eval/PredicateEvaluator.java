package io.datahub.test.eval;

import com.google.common.collect.ImmutableList;
import io.datahub.test.definition.expression.Expression;
import io.datahub.test.definition.expression.Query;
import io.datahub.test.definition.literal.Literal;
import io.datahub.test.definition.operator.Operands;
import io.datahub.test.definition.operator.OperatorType;
import io.datahub.test.definition.operator.Predicate;
import io.datahub.test.eval.operator.AndEvaluator;
import io.datahub.test.eval.operator.AnyEqualsEvaluator;
import io.datahub.test.eval.operator.BaseOperatorEvaluator;
import io.datahub.test.eval.operator.ContainsAnyEvaluator;
import io.datahub.test.eval.operator.ContainsStrEvaluator;
import io.datahub.test.eval.operator.ExistsEvaluator;
import io.datahub.test.eval.operator.GreaterThanEvaluator;
import io.datahub.test.eval.operator.IsFalseEvaluator;
import io.datahub.test.eval.operator.IsTrueEvaluator;
import io.datahub.test.eval.operator.LessThanEvaluator;
import io.datahub.test.eval.operator.NotEvaluator;
import io.datahub.test.eval.operator.OrEvaluator;
import io.datahub.test.eval.operator.RegexMatchEvaluator;
import io.datahub.test.eval.operator.StartsWithEvaluator;
import io.datahub.test.exception.InvalidOperandException;
import io.datahub.test.query.TestQuery;
import io.datahub.test.query.TestQueryResponse;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


/**
 * Class that evaluates all {@link Predicate}s provided batched query responses
 */
@Slf4j
public class PredicateEvaluator {

  private final Map<OperatorType, BaseOperatorEvaluator> operationEvaluators;
  private static final PredicateEvaluator INSTANCE = new PredicateEvaluator();

  public PredicateEvaluator() {
    this(ImmutableList.of(
        new AnyEqualsEvaluator(),
        new ExistsEvaluator(),
        new OrEvaluator(),
        new AndEvaluator(),
        new NotEvaluator(),
        new GreaterThanEvaluator(),
        new LessThanEvaluator(),
        new StartsWithEvaluator(),
        new RegexMatchEvaluator(),
        new ContainsAnyEvaluator(),
        new IsTrueEvaluator(),
        new IsFalseEvaluator(),
        new ContainsStrEvaluator()));
  }

  public PredicateEvaluator(List<BaseOperatorEvaluator> operationEvaluators) {
    this.operationEvaluators = operationEvaluators.stream()
        .collect(Collectors.toMap(BaseOperatorEvaluator::getOperatorType, Function.identity()));
  }

  public static PredicateEvaluator getInstance() {
    return INSTANCE;
  }

  private BaseOperatorEvaluator getOperationEvaluator(OperatorType operatorType) {
    if (!operationEvaluators.containsKey(operatorType)) {
      throw new IllegalArgumentException(String.format("Unsupported operator type %s", operatorType));
    }
    return operationEvaluators.get(operatorType);
  }

  /**
   * Validate the test predicate. Make sure it has the set of parameters required to evaluate the operator
   *
   * @param predicate Predicate to validate
   * @throws InvalidOperandException if parameters are not sufficient to evaluate the operation
   */
  public void validate(Predicate predicate) throws InvalidOperandException {
    getOperationEvaluator(predicate.getOperatorType()).validate(predicate.getOperands());
  }

  /**
   * Evaluate the input test predicate given the batched query responses
   *
   * @param predicate The operator to evaluate
   * @param batchedQueryResponse Batched query responses containing the query responses of all queries
   *                             required by the predicate
   * @return whether or not the predicate passed
   */
  public boolean evaluatePredicate(Predicate predicate, Map<TestQuery, TestQueryResponse> batchedQueryResponse) {
    // i.e. get correct query response for query parameter, evaluate predicate for predicate parameter
    ResolvedOperands resolvedOperands = evaluateOperands(predicate.operands(), batchedQueryResponse);

    // 2. Evaluate predicate by passing in the resolved params
    return (boolean) getOperationEvaluator(predicate.operatorType()).evaluate(resolvedOperands);
  }

  private ResolvedOperands evaluateOperands(
      Operands namedOperands,
      Map<TestQuery, TestQueryResponse> batchedQueryResponse) {
    return new ResolvedOperands(namedOperands.get()
        .stream()
        .map(op -> new ResolvedOperand(op.getIndex(), op.getName(), evaluate(op.getExpression(), batchedQueryResponse)))
        .collect(Collectors.toList()));
  }

  public ResolvedExpression evaluate(
      Expression expression,
      Map<TestQuery, TestQueryResponse> batchedQueryResponse) {
    if (expression instanceof Query) {
      // For a query param, fetch the query response from the batched responses
      Query queryParam = (Query) expression;
      return new ResolvedExpression(
          queryParam,
          batchedQueryResponse.getOrDefault(queryParam.getQuery(), TestQueryResponse.empty()).getValues());
    }
    if (expression instanceof Predicate) {
      // For a predicate param, recursively evaluate the child predicates
      Predicate predicate = (Predicate) expression;
      return new ResolvedExpression(predicate, evaluatePredicate(predicate, batchedQueryResponse));
    }
    if (expression instanceof Literal) {
      Literal literal = (Literal) expression;
      return new ResolvedExpression(literal, literal.value());
    }
    throw new IllegalArgumentException(
        String.format("Unsupported operation param type: %s", expression.getClass().getSimpleName()));
  }

  /**
   * Retrieve the set of {@link TestQuery}s required to evaluate a given {@link Predicate}.
   */
  public Set<TestQuery> extractQueriesForPredicate(final @Nonnull Predicate predicate) {

    // If the predicate is a leaf, then simply return the Queries inside the leaf nodes.
    List<Query> queryParams = predicate.getOperands().getOperandsOfType(Query.class);
    if (!queryParams.isEmpty()) {
      return queryParams.stream().map(Query::getQuery).collect(Collectors.toSet());
    }

    // If the predicate is a non-leaf, then recurse down to subpredicates.
    List<Predicate> subPredicates = predicate.getOperands().getOperandsOfType(Predicate.class);
    if (!subPredicates.isEmpty()) {
      return subPredicates.stream()
          .flatMap(pred -> extractQueriesForPredicate(pred).stream())
          .collect(Collectors.toSet());
    }

    // Otherwise, there are no required queries to be resolved
    return Collections.emptySet();
  }

  public boolean isOperationValid(String operation) {
    try {
      return operationEvaluators.containsKey(OperatorType.fromCommonName(operation));
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
