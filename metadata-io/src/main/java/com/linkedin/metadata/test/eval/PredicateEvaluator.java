package com.linkedin.metadata.test.eval;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.test.definition.expression.Expression;
import com.linkedin.metadata.test.definition.expression.Query;
import com.linkedin.metadata.test.definition.literal.Literal;
import com.linkedin.metadata.test.definition.operator.Operands;
import com.linkedin.metadata.test.definition.operator.OperatorType;
import com.linkedin.metadata.test.definition.operator.Predicate;
import com.linkedin.metadata.test.eval.operator.AndEvaluator;
import com.linkedin.metadata.test.eval.operator.AnyEqualsEvaluator;
import com.linkedin.metadata.test.eval.operator.BaseOperatorEvaluator;
import com.linkedin.metadata.test.eval.operator.ContainsAnyEvaluator;
import com.linkedin.metadata.test.eval.operator.ContainsStrEvaluator;
import com.linkedin.metadata.test.eval.operator.ExistsEvaluator;
import com.linkedin.metadata.test.eval.operator.GreaterThanEvaluator;
import com.linkedin.metadata.test.eval.operator.IsFalseEvaluator;
import com.linkedin.metadata.test.eval.operator.IsTrueEvaluator;
import com.linkedin.metadata.test.eval.operator.LessThanEvaluator;
import com.linkedin.metadata.test.eval.operator.NotEvaluator;
import com.linkedin.metadata.test.eval.operator.OrEvaluator;
import com.linkedin.metadata.test.eval.operator.RegexMatchEvaluator;
import com.linkedin.metadata.test.eval.operator.SchemaFieldsHaveDescriptions;
import com.linkedin.metadata.test.eval.operator.StartsWithEvaluator;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import com.linkedin.metadata.test.query.TestQuery;
import com.linkedin.metadata.test.query.TestQueryResponse;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** Class that evaluates all {@link Predicate}s provided batched query responses */
@Slf4j
public class PredicateEvaluator {

  private final Map<OperatorType, BaseOperatorEvaluator> operationEvaluators;
  private static final PredicateEvaluator INSTANCE = new PredicateEvaluator();

  public PredicateEvaluator() {
    this(
        ImmutableList.of(
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
            new ContainsStrEvaluator(),
            new SchemaFieldsHaveDescriptions()));
  }

  public PredicateEvaluator(List<BaseOperatorEvaluator> operationEvaluators) {
    this.operationEvaluators =
        operationEvaluators.stream()
            .collect(Collectors.toMap(BaseOperatorEvaluator::getOperatorType, Function.identity()));
  }

  public static PredicateEvaluator getInstance() {
    return INSTANCE;
  }

  private static BaseOperatorEvaluator getOperationEvaluator(OperatorType operatorType) {
    if (!INSTANCE.operationEvaluators.containsKey(operatorType)) {
      throw new IllegalArgumentException(
          String.format("Unsupported operator type %s", operatorType));
    }
    return INSTANCE.operationEvaluators.get(operatorType);
  }

  /**
   * Validate the test predicate. Make sure it has the set of parameters required to evaluate the
   * operator
   *
   * @param predicate Predicate to validate
   * @throws InvalidOperandException if parameters are not sufficient to evaluate the operation
   */
  public static void validate(Predicate predicate) throws InvalidOperandException {
    getOperationEvaluator(predicate.getOperatorType()).validate(predicate.getOperands());
  }

  /**
   * Evaluate the input test predicate given the batched query responses
   *
   * @param predicate The operator to evaluate
   * @param batchedQueryResponse Batched query responses containing the query responses of all
   *     queries required by the predicate
   * @return whether or not the predicate passed
   */
  public boolean evaluatePredicate(
      Predicate predicate, Map<TestQuery, TestQueryResponse> batchedQueryResponse) {
    // i.e. get correct query response for query parameter, evaluate predicate for predicate
    // parameter
    ResolvedOperands resolvedOperands =
        evaluateOperands(predicate.operands(), batchedQueryResponse);

    // 2. Evaluate predicate by passing in the resolved params
    return (boolean) getOperationEvaluator(predicate.operatorType()).evaluate(resolvedOperands);
  }

  private ResolvedOperands evaluateOperands(
      Operands namedOperands, Map<TestQuery, TestQueryResponse> batchedQueryResponse) {
    return new ResolvedOperands(
        namedOperands.get().stream()
            .map(
                op ->
                    new ResolvedOperand(
                        op.getIndex(),
                        op.getName(),
                        evaluate(op.getExpression(), batchedQueryResponse)))
            .collect(Collectors.toList()));
  }

  public ResolvedExpression evaluate(
      Expression expression, Map<TestQuery, TestQueryResponse> batchedQueryResponse) {
    if (expression instanceof Query) {
      // For a query param, fetch the query response from the batched responses
      Query queryParam = (Query) expression;
      return new ResolvedExpression(
          queryParam,
          batchedQueryResponse
              .getOrDefault(queryParam.getQuery(), TestQueryResponse.empty())
              .getValues());
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
        String.format(
            "Unsupported operation param type: %s", expression.getClass().getSimpleName()));
  }

  public static boolean isOperationValid(String operation) {
    try {
      return INSTANCE.operationEvaluators.containsKey(OperatorType.fromCommonName(operation));
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
