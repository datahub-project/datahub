package com.linkedin.metadata.test.definition.operator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.collect.ImmutableSet;
import com.linkedin.metadata.test.definition.expression.Expression;
import com.linkedin.metadata.test.definition.expression.ExpressionType;
import com.linkedin.metadata.test.definition.expression.Query;
import com.linkedin.metadata.test.definition.literal.Literal;
import com.linkedin.metadata.test.definition.value.BooleanType;
import com.linkedin.metadata.test.definition.value.ValueType;
import com.linkedin.metadata.test.query.QueryOperation;
import com.linkedin.metadata.test.query.TestQuery;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;

/** This represents an {@link Operator} which returns a boolean (true / false) value. */
@Value
@ToString
@JsonDeserialize(builder = Predicate.PredicateBuilder.class)
public class Predicate implements Operator {

  @JsonProperty("operatorType")
  OperatorType operatorType;

  @JsonProperty("operands")
  Operands operands;

  public static Predicate of(OperatorType operatorType, List<Expression> expressions) {
    return Predicate.of(operatorType, expressions, false);
  }

  public static Predicate of(
      OperatorType operatorType, List<Expression> expressions, boolean negated) {
    return new Predicate(operatorType, convertExpListToOperands(expressions), negated);
  }

  @Override
  public ExpressionType expressionType() {
    return ExpressionType.PREDICATE;
  }

  @Override
  public ValueType valueType() {
    return new BooleanType();
  }

  public Predicate(OperatorType operator, List<Operand> operands) {
    this(operator, operands, false);
  }

  public Predicate(OperatorType operatorType, List<Operand> operands, boolean negated) {
    this(operatorType, new Operands(operands), negated);
  }

  @Builder
  public Predicate(OperatorType operatorType, Operands operands, boolean negated) {
    // TODO: Clean this hack up.
    if (negated) {
      // Wrap the operands in a NOT predicate.
      this.operatorType = OperatorType.NOT;
      this.operands = injectNotPredicate(operatorType, operands);
    } else {
      this.operatorType = operatorType;
      this.operands = operands;
    }
  }

  @Override
  public OperatorType operatorType() {
    return this.operatorType;
  }

  @Override
  public Operands operands() {
    return this.operands;
  }

  private static List<Operand> convertExpListToOperands(List<Expression> expressions) {
    final List<Operand> result = new ArrayList<>();
    for (int i = 0; i < expressions.size(); i++) {
      result.add(new Operand(i, expressions.get(i)));
    }
    return result;
  }

  private static Operands injectNotPredicate(OperatorType operatorType, Operands operands) {
    final Predicate innerPredicate = new Predicate(operatorType, operands, false);
    return new Operands(createNotOperands(innerPredicate));
  }

  private static List<Operand> createNotOperands(Predicate base) {
    final List<Operand> result = new ArrayList<>();
    result.add(new Operand(0, base));
    return result;
  }

  /** Retrieve the set of {@link TestQuery}s required to evaluate a given {@link Predicate}. */
  public static Set<TestQuery> extractQueriesForPredicate(final @Nonnull Predicate predicate) {
    return extractQueryOperationsForPredicate(predicate).stream()
        .map(QueryOperation::getQuery)
        .collect(Collectors.toSet());
  }

  /**
   * Retrieve the set of full operations at leaf nodes of a predicate, can have a single {@link
   * TestQuery} and associated {@link Literal}(s)
   */
  public static Set<QueryOperation> extractQueryOperationsForPredicate(
      final @Nonnull Predicate predicate) {

    // If the predicate is a leaf, then simply return the Queries inside the leaf nodes.
    List<Query> queryParams = predicate.getOperands().getOperandsOfType(Query.class);
    if (!queryParams.isEmpty()) {
      // Assumption of single query based on TestDefinitionParser
      TestQuery query = queryParams.get(0).getQuery();
      OperatorType opType = predicate.getOperatorType();
      List<Literal> values = predicate.getOperands().getOperandsOfType(Literal.class);
      QueryOperation queryOperation = new QueryOperation(query, opType);
      if (!values.isEmpty()) {
        queryOperation.setValues(values);
      }
      return ImmutableSet.of(queryOperation);
    }

    // If the predicate is a non-leaf, then recurse down to subpredicates.
    List<Predicate> subPredicates = predicate.getOperands().getOperandsOfType(Predicate.class);
    if (!subPredicates.isEmpty()) {
      return subPredicates.stream()
          .flatMap(pred -> extractQueryOperationsForPredicate(pred).stream())
          .collect(Collectors.toSet());
    }

    // Otherwise, there are no required queries to be resolved
    return Collections.emptySet();
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class PredicateBuilder {}
}
