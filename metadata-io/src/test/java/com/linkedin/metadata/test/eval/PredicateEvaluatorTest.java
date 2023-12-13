package com.linkedin.metadata.test.eval;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.test.definition.expression.Expression;
import com.linkedin.metadata.test.definition.expression.Query;
import com.linkedin.metadata.test.definition.literal.StringListLiteral;
import com.linkedin.metadata.test.definition.operator.Operand;
import com.linkedin.metadata.test.definition.operator.OperatorType;
import com.linkedin.metadata.test.definition.operator.Predicate;
import com.linkedin.metadata.test.query.TestQuery;
import com.linkedin.metadata.test.query.TestQueryResponse;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

public class PredicateEvaluatorTest {
  PredicateEvaluator _evaluator = PredicateEvaluator.getInstance();

  private Predicate createCompositePredicate(String operation, List<Predicate> childPredicates) {
    return Predicate.of(
        OperatorType.fromCommonName(operation),
        childPredicates.stream().map(pred -> (Expression) pred).collect(Collectors.toList()),
        false);
  }

  @Test
  public void testEvaluateAnyEqualsPredicate() {
    Predicate equalsRule =
        new Predicate(
            OperatorType.ANY_EQUALS,
            ImmutableList.of(
                new Operand(0, "query", new Query("testProperty")),
                new Operand(
                    1, "values", new StringListLiteral(ImmutableList.of("value1", "value2")))));

    Map<TestQuery, TestQueryResponse> queryResponses = Collections.emptyMap();
    assertFalse((boolean) _evaluator.evaluatePredicate(equalsRule, queryResponses));

    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("value1")));
    assertTrue((boolean) _evaluator.evaluatePredicate(equalsRule, queryResponses));

    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("value3")));
    assertFalse((boolean) _evaluator.evaluatePredicate(equalsRule, queryResponses));

    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("value1", "value3")));
    assertTrue((boolean) _evaluator.evaluatePredicate(equalsRule, queryResponses));

    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("value2")),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertTrue((boolean) _evaluator.evaluatePredicate(equalsRule, queryResponses));

    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty2"), new TestQueryResponse(ImmutableList.of("value1")));
    assertFalse((boolean) _evaluator.evaluatePredicate(equalsRule, queryResponses));

    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")),
            new TestQuery("testProperty3"),
            new TestQueryResponse(ImmutableList.of("value1")));
    assertFalse((boolean) _evaluator.evaluatePredicate(equalsRule, queryResponses));

    // Is empty string
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("")),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertFalse((boolean) _evaluator.evaluatePredicate(equalsRule, queryResponses));

    // Is empty string + good value
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("", "value1")),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertTrue((boolean) _evaluator.evaluatePredicate(equalsRule, queryResponses));

    // Is empty list -> does not exist
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(Collections.emptyList()),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertFalse((boolean) _evaluator.evaluatePredicate(equalsRule, queryResponses));
  }

  @Test
  public void testEvaluateContainsAnyPredicate() {
    Predicate containsAnyRule =
        new Predicate(
            OperatorType.CONTAINS_ANY,
            ImmutableList.of(
                new Operand(0, "query", new Query("testProperty")),
                new Operand(
                    1, "values", new StringListLiteral(ImmutableList.of("value1", "value2")))));

    Map<TestQuery, TestQueryResponse> queryResponses = Collections.emptyMap();
    assertFalse((boolean) _evaluator.evaluatePredicate(containsAnyRule, queryResponses));

    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("value1")));
    assertTrue((boolean) _evaluator.evaluatePredicate(containsAnyRule, queryResponses));

    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("value3")));
    assertFalse((boolean) _evaluator.evaluatePredicate(containsAnyRule, queryResponses));

    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("value2")),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertTrue((boolean) _evaluator.evaluatePredicate(containsAnyRule, queryResponses));

    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty2"), new TestQueryResponse(ImmutableList.of("value1")));
    assertFalse((boolean) _evaluator.evaluatePredicate(containsAnyRule, queryResponses));

    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")),
            new TestQuery("testProperty3"),
            new TestQueryResponse(ImmutableList.of("value1")));
    assertFalse((boolean) _evaluator.evaluatePredicate(containsAnyRule, queryResponses));

    // Is empty string
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("")),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertFalse((boolean) _evaluator.evaluatePredicate(containsAnyRule, queryResponses));

    // Is empty string + good value
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("", "value1")),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertTrue((boolean) _evaluator.evaluatePredicate(containsAnyRule, queryResponses));

    // Is empty list -> does not exist
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(Collections.emptyList()),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertFalse((boolean) _evaluator.evaluatePredicate(containsAnyRule, queryResponses));
  }

  @Test
  public void testEvaluateRegexMatchPredicate() {
    Predicate regexMatchRule =
        new Predicate(
            OperatorType.REGEX_MATCH,
            ImmutableList.of(
                new Operand(0, "query", new Query("testProperty")),
                new Operand(
                    1,
                    "values",
                    new StringListLiteral(ImmutableList.of("testtext.*", ".*testtext")))));

    // No results
    Map<TestQuery, TestQueryResponse> queryResponses = Collections.emptyMap();
    assertFalse((boolean) _evaluator.evaluatePredicate(regexMatchRule, queryResponses));

    // Does not match
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("value1")));
    assertFalse((boolean) _evaluator.evaluatePredicate(regexMatchRule, queryResponses));

    // Matches 1, single property in response
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("testtext123")));
    assertTrue((boolean) _evaluator.evaluatePredicate(regexMatchRule, queryResponses));

    // Matches 1, multiple properties in response
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("testtext123", "value2")),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertTrue((boolean) _evaluator.evaluatePredicate(regexMatchRule, queryResponses));

    // Matches 2, multiple properties in response
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("testtext123", "123testtext")));
    assertTrue((boolean) _evaluator.evaluatePredicate(regexMatchRule, queryResponses));

    // No matching properties
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("testtext123")),
            new TestQuery("testProperty3"),
            new TestQueryResponse(ImmutableList.of("123testtext")));
    assertFalse((boolean) _evaluator.evaluatePredicate(regexMatchRule, queryResponses));

    // Is empty string
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("")),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertFalse((boolean) _evaluator.evaluatePredicate(regexMatchRule, queryResponses));

    // Is empty string + good value
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("", "testtext123")),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertTrue((boolean) _evaluator.evaluatePredicate(regexMatchRule, queryResponses));

    // Is empty list -> does not exist
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(Collections.emptyList()),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertFalse((boolean) _evaluator.evaluatePredicate(regexMatchRule, queryResponses));
  }

  @Test
  public void testEvaluateStartsWithPredicate() {
    Predicate startsWithRule =
        new Predicate(
            OperatorType.STARTS_WITH,
            ImmutableList.of(
                new Operand(0, "query", new Query("testProperty")),
                new Operand(
                    1, "values", new StringListLiteral(ImmutableList.of("test1", "test2")))));

    // No results
    Map<TestQuery, TestQueryResponse> queryResponses = Collections.emptyMap();
    assertFalse((boolean) _evaluator.evaluatePredicate(startsWithRule, queryResponses));

    // Does not match
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("value1")));
    assertFalse((boolean) _evaluator.evaluatePredicate(startsWithRule, queryResponses));

    // Matches 1, single property in response
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("test1something")));
    assertTrue((boolean) _evaluator.evaluatePredicate(startsWithRule, queryResponses));

    // Matches 1, multiple properties in response
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("test2something", "value2")),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertTrue((boolean) _evaluator.evaluatePredicate(startsWithRule, queryResponses));

    // Matches 2, multiple properties in response
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("test1something", "test2something")));
    assertTrue((boolean) _evaluator.evaluatePredicate(startsWithRule, queryResponses));

    // No matching properties
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("test1something")),
            new TestQuery("testProperty3"),
            new TestQueryResponse(ImmutableList.of("test2something")));
    assertFalse((boolean) _evaluator.evaluatePredicate(startsWithRule, queryResponses));

    // Is empty string
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("")),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertFalse((boolean) _evaluator.evaluatePredicate(startsWithRule, queryResponses));

    // Is empty string + good value
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("", "test1")),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertTrue((boolean) _evaluator.evaluatePredicate(startsWithRule, queryResponses));

    // Is empty list -> does not exist
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(Collections.emptyList()),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertFalse((boolean) _evaluator.evaluatePredicate(startsWithRule, queryResponses));
  }

  @Test
  public void testEvaluateContainsStrPredicate() {
    Predicate containsStrRule =
        new Predicate(
            OperatorType.CONTAINS_STR,
            ImmutableList.of(
                new Operand(0, "query", new Query("testProperty")),
                new Operand(
                    1, "values", new StringListLiteral(ImmutableList.of("test1", "test2")))));

    // No results
    Map<TestQuery, TestQueryResponse> queryResponses = Collections.emptyMap();
    assertFalse((boolean) _evaluator.evaluatePredicate(containsStrRule, queryResponses));

    // Does not match
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("value1")));
    assertFalse((boolean) _evaluator.evaluatePredicate(containsStrRule, queryResponses));

    // Matches 1, single property in response
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("prefix-test1-suffix")));
    assertTrue((boolean) _evaluator.evaluatePredicate(containsStrRule, queryResponses));

    // Matches 1, multiple properties in response
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("prefix-test1-suffix", "value2")),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertTrue((boolean) _evaluator.evaluatePredicate(containsStrRule, queryResponses));

    // Matches 2, multiple properties in response
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("prefix-test1-suffix", "prefix-test2-suffix")));
    assertTrue((boolean) _evaluator.evaluatePredicate(containsStrRule, queryResponses));

    // No matching properties
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("prefix-test1-suffix")),
            new TestQuery("testProperty3"),
            new TestQueryResponse(ImmutableList.of("prefix-test2-suffix")));
    assertFalse((boolean) _evaluator.evaluatePredicate(containsStrRule, queryResponses));

    // Is empty string
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("")),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertFalse((boolean) _evaluator.evaluatePredicate(containsStrRule, queryResponses));

    // Is empty string + good value
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("", "test1")),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertTrue((boolean) _evaluator.evaluatePredicate(containsStrRule, queryResponses));

    // Is empty list -> does not exist
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(Collections.emptyList()),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertFalse((boolean) _evaluator.evaluatePredicate(containsStrRule, queryResponses));
  }

  @Test
  public void testEvaluateExistsPredicate() {
    Predicate existsRule =
        new Predicate(
            OperatorType.EXISTS,
            ImmutableList.of(new Operand(0, "query", new Query("testProperty"))));

    // No results
    Map<TestQuery, TestQueryResponse> queryResponses = Collections.emptyMap();
    assertFalse((boolean) _evaluator.evaluatePredicate(existsRule, queryResponses));

    // Exists
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("value1")));
    assertTrue((boolean) _evaluator.evaluatePredicate(existsRule, queryResponses));

    // Does not exist
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("prefix-test1-suffix")));
    assertFalse((boolean) _evaluator.evaluatePredicate(existsRule, queryResponses));

    // Is empty string
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("")),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertFalse((boolean) _evaluator.evaluatePredicate(existsRule, queryResponses));

    // Is empty string + value
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("", "value")),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertTrue((boolean) _evaluator.evaluatePredicate(existsRule, queryResponses));

    // Is empty list -> does not exist
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(Collections.emptyList()),
            new TestQuery("testProperty2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertFalse((boolean) _evaluator.evaluatePredicate(existsRule, queryResponses));
  }

  @Test
  public void testEvaluateGreaterThanPredicate() {
    Predicate greaterThanRule =
        new Predicate(
            OperatorType.GREATER_THAN,
            ImmutableList.of(
                new Operand(0, "query", new Query("testProperty")),
                new Operand(1, "values", new StringListLiteral(ImmutableList.of("5")))));

    // No results
    Map<TestQuery, TestQueryResponse> queryResponses = Collections.emptyMap();
    assertFalse((boolean) _evaluator.evaluatePredicate(greaterThanRule, queryResponses));

    // Is greater than
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("10")));
    assertTrue((boolean) _evaluator.evaluatePredicate(greaterThanRule, queryResponses));

    // Some greater than
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("10", "1")));
    assertTrue((boolean) _evaluator.evaluatePredicate(greaterThanRule, queryResponses));

    // Is equal to
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty2"), new TestQueryResponse(ImmutableList.of("5")));
    assertFalse((boolean) _evaluator.evaluatePredicate(greaterThanRule, queryResponses));

    // Is less than
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("4")));
    assertFalse((boolean) _evaluator.evaluatePredicate(greaterThanRule, queryResponses));

    // Empty list
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(Collections.emptyList()));
    assertFalse((boolean) _evaluator.evaluatePredicate(greaterThanRule, queryResponses));

    // Empty string
    queryResponses =
        ImmutableMap.of(new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("")));
    assertFalse((boolean) _evaluator.evaluatePredicate(greaterThanRule, queryResponses));

    // Empty string + good value
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("", "10")));
    assertTrue((boolean) _evaluator.evaluatePredicate(greaterThanRule, queryResponses));

    // Bad input operand -> not a number
    Predicate badRule =
        new Predicate(
            OperatorType.GREATER_THAN,
            ImmutableList.of(
                new Operand(0, "query", new Query("testProperty")),
                new Operand(1, "values", new StringListLiteral(ImmutableList.of("test")))));

    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("10")));
    assertFalse((boolean) _evaluator.evaluatePredicate(badRule, queryResponses));

    // Bad query response -> not a number
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("test")));
    assertFalse((boolean) _evaluator.evaluatePredicate(greaterThanRule, queryResponses));
  }

  @Test
  public void testEvaluateLessThanPredicate() {
    Predicate lessThanRule =
        new Predicate(
            OperatorType.LESS_THAN,
            ImmutableList.of(
                new Operand(0, "query", new Query("testProperty")),
                new Operand(1, "values", new StringListLiteral(ImmutableList.of("5")))));

    // No results
    Map<TestQuery, TestQueryResponse> queryResponses = Collections.emptyMap();
    assertFalse((boolean) _evaluator.evaluatePredicate(lessThanRule, queryResponses));

    // Is greater than
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("10")));
    assertFalse((boolean) _evaluator.evaluatePredicate(lessThanRule, queryResponses));

    // Some less than
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("10", "1")));
    assertTrue((boolean) _evaluator.evaluatePredicate(lessThanRule, queryResponses));

    // Is equal to
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty2"), new TestQueryResponse(ImmutableList.of("5")));
    assertFalse((boolean) _evaluator.evaluatePredicate(lessThanRule, queryResponses));

    // Is less than
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("4")));
    assertTrue((boolean) _evaluator.evaluatePredicate(lessThanRule, queryResponses));

    // Empty list
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(Collections.emptyList()));
    assertFalse((boolean) _evaluator.evaluatePredicate(lessThanRule, queryResponses));

    // Empty string
    queryResponses =
        ImmutableMap.of(new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("")));
    assertFalse((boolean) _evaluator.evaluatePredicate(lessThanRule, queryResponses));

    // Empty string + good value
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("", "1")));
    assertTrue((boolean) _evaluator.evaluatePredicate(lessThanRule, queryResponses));

    // Bad input operand -> not a number
    Predicate badRule =
        new Predicate(
            OperatorType.LESS_THAN,
            ImmutableList.of(
                new Operand(0, "query", new Query("testProperty")),
                new Operand(1, "values", new StringListLiteral(ImmutableList.of("test")))));

    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("10")));
    assertFalse((boolean) _evaluator.evaluatePredicate(badRule, queryResponses));

    // Bad query response -> not a number
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("test")));
    assertFalse((boolean) _evaluator.evaluatePredicate(lessThanRule, queryResponses));
  }

  @Test
  public void testEvaluateIsTruePredicate() {
    Predicate isTrueRule =
        new Predicate(
            OperatorType.IS_TRUE,
            ImmutableList.of(new Operand(0, "query", new Query("testProperty"))));

    // No results
    Map<TestQuery, TestQueryResponse> queryResponses = Collections.emptyMap();
    assertFalse((boolean) _evaluator.evaluatePredicate(isTrueRule, queryResponses));

    // Contains value true
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("true")));
    assertTrue((boolean) _evaluator.evaluatePredicate(isTrueRule, queryResponses));

    // Contains value true upper case
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("TRUE")));
    assertTrue((boolean) _evaluator.evaluatePredicate(isTrueRule, queryResponses));

    // Contains value true AND false (still true)
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("true", "false")));
    assertTrue((boolean) _evaluator.evaluatePredicate(isTrueRule, queryResponses));

    // Contains false
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("false")));
    assertFalse((boolean) _evaluator.evaluatePredicate(isTrueRule, queryResponses));

    // Empty list
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(Collections.emptyList()));
    assertFalse((boolean) _evaluator.evaluatePredicate(isTrueRule, queryResponses));

    // Empty string
    queryResponses =
        ImmutableMap.of(new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("")));
    assertFalse((boolean) _evaluator.evaluatePredicate(isTrueRule, queryResponses));

    // Empty string + non-false value -> interpreted as truthy
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("", "10")));
    assertTrue((boolean) _evaluator.evaluatePredicate(isTrueRule, queryResponses));

    // Bad query response -> interpreted as truthy
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("test")));
    assertTrue((boolean) _evaluator.evaluatePredicate(isTrueRule, queryResponses));
  }

  @Test
  public void testEvaluateIsFalsePredicate() {
    Predicate isFalseRule =
        new Predicate(
            OperatorType.IS_FALSE,
            ImmutableList.of(new Operand(0, "query", new Query("testProperty"))));

    // No results -> This is considered "falsy"
    Map<TestQuery, TestQueryResponse> queryResponses = Collections.emptyMap();
    assertTrue((boolean) _evaluator.evaluatePredicate(isFalseRule, queryResponses));

    // Contains value false
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("false")));
    assertTrue((boolean) _evaluator.evaluatePredicate(isFalseRule, queryResponses));

    // Contains value false upper case
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("FALSE")));
    assertTrue((boolean) _evaluator.evaluatePredicate(isFalseRule, queryResponses));

    // Contains value true AND false (still true)
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"),
            new TestQueryResponse(ImmutableList.of("true", "false")));
    assertTrue((boolean) _evaluator.evaluatePredicate(isFalseRule, queryResponses));

    // Contains true
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("true")));
    assertFalse((boolean) _evaluator.evaluatePredicate(isFalseRule, queryResponses));

    // Empty list -> This should be interpreted as "falsy"
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(Collections.emptyList()));
    assertTrue((boolean) _evaluator.evaluatePredicate(isFalseRule, queryResponses));

    // Empty string -> This should be interpreted as "falsy"
    queryResponses =
        ImmutableMap.of(new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("")));
    assertTrue((boolean) _evaluator.evaluatePredicate(isFalseRule, queryResponses));

    // Empty string + false
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("", "false")));
    assertTrue((boolean) _evaluator.evaluatePredicate(isFalseRule, queryResponses));

    // Bad query response -> not a true or false
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testProperty"), new TestQueryResponse(ImmutableList.of("test")));
    assertFalse((boolean) _evaluator.evaluatePredicate(isFalseRule, queryResponses));
  }

  @Test
  public void testEvaluateAndPredicate() {
    Predicate equalsRule =
        new Predicate(
            OperatorType.ANY_EQUALS,
            ImmutableList.of(
                new Operand(0, "query", new Query("testStrProperty")),
                new Operand(
                    1, "values", new StringListLiteral(ImmutableList.of("value1", "value2")))));

    Predicate greaterThanRule =
        new Predicate(
            OperatorType.GREATER_THAN,
            ImmutableList.of(
                new Operand(0, "query", new Query("testNumProperty")),
                new Operand(1, "values", new StringListLiteral(ImmutableList.of("5")))));

    Predicate andRule =
        new Predicate(
            OperatorType.AND,
            ImmutableList.of(
                new Operand(0, null, greaterThanRule), new Operand(1, null, equalsRule)));

    // No results
    Map<TestQuery, TestQueryResponse> queryResponses = Collections.emptyMap();
    assertFalse((boolean) _evaluator.evaluatePredicate(andRule, queryResponses));

    // Both sub-predicates true
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testStrProperty"), new TestQueryResponse(ImmutableList.of("value2")),
            new TestQuery("testNumProperty"), new TestQueryResponse(ImmutableList.of("10")));
    assertTrue((boolean) _evaluator.evaluatePredicate(andRule, queryResponses));

    // Left sub predicate not true
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testStrProperty"), new TestQueryResponse(ImmutableList.of("value3")),
            new TestQuery("testNumProperty"), new TestQueryResponse(ImmutableList.of("10")));
    assertFalse((boolean) _evaluator.evaluatePredicate(andRule, queryResponses));

    // Right sub predicate not true
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testStrProperty"), new TestQueryResponse(ImmutableList.of("value1")),
            new TestQuery("testNumProperty"), new TestQueryResponse(ImmutableList.of("1")));
    assertFalse((boolean) _evaluator.evaluatePredicate(andRule, queryResponses));

    // Both predicates not true
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testStrProperty"), new TestQueryResponse(ImmutableList.of("value3")),
            new TestQuery("testNumProperty"), new TestQueryResponse(ImmutableList.of("1")));
    assertFalse((boolean) _evaluator.evaluatePredicate(andRule, queryResponses));

    // Empty lists
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testStrProperty"), new TestQueryResponse(Collections.emptyList()),
            new TestQuery("testNumProperty"), new TestQueryResponse(Collections.emptyList()));
    assertFalse((boolean) _evaluator.evaluatePredicate(andRule, queryResponses));

    // Empty strings
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testStrProperty"), new TestQueryResponse(ImmutableList.of("")),
            new TestQuery("testNumProperty"), new TestQueryResponse(ImmutableList.of("")));
    assertFalse((boolean) _evaluator.evaluatePredicate(andRule, queryResponses));
  }

  @Test
  public void testEvaluateOrPredicate() {
    Predicate equalsRule =
        new Predicate(
            OperatorType.ANY_EQUALS,
            ImmutableList.of(
                new Operand(0, "query", new Query("testStrProperty")),
                new Operand(
                    1, "values", new StringListLiteral(ImmutableList.of("value1", "value2")))));

    Predicate greaterThanRule =
        new Predicate(
            OperatorType.GREATER_THAN,
            ImmutableList.of(
                new Operand(0, "query", new Query("testNumProperty")),
                new Operand(1, "values", new StringListLiteral(ImmutableList.of("5")))));

    Predicate orRule =
        new Predicate(
            OperatorType.OR,
            ImmutableList.of(
                new Operand(0, null, greaterThanRule), new Operand(1, null, equalsRule)));

    // No results
    Map<TestQuery, TestQueryResponse> queryResponses = Collections.emptyMap();
    assertFalse((boolean) _evaluator.evaluatePredicate(orRule, queryResponses));

    // Both sub-predicates true
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testStrProperty"), new TestQueryResponse(ImmutableList.of("value2")),
            new TestQuery("testNumProperty"), new TestQueryResponse(ImmutableList.of("10")));
    assertTrue((boolean) _evaluator.evaluatePredicate(orRule, queryResponses));

    // Left sub predicate not true -> still true
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testStrProperty"), new TestQueryResponse(ImmutableList.of("value3")),
            new TestQuery("testNumProperty"), new TestQueryResponse(ImmutableList.of("10")));
    assertTrue((boolean) _evaluator.evaluatePredicate(orRule, queryResponses));

    // Right sub predicate not true -> still true
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testStrProperty"), new TestQueryResponse(ImmutableList.of("value1")),
            new TestQuery("testNumProperty"), new TestQueryResponse(ImmutableList.of("1")));
    assertTrue((boolean) _evaluator.evaluatePredicate(orRule, queryResponses));

    // Both predicates not true -> false
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testStrProperty"), new TestQueryResponse(ImmutableList.of("value3")),
            new TestQuery("testNumProperty"), new TestQueryResponse(ImmutableList.of("1")));
    assertFalse((boolean) _evaluator.evaluatePredicate(orRule, queryResponses));

    // Empty lists
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testStrProperty"), new TestQueryResponse(Collections.emptyList()),
            new TestQuery("testNumProperty"), new TestQueryResponse(Collections.emptyList()));
    assertFalse((boolean) _evaluator.evaluatePredicate(orRule, queryResponses));

    // Empty strings
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testStrProperty"), new TestQueryResponse(ImmutableList.of("")),
            new TestQuery("testNumProperty"), new TestQueryResponse(ImmutableList.of("")));
    assertFalse((boolean) _evaluator.evaluatePredicate(orRule, queryResponses));
  }

  @Test
  public void testEvaluateNotPredicate() {

    Predicate greaterThanRule =
        new Predicate(
            OperatorType.GREATER_THAN,
            ImmutableList.of(
                new Operand(0, "query", new Query("testNumProperty")),
                new Operand(1, "values", new StringListLiteral(ImmutableList.of("5")))));

    Predicate notRule =
        new Predicate(OperatorType.NOT, ImmutableList.of(new Operand(0, null, greaterThanRule)));

    // No results -> interpreted as falsy
    Map<TestQuery, TestQueryResponse> queryResponses = Collections.emptyMap();
    assertTrue((boolean) _evaluator.evaluatePredicate(notRule, queryResponses));

    // Sub predicate is true --> assert false
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testNumProperty"), new TestQueryResponse(ImmutableList.of("10")));
    assertFalse((boolean) _evaluator.evaluatePredicate(notRule, queryResponses));

    // Sub predicate is false --> assert true
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testNumProperty"), new TestQueryResponse(ImmutableList.of("1")));
    assertTrue((boolean) _evaluator.evaluatePredicate(notRule, queryResponses));

    // Empty list response -> true
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testNumProperty"), new TestQueryResponse(Collections.emptyList()));
    assertTrue((boolean) _evaluator.evaluatePredicate(notRule, queryResponses));

    // Empty string response -> true
    queryResponses =
        ImmutableMap.of(
            new TestQuery("testNumProperty"), new TestQueryResponse(ImmutableList.of("")));
    assertTrue((boolean) _evaluator.evaluatePredicate(notRule, queryResponses));
  }

  @Test
  public void testEvaluateNestedPredicates() {
    Predicate equalsRule1 =
        new Predicate(
            OperatorType.fromCommonName("equals"),
            ImmutableList.of(
                new Operand(0, "query", new Query("query1")),
                new Operand(
                    1, "values", new StringListLiteral(ImmutableList.of("value1", "value2")))));
    Predicate equalsRule2 =
        new Predicate(
            OperatorType.fromCommonName("equals"),
            ImmutableList.of(
                new Operand(0, "query", new Query("query2")),
                new Operand(1, "value", new StringListLiteral(ImmutableList.of("value3")))));

    Predicate existsRule =
        new Predicate(
            OperatorType.fromCommonName("exists"),
            Collections.singletonList(new Operand(0, "query", new Query("query3"))));

    Predicate orRule = createCompositePredicate("or", ImmutableList.of(equalsRule1, equalsRule2));
    Predicate andRule = createCompositePredicate("and", ImmutableList.of(equalsRule1, equalsRule2));
    Predicate complexRule =
        createCompositePredicate(
            "or",
            ImmutableList.of(
                equalsRule1,
                createCompositePredicate("and", ImmutableList.of(equalsRule2, existsRule))));
    Predicate notRule = createCompositePredicate("not", ImmutableList.of(complexRule));

    Map<TestQuery, TestQueryResponse> queryResponses = Collections.emptyMap();
    assertFalse((boolean) _evaluator.evaluatePredicate(equalsRule1, queryResponses));
    assertFalse((boolean) _evaluator.evaluatePredicate(equalsRule2, queryResponses));
    assertFalse((boolean) _evaluator.evaluatePredicate(existsRule, queryResponses));
    assertFalse((boolean) _evaluator.evaluatePredicate(orRule, queryResponses));
    assertFalse((boolean) _evaluator.evaluatePredicate(andRule, queryResponses));
    assertFalse((boolean) _evaluator.evaluatePredicate(complexRule, queryResponses));
    assertTrue((boolean) _evaluator.evaluatePredicate(notRule, queryResponses));

    queryResponses =
        ImmutableMap.of(new TestQuery("query1"), new TestQueryResponse(ImmutableList.of("value1")));
    assertTrue((boolean) _evaluator.evaluatePredicate(equalsRule1, queryResponses));
    assertFalse((boolean) _evaluator.evaluatePredicate(equalsRule2, queryResponses));
    assertFalse((boolean) _evaluator.evaluatePredicate(existsRule, queryResponses));
    assertTrue((boolean) _evaluator.evaluatePredicate(orRule, queryResponses));
    assertFalse((boolean) _evaluator.evaluatePredicate(andRule, queryResponses));
    assertTrue((boolean) _evaluator.evaluatePredicate(complexRule, queryResponses));
    assertFalse((boolean) _evaluator.evaluatePredicate(notRule, queryResponses));

    queryResponses =
        ImmutableMap.of(new TestQuery("query1"), new TestQueryResponse(ImmutableList.of("value3")));
    assertFalse((boolean) _evaluator.evaluatePredicate(equalsRule1, queryResponses));
    assertFalse((boolean) _evaluator.evaluatePredicate(equalsRule2, queryResponses));
    assertFalse((boolean) _evaluator.evaluatePredicate(existsRule, queryResponses));
    assertFalse((boolean) _evaluator.evaluatePredicate(orRule, queryResponses));
    assertFalse((boolean) _evaluator.evaluatePredicate(andRule, queryResponses));
    assertFalse((boolean) _evaluator.evaluatePredicate(complexRule, queryResponses));
    assertTrue((boolean) _evaluator.evaluatePredicate(notRule, queryResponses));

    queryResponses =
        ImmutableMap.of(
            new TestQuery("query1"),
            new TestQueryResponse(ImmutableList.of("value1")),
            new TestQuery("query2"),
            new TestQueryResponse(ImmutableList.of("value3")));
    assertTrue((boolean) _evaluator.evaluatePredicate(equalsRule1, queryResponses));
    assertTrue((boolean) _evaluator.evaluatePredicate(equalsRule2, queryResponses));
    assertFalse((boolean) _evaluator.evaluatePredicate(existsRule, queryResponses));
    assertTrue((boolean) _evaluator.evaluatePredicate(orRule, queryResponses));
    assertTrue((boolean) _evaluator.evaluatePredicate(andRule, queryResponses));
    assertTrue((boolean) _evaluator.evaluatePredicate(complexRule, queryResponses));
    assertFalse((boolean) _evaluator.evaluatePredicate(notRule, queryResponses));

    queryResponses =
        ImmutableMap.of(new TestQuery("query3"), new TestQueryResponse(ImmutableList.of("value1")));
    assertFalse((boolean) _evaluator.evaluatePredicate(equalsRule1, queryResponses));
    assertFalse((boolean) _evaluator.evaluatePredicate(equalsRule2, queryResponses));
    assertTrue((boolean) _evaluator.evaluatePredicate(existsRule, queryResponses));
    assertFalse((boolean) _evaluator.evaluatePredicate(orRule, queryResponses));
    assertFalse((boolean) _evaluator.evaluatePredicate(andRule, queryResponses));
    assertFalse((boolean) _evaluator.evaluatePredicate(complexRule, queryResponses));
    assertTrue((boolean) _evaluator.evaluatePredicate(notRule, queryResponses));

    queryResponses =
        ImmutableMap.of(
            new TestQuery("query2"),
            new TestQueryResponse(ImmutableList.of("value3")),
            new TestQuery("query3"),
            new TestQueryResponse(ImmutableList.of("value1")));
    assertFalse((boolean) _evaluator.evaluatePredicate(equalsRule1, queryResponses));
    assertTrue((boolean) _evaluator.evaluatePredicate(equalsRule2, queryResponses));
    assertTrue((boolean) _evaluator.evaluatePredicate(existsRule, queryResponses));
    assertTrue((boolean) _evaluator.evaluatePredicate(orRule, queryResponses));
    assertFalse((boolean) _evaluator.evaluatePredicate(andRule, queryResponses));
    assertTrue((boolean) _evaluator.evaluatePredicate(complexRule, queryResponses));
    assertFalse((boolean) _evaluator.evaluatePredicate(notRule, queryResponses));
  }
}
