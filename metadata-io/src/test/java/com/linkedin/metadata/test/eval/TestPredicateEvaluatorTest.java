package com.linkedin.metadata.test.eval;

import com.linkedin.metadata.test.definition.TestPredicate;
import com.linkedin.metadata.test.definition.TestQuery;
import com.linkedin.metadata.test.definition.operation.ParamKeyConstants;
import com.linkedin.metadata.test.definition.operation.PredicateParam;
import com.linkedin.metadata.test.definition.operation.QueryParam;
import com.linkedin.metadata.test.definition.operation.StringParam;
import com.linkedin.metadata.test.query.TestQueryResponse;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class TestPredicateEvaluatorTest {
  TestPredicateEvaluator _evaluator = TestPredicateEvaluator.getInstance();

  private TestPredicate createCompositePredicate(String operation, List<TestPredicate> childPredicates) {
    return new TestPredicate(operation,
        ImmutableMap.of(ParamKeyConstants.PREDICATES, new PredicateParam(childPredicates)));
  }

  @Test
  public void testEvaluator() {
    TestPredicate equalsRule1 = new TestPredicate("equals", ImmutableMap.of("query", new QueryParam("query1"), "values",
        new StringParam(ImmutableList.of("value1", "value2"))));
    TestPredicate equalsRule2 = new TestPredicate("equals", ImmutableMap.of("query", new QueryParam("query2"), "value",
        new StringParam(Collections.singletonList("value3"))));
    TestPredicate existsRule = new TestPredicate("exists", ImmutableMap.of("query", new QueryParam("query3")));

    TestPredicate orRule = createCompositePredicate("or", ImmutableList.of(equalsRule1, equalsRule2));
    TestPredicate andRule = createCompositePredicate("and", ImmutableList.of(equalsRule1, equalsRule2));
    TestPredicate complexRule = createCompositePredicate("or",
        ImmutableList.of(equalsRule1, createCompositePredicate("and", ImmutableList.of(equalsRule2, existsRule))));

    Map<TestQuery, TestQueryResponse> queryResponses = Collections.emptyMap();
    assertFalse(_evaluator.evaluate(equalsRule1, queryResponses));
    assertFalse(_evaluator.evaluate(equalsRule2, queryResponses));
    assertFalse(_evaluator.evaluate(existsRule, queryResponses));
    assertFalse(_evaluator.evaluate(orRule, queryResponses));
    assertFalse(_evaluator.evaluate(andRule, queryResponses));
    assertFalse(_evaluator.evaluate(complexRule, queryResponses));

    queryResponses = ImmutableMap.of(new TestQuery("query1"), new TestQueryResponse(ImmutableList.of("value1")));
    assertTrue(_evaluator.evaluate(equalsRule1, queryResponses));
    assertFalse(_evaluator.evaluate(equalsRule2, queryResponses));
    assertFalse(_evaluator.evaluate(existsRule, queryResponses));
    assertTrue(_evaluator.evaluate(orRule, queryResponses));
    assertFalse(_evaluator.evaluate(andRule, queryResponses));
    assertTrue(_evaluator.evaluate(complexRule, queryResponses));

    queryResponses = ImmutableMap.of(new TestQuery("query1"), new TestQueryResponse(ImmutableList.of("value3")));
    assertFalse(_evaluator.evaluate(equalsRule1, queryResponses));
    assertFalse(_evaluator.evaluate(equalsRule2, queryResponses));
    assertFalse(_evaluator.evaluate(existsRule, queryResponses));
    assertFalse(_evaluator.evaluate(orRule, queryResponses));
    assertFalse(_evaluator.evaluate(andRule, queryResponses));
    assertFalse(_evaluator.evaluate(complexRule, queryResponses));

    queryResponses = ImmutableMap.of(new TestQuery("query1"), new TestQueryResponse(ImmutableList.of("value1")),
        new TestQuery("query2"), new TestQueryResponse(ImmutableList.of("value3")));
    assertTrue(_evaluator.evaluate(equalsRule1, queryResponses));
    assertTrue(_evaluator.evaluate(equalsRule2, queryResponses));
    assertFalse(_evaluator.evaluate(existsRule, queryResponses));
    assertTrue(_evaluator.evaluate(orRule, queryResponses));
    assertTrue(_evaluator.evaluate(andRule, queryResponses));
    assertTrue(_evaluator.evaluate(complexRule, queryResponses));

    queryResponses = ImmutableMap.of(new TestQuery("query3"), new TestQueryResponse(ImmutableList.of("value1")));
    assertFalse(_evaluator.evaluate(equalsRule1, queryResponses));
    assertFalse(_evaluator.evaluate(equalsRule2, queryResponses));
    assertTrue(_evaluator.evaluate(existsRule, queryResponses));
    assertFalse(_evaluator.evaluate(orRule, queryResponses));
    assertFalse(_evaluator.evaluate(andRule, queryResponses));
    assertFalse(_evaluator.evaluate(complexRule, queryResponses));

    queryResponses = ImmutableMap.of(new TestQuery("query2"), new TestQueryResponse(ImmutableList.of("value3")),
        new TestQuery("query3"), new TestQueryResponse(ImmutableList.of("value1")));
    assertFalse(_evaluator.evaluate(equalsRule1, queryResponses));
    assertTrue(_evaluator.evaluate(equalsRule2, queryResponses));
    assertTrue(_evaluator.evaluate(existsRule, queryResponses));
    assertTrue(_evaluator.evaluate(orRule, queryResponses));
    assertFalse(_evaluator.evaluate(andRule, queryResponses));
    assertTrue(_evaluator.evaluate(complexRule, queryResponses));
  }
}
