package com.linkedin.metadata.test.eval;

import com.linkedin.metadata.test.definition.CompositeTestPredicate;
import com.linkedin.metadata.test.definition.TestPredicate;
import com.linkedin.metadata.test.definition.LeafTestPredicate;
import com.linkedin.metadata.test.query.TestQuery;
import com.linkedin.metadata.test.query.TestQueryResponse;
import java.util.Collections;
import java.util.Map;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class TestRuleEvaluatorTest {
  TestPredicateEvaluator _evaluator = new TestPredicateEvaluator(LeafTestPredicateEvaluator.getInstance());

  @Test
  public void testEvaluator() {
    TestPredicate equalsRule1 =
        new LeafTestPredicate("query1", "equals", ImmutableMap.of("values", ImmutableList.of("value1", "value2")));
    TestPredicate equalsRule2 = new LeafTestPredicate("query2", "equals", ImmutableMap.of("value", "value3"));
    TestPredicate existsRule = new LeafTestPredicate("query3", "exists", ImmutableMap.of());

    TestPredicate orRule =
        new CompositeTestPredicate(
            CompositeTestPredicate.CompositionOperation.OR, ImmutableList.of(equalsRule1, equalsRule2));
    TestPredicate andRule =
        new CompositeTestPredicate(
            CompositeTestPredicate.CompositionOperation.AND, ImmutableList.of(equalsRule1, equalsRule2));
    TestPredicate complexRule = new CompositeTestPredicate(CompositeTestPredicate.CompositionOperation.OR,
        ImmutableList.of(equalsRule1, new CompositeTestPredicate(CompositeTestPredicate.CompositionOperation.AND,
            ImmutableList.of(equalsRule2, existsRule))));

    Map<TestQuery, TestQueryResponse> queryResponses = Collections.emptyMap();
    assertFalse(_evaluator.evaluate(queryResponses, equalsRule1));
    assertFalse(_evaluator.evaluate(queryResponses, equalsRule2));
    assertFalse(_evaluator.evaluate(queryResponses, existsRule));
    assertFalse(_evaluator.evaluate(queryResponses, orRule));
    assertFalse(_evaluator.evaluate(queryResponses, andRule));
    assertFalse(_evaluator.evaluate(queryResponses, complexRule));

    queryResponses = ImmutableMap.of(new TestQuery("query1"), new TestQueryResponse(ImmutableList.of("value1")));
    assertTrue(_evaluator.evaluate(queryResponses, equalsRule1));
    assertFalse(_evaluator.evaluate(queryResponses, equalsRule2));
    assertFalse(_evaluator.evaluate(queryResponses, existsRule));
    assertTrue(_evaluator.evaluate(queryResponses, orRule));
    assertFalse(_evaluator.evaluate(queryResponses, andRule));
    assertTrue(_evaluator.evaluate(queryResponses, complexRule));

    queryResponses = ImmutableMap.of(new TestQuery("query1"), new TestQueryResponse(ImmutableList.of("value3")));
    assertFalse(_evaluator.evaluate(queryResponses, equalsRule1));
    assertFalse(_evaluator.evaluate(queryResponses, equalsRule2));
    assertFalse(_evaluator.evaluate(queryResponses, existsRule));
    assertFalse(_evaluator.evaluate(queryResponses, orRule));
    assertFalse(_evaluator.evaluate(queryResponses, andRule));
    assertFalse(_evaluator.evaluate(queryResponses, complexRule));

    queryResponses = ImmutableMap.of(new TestQuery("query1"), new TestQueryResponse(ImmutableList.of("value1")),
        new TestQuery("query2"), new TestQueryResponse(ImmutableList.of("value3")));
    assertTrue(_evaluator.evaluate(queryResponses, equalsRule1));
    assertTrue(_evaluator.evaluate(queryResponses, equalsRule2));
    assertFalse(_evaluator.evaluate(queryResponses, existsRule));
    assertTrue(_evaluator.evaluate(queryResponses, orRule));
    assertTrue(_evaluator.evaluate(queryResponses, andRule));
    assertTrue(_evaluator.evaluate(queryResponses, complexRule));

    queryResponses = ImmutableMap.of(new TestQuery("query3"), new TestQueryResponse(ImmutableList.of("value1")));
    assertFalse(_evaluator.evaluate(queryResponses, equalsRule1));
    assertFalse(_evaluator.evaluate(queryResponses, equalsRule2));
    assertTrue(_evaluator.evaluate(queryResponses, existsRule));
    assertFalse(_evaluator.evaluate(queryResponses, orRule));
    assertFalse(_evaluator.evaluate(queryResponses, andRule));
    assertFalse(_evaluator.evaluate(queryResponses, complexRule));

    queryResponses = ImmutableMap.of(new TestQuery("query2"), new TestQueryResponse(ImmutableList.of("value3")),
        new TestQuery("query3"), new TestQueryResponse(ImmutableList.of("value1")));
    assertFalse(_evaluator.evaluate(queryResponses, equalsRule1));
    assertTrue(_evaluator.evaluate(queryResponses, equalsRule2));
    assertTrue(_evaluator.evaluate(queryResponses, existsRule));
    assertTrue(_evaluator.evaluate(queryResponses, orRule));
    assertFalse(_evaluator.evaluate(queryResponses, andRule));
    assertTrue(_evaluator.evaluate(queryResponses, complexRule));
  }
}
