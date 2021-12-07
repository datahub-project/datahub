package com.linkedin.metadata.utils.elasticsearch;

import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Criterion;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class SearchUtilsTest {
  @Test
  public void testGetQueryBuilderFromContainCriterion() {

    // Given: a 'contain' criterion
    Criterion containCriterion = new Criterion();
    containCriterion.setValue("match * text");
    containCriterion.setCondition(Condition.CONTAIN);
    containCriterion.setField("text");

    // Expect 'contain' criterion creates a MatchQueryBuilder
    QueryBuilder queryBuilder = SearchUtils.getQueryBuilderFromCriterion(containCriterion);
    assertNotNull(queryBuilder);
    assertTrue(queryBuilder instanceof WildcardQueryBuilder);

    // Expect 'field name' and search terms
    assertEquals(((WildcardQueryBuilder) queryBuilder).fieldName(), "text");
    assertEquals(((WildcardQueryBuilder) queryBuilder).value(), "*match \\* text*");
  }

  @Test
  public void testGetQueryBuilderFromStartWithCriterion() {

    // Given: a 'start_with' criterion
    Criterion containCriterion = new Criterion();
    containCriterion.setValue("match * text");
    containCriterion.setCondition(Condition.START_WITH);
    containCriterion.setField("text");

    // Expect 'start_with' criterion creates a WildcardQueryBuilder
    QueryBuilder queryBuilder = SearchUtils.getQueryBuilderFromCriterion(containCriterion);
    assertNotNull(queryBuilder);
    assertTrue(queryBuilder instanceof WildcardQueryBuilder);

    // Expect 'field name' and search terms
    assertEquals(((WildcardQueryBuilder) queryBuilder).fieldName(), "text");
    assertEquals(((WildcardQueryBuilder) queryBuilder).value(), "match \\* text*");
  }

  @Test
  public void testGetQueryBuilderFromEndWithCriterion() {

    // Given: a 'end_with' criterion
    Criterion containCriterion = new Criterion();
    containCriterion.setValue("match * text");
    containCriterion.setCondition(Condition.END_WITH);
    containCriterion.setField("text");

    // Expect 'end_with' criterion creates a MatchQueryBuilder
    QueryBuilder queryBuilder = SearchUtils.getQueryBuilderFromCriterion(containCriterion);
    assertNotNull(queryBuilder);
    assertTrue(queryBuilder instanceof WildcardQueryBuilder);

    // Expect 'field name' and search terms
    assertEquals(((WildcardQueryBuilder) queryBuilder).fieldName(), "text");
    assertEquals(((WildcardQueryBuilder) queryBuilder).value(), "*match \\* text");
  }
}