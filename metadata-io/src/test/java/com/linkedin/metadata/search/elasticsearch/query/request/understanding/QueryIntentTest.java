package com.linkedin.metadata.search.elasticsearch.query.request.understanding;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class QueryIntentTest {

  @Test
  public void testStartingTierIdentity() {
    assertEquals(QueryIntent.IDENTITY.startingTier(), 1);
  }

  @Test
  public void testStartingTierFqn() {
    assertEquals(QueryIntent.FQN.startingTier(), 1);
  }

  @Test
  public void testStartingTierExactName() {
    assertEquals(QueryIntent.EXACT_NAME.startingTier(), 2);
  }

  @Test
  public void testStartingTierKeyword() {
    assertEquals(QueryIntent.KEYWORD.startingTier(), 3);
  }

  @Test
  public void testStartingTierExploratory() {
    assertEquals(QueryIntent.EXPLORATORY.startingTier(), 4);
  }
}
