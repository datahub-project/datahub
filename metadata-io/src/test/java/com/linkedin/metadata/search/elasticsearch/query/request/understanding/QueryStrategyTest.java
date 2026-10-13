package com.linkedin.metadata.search.elasticsearch.query.request.understanding;

import static org.testng.Assert.*;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.testng.annotations.Test;

public class QueryStrategyTest {

  // --- Tier 1: IdentityQueryStrategy ---

  @Test
  public void testIdentityStrategyUrn() {
    IdentityQueryStrategy strategy = new IdentityQueryStrategy();
    QueryBuilder query =
        strategy.buildQuery("urn:li:dataset:(urn:li:dataPlatform:hive,my_db.orders,PROD)", null);
    assertNotNull(query);
    assertTrue(query instanceof BoolQueryBuilder);
    BoolQueryBuilder bool = (BoolQueryBuilder) query;
    assertTrue(bool.should().size() >= 3);
  }

  @Test
  public void testIdentityStrategyS3() {
    IdentityQueryStrategy strategy = new IdentityQueryStrategy();
    QueryBuilder query = strategy.buildQuery("s3://bucket/path/to/dataset", null);
    assertNotNull(query);
  }

  @Test
  public void testIdentityStrategyFqn() {
    IdentityQueryStrategy strategy = new IdentityQueryStrategy();
    QueryBuilder query = strategy.buildQuery("my_db.sales.orders", null);
    assertNotNull(query);
    assertTrue(query instanceof BoolQueryBuilder);
    BoolQueryBuilder bool = (BoolQueryBuilder) query;
    // FQN should include name.keyword for last segment
    assertTrue(bool.should().size() >= 4);
  }

  @Test
  public void testIdentityStrategyTier() {
    assertEquals(new IdentityQueryStrategy().tier(), 1);
    assertEquals(new IdentityQueryStrategy().name(), "IDENTITY");
  }

  @Test
  public void testIdentityStrategyEmptyQuery() {
    assertNull(new IdentityQueryStrategy().buildQuery("", null));
  }
}
