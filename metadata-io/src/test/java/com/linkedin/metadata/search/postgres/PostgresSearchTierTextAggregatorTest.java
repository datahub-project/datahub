package com.linkedin.metadata.search.postgres;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.Test;

public class PostgresSearchTierTextAggregatorTest {

  private static final String DATASET_DOC =
      "{\"urn\":\"urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)\","
          + "\"_entityType\":\"dataset\","
          + "\"_aspects\":{\"datasetProperties\":{"
          + "\"name\":\"MyName\",\"qualifiedName\":\"q\",\"description\":\"DescBody\"}}}";

  private static final String CORP_USER_DOC =
      "{\"urn\":\"urn:li:corpuser:datahub\","
          + "\"_entityType\":\"corpuser\","
          + "\"_aspects\":{"
          + "\"corpUserKey\":{\"ldap\":\"datahub\"},"
          + "\"corpUserInfo\":{"
          + "\"displayName\":\"DataHub Admin\","
          + "\"email\":\"datahub@datahub.io\","
          + "\"fullName\":\"DataHub Admin\","
          + "\"title\":\"Platform Admin\"}}}";

  @Test
  public void deriveTierPlainTextsSeparatesTiersWhenColumnCountMatches() {
    OperationContext ctx = TestOperationContexts.systemContextNoSearchAuthorization();
    EntityRegistry registry = ctx.getEntityRegistry();
    String[] tiers =
        PostgresSearchTierTextAggregator.deriveTierPlainTexts(registry, DATASET_DOC, 2);
    assertTrue(tiers[0].contains("MyName"), tiers[0]);
    assertTrue(tiers[0].contains("q"), tiers[0]);
    assertTrue(tiers[1].contains("DescBody"), tiers[1]);
    assertFalse(tiers[1].contains("MyName"));
  }

  @Test
  public void deriveTierPlainTextsClampsHighTiersToLastColumn() {
    OperationContext ctx = TestOperationContexts.systemContextNoSearchAuthorization();
    EntityRegistry registry = ctx.getEntityRegistry();
    String[] tiers =
        PostgresSearchTierTextAggregator.deriveTierPlainTexts(registry, DATASET_DOC, 1);
    assertTrue(tiers[0].contains("MyName"));
    assertTrue(tiers[0].contains("DescBody"));
  }

  @Test
  public void corpUserTier1IncludesUsernameAndNames() {
    OperationContext ctx = TestOperationContexts.systemContextNoSearchAuthorization();
    EntityRegistry registry = ctx.getEntityRegistry();
    String[] tiers =
        PostgresSearchTierTextAggregator.deriveTierPlainTexts(registry, CORP_USER_DOC, 2);
    assertTrue(tiers[0].contains("datahub"), "tier1 should contain username: " + tiers[0]);
    assertTrue(tiers[0].contains("DataHub Admin"), "tier1 should contain displayName: " + tiers[0]);
    assertTrue(tiers[0].contains("datahub@datahub.io"), "tier1 should contain email: " + tiers[0]);
    assertTrue(tiers[1].contains("Platform Admin"), "tier2 should contain title: " + tiers[1]);
  }
}
