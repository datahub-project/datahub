package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.sqlsetup.postgres.pgsearch.PgSearchEntitySqlSetupSupport;
import io.ebean.Database;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PgSearchEntitySchemaStepTest {

  @Mock private Database mockDatabase;

  private PgSearchEntitySchemaStep step;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    PostgresSqlSetupProperties props = PostgresSqlSetupProperties.disabled();
    props.getPgSearch().getEntity().setEnabled(true);
    props.getPgSearch().getEntity().getVector().setEnabled(false);
    step = new PgSearchEntitySchemaStep(mockDatabase, props);
  }

  @Test
  public void testId() {
    assertEquals(step.id(), "PgSearchEntitySchemaStep");
  }

  @Test
  public void testRetryCount() {
    assertEquals(step.retryCount(), 0);
  }

  @Test
  public void testBuildTierTsvectorColumnDefinitions() {
    String ddl = PgSearchEntitySqlSetupSupport.buildTierTsvectorColumnDefinitions(2);
    assertTrue(ddl.contains("search_vector_tier1 tsvector"));
    assertTrue(ddl.contains("search_vector_tier2 tsvector"));
  }

  @Test
  public void testBuildTierTsvectorIndexDefinitions() {
    String idx = PgSearchEntitySqlSetupSupport.buildTierTsvectorIndexDefinitions(1);
    assertTrue(idx.contains("search_vector_tier1"));
    assertTrue(idx.contains("USING gin (search_vector_tier1)"));
  }

  @Test
  public void testBuildTierTextColumnDefinitions() {
    String ddl = PgSearchEntitySqlSetupSupport.buildTierTextColumnDefinitions(2);
    assertTrue(ddl.contains("search_text_tier1 TEXT"));
    assertTrue(ddl.contains("search_text_tier2 TEXT"));
  }

  @Test
  public void testBuildTierEmbeddingVectorColumnDefinitionsForCreateTable() {
    String ddl =
        PostgresSqlSetupProperties.buildTierEmbeddingVectorColumnDefinitionsForCreateTable(2, 384);
    assertTrue(ddl.contains("embedding_tier1 vector(384)"));
    assertTrue(ddl.contains("embedding_tier2 vector(384)"));
    assertTrue(ddl.endsWith(",\n"));
  }

  @Test
  public void testCreateTableEmbeddingFragmentUsesResolvedPrefix() {
    String tablePrefix = "metadata_search";
    String sql =
        "CREATE TABLE __PGSEARCH_PREFIX___search_row (\n__PGSEARCH_TIER_EMBEDDING_VECTOR_COLUMNS__\n)"
            .replace(
                "__PGSEARCH_TIER_EMBEDDING_VECTOR_COLUMNS__",
                PostgresSqlSetupProperties.buildTierEmbeddingVectorColumnDefinitionsForCreateTable(
                    1, 8))
            .replace("__PGSEARCH_PREFIX__", tablePrefix);
    assertTrue(sql.contains(tablePrefix + "_search_row"));
    assertTrue(sql.contains("embedding_tier1 vector(8)"));
    assertFalse(sql.contains("__PGSEARCH_PREFIX__"));
    assertFalse(sql.contains("__PGSEARCH_TIER_EMBEDDING_VECTOR_COLUMNS__"));
  }
}
