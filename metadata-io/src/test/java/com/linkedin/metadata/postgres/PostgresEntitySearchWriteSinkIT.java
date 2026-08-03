package com.linkedin.metadata.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.PostgresTestUtils;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.search.postgres.PostgresEntitySearchWriteSink;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Integration test for {@link PostgresEntitySearchWriteSink} against a real PostgreSQL instance
 * (Testcontainers).
 */
public class PostgresEntitySearchWriteSinkIT {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private String schema;
  private String tablePrefix;

  private static final String DATASET_DOC =
      "{\"urn\":\"urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)\","
          + "\"_entityType\":\"dataset\","
          + "\"_aspects\":{\"datasetProperties\":{"
          + "\"name\":\"PgItName\",\"qualifiedName\":\"q\",\"description\":\"PgItDesc\","
          + "\"_systemmetadata\":{\"version\":7,\"runId\":\"pg_it_run\"}"
          + "}}}";

  private Database database;
  private PostgresSqlSetupProperties props;
  private PostgresEntitySearchWriteSink sink;
  private OperationContext opContext;

  @BeforeClass
  public void beforeClass() throws Exception {
    PostgresTestUtils.IntegrationNamespace ns =
        PostgresTestUtils.newIntegrationNamespace("pgsearch");
    schema = ns.getSchema();
    tablePrefix = ns.getTablePrefix();

    PostgreSQLContainer<?> postgres = PostgresTestUtils.startPostgresWithPgvector();
    props = PostgresTestUtils.testPgSearchProperties(schema, tablePrefix);
    database =
        PostgresTestUtils.createEbeanDatabase(
            postgres, PostgresTestUtils.uniqueServerName("pgsearch_it"));
    try (Connection c = database.dataSource().getConnection()) {
      c.setAutoCommit(false);
      PostgresTestUtils.applyPgSearchEntityTables(c, props);
    }
    sink = new PostgresEntitySearchWriteSink(database, props);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @AfterClass(alwaysRun = true)
  public void afterClass() {
    EbeanTestUtils.shutdownDatabase(database);
  }

  @BeforeMethod
  public void truncate() throws Exception {
    try (Connection c = database.dataSource().getConnection()) {
      c.setAutoCommit(false);
      PostgresTestUtils.truncateSearchRow(c, props);
    }
  }

  @Test(priority = -10)
  public void tierEmbeddingColumnsAbsentWhenVectorDisabled() throws Exception {
    try (Connection c = database.dataSource().getConnection();
        Statement st = c.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = '"
                    + schema
                    + "' AND table_name = '"
                    + tablePrefix.toLowerCase()
                    + "_search_row' AND column_name LIKE 'embedding_tier%'")) {
      assertTrue(rs.next());
      assertEquals(rs.getInt(1), 0);
    }
  }

  @Test
  public void upsertThenDeleteRoundTrip() throws Exception {
    String urn = "urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)";
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, urn);

    String qualified = schema + "." + tablePrefix + "_search_row";
    try (Connection c = database.dataSource().getConnection();
        Statement st = c.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT document::text, search_text_tier1::text, systemmetadata::text, entity_type FROM "
                    + qualified
                    + " WHERE urn = '"
                    + urn.replace("'", "''")
                    + "' AND search_group = 'primary'")) {
      assertTrue(rs.next());
      JsonNode expectedDoc = MAPPER.readTree(DATASET_DOC);
      JsonNode storedDoc = MAPPER.readTree(rs.getString(1));
      // JsonNode implements Iterable — TestNG assertEquals would compare iterators, not tree value
      assertTrue(
          storedDoc.equals(expectedDoc),
          "stored document != expected; stored=" + storedDoc + " expected=" + expectedDoc);
      String tier1 = rs.getString(2);
      assertFalse(tier1 == null || tier1.isBlank());
      JsonNode sysMeta = MAPPER.readTree(rs.getString(3));
      assertTrue(sysMeta.has("datasetProperties"));
      assertTrue(sysMeta.get("datasetProperties").get("runId").asText().equals("pg_it_run"));
      assertEquals(rs.getString(4), "dataset");
    }

    sink.deleteDocumentBySearchGroup(opContext, "primary", urn);
    try (Connection c = database.dataSource().getConnection();
        Statement st = c.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT 1 FROM " + qualified + " WHERE urn = '" + urn.replace("'", "''") + "'")) {
      assertFalse(rs.next());
    }
  }

  /**
   * When the indexer sends a document without per-aspect {@code _systemmetadata}, the column keeps
   * the prior aspect payload (same as {@link
   * com.linkedin.metadata.search.postgres.PostgresSearchSystemMetadataJson#mergeAspectSystemMetadataForUpsert}).
   */
  @Test
  public void systemMetadataPreservedWhenLaterUpsertOmitsAspectSystemMetadata() throws Exception {
    String urn = "urn:li:dataset:(urn:li:dataPlatform:hive,db.siblings_keep,PROD)";
    String withSm =
        "{\"urn\":\"urn:li:dataset:(urn:li:dataPlatform:hive,db.siblings_keep,PROD)\","
            + "\"_entityType\":\"dataset\","
            + "\"_aspects\":{\"siblings\":{\"hasSiblings\":true,"
            + "\"_systemmetadata\":{\"version\":\"1\",\"schemaVersion\":1}}}}";
    String withoutSm =
        "{\"urn\":\"urn:li:dataset:(urn:li:dataPlatform:hive,db.siblings_keep,PROD)\","
            + "\"_entityType\":\"dataset\","
            + "\"_aspects\":{\"siblings\":{\"siblings\":[],\"hasSiblings\":true}}}";

    sink.upsertDocumentBySearchGroup(opContext, "primary", withSm, urn);
    sink.upsertDocumentBySearchGroup(opContext, "primary", withoutSm, urn);

    String qualified = schema + "." + tablePrefix + "_search_row";
    try (Connection c = database.dataSource().getConnection();
        Statement st = c.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT systemmetadata::text FROM "
                    + qualified
                    + " WHERE urn = '"
                    + urn.replace("'", "''")
                    + "' AND search_group = 'primary'")) {
      assertTrue(rs.next());
      JsonNode sm = MAPPER.readTree(rs.getString(1));
      assertTrue(sm.has("siblings"));
      assertTrue(sm.get("siblings").get("version").asText().equals("1"));
    }
  }

  @Test(priority = 10)
  public void upsertWithVectorEnabledLeavesEmbeddingsNull() throws Exception {
    try (Connection c = database.dataSource().getConnection()) {
      c.setAutoCommit(false);
      props.getPgSearch().getEntity().getVector().setEnabled(true);
      props.getPgSearch().getEntity().getVector().setEmbeddingDimensions(3);
      PostgresTestUtils.applyPgSearchEntityTables(c, props);
    }
    sink = new PostgresEntitySearchWriteSink(database, props);

    String urn = "urn:li:dataset:(urn:li:dataPlatform:hive,db.vec,PROD)";
    sink.upsertDocumentBySearchGroup(opContext, "primary", DATASET_DOC, urn);

    String qualified = schema + "." + tablePrefix + "_search_row";
    try (Connection c = database.dataSource().getConnection();
        Statement st = c.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = '"
                    + schema
                    + "' AND table_name = '"
                    + tablePrefix.toLowerCase()
                    + "_search_row' AND column_name LIKE 'embedding_tier%'")) {
      assertTrue(rs.next());
      int tierCols = props.getPgSearch().getEntity().getFulltext().getTierTsvectorColumnCount();
      assertEquals(rs.getInt(1), tierCols);
    }
    try (Connection c = database.dataSource().getConnection();
        Statement st = c.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT embedding_tier1 IS NULL FROM "
                    + qualified
                    + " WHERE urn = '"
                    + urn.replace("'", "''")
                    + "' AND search_group = 'primary'")) {
      assertTrue(rs.next());
      assertTrue(rs.getBoolean(1));
    }

    try (Connection c = database.dataSource().getConnection()) {
      c.setAutoCommit(false);
      props.getPgSearch().getEntity().getVector().setEnabled(false);
      PostgresTestUtils.applyPgSearchEntityTables(c, props);
    }
    sink = new PostgresEntitySearchWriteSink(database, props);
  }
}
