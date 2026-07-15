package com.linkedin.metadata.datahubusage.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.PostgresTestUtils;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.datahubusage.DataHubUsageEventConstants;
import com.linkedin.metadata.datahubusage.DataHubUsageEventType;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Integration tests for {@link PostgresUsageEventsRecommendationQueries} (Postgres-backed home-page
 * recommendations). Pins the {@code mostPopularEntityUrns} SQL parses cleanly with and without the
 * peer-actor IN clause, and that {@code recentlyEditedEntityUrns} / {@code
 * recentlyViewedEntityUrns} order results by max timestamp.
 */
public class PostgresUsageEventsRecommendationQueriesIT {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private PostgreSQLContainer<?> postgres;
  private Database database;
  private PostgresUsageEventsStore store;
  private PostgresUsageEventsRecommendationQueries recos;

  @BeforeClass
  public void init() throws Exception {
    postgres = PostgresTestUtils.startPostgres();
    PostgresTestUtils.IntegrationNamespace ns = PostgresTestUtils.newIntegrationNamespace("pgreco");
    database =
        PostgresTestUtils.createEbeanDatabase(
            postgres, PostgresTestUtils.uniqueServerName("pgreco_it"));
    PostgresSqlSetupProperties props = new PostgresSqlSetupProperties();
    props.setSchema(ns.getSchema());

    try (Connection c = database.dataSource().getConnection()) {
      c.setAutoCommit(false);
      try (Statement st = c.createStatement()) {
        st.execute("CREATE SCHEMA IF NOT EXISTS " + ns.getSchema());
      }
      PostgresTestUtils.applyPgUsageEventsTables(c, props);
    }

    store = new PostgresUsageEventsStore(database, props, 12, 2);
    recos = new PostgresUsageEventsRecommendationQueries(store, 30);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    EbeanTestUtils.shutdownDatabase(database);
  }

  @BeforeMethod
  public void truncateRows() throws SQLException {
    try (Connection c = database.dataSource().getConnection();
        Statement st = c.createStatement()) {
      st.execute("TRUNCATE TABLE " + store.qualifiedParentTable());
    }
  }

  @Test
  public void testMostPopularEntityUrns_noPeerFilter() throws Exception {
    long now = System.currentTimeMillis();
    insertViewEvents(
        view("v1", now, "urn:li:dataset:(a,b,PROD)", "urn:li:corpuser:alice"),
        view("v2", now - 1_000, "urn:li:dataset:(a,b,PROD)", "urn:li:corpuser:bob"),
        view("v3", now - 2_000, "urn:li:dataset:(a,b,PROD)", "urn:li:corpuser:carol"),
        view("v4", now - 3_000, "urn:li:dataset:(c,d,PROD)", "urn:li:corpuser:alice"));

    List<String> popular = recos.mostPopularEntityUrns((List<String>) null);

    Assert.assertEquals(popular.size(), 2);
    Assert.assertEquals(popular.get(0), "urn:li:dataset:(a,b,PROD)", "ranked first by COUNT");
    Assert.assertEquals(popular.get(1), "urn:li:dataset:(c,d,PROD)");
  }

  @Test
  public void testMostPopularEntityUrns_peerFilterRestrictsActors() throws Exception {
    // Pins the IN (?,?,...) parameter binding for peer actors.
    long now = System.currentTimeMillis();
    insertViewEvents(
        view("v1", now, "urn:li:dataset:(a,b,PROD)", "urn:li:corpuser:alice"),
        view("v2", now - 1_000, "urn:li:dataset:(c,d,PROD)", "urn:li:corpuser:bob"),
        view("v3", now - 2_000, "urn:li:dataset:(c,d,PROD)", "urn:li:corpuser:carol"));

    List<String> popular =
        recos.mostPopularEntityUrns(List.of("urn:li:corpuser:bob", "urn:li:corpuser:carol"));

    Assert.assertEquals(popular.size(), 1);
    Assert.assertEquals(popular.get(0), "urn:li:dataset:(c,d,PROD)");
  }

  @Test
  public void testMostPopularEntityUrns_excludesBackendUsage() throws Exception {
    long now = System.currentTimeMillis();
    Map<String, Object> backendDoc =
        view("b1", now, "urn:li:dataset:(backend,only,PROD)", "urn:li:corpuser:alice");
    backendDoc.put(
        DataHubUsageEventConstants.USAGE_SOURCE, DataHubUsageEventConstants.BACKEND_SOURCE);

    insertViewEvents(
        backendDoc, view("v1", now - 1_000, "urn:li:dataset:(a,b,PROD)", "urn:li:corpuser:alice"));

    List<String> popular = recos.mostPopularEntityUrns((List<String>) null);

    Assert.assertEquals(popular.size(), 1);
    Assert.assertEquals(popular.get(0), "urn:li:dataset:(a,b,PROD)");
  }

  @Test
  public void testRecentlyEditedUrnsOrdered_byMaxTimestamp() throws Exception {
    long now = System.currentTimeMillis();
    insertEvents(
        action("a1", now - 5_000, "urn:li:dataset:(a,b,PROD)", "urn:li:corpuser:alice"),
        action("a2", now, "urn:li:dataset:(a,b,PROD)", "urn:li:corpuser:alice"),
        action("a3", now - 1_000, "urn:li:dataset:(c,d,PROD)", "urn:li:corpuser:alice"),
        action("a4", now - 2_000, "urn:li:dataset:(e,f,PROD)", "urn:li:corpuser:other"));

    java.util.LinkedHashMap<String, Long> ranked =
        recos.recentlyEditedUrnsOrdered(
            com.linkedin.common.urn.Urn.createFromString("urn:li:corpuser:alice"));

    List<String> ordered = new ArrayList<>(ranked.keySet());
    Assert.assertEquals(ordered.size(), 2);
    Assert.assertEquals(ordered.get(0), "urn:li:dataset:(a,b,PROD)");
    Assert.assertEquals(ordered.get(1), "urn:li:dataset:(c,d,PROD)");
  }

  @Test
  public void testIsRecommendationDataAvailable_afterPartitionEnsured() throws Exception {
    // ensureParentTable was called in @BeforeClass; insertBatch ensures a partition is created.
    long now = System.currentTimeMillis();
    insertViewEvents(view("v1", now, "urn:li:dataset:(x,y,PROD)", "urn:li:corpuser:alice"));
    Assert.assertTrue(recos.isRecommendationDataAvailable());
  }

  private static Map<String, Object> view(
      String id, long timestampMs, String entityUrn, String actorUrn) {
    return event(
        id, timestampMs, DataHubUsageEventType.ENTITY_VIEW_EVENT.getType(), actorUrn, entityUrn);
  }

  private static Map<String, Object> action(
      String id, long timestampMs, String entityUrn, String actorUrn) {
    return event(
        id, timestampMs, DataHubUsageEventType.ENTITY_ACTION_EVENT.getType(), actorUrn, entityUrn);
  }

  private static Map<String, Object> event(
      String id, long timestampMs, String type, String actorUrn, String entityUrn) {
    Map<String, Object> spec = new LinkedHashMap<>();
    spec.put(DataHubUsageEventConstants.TYPE, type);
    spec.put(DataHubUsageEventConstants.TIMESTAMP, timestampMs);
    spec.put(DataHubUsageEventConstants.ACTOR_URN, actorUrn);
    spec.put(DataHubUsageEventConstants.ENTITY_URN, entityUrn);
    spec.put("__id", id);
    return spec;
  }

  private void insertViewEvents(Map<String, Object>... events) throws Exception {
    insertEvents(events);
  }

  private void insertEvents(Map<String, Object>... events) throws Exception {
    List<PostgresUsageEventInsertRow> rows = new ArrayList<>();
    for (Map<String, Object> spec : events) {
      String id = (String) spec.remove("__id");
      long ts = ((Number) spec.get(DataHubUsageEventConstants.TIMESTAMP)).longValue();
      rows.add(
          PostgresUsageEventInsertRow.builder()
              .id(id)
              .timestampMs(ts)
              .eventType((String) spec.get(DataHubUsageEventConstants.TYPE))
              .usageSource((String) spec.get(DataHubUsageEventConstants.USAGE_SOURCE))
              .actorUrn((String) spec.get(DataHubUsageEventConstants.ACTOR_URN))
              .entityUrn((String) spec.get(DataHubUsageEventConstants.ENTITY_URN))
              .documentJson(MAPPER.writeValueAsString(spec))
              .build());
    }
    store.insertBatch(rows);
  }
}
