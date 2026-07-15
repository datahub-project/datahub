package com.linkedin.metadata.datahubusage;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.PostgresTestUtils;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.datahubusage.postgres.PostgresUsageEventInsertRow;
import com.linkedin.metadata.datahubusage.postgres.PostgresUsageEventsStore;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
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
 * Integration tests for {@link PostgresDataHubUsageService} backed by a real PostgreSQL container.
 *
 * <p>Pinned regressions:
 *
 * <ul>
 *   <li>{@code testExternalAuditEventsSearch_paginationAcrossTwoPages} reproduces the search_after
 *       AND clause and guards the paren-balance fix in {@link PostgresDataHubUsageService}.
 *   <li>The filter cases exercise the {@code IN (?,?,...)} fragment built by {@code inClause} so
 *       parameter binding stays in sync with the WHERE shape.
 * </ul>
 */
public class PostgresDataHubUsageServiceIT {

  private static final String INDEX_NAME = "datahub_usage_event";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private PostgreSQLContainer<?> postgres;
  private Database database;
  private PostgresUsageEventsStore store;
  private PostgresDataHubUsageService service;
  private OperationContext opContext;

  @BeforeClass
  public void init() throws Exception {
    postgres = PostgresTestUtils.startPostgres();
    PostgresTestUtils.IntegrationNamespace ns = PostgresTestUtils.newIntegrationNamespace("pgduit");
    database =
        PostgresTestUtils.createEbeanDatabase(
            postgres, PostgresTestUtils.uniqueServerName("pgdu_audit_it"));
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

    IndexConvention indexConvention = mock(IndexConvention.class);
    when(indexConvention.getIndexName(anyString())).thenReturn(INDEX_NAME);

    service = new PostgresDataHubUsageService(store, indexConvention);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
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
  public void testExternalAuditEventsSearch_basicHappyPath() throws Exception {
    long now = System.currentTimeMillis();
    insertEvents(
        rowOf(
            "id-1",
            now,
            "PageViewEvent",
            "urn:li:corpuser:alice",
            "ownership",
            "dataset",
            "urn:li:dataset:(a,b,PROD)"),
        rowOf("id-2", now - 1_000, "SearchEvent", "urn:li:corpuser:bob", null, null, null));

    ExternalAuditEventsSearchRequest req =
        ExternalAuditEventsSearchRequest.builder().size(10).startTime(-1).endTime(-1).build();

    ExternalAuditEventsSearchResponse resp = service.externalAuditEventsSearch(opContext, req);

    Assert.assertEquals(resp.getTotal(), 2);
    Assert.assertEquals(resp.getCount(), 2);
    Assert.assertEquals(resp.getUsageEvents().size(), 2);
    Assert.assertEquals(resp.getUsageEvents().get(0).getEventType(), "PageViewEvent");
    Assert.assertEquals(resp.getUsageEvents().get(1).getEventType(), "SearchEvent");
    Assert.assertNull(resp.getNextScrollId(), "no scroll id when fewer rows than page size");
  }

  @Test
  public void testExternalAuditEventsSearch_paginationAcrossTwoPages() throws Exception {
    // Pin the concern-6 paren-balance fix: any non-null scrollId triggers the search_after AND
    // clause; if the SQL is unbalanced PostgreSQL throws at parse time and page 2 fails.
    long base = System.currentTimeMillis();
    insertEvents(
        rowOf(
            "id-a",
            base,
            "PageViewEvent",
            "urn:li:corpuser:alice",
            "ownership",
            "dataset",
            "urn:li:dataset:(a,b,PROD)"),
        rowOf(
            "id-b",
            base - 1_000,
            "PageViewEvent",
            "urn:li:corpuser:bob",
            "ownership",
            "dataset",
            "urn:li:dataset:(c,d,PROD)"),
        rowOf("id-c", base - 2_000, "SearchEvent", "urn:li:corpuser:carol", null, null, null),
        rowOf("id-d", base - 3_000, "SearchEvent", "urn:li:corpuser:dan", null, null, null),
        rowOf("id-e", base - 4_000, "PageViewEvent", "urn:li:corpuser:eve", null, null, null));

    ExternalAuditEventsSearchRequest page1Req =
        ExternalAuditEventsSearchRequest.builder().size(2).startTime(-1).endTime(-1).build();
    ExternalAuditEventsSearchResponse page1 =
        service.externalAuditEventsSearch(opContext, page1Req);
    Assert.assertEquals(page1.getCount(), 2);
    Assert.assertEquals(page1.getTotal(), 5);
    Assert.assertNotNull(page1.getNextScrollId(), "page 1 must hand back a scroll id");

    ExternalAuditEventsSearchRequest page2Req =
        ExternalAuditEventsSearchRequest.builder()
            .size(2)
            .startTime(-1)
            .endTime(-1)
            .scrollId(page1.getNextScrollId())
            .build();
    ExternalAuditEventsSearchResponse page2 =
        service.externalAuditEventsSearch(opContext, page2Req);
    Assert.assertEquals(page2.getCount(), 2);
    Assert.assertEquals(page2.getTotal(), 5);
    Assert.assertNotNull(page2.getNextScrollId());

    ExternalAuditEventsSearchRequest page3Req =
        ExternalAuditEventsSearchRequest.builder()
            .size(2)
            .startTime(-1)
            .endTime(-1)
            .scrollId(page2.getNextScrollId())
            .build();
    ExternalAuditEventsSearchResponse page3 =
        service.externalAuditEventsSearch(opContext, page3Req);
    Assert.assertEquals(page3.getCount(), 1);
    Assert.assertEquals(page3.getTotal(), 5);
    Assert.assertNull(
        page3.getNextScrollId(), "no scroll id when last page returns fewer than size");

    // Pages collectively covered all 5 events without duplicates.
    List<String> seen = new ArrayList<>();
    page1.getUsageEvents().forEach(e -> seen.add(e.getActorUrn()));
    page2.getUsageEvents().forEach(e -> seen.add(e.getActorUrn()));
    page3.getUsageEvents().forEach(e -> seen.add(e.getActorUrn()));
    Assert.assertEquals(seen.size(), 5);
    Assert.assertEquals(seen.stream().distinct().count(), 5L);
  }

  @Test
  public void testExternalAuditEventsSearch_filters() throws Exception {
    long now = System.currentTimeMillis();
    insertEvents(
        rowOf(
            "id-1",
            now,
            "PageViewEvent",
            "urn:li:corpuser:alice",
            "ownership",
            "dataset",
            "urn:li:dataset:(a,b,PROD)"),
        rowOf(
            "id-2",
            now - 1_000,
            "SearchEvent",
            "urn:li:corpuser:bob",
            "documentation",
            "chart",
            "urn:li:chart:(c1)"),
        rowOf(
            "id-3",
            now - 2_000,
            "PageViewEvent",
            "urn:li:corpuser:carol",
            "ownership",
            "dashboard",
            "urn:li:dashboard:(d1)"));

    ExternalAuditEventsSearchResponse byEventType =
        service.externalAuditEventsSearch(
            opContext,
            ExternalAuditEventsSearchRequest.builder()
                .size(10)
                .startTime(-1)
                .endTime(-1)
                .eventTypes(List.of("PageViewEvent"))
                .build());
    Assert.assertEquals(byEventType.getTotal(), 2);
    byEventType
        .getUsageEvents()
        .forEach(e -> Assert.assertEquals(e.getEventType(), "PageViewEvent"));

    ExternalAuditEventsSearchResponse byAspect =
        service.externalAuditEventsSearch(
            opContext,
            ExternalAuditEventsSearchRequest.builder()
                .size(10)
                .startTime(-1)
                .endTime(-1)
                .aspectTypes(List.of("documentation"))
                .build());
    Assert.assertEquals(byAspect.getTotal(), 1);

    ExternalAuditEventsSearchResponse byEntity =
        service.externalAuditEventsSearch(
            opContext,
            ExternalAuditEventsSearchRequest.builder()
                .size(10)
                .startTime(-1)
                .endTime(-1)
                .entityTypes(List.of("dataset", "chart"))
                .build());
    Assert.assertEquals(byEntity.getTotal(), 2);

    ExternalAuditEventsSearchResponse byActor =
        service.externalAuditEventsSearch(
            opContext,
            ExternalAuditEventsSearchRequest.builder()
                .size(10)
                .startTime(-1)
                .endTime(-1)
                .actorUrns(List.of("urn:li:corpuser:alice"))
                .build());
    Assert.assertEquals(byActor.getTotal(), 1);
    Assert.assertEquals(byActor.getUsageEvents().get(0).getActorUrn(), "urn:li:corpuser:alice");
  }

  @Test
  public void testGetUsageIndexName() {
    Assert.assertEquals(service.getUsageIndexName(), INDEX_NAME);
  }

  /**
   * Builds a "row spec" map (kept separate from the JSON document) so the JSONB body matches the
   * shape consumed by {@link DataHubUsageEventResultMapper}.
   */
  private static Map<String, Object> rowOf(
      String id,
      long timestampMs,
      String type,
      String actorUrn,
      String aspectName,
      String entityType,
      String entityUrn) {
    Map<String, Object> spec = new LinkedHashMap<>();
    spec.put(DataHubUsageEventConstants.TYPE, type);
    spec.put(DataHubUsageEventConstants.TIMESTAMP, timestampMs);
    spec.put(DataHubUsageEventConstants.ACTOR_URN, actorUrn);
    spec.put(DataHubUsageEventConstants.USAGE_SOURCE, DataHubUsageEventConstants.BACKEND_SOURCE);
    if (aspectName != null) {
      spec.put(DataHubUsageEventConstants.ASPECT_NAME, aspectName);
    }
    if (entityType != null) {
      spec.put(DataHubUsageEventConstants.ENTITY_TYPE, entityType);
    }
    if (entityUrn != null) {
      spec.put(DataHubUsageEventConstants.ENTITY_URN, entityUrn);
    }
    spec.put("__id", id);
    return spec;
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
              .aspectName((String) spec.get(DataHubUsageEventConstants.ASPECT_NAME))
              .entityType((String) spec.get(DataHubUsageEventConstants.ENTITY_TYPE))
              .entityUrn((String) spec.get(DataHubUsageEventConstants.ENTITY_URN))
              .documentJson(MAPPER.writeValueAsString(spec))
              .build());
    }
    store.insertBatch(rows);
  }
}
