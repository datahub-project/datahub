package com.linkedin.metadata.systemmetadata.postgres;

import static io.datahubproject.test.search.SearchTestUtils.TEST_SYSTEM_METADATA_SERVICE_CONFIG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.PostgresTestUtils;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.IngestionRunSummary;
import com.linkedin.metadata.systemmetadata.KeyAspectCount;
import com.linkedin.metadata.systemmetadata.PostgresSystemMetadataService;
import com.linkedin.metadata.systemmetadata.scroll.PostgresSystemMetadataScrollClient;
import com.linkedin.metadata.systemmetadata.scroll.SystemMetadataScrollRequest;
import com.linkedin.metadata.systemmetadata.scroll.SystemMetadataScrollResult;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Integration tests for {@link PostgresSystemMetadataService} on PostgreSQL (SqlSetup-aligned DDL
 * via {@link PostgresTestUtils#applyPgSystemMetadataTables}).
 */
public class PostgresSystemMetadataServiceIT {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private Database database;
  private PostgresSqlSetupProperties props;
  private PostgresSystemMetadataService service;

  @BeforeClass
  public void beforeClass() throws Exception {
    PostgresTestUtils.IntegrationNamespace ns =
        PostgresTestUtils.newIntegrationNamespace("pg_sysmeta");
    props = PostgresTestUtils.testPgSystemMetadataProperties(ns.getSchema(), ns.getTablePrefix());
    PostgreSQLContainer<?> postgres = PostgresTestUtils.startPostgres();
    database =
        PostgresTestUtils.createEbeanDatabase(
            postgres, PostgresTestUtils.uniqueServerName("pg_sysmeta_it"));
    try (var c = database.dataSource().getConnection()) {
      PostgresTestUtils.applyPgSystemMetadataTables(c, props);
    }
    service =
        new PostgresSystemMetadataService(
            database, props, TEST_SYSTEM_METADATA_SERVICE_CONFIG, "MD5");
  }

  @AfterClass(alwaysRun = true)
  public void afterClass() {
    EbeanTestUtils.shutdownDatabase(database);
  }

  @BeforeMethod
  public void truncate() throws Exception {
    try (var c = database.dataSource().getConnection()) {
      c.setAutoCommit(false);
      PostgresTestUtils.truncatePgSystemMetadata(c, props);
    }
  }

  @Test
  public void listRuns_groupsByRunId_orderedByLatestTimestampDesc() {
    SystemMetadata metadata1 = new SystemMetadata();
    metadata1.setRunId("abc-123");
    metadata1.setLastObserved(120L);
    SystemMetadata metadata2 = new SystemMetadata();
    metadata2.setRunId("abc-456");
    metadata2.setLastObserved(240L);

    service.insert(OP_CONTEXT, metadata1, "urn:li:chart:1", "chartKey");
    service.insert(OP_CONTEXT, metadata1, "urn:li:chart:1", "ChartInfo");
    service.insert(OP_CONTEXT, metadata1, "urn:li:chart:1", "Ownership");
    service.insert(OP_CONTEXT, metadata2, "urn:li:chart:2", "chartKey");
    service.insert(OP_CONTEXT, metadata2, "urn:li:chart:2", "Ownership");

    List<IngestionRunSummary> runs = service.listRuns(OP_CONTEXT, 0, 20, false);
    assertEquals(runs.size(), 2);
    assertEquals(runs.get(0).getRunId(), "abc-456");
    assertEquals(runs.get(0).getRows(), Long.valueOf(2));
    assertEquals(runs.get(1).getRunId(), "abc-123");
    assertEquals(runs.get(1).getRows(), Long.valueOf(3));
  }

  @Test
  public void overwriteSameUrnAspect_upsertsCountsForListRuns() {
    SystemMetadata metadata1 = new SystemMetadata();
    metadata1.setRunId("abc-123");
    metadata1.setLastObserved(120L);
    SystemMetadata metadata2 = new SystemMetadata();
    metadata2.setRunId("abc-456");
    metadata2.setLastObserved(240L);

    service.insert(OP_CONTEXT, metadata1, "urn:li:chart:1", "chartKey");
    service.insert(OP_CONTEXT, metadata1, "urn:li:chart:1", "ChartInfo");
    service.insert(OP_CONTEXT, metadata1, "urn:li:chart:1", "Ownership");
    service.insert(OP_CONTEXT, metadata2, "urn:li:chart:1", "ChartInfo");
    service.insert(OP_CONTEXT, metadata2, "urn:li:chart:1", "Ownership");
    service.insert(OP_CONTEXT, metadata2, "urn:li:chart:2", "chartKey");
    service.insert(OP_CONTEXT, metadata2, "urn:li:chart:2", "Ownership");

    List<IngestionRunSummary> runs = service.listRuns(OP_CONTEXT, 0, 20, false);
    assertEquals(runs.size(), 2);
    assertEquals(runs.get(0).getRows(), Long.valueOf(4));
    assertEquals(runs.get(1).getRows(), Long.valueOf(1));
  }

  @Test
  public void findByRunId_returnsOnlyMatchingDocs() {
    SystemMetadata metadata1 = new SystemMetadata();
    metadata1.setRunId("abc-123");
    metadata1.setLastObserved(120L);
    SystemMetadata metadata2 = new SystemMetadata();
    metadata2.setRunId("abc-456");
    metadata2.setLastObserved(240L);

    service.insert(OP_CONTEXT, metadata1, "urn:li:chart:1", "chartKey");
    service.insert(OP_CONTEXT, metadata1, "urn:li:chart:1", "ChartInfo");
    service.insert(OP_CONTEXT, metadata1, "urn:li:chart:1", "Ownership");
    service.insert(OP_CONTEXT, metadata2, "urn:li:chart:1", "ChartInfo");
    service.insert(OP_CONTEXT, metadata2, "urn:li:chart:1", "Ownership");
    service.insert(OP_CONTEXT, metadata2, "urn:li:chart:2", "chartKey");
    service.insert(OP_CONTEXT, metadata2, "urn:li:chart:2", "Ownership");

    List<AspectRowSummary> rows = service.findByRunId(OP_CONTEXT, "abc-456", false, 0, null);
    assertEquals(rows.size(), 4);
    rows.forEach(row -> assertEquals(row.getRunId(), "abc-456"));
  }

  @Test
  public void deleteUrn_removesAllAspectRowsForUrn() {
    SystemMetadata metadata1 = new SystemMetadata();
    metadata1.setRunId("abc-123");
    metadata1.setLastObserved(120L);
    SystemMetadata metadata2 = new SystemMetadata();
    metadata2.setRunId("abc-456");
    metadata2.setLastObserved(240L);

    service.insert(OP_CONTEXT, metadata1, "urn:li:chart:1", "chartKey");
    service.insert(OP_CONTEXT, metadata1, "urn:li:chart:1", "ChartInfo");
    service.insert(OP_CONTEXT, metadata1, "urn:li:chart:1", "Ownership");
    service.insert(OP_CONTEXT, metadata2, "urn:li:chart:1", "ChartInfo");
    service.insert(OP_CONTEXT, metadata2, "urn:li:chart:1", "Ownership");
    service.insert(OP_CONTEXT, metadata2, "urn:li:chart:2", "chartKey");
    service.insert(OP_CONTEXT, metadata2, "urn:li:chart:2", "Ownership");

    service.deleteUrn(OP_CONTEXT, "urn:li:chart:1");

    List<AspectRowSummary> rows = service.findByRunId(OP_CONTEXT, "abc-456", false, 0, null);
    assertEquals(rows.size(), 2);
    rows.forEach(row -> assertEquals(row.getRunId(), "abc-456"));
  }

  @Test
  public void deleteAspect_removesSingleRow() {
    SystemMetadata m = new SystemMetadata();
    m.setRunId("r1");
    m.setLastObserved(10L);
    service.insert(OP_CONTEXT, m, "urn:li:chart:99", "a");
    service.insert(OP_CONTEXT, m, "urn:li:chart:99", "b");

    assertEquals(service.findByUrn(OP_CONTEXT, "urn:li:chart:99", false, 0, null).size(), 2);
    service.deleteAspect(OP_CONTEXT, "urn:li:chart:99", "a");
    List<AspectRowSummary> rows = service.findByUrn(OP_CONTEXT, "urn:li:chart:99", false, 0, null);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).getAspectName(), "b");
  }

  @Test
  public void clear_truncatesBackingTable() {
    SystemMetadata m = new SystemMetadata();
    m.setRunId("r");
    m.setLastObserved(1L);
    service.insert(
        OP_CONTEXT, m, "urn:li:dataset:(urn:li:dataPlatform:hive,t,PROD)", "datasetProperties");

    assertTrue(service.listRuns(OP_CONTEXT, 0, 10, false).size() >= 1);
    service.clear(OP_CONTEXT);
    assertEquals(service.listRuns(OP_CONTEXT, 0, 10, false).size(), 0);
  }

  @Test
  public void clearThenInsert_stillWorks() {
    service.clear(OP_CONTEXT);

    SystemMetadata m = new SystemMetadata();
    m.setRunId("after-clear");
    m.setLastObserved(99L);
    service.insert(OP_CONTEXT, m, "urn:li:chart:7", "ChartInfo");

    assertEquals(service.findByUrn(OP_CONTEXT, "urn:li:chart:7", false, 0, null).size(), 1);
  }

  @Test
  public void setDocStatus_hidesRowUntilIncludeSoftDeleted() {
    SystemMetadata m = new SystemMetadata();
    m.setRunId("r-status");
    m.setLastObserved(5L);
    service.insert(OP_CONTEXT, m, "urn:li:chart:status", "ChartInfo");

    service.setDocStatus(OP_CONTEXT, "urn:li:chart:status", true);
    assertEquals(service.findByUrn(OP_CONTEXT, "urn:li:chart:status", false, 0, null).size(), 0);
    assertEquals(service.findByUrn(OP_CONTEXT, "urn:li:chart:status", true, 0, null).size(), 1);
  }

  @Test
  public void raw_returnsDocumentMapForUrnAspect() {
    SystemMetadata m = new SystemMetadata();
    m.setRunId("raw-run");
    m.setLastObserved(11L);
    String urn = "urn:li:chart:raw";
    service.insert(OP_CONTEXT, m, urn, "ChartInfo");

    Map<Urn, Map<String, Map<String, Object>>> docs =
        service.raw(OP_CONTEXT, Map.of(urn, Set.of("ChartInfo")));
    assertEquals(docs.size(), 1);
    assertTrue(docs.get(UrnUtils.getUrn(urn)).containsKey("ChartInfo"));
    assertEquals(docs.get(UrnUtils.getUrn(urn)).get("ChartInfo").get("runId"), "raw-run");
  }

  @Test
  public void countByKeyAspect_countsActiveAndSoftDeleted() {
    SystemMetadata m = new SystemMetadata();
    m.setRunId("count-run");
    m.setLastObserved(3L);
    service.insert(OP_CONTEXT, m, "urn:li:chart:c1", "chartKey");
    service.insert(OP_CONTEXT, m, "urn:li:chart:c2", "chartKey");
    service.setDocStatus(OP_CONTEXT, "urn:li:chart:c2", true);

    KeyAspectCount counts = service.countByKeyAspect(OP_CONTEXT, "chartKey");
    assertEquals(counts.getActiveCount(), 1L);
    assertEquals(counts.getSoftDeletedCount(), 1L);

    Map<String, KeyAspectCount> batch =
        service.countByKeyAspects(OP_CONTEXT, List.of("chartKey", "datasetKey"));
    assertEquals(batch.get("chartKey").getActiveCount(), 1L);
    assertEquals(batch.get("chartKey").getSoftDeletedCount(), 1L);
    assertEquals(batch.get("datasetKey").getActiveCount(), 0L);
    assertEquals(batch.get("datasetKey").getSoftDeletedCount(), 0L);
  }

  @Test
  public void findAspectsByUrn_filtersAspectList() {
    SystemMetadata m = new SystemMetadata();
    m.setRunId("aspects");
    m.setLastObserved(4L);
    Urn urn = UrnUtils.getUrn("urn:li:chart:fa");
    service.insert(OP_CONTEXT, m, urn.toString(), "chartKey");
    service.insert(OP_CONTEXT, m, urn.toString(), "ChartInfo");
    service.insert(OP_CONTEXT, m, urn.toString(), "Ownership");

    List<AspectRowSummary> rows =
        service.findAspectsByUrn(OP_CONTEXT, urn, List.of("ChartInfo", "Ownership"), false);
    assertEquals(rows.size(), 2);
  }

  @Test
  public void findAspectsByUrn_emptyAspectList_returnsEmptyWithoutQuery() {
    SystemMetadata m = new SystemMetadata();
    m.setRunId("empty-in");
    m.setLastObserved(4L);
    Urn urn = UrnUtils.getUrn("urn:li:chart:empty-in");
    service.insert(OP_CONTEXT, m, urn.toString(), "ChartInfo");

    assertEquals(service.findAspectsByUrn(OP_CONTEXT, urn, List.of(), false).size(), 0);
    assertEquals(service.findByUrn(OP_CONTEXT, urn.toString(), false, 0, null).size(), 1);
  }

  @Test
  public void findByParams_unknownKeyThrows() {
    expectThrows(
        IllegalArgumentException.class,
        () ->
            service.findByParams(OP_CONTEXT, Map.of("notASystemMetadataField", "x"), false, 0, 10));
  }

  @Test
  public void setDocStatus_afterDeleteUrn_doesNotResurrectRow() {
    SystemMetadata m = new SystemMetadata();
    m.setRunId("resurrect");
    m.setLastObserved(8L);
    String urn = "urn:li:chart:no-resurrect";
    service.insert(OP_CONTEXT, m, urn, "ChartInfo");
    service.deleteUrn(OP_CONTEXT, urn);
    assertEquals(service.findByUrn(OP_CONTEXT, urn, true, 0, null).size(), 0);

    service.setDocStatus(OP_CONTEXT, urn, true);
    assertEquals(service.findByUrn(OP_CONTEXT, urn, true, 0, null).size(), 0);
  }

  @Test
  public void postgresScrollClient_pagesWithKeysetCursor() {
    SystemMetadata m = new SystemMetadata();
    m.setRunId("scroll");
    m.setLastObserved(9L);
    service.insert(OP_CONTEXT, m, "urn:li:chart:scroll-a", "ChartInfo");
    service.insert(OP_CONTEXT, m, "urn:li:chart:scroll-b", "ChartInfo");
    service.insert(OP_CONTEXT, m, "urn:li:chart:scroll-c", "ChartInfo");

    PostgresSystemMetadataScrollClient scrollClient =
        new PostgresSystemMetadataScrollClient(database, props);
    SystemMetadataScrollResult first =
        scrollClient.scrollUrns(
            OP_CONTEXT,
            SystemMetadataScrollRequest.builder().entityType("chart").batchSize(2).build());
    assertEquals(first.getUrns().size(), 2);
    assertNotNull(first.getNextScrollId());

    SystemMetadataScrollResult second =
        scrollClient.scrollUrns(
            OP_CONTEXT,
            SystemMetadataScrollRequest.builder()
                .entityType("chart")
                .batchSize(2)
                .scrollId(first.getNextScrollId())
                .build());
    assertEquals(second.getUrns().size(), 1);
    assertNull(second.getNextScrollId());
  }
}
