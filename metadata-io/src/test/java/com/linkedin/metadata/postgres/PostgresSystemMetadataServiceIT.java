package com.linkedin.metadata.postgres;

import static io.datahubproject.test.search.SearchTestUtils.TEST_SYSTEM_METADATA_SERVICE_CONFIG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.PostgresTestUtils;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.IngestionRunSummary;
import com.linkedin.metadata.systemmetadata.PostgresSystemMetadataService;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import java.util.List;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Integration tests for {@link PostgresSystemMetadataService} on PostgreSQL (SqlSetup-aligned DDL
 * via {@link PostgresTestUtils#applyPgSystemMetadataTables}). Parity targets for aggregation and
 * query behavior are drawn from {@link
 * com.linkedin.metadata.systemmetadata.SystemMetadataServiceTestBase}.
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
    props = PostgresTestUtils.testPgSystemMetadataProperties(ns.getSchema());
    PostgreSQLContainer<?> postgres = PostgresTestUtils.startPostgres();
    database =
        PostgresTestUtils.createEbeanDatabase(
            postgres, PostgresTestUtils.uniqueServerName("pg_sysmeta_it"));
    try (var c = database.dataSource().getConnection()) {
      c.setAutoCommit(false);
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

  /**
   * Pre-created indexes + DDL from SqlSetup should allow clear/insert without duplicate index
   * errors after {@link PostgresSystemMetadataService#clear(OperationContext)}.
   */
  @Test
  public void clearThenInsert_stillWorks() {
    service.clear(OP_CONTEXT);

    SystemMetadata m = new SystemMetadata();
    m.setRunId("after-clear");
    m.setLastObserved(99L);
    service.insert(OP_CONTEXT, m, "urn:li:chart:7", "ChartInfo");

    assertEquals(service.findByUrn(OP_CONTEXT, "urn:li:chart:7", false, 0, null).size(), 1);
  }
}
