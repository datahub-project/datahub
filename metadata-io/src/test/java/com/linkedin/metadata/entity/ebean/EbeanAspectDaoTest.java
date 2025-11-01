package com.linkedin.metadata.entity.ebean;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.entity.TransactionResult;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.ebean.Database;
import io.ebean.test.LoggedSql;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EbeanAspectDaoTest {

  private EbeanAspectDao testDao;

  @BeforeMethod
  public void setupTest() {
    Database server = EbeanTestUtils.createTestServer(EbeanAspectDaoTest.class.getSimpleName());
    testDao = new EbeanAspectDao(server, EbeanConfiguration.testDefault, mock(MetricUtils.class));
  }

  @Test
  public void testGetNextVersionForUpdate() {
    LoggedSql.start();

    testDao.runInTransactionWithRetryUnlocked(
        (txContext) -> {
          testDao.getNextVersions(
              Map.of("urn:li:corpuser:testGetNextVersionForUpdate", Set.of("status")));
          return TransactionResult.commit("");
        },
        mock(AspectsBatch.class),
        0);

    // Get the captured SQL statements
    List<String> sql =
        LoggedSql.stop().stream()
            .filter(str -> str.contains("testGetNextVersionForUpdate"))
            .toList();
    assertEquals(sql.size(), 2, String.format("Found: %s", sql));
    assertTrue(
        sql.get(0).contains("for update;"), String.format("Did not find `for update` in %s ", sql));
  }

  @Test
  public void testGetLatestAspectsForUpdate() throws JsonProcessingException {
    LoggedSql.start();

    testDao.runInTransactionWithRetryUnlocked(
        (txContext) -> {
          testDao.getLatestAspects(
              mock(OperationContext.class),
              Map.of("urn:li:corpuser:testGetLatestAspectsForUpdate", Set.of("status")),
              true);
          return TransactionResult.commit("");
        },
        mock(AspectsBatch.class),
        0);

    // Get the captured SQL statements
    List<String> sql =
        LoggedSql.stop().stream()
            .filter(str -> str.contains("testGetLatestAspectsForUpdate"))
            .toList();
    assertEquals(
        sql.size(), 1, String.format("Found: %s", new ObjectMapper().writeValueAsString(sql)));
    assertTrue(
        sql.get(0).contains("for update;"), String.format("Did not find `for update` in %s ", sql));
  }

  @Test
  public void testbatchGetForUpdate() throws JsonProcessingException {
    LoggedSql.start();

    testDao.runInTransactionWithRetryUnlocked(
        (txContext) -> {
          testDao.batchGet(
              Set.of(
                  new EntityAspectIdentifier(
                      "urn:li:corpuser:testbatchGetForUpdate1",
                      DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                      ASPECT_LATEST_VERSION),
                  new EntityAspectIdentifier(
                      "urn:li:corpuser:testbatchGetForUpdate2",
                      DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                      ASPECT_LATEST_VERSION)),
              true);
          return TransactionResult.commit("");
        },
        mock(AspectsBatch.class),
        0);

    // Get the captured SQL statements
    List<String> sql =
        LoggedSql.stop().stream()
            .filter(
                str ->
                    str.contains("testbatchGetForUpdate1")
                        && str.contains("testbatchGetForUpdate2"))
            .toList();
    assertEquals(
        sql.size(), 1, String.format("Found: %s", new ObjectMapper().writeValueAsString(sql)));
    assertTrue(
        sql.get(0).contains("FOR UPDATE;"), String.format("Did not find `for update` in %s ", sql));
  }

  @Test
  public void testStreamAspectBatchesWithIsolationLevel() {
    // Test the new overloaded method with isolation level parameter
    var args = new com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs();
    args.limit = 10;

    // Test with READ_UNCOMMITTED isolation level
    var stream =
        testDao.streamAspectBatches(args, io.ebean.annotation.TxIsolation.READ_UNCOMMITTED);
    assertTrue(stream != null);

    // Test with null isolation level (should use default)
    var defaultStream = testDao.streamAspectBatches(args, null);
    assertTrue(defaultStream != null);
  }

  @Test
  public void testStreamAspectBatchesDefault() {
    // Test the original method still works
    var args = new com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs();
    args.limit = 5;

    var stream = testDao.streamAspectBatches(args);
    assertTrue(stream != null);
  }

  private void insertTestData() {
    // Insert test data with different URN patterns and aspects
    insertAspect("urn:li:test:test1", "testAspect1", 0, "test1");
    insertAspect("urn:li:test:test2", "testAspect1", 0, "test2");
    insertAspect("urn:li:test:test3", "testAspect2", 0, "test3");
    insertAspect("urn:li:other:test4", "testAspect1", 0, "test4");
    insertAspect("urn:li:other:test5", "testAspect2", 0, "test5");
  }

  private void insertAspect(String urn, String aspect, long version, String metadata) {
    EbeanAspectV2 aspectRecord = new EbeanAspectV2();
    aspectRecord.setKey(new EbeanAspectV2.PrimaryKey(urn, aspect, version));
    aspectRecord.setMetadata(metadata);
    aspectRecord.setCreatedBy("test");
    aspectRecord.setCreatedFor(null);
    aspectRecord.setCreatedOn(new Timestamp(System.currentTimeMillis()));
    aspectRecord.setSystemMetadata(null);
    testDao.getServer().save(aspectRecord);
  }

  @Test
  public void testCountAspect() {
    // Setup test data
    insertTestData();

    // Test case 1: No filter - should return count of all aspects
    var args1 = new com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs();
    int count1 = testDao.countAspect(args1);
    assertEquals(count1, 5, "Should return count of all aspects");

    // Test case 2: urnLike filter - should return count of aspects matching the URN pattern
    var args2 = new com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs();
    args2.urnLike = "%:test:%";
    int count2 = testDao.countAspect(args2);
    assertEquals(count2, 3, "Should return count of aspects matching URN pattern '%:test:%'");

    // Test case 3: urnLike + aspect filter - should return count of matching aspects
    var args3 = new com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs();
    args3.urnLike = "%:test:%";
    args3.aspectName = "testAspect1";
    int count3 = testDao.countAspect(args3);
    assertEquals(
        count3, 2, "Should return count of aspects matching both URN pattern and aspect name");
  }
}
