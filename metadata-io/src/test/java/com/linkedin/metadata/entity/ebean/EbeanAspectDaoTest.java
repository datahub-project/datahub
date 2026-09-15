package com.linkedin.metadata.entity.ebean;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.util.exception.DatabaseTransactionConflictException;
import com.datahub.util.exception.RetryLimitReached;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.entity.AspectWriteDisabledException;
import com.linkedin.metadata.entity.ConditionalSaveResult;
import com.linkedin.metadata.entity.ConditionalWriteOutcome;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.entity.OptimisticLockConflictException;
import com.linkedin.metadata.entity.TransactionResult;
import com.linkedin.metadata.entity.storage.PrimaryStorageResolver;
import com.linkedin.metadata.entity.storage.PrimaryStorageTestUtils;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import io.ebean.DuplicateKeyException;
import io.ebean.test.LoggedSql;
import jakarta.persistence.PersistenceException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class EbeanAspectDaoTest {

  private EbeanAspectDao testDao;
  private OperationContext opContext = TestOperationContexts.systemContextNoValidate();
  private Database server;

  @BeforeMethod
  public void setupTest() {
    server = EbeanTestUtils.createTestServer(EbeanAspectDaoTest.class.getSimpleName());
    testDao =
        new EbeanAspectDao(
            PrimaryStorageTestUtils.ebeanResolver(server),
            EbeanConfiguration.testDefault,
            mock(MetricUtils.class),
            List.of(),
            null,
            new PlainAspectTableResolver(),
            new PassThroughScopedTransactionFactory(server));
  }

  /**
   * Snapshot {@link LoggedSql#stop()} without streaming the live buffer. Under {@code
   * parallel="classes"}, other suites can mutate the process-global LoggedSql list and cause {@link
   * java.util.ConcurrentModificationException} on stream/spliterator.
   */
  private static List<String> snapshotStoppedSql() {
    return Arrays.asList(LoggedSql.stop().toArray(new String[0]));
  }

  @AfterMethod
  public void cleanup() {
    // Shutdown Database instance to prevent thread pool and connection leaks
    EbeanTestUtils.shutdownDatabase(server);
  }

  @Test
  public void embeddedIdOrderByResolvesToColumns() {
    LoggedSql.start();
    server
        .find(EbeanAspectV2.class)
        .where()
        .idIn(
            List.of(
                new EbeanAspectV2.PrimaryKey(
                    "urn:li:corpuser:orderby", STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION)))
        .orderBy(EbeanAspectV2.KEY_ORDER_BY_PROPERTY_PATH)
        .findList();
    final String sql = String.join(" ", snapshotStoppedSql()).toLowerCase();
    assertTrue(sql.contains("order by"), "embedded-id orderBy must emit a SQL ORDER BY clause");
    assertFalse(sql.contains("key.urn"), "embedded-id path must resolve to columns, not 'key.urn'");
  }

  @DataProvider(name = "writabilityConfig")
  public Object[][] writabilityConfigProvider() {
    return new Object[][] {
      {true, "Writable"}, // canWrite = true, description
      {false, "ReadOnly"} // canWrite = false, description
    };
  }

  @Test
  public void testGetNextVersionsThrowsIllegalStateWhenDbKeyNotInRequest() {
    Map<String, Map<String, Long>> result = new HashMap<>();
    result.put("urn:li:corpuser:requested", new HashMap<>(Map.of("status", 0L)));
    List<EbeanAspectV2.PrimaryKey> dbResults =
        List.of(
            new EbeanAspectV2.PrimaryKey("urn:li:corpuser:from-db-not-in-request", "status", 0));

    try {
      EbeanAspectDao.mergeNextVersionsFromDb(result, dbResults);
      throw new AssertionError("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertTrue(
          e.getMessage().contains("urn:li:corpuser:from-db-not-in-request"),
          "Message should include failing URN");
      assertTrue(e.getMessage().contains("status"), "Message should include failing aspect");
      assertTrue(
          e.getMessage().contains("utf8mb4_bin"), "Message should hint at charset/collation fix");
      assertNotNull(e.getCause(), "Cause should be set");
      assertTrue(e.getCause() instanceof NullPointerException);
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testGetNextVersionForUpdate(boolean canWrite, String description) {
    testDao.setWritable(canWrite);
    LoggedSql.start();

    testDao.runInTransactionWithRetryUnlocked(
        opContext,
        (txContext) -> {
          testDao.getNextVersions(
              opContext,
              Map.of("urn:li:corpuser:testGetNextVersionForUpdate", Set.of("status")),
              true);
          return TransactionResult.commit("");
        },
        mock(AspectsBatch.class),
        0);

    // Get the captured SQL statements
    List<String> sql =
        snapshotStoppedSql().stream()
            .filter(str -> str.contains("testGetNextVersionForUpdate"))
            .toList();
    if (canWrite) {
      assertEquals(sql.size(), 2, String.format("Found: %s", sql));
      assertTrue(
          sql.get(0).contains("for update;"),
          String.format("Did not find `for update` in %s ", sql));
    } else {
      assertEquals(sql.size(), 1, String.format("Found: %s", sql));
      assertFalse(
          sql.get(0).contains("for update;"), String.format("Found `for update` in %s ", sql));
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testGetLatestAspectsForUpdate(boolean canWrite, String description)
      throws JsonProcessingException {
    testDao.setWritable(canWrite);
    LoggedSql.start();

    testDao.runInTransactionWithRetryUnlocked(
        opContext,
        (txContext) -> {
          testDao.getLatestAspects(
              opContext,
              Map.of("urn:li:corpuser:testGetLatestAspectsForUpdate", Set.of("status")),
              true);
          return TransactionResult.commit("");
        },
        mock(AspectsBatch.class),
        0);

    // Get the captured SQL statements
    List<String> sql =
        snapshotStoppedSql().stream()
            .filter(str -> str.contains("testGetLatestAspectsForUpdate"))
            .toList();
    assertEquals(
        sql.size(), 1, String.format("Found: %s", new ObjectMapper().writeValueAsString(sql)));
    if (canWrite) {
      assertTrue(
          sql.get(0).contains("FOR UPDATE;"),
          String.format("Did not find `for update` in %s ", sql));
    } else {
      assertFalse(
          sql.get(0).contains("for update;"), String.format("Found `for update` in %s ", sql));
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testbatchGetForUpdate(boolean canWrite, String description)
      throws JsonProcessingException {
    testDao.setWritable(canWrite);
    LoggedSql.start();

    testDao.runInTransactionWithRetryUnlocked(
        opContext,
        (txContext) -> {
          testDao.batchGet(
              opContext,
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
        snapshotStoppedSql().stream()
            .filter(
                str ->
                    str.contains("testbatchGetForUpdate1")
                        && str.contains("testbatchGetForUpdate2"))
            .toList();
    assertEquals(
        sql.size(), 1, String.format("Found: %s", new ObjectMapper().writeValueAsString(sql)));
    if (canWrite) {
      assertTrue(
          sql.get(0).contains("FOR UPDATE;"),
          String.format("Did not find `for update` in %s ", sql));
    } else {
      assertFalse(
          sql.get(0).contains("FOR UPDATE;"), String.format("Found `for update` in %s ", sql));
    }
  }

  @Test
  public void testGetLatestAspectsPassesSortedKeysToQuery() {
    // Regression guard (#17206): batchGet must deliver keys in (urn, aspect, version) order
    // regardless of input order, so concurrent FOR UPDATE writers avoid lock-order deadlocks.
    Map<String, Set<String>> urnAspects = new HashMap<>();
    urnAspects.put("urn:li:corpuser:c", Set.of(STATUS_ASPECT_NAME));
    urnAspects.put("urn:li:corpuser:a", Set.of(STATUS_ASPECT_NAME, "ownership"));
    urnAspects.put("urn:li:corpuser:b", Set.of(STATUS_ASPECT_NAME));

    List<String> actual =
        captureKeysHandedToQuery(dao -> dao.getLatestAspects(opContext, urnAspects, true));

    assertEquals(
        actual,
        List.of(
            "urn:li:corpuser:a|ownership|0",
            "urn:li:corpuser:a|status|0",
            "urn:li:corpuser:b|status|0",
            "urn:li:corpuser:c|status|0"));
  }

  @Test
  public void testBatchGetPassesSortedKeysToQuery() {
    // The public batchGet(Set, forUpdate) path takes an unordered Set and must also lock in sorted
    // order (it was never sorted, even before #17206).
    Set<EntityAspectIdentifier> ids =
        Set.of(
            new EntityAspectIdentifier(
                "urn:li:corpuser:c", STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION),
            new EntityAspectIdentifier(
                "urn:li:corpuser:a", STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION),
            new EntityAspectIdentifier(
                "urn:li:corpuser:b", STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION));

    List<String> actual = captureKeysHandedToQuery(dao -> dao.batchGet(opContext, ids, true));

    assertEquals(
        actual,
        List.of(
            "urn:li:corpuser:a|status|0",
            "urn:li:corpuser:b|status|0",
            "urn:li:corpuser:c|status|0"));
  }

  /**
   * Spies the DAO, stubs the query builder so no real DB round-trip happens, runs the given locking
   * read, and returns the (urn, aspect, version) tuples in the order they reached the query
   * builder.
   */
  private List<String> captureKeysHandedToQuery(Consumer<EbeanAspectDao> lockingRead) {
    EbeanAspectDao spyDao = spy(testDao);
    // Mutable list: batchGet may addAll into the first page's result during pagination.
    doReturn(new ArrayList<EbeanAspectV2>())
        .when(spyDao)
        .batchGetSelectString(any(), anyList(), anyInt(), anyInt(), anyBoolean(), anyBoolean());

    lockingRead.accept(spyDao);

    ArgumentCaptor<List<EbeanAspectV2.PrimaryKey>> captor = ArgumentCaptor.captor();
    verify(spyDao)
        .batchGetSelectString(
            any(), captor.capture(), anyInt(), anyInt(), anyBoolean(), anyBoolean());
    return captor.getValue().stream().map(EbeanAspectDaoTest::keyTuple).toList();
  }

  private static String keyTuple(EbeanAspectV2.PrimaryKey key) {
    return key.getUrn() + "|" + key.getAspect() + "|" + key.getVersion();
  }

  @Test
  public void testStreamAspectBatchesWithIsolationLevel() {
    // 5 latest-version rows; the consumer flattens the batches and counts rows, so we assert the
    // stream actually yielded every seeded row (not just that the call returned).
    insertTestData();
    var args = new com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs();
    args.limit = 10;

    long uncommittedRows =
        testDao.streamAspectBatches(
            opContext,
            args,
            io.ebean.annotation.TxIsolation.READ_UNCOMMITTED,
            stream -> streamedRowCount(stream, args));
    assertEquals(uncommittedRows, 5L, "READ_UNCOMMITTED stream should yield all 5 latest rows");

    // null isolation -> database default
    long defaultRows =
        testDao.streamAspectBatches(
            opContext, args, null, stream -> streamedRowCount(stream, args));
    assertEquals(defaultRows, 5L, "default-isolation stream should yield all 5 latest rows");
  }

  @Test
  public void testStreamAspectBatchesDefault() {
    insertTestData();
    var args = new com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs();
    args.limit = 5;

    long rows =
        testDao.streamAspectBatches(opContext, args, stream -> streamedRowCount(stream, args));
    assertEquals(rows, 5L, "stream should yield all 5 latest rows within the limit");
  }

  private static long streamedRowCount(
      PartitionedStream<EbeanAspectV2> stream,
      com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs args) {
    return stream
        .partition(args.batchSize > 0 ? args.batchSize : 100)
        .flatMap(java.util.function.Function.identity())
        .count();
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
    insertAspect(urn, aspect, version, metadata, System.currentTimeMillis());
  }

  private void insertAspect(
      String urn, String aspect, long version, String metadata, long createdOnMs) {
    EbeanAspectV2 aspectRecord = new EbeanAspectV2();
    aspectRecord.setKey(new EbeanAspectV2.PrimaryKey(urn, aspect, version));
    aspectRecord.setMetadata(metadata);
    aspectRecord.setCreatedBy("urn:li:corpuser:tester");
    aspectRecord.setCreatedFor(null);
    aspectRecord.setCreatedOn(new Timestamp(createdOnMs));
    aspectRecord.setSystemMetadata(null);
    testDao.getServer().save(aspectRecord);
  }

  @Test
  public void testCountAspect() {
    // Setup test data
    insertTestData();

    // Test case 1: No filter - should return count of all aspects
    var args1 = new com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs();
    int count1 = testDao.countAspect(opContext, args1);
    assertEquals(count1, 5, "Should return count of all aspects");

    // Test case 2: urnLike filter - should return count of aspects matching the URN pattern
    var args2 = new com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs();
    args2.urnLike = "%:test:%";
    int count2 = testDao.countAspect(opContext, args2);
    assertEquals(count2, 3, "Should return count of aspects matching URN pattern '%:test:%'");

    // Test case 3: urnLike + aspect filter - should return count of matching aspects
    var args3 = new com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs();
    args3.urnLike = "%:test:%";
    args3.aspectName = "testAspect1";
    int count3 = testDao.countAspect(opContext, args3);
    assertEquals(
        count3, 2, "Should return count of aspects matching both URN pattern and aspect name");
  }

  @Test
  public void testCountAspectWithPitEpochMsBounds() {
    long nowMs = System.currentTimeMillis();
    long oneHourAgoMs = nowMs - 3_600_000L;
    long twoHoursAgoMs = nowMs - 7_200_000L;

    insertAspect("urn:li:test:recent", "testAspect1", 0, "recent", nowMs);
    insertAspect("urn:li:test:middle", "testAspect1", 0, "middle", oneHourAgoMs);
    insertAspect("urn:li:test:old", "testAspect1", 0, "old", twoHoursAgoMs);

    // Regression test: only gePitEpochMs set (lePitEpochMs left at its 0 default, as
    // datahub-upgrade's RestoreIndices does) must not silently match zero rows.
    var geOnlyArgs = new com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs();
    geOnlyArgs.gePitEpochMs = oneHourAgoMs;
    assertEquals(
        testDao.countAspect(opContext, geOnlyArgs),
        2,
        "Only gePitEpochMs set: should match rows created at or after the lower bound");

    var leOnlyArgs = new com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs();
    leOnlyArgs.lePitEpochMs = oneHourAgoMs;
    assertEquals(
        testDao.countAspect(opContext, leOnlyArgs),
        2,
        "Only lePitEpochMs set: should match rows created at or before the upper bound");

    var bothArgs = new com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs();
    bothArgs.gePitEpochMs = oneHourAgoMs;
    bothArgs.lePitEpochMs = oneHourAgoMs;
    assertEquals(
        testDao.countAspect(opContext, bothArgs),
        1,
        "Both bounds set: should match only rows created within the range");
  }

  @Test(dataProvider = "writabilityConfig")
  public void testUpdateAspectWithWritability(boolean canWrite, String description) {
    // Set writability
    testDao.setWritable(canWrite);

    SystemAspect systemAspect =
        new EbeanSystemAspect(
            null,
            UrnUtils.getUrn("urn:li:corpuser:testUpdateAspect" + description),
            STATUS_ASPECT_NAME,
            opContext.getEntityRegistry().getEntitySpec(CORP_USER_ENTITY_NAME),
            opContext.getEntityRegistry().getAspectSpecs().get(STATUS_ASPECT_NAME),
            new Status(),
            new SystemMetadata(),
            AuditStampUtils.createDefaultAuditStamp(),
            null, // systemAspectValidators
            null, // validationConfig
            null); // operationContext

    // Try to update aspect
    Optional<com.linkedin.metadata.aspect.EntityAspect> result =
        testDao.insertAspect(opContext, null, systemAspect, ASPECT_LATEST_VERSION);

    if (canWrite) {
      // When writable, operation should succeed
      assertTrue(result.isPresent(), "Update should succeed when writable");
    } else {
      // When not writable, operation should return empty
      assertFalse(result.isPresent(), "Update should return empty when not writable");
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testInsertAspectWithWritability(boolean canWrite, String description) {
    // Set writability
    testDao.setWritable(canWrite);

    // Create a mock SystemAspect
    SystemAspect mockAspect = mock(SystemAspect.class, description + "Aspect");
    com.linkedin.metadata.aspect.EntityAspect mockEntityAspect =
        mock(com.linkedin.metadata.aspect.EntityAspect.class);

    when(mockAspect.withVersion(anyLong())).thenReturn(mockEntityAspect);
    when(mockEntityAspect.getUrn()).thenReturn("urn:li:corpuser:testInsertAspect" + description);
    when(mockEntityAspect.getAspect()).thenReturn("status");
    when(mockEntityAspect.getVersion()).thenReturn(1L);
    when(mockEntityAspect.getMetadata()).thenReturn("{}");
    when(mockEntityAspect.getCreatedBy()).thenReturn("urn:li:corpuser:test");
    when(mockEntityAspect.getCreatedOn()).thenReturn(new Timestamp(System.currentTimeMillis()));

    // Try to insert aspect
    Optional<com.linkedin.metadata.aspect.EntityAspect> result =
        testDao.insertAspect(opContext, null, mockAspect, 1L);

    if (canWrite) {
      // When writable, operation should succeed
      assertTrue(result.isPresent(), "Insert should succeed when writable");
    } else {
      // When not writable, operation should return empty
      assertFalse(result.isPresent(), "Insert should return empty when not writable");
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testDeleteAspectWithWritability(boolean canWrite, String description) {
    // Set writability
    testDao.setWritable(canWrite);

    // First, insert an aspect to delete (when writable)
    String urnString = "urn:li:corpuser:testDeleteAspect" + description;
    String aspectName = "status";

    if (canWrite) {
      // Only insert if writable, so we have something to delete
      insertAspect(urnString, aspectName, ASPECT_LATEST_VERSION, "test-metadata");

      // Verify it exists
      com.linkedin.metadata.aspect.EntityAspect beforeDelete =
          testDao.getAspect(opContext, urnString, aspectName, ASPECT_LATEST_VERSION);
      assertNotNull(beforeDelete, "Aspect should exist before delete");

      // Delete the aspect
      Urn urn = UrnUtils.getUrn(urnString);
      testDao.deleteAspect(opContext, urn, aspectName, ASPECT_LATEST_VERSION);

      // Verify it's deleted
      com.linkedin.metadata.aspect.EntityAspect afterDelete =
          testDao.getAspect(opContext, urnString, aspectName, ASPECT_LATEST_VERSION);
      assertNull(afterDelete, "Aspect should be deleted when writable");
    } else {
      // When not writable, delete should be a no-op
      Urn urn = UrnUtils.getUrn(urnString);
      testDao.deleteAspect(opContext, urn, aspectName, ASPECT_LATEST_VERSION);
      // No exception should be thrown, operation just returns silently
      assertTrue(true, "Delete should complete without error when not writable");
    }
  }

  @Test(dataProvider = "writabilityConfig")
  public void testDeleteUrnWithWritability(boolean canWrite, String description) throws Exception {
    // Set writability
    testDao.setWritable(canWrite);

    String urnString = "urn:li:corpuser:testDeleteUrn" + description;

    if (canWrite) {
      // Insert multiple aspects for the same URN
      insertAspect(urnString, "corpUserInfo", ASPECT_LATEST_VERSION, "test-metadata-1");
      insertAspect(urnString, "status", ASPECT_LATEST_VERSION, "test-metadata-2");
      insertAspect(urnString, "corpUserKey", ASPECT_LATEST_VERSION, "test-metadata-key");

      // Verify aspects exist
      com.linkedin.metadata.aspect.EntityAspect aspect1 =
          testDao.getAspect(opContext, urnString, "corpUserInfo", ASPECT_LATEST_VERSION);
      com.linkedin.metadata.aspect.EntityAspect aspect2 =
          testDao.getAspect(opContext, urnString, "status", ASPECT_LATEST_VERSION);
      com.linkedin.metadata.aspect.EntityAspect keyAspect =
          testDao.getAspect(opContext, urnString, "corpUserKey", ASPECT_LATEST_VERSION);
      assertTrue(
          aspect1 != null && aspect2 != null && keyAspect != null,
          "All aspects should exist before deletion");

      // Delete the URN
      OperationContext mockOpContext = mock(OperationContext.class);
      when(mockOpContext.getKeyAspectName(any())).thenReturn("corpUserKey");

      int deletedCount = testDao.deleteUrn(mockOpContext, null, urnString);

      // Verify deletion count
      assertTrue(deletedCount > 0, "Should delete aspects when writable");

      // Verify aspects are deleted
      com.linkedin.metadata.aspect.EntityAspect afterAspect1 =
          testDao.getAspect(opContext, urnString, "corpUserInfo", ASPECT_LATEST_VERSION);
      com.linkedin.metadata.aspect.EntityAspect afterAspect2 =
          testDao.getAspect(opContext, urnString, "status", ASPECT_LATEST_VERSION);
      com.linkedin.metadata.aspect.EntityAspect afterKeyAspect =
          testDao.getAspect(opContext, urnString, "corpUserKey", ASPECT_LATEST_VERSION);
      assertTrue(
          afterAspect1 == null && afterAspect2 == null && afterKeyAspect == null,
          "All aspects should be deleted");
    } else {
      // When not writable, delete should return 0
      OperationContext mockOpContext = mock(OperationContext.class);
      when(mockOpContext.getKeyAspectName(any())).thenReturn("corpUserKey");

      int deletedCount = testDao.deleteUrn(mockOpContext, null, urnString);

      assertEquals(deletedCount, 0, "Should return 0 when not writable");
    }
  }

  @Test
  public void testSetWritableToggle() {
    // Test that we can toggle writability
    testDao.setWritable(true);

    // Insert should work
    String urnString = "urn:li:corpuser:testToggle";
    insertAspect(urnString, "status", ASPECT_LATEST_VERSION, "test-metadata");
    com.linkedin.metadata.aspect.EntityAspect aspect =
        testDao.getAspect(opContext, urnString, "status", ASPECT_LATEST_VERSION);
    assertTrue(aspect != null, "Insert should work when writable");

    // Now set to read-only
    testDao.setWritable(false);

    // Try to insert another aspect
    SystemAspect mockAspect = mock(SystemAspect.class);
    com.linkedin.metadata.aspect.EntityAspect mockEntityAspect =
        mock(com.linkedin.metadata.aspect.EntityAspect.class);
    when(mockAspect.withVersion(anyLong())).thenReturn(mockEntityAspect);
    when(mockEntityAspect.getUrn()).thenReturn("urn:li:corpuser:testToggle2");
    when(mockEntityAspect.getAspect()).thenReturn("status");
    when(mockEntityAspect.getVersion()).thenReturn(ASPECT_LATEST_VERSION);

    Optional<com.linkedin.metadata.aspect.EntityAspect> result =
        testDao.insertAspect(opContext, null, mockAspect, ASPECT_LATEST_VERSION);
    assertFalse(result.isPresent(), "Insert should fail when not writable");

    // Set back to writable
    testDao.setWritable(true);

    // Insert should work again
    insertAspect("urn:li:corpuser:testToggle3", "status", ASPECT_LATEST_VERSION, "test-metadata");
    com.linkedin.metadata.aspect.EntityAspect aspect3 =
        testDao.getAspect(
            opContext, "urn:li:corpuser:testToggle3", "status", ASPECT_LATEST_VERSION);
    assertTrue(aspect3 != null, "Insert should work again after re-enabling write");
  }

  @Test
  public void testReadOperationsWorkWhenNotWritable() {
    // First, insert data while writable
    testDao.setWritable(true);
    String urnString = "urn:li:corpuser:testReadOnly";
    insertAspect(urnString, "status", ASPECT_LATEST_VERSION, "test-metadata");

    // Now set to read-only
    testDao.setWritable(false);

    // Read operations should still work
    com.linkedin.metadata.aspect.EntityAspect aspect =
        testDao.getAspect(opContext, urnString, "status", ASPECT_LATEST_VERSION);
    assertTrue(aspect != null, "Read operations should work when not writable");
    assertEquals(aspect.getMetadata(), "test-metadata", "Read should return correct data");

    // Batch get should work
    Map<EntityAspectIdentifier, com.linkedin.metadata.aspect.EntityAspect> batchResult =
        testDao.batchGet(
            opContext,
            Set.of(new EntityAspectIdentifier(urnString, "status", ASPECT_LATEST_VERSION)),
            false);
    assertEquals(batchResult.size(), 1, "Batch get should work when not writable");

    // Count should work
    long count = testDao.countEntities(opContext);
    assertTrue(count > 0, "Count operations should work when not writable");
  }

  @Test
  public void testWritabilityDuringMigration() {
    // Simulate scenario where storage is being migrated
    testDao.setWritable(false);

    // All write operations should be blocked
    SystemAspect mockAspect = mock(SystemAspect.class);
    com.linkedin.metadata.aspect.EntityAspect mockEntityAspect =
        mock(com.linkedin.metadata.aspect.EntityAspect.class);
    when(mockAspect.asLatest()).thenReturn(mockEntityAspect);
    when(mockAspect.withVersion(anyLong())).thenReturn(mockEntityAspect);
    when(mockEntityAspect.getUrn()).thenReturn("urn:li:corpuser:migration");
    when(mockEntityAspect.getAspect()).thenReturn("status");
    when(mockEntityAspect.getVersion()).thenReturn(ASPECT_LATEST_VERSION);

    Optional<com.linkedin.metadata.aspect.EntityAspect> updateResult =
        testDao.updateAspect(opContext, null, mockAspect);
    assertFalse(updateResult.isPresent(), "Update blocked during migration");

    Optional<EntityAspect> insertResult = testDao.insertAspect(opContext, null, mockAspect, 1L);
    assertFalse(insertResult.isPresent(), "Insert blocked during migration");

    Urn urn = UrnUtils.getUrn("urn:li:corpuser:migration");
    testDao.deleteAspect(opContext, urn, "status", ASPECT_LATEST_VERSION);
    // Should not throw exception

    OperationContext mockOpContext = mock(OperationContext.class);
    when(mockOpContext.getKeyAspectName(any())).thenReturn("corpUserKey");
    int deletedCount = testDao.deleteUrn(mockOpContext, null, "urn:li:corpuser:migration");
    assertEquals(deletedCount, 0, "Delete URN blocked during migration");

    // After migration completes
    testDao.setWritable(true);

    // Writes should work again
    insertAspect("urn:li:corpuser:postMigration", "status", ASPECT_LATEST_VERSION, "test");
    com.linkedin.metadata.aspect.EntityAspect aspect =
        testDao.getAspect(
            opContext, "urn:li:corpuser:postMigration", "status", ASPECT_LATEST_VERSION);
    assertTrue(aspect != null, "Writes work after migration");
  }

  @Test
  public void getVersionRangeReturnsSentinelWhenAspectMissing() {
    com.linkedin.util.Pair<Long, Long> range =
        testDao.getVersionRange(
            opContext, "urn:li:container:missing-aspect-range-test", "containerKey");

    assertEquals(range.getFirst(), Long.valueOf(-1L));
    assertEquals(range.getSecond(), Long.valueOf(-1L));
  }

  private EbeanAspectDao newOptimisticDao() {
    return newOptimisticDao(mock(MetricUtils.class));
  }

  private EbeanAspectDao newOptimisticDao(MetricUtils metricUtils) {
    EbeanAspectDao dao =
        new EbeanAspectDao(
            PrimaryStorageTestUtils.ebeanResolver(server),
            EbeanConfiguration.testDefault,
            metricUtils,
            List.of(),
            null,
            true);
    dao.setWritable(true);
    return dao;
  }

  private EbeanAspectDao newLegacyDao() {
    EbeanAspectDao dao =
        new EbeanAspectDao(
            PrimaryStorageTestUtils.ebeanResolver(server),
            EbeanConfiguration.testDefault,
            mock(MetricUtils.class),
            List.of(),
            null,
            false);
    dao.setWritable(true);
    return dao;
  }

  private SystemAspect buildStatusAspect(String urn, Status status, SystemMetadata systemMetadata) {
    return new EbeanSystemAspect(
        null,
        UrnUtils.getUrn(urn),
        STATUS_ASPECT_NAME,
        opContext.getEntityRegistry().getEntitySpec(CORP_USER_ENTITY_NAME),
        opContext.getEntityRegistry().getAspectSpecs().get(STATUS_ASPECT_NAME),
        status,
        systemMetadata,
        AuditStampUtils.createDefaultAuditStamp(),
        null,
        null,
        null);
  }

  @Test
  public void testOptimisticLockingPinsPrimaryOnlyOnWriteIntent() {
    Database primary = mock(Database.class);
    Database read = mock(Database.class);
    PrimaryStorageResolver resolver = PrimaryStorageTestUtils.splitPoolEbeanResolver(primary, read);
    EbeanAspectDao optimisticDao =
        new EbeanAspectDao(
            resolver,
            EbeanConfiguration.testDefault,
            mock(MetricUtils.class),
            List.of(),
            null,
            true);
    optimisticDao.setWritable(true);

    OperationContext readCtx =
        TestOperationContexts.systemContextNoValidate()
            .withReadPreference(io.datahubproject.metadata.context.ReadPreference.READ);

    assertEquals(
        optimisticDao.resolveBatchGetDatabase(readCtx, true),
        primary,
        "write-intent under OL must pin PRIMARY");
    assertEquals(
        optimisticDao.resolveBatchGetDatabase(readCtx, false),
        read,
        "pure reads under OL may use the read pool");
    assertEquals(
        optimisticDao.resolveGetNextVersionsDatabase(readCtx, true),
        primary,
        "getNextVersions write-intent under OL must pin PRIMARY");
    assertEquals(
        optimisticDao.resolveGetNextVersionsDatabase(readCtx, false),
        read,
        "getNextVersions pure reads under OL may use the read pool (e.g. TimelineService)");
  }

  @Test
  public void testGetLatestAspectsSkipsForUpdateWhenOptimisticLockingOn() {
    EbeanAspectDao optimisticDao = newOptimisticDao();
    LoggedSql.start();

    optimisticDao.runInTransactionWithRetryUnlocked(
        opContext,
        (txContext) -> {
          optimisticDao.getLatestAspects(
              opContext, Map.of("urn:li:corpuser:testOptLockGetLatest", Set.of("status")), true);
          return TransactionResult.commit("");
        },
        mock(AspectsBatch.class),
        0);

    List<String> sql =
        LoggedSql.stop().stream().filter(str -> str.contains("testOptLockGetLatest")).toList();
    assertEquals(sql.size(), 1, String.format("Found: %s", sql));
    assertFalse(
        sql.get(0).toLowerCase().contains("for update"),
        String.format("Expected no FOR UPDATE when optimisticLocking=true, got: %s", sql));
  }

  @Test
  public void testBatchGetSkipsForUpdateWhenOptimisticLockingOn() {
    EbeanAspectDao optimisticDao = newOptimisticDao();
    LoggedSql.start();

    optimisticDao.runInTransactionWithRetryUnlocked(
        opContext,
        (txContext) -> {
          optimisticDao.batchGet(
              opContext,
              Set.of(
                  new EntityAspectIdentifier(
                      "urn:li:corpuser:testOptLockBatchGet",
                      STATUS_ASPECT_NAME,
                      ASPECT_LATEST_VERSION)),
              true);
          return TransactionResult.commit("");
        },
        mock(AspectsBatch.class),
        0);

    List<String> sql =
        LoggedSql.stop().stream().filter(str -> str.contains("testOptLockBatchGet")).toList();
    assertEquals(sql.size(), 1, String.format("Found: %s", sql));
    assertFalse(
        sql.get(0).toLowerCase().contains("for update"),
        String.format("Expected no FOR UPDATE when optimisticLocking=true, got: %s", sql));
  }

  @Test
  public void testGetNextVersionsSkipsForUpdateButQueriesMaxWhenOptimisticLockingOn() {
    // Under OL we still need MAX(version) for legacy null SystemMetadata.version paths;
    // only the FOR UPDATE pin is skipped (write-intent already pins primary).
    EbeanAspectDao optimisticDao = newOptimisticDao();
    LoggedSql.start();

    optimisticDao.runInTransactionWithRetryUnlocked(
        opContext,
        (txContext) -> {
          optimisticDao.getNextVersions(
              opContext,
              Map.of("urn:li:corpuser:testOptLockGetNextVersions", Set.of("status")),
              true);
          return TransactionResult.commit("");
        },
        mock(AspectsBatch.class),
        0);

    List<String> sql =
        LoggedSql.stop().stream()
            .filter(str -> str.contains("testOptLockGetNextVersions"))
            .toList();
    assertEquals(sql.size(), 1, String.format("Expected one version query under OL, got: %s", sql));
    assertFalse(
        sql.get(0).toLowerCase().contains("for update"),
        String.format("Expected no FOR UPDATE when optimisticLocking=true, got: %s", sql));
  }

  @Test
  public void testConditionalUpdateSqlPerDialect() {
    assertEquals(testDao.getDialect(), EbeanAspectDao.Dialect.H2_OR_OTHER);
    String mysql = testDao.buildConditionalUpdateSql(EbeanAspectDao.Dialect.MYSQL);
    assertTrue(mysql.contains("systemmetadata->>'$.version' = :expectedVersion"));
    String pg = testDao.buildConditionalUpdateSql(EbeanAspectDao.Dialect.POSTGRES);
    assertTrue(pg.contains("(systemmetadata::jsonb ->> 'version') = :expectedVersion"));
    String h2 = testDao.buildConditionalUpdateSql(EbeanAspectDao.Dialect.H2_OR_OTHER);
    assertTrue(h2.contains("INSTR(CAST(systemmetadata AS VARCHAR)"));
    for (String s : List.of(mysql, pg, h2)) {
      assertTrue(s.contains("WHERE urn = :urn AND aspect = :aspect AND version = 0"));
      assertTrue(s.contains("createdfor = :createdFor"));
    }
  }

  private void insertAspectWithSystemMetadata(
      EbeanAspectDao dao,
      String urn,
      String aspect,
      long version,
      String systemMetadata,
      String metadata) {
    EbeanAspectV2 aspectRecord = new EbeanAspectV2();
    aspectRecord.setKey(new EbeanAspectV2.PrimaryKey(urn, aspect, version));
    aspectRecord.setMetadata(metadata);
    aspectRecord.setCreatedBy("urn:li:corpuser:tester");
    aspectRecord.setCreatedFor(null);
    aspectRecord.setCreatedOn(new Timestamp(System.currentTimeMillis()));
    aspectRecord.setSystemMetadata(systemMetadata);
    dao.getServer().save(aspectRecord);
  }

  @Test
  public void testOptimisticLockRetryExhaustedIncrementsMetric() {
    MetricUtils metricUtils = mock(MetricUtils.class);
    EbeanAspectDao optimisticDao = newOptimisticDao(metricUtils);
    String urn = "urn:li:corpuser:testOptLockRetryExhausted";
    insertAspectWithSystemMetadata(
        optimisticDao,
        urn,
        STATUS_ASPECT_NAME,
        ASPECT_LATEST_VERSION,
        "{\"version\":\"1\"}",
        "{\"removed\":false}");

    try {
      optimisticDao.runInTransactionWithRetryUnlocked(
          opContext,
          (txContext) -> {
            throw new OptimisticLockConflictException("force exhaust retries");
          },
          mock(AspectsBatch.class),
          2);
      throw new AssertionError("expected DatabaseTransactionConflictException");
    } catch (DatabaseTransactionConflictException expected) {
      // OL exhaustion must surface as this specific subtype: RestliUtils,
      // GlobalControllerExceptionHandler, and DataHubDataFetcherExceptionHandler all key off it
      // to produce a 503 + Retry-After instead of a generic 500.
    }

    verify(metricUtils, org.mockito.Mockito.atLeastOnce())
        .increment(
            com.codahale.metrics.MetricRegistry.name(EbeanAspectDao.class, "optimistic_lock_retry"),
            1);
    verify(metricUtils, org.mockito.Mockito.atLeastOnce())
        .increment(
            com.codahale.metrics.MetricRegistry.name(
                EbeanAspectDao.class, "optimistic_lock_retry_exhausted"),
            1);
  }

  @Test
  public void testSaveLatestAspectConditionalNullVersionFallsBackToLegacy() {
    EbeanAspectDao optimisticDao = newOptimisticDao();
    String urn = "urn:li:corpuser:testOptLockNullVersion";
    SystemMetadata legacySysMeta = new SystemMetadata();
    legacySysMeta.setRunId("legacy");
    SystemAspect initial =
        new EbeanSystemAspect(
            null,
            UrnUtils.getUrn(urn),
            STATUS_ASPECT_NAME,
            opContext.getEntityRegistry().getEntitySpec(CORP_USER_ENTITY_NAME),
            opContext.getEntityRegistry().getAspectSpecs().get(STATUS_ASPECT_NAME),
            new Status(),
            legacySysMeta,
            AuditStampUtils.createDefaultAuditStamp(),
            null,
            null,
            null);
    optimisticDao.insertAspect(opContext, null, initial, ASPECT_LATEST_VERSION);

    SystemAspect latestAspect =
        optimisticDao
            .getLatestAspects(opContext, Map.of(urn, Set.of(STATUS_ASPECT_NAME)), false)
            .get(urn)
            .get(STATUS_ASPECT_NAME);
    assertNotNull(latestAspect);
    assertNull(latestAspect.getSystemMetadata().getVersion());

    SystemMetadata newSysMeta = new SystemMetadata();
    newSysMeta.setVersion("1");
    SystemAspect newAspect =
        new EbeanSystemAspect(
            null,
            UrnUtils.getUrn(urn),
            STATUS_ASPECT_NAME,
            opContext.getEntityRegistry().getEntitySpec(CORP_USER_ENTITY_NAME),
            opContext.getEntityRegistry().getAspectSpecs().get(STATUS_ASPECT_NAME),
            new Status(),
            newSysMeta,
            AuditStampUtils.createDefaultAuditStamp(),
            null,
            null,
            null);

    ConditionalSaveResult result =
        optimisticDao.saveLatestAspectConditional(opContext, null, latestAspect, newAspect, 1);

    assertEquals(result.getOutcome(), ConditionalWriteOutcome.UPDATED);
    EntityAspect after =
        optimisticDao
            .batchGet(
                opContext,
                Set.of(new EntityAspectIdentifier(urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION)),
                false)
            .get(new EntityAspectIdentifier(urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION));
    assertNotNull(after);
  }

  @Test
  public void testSaveLatestAspectConditionalNullSystemMetadataFallsBackToLegacy() {
    // Distinct from testSaveLatestAspectConditionalNullVersionFallsBackToLegacy above: that test
    // covers a row whose SystemMetadata is present but has no version field. Here the database
    // aspect's SystemMetadata itself is null (permitted by the SystemAspect contract, e.g.
    // EnvelopedSystemAspect#getSystemMetadata is @Nullable), which used to NPE while reading
    // expectedVersion before a null check was added.
    EbeanAspectDao optimisticDao = newOptimisticDao();
    String urn = "urn:li:corpuser:testOptLockNullSystemMetadata";

    // Seed a real row so the legacy fallback's updateAspect() has something to update.
    SystemMetadata seedSysMeta = new SystemMetadata();
    seedSysMeta.setVersion("1");
    SystemAspect seed =
        new EbeanSystemAspect(
            null,
            UrnUtils.getUrn(urn),
            STATUS_ASPECT_NAME,
            opContext.getEntityRegistry().getEntitySpec(CORP_USER_ENTITY_NAME),
            opContext.getEntityRegistry().getAspectSpecs().get(STATUS_ASPECT_NAME),
            new Status().setRemoved(false),
            seedSysMeta,
            AuditStampUtils.createDefaultAuditStamp(),
            null,
            null,
            null);
    optimisticDao.insertAspect(opContext, null, seed, ASPECT_LATEST_VERSION);

    SystemAspect currentVersion0 = mock(SystemAspect.class);
    when(currentVersion0.getSystemMetadata()).thenReturn(null);
    when(currentVersion0.getRecordTemplate()).thenReturn(new Status().setRemoved(false));

    SystemAspect latestAspect = mock(SystemAspect.class);
    when(latestAspect.getDatabaseAspect()).thenReturn(Optional.of(currentVersion0));

    SystemMetadata newSysMeta = new SystemMetadata();
    newSysMeta.setVersion("1");
    SystemAspect newAspect =
        new EbeanSystemAspect(
            null,
            UrnUtils.getUrn(urn),
            STATUS_ASPECT_NAME,
            opContext.getEntityRegistry().getEntitySpec(CORP_USER_ENTITY_NAME),
            opContext.getEntityRegistry().getAspectSpecs().get(STATUS_ASPECT_NAME),
            new Status().setRemoved(true),
            newSysMeta,
            AuditStampUtils.createDefaultAuditStamp(),
            null,
            null,
            null);

    ConditionalSaveResult result =
        optimisticDao.saveLatestAspectConditional(opContext, null, latestAspect, newAspect, 1);

    assertEquals(result.getOutcome(), ConditionalWriteOutcome.UPDATED);
    EntityAspect after =
        optimisticDao
            .batchGet(
                opContext,
                Set.of(new EntityAspectIdentifier(urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION)),
                false)
            .get(new EntityAspectIdentifier(urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION));
    assertNotNull(after);
    assertTrue(after.getMetadata().contains("\"removed\":true"));
  }

  @Test
  public void testGetNextVersionsComputesMaxWithHistory() {
    EbeanAspectDao optimisticDao = newOptimisticDao();
    String urn = "urn:li:corpuser:testOptLockGetNextFallback";
    insertAspectWithSystemMetadata(
        optimisticDao,
        urn,
        STATUS_ASPECT_NAME,
        ASPECT_LATEST_VERSION,
        "{\"version\":\"1\"}",
        "v0-metadata");
    insertAspectWithSystemMetadata(
        optimisticDao, urn, STATUS_ASPECT_NAME, 1L, "{\"version\":\"0\"}", "v1-metadata");

    LoggedSql.start();
    Map<String, Map<String, Long>> versions =
        optimisticDao.getNextVersions(
            opContext, null, Map.of(urn, Set.of(STATUS_ASPECT_NAME)), true);
    List<String> sql = LoggedSql.stop();

    assertEquals(versions.get(urn).get(STATUS_ASPECT_NAME), Long.valueOf(2L));
    assertTrue(
        sql.stream().anyMatch(s -> s.contains("metadata_aspect_v2") && s.contains(urn)),
        "should query metadata_aspect_v2 for the urn, got: " + sql);
  }

  @Test
  public void testGetNextVersionsQueriesMaxForLegacyNullVersionWithHistory() {
    // EntityUtils only calls getNextVersions for aspects missing SystemMetadata.version. Under OL
    // we must still query MAX(version)+1 — returning 0 would collide with existing history rows.
    EbeanAspectDao optimisticDao = newOptimisticDao();
    String urn = "urn:li:corpuser:testOptLockGetNextLegacyHistory";
    insertAspectWithSystemMetadata(
        optimisticDao,
        urn,
        STATUS_ASPECT_NAME,
        ASPECT_LATEST_VERSION,
        "{\"runId\":\"legacy-no-version\"}",
        "v0-metadata");
    insertAspectWithSystemMetadata(
        optimisticDao, urn, STATUS_ASPECT_NAME, 1L, "{\"runId\":\"h1\"}", "v1-metadata");
    insertAspectWithSystemMetadata(
        optimisticDao, urn, STATUS_ASPECT_NAME, 2L, "{\"runId\":\"h2\"}", "v2-metadata");

    LoggedSql.start();
    Map<String, Map<String, Long>> versions =
        optimisticDao.getNextVersions(
            opContext, null, Map.of(urn, Set.of(STATUS_ASPECT_NAME)), true);
    List<String> sql = LoggedSql.stop();

    assertEquals(
        versions.get(urn).get(STATUS_ASPECT_NAME),
        Long.valueOf(3L),
        "legacy null-version v0 with history at 1–2 must get MAX+1=3, not default 0");
    assertTrue(
        sql.stream().anyMatch(s -> s.contains("metadata_aspect_v2") && s.contains(urn)),
        "must query metadata_aspect_v2 for MAX(version), got: " + sql);
  }

  /**
   * Concurrent v0 insert under OL must not CAS-overwrite the winner. Throw {@link
   * OptimisticLockConflictException} so the txn retry loop re-reads and re-applies.
   */
  @Test
  public void testDuplicateKeyInsertThrowsOptimisticLockConflict() {
    MetricUtils metricUtils = mock(MetricUtils.class);
    EbeanAspectDao optimisticDao = newOptimisticDao(metricUtils);
    String urn = "urn:li:corpuser:testOptLockInsertConflict";

    insertAspectWithSystemMetadata(
        optimisticDao,
        urn,
        STATUS_ASPECT_NAME,
        ASPECT_LATEST_VERSION,
        "{\"version\":\"1\"}",
        "{\"removed\":false}");

    SystemMetadata secondMeta = new SystemMetadata();
    secondMeta.setVersion("2");
    try {
      optimisticDao.insertAspect(
          opContext,
          null,
          buildStatusAspect(urn, new Status().setRemoved(true), secondMeta),
          ASPECT_LATEST_VERSION);
      throw new AssertionError("expected OptimisticLockConflictException");
    } catch (OptimisticLockConflictException thrown) {
      assertTrue(thrown.getMessage().contains(urn));
    }
    verify(metricUtils, org.mockito.Mockito.atLeastOnce())
        .increment(
            com.codahale.metrics.MetricRegistry.name(
                EbeanAspectDao.class, "optimistic_lock_insert_fallback"),
            1);

    EntityAspect after =
        optimisticDao
            .batchGet(
                opContext,
                Set.of(new EntityAspectIdentifier(urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION)),
                false)
            .get(new EntityAspectIdentifier(urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION));
    SystemMetadata afterMeta =
        com.linkedin.metadata.utils.SystemMetadataUtils.parseSystemMetadata(
            after.getSystemMetadata());
    assertEquals(afterMeta.getVersion(), "1", "winner row must remain unchanged");
    assertTrue(
        after.getMetadata().contains("\"removed\":false")
            || after.getMetadata().contains("\"removed\": false"));
  }

  /** Same conflict-for-retry behavior when the existing row is pre-versioning. */
  @Test
  public void testDuplicateKeyInsertOnLegacyRowThrowsOptimisticLockConflict() {
    MetricUtils metricUtils = mock(MetricUtils.class);
    EbeanAspectDao optimisticDao = newOptimisticDao(metricUtils);
    String urn = "urn:li:corpuser:testOptLockInsertLegacyConflict";

    insertAspectWithSystemMetadata(
        optimisticDao,
        urn,
        STATUS_ASPECT_NAME,
        ASPECT_LATEST_VERSION,
        "{\"runId\":\"no-version\"}",
        "{\"removed\":false}");

    SystemMetadata nextMeta = new SystemMetadata();
    nextMeta.setVersion("1");
    assertThrows(
        OptimisticLockConflictException.class,
        () ->
            optimisticDao.insertAspect(
                opContext,
                null,
                buildStatusAspect(urn, new Status().setRemoved(true), nextMeta),
                ASPECT_LATEST_VERSION));
    verify(metricUtils, org.mockito.Mockito.atLeastOnce())
        .increment(
            com.codahale.metrics.MetricRegistry.name(
                EbeanAspectDao.class, "optimistic_lock_insert_fallback"),
            1);

    EntityAspect after =
        optimisticDao.getAspect(opContext, urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION);
    assertTrue(
        after.getMetadata().contains("\"removed\":false")
            || after.getMetadata().contains("\"removed\": false"),
        "legacy winner row must not be overwritten by loser insert");
  }

  @Test
  public void testConcurrentWritersWithHistoryRetention() throws Exception {
    EbeanAspectDao optimisticDao = newOptimisticDao();
    String urn = "urn:li:corpuser:testOptLockHistory";
    insertAspectWithSystemMetadata(
        optimisticDao,
        urn,
        STATUS_ASPECT_NAME,
        ASPECT_LATEST_VERSION,
        "{\"version\":\"1\"}",
        "{\"removed\":false}");

    AtomicInteger successes = new AtomicInteger();
    AtomicReference<Throwable> firstError = new AtomicReference<>();
    CountDownLatch bothRead = new CountDownLatch(2);
    CountDownLatch writeGate = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(2);
    ExecutorService pool = Executors.newFixedThreadPool(2);

    for (int i = 0; i < 2; i++) {
      final boolean removed = i == 0;
      pool.submit(
          () -> {
            try {
              optimisticDao.runInTransactionWithRetryUnlocked(
                  opContext,
                  (txContext) -> {
                    SystemAspect latest =
                        optimisticDao
                            .getLatestAspects(
                                opContext, Map.of(urn, Set.of(STATUS_ASPECT_NAME)), false)
                            .get(urn)
                            .get(STATUS_ASPECT_NAME);
                    String expected =
                        latest.getDatabaseAspect().get().getSystemMetadata().getVersion();
                    bothRead.countDown();
                    try {
                      assertTrue(writeGate.await(30, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      throw new RuntimeException(e);
                    }
                    SystemMetadata sm = new SystemMetadata();
                    sm.setVersion(String.valueOf(Long.parseLong(expected) + 1));
                    ConditionalSaveResult r =
                        optimisticDao.saveLatestAspectConditional(
                            opContext,
                            txContext,
                            latest,
                            buildStatusAspect(urn, new Status().setRemoved(removed), sm),
                            /* maxVersionsToKeep */ 5);
                    if (r.getOutcome() == ConditionalWriteOutcome.CONFLICT) {
                      throw new OptimisticLockConflictException("history path conflict");
                    }
                    assertEquals(r.getOutcome(), ConditionalWriteOutcome.UPDATED);
                    successes.incrementAndGet();
                    return TransactionResult.commit("");
                  },
                  mock(AspectsBatch.class),
                  10);
            } catch (Throwable t) {
              firstError.compareAndSet(null, t);
            } finally {
              done.countDown();
            }
          });
    }
    assertTrue(bothRead.await(30, TimeUnit.SECONDS));
    writeGate.countDown();
    assertTrue(done.await(60, TimeUnit.SECONDS));
    pool.shutdownNow();

    assertNull(
        firstError.get(),
        "retention>1 concurrent writers failed: "
            + firstError.get()
            + (firstError.get() != null && firstError.get().getCause() != null
                ? " cause=" + firstError.get().getCause()
                : ""));
    assertEquals(successes.get(), 2);

    EntityAspect v0 =
        optimisticDao
            .batchGet(
                opContext,
                Set.of(new EntityAspectIdentifier(urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION)),
                false)
            .get(new EntityAspectIdentifier(urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION));
    SystemMetadata afterMeta =
        com.linkedin.metadata.utils.SystemMetadataUtils.parseSystemMetadata(v0.getSystemMetadata());
    assertEquals(afterMeta.getVersion(), "3");

    EntityAspect history1 = optimisticDao.getAspect(opContext, urn, STATUS_ASPECT_NAME, 1L);
    EntityAspect history2 = optimisticDao.getAspect(opContext, urn, STATUS_ASPECT_NAME, 2L);
    assertTrue(
        history1 != null && history2 != null,
        "expected both history rows (versions 1 and 2) with maxVersionsToKeep=5 and two writes");
  }

  /**
   * Reproduction for history-before-CAS: with {@code maxVersionsToKeep > 1} and no surrounding
   * transaction, a conflicting conditional save must not leave an orphaned history row. Inserting
   * history before CAS autocommits the history write even when version-0 CAS fails.
   */
  @Test
  public void testConflictWithRetentionDoesNotLeaveOrphanHistoryRow() {
    EbeanAspectDao optimisticDao = newOptimisticDao();
    String urn = "urn:li:corpuser:testOptLockOrphanHistory";

    insertAspectWithSystemMetadata(
        optimisticDao,
        urn,
        STATUS_ASPECT_NAME,
        ASPECT_LATEST_VERSION,
        "{\"version\":\"1\"}",
        "{\"removed\":false}");

    SystemAspect staleLatest =
        optimisticDao
            .getLatestAspects(opContext, Map.of(urn, Set.of(STATUS_ASPECT_NAME)), false)
            .get(urn)
            .get(STATUS_ASPECT_NAME);

    // Winner bumps v0 without writing history, so history version 1 is free for the stale writer
    // to wrongly insert before its failed CAS.
    SystemMetadata winnerMeta = new SystemMetadata();
    winnerMeta.setVersion("2");
    assertEquals(
        optimisticDao
            .saveLatestAspectConditional(
                opContext,
                null,
                staleLatest,
                buildStatusAspect(urn, new Status().setRemoved(true), winnerMeta),
                /* maxVersionsToKeep */ 1)
            .getOutcome(),
        ConditionalWriteOutcome.UPDATED);
    assertNull(
        optimisticDao.getAspect(opContext, urn, STATUS_ASPECT_NAME, 1L),
        "winner with maxVersionsToKeep=1 must not write history");

    // Stale writer still holds the pre-winner latestAspect (expectedVersion=1) but asks for
    // retention, which (with history-before-CAS) inserts history@1 before the failing CAS.
    SystemMetadata loserMeta = new SystemMetadata();
    loserMeta.setVersion("2");
    ConditionalSaveResult conflict =
        optimisticDao.saveLatestAspectConditional(
            opContext,
            null,
            staleLatest,
            buildStatusAspect(urn, new Status().setRemoved(false), loserMeta),
            /* maxVersionsToKeep */ 5);

    assertEquals(conflict.getOutcome(), ConditionalWriteOutcome.CONFLICT);
    assertFalse(conflict.getInserted().isPresent(), "conflict must not report a history insert");
    assertFalse(conflict.getUpdated().isPresent());

    assertNull(
        optimisticDao.getAspect(opContext, urn, STATUS_ASPECT_NAME, 1L),
        "conflicting save must not orphan history version 1 when CAS fails");

    EntityAspect v0 =
        optimisticDao.getAspect(opContext, urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION);
    SystemMetadata afterMeta =
        com.linkedin.metadata.utils.SystemMetadataUtils.parseSystemMetadata(v0.getSystemMetadata());
    assertEquals(afterMeta.getVersion(), "2", "v0 must remain at winner's version");
  }

  /**
   * Reproduction for {@code !canWrite} looking like an OL conflict: conditional UPDATE used to
   * return empty (same as CAS miss), so the txn retry loop kept retrying until {@link
   * RetryLimitReached}. Read-only mode must fail fast with a non-{@link
   * jakarta.persistence.PersistenceException}.
   */
  @Test
  public void testReadOnlyFailsFastWithoutOptimisticLockRetries() {
    EbeanAspectDao optimisticDao = newOptimisticDao();
    String urn = "urn:li:corpuser:testOptLockReadOnly";

    insertAspectWithSystemMetadata(
        optimisticDao,
        urn,
        STATUS_ASPECT_NAME,
        ASPECT_LATEST_VERSION,
        "{\"version\":\"1\"}",
        "{\"removed\":false}");

    optimisticDao.setWritable(false);

    AtomicInteger attempts = new AtomicInteger();
    final int maxRetries = 5;

    try {
      optimisticDao.runInTransactionWithRetryUnlocked(
          opContext,
          (txContext) -> {
            attempts.incrementAndGet();
            SystemAspect latest =
                optimisticDao
                    .getLatestAspects(opContext, Map.of(urn, Set.of(STATUS_ASPECT_NAME)), false)
                    .get(urn)
                    .get(STATUS_ASPECT_NAME);
            SystemMetadata next = new SystemMetadata();
            next.setVersion("2");
            ConditionalSaveResult result =
                optimisticDao.saveLatestAspectConditional(
                    opContext,
                    txContext,
                    latest,
                    buildStatusAspect(urn, new Status().setRemoved(true), next),
                    1);
            if (result.getOutcome() == ConditionalWriteOutcome.CONFLICT) {
              // Old bug path: empty conditional update → CONFLICT → retryable OL exception.
              throw new OptimisticLockConflictException("should not treat read-only as conflict");
            }
            return TransactionResult.commit("");
          },
          mock(AspectsBatch.class),
          maxRetries);
      throw new AssertionError("expected AspectWriteDisabledException");
    } catch (AspectWriteDisabledException expected) {
      // fail-fast path
    } catch (RetryLimitReached e) {
      throw new AssertionError(
          "read-only was retried as an optimistic-lock conflict until RetryLimitReached", e);
    }

    assertEquals(
        attempts.get(),
        1,
        "read-only must not be retried as an optimistic-lock conflict (would burn "
            + (maxRetries + 1)
            + " attempts)");

    // Row unchanged.
    EntityAspect after =
        optimisticDao.getAspect(opContext, urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION);
    SystemMetadata afterMeta =
        com.linkedin.metadata.utils.SystemMetadataUtils.parseSystemMetadata(
            after.getSystemMetadata());
    assertEquals(afterMeta.getVersion(), "1");
  }

  @Test
  public void testUpdateAspectConditionalThrowsWhenReadOnly() {
    EbeanAspectDao optimisticDao = newOptimisticDao();
    String urn = "urn:li:corpuser:testOptLockReadOnlyConditional";
    insertAspectWithSystemMetadata(
        optimisticDao,
        urn,
        STATUS_ASPECT_NAME,
        ASPECT_LATEST_VERSION,
        "{\"version\":\"1\"}",
        "{\"removed\":false}");

    optimisticDao.setWritable(false);
    SystemMetadata next = new SystemMetadata();
    next.setVersion("2");
    assertThrows(
        AspectWriteDisabledException.class,
        () ->
            optimisticDao.updateAspectConditional(
                opContext, null, buildStatusAspect(urn, new Status().setRemoved(true), next), "1"));
  }

  /**
   * Pre-existing bug (flag-independent): a successful retry after a PersistenceException used to
   * still throw {@link RetryLimitReached} because prior exceptions were not cleared. {@code
   * transactionContext.success()} must clear them for both OL and non-OL DAOs.
   */
  @Test
  public void testTransactionRetrySuccessClearsPriorExceptionsWithoutOptimisticLocking() {
    AtomicInteger attempts = new AtomicInteger();
    TransactionResult<String> result =
        testDao.runInTransactionWithRetryUnlocked(
            opContext,
            (txContext) -> {
              if (attempts.getAndIncrement() == 0) {
                throw new jakarta.persistence.PersistenceException("transient failure");
              }
              return TransactionResult.commit("ok");
            },
            mock(AspectsBatch.class),
            5);

    assertEquals(attempts.get(), 2);
    assertTrue(result.isCommitOrRollback());
    assertEquals(result.getResults().orElse(null), "ok");
  }

  /**
   * Intentional rollback must not surface as {@link RetryLimitReached} after {@code success()}
   * clears the exception list.
   */
  @Test
  public void testIntentionalRollbackDoesNotThrowRetryLimitReached() {
    TransactionResult<String> result =
        testDao.runInTransactionWithRetryUnlocked(
            opContext, (txContext) -> TransactionResult.rollback(), mock(AspectsBatch.class), 3);

    assertFalse(result.isCommitOrRollback());
    assertTrue(result.getResults().isEmpty());
  }

  /**
   * Mixed fleet on H2: legacy {@code FOR UPDATE} path and OL CAS path share one DB and must keep
   * {@code SystemMetadata.version} coherent across a sequential hand-off (GMS OL-off ↔ MCE OL-on).
   */
  @Test
  public void testMixedModeLegacyAndOptimisticSequentialConverges() {
    EbeanAspectDao legacy = newLegacyDao();
    EbeanAspectDao optimistic = newOptimisticDao();
    String urn = "urn:li:corpuser:testOptLockMixedModeSeq";

    insertAspectWithSystemMetadata(
        legacy,
        urn,
        STATUS_ASPECT_NAME,
        ASPECT_LATEST_VERSION,
        "{\"version\":\"1\"}",
        "{\"removed\":false}");

    legacy.runInTransactionWithRetryUnlocked(
        opContext,
        (tx) -> {
          SystemAspect latest =
              legacy
                  .getLatestAspects(opContext, Map.of(urn, Set.of(STATUS_ASPECT_NAME)), true)
                  .get(urn)
                  .get(STATUS_ASPECT_NAME);
          SystemMetadata next = new SystemMetadata();
          next.setVersion("2");
          legacy.saveLatestAspect(
              opContext,
              tx,
              latest,
              buildStatusAspect(urn, new Status().setRemoved(true), next),
              1);
          return TransactionResult.commit("");
        },
        mock(AspectsBatch.class),
        3);

    optimistic.runInTransactionWithRetryUnlocked(
        opContext,
        (tx) -> {
          SystemAspect latest =
              optimistic
                  .getLatestAspects(opContext, Map.of(urn, Set.of(STATUS_ASPECT_NAME)), true)
                  .get(urn)
                  .get(STATUS_ASPECT_NAME);
          SystemMetadata next = new SystemMetadata();
          next.setVersion("3");
          ConditionalSaveResult r =
              optimistic.saveLatestAspectConditional(
                  opContext,
                  tx,
                  latest,
                  buildStatusAspect(urn, new Status().setRemoved(false), next),
                  1);
          assertEquals(r.getOutcome(), ConditionalWriteOutcome.UPDATED);
          return TransactionResult.commit("");
        },
        mock(AspectsBatch.class),
        3);

    EntityAspect after =
        optimistic.getAspect(opContext, urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION);
    assertEquals(
        com.linkedin.metadata.utils.SystemMetadataUtils.parseSystemMetadata(
                after.getSystemMetadata())
            .getVersion(),
        "3");
  }

  @Test
  public void testIsDuplicateKeyCauseDetectsEbeanDuplicateKey() {
    assertTrue(
        EbeanAspectDao.isDuplicateKeyCause(
            new DuplicateKeyException("dup", new java.sql.SQLException("x", "23505"))));
  }

  @Test
  public void testIsDuplicateKeyCauseDetectsPostgresSqlState() {
    PersistenceException pe =
        new PersistenceException(
            "wrap",
            new java.sql.SQLException("duplicate key value violates unique constraint", "23505"));
    assertTrue(EbeanAspectDao.isDuplicateKeyCause(pe));
  }

  @Test
  public void testIsDuplicateKeyCauseDetectsMysqlErrorCode() {
    PersistenceException pe =
        new PersistenceException(
            "wrap",
            new java.sql.SQLException("Duplicate entry 'x' for key 'PRIMARY'", "23000", 1062));
    assertTrue(EbeanAspectDao.isDuplicateKeyCause(pe));
  }

  @Test
  public void testIsDuplicateKeyCauseIgnoresForeignKeyViolation() {
    // MySQL FK often uses SQLState 23000 with error 1452 — must not look like a dup-key.
    PersistenceException pe =
        new PersistenceException(
            "wrap",
            new java.sql.SQLException(
                "Cannot add or update a child row: a foreign key constraint fails", "23000", 1452));
    assertFalse(EbeanAspectDao.isDuplicateKeyCause(pe));
  }

  @Test
  public void testIsDuplicateKeyCauseIgnoresMessageOnlyMatch() {
    // Former fragile path matched message substrings without SQLState / typed cause.
    assertFalse(
        EbeanAspectDao.isDuplicateKeyCause(
            new PersistenceException("Duplicate entry 'foo' for key 'PRIMARY'")));
  }
}
