package com.linkedin.metadata.entity.ebean;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

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
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.entity.TransactionResult;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import io.ebean.test.LoggedSql;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
            server, EbeanConfiguration.testDefault, mock(MetricUtils.class), List.of(), null);
  }

  @AfterMethod
  public void cleanup() {
    // Shutdown Database instance to prevent thread pool and connection leaks
    EbeanTestUtils.shutdownDatabase(server);
  }

  @DataProvider(name = "writabilityConfig")
  public Object[][] writabilityConfigProvider() {
    return new Object[][] {
      {true, "Writable"}, // canWrite = true, description
      {false, "ReadOnly"} // canWrite = false, description
    };
  }

  @Test(dataProvider = "writabilityConfig")
  public void testGetNextVersionForUpdate(boolean canWrite, String description) {
    testDao.setWritable(canWrite);
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
    if (canWrite) {
      assertTrue(
          sql.get(0).contains("for update;"),
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
            null, // payloadValidators
            null); // validationConfig

    // Try to update aspect
    Optional<com.linkedin.metadata.aspect.EntityAspect> result =
        testDao.insertAspect(null, systemAspect, ASPECT_LATEST_VERSION);

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
        testDao.insertAspect(null, mockAspect, 1L);

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
          testDao.getAspect(urnString, aspectName, ASPECT_LATEST_VERSION);
      assertNotNull(beforeDelete, "Aspect should exist before delete");

      // Delete the aspect
      Urn urn = UrnUtils.getUrn(urnString);
      testDao.deleteAspect(urn, aspectName, ASPECT_LATEST_VERSION);

      // Verify it's deleted
      com.linkedin.metadata.aspect.EntityAspect afterDelete =
          testDao.getAspect(urnString, aspectName, ASPECT_LATEST_VERSION);
      assertNull(afterDelete, "Aspect should be deleted when writable");
    } else {
      // When not writable, delete should be a no-op
      Urn urn = UrnUtils.getUrn(urnString);
      testDao.deleteAspect(urn, aspectName, ASPECT_LATEST_VERSION);
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
          testDao.getAspect(urnString, "corpUserInfo", ASPECT_LATEST_VERSION);
      com.linkedin.metadata.aspect.EntityAspect aspect2 =
          testDao.getAspect(urnString, "status", ASPECT_LATEST_VERSION);
      com.linkedin.metadata.aspect.EntityAspect keyAspect =
          testDao.getAspect(urnString, "corpUserKey", ASPECT_LATEST_VERSION);
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
          testDao.getAspect(urnString, "corpUserInfo", ASPECT_LATEST_VERSION);
      com.linkedin.metadata.aspect.EntityAspect afterAspect2 =
          testDao.getAspect(urnString, "status", ASPECT_LATEST_VERSION);
      com.linkedin.metadata.aspect.EntityAspect afterKeyAspect =
          testDao.getAspect(urnString, "corpUserKey", ASPECT_LATEST_VERSION);
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
        testDao.getAspect(urnString, "status", ASPECT_LATEST_VERSION);
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
        testDao.insertAspect(null, mockAspect, ASPECT_LATEST_VERSION);
    assertFalse(result.isPresent(), "Insert should fail when not writable");

    // Set back to writable
    testDao.setWritable(true);

    // Insert should work again
    insertAspect("urn:li:corpuser:testToggle3", "status", ASPECT_LATEST_VERSION, "test-metadata");
    com.linkedin.metadata.aspect.EntityAspect aspect3 =
        testDao.getAspect("urn:li:corpuser:testToggle3", "status", ASPECT_LATEST_VERSION);
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
        testDao.getAspect(urnString, "status", ASPECT_LATEST_VERSION);
    assertTrue(aspect != null, "Read operations should work when not writable");
    assertEquals(aspect.getMetadata(), "test-metadata", "Read should return correct data");

    // Batch get should work
    Map<EntityAspectIdentifier, com.linkedin.metadata.aspect.EntityAspect> batchResult =
        testDao.batchGet(
            Set.of(new EntityAspectIdentifier(urnString, "status", ASPECT_LATEST_VERSION)), false);
    assertEquals(batchResult.size(), 1, "Batch get should work when not writable");

    // Count should work
    long count = testDao.countEntities();
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
        testDao.updateAspect(null, mockAspect);
    assertFalse(updateResult.isPresent(), "Update blocked during migration");

    Optional<EntityAspect> insertResult = testDao.insertAspect(null, mockAspect, 1L);
    assertFalse(insertResult.isPresent(), "Insert blocked during migration");

    Urn urn = UrnUtils.getUrn("urn:li:corpuser:migration");
    testDao.deleteAspect(urn, "status", ASPECT_LATEST_VERSION);
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
        testDao.getAspect("urn:li:corpuser:postMigration", "status", ASPECT_LATEST_VERSION);
    assertTrue(aspect != null, "Writes work after migration");
  }
}
