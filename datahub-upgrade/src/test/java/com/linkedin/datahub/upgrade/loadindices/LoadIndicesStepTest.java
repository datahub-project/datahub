package com.linkedin.datahub.upgrade.loadindices;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.ActorContext;
import io.datahubproject.metadata.context.OperationContext;
import io.ebean.Database;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LoadIndicesStepTest {

  private LoadIndicesStep loadIndicesStep;
  private Database database;
  @Mock private EntityService<?> mockEntityService;
  @Mock private UpdateIndicesService mockUpdateIndicesService;
  @Mock private LoadIndicesIndexManager mockIndexManager;
  @Mock private UpgradeContext mockUpgradeContext;
  @Mock private UpgradeReport mockUpgradeReport;
  @Mock private OperationContext mockOperationContext;
  @Mock private EntityRegistry mockEntityRegistry;
  @Mock private ActorContext mockActorContext;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);

    // Create a real H2 in-memory database for testing with a unique name to avoid conflicts
    String instanceId = "loadindices_" + UUID.randomUUID().toString().replace("-", "");
    String serverName = "loadindices_test_" + UUID.randomUUID().toString().replace("-", "");
    database = EbeanTestUtils.createNamedTestServer(instanceId, serverName);

    // Setup test database with some sample data
    setupTestDatabase();

    loadIndicesStep =
        new LoadIndicesStep(
            database, mockEntityService, mockUpdateIndicesService, mockIndexManager);

    when(mockUpgradeContext.report()).thenReturn(mockUpgradeReport);
    when(mockUpgradeContext.opContext()).thenReturn(mockOperationContext);
    when(mockOperationContext.getEntityRegistry()).thenReturn(mockEntityRegistry);
    when(mockOperationContext.getActorContext()).thenReturn(mockActorContext);

    // Mock authentication context
    com.datahub.authentication.Authentication mockAuth =
        mock(com.datahub.authentication.Authentication.class);
    com.datahub.authentication.Actor mockActor = mock(com.datahub.authentication.Actor.class);
    when(mockActorContext.getAuthentication()).thenReturn(mockAuth);
    when(mockAuth.getActor()).thenReturn(mockActor);
    when(mockActor.toUrnStr()).thenReturn("urn:li:corpuser:testUser");
  }

  @AfterMethod
  public void cleanup() {
    if (database != null) {
      database.shutdown();
    }
  }

  private void setupTestDatabase() {
    // Insert a few test rows to simulate real data
    insertTestRow(
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleDataset1,PROD)",
        "container",
        0,
        Instant.now(),
        "testUser");
    insertTestRow(
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleDataset2,PROD)",
        "container",
        0,
        Instant.now(),
        "testUser");
    insertTestRow(
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleDataset3,PROD)",
        "ownership",
        0,
        Instant.now(),
        "testUser");
  }

  private void insertTestRow(
      String urn, String aspect, int version, Instant createdTime, String createdBy) {
    EbeanAspectV2 aspectV2 = new EbeanAspectV2();
    aspectV2.setUrn(urn);
    aspectV2.setAspect(aspect);
    aspectV2.setVersion(version);
    aspectV2.setMetadata("{}"); // Required field
    aspectV2.setCreatedOn(Timestamp.from(createdTime));
    aspectV2.setCreatedBy(createdBy);
    database.save(aspectV2);
  }

  @Test
  public void testId() {
    assertEquals(loadIndicesStep.id(), "LoadIndicesStep");
  }

  @Test
  public void testRetryCount() {
    assertEquals(loadIndicesStep.retryCount(), 0);
  }

  @Test
  public void testExecutableSuccess() throws IOException {
    var executable = loadIndicesStep.executable();
    assertNotNull(executable);

    // Mock successful index manager operations
    doNothing().when(mockIndexManager).disableRefresh();
    when(mockIndexManager.isRefreshDisabled()).thenReturn(true);
    doNothing().when(mockIndexManager).restoreRefresh();

    // Execute the step
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "LoadIndicesStep");
    assertTrue(result.result() == DataHubUpgradeState.SUCCEEDED);

    // Verify index manager was called
    verify(mockIndexManager, times(1)).disableRefresh();
    verify(mockIndexManager, times(1)).restoreRefresh();
  }

  @Test
  public void testExecutableWithIndexManagerDisableFailure() throws IOException {
    var executable = loadIndicesStep.executable();
    assertNotNull(executable);

    // Mock index manager disable failure
    doThrow(new IOException("Failed to disable refresh")).when(mockIndexManager).disableRefresh();

    // Execute the step
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "LoadIndicesStep");
    assertTrue(result.result() == DataHubUpgradeState.FAILED);

    // Verify index manager was called
    verify(mockIndexManager, times(1)).disableRefresh();
    verify(mockIndexManager, never()).restoreRefresh();
  }

  @Test
  public void testExecutableWithIndexManagerRestoreFailure() throws IOException {
    var executable = loadIndicesStep.executable();
    assertNotNull(executable);

    // Mock successful disable but failed restore
    doNothing().when(mockIndexManager).disableRefresh();
    when(mockIndexManager.isRefreshDisabled()).thenReturn(true);
    doThrow(new IOException("Failed to restore refresh")).when(mockIndexManager).restoreRefresh();

    // Execute the step
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "LoadIndicesStep");
    // The step should still succeed even if restore fails, as it logs the error but doesn't fail
    // the step
    assertTrue(result.result() == DataHubUpgradeState.SUCCEEDED);

    // Verify index manager was called
    verify(mockIndexManager, times(1)).disableRefresh();
    verify(mockIndexManager, times(1)).restoreRefresh();
  }

  @Test
  public void testProcessAllDataDirectly() throws Exception {
    // Test the processAllDataDirectly method directly
    // This method processes data from the database and calls updateIndicesService

    // Mock the updateIndicesService to not throw exceptions
    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());

    // Create test args
    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = 10;
    args.aspectNames = java.util.List.of("container", "ownership");

    // Call the method via reflection since it's private - it takes 3 parameters
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "processAllDataDirectly",
            OperationContext.class,
            LoadIndicesArgs.class,
            java.util.function.Function.class);
    method.setAccessible(true);

    // This should complete successfully with our test data
    method.invoke(
        loadIndicesStep,
        mockOperationContext,
        args,
        (java.util.function.Function<String, Void>) msg -> null);

    // Verify that updateIndicesService was called - it calls flush() instead of
    // handleChangeEvents()
    verify(mockUpdateIndicesService, atLeastOnce()).flush();
  }

  @Test
  public void testGetDefaultAspectNames() throws Exception {
    // Test the getDefaultAspectNames method
    var method =
        LoadIndicesStep.class.getDeclaredMethod("getDefaultAspectNames", OperationContext.class);
    method.setAccessible(true);

    // Mock entity registry to return some entity specs
    Map<String, EntitySpec> entitySpecs = new HashMap<>();
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) method.invoke(loadIndicesStep, mockOperationContext);

    assertNotNull(result);
    // Result should be empty since we have no entity specs
    assertTrue(result.isEmpty());
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetDefaultAspectNamesWithNullOperationContext() {
    // Test with null operation context - should throw NullPointerException wrapped in
    // RuntimeException
    try {
      var method =
          LoadIndicesStep.class.getDeclaredMethod("getDefaultAspectNames", OperationContext.class);
      method.setAccessible(true);
      method.invoke(loadIndicesStep, (OperationContext) null);
      fail("Expected RuntimeException to be thrown");
    } catch (Exception e) {
      // The actual exception will be wrapped in InvocationTargetException
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new RuntimeException(e);
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetDefaultAspectNamesWithNullEntityRegistry() {
    // Test with null entity registry - should throw NullPointerException wrapped in
    // RuntimeException
    when(mockOperationContext.getEntityRegistry()).thenReturn(null);

    try {
      var method =
          LoadIndicesStep.class.getDeclaredMethod("getDefaultAspectNames", OperationContext.class);
      method.setAccessible(true);
      method.invoke(loadIndicesStep, mockOperationContext);
      fail("Expected RuntimeException to be thrown");
    } catch (Exception e) {
      // The actual exception will be wrapped in InvocationTargetException
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testContainsKey() throws Exception {
    // Test the containsKey method
    var method = LoadIndicesStep.class.getDeclaredMethod("containsKey", Map.class, String.class);
    method.setAccessible(true);

    Map<String, Optional<String>> testMap = new HashMap<>();
    testMap.put("key1", Optional.of("value1"));
    testMap.put("key2", Optional.of("value2"));
    testMap.put("key3", Optional.empty());

    // Test with existing key that has a value
    boolean result1 = (Boolean) method.invoke(loadIndicesStep, testMap, "key1");
    assertTrue(result1);

    // Test with existing key that has empty optional
    boolean result2 = (Boolean) method.invoke(loadIndicesStep, testMap, "key3");
    assertTrue(!result2);

    // Test with non-existing key
    boolean result3 = (Boolean) method.invoke(loadIndicesStep, testMap, "key4");
    assertTrue(!result3);

    // Test with null map - this should throw NullPointerException
    try {
      method.invoke(loadIndicesStep, null, "key1");
      fail("Expected NullPointerException for null map");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof NullPointerException);
    }
  }

  @Test
  public void testGetArgs() throws Exception {
    // Test the getArgs method
    var method = LoadIndicesStep.class.getDeclaredMethod("getArgs", UpgradeContext.class);
    method.setAccessible(true);

    // Test with empty parsed args
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    when(mockUpgradeContext.parsedArgs()).thenReturn(parsedArgs);

    @SuppressWarnings("unchecked")
    LoadIndicesArgs result = (LoadIndicesArgs) method.invoke(loadIndicesStep, mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.batchSize, 10000); // Default batch size
    assertEquals(result.limit, Integer.MAX_VALUE); // Default limit
  }

  @Test
  public void testGetArgsWithAllArguments() throws Exception {
    var method = LoadIndicesStep.class.getDeclaredMethod("getArgs", UpgradeContext.class);
    method.setAccessible(true);

    // Test with all arguments provided
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put(LoadIndices.BATCH_SIZE_ARG_NAME, Optional.of("5000"));
    parsedArgs.put(LoadIndices.LIMIT_ARG_NAME, Optional.of("1000"));
    parsedArgs.put(LoadIndices.URN_LIKE_ARG_NAME, Optional.of("urn:li:dataset:%"));
    parsedArgs.put(LoadIndices.LE_PIT_EPOCH_MS_ARG_NAME, Optional.of("1640995200000"));
    parsedArgs.put(LoadIndices.GE_PIT_EPOCH_MS_ARG_NAME, Optional.of("1640908800000"));
    parsedArgs.put(LoadIndices.ASPECT_NAMES_ARG_NAME, Optional.of("container,ownership"));
    parsedArgs.put(LoadIndices.LAST_URN_ARG_NAME, Optional.of("urn:li:dataset:test"));

    when(mockUpgradeContext.parsedArgs()).thenReturn(parsedArgs);

    @SuppressWarnings("unchecked")
    LoadIndicesArgs result = (LoadIndicesArgs) method.invoke(loadIndicesStep, mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.batchSize, 5000);
    assertEquals(result.limit, 1000);
    assertEquals(result.urnLike, "urn:li:dataset:%");
    assertEquals(result.lePitEpochMs, Long.valueOf(1640995200000L));
    assertEquals(result.gePitEpochMs, Long.valueOf(1640908800000L));
    assertEquals(result.aspectNames, Arrays.asList("container", "ownership"));
    assertEquals(result.lastUrn, "urn:li:dataset:test");

    // Verify report messages were added
    verify(mockUpgradeReport, atLeastOnce()).addLine(anyString());
  }

  @Test
  public void testGetArgsWithLimitMaxValue() throws Exception {
    var method = LoadIndicesStep.class.getDeclaredMethod("getArgs", UpgradeContext.class);
    method.setAccessible(true);

    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put(LoadIndices.LIMIT_ARG_NAME, Optional.of(String.valueOf(Integer.MAX_VALUE)));
    when(mockUpgradeContext.parsedArgs()).thenReturn(parsedArgs);

    @SuppressWarnings("unchecked")
    LoadIndicesArgs result = (LoadIndicesArgs) method.invoke(loadIndicesStep, mockUpgradeContext);

    assertEquals(result.limit, Integer.MAX_VALUE);
    verify(mockUpgradeReport).addLine("limit is not applied (processing all matching records)");
  }

  @Test
  public void testGetArgsWithDefaultAspectNames() throws Exception {
    var method = LoadIndicesStep.class.getDeclaredMethod("getArgs", UpgradeContext.class);
    method.setAccessible(true);

    // Mock entity registry with searchable aspects
    Map<String, EntitySpec> entitySpecs = new HashMap<>();
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);

    when(mockEntitySpec.getAspectSpecs()).thenReturn(Arrays.asList(mockAspectSpec));
    when(mockAspectSpec.getSearchableFieldSpecs())
        .thenReturn(Arrays.asList()); // Empty list for simplicity
    when(mockAspectSpec.getName()).thenReturn("container");
    when(mockEntitySpec.getKeyAspectName()).thenReturn("datasetKey");

    entitySpecs.put("dataset", mockEntitySpec);
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    when(mockUpgradeContext.parsedArgs()).thenReturn(parsedArgs);

    @SuppressWarnings("unchecked")
    LoadIndicesArgs result = (LoadIndicesArgs) method.invoke(loadIndicesStep, mockUpgradeContext);

    assertNotNull(result.aspectNames);
    // Since we have no searchable aspects, the result should be empty
    assertTrue(result.aspectNames.isEmpty());
  }

  @Test
  public void testGetBatchSize() throws Exception {
    var method = LoadIndicesStep.class.getDeclaredMethod("getBatchSize", Map.class);
    method.setAccessible(true);

    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put(LoadIndices.BATCH_SIZE_ARG_NAME, Optional.of("5000"));

    int result = (Integer) method.invoke(loadIndicesStep, parsedArgs);
    assertEquals(result, 5000);

    // Test with empty args (should return default)
    Map<String, Optional<String>> emptyArgs = new HashMap<>();
    int defaultResult = (Integer) method.invoke(loadIndicesStep, emptyArgs);
    assertEquals(defaultResult, 10000);
  }

  @Test
  public void testGetLimit() throws Exception {
    var method = LoadIndicesStep.class.getDeclaredMethod("getLimit", Map.class);
    method.setAccessible(true);

    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put(LoadIndices.LIMIT_ARG_NAME, Optional.of("1000"));

    int result = (Integer) method.invoke(loadIndicesStep, parsedArgs);
    assertEquals(result, 1000);

    // Test with empty args (should return default)
    Map<String, Optional<String>> emptyArgs = new HashMap<>();
    int defaultResult = (Integer) method.invoke(loadIndicesStep, emptyArgs);
    assertEquals(defaultResult, Integer.MAX_VALUE);
  }

  @Test
  public void testGetInt() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod("getInt", Map.class, int.class, String.class);
    method.setAccessible(true);

    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("testKey", Optional.of("42"));

    int result = (Integer) method.invoke(loadIndicesStep, parsedArgs, 10, "testKey");
    assertEquals(result, 42);

    // Test with missing key (should return default)
    int defaultResult = (Integer) method.invoke(loadIndicesStep, parsedArgs, 10, "missingKey");
    assertEquals(defaultResult, 10);
  }

  @Test
  public void testConvertToRestoreIndicesArgs() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "convertToRestoreIndicesArgs", LoadIndicesArgs.class, int.class);
    method.setAccessible(true);

    LoadIndicesArgs args = new LoadIndicesArgs();
    args.aspectNames = Arrays.asList("container", "ownership");
    args.urnLike = "urn:li:dataset:%";
    args.gePitEpochMs = 1640908800000L;
    args.lePitEpochMs = 1640995200000L;
    args.lastUrn = "urn:li:dataset:test";

    RestoreIndicesArgs result = (RestoreIndicesArgs) method.invoke(loadIndicesStep, args, 1000);

    assertNotNull(result);
    assertEquals(result.aspectNames, Arrays.asList("container", "ownership"));
    assertEquals(result.urnLike, "urn:li:dataset:%");
    assertEquals(result.gePitEpochMs, Long.valueOf(1640908800000L));
    assertEquals(result.lePitEpochMs, Long.valueOf(1640995200000L));
    assertEquals(result.limit, 1000);
    assertTrue(result.urnBasedPagination);
    assertEquals(result.lastUrn, "urn:li:dataset:test");
  }

  @Test
  public void testConvertToRestoreIndicesArgsWithNulls() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "convertToRestoreIndicesArgs", LoadIndicesArgs.class, int.class);
    method.setAccessible(true);

    LoadIndicesArgs args = new LoadIndicesArgs();
    // All fields are null

    RestoreIndicesArgs result = (RestoreIndicesArgs) method.invoke(loadIndicesStep, args, 1000);

    assertNotNull(result);
    assertTrue(result.aspectNames == null || result.aspectNames.isEmpty());
    assertTrue(result.urnLike == null);
    assertEquals(result.gePitEpochMs, Long.valueOf(0L));
    assertTrue(result.lePitEpochMs > 0); // Should be current time
    assertEquals(result.limit, 1000);
    assertFalse(result.urnBasedPagination);
    assertTrue(result.lastUrn == null || result.lastUrn.isEmpty());
  }

  @Test
  public void testConvertToMetadataChangeLog() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "convertToMetadataChangeLog", OperationContext.class, EbeanAspectV2.class);
    method.setAccessible(true);

    // Create test aspect with proper key
    EbeanAspectV2 aspect = new EbeanAspectV2();
    EbeanAspectV2.PrimaryKey key =
        new EbeanAspectV2.PrimaryKey(
            "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleDataset,PROD)", "container", 0);
    aspect.setKey(key);
    aspect.setMetadata("{\"container\":{\"urn\":\"urn:li:container:test\"}}");
    aspect.setCreatedOn(Timestamp.from(Instant.now()));
    aspect.setCreatedBy("testUser");

    // Mock entity registry
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    when(mockEntitySpec.getAspectSpec("container")).thenReturn(mockAspectSpec);
    when(mockAspectSpec.getDataTemplateClass())
        .thenReturn((Class) com.linkedin.container.Container.class);
    when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(mockEntitySpec);

    MetadataChangeLog result =
        (MetadataChangeLog) method.invoke(loadIndicesStep, mockOperationContext, aspect);

    assertNotNull(result);
    assertEquals(result.getEntityType(), "dataset");
    assertEquals(result.getChangeType(), ChangeType.RESTATE);
    assertEquals(result.getAspectName(), "container");
  }

  @Test(expectedExceptions = Exception.class)
  public void testConvertToMetadataChangeLogWithInvalidUrn() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "convertToMetadataChangeLog", OperationContext.class, EbeanAspectV2.class);
    method.setAccessible(true);

    // Create test aspect with invalid URN
    EbeanAspectV2 aspect = new EbeanAspectV2();
    aspect.setUrn("invalid-urn");
    aspect.setAspect("container");
    aspect.setVersion(0);
    aspect.setMetadata("{}");
    aspect.setCreatedOn(Timestamp.from(Instant.now()));
    aspect.setCreatedBy("testUser");

    method.invoke(loadIndicesStep, mockOperationContext, aspect);
  }

  @Test
  public void testWriteBatchWithRetrySuccess() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "writeBatchWithRetry",
            OperationContext.class,
            List.class,
            LoadIndicesResult.class,
            java.util.function.Function.class);
    method.setAccessible(true);

    List<MetadataChangeLog> batch = new ArrayList<>();
    batch.add(mock(MetadataChangeLog.class));

    LoadIndicesResult result = new LoadIndicesResult();

    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());

    method.invoke(
        loadIndicesStep,
        mockOperationContext,
        batch,
        result,
        (java.util.function.Function<String, Void>) msg -> null);

    verify(mockUpdateIndicesService, times(1)).handleChangeEvents(any(), any());
    assertEquals(result.ignored, 0);
  }

  @Test
  public void testWriteBatchWithRetryFailureAndSplit() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "writeBatchWithRetry",
            OperationContext.class,
            List.class,
            LoadIndicesResult.class,
            java.util.function.Function.class);
    method.setAccessible(true);

    List<MetadataChangeLog> batch = new ArrayList<>();
    batch.add(mock(MetadataChangeLog.class));
    batch.add(mock(MetadataChangeLog.class));
    batch.add(mock(MetadataChangeLog.class));
    batch.add(mock(MetadataChangeLog.class));

    LoadIndicesResult result = new LoadIndicesResult();

    // First call fails, second succeeds
    doThrow(new RuntimeException("First attempt fails"))
        .doNothing()
        .when(mockUpdateIndicesService)
        .handleChangeEvents(any(), any());

    method.invoke(
        loadIndicesStep,
        mockOperationContext,
        batch,
        result,
        (java.util.function.Function<String, Void>) msg -> null);

    // Should have been called multiple times due to retry and split
    verify(mockUpdateIndicesService, atLeast(2)).handleChangeEvents(any(), any());
  }

  @Test
  public void testWriteBatchWithRetryMaxRetriesExceeded() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "writeBatchWithRetry",
            OperationContext.class,
            List.class,
            LoadIndicesResult.class,
            java.util.function.Function.class);
    method.setAccessible(true);

    List<MetadataChangeLog> batch = new ArrayList<>();
    batch.add(mock(MetadataChangeLog.class));
    batch.add(mock(MetadataChangeLog.class));
    batch.add(mock(MetadataChangeLog.class));
    batch.add(mock(MetadataChangeLog.class));

    LoadIndicesResult result = new LoadIndicesResult();

    // Always fail
    doThrow(new RuntimeException("Always fails"))
        .when(mockUpdateIndicesService)
        .handleChangeEvents(any(), any());

    method.invoke(
        loadIndicesStep,
        mockOperationContext,
        batch,
        result,
        (java.util.function.Function<String, Void>) msg -> null);

    // Should have been called multiple times due to retries and batch splitting
    verify(mockUpdateIndicesService, atLeast(4)).handleChangeEvents(any(), any());
    assertEquals(result.ignored, 4); // All items should be marked as ignored after max retries
  }

  @Test
  public void testWriteBatchWithRetrySuccessAfterRetries() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "writeBatchWithRetry",
            OperationContext.class,
            List.class,
            LoadIndicesResult.class,
            java.util.function.Function.class);
    method.setAccessible(true);

    List<MetadataChangeLog> batch = new ArrayList<>();
    batch.add(mock(MetadataChangeLog.class));
    batch.add(mock(MetadataChangeLog.class));

    LoadIndicesResult result = new LoadIndicesResult();

    // Fail first 2 times, succeed on 3rd attempt
    doThrow(new RuntimeException("Fail attempt 1"))
        .doThrow(new RuntimeException("Fail attempt 2"))
        .doNothing()
        .when(mockUpdateIndicesService)
        .handleChangeEvents(any(), any());

    method.invoke(
        loadIndicesStep,
        mockOperationContext,
        batch,
        result,
        (java.util.function.Function<String, Void>) msg -> null);

    // Should have been called 3 times (2 failures + 1 success)
    verify(mockUpdateIndicesService, times(3)).handleChangeEvents(any(), any());
    assertEquals(result.ignored, 1); // First half of split batch gets ignored, second half succeeds
  }

  @Test
  public void testWriteBatchWithRetryMaxRetriesExceededWithSmallBatch() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "writeBatchWithRetry",
            OperationContext.class,
            List.class,
            LoadIndicesResult.class,
            java.util.function.Function.class);
    method.setAccessible(true);

    List<MetadataChangeLog> batch = new ArrayList<>();
    batch.add(mock(MetadataChangeLog.class));
    batch.add(mock(MetadataChangeLog.class));

    LoadIndicesResult result = new LoadIndicesResult();

    // Always fail - should exceed max retries (3) and split batch
    doThrow(new RuntimeException("Always fails"))
        .when(mockUpdateIndicesService)
        .handleChangeEvents(any(), any());

    method.invoke(
        loadIndicesStep,
        mockOperationContext,
        batch,
        result,
        (java.util.function.Function<String, Void>) msg -> null);

    // Should have been called 3 times: initial attempt + 2 retries before exceeding max retries
    verify(mockUpdateIndicesService, times(3)).handleChangeEvents(any(), any());
    assertEquals(result.ignored, 2); // Both items should be marked as ignored after max retries
  }

  @Test
  public void testWriteBatchWithRetryBatchSplitting() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "writeBatchWithRetry",
            OperationContext.class,
            List.class,
            LoadIndicesResult.class,
            java.util.function.Function.class);
    method.setAccessible(true);

    List<MetadataChangeLog> batch = new ArrayList<>();
    batch.add(mock(MetadataChangeLog.class));
    batch.add(mock(MetadataChangeLog.class));
    batch.add(mock(MetadataChangeLog.class));
    batch.add(mock(MetadataChangeLog.class));

    LoadIndicesResult result = new LoadIndicesResult();

    // Fail on full batch, succeed on split batches
    doThrow(new RuntimeException("Full batch fails"))
        .doNothing() // First half succeeds
        .doNothing() // Second half succeeds
        .when(mockUpdateIndicesService)
        .handleChangeEvents(any(), any());

    method.invoke(
        loadIndicesStep,
        mockOperationContext,
        batch,
        result,
        (java.util.function.Function<String, Void>) msg -> null);

    // Should have been called 3 times: 1 full batch failure + 2 successful split batches
    verify(mockUpdateIndicesService, times(3)).handleChangeEvents(any(), any());
    assertEquals(result.ignored, 0); // No items should be ignored since splits succeeded
  }

  @Test
  public void testWriteBatchWithRetryFirstHalfFailure() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "writeBatchWithRetry",
            OperationContext.class,
            List.class,
            LoadIndicesResult.class,
            java.util.function.Function.class);
    method.setAccessible(true);

    List<MetadataChangeLog> batch = new ArrayList<>();
    batch.add(mock(MetadataChangeLog.class));
    batch.add(mock(MetadataChangeLog.class));
    batch.add(mock(MetadataChangeLog.class));
    batch.add(mock(MetadataChangeLog.class));

    LoadIndicesResult result = new LoadIndicesResult();

    // Fail on full batch and first half, succeed on second half
    doThrow(new RuntimeException("Full batch fails"))
        .doThrow(new RuntimeException("First half fails"))
        .doNothing() // Second half succeeds
        .when(mockUpdateIndicesService)
        .handleChangeEvents(any(), any());

    method.invoke(
        loadIndicesStep,
        mockOperationContext,
        batch,
        result,
        (java.util.function.Function<String, Void>) msg -> null);

    // Should have been called 3 times: 1 full batch failure + 1 first half failure + 1 second half
    // success
    verify(mockUpdateIndicesService, times(3)).handleChangeEvents(any(), any());
    assertEquals(result.ignored, 2); // First half (2 items) should be ignored
  }

  @Test
  public void testWriteBatchWithRetrySingleItemFailure() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "writeBatchWithRetry",
            OperationContext.class,
            List.class,
            LoadIndicesResult.class,
            java.util.function.Function.class);
    method.setAccessible(true);

    List<MetadataChangeLog> batch = new ArrayList<>();
    batch.add(mock(MetadataChangeLog.class));

    LoadIndicesResult result = new LoadIndicesResult();

    // Always fail
    doThrow(new RuntimeException("Always fails"))
        .when(mockUpdateIndicesService)
        .handleChangeEvents(any(), any());

    method.invoke(
        loadIndicesStep,
        mockOperationContext,
        batch,
        result,
        (java.util.function.Function<String, Void>) msg -> null);

    assertEquals(
        result.ignored,
        1); // Single item should be marked as ignored when it can't be split further
  }

  @Test
  public void testProcessAllDataDirectlyWithConversionErrors() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "processAllDataDirectly",
            OperationContext.class,
            LoadIndicesArgs.class,
            java.util.function.Function.class);
    method.setAccessible(true);

    // Mock the updateIndicesService to not throw exceptions
    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());

    // Create test args with limit
    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = 10;
    args.aspectNames = java.util.List.of("container", "ownership");

    // Add a test row with invalid metadata to cause conversion error
    insertTestRow(
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,InvalidDataset,PROD)",
        "container",
        0,
        Instant.now(),
        "testUser");

    // Call the method
    Object result =
        method.invoke(
            loadIndicesStep,
            mockOperationContext,
            args,
            (java.util.function.Function<String, Void>) msg -> null);

    assertNotNull(result);
    verify(mockUpdateIndicesService, atLeastOnce()).flush();
  }

  @Test
  public void testProcessAllDataDirectlyWithLimit() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "processAllDataDirectly",
            OperationContext.class,
            LoadIndicesArgs.class,
            java.util.function.Function.class);
    method.setAccessible(true);

    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());

    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = 1; // Very small limit
    args.aspectNames = java.util.List.of("container");

    Object result =
        method.invoke(
            loadIndicesStep,
            mockOperationContext,
            args,
            (java.util.function.Function<String, Void>) msg -> null);

    assertNotNull(result);
    verify(mockUpdateIndicesService, atLeastOnce()).flush();
  }

  @Test
  public void testProcessAllDataDirectlyWithUrnLike() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "processAllDataDirectly",
            OperationContext.class,
            LoadIndicesArgs.class,
            java.util.function.Function.class);
    method.setAccessible(true);

    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());

    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = 10;
    args.urnLike = "urn:li:dataset:%";
    args.aspectNames = java.util.List.of("container");

    Object result =
        method.invoke(
            loadIndicesStep,
            mockOperationContext,
            args,
            (java.util.function.Function<String, Void>) msg -> null);

    assertNotNull(result);
    verify(mockUpdateIndicesService, atLeastOnce()).flush();
  }

  @Test
  public void testProcessAllDataDirectlyWithTimeFilters() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "processAllDataDirectly",
            OperationContext.class,
            LoadIndicesArgs.class,
            java.util.function.Function.class);
    method.setAccessible(true);

    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());

    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = 10;
    args.gePitEpochMs = Instant.now().minusSeconds(3600).toEpochMilli(); // 1 hour ago
    args.lePitEpochMs = Instant.now().toEpochMilli(); // now
    args.aspectNames = java.util.List.of("container");

    Object result =
        method.invoke(
            loadIndicesStep,
            mockOperationContext,
            args,
            (java.util.function.Function<String, Void>) msg -> null);

    assertNotNull(result);
    verify(mockUpdateIndicesService, atLeastOnce()).flush();
  }

  @Test
  public void testProcessAllDataDirectlyWithLastUrn() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "processAllDataDirectly",
            OperationContext.class,
            LoadIndicesArgs.class,
            java.util.function.Function.class);
    method.setAccessible(true);

    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());

    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = 10;
    args.lastUrn = "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleDataset1,PROD)";
    args.aspectNames = java.util.List.of("container");

    Object result =
        method.invoke(
            loadIndicesStep,
            mockOperationContext,
            args,
            (java.util.function.Function<String, Void>) msg -> null);

    assertNotNull(result);
    verify(mockUpdateIndicesService, atLeastOnce()).flush();
  }

  @Test
  public void testProcessAllDataDirectlyWithEmptyBatch() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "processAllDataDirectly",
            OperationContext.class,
            LoadIndicesArgs.class,
            java.util.function.Function.class);
    method.setAccessible(true);

    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());

    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = 10;
    args.aspectNames = java.util.List.of("nonexistent"); // No matching aspects

    Object result =
        method.invoke(
            loadIndicesStep,
            mockOperationContext,
            args,
            (java.util.function.Function<String, Void>) msg -> null);

    assertNotNull(result);
    verify(mockUpdateIndicesService, atLeastOnce()).flush();
  }

  @Test
  public void testProcessAllDataDirectlyWithFinalFlushFailure() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "processAllDataDirectly",
            OperationContext.class,
            LoadIndicesArgs.class,
            java.util.function.Function.class);
    method.setAccessible(true);

    // Mock successful batch processing but fail on final flush
    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());
    doThrow(new RuntimeException("Flush failed")).when(mockUpdateIndicesService).flush();

    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = 10;
    args.aspectNames = java.util.List.of("container");

    // Execute - should not throw exception despite flush failure
    Object result =
        method.invoke(
            loadIndicesStep,
            mockOperationContext,
            args,
            (java.util.function.Function<String, Void>) msg -> null);

    // Verify flush was called and failed gracefully
    verify(mockUpdateIndicesService).flush();
    assertNotNull(result);
  }

  @Test
  public void testGetDefaultAspectNamesWithSearchableAspects() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod("getDefaultAspectNames", OperationContext.class);
    method.setAccessible(true);

    // Create mock entity specs with searchable aspects
    com.linkedin.metadata.models.EntitySpec mockEntitySpec1 =
        mock(com.linkedin.metadata.models.EntitySpec.class);
    com.linkedin.metadata.models.EntitySpec mockEntitySpec2 =
        mock(com.linkedin.metadata.models.EntitySpec.class);

    // Create mock aspect specs
    com.linkedin.metadata.models.AspectSpec mockAspectSpec1 =
        mock(com.linkedin.metadata.models.AspectSpec.class);
    com.linkedin.metadata.models.AspectSpec mockAspectSpec2 =
        mock(com.linkedin.metadata.models.AspectSpec.class);
    com.linkedin.metadata.models.AspectSpec mockKeyAspectSpec =
        mock(com.linkedin.metadata.models.AspectSpec.class);

    // Create mock searchable field specs
    com.linkedin.metadata.models.SearchableFieldSpec mockSearchableFieldSpec =
        mock(com.linkedin.metadata.models.SearchableFieldSpec.class);

    // Setup entity registry mock
    when(mockEntityRegistry.getEntitySpecs())
        .thenReturn(
            java.util.Map.of(
                "dataset", mockEntitySpec1,
                "chart", mockEntitySpec2));

    when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(mockEntitySpec1);
    when(mockEntityRegistry.getEntitySpec("chart")).thenReturn(mockEntitySpec2);

    // Setup entity specs
    when(mockEntitySpec1.getAspectSpecs())
        .thenReturn(java.util.List.of(mockAspectSpec1, mockKeyAspectSpec));
    when(mockEntitySpec2.getAspectSpecs()).thenReturn(java.util.List.of(mockAspectSpec2));

    // Setup aspect specs - first has searchable fields, second doesn't
    when(mockAspectSpec1.getName()).thenReturn("datasetProperties");
    when(mockAspectSpec1.getSearchableFieldSpecs())
        .thenReturn(java.util.List.of(mockSearchableFieldSpec));

    when(mockAspectSpec2.getName()).thenReturn("chartInfo");
    when(mockAspectSpec2.getSearchableFieldSpecs()).thenReturn(java.util.List.of());

    when(mockKeyAspectSpec.getName()).thenReturn("datasetKey");
    when(mockKeyAspectSpec.getSearchableFieldSpecs()).thenReturn(java.util.List.of());

    // Setup key aspect names
    when(mockEntitySpec1.getKeyAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec2.getKeyAspectName()).thenReturn("chartKey");

    // Execute method
    @SuppressWarnings("unchecked")
    java.util.Set<String> result =
        (java.util.Set<String>) method.invoke(loadIndicesStep, mockOperationContext);

    // Verify results
    assertNotNull(result);
    assertTrue(result.contains("datasetProperties")); // Has searchable fields
    assertTrue(result.contains("datasetKey")); // Key aspect included
    assertFalse(result.contains("chartInfo")); // No searchable fields, not included
    assertFalse(result.contains("chartKey")); // Key aspect not included since no searchable aspects
  }

  @Test
  public void testGetDefaultAspectNamesWithEntityProcessingError() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod("getDefaultAspectNames", OperationContext.class);
    method.setAccessible(true);

    // Create mock entity specs
    com.linkedin.metadata.models.EntitySpec mockEntitySpec1 =
        mock(com.linkedin.metadata.models.EntitySpec.class);
    com.linkedin.metadata.models.EntitySpec mockEntitySpec2 =
        mock(com.linkedin.metadata.models.EntitySpec.class);

    // Setup entity registry mock
    when(mockEntityRegistry.getEntitySpecs())
        .thenReturn(
            java.util.Map.of(
                "dataset", mockEntitySpec1,
                "chart", mockEntitySpec2));

    // First entity works fine
    when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(mockEntitySpec1);
    com.linkedin.metadata.models.AspectSpec mockAspectSpec1 =
        mock(com.linkedin.metadata.models.AspectSpec.class);
    when(mockEntitySpec1.getAspectSpecs()).thenReturn(java.util.List.of(mockAspectSpec1));
    when(mockAspectSpec1.getName()).thenReturn("datasetProperties");
    when(mockAspectSpec1.getSearchableFieldSpecs())
        .thenReturn(
            java.util.List.of(mock(com.linkedin.metadata.models.SearchableFieldSpec.class)));
    when(mockEntitySpec1.getKeyAspectName()).thenReturn("datasetKey");

    // Second entity throws exception
    when(mockEntityRegistry.getEntitySpec("chart"))
        .thenThrow(new RuntimeException("Entity processing failed"));

    // Execute method - should not throw exception
    @SuppressWarnings("unchecked")
    java.util.Set<String> result =
        (java.util.Set<String>) method.invoke(loadIndicesStep, mockOperationContext);

    // Verify results - should still include aspects from first entity
    assertNotNull(result);
    assertTrue(result.contains("datasetProperties"));
    assertTrue(result.contains("datasetKey"));
  }

  @Test
  public void testGetDefaultAspectNamesWithNoSearchableAspects() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod("getDefaultAspectNames", OperationContext.class);
    method.setAccessible(true);

    // Create mock entity spec with no searchable aspects
    com.linkedin.metadata.models.EntitySpec mockEntitySpec =
        mock(com.linkedin.metadata.models.EntitySpec.class);
    com.linkedin.metadata.models.AspectSpec mockAspectSpec =
        mock(com.linkedin.metadata.models.AspectSpec.class);

    // Setup entity registry mock
    when(mockEntityRegistry.getEntitySpecs())
        .thenReturn(java.util.Map.of("dataset", mockEntitySpec));
    when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(mockEntitySpec);

    // Setup aspect spec with no searchable fields
    when(mockEntitySpec.getAspectSpecs()).thenReturn(java.util.List.of(mockAspectSpec));
    when(mockAspectSpec.getName()).thenReturn("datasetProperties");
    when(mockAspectSpec.getSearchableFieldSpecs()).thenReturn(java.util.List.of());
    when(mockEntitySpec.getKeyAspectName()).thenReturn("datasetKey");

    // Execute method
    @SuppressWarnings("unchecked")
    java.util.Set<String> result =
        (java.util.Set<String>) method.invoke(loadIndicesStep, mockOperationContext);

    // Verify results - should be empty since no searchable aspects
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testProcessAllDataDirectlyWithNonEmptyBatchButAllConversionErrors() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "processAllDataDirectly",
            OperationContext.class,
            LoadIndicesArgs.class,
            java.util.function.Function.class);
    method.setAccessible(true);

    // Mock the updateIndicesService to not throw exceptions
    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());

    // Create test args with small batch size to ensure we hit the batch processing logic
    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 2; // Small batch size to trigger batch processing
    args.limit = 10;
    args.aspectNames = java.util.List.of("container", "ownership");

    // Add test rows with invalid metadata to cause conversion errors
    // This will create a batch that is not empty but all aspects fail conversion
    insertTestRow(
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,InvalidDataset1,PROD)",
        "container",
        0,
        Instant.now(),
        "testUser");

    insertTestRow(
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,InvalidDataset2,PROD)",
        "ownership",
        0,
        Instant.now(),
        "testUser");

    // Add a valid row to ensure we have some data to process
    insertTestRow(
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,ValidDataset,PROD)",
        "container",
        0,
        Instant.now(),
        "testUser");

    // Call the method
    Object result =
        method.invoke(
            loadIndicesStep,
            mockOperationContext,
            args,
            (java.util.function.Function<String, Void>) msg -> null);

    assertNotNull(result);
    verify(mockUpdateIndicesService, atLeastOnce()).flush();

    // Verify that the method completed successfully even with conversion errors
    LoadIndicesResult loadResult = (LoadIndicesResult) result;
    assertTrue(loadResult.ignored > 0); // Some aspects should be ignored due to conversion errors
  }

  @Test
  public void testProcessAllDataDirectlyWithBatchProcessing() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "processAllDataDirectly",
            OperationContext.class,
            LoadIndicesArgs.class,
            java.util.function.Function.class);
    method.setAccessible(true);

    // Mock the updateIndicesService to not throw exceptions
    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());

    // Create test args with small batch size to ensure batch processing is triggered
    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 2; // Small batch size to trigger batch processing
    args.limit = 5;
    args.aspectNames = java.util.List.of("container");

    // Add test rows to ensure we have data to process
    insertTestRow(
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,TestDataset1,PROD)",
        "container",
        0,
        Instant.now(),
        "testUser");

    insertTestRow(
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,TestDataset2,PROD)",
        "container",
        0,
        Instant.now(),
        "testUser");

    // Call the method
    Object result =
        method.invoke(
            loadIndicesStep,
            mockOperationContext,
            args,
            (java.util.function.Function<String, Void>) msg -> null);

    assertNotNull(result);
    verify(mockUpdateIndicesService, atLeastOnce()).flush();

    // Verify that the method completed successfully
    LoadIndicesResult loadResult = (LoadIndicesResult) result;
    assertNotNull(loadResult);
    // The key test is that the method doesn't throw an exception when processing batches
    // This tests the !mclBatch.isEmpty() logic path
  }

  @Test
  public void testProcessAllDataDirectlyWithFlushFailure() throws Exception {
    var method =
        LoadIndicesStep.class.getDeclaredMethod(
            "processAllDataDirectly",
            OperationContext.class,
            LoadIndicesArgs.class,
            java.util.function.Function.class);
    method.setAccessible(true);

    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());
    doThrow(new RuntimeException("Flush failed")).when(mockUpdateIndicesService).flush();

    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = 10;
    args.aspectNames = java.util.List.of("container");

    // Should not throw exception even if flush fails
    Object result =
        method.invoke(
            loadIndicesStep,
            mockOperationContext,
            args,
            (java.util.function.Function<String, Void>) msg -> null);

    assertNotNull(result);
    verify(mockUpdateIndicesService, atLeastOnce()).flush();
  }
}
