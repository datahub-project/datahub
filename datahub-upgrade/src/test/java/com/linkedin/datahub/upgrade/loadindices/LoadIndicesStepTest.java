package com.linkedin.datahub.upgrade.loadindices;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PassThroughScopedTransactionFactory;
import com.linkedin.metadata.entity.ebean.PlainAspectTableResolver;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.entity.storage.PrimaryStorageResolver;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.BackfillHeaders;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.ActorContext;
import io.datahubproject.metadata.context.OperationContext;
import io.ebean.Database;
import io.ebean.annotation.TxIsolation;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LoadIndicesStepTest {

  private LoadIndicesStep loadIndicesStep;
  private AspectIndexLoader loader;
  private Database database;
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

    loader = new AspectIndexLoader(aspectDao(database), mockUpdateIndicesService);
    loadIndicesStep = new LoadIndicesStep(loader, mockUpdateIndicesService, mockIndexManager);

    when(mockUpgradeContext.report()).thenReturn(mockUpgradeReport);
    when(mockUpgradeContext.opContext()).thenReturn(mockOperationContext);
    when(mockOperationContext.getEntityRegistry()).thenReturn(mockEntityRegistry);
    when(mockOperationContext.getActorContext()).thenReturn(mockActorContext);
    EntitySpec defaultEntitySpec = mock(EntitySpec.class);
    AspectSpec containerAspectSpec = mock(AspectSpec.class);
    AspectSpec ownershipAspectSpec = mock(AspectSpec.class);
    when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(defaultEntitySpec);
    when(defaultEntitySpec.getAspectSpec("container")).thenReturn(containerAspectSpec);
    when(defaultEntitySpec.getAspectSpec("ownership")).thenReturn(ownershipAspectSpec);
    when(containerAspectSpec.getDataTemplateClass())
        .thenReturn((Class) com.linkedin.container.Container.class);
    when(ownershipAspectSpec.getDataTemplateClass())
        .thenReturn((Class) com.linkedin.common.Ownership.class);

    // Mock authentication context
    com.datahub.authentication.Authentication mockAuth =
        mock(com.datahub.authentication.Authentication.class);
    com.datahub.authentication.Actor mockActor = mock(com.datahub.authentication.Actor.class);
    when(mockActorContext.getAuthentication()).thenReturn(mockAuth);
    when(mockAuth.getActor()).thenReturn(mockActor);
    when(mockActor.toUrnStr()).thenReturn("urn:li:corpuser:testUser");
  }

  /** The loader reads through a real DAO over the test database, as the Spring bean does. */
  private static EbeanAspectDao aspectDao(Database database) {
    EbeanAspectDao aspectDao =
        new EbeanAspectDao(
            PrimaryStorageResolver.forSingleEbeanDatabase(database),
            EbeanConfiguration.testDefault,
            null,
            Collections.emptyList(),
            null,
            new PlainAspectTableResolver(),
            new PassThroughScopedTransactionFactory(database));
    aspectDao.setConnectionValidated(true);
    return aspectDao;
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
    aspectV2.setCreatedOn(Timestamp.from(createdTime.truncatedTo(ChronoUnit.MILLIS)));
    aspectV2.setCreatedBy(createdBy);
    database.save(aspectV2);
  }

  private void insertTestRowWithValidData(
      String urn, String aspect, int version, Instant createdTime, String createdBy) {
    EbeanAspectV2 aspectV2 = new EbeanAspectV2();
    aspectV2.setUrn(urn);
    aspectV2.setAspect(aspect);
    aspectV2.setVersion(version);
    // Provide valid container aspect JSON data
    aspectV2.setMetadata("{\"container\":{\"urn\":\"" + urn + "\"}}");
    aspectV2.setCreatedOn(Timestamp.from(createdTime.truncatedTo(ChronoUnit.MILLIS)));
    aspectV2.setCreatedBy(createdBy);
    database.save(aspectV2);
  }

  private static final String PAGED_URN_PREFIX = "urn:li:dataset:(urn:li:dataPlatform:paged,";

  private void seedPagedEntities(int count) {
    for (int i = 0; i < count; i++) {
      String urn = PAGED_URN_PREFIX + "entity" + i + ",PROD)";
      insertTestRow(urn, "container", 0, Instant.now(), "testUser");
      insertTestRow(urn, "ownership", 0, Instant.now(), "testUser");
    }
  }

  private List<List<MetadataChangeLog>> loadPaged(LoadIndicesArgs args) {
    List<List<MetadataChangeLog>> batches = new ArrayList<>();
    loader.load(
        mockOperationContext,
        args,
        msg -> null,
        null,
        (opContext, batch) -> batches.add(new ArrayList<>(batch)));
    return batches;
  }

  private static boolean stampedComplete(MetadataChangeLog event) {
    return event.getHeaders() != null
        && Boolean.parseBoolean(
            event.getHeaders().get(BackfillHeaders.COMPLETE_ENTITY_HEADER_NAME));
  }

  @Test
  public void testPagesHoldWholeEntitiesAndStampThemComplete() {
    seedPagedEntities(3);
    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 3;
    args.limit = Integer.MAX_VALUE;
    args.urnLike = PAGED_URN_PREFIX + "%";

    List<List<MetadataChangeLog>> batches = loadPaged(args);

    // A three-row page cuts the second entity in half; the loader completes it, so the first page
    // holds two whole entities and the second page the third.
    assertEquals(batches.size(), 2);
    assertEquals(batches.get(0).size(), 4);
    assertEquals(batches.get(1).size(), 2);
    for (List<MetadataChangeLog> batch : batches) {
      Map<String, Long> aspectsPerUrn =
          batch.stream()
              .collect(
                  Collectors.groupingBy(e -> e.getEntityUrn().toString(), Collectors.counting()));
      assertTrue(
          aspectsPerUrn.values().stream().allMatch(n -> n == 2L),
          "every entity in a page is whole: " + aspectsPerUrn);
      assertTrue(batch.stream().allMatch(LoadIndicesStepTest::stampedComplete));
    }
  }

  @Test
  public void testResumeFromACheckpointReReadsTheCheckpointedEntityWhole() {
    seedPagedEntities(3);
    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = Integer.MAX_VALUE;
    args.urnLike = PAGED_URN_PREFIX + "%";
    // A checkpoint names the last entity written; the resume starts at its first row so a
    // partially written entity is rewritten whole, and nothing before it is read again.
    args.lastUrn = PAGED_URN_PREFIX + "entity1,PROD)";

    List<List<MetadataChangeLog>> batches = loadPaged(args);

    assertEquals(batches.size(), 1);
    List<String> urns =
        batches.get(0).stream()
            .map(e -> e.getEntityUrn().toString())
            .distinct()
            .collect(Collectors.toList());
    assertEquals(
        urns, List.of(PAGED_URN_PREFIX + "entity1,PROD)", PAGED_URN_PREFIX + "entity2,PROD)"));
    assertEquals(batches.get(0).size(), 4);
    assertTrue(batches.get(0).stream().allMatch(LoadIndicesStepTest::stampedComplete));
  }

  @Test
  public void testAspectFilterLeavesEntitiesUnstamped() {
    seedPagedEntities(2);
    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = Integer.MAX_VALUE;
    args.urnLike = PAGED_URN_PREFIX + "%";
    args.aspectNames = List.of("container");

    List<List<MetadataChangeLog>> batches = loadPaged(args);

    assertEquals(batches.size(), 1);
    assertEquals(batches.get(0).size(), 2);
    assertTrue(batches.get(0).stream().noneMatch(LoadIndicesStepTest::stampedComplete));
  }

  @Test
  public void testRowLimitDoesNotCompleteTheTruncatedEntity() {
    seedPagedEntities(2);
    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = 1;
    args.urnLike = PAGED_URN_PREFIX + "%";

    List<List<MetadataChangeLog>> batches = loadPaged(args);

    // The row cap, not the page size, cut this page: the cap is honoured and the half-read entity
    // is left for hydration rather than stamped complete.
    assertEquals(batches.size(), 1);
    assertEquals(batches.get(0).size(), 1);
    assertFalse(stampedComplete(batches.get(0).get(0)));
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
    when(mockUpgradeContext.parsedArgs())
        .thenReturn(
            Map.of(
                LoadIndices.LIMIT_ARG_NAME,
                Optional.of("0"),
                LoadIndices.ASPECT_NAMES_ARG_NAME,
                Optional.of("nonexistent")));

    // Mock successful index manager operations
    doNothing().when(mockIndexManager).optimizeForBulkOperations(any(OperationContext.class));
    when(mockIndexManager.isSettingsOptimized()).thenReturn(true);
    doNothing().when(mockIndexManager).restoreFromConfiguration(any(OperationContext.class));

    // Execute the step
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "LoadIndicesStep");
    assertTrue(result.result() == DataHubUpgradeState.SUCCEEDED);

    // Verify index manager was called
    verify(mockIndexManager, times(1)).optimizeForBulkOperations(any(OperationContext.class));
    verify(mockIndexManager, times(1)).restoreFromConfiguration(any(OperationContext.class));
  }

  @Test
  public void testExecutableFailsOnFlushException() {
    doThrow(new RuntimeException("flush failed")).when(mockUpdateIndicesService).flushAndWait();

    assertExecutableFailsAfterDrain();
  }

  @Test
  public void testExecutableDrainsAfterExhaustedWriteFailure() throws IOException {
    when(mockUpgradeContext.parsedArgs())
        .thenReturn(
            Map.of(
                LoadIndices.LIMIT_ARG_NAME,
                Optional.of("1"),
                LoadIndices.ASPECT_NAMES_ARG_NAME,
                Optional.of("container")));
    doThrow(new RuntimeException("write failed"))
        .when(mockUpdateIndicesService)
        .handleChangeEvents(any(), any());
    when(mockIndexManager.isSettingsOptimized()).thenReturn(true);

    UpgradeStepResult result = loadIndicesStep.executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(mockUpdateIndicesService, times(4)).handleChangeEvents(any(), any());
    var ordered = inOrder(mockUpdateIndicesService, mockIndexManager);
    ordered.verify(mockUpdateIndicesService).flushAndWait();
    ordered.verify(mockIndexManager).restoreFromConfiguration(any());
  }

  private void assertExecutableFailsAfterDrain() {
    when(mockUpgradeContext.parsedArgs())
        .thenReturn(
            Map.of(
                LoadIndices.LIMIT_ARG_NAME,
                Optional.of("0"),
                LoadIndices.ASPECT_NAMES_ARG_NAME,
                Optional.of("nonexistent")));

    UpgradeStepResult result = loadIndicesStep.executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(mockUpdateIndicesService).flushAndWait();
  }

  @Test
  public void testExecutableWithIndexManagerDisableFailure() throws IOException {
    var executable = loadIndicesStep.executable();
    assertNotNull(executable);

    // Mock index manager disable failure
    doThrow(new IOException("Failed to optimize settings"))
        .when(mockIndexManager)
        .optimizeForBulkOperations(any(OperationContext.class));

    // Execute the step
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "LoadIndicesStep");
    assertTrue(result.result() == DataHubUpgradeState.FAILED);

    // Verify index manager was called
    verify(mockIndexManager, times(1)).optimizeForBulkOperations(any(OperationContext.class));
    verify(mockIndexManager, never()).restoreFromConfiguration(any(OperationContext.class));
  }

  @Test
  public void testExecutableWithIndexManagerRestoreFailure() throws IOException {
    var executable = loadIndicesStep.executable();
    assertNotNull(executable);
    when(mockUpgradeContext.parsedArgs())
        .thenReturn(
            Map.of(
                LoadIndices.LIMIT_ARG_NAME,
                Optional.of("0"),
                LoadIndices.ASPECT_NAMES_ARG_NAME,
                Optional.of("nonexistent")));

    // Mock successful optimization but failed restore
    doNothing().when(mockIndexManager).optimizeForBulkOperations(any(OperationContext.class));
    when(mockIndexManager.isSettingsOptimized()).thenReturn(true);
    doThrow(new IOException("Failed to restore settings"))
        .when(mockIndexManager)
        .restoreFromConfiguration(any(OperationContext.class));

    // Execute the step
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "LoadIndicesStep");
    // The step should still succeed even if restore fails, as it logs the error but doesn't fail
    // the step
    assertTrue(result.result() == DataHubUpgradeState.SUCCEEDED);

    // Verify index manager was called
    verify(mockIndexManager, times(1)).optimizeForBulkOperations(any(OperationContext.class));
    verify(mockIndexManager, times(1)).restoreFromConfiguration(any(OperationContext.class));
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

    // This should complete successfully with our test data
    loader.load(
        mockOperationContext,
        args,
        msg -> null,
        null,
        mockUpdateIndicesService::handleChangeEvents);

    // The loader drains through the service once the run is over.
    verify(mockUpdateIndicesService, atLeastOnce()).flushAndWait();
  }

  @Test
  public void testProcessAllDataDirectlySkipsConversionErrorsWhenEnabled() throws Exception {
    // "status" has no stubbed AspectSpec, so convertToMetadataChangeLog throws when it tries to
    // resolve the data-template class - a stand-in for a corrupt/unconvertible aspect.
    insertTestRow(
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleBadAspect,PROD)",
        "status",
        0,
        Instant.now(),
        "testUser");
    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());

    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = 10;
    args.aspectNames = java.util.List.of("status");
    args.skipConversionErrors = true;

    LoadIndicesResult result =
        loader.load(
            mockOperationContext,
            args,
            msg -> null,
            null,
            mockUpdateIndicesService::handleChangeEvents);

    assertEquals(result.conversionErrorsSkipped, 1L);
  }

  @Test
  public void testProcessAllDataDirectlyFailsHardOnConversionErrorByDefault() throws Exception {
    insertTestRow(
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleBadAspect,PROD)",
        "status",
        0,
        Instant.now(),
        "testUser");

    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = 10;
    args.aspectNames = java.util.List.of("status");
    // skipConversionErrors defaults to false - the run must fail loudly.

    try {
      loader.load(
          mockOperationContext,
          args,
          msg -> null,
          null,
          mockUpdateIndicesService::handleChangeEvents);
      fail("Expected conversion error to fail the run when skipConversionErrors is off");
    } catch (RuntimeException expected) {
    }
  }

  @Test
  public void testGetArgsSkipConversionErrors() throws Exception {
    var method = LoadIndicesStep.class.getDeclaredMethod("getArgs", UpgradeContext.class);
    method.setAccessible(true);

    when(mockUpgradeContext.parsedArgs()).thenReturn(new HashMap<>());
    LoadIndicesArgs defaults = (LoadIndicesArgs) method.invoke(loadIndicesStep, mockUpgradeContext);
    assertFalse(defaults.skipConversionErrors);

    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put(LoadIndices.SKIP_CONVERSION_ERRORS_ARG_NAME, Optional.of("true"));
    when(mockUpgradeContext.parsedArgs()).thenReturn(parsedArgs);
    LoadIndicesArgs enabled = (LoadIndicesArgs) method.invoke(loadIndicesStep, mockUpgradeContext);
    assertTrue(enabled.skipConversionErrors);
  }

  @Test
  public void testGetDefaultAspectNames() throws Exception {
    // Test the getDefaultAspectNames method
    var method =
        AspectIndexLoader.class.getDeclaredMethod("getDefaultAspectNames", OperationContext.class);
    method.setAccessible(true);

    // Mock entity registry to return some entity specs
    Map<String, EntitySpec> entitySpecs = new HashMap<>();
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    @SuppressWarnings("unchecked")
    Set<String> result = (Set<String>) method.invoke(loader, mockOperationContext);

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
          AspectIndexLoader.class.getDeclaredMethod(
              "getDefaultAspectNames", OperationContext.class);
      method.setAccessible(true);
      method.invoke(loader, (OperationContext) null);
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
          AspectIndexLoader.class.getDeclaredMethod(
              "getDefaultAspectNames", OperationContext.class);
      method.setAccessible(true);
      method.invoke(loader, mockOperationContext);
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
    assertEquals(result.v3BackfillMode, BackfillHeaders.MODE_BASELINE);
    assertEquals(result.txIsolation, TxIsolation.READ_UNCOMMITTED);
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
    parsedArgs.put(
        LoadIndices.V3_BACKFILL_MODE_ARG_NAME, Optional.of(BackfillHeaders.MODE_CATCH_UP));

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
    assertEquals(result.v3BackfillMode, BackfillHeaders.MODE_CATCH_UP);

    // Verify report messages were added
    verify(mockUpgradeReport, atLeastOnce()).addLine(anyString());
  }

  @Test
  public void testGetArgsRejectsInvalidV3BackfillMode() throws Exception {
    var method = LoadIndicesStep.class.getDeclaredMethod("getArgs", UpgradeContext.class);
    method.setAccessible(true);

    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put(LoadIndices.V3_BACKFILL_MODE_ARG_NAME, Optional.of("invalid"));
    when(mockUpgradeContext.parsedArgs()).thenReturn(parsedArgs);

    try {
      method.invoke(loadIndicesStep, mockUpgradeContext);
      fail("Expected invalid v3BackfillMode to be rejected");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof IllegalArgumentException);
    }
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
        AspectIndexLoader.class.getDeclaredMethod(
            "convertToRestoreIndicesArgs", LoadIndicesArgs.class, int.class);
    method.setAccessible(true);

    LoadIndicesArgs args = new LoadIndicesArgs();
    args.aspectNames = Arrays.asList("container", "ownership");
    args.urnLike = "urn:li:dataset:%";
    args.gePitEpochMs = 1640908800000L;
    args.lePitEpochMs = 1640995200000L;

    RestoreIndicesArgs result = (RestoreIndicesArgs) method.invoke(loader, args, 1000);

    assertNotNull(result);
    assertEquals(result.aspectNames, Arrays.asList("container", "ownership"));
    assertEquals(result.urnLike, "urn:li:dataset:%");
    assertEquals(result.gePitEpochMs, Long.valueOf(1640908800000L));
    assertEquals(result.lePitEpochMs, Long.valueOf(1640995200000L));
    assertEquals(result.limit, 1000);
    // The cursor is applied per page by the loader, not by the conversion.
    assertFalse(result.urnBasedPagination);
  }

  @Test
  public void testConvertToRestoreIndicesArgsWithNulls() throws Exception {
    var method =
        AspectIndexLoader.class.getDeclaredMethod(
            "convertToRestoreIndicesArgs", LoadIndicesArgs.class, int.class);
    method.setAccessible(true);

    LoadIndicesArgs args = new LoadIndicesArgs();
    // All fields are null

    RestoreIndicesArgs result = (RestoreIndicesArgs) method.invoke(loader, args, 1000);

    assertNotNull(result);
    assertTrue(result.aspectNames == null || result.aspectNames.isEmpty());
    assertTrue(result.urnLike == null);
    assertEquals(result.gePitEpochMs, Long.valueOf(0L));
    assertEquals(result.lePitEpochMs, Long.valueOf(0L));
    assertEquals(result.limit, 1000);
    assertFalse(result.urnBasedPagination);
    assertTrue(result.lastUrn == null || result.lastUrn.isEmpty());
  }

  @Test
  public void testConvertToMetadataChangeLog() throws Exception {
    var method =
        AspectIndexLoader.class.getDeclaredMethod(
            "convertToMetadataChangeLog",
            OperationContext.class,
            EbeanAspectV2.class,
            LoadIndicesArgs.class,
            boolean.class);
    method.setAccessible(true);
    LoadIndicesArgs args = new LoadIndicesArgs();
    args.v3BackfillMode = BackfillHeaders.MODE_BASELINE;

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
        (MetadataChangeLog) method.invoke(loader, mockOperationContext, aspect, args, false);

    assertNotNull(result);
    assertEquals(result.getEntityType(), "dataset");
    assertEquals(result.getChangeType(), ChangeType.RESTATE);
    assertEquals(result.getAspectName(), "container");
    assertEquals(
        result.getHeaders().get(BackfillHeaders.MODE_HEADER_NAME), BackfillHeaders.MODE_BASELINE);
  }

  @Test
  public void testConvertToMetadataChangeLogWithSystemMetadata() throws Exception {
    var method =
        AspectIndexLoader.class.getDeclaredMethod(
            "convertToMetadataChangeLog",
            OperationContext.class,
            EbeanAspectV2.class,
            LoadIndicesArgs.class,
            boolean.class);
    method.setAccessible(true);
    LoadIndicesArgs args = new LoadIndicesArgs();
    args.v3BackfillMode = BackfillHeaders.MODE_BASELINE;

    // Create test aspect with proper key and system metadata
    EbeanAspectV2 aspect = new EbeanAspectV2();
    EbeanAspectV2.PrimaryKey key =
        new EbeanAspectV2.PrimaryKey(
            "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleDataset,PROD)", "container", 0);
    aspect.setKey(key);
    aspect.setMetadata("{\"container\":{\"urn\":\"urn:li:container:test\"}}");
    aspect.setCreatedOn(Timestamp.from(Instant.now()));
    aspect.setCreatedBy("testUser");

    // Set system metadata to test the systemMetadata handling
    aspect.setSystemMetadata("{\"lastObserved\":1234567890,\"runId\":\"test-run-id\"}");

    // Mock entity registry
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    when(mockEntitySpec.getAspectSpec("container")).thenReturn(mockAspectSpec);
    when(mockAspectSpec.getDataTemplateClass())
        .thenReturn((Class) com.linkedin.container.Container.class);
    when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(mockEntitySpec);

    MetadataChangeLog result =
        (MetadataChangeLog) method.invoke(loader, mockOperationContext, aspect, args, false);

    assertNotNull(result);
    assertEquals(result.getEntityType(), "dataset");
    assertEquals(result.getChangeType(), ChangeType.RESTATE);
    assertEquals(result.getAspectName(), "container");

    // Verify that systemMetadata was properly set
    assertNotNull(result.getSystemMetadata());
    assertEquals(result.getSystemMetadata().getLastObserved(), 1234567890L);
    assertEquals(result.getSystemMetadata().getRunId(), "test-run-id");
  }

  @Test
  public void testConvertToMetadataChangeLogWithNullSystemMetadata() throws Exception {
    var method =
        AspectIndexLoader.class.getDeclaredMethod(
            "convertToMetadataChangeLog",
            OperationContext.class,
            EbeanAspectV2.class,
            LoadIndicesArgs.class,
            boolean.class);
    method.setAccessible(true);
    LoadIndicesArgs args = new LoadIndicesArgs();
    args.v3BackfillMode = BackfillHeaders.MODE_BASELINE;

    // Create test aspect with proper key but no system metadata
    EbeanAspectV2 aspect = new EbeanAspectV2();
    EbeanAspectV2.PrimaryKey key =
        new EbeanAspectV2.PrimaryKey(
            "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleDataset,PROD)", "container", 0);
    aspect.setKey(key);
    aspect.setMetadata("{\"container\":{\"urn\":\"urn:li:container:test\"}}");
    aspect.setCreatedOn(Timestamp.from(Instant.now()));
    aspect.setCreatedBy("testUser");

    // Explicitly set systemMetadata to null to test the null handling
    aspect.setSystemMetadata(null);

    // Mock entity registry
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    when(mockEntitySpec.getAspectSpec("container")).thenReturn(mockAspectSpec);
    when(mockAspectSpec.getDataTemplateClass())
        .thenReturn((Class) com.linkedin.container.Container.class);
    when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(mockEntitySpec);

    MetadataChangeLog result =
        (MetadataChangeLog) method.invoke(loader, mockOperationContext, aspect, args, false);

    assertNotNull(result);
    assertEquals(result.getEntityType(), "dataset");
    assertEquals(result.getChangeType(), ChangeType.RESTATE);
    assertEquals(result.getAspectName(), "container");

    // Verify that systemMetadata is null when input is null
    assertNull(result.getSystemMetadata());
  }

  @Test(expectedExceptions = Exception.class)
  public void testConvertToMetadataChangeLogWithInvalidUrn() throws Exception {
    var method =
        AspectIndexLoader.class.getDeclaredMethod(
            "convertToMetadataChangeLog",
            OperationContext.class,
            EbeanAspectV2.class,
            LoadIndicesArgs.class,
            boolean.class);
    method.setAccessible(true);
    LoadIndicesArgs args = new LoadIndicesArgs();
    args.v3BackfillMode = BackfillHeaders.MODE_BASELINE;

    // Create test aspect with invalid URN
    EbeanAspectV2 aspect = new EbeanAspectV2();
    aspect.setUrn("invalid-urn");
    aspect.setAspect("container");
    aspect.setVersion(0);
    aspect.setMetadata("{}");
    aspect.setCreatedOn(Timestamp.from(Instant.now()));
    aspect.setCreatedBy("testUser");

    method.invoke(loader, mockOperationContext, aspect, args, false);
  }

  @Test
  public void testWriteBatchWithRetrySuccess() throws Exception {
    var method =
        AspectIndexLoader.class.getDeclaredMethod(
            "writeBatchWithRetry",
            OperationContext.class,
            List.class,
            java.util.function.Function.class,
            AspectIndexLoader.BatchWriter.class,
            int.class);
    method.setAccessible(true);

    List<MetadataChangeLog> batch = new ArrayList<>();
    batch.add(mock(MetadataChangeLog.class));

    LoadIndicesResult result = new LoadIndicesResult();

    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());

    method.invoke(
        loader,
        mockOperationContext,
        batch,
        (java.util.function.Function<String, Void>) msg -> null,
        (AspectIndexLoader.BatchWriter) mockUpdateIndicesService::handleChangeEvents,
        0);

    verify(mockUpdateIndicesService, times(1)).handleChangeEvents(any(), any());
  }

  @Test
  public void testWriteBatchWithRetryFailureAndSplit() throws Exception {
    var method =
        AspectIndexLoader.class.getDeclaredMethod(
            "writeBatchWithRetry",
            OperationContext.class,
            List.class,
            java.util.function.Function.class,
            AspectIndexLoader.BatchWriter.class,
            int.class);
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
        loader,
        mockOperationContext,
        batch,
        (java.util.function.Function<String, Void>) msg -> null,
        (AspectIndexLoader.BatchWriter) mockUpdateIndicesService::handleChangeEvents,
        0);

    // Should have been called multiple times due to retry and split
    verify(mockUpdateIndicesService, atLeast(2)).handleChangeEvents(any(), any());
  }

  @Test
  public void testWriteBatchWithRetryMaxRetriesExceeded() throws Exception {
    var method =
        AspectIndexLoader.class.getDeclaredMethod(
            "writeBatchWithRetry",
            OperationContext.class,
            List.class,
            java.util.function.Function.class,
            AspectIndexLoader.BatchWriter.class,
            int.class);
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

    try {
      method.invoke(
          loader,
          mockOperationContext,
          batch,
          (java.util.function.Function<String, Void>) msg -> null,
          (AspectIndexLoader.BatchWriter) mockUpdateIndicesService::handleChangeEvents,
          0);
      fail("Expected exhausted batch writes to fail");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof RuntimeException);
    }

    verify(mockUpdateIndicesService, atLeast(4)).handleChangeEvents(any(), any());
  }

  @Test
  public void testWriteBatchWithRetrySuccessAfterRetries() throws Exception {
    var method =
        AspectIndexLoader.class.getDeclaredMethod(
            "writeBatchWithRetry",
            OperationContext.class,
            List.class,
            java.util.function.Function.class,
            AspectIndexLoader.BatchWriter.class,
            int.class);
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
        loader,
        mockOperationContext,
        batch,
        (java.util.function.Function<String, Void>) msg -> null,
        (AspectIndexLoader.BatchWriter) mockUpdateIndicesService::handleChangeEvents,
        0);

    verify(mockUpdateIndicesService, times(4)).handleChangeEvents(any(), any());
  }

  @Test
  public void testWriteBatchWithRetryMaxRetriesExceededWithSmallBatch() throws Exception {
    var method =
        AspectIndexLoader.class.getDeclaredMethod(
            "writeBatchWithRetry",
            OperationContext.class,
            List.class,
            java.util.function.Function.class,
            AspectIndexLoader.BatchWriter.class,
            int.class);
    method.setAccessible(true);

    List<MetadataChangeLog> batch = new ArrayList<>();
    batch.add(mock(MetadataChangeLog.class));
    batch.add(mock(MetadataChangeLog.class));

    LoadIndicesResult result = new LoadIndicesResult();

    // Always fail - should exceed max retries (3) and split batch
    doThrow(new RuntimeException("Always fails"))
        .when(mockUpdateIndicesService)
        .handleChangeEvents(any(), any());

    try {
      method.invoke(
          loader,
          mockOperationContext,
          batch,
          (java.util.function.Function<String, Void>) msg -> null,
          (AspectIndexLoader.BatchWriter) mockUpdateIndicesService::handleChangeEvents,
          0);
      fail("Expected exhausted split writes to fail");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof RuntimeException);
    }

    verify(mockUpdateIndicesService, atLeast(8)).handleChangeEvents(any(), any());
  }

  @Test
  public void testWriteBatchWithRetryBatchSplitting() throws Exception {
    var method =
        AspectIndexLoader.class.getDeclaredMethod(
            "writeBatchWithRetry",
            OperationContext.class,
            List.class,
            java.util.function.Function.class,
            AspectIndexLoader.BatchWriter.class,
            int.class);
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
        loader,
        mockOperationContext,
        batch,
        (java.util.function.Function<String, Void>) msg -> null,
        (AspectIndexLoader.BatchWriter) mockUpdateIndicesService::handleChangeEvents,
        0);

    // Should have been called 3 times: 1 full batch failure + 2 successful split batches
    verify(mockUpdateIndicesService, times(3)).handleChangeEvents(any(), any());
  }

  @Test
  public void testWriteBatchWithRetryFirstHalfFailure() throws Exception {
    var method =
        AspectIndexLoader.class.getDeclaredMethod(
            "writeBatchWithRetry",
            OperationContext.class,
            List.class,
            java.util.function.Function.class,
            AspectIndexLoader.BatchWriter.class,
            int.class);
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
        loader,
        mockOperationContext,
        batch,
        (java.util.function.Function<String, Void>) msg -> null,
        (AspectIndexLoader.BatchWriter) mockUpdateIndicesService::handleChangeEvents,
        0);

    verify(mockUpdateIndicesService, times(5)).handleChangeEvents(any(), any());
  }

  @Test
  public void testWriteBatchWithRetrySingleItemFailure() throws Exception {
    var method =
        AspectIndexLoader.class.getDeclaredMethod(
            "writeBatchWithRetry",
            OperationContext.class,
            List.class,
            java.util.function.Function.class,
            AspectIndexLoader.BatchWriter.class,
            int.class);
    method.setAccessible(true);

    List<MetadataChangeLog> batch = new ArrayList<>();
    batch.add(mock(MetadataChangeLog.class));

    LoadIndicesResult result = new LoadIndicesResult();

    // Always fail
    doThrow(new RuntimeException("Always fails"))
        .when(mockUpdateIndicesService)
        .handleChangeEvents(any(), any());

    try {
      method.invoke(
          loader,
          mockOperationContext,
          batch,
          (java.util.function.Function<String, Void>) msg -> null,
          (AspectIndexLoader.BatchWriter) mockUpdateIndicesService::handleChangeEvents,
          0);
      fail("Expected exhausted single-event write to fail");
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof RuntimeException);
    }

    verify(mockUpdateIndicesService, times(4)).handleChangeEvents(any(), any());
  }

  @Test
  public void testProcessAllDataDirectlyFailsWithConversionIdentity() throws Exception {

    // Mock the updateIndicesService to not throw exceptions
    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());

    // Create test args with limit
    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = 10;
    args.aspectNames = java.util.List.of("invalidAspect");

    // Add a test row with invalid metadata to cause conversion error
    insertTestRow(
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,InvalidDataset,PROD)",
        "invalidAspect",
        0,
        Instant.now(),
        "testUser");

    try {
      loader.load(
          mockOperationContext,
          args,
          msg -> null,
          null,
          mockUpdateIndicesService::handleChangeEvents);
      fail("Expected conversion failure");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("urn:li:dataset:"));
      assertTrue(e.getMessage().contains("invalidAspect"));
    }
    verify(mockUpdateIndicesService).flushAndWait();
  }

  @Test
  public void testProcessingFailureSuppressesDrainFailure() throws Exception {
    RuntimeException drainFailure = new RuntimeException("drain failed");
    doThrow(drainFailure).when(mockUpdateIndicesService).flushAndWait();

    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = 10;
    args.aspectNames = java.util.List.of("invalidAspect");
    insertTestRow(
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,InvalidSuppressionDataset,PROD)",
        "invalidAspect",
        0,
        Instant.now(),
        "testUser");

    try {
      loader.load(
          mockOperationContext,
          args,
          msg -> null,
          null,
          mockUpdateIndicesService::handleChangeEvents);
      fail("Expected processing failure");
    } catch (RuntimeException e) {
      Throwable processingFailure = e;
      assertTrue(processingFailure.getMessage().contains("InvalidSuppressionDataset"));
      assertEquals(processingFailure.getSuppressed().length, 1);
      assertSame(processingFailure.getSuppressed()[0], drainFailure);
    }
    verify(mockUpdateIndicesService).flushAndWait();
  }

  @Test
  public void testProcessAllDataDirectlyWithLimit() throws Exception {

    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());

    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = 1; // Very small limit
    args.aspectNames = java.util.List.of("container");

    Object result =
        loader.load(
            mockOperationContext,
            args,
            msg -> null,
            null,
            mockUpdateIndicesService::handleChangeEvents);

    assertNotNull(result);
    verify(mockUpdateIndicesService, atLeastOnce()).flushAndWait();
  }

  @Test
  public void testProcessAllDataDirectlyWithUrnLike() throws Exception {

    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());

    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = 10;
    args.urnLike = "urn:li:dataset:%";
    args.aspectNames = java.util.List.of("container");

    Object result =
        loader.load(
            mockOperationContext,
            args,
            msg -> null,
            null,
            mockUpdateIndicesService::handleChangeEvents);

    assertNotNull(result);
    verify(mockUpdateIndicesService, atLeastOnce()).flushAndWait();
  }

  @Test
  public void testProcessAllDataDirectlyWithTimeFilters() throws Exception {

    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());

    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = 10;
    args.gePitEpochMs = Instant.now().minusSeconds(3600).toEpochMilli(); // 1 hour ago
    args.lePitEpochMs = Instant.now().toEpochMilli(); // now
    args.aspectNames = java.util.List.of("container");

    Object result =
        loader.load(
            mockOperationContext,
            args,
            msg -> null,
            null,
            mockUpdateIndicesService::handleChangeEvents);

    assertNotNull(result);
    verify(mockUpdateIndicesService, atLeastOnce()).flushAndWait();
  }

  @Test
  public void testProcessAllDataDirectlyWithLastUrn() throws Exception {

    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());

    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = 10;
    args.lastUrn = "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleDataset1,PROD)";
    args.aspectNames = java.util.List.of("container");

    Object result =
        loader.load(
            mockOperationContext,
            args,
            msg -> null,
            null,
            mockUpdateIndicesService::handleChangeEvents);

    assertNotNull(result);
    verify(mockUpdateIndicesService, atLeastOnce()).flushAndWait();
  }

  @Test
  public void testProcessAllDataDirectlyWithEmptyBatch() throws Exception {

    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());

    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = 10;
    args.aspectNames = java.util.List.of("nonexistent"); // No matching aspects

    Object result =
        loader.load(
            mockOperationContext,
            args,
            msg -> null,
            null,
            mockUpdateIndicesService::handleChangeEvents);

    assertNotNull(result);
    verify(mockUpdateIndicesService, atLeastOnce()).flushAndWait();
  }

  @Test
  public void testProcessAllDataDirectlyWithFinalFlushFailure() throws Exception {

    // Mock successful batch processing but fail on final flush
    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());
    doThrow(new RuntimeException("Flush failed")).when(mockUpdateIndicesService).flushAndWait();

    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = 10;
    args.aspectNames = java.util.List.of("container");

    try {
      loader.load(
          mockOperationContext,
          args,
          msg -> null,
          null,
          mockUpdateIndicesService::handleChangeEvents);
      fail("Expected final flush failure");
    } catch (RuntimeException expected) {
    }
    verify(mockUpdateIndicesService, times(2)).flushAndWait();
  }

  @Test
  public void testGetDefaultAspectNamesWithSearchableAspects() throws Exception {
    var method =
        AspectIndexLoader.class.getDeclaredMethod("getDefaultAspectNames", OperationContext.class);
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
        (java.util.Set<String>) method.invoke(loader, mockOperationContext);

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
        AspectIndexLoader.class.getDeclaredMethod("getDefaultAspectNames", OperationContext.class);
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
        (java.util.Set<String>) method.invoke(loader, mockOperationContext);

    // Verify results - should still include aspects from first entity
    assertNotNull(result);
    assertTrue(result.contains("datasetProperties"));
    assertTrue(result.contains("datasetKey"));
  }

  @Test
  public void testGetDefaultAspectNamesWithNoSearchableAspects() throws Exception {
    var method =
        AspectIndexLoader.class.getDeclaredMethod("getDefaultAspectNames", OperationContext.class);
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
        (java.util.Set<String>) method.invoke(loader, mockOperationContext);

    // Verify results - should be empty since no searchable aspects
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testProcessAllDataDirectlyWithNonEmptyBatchButAllConversionErrors() throws Exception {

    // Mock the updateIndicesService to not throw exceptions
    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());

    // Create test args with small batch size to ensure we hit the batch processing logic
    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 2; // Small batch size to trigger batch processing
    args.limit = 10;
    args.aspectNames = java.util.List.of("invalidAspect");

    // Add test rows with invalid metadata to cause conversion errors
    // This will create a batch that is not empty but all aspects fail conversion
    insertTestRow(
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,InvalidDataset1,PROD)",
        "invalidAspect",
        0,
        Instant.now(),
        "testUser");

    insertTestRow(
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,InvalidDataset2,PROD)",
        "invalidAspect",
        0,
        Instant.now(),
        "testUser");

    // Add a valid row to ensure we have some data to process
    insertTestRow(
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,ValidDataset,PROD)",
        "invalidAspect",
        0,
        Instant.now(),
        "testUser");

    try {
      loader.load(
          mockOperationContext,
          args,
          msg -> null,
          null,
          mockUpdateIndicesService::handleChangeEvents);
      fail("Expected conversion failure");
    } catch (RuntimeException expected) {
    }
  }

  @Test
  public void testProcessAllDataDirectlyWithBatchProcessing() throws Exception {

    // Mock the updateIndicesService to not throw exceptions
    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());

    // Setup entity registry mock for dataset entity
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(mockEntitySpec);
    when(mockEntitySpec.getAspectSpec("container")).thenReturn(mockAspectSpec);
    when(mockAspectSpec.getDataTemplateClass())
        .thenReturn((Class) com.linkedin.container.Container.class);

    // Create test args with small batch size to ensure batch processing is triggered
    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 2; // Small batch size to trigger batch processing
    args.limit = 5;
    args.aspectNames = java.util.List.of("container");

    // Add test rows with valid aspect data to ensure we have data to process
    insertTestRowWithValidData(
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,TestDataset1,PROD)",
        "container",
        0,
        Instant.now(),
        "urn:li:corpuser:testUser");

    insertTestRowWithValidData(
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,TestDataset2,PROD)",
        "container",
        0,
        Instant.now(),
        "urn:li:corpuser:testUser");

    // Call the method
    Object result =
        loader.load(
            mockOperationContext,
            args,
            msg -> null,
            null,
            mockUpdateIndicesService::handleChangeEvents);

    assertNotNull(result);
    verify(mockUpdateIndicesService, atLeastOnce()).flushAndWait();

    // Verify that the method completed successfully
    LoadIndicesResult loadResult = (LoadIndicesResult) result;
    assertNotNull(loadResult);
    // The key test is that the method doesn't throw an exception when processing batches
    // This tests the !mclBatch.isEmpty() logic path
  }

  @Test
  public void testProcessAllDataDirectlyWithProgressReporting() throws Exception {

    // Mock the updateIndicesService to not throw exceptions
    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());

    // Setup entity registry mock for dataset entity
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(mockEntitySpec);
    when(mockEntitySpec.getAspectSpec("container")).thenReturn(mockAspectSpec);
    when(mockAspectSpec.getDataTemplateClass())
        .thenReturn((Class) com.linkedin.container.Container.class);

    // Create test args with small batch size to trigger progress reporting
    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 2; // Small batch size to trigger batch processing
    args.limit = 10;
    args.aspectNames = java.util.List.of("container");

    // Add multiple test rows with valid aspect data
    for (int i = 0; i < 5; i++) {
      insertTestRowWithValidData(
          "urn:li:dataset:(urn:li:dataPlatform:hdfs,TestDataset" + i + ",PROD)",
          "container",
          0,
          Instant.now(),
          "urn:li:corpuser:testUser");
    }

    // Track progress messages
    java.util.List<String> progressMessages = new java.util.ArrayList<>();
    java.util.function.Function<String, Void> reportFunction =
        msg -> {
          progressMessages.add(msg);
          return null;
        };

    // Call the method
    Object result =
        loader.load(
            mockOperationContext,
            args,
            reportFunction,
            null,
            mockUpdateIndicesService::handleChangeEvents);

    assertNotNull(result);
    verify(mockUpdateIndicesService, atLeastOnce()).flushAndWait();

    // Verify that progress messages were generated
    assertTrue(progressMessages.size() > 0);

    // Check that we have "Last URN processed" messages
    boolean hasLastUrnMessage = progressMessages.stream().anyMatch(msg -> msg.contains("Last URN"));
    assertTrue(hasLastUrnMessage, "Should have 'Last URN' messages");
  }

  @Test
  public void testProcessAllDataDirectlyWithLargeDatasetETA() throws Exception {

    // Mock the updateIndicesService to not throw exceptions
    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());

    // Create test args that will trigger ETA calculation (>50000 processed)
    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 1000; // Larger batch size
    args.limit = 60000; // Set limit to trigger ETA calculation
    args.aspectNames = java.util.List.of("container");

    // Add many test rows to simulate large dataset
    for (int i = 0; i < 100; i++) {
      insertTestRow(
          "urn:li:dataset:(urn:li:dataPlatform:hdfs,LargeDataset" + i + ",PROD)",
          "container",
          0,
          Instant.now(),
          "testUser");
    }

    // Track progress messages
    java.util.List<String> progressMessages = new java.util.ArrayList<>();
    java.util.function.Function<String, Void> reportFunction =
        msg -> {
          progressMessages.add(msg);
          return null;
        };

    // Call the method
    Object result =
        loader.load(
            mockOperationContext,
            args,
            reportFunction,
            null,
            mockUpdateIndicesService::handleChangeEvents);

    assertNotNull(result);
    verify(mockUpdateIndicesService, atLeastOnce()).flushAndWait();

    // Verify that progress messages were generated
    assertTrue(progressMessages.size() > 0);

    // Check for ETA-related messages (though they may not appear due to test data size)
    boolean hasProgressMessage =
        progressMessages.stream().anyMatch(msg -> msg.contains("aspects/sec"));
    assertTrue(hasProgressMessage, "Should have throughput messages");
  }

  @Test
  public void testProcessAllDataDirectlyWithFlushFailure() throws Exception {

    doNothing().when(mockUpdateIndicesService).handleChangeEvents(any(), any());
    doThrow(new RuntimeException("Flush failed")).when(mockUpdateIndicesService).flushAndWait();

    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = 10;
    args.aspectNames = java.util.List.of("container");

    try {
      loader.load(
          mockOperationContext,
          args,
          msg -> null,
          null,
          mockUpdateIndicesService::handleChangeEvents);
      fail("Expected flush failure");
    } catch (RuntimeException expected) {
    }
    verify(mockUpdateIndicesService, atLeastOnce()).flushAndWait();
  }

  @Test
  public void testLoadReportsEachBatchWithCumulativeCountsAndDistinctUrns() {
    // SampleDataset1 already has a container row; a second aspect on it must not count twice.
    insertTestRow(
        "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleDataset1,PROD)",
        "ownership",
        0,
        Instant.now(),
        "testUser");
    List<long[]> batches = new ArrayList<>();
    List<String> lastUrns = new ArrayList<>();
    AspectIndexLoader.BatchListener listener = mock(AspectIndexLoader.BatchListener.class);
    doAnswer(
            invocation -> {
              lastUrns.add(invocation.getArgument(0));
              long rows = invocation.getArgument(1);
              long urns = invocation.getArgument(2);
              batches.add(new long[] {rows, urns});
              return null;
            })
        .when(listener)
        .onBatch(any(), anyLong(), anyLong());

    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 2;
    args.limit = Integer.MAX_VALUE;
    args.aspectNames = List.of("container", "ownership");

    LoadIndicesResult result =
        loader.load(
            mockOperationContext,
            args,
            msg -> null,
            listener,
            mockUpdateIndicesService::handleChangeEvents);

    assertEquals(result.rowsProcessed, 4);
    assertEquals(result.urnsProcessed, 3);
    assertEquals(batches.size(), 2);
    assertEquals(batches.get(0)[0], 2L);
    assertEquals(batches.get(1)[0], 4L);
    assertEquals(batches.get(1)[1], 3L);
    assertEquals(lastUrns.get(1), "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleDataset3,PROD)");
    // Each page is drained before it is reported, then the run drains once more on the way out.
    var ordered = inOrder(mockUpdateIndicesService, listener);
    ordered.verify(mockUpdateIndicesService).handleChangeEvents(any(), any());
    ordered.verify(mockUpdateIndicesService).flushAndWait();
    ordered.verify(listener).onBatch(any(), eq(2L), anyLong());
    ordered.verify(mockUpdateIndicesService).handleChangeEvents(any(), any());
    ordered.verify(mockUpdateIndicesService).flushAndWait();
    ordered.verify(listener).onBatch(any(), eq(4L), eq(3L));
    ordered.verify(mockUpdateIndicesService).flushAndWait();
  }

  @Test
  public void testLoadStopsAfterTheReportedBatchOnceTheDeadlineHasPassed() {
    List<String> checkpointed = new ArrayList<>();
    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 1;
    args.limit = Integer.MAX_VALUE;
    args.aspectNames = List.of("container", "ownership");
    args.deadlineEpochMs = System.currentTimeMillis() - 1;

    try {
      loader.load(
          mockOperationContext,
          args,
          msg -> null,
          (lastUrn, rows, urns) -> checkpointed.add(lastUrn),
          mockUpdateIndicesService::handleChangeEvents);
      fail("Expected the deadline to stop the run");
    } catch (AspectIndexLoader.DeadlineExceeded e) {
      assertEquals(e.rowsProcessed(), 1L);
      assertEquals(e.lastUrn(), checkpointed.get(0));
    }
    // The reported batch was drained before the listener ran. The exit drain runs after the
    // deadline.
    assertEquals(checkpointed.size(), 1);
    verify(mockUpdateIndicesService, times(1)).handleChangeEvents(any(), any());
    verify(mockUpdateIndicesService, times(2)).flushAndWait();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTargetIndexTravelsOnEveryBaselineEvent() {
    List<MetadataChangeLog> written = new ArrayList<>();
    LoadIndicesArgs args = new LoadIndicesArgs();
    args.batchSize = 100;
    args.limit = Integer.MAX_VALUE;
    args.aspectNames = List.of("container");
    args.v3TargetIndex = "primaryindex_v3_0_14_0-0_1679000000000";

    loader.load(
        mockOperationContext, args, msg -> null, null, (ctx, batch) -> written.addAll(batch));

    assertEquals(written.size(), 2);
    for (MetadataChangeLog event : written) {
      assertEquals(
          event.getHeaders().get(BackfillHeaders.TARGET_INDEX_HEADER_NAME), args.v3TargetIndex);
    }

    written.clear();
    args.v3BackfillMode = BackfillHeaders.MODE_CATCH_UP;
    loader.load(
        mockOperationContext, args, msg -> null, null, (ctx, batch) -> written.addAll(batch));
    for (MetadataChangeLog event : written) {
      assertFalse(event.getHeaders().containsKey(BackfillHeaders.TARGET_INDEX_HEADER_NAME));
    }
  }
}
