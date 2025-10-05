package com.linkedin.datahub.upgrade.loadindices;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.ebean.Database;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
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
}
