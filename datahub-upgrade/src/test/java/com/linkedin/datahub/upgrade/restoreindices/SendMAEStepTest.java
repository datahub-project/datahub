package com.linkedin.datahub.upgrade.restoreindices;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.ebean.Database;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SendMAEStepTest {

  @Mock private EntityService<?> mockEntityService;

  @Mock private UpgradeContext mockContext;

  @Mock private UpgradeReport mockReport;

  @Mock private OperationContext mockOpContext;

  // Use a real H2 database instead of mocking
  private Database database;
  private SendMAEStep sendMAEStep;
  private Map<String, Optional<String>> parsedArgs;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);

    // Create a real H2 in-memory database for testing
    String instanceId = "sendmae_" + UUID.randomUUID().toString().replace("-", "");
    database = EbeanTestUtils.createTestServer(instanceId);

    // Setup the test database with required schema if needed
    setupTestDatabase();

    // Create the real SendMAEStep with the test database
    sendMAEStep = new SendMAEStep(database, mockEntityService);

    parsedArgs = new HashMap<>();

    when(mockContext.parsedArgs()).thenReturn(parsedArgs);
    when(mockContext.report()).thenReturn(mockReport);
    when(mockContext.opContext()).thenReturn(mockOpContext);

    // Setup default result for entityService
    RestoreIndicesResult mockResult = new RestoreIndicesResult();
    mockResult.rowsMigrated = 0;
    mockResult.ignored = 0;

    when(mockEntityService.restoreIndices(eq(mockOpContext), any(RestoreIndicesArgs.class), any()))
        .thenReturn(Collections.singletonList(mockResult));
  }

  private void setupTestDatabase() {
    // Insert a few test rows
    insertTestRow("urn:li:test:1", "testAspect", 0, Instant.now(), "testUser");
  }

  private void insertTestRow(
      String urn, String aspect, long version, Instant createdTime, String createdBy) {
    EbeanAspectV2 aspectV2 = new EbeanAspectV2();
    aspectV2.setUrn(urn);
    aspectV2.setAspect(aspect);
    aspectV2.setVersion(version);
    aspectV2.setMetadata("{}"); // Required field
    // Set the required createdOn timestamp
    aspectV2.setCreatedOn(Timestamp.from(createdTime));
    // Set the required createdBy field
    aspectV2.setCreatedBy(createdBy);
    // createdFor is nullable, so we can leave it null
    database.save(aspectV2);
  }

  private void clearTestRows() {
    // Use truncate for faster cleanup between tests
    database.truncate(EbeanAspectV2.class);
  }

  private void insertTestRows(int count, String aspectName) {
    clearTestRows();
    Instant now = Instant.now();
    for (int i = 0; i < count; i++) {
      // Create test rows with different timestamps for testing time-based queries
      Instant timestamp = now.minusSeconds(i * 3600); // Each row 1 hour apart
      insertTestRow(
          "urn:li:test:" + i,
          aspectName != null ? aspectName : "testAspect",
          0,
          timestamp,
          "testUser");
    }
  }

  @Test
  public void testId() {
    assertEquals(sendMAEStep.id(), "SendMAEStep");
  }

  @Test
  public void testRetryCount() {
    assertEquals(sendMAEStep.retryCount(), 0);
  }

  @Test
  public void testExecutableWithDefaultArgs() {
    // Insert a few test rows
    insertTestRows(5, null);

    // Execute
    UpgradeStepResult result = sendMAEStep.executable().apply(mockContext);

    // Verify
    assertTrue(result instanceof DefaultUpgradeStepResult);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    assertEquals(result.stepId(), sendMAEStep.id());
    assertEquals(result.action(), UpgradeStepResult.Action.CONTINUE);

    // Verify argument defaults using a capture
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(mockEntityService).restoreIndices(eq(mockOpContext), argsCaptor.capture(), any());

    RestoreIndicesArgs capturedArgs = argsCaptor.getValue();
    assertEquals(capturedArgs.batchSize, 1000);
    assertEquals(capturedArgs.limit, 1000);
    assertEquals(capturedArgs.numThreads, 1);
    assertEquals(capturedArgs.batchDelayMs, 250);
    assertEquals(capturedArgs.start, 0);
    assertFalse(capturedArgs.urnBasedPagination);
    assertFalse(capturedArgs.createDefaultAspects);
  }

  @Test
  public void testExecutableWithCustomArgs() {
    // Setup custom args
    parsedArgs.put(RestoreIndices.BATCH_SIZE_ARG_NAME, Optional.of("500"));
    parsedArgs.put(RestoreIndices.NUM_THREADS_ARG_NAME, Optional.of("2"));
    parsedArgs.put(RestoreIndices.BATCH_DELAY_MS_ARG_NAME, Optional.of("100"));
    parsedArgs.put(RestoreIndices.STARTING_OFFSET_ARG_NAME, Optional.of("100"));
    parsedArgs.put(RestoreIndices.URN_BASED_PAGINATION_ARG_NAME, Optional.of("true"));
    parsedArgs.put(RestoreIndices.CREATE_DEFAULT_ASPECTS_ARG_NAME, Optional.of("true"));
    parsedArgs.put(RestoreIndices.ASPECT_NAME_ARG_NAME, Optional.of("testAspect"));
    parsedArgs.put(RestoreIndices.URN_ARG_NAME, Optional.of("testUrn"));

    // Insert some test data
    insertTestRows(5, "testAspect");

    // Execute
    UpgradeStepResult result = sendMAEStep.executable().apply(mockContext);

    // Verify
    assertTrue(result instanceof DefaultUpgradeStepResult);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    assertEquals(result.stepId(), sendMAEStep.id());
    assertEquals(result.action(), UpgradeStepResult.Action.CONTINUE);

    // Verify custom args
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(mockEntityService).restoreIndices(eq(mockOpContext), argsCaptor.capture(), any());

    RestoreIndicesArgs capturedArgs = argsCaptor.getValue();
    assertEquals(capturedArgs.batchSize, 500);
    assertEquals(capturedArgs.limit, 500);
    assertEquals(capturedArgs.numThreads, 2);
    assertEquals(capturedArgs.batchDelayMs, 100);
    assertEquals(capturedArgs.start, 100);
    assertTrue(capturedArgs.urnBasedPagination);
    assertTrue(capturedArgs.createDefaultAspects);
    assertEquals(capturedArgs.aspectName, "testAspect");
    assertEquals(capturedArgs.urn, "testUrn");
  }

  @Test
  public void testExecutableWithUrnLike() {
    // Setup
    parsedArgs.put(RestoreIndices.URN_LIKE_ARG_NAME, Optional.of("urn:li:test:%"));

    // Insert data that matches the URN pattern
    insertTestRows(3, null);

    // Execute
    UpgradeStepResult result = sendMAEStep.executable().apply(mockContext);

    // Verify urn like parameter
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(mockEntityService).restoreIndices(eq(mockOpContext), argsCaptor.capture(), any());

    RestoreIndicesArgs capturedArgs = argsCaptor.getValue();
    assertEquals(capturedArgs.urnLike, "urn:li:test:%");
  }

  @Test
  public void testExecutableWithPitEpochMs() {
    // Setup time range query parameters
    long currentTimeMs = System.currentTimeMillis();
    long oneHourAgoMs = currentTimeMs - (60 * 60 * 1000);

    parsedArgs.put(
        RestoreIndices.LE_PIT_EPOCH_MS_ARG_NAME, Optional.of(String.valueOf(currentTimeMs)));
    parsedArgs.put(
        RestoreIndices.GE_PIT_EPOCH_MS_ARG_NAME, Optional.of(String.valueOf(oneHourAgoMs)));

    // Add some test data with timestamps that fall within the time range
    Instant now = Instant.now();
    Instant oneHourAgo = now.minusSeconds(3600);
    Instant twoHoursAgo = now.minusSeconds(7200);

    clearTestRows();
    insertTestRow("urn:li:test:1", "testAspect", 0, now, "testUser"); // Within range
    insertTestRow("urn:li:test:2", "testAspect", 0, oneHourAgo, "testUser"); // Edge of range
    insertTestRow("urn:li:test:3", "testAspect", 0, twoHoursAgo, "testUser"); // Outside range

    // Execute
    UpgradeStepResult result = sendMAEStep.executable().apply(mockContext);

    // Verify pit epoch parameters
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(mockEntityService).restoreIndices(eq(mockOpContext), argsCaptor.capture(), any());

    RestoreIndicesArgs capturedArgs = argsCaptor.getValue();
    assertEquals(capturedArgs.lePitEpochMs, currentTimeMs);
    assertEquals(capturedArgs.gePitEpochMs, oneHourAgoMs);
  }

  @Test
  public void testExecutableWithAspectNames() {
    // Setup
    parsedArgs.put(RestoreIndices.ASPECT_NAMES_ARG_NAME, Optional.of("aspect1,aspect2,aspect3"));

    // Add test data for these aspects
    Instant now = Instant.now();
    clearTestRows();
    insertTestRow("urn:li:test:1", "aspect1", 0, now, "testUser");
    insertTestRow("urn:li:test:2", "aspect2", 0, now, "testUser");
    insertTestRow("urn:li:test:3", "aspect3", 0, now, "testUser");

    // Execute
    UpgradeStepResult result = sendMAEStep.executable().apply(mockContext);

    // Verify aspect names parameter
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(mockEntityService).restoreIndices(eq(mockOpContext), argsCaptor.capture(), any());

    RestoreIndicesArgs capturedArgs = argsCaptor.getValue();
    assertEquals(capturedArgs.aspectNames.size(), 3);
    assertTrue(capturedArgs.aspectNames.contains("aspect1"));
    assertTrue(capturedArgs.aspectNames.contains("aspect2"));
    assertTrue(capturedArgs.aspectNames.contains("aspect3"));
  }

  @Test(timeOut = 10000) // 10 second timeout
  public void testExecutableWithUrnBasedPagination() {
    // Setup
    parsedArgs.put(RestoreIndices.URN_BASED_PAGINATION_ARG_NAME, Optional.of("true"));
    parsedArgs.put(RestoreIndices.BATCH_SIZE_ARG_NAME, Optional.of("2"));

    // Insert enough rows to trigger pagination
    insertTestRows(5, null);

    // Setup sequential results for pagination
    RestoreIndicesResult firstResult = new RestoreIndicesResult();
    firstResult.rowsMigrated = 2;
    firstResult.ignored = 0;
    firstResult.lastUrn = "urn:li:test:1";
    firstResult.lastAspect = "testAspect";

    RestoreIndicesResult secondResult = new RestoreIndicesResult();
    secondResult.rowsMigrated = 2;
    secondResult.ignored = 0;
    secondResult.lastUrn = "urn:li:test:3";
    secondResult.lastAspect = "testAspect";

    RestoreIndicesResult thirdResult = new RestoreIndicesResult();
    thirdResult.rowsMigrated = 1;
    thirdResult.ignored = 0;
    thirdResult.lastUrn = "urn:li:test:4";
    thirdResult.lastAspect = "testAspect";

    // Empty result to terminate the loop
    RestoreIndicesResult emptyResult = new RestoreIndicesResult();
    emptyResult.rowsMigrated = 0;
    emptyResult.ignored = 0;

    // Mock the sequential calls with different results
    when(mockEntityService.restoreIndices(eq(mockOpContext), any(RestoreIndicesArgs.class), any()))
        .thenReturn(Collections.singletonList(firstResult))
        .thenReturn(Collections.singletonList(secondResult))
        .thenReturn(Collections.singletonList(thirdResult))
        .thenReturn(Collections.singletonList(emptyResult));

    // Execute
    UpgradeStepResult result = sendMAEStep.executable().apply(mockContext);

    // Verify it succeeded
    assertTrue(result instanceof DefaultUpgradeStepResult);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    assertEquals(result.stepId(), sendMAEStep.id());
    assertEquals(result.action(), UpgradeStepResult.Action.CONTINUE);

    // Verify the calls - should have 4 calls for pagination (3 with data + 1 empty to terminate)
    verify(mockEntityService, times(4))
        .restoreIndices(eq(mockOpContext), any(RestoreIndicesArgs.class), any());

    // Verify the args used for each call
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(mockEntityService, times(4))
        .restoreIndices(eq(mockOpContext), argsCaptor.capture(), any());

    RestoreIndicesArgs firstCallArgs = argsCaptor.getAllValues().get(0);
    RestoreIndicesArgs secondCallArgs = argsCaptor.getAllValues().get(1);
    RestoreIndicesArgs thirdCallArgs = argsCaptor.getAllValues().get(2);

    // First call should have null for lastUrn and lastAspect
    assertEquals(firstCallArgs.lastUrn, "");
    assertEquals(firstCallArgs.lastAspect, "");

    // Second call should have the values from firstResult
    assertEquals(secondCallArgs.lastUrn, "urn:li:test:1");
    assertEquals(secondCallArgs.lastAspect, "testAspect");

    // Third call should have the values from secondResult
    assertEquals(thirdCallArgs.lastUrn, "urn:li:test:3");
    assertEquals(thirdCallArgs.lastAspect, "testAspect");
  }

  @Test
  public void testContainsKey() {
    Map<String, Optional<String>> map = new HashMap<>();
    map.put("key1", Optional.of("value1"));
    map.put("key2", Optional.empty());

    assertTrue(SendMAEStep.containsKey(map, "key1"));
    assertFalse(SendMAEStep.containsKey(map, "key2"));
    assertFalse(SendMAEStep.containsKey(map, "nonExistentKey"));
  }

  @Test
  public void testExecutableWithError() {
    // Insert rows so the query returns data
    insertTestRows(10, null);

    // Force the service to throw an exception
    when(mockEntityService.restoreIndices(eq(mockOpContext), any(RestoreIndicesArgs.class), any()))
        .thenThrow(new RuntimeException("Test exception"));

    // Execute
    UpgradeStepResult result = sendMAEStep.executable().apply(mockContext);

    // Verify failure
    assertTrue(result instanceof DefaultUpgradeStepResult);
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    assertEquals(result.stepId(), sendMAEStep.id());
  }

  @Test
  public void testReportAddedLines() {
    // Insert some test data
    insertTestRows(1, null);

    // Execute
    sendMAEStep.executable().apply(mockContext);

    // Verify report lines are added
    verify(mockReport, atLeastOnce()).addLine(anyString());
  }

  @Test
  public void testExecutableWithCreateDefaultAspects() {
    // Setup
    parsedArgs.put(RestoreIndices.CREATE_DEFAULT_ASPECTS_ARG_NAME, Optional.of("true"));

    // Insert test data
    insertTestRows(3, null);

    // Execute
    UpgradeStepResult result = sendMAEStep.executable().apply(mockContext);

    // Verify result
    assertTrue(result instanceof DefaultUpgradeStepResult);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    assertEquals(result.stepId(), sendMAEStep.id());
    assertEquals(result.action(), UpgradeStepResult.Action.CONTINUE);

    // Verify createDefaultAspects parameter
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(mockEntityService).restoreIndices(eq(mockOpContext), argsCaptor.capture(), any());

    RestoreIndicesArgs capturedArgs = argsCaptor.getValue();
    assertTrue(capturedArgs.createDefaultAspects);
  }
}
