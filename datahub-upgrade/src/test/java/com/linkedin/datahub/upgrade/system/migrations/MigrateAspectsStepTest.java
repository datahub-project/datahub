package com.linkedin.datahub.upgrade.system.migrations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.util.RecordUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MigrateAspectsStepTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private EntityService<?> mockEntityService;
  private AspectDao mockAspectDao;
  private UpgradeContext mockContext;
  private Upgrade mockUpgrade;

  @BeforeMethod
  public void setup() {
    mockEntityService = mock(EntityService.class);
    mockAspectDao = mock(AspectDao.class);
    mockUpgrade = mock(Upgrade.class);
    mockContext = mock(UpgradeContext.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockContext.report()).thenReturn(mock(UpgradeReport.class));
  }

  // ── cursorState ────────────────────────────────────────────────────────────

  @Test
  public void testCursorStateEmptyBatchReturnsEmptyMap() {
    MigrateAspectsStep step = buildStep(Map.of("testAspect", 2L));
    Map<String, String> cursor = step.cursorState(List.of());
    assertTrue(cursor.isEmpty());
  }

  @Test
  public void testCursorStateReturnsLastRowEpochMs() {
    long expectedMs = 1_700_000_000_000L;
    EbeanAspectV2 aspect =
        buildAspect(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.t,PROD)", "schemaMetadata", expectedMs);

    MigrateAspectsStep step = buildStep(Map.of("schemaMetadata", 2L));
    Map<String, String> cursor = step.cursorState(List.of(aspect));

    assertEquals(cursor.get(MigrateAspectsStep.LAST_CREATED_ON_MS_KEY), String.valueOf(expectedMs));
  }

  @Test
  public void testCursorStateUsesLastElementOfBatch() {
    EbeanAspectV2 first = buildAspect("urn:li:corpuser:alice", "ownership", 1_000L);
    EbeanAspectV2 last = buildAspect("urn:li:corpuser:bob", "ownership", 9_000L);

    MigrateAspectsStep step = buildStep(Map.of("ownership", 2L));
    Map<String, String> cursor = step.cursorState(List.of(first, last));

    assertEquals(cursor.get(MigrateAspectsStep.LAST_CREATED_ON_MS_KEY), "9000");
  }

  // ── loadResumeState ────────────────────────────────────────────────────────

  @Test
  public void testLoadResumeStateNoResultReturnsEmptyMap() {
    MigrateAspectsStep step = buildStep(Map.of("testAspect", 2L));
    Map<String, String> state = step.loadResumeState(Optional.empty());
    assertTrue(state.isEmpty());
  }

  @Test
  public void testLoadResumeStateSucceededResultReturnsEmptyMap() {
    DataHubUpgradeResult result = mock(DataHubUpgradeResult.class);
    when(result.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    when(result.getResult()).thenReturn(new StringMap(Map.of("lastCreatedOnMs", "12345")));

    MigrateAspectsStep step = buildStep(Map.of("testAspect", 2L));
    Map<String, String> state = step.loadResumeState(Optional.of(result));

    // Only IN_PROGRESS results carry the cursor — SUCCEEDED means step was complete
    assertTrue(state.isEmpty());
  }

  @Test
  public void testLoadResumeStateInProgressResultReturnsCursor() {
    StringMap cursor = new StringMap(Map.of("lastCreatedOnMs", "999999"));
    DataHubUpgradeResult result = mock(DataHubUpgradeResult.class);
    when(result.getState()).thenReturn(DataHubUpgradeState.IN_PROGRESS);
    when(result.getResult()).thenReturn(cursor);

    MigrateAspectsStep step = buildStep(Map.of("testAspect", 2L));
    Map<String, String> state = step.loadResumeState(Optional.of(result));

    assertEquals(state.get("lastCreatedOnMs"), "999999");
  }

  // ── skip ──────────────────────────────────────────────────────────────────

  @Test
  public void testSkipWhenPreviousResultIsSucceeded() {
    DataHubUpgradeResult prev = succeededResult();
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.of(prev));

    MigrateAspectsStep step = buildStep(Map.of("testAspect", 2L));
    assertTrue(step.skip(mockContext));
  }

  @Test
  public void testSkipWhenPreviousResultIsAborted() {
    DataHubUpgradeResult prev = abortedResult();
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.of(prev));

    MigrateAspectsStep step = buildStep(Map.of("testAspect", 2L));
    assertTrue(step.skip(mockContext));
  }

  @Test
  public void testNoSkipWhenNoPreviousResult() {
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());

    MigrateAspectsStep step = buildStep(Map.of("testAspect", 2L));
    assertFalse(step.skip(mockContext));
  }

  // ── executable ────────────────────────────────────────────────────────────

  @Test
  public void testExecutableWithEmptyStreamSucceeds() {
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());

    PartitionedStream<EbeanAspectV2> emptyStream =
        PartitionedStream.<EbeanAspectV2>builder().delegateStream(Stream.empty()).build();
    when(mockAspectDao.streamAspectBatchesForMigration(any(), eq(0L), anyInt(), anyInt()))
        .thenReturn(emptyStream);

    MigrateAspectsStep step = buildStep(Map.of("testAspect", 2L));
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    // entityService.ingestProposal is called by BootstrapStep.setUpgradeResult for the final state
    verify(mockEntityService, atLeastOnce()).ingestProposal(any(), any(), any(), anyBoolean());
  }

  @Test
  public void testExecutableWithValidAspectIngests() {
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());

    // Valid corpUserKey JSON so EntityUtils can parse it into a ChangeItem
    long createdOnMs = 1_700_000_000_000L;
    EbeanAspectV2 aspect =
        new EbeanAspectV2(
            "urn:li:corpuser:alice",
            "corpUserKey",
            0L,
            "{\"username\":\"alice\"}",
            new Timestamp(createdOnMs),
            "urn:li:corpuser:datahub",
            null,
            RecordUtils.toJsonString(SystemMetadataUtils.createDefaultSystemMetadata()));
    PartitionedStream<EbeanAspectV2> stream =
        PartitionedStream.<EbeanAspectV2>builder().delegateStream(Stream.of(aspect)).build();
    when(mockAspectDao.streamAspectBatchesForMigration(any(), eq(0L), anyInt(), anyInt()))
        .thenReturn(stream);

    MigrateAspectsStep step = buildStep(Map.of("corpUserKey", 2L));
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    // Cursor state for the non-empty batch should have been persisted as IN_PROGRESS
    verify(mockUpgrade)
        .setUpgradeResult(any(), any(), any(), eq(DataHubUpgradeState.IN_PROGRESS), any());
  }

  @Test
  public void testExecutableWithBatchDelayMs() {
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());

    EbeanAspectV2 aspect =
        new EbeanAspectV2(
            "urn:li:corpuser:bob",
            "corpUserKey",
            0L,
            "{\"username\":\"bob\"}",
            new Timestamp(2_000L),
            "urn:li:corpuser:datahub",
            null,
            RecordUtils.toJsonString(SystemMetadataUtils.createDefaultSystemMetadata()));
    PartitionedStream<EbeanAspectV2> stream =
        PartitionedStream.<EbeanAspectV2>builder().delegateStream(Stream.of(aspect)).build();
    when(mockAspectDao.streamAspectBatchesForMigration(any(), eq(0L), anyInt(), anyInt()))
        .thenReturn(stream);

    MigrateAspectsStep step =
        new MigrateAspectsStep(
            OP_CONTEXT,
            mockEntityService,
            mockAspectDao,
            Map.of("corpUserKey", 2L),
            UPGRADE_VERSION,
            100,
            1, // 1 ms delay — exercises the sleep path without slowing the test
            0);
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testExecutableResumesFromSavedCursorState() {
    StringMap cursor = new StringMap(Map.of(MigrateAspectsStep.LAST_CREATED_ON_MS_KEY, "999999"));
    DataHubUpgradeResult inProgress = mock(DataHubUpgradeResult.class);
    when(inProgress.getState()).thenReturn(DataHubUpgradeState.IN_PROGRESS);
    when(inProgress.getResult()).thenReturn(cursor);
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.of(inProgress));

    PartitionedStream<EbeanAspectV2> emptyStream =
        PartitionedStream.<EbeanAspectV2>builder().delegateStream(Stream.empty()).build();
    when(mockAspectDao.streamAspectBatchesForMigration(any(), eq(999999L), anyInt(), anyInt()))
        .thenReturn(emptyStream);

    MigrateAspectsStep step = buildStep(Map.of("corpUserKey", 2L));
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockAspectDao).streamAspectBatchesForMigration(any(), eq(999999L), anyInt(), anyInt());
  }

  // ── withAppSource ─────────────────────────────────────────────────────────

  @Test
  public void testWithAppSourceNullInput() throws Exception {
    Method m = MigrateAspectsStep.class.getDeclaredMethod("withAppSource", SystemMetadata.class);
    m.setAccessible(true);

    SystemMetadata result = (SystemMetadata) m.invoke(null, (SystemMetadata) null);

    assertNotNull(result);
    assertNotNull(result.getProperties());
    assertEquals(
        result.getProperties().get(com.linkedin.metadata.Constants.APP_SOURCE),
        com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE);
  }

  @Test
  public void testWithAppSourceCopiesAndAddsSource() throws Exception {
    Method m = MigrateAspectsStep.class.getDeclaredMethod("withAppSource", SystemMetadata.class);
    m.setAccessible(true);

    SystemMetadata input = new SystemMetadata();
    StringMap props = new StringMap();
    props.put("existingKey", "existingValue");
    input.setProperties(props);

    SystemMetadata result = (SystemMetadata) m.invoke(null, input);

    assertEquals(result.getProperties().get("existingKey"), "existingValue");
    assertEquals(
        result.getProperties().get(com.linkedin.metadata.Constants.APP_SOURCE),
        com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE);
    // original must not be mutated
    assertNull(input.getProperties().get(com.linkedin.metadata.Constants.APP_SOURCE));
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  private static final String UPGRADE_VERSION = "v0.14.1-abc123def456abc123def456abc123def456abc1";

  private MigrateAspectsStep buildStep(Map<String, Long> aspectTargetVersions) {
    return new MigrateAspectsStep(
        OP_CONTEXT,
        mockEntityService,
        mockAspectDao,
        aspectTargetVersions,
        UPGRADE_VERSION,
        100,
        0,
        0);
  }

  private static EbeanAspectV2 buildAspect(String urn, String aspectName, long createdOnMs) {
    return new EbeanAspectV2(
        urn,
        aspectName,
        0L,
        "{}",
        new Timestamp(createdOnMs),
        "urn:li:corpuser:datahub",
        null,
        RecordUtils.toJsonString(SystemMetadataUtils.createDefaultSystemMetadata()));
  }

  private static DataHubUpgradeResult succeededResult() {
    DataHubUpgradeResult r = mock(DataHubUpgradeResult.class);
    when(r.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    return r;
  }

  private static DataHubUpgradeResult abortedResult() {
    DataHubUpgradeResult r = mock(DataHubUpgradeResult.class);
    when(r.getState()).thenReturn(DataHubUpgradeState.ABORTED);
    return r;
  }
}
