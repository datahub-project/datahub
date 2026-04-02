package com.linkedin.datahub.upgrade.system.migrations;

import static com.linkedin.metadata.Constants.DEFAULT_SCHEMA_VERSION;
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
import com.linkedin.metadata.entity.AspectMigrationsDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MigrateAspectsStepTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  /** Combines both AspectDao and AspectMigrationsDao for mock creation. */
  interface MigratingAspectDao extends AspectDao, AspectMigrationsDao {}

  private EntityService<?> mockEntityService;
  private MigratingAspectDao mockMigratingDao;
  private AspectDao mockPlainDao;
  private UpgradeContext mockContext;
  private Upgrade mockUpgrade;

  @BeforeMethod
  public void setup() {
    mockEntityService = mock(EntityService.class);
    mockMigratingDao = mock(MigratingAspectDao.class);
    mockPlainDao = mock(AspectDao.class);
    mockUpgrade = mock(Upgrade.class);
    mockContext = mock(UpgradeContext.class);
    when(mockContext.upgrade()).thenReturn(mockUpgrade);
    when(mockContext.report()).thenReturn(mock(UpgradeReport.class));
  }

  // ── cursorState ────────────────────────────────────────────────────────────

  @Test
  public void testCursorStateEmptyBatchReturnsEmptyMap() {
    MigrateAspectsStep step = buildStep(mockMigratingDao, Map.of("testAspect", 2L));
    Map<String, String> cursor = step.cursorState(List.of());
    assertTrue(cursor.isEmpty());
  }

  @Test
  public void testCursorStateReturnsLastRowEpochMs() {
    long expectedMs = 1_700_000_000_000L;
    EbeanAspectV2 aspect =
        buildAspect(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.t,PROD)", "schemaMetadata", expectedMs);

    MigrateAspectsStep step = buildStep(mockMigratingDao, Map.of("schemaMetadata", 2L));
    Map<String, String> cursor = step.cursorState(List.of(aspect));

    assertEquals(cursor.get(MigrateAspectsStep.LAST_CREATED_ON_MS_KEY), String.valueOf(expectedMs));
  }

  @Test
  public void testCursorStateUsesLastElementOfBatch() {
    EbeanAspectV2 first = buildAspect("urn:li:corpuser:alice", "ownership", 1_000L);
    EbeanAspectV2 last = buildAspect("urn:li:corpuser:bob", "ownership", 9_000L);

    MigrateAspectsStep step = buildStep(mockMigratingDao, Map.of("ownership", 2L));
    Map<String, String> cursor = step.cursorState(List.of(first, last));

    assertEquals(cursor.get(MigrateAspectsStep.LAST_CREATED_ON_MS_KEY), "9000");
  }

  // ── loadResumeState ────────────────────────────────────────────────────────

  @Test
  public void testLoadResumeStateNoResultReturnsEmptyMap() {
    MigrateAspectsStep step = buildStep(mockMigratingDao, Map.of("testAspect", 2L));
    Map<String, String> state = step.loadResumeState(Optional.empty());
    assertTrue(state.isEmpty());
  }

  @Test
  public void testLoadResumeStateSucceededResultReturnsEmptyMap() {
    DataHubUpgradeResult result = mock(DataHubUpgradeResult.class);
    when(result.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    when(result.getResult()).thenReturn(new StringMap(Map.of("lastCreatedOnMs", "12345")));

    MigrateAspectsStep step = buildStep(mockMigratingDao, Map.of("testAspect", 2L));
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

    MigrateAspectsStep step = buildStep(mockMigratingDao, Map.of("testAspect", 2L));
    Map<String, String> state = step.loadResumeState(Optional.of(result));

    assertEquals(state.get("lastCreatedOnMs"), "999999");
  }

  // ── skip ──────────────────────────────────────────────────────────────────

  @Test
  public void testSkipWhenPreviousResultIsSucceeded() {
    DataHubUpgradeResult prev = succeededResult();
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.of(prev));

    MigrateAspectsStep step = buildStep(mockMigratingDao, Map.of("testAspect", 2L));
    assertTrue(step.skip(mockContext));

    // DB should not be queried when a prior SUCCEEDED result exists
    verify(mockMigratingDao, never()).hasAspectsNeedingMigration(any(), any());
  }

  @Test
  public void testSkipWhenPreviousResultIsAborted() {
    DataHubUpgradeResult prev = abortedResult();
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.of(prev));

    MigrateAspectsStep step = buildStep(mockMigratingDao, Map.of("testAspect", 2L));
    assertTrue(step.skip(mockContext));
  }

  @Test
  public void testSkipWhenAspectDaoIsNotAspectMigrationsDao() {
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());

    // Use the plain AspectDao mock (does not implement AspectMigrationsDao)
    MigrateAspectsStep step = buildStep(mockPlainDao, Map.of("testAspect", 2L));
    assertFalse(step.skip(mockContext));
  }

  @Test
  public void testSkipWhenNoRowsNeedMigration() {
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());
    when(mockMigratingDao.hasAspectsNeedingMigration(any(), any())).thenReturn(false);

    MigrateAspectsStep step = buildStep(mockMigratingDao, Map.of("testAspect", 2L));
    assertTrue(step.skip(mockContext));
  }

  @Test
  public void testNoSkipWhenRowsNeedMigration() {
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());
    when(mockMigratingDao.hasAspectsNeedingMigration(eq("testAspect"), any())).thenReturn(true);

    MigrateAspectsStep step = buildStep(mockMigratingDao, Map.of("testAspect", 2L));
    assertFalse(step.skip(mockContext));
  }

  @Test
  public void testSkipChecksSourceVersionsCorrectly() {
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());
    when(mockMigratingDao.hasAspectsNeedingMigration(any(), any())).thenReturn(false);

    // targetVersion = 3 → sourceVersions should be {1, 2} (DEFAULT..target-1)
    MigrateAspectsStep step = buildStep(mockMigratingDao, Map.of("testAspect", 3L));
    step.skip(mockContext);

    verify(mockMigratingDao)
        .hasAspectsNeedingMigration(
            eq("testAspect"), eq(Set.of(DEFAULT_SCHEMA_VERSION, DEFAULT_SCHEMA_VERSION + 1)));
  }

  // ── executable ────────────────────────────────────────────────────────────

  @Test
  public void testExecutableWithEmptyStreamSucceeds() {
    when(mockUpgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());

    PartitionedStream<EbeanAspectV2> emptyStream =
        PartitionedStream.<EbeanAspectV2>builder().delegateStream(Stream.empty()).build();
    when(mockMigratingDao.streamAspectBatches(any(), eq(0L), anyInt(), anyInt()))
        .thenReturn(emptyStream);

    MigrateAspectsStep step = buildStep(mockMigratingDao, Map.of("testAspect", 2L));
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    // entityService.ingestProposal is called by BootstrapStep.setUpgradeResult for the final state
    verify(mockEntityService, atLeastOnce()).ingestProposal(any(), any(), any(), anyBoolean());
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  private static final String UPGRADE_VERSION = "v0.14.1-abc123def456abc123def456abc123def456abc1";

  private MigrateAspectsStep buildStep(AspectDao dao, Map<String, Long> aspectTargetVersions) {
    return new MigrateAspectsStep(
        OP_CONTEXT, mockEntityService, dao, aspectTargetVersions, UPGRADE_VERSION, 100, 0, 0);
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
