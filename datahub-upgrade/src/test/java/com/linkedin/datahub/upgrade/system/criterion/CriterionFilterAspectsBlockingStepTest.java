package com.linkedin.datahub.upgrade.system.criterion;

import static com.linkedin.datahub.upgrade.system.AbstractMCLStep.LAST_URN_KEY;
import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.DATAHUB_VIEW_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATAHUB_VIEW_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE;
import static com.linkedin.metadata.aspect.validation.ConditionalWriteValidator.HTTP_HEADER_IF_VERSION_MATCH;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.view.DataHubViewDefinition;
import com.linkedin.view.DataHubViewInfo;
import com.linkedin.view.DataHubViewType;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CriterionFilterAspectsBlockingStepTest {

  private static final String UPGRADE_VERSION = "test-v0";
  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private EntityService<?> entityService;
  private AspectDao aspectDao;
  private UpgradeContext upgradeContext;
  private Upgrade upgrade;
  private UpgradeReport upgradeReport;
  private MockedStatic<EntityUtils> entityUtilsMock;

  @BeforeMethod
  public void setup() {
    entityService = mock(EntityService.class);
    aspectDao = mock(AspectDao.class);
    upgradeContext = mock(UpgradeContext.class);
    upgrade = mock(Upgrade.class);
    upgradeReport = mock(UpgradeReport.class);
    when(upgradeContext.upgrade()).thenReturn(upgrade);
    when(upgradeContext.report()).thenReturn(upgradeReport);
  }

  @AfterMethod
  public void tearDown() {
    if (entityUtilsMock != null) {
      entityUtilsMock.close();
      entityUtilsMock = null;
    }
  }

  // ── id() / stepId ─────────────────────────────────────────────────────────

  @Test
  public void testStepIdEncodesUpgradeVersion() {
    CriterionFilterAspectsBlockingStep step = buildStep();
    assertEquals(step.id(), CriterionFilterAspectsBlockingStep.stepId(UPGRADE_VERSION));
    assertTrue(step.id().startsWith(CriterionFilterAspectsBlockingStep.STEP_ID_PREFIX));
    assertTrue(step.id().endsWith(UPGRADE_VERSION));
  }

  @Test
  public void testGetUpgradeIdUrnIsDataHubUpgradeUrn() {
    CriterionFilterAspectsBlockingStep step = buildStep();
    Urn urn = step.getUpgradeIdUrn();
    assertEquals(urn.getEntityType(), Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
    assertEquals(urn, BootstrapStep.getUpgradeUrn(step.id()));
  }

  // ── skip() ────────────────────────────────────────────────────────────────

  @Test
  public void testSkipWhenPreviousSucceeded() {
    CriterionFilterAspectsBlockingStep step = buildStep();
    when(upgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(succeededResult()));
    assertTrue(step.skip(upgradeContext));
  }

  @Test
  public void testSkipWhenPreviousAborted() {
    CriterionFilterAspectsBlockingStep step = buildStep();
    when(upgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(abortedResult()));
    assertTrue(step.skip(upgradeContext));
  }

  @Test
  public void testSkipWhenPreviousInProgress() {
    CriterionFilterAspectsBlockingStep step = buildStep();
    DataHubUpgradeResult inProgress =
        new DataHubUpgradeResult().setState(DataHubUpgradeState.IN_PROGRESS);
    when(upgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(inProgress));
    assertFalse(step.skip(upgradeContext));
  }

  @Test
  public void testSkipWhenNoPreviousResult() {
    CriterionFilterAspectsBlockingStep step = buildStep();
    when(upgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());
    assertFalse(step.skip(upgradeContext));
  }

  // ── executable() ──────────────────────────────────────────────────────────

  @Test
  public void testExecutableEmptyStreamMarksSucceeded() {
    CriterionFilterAspectsBlockingStep step = buildStep();
    when(upgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());
    when(aspectDao.streamAspectBatches(any(RestoreIndicesArgs.class)))
        .thenReturn(
            PartitionedStream.<EbeanAspectV2>builder().delegateStream(Stream.empty()).build());

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    // The final state write goes through entityService.ingestProposal (via
    // BootstrapStep.setUpgradeResult).
    verify(entityService, atLeastOnce()).ingestProposal(any(), any(), any(), anyBoolean());
    // No work was done, so we never call ingestAspects.
    verify(entityService, never())
        .ingestAspects(any(), any(AspectsBatch.class), anyBoolean(), anyBoolean());
    verify(upgradeReport, atLeastOnce()).addLine(any());
  }

  @Test
  public void testExecutableScansBothSupportedAspectsAndForwardsBatchSizeAndLimit() {
    CriterionFilterAspectsBlockingStep step =
        new CriterionFilterAspectsBlockingStep(
            OP_CONTEXT,
            entityService,
            aspectDao,
            UPGRADE_VERSION,
            /* batchSize */ 250,
            /* batchDelayMs */ 0,
            /* limit */ 5000);
    when(upgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());
    when(aspectDao.streamAspectBatches(any(RestoreIndicesArgs.class)))
        .thenReturn(
            PartitionedStream.<EbeanAspectV2>builder().delegateStream(Stream.empty()).build());

    step.executable().apply(upgradeContext);

    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(aspectDao).streamAspectBatches(argsCaptor.capture());
    RestoreIndicesArgs args = argsCaptor.getValue();
    assertEquals(args.batchSize, 250);
    assertEquals(args.limit, 5000);
    assertNull(args.lastUrn);
    assertFalse(args.urnBasedPagination);
    assertEquals(
        args.aspectNames,
        List.of(DATAHUB_VIEW_INFO_ASPECT_NAME, Constants.DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME));
  }

  @Test
  public void testExecutableResumesFromLastUrnInPriorInProgress() {
    String resumeUrnStr = "urn:li:dataHubView:resume-from-here";
    DataHubUpgradeResult inProgress =
        new DataHubUpgradeResult()
            .setState(DataHubUpgradeState.IN_PROGRESS)
            .setResult(new StringMap(Map.of(LAST_URN_KEY, resumeUrnStr)));
    when(upgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(inProgress));
    when(aspectDao.streamAspectBatches(any(RestoreIndicesArgs.class)))
        .thenReturn(
            PartitionedStream.<EbeanAspectV2>builder().delegateStream(Stream.empty()).build());

    CriterionFilterAspectsBlockingStep step = buildStep();
    step.executable().apply(upgradeContext);

    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(aspectDao).streamAspectBatches(argsCaptor.capture());
    assertEquals(argsCaptor.getValue().lastUrn, resumeUrnStr);
    assertTrue(argsCaptor.getValue().urnBasedPagination);
  }

  @Test
  public void testExecutableInProgressWithoutResultDoesNotResume() {
    // IN_PROGRESS with no result map → no LAST_URN_KEY → resume URN should be null
    DataHubUpgradeResult inProgress =
        new DataHubUpgradeResult().setState(DataHubUpgradeState.IN_PROGRESS);
    when(upgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenReturn(Optional.of(inProgress));
    when(aspectDao.streamAspectBatches(any(RestoreIndicesArgs.class)))
        .thenReturn(
            PartitionedStream.<EbeanAspectV2>builder().delegateStream(Stream.empty()).build());

    CriterionFilterAspectsBlockingStep step = buildStep();
    step.executable().apply(upgradeContext);

    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(aspectDao).streamAspectBatches(argsCaptor.capture());
    assertNull(argsCaptor.getValue().lastUrn);
    assertFalse(argsCaptor.getValue().urnBasedPagination);
  }

  @Test
  public void testExecutableIngestsBatchAndSavesProgress() {
    Urn viewUrn = UrnUtils.getUrn("urn:li:dataHubView:test-view-1");
    SystemAspect prepared = newMockViewSystemAspectWithLegacyValue(viewUrn, "42");
    setupStaticEntityUtilsToReturn(prepared);

    EbeanAspectV2 ebeanAspect = mock(EbeanAspectV2.class);
    PartitionedStream<EbeanAspectV2> mockStream = mock(PartitionedStream.class);
    when(mockStream.partition(org.mockito.ArgumentMatchers.anyInt()))
        .thenReturn(Stream.of(Stream.of(ebeanAspect)));
    when(aspectDao.streamAspectBatches(any(RestoreIndicesArgs.class))).thenReturn(mockStream);

    when(upgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    CriterionFilterAspectsBlockingStep step = buildStep();
    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Ingest happens via ingestAspects(opCtx, batch, emitMCL=true, overwrite=false).
    ArgumentCaptor<AspectsBatch> batchCaptor = ArgumentCaptor.forClass(AspectsBatch.class);
    verify(entityService).ingestAspects(eq(OP_CONTEXT), batchCaptor.capture(), eq(true), eq(false));
    AspectsBatch batch = batchCaptor.getValue();
    assertNotNull(batch);
    assertFalse(batch.getItems().isEmpty());

    // Conditional-write header should be propagated from the row's SystemMetadata.version.
    com.linkedin.metadata.aspect.batch.MCPItem firstItem =
        (com.linkedin.metadata.aspect.batch.MCPItem) batch.getItems().iterator().next();
    assertEquals(firstItem.getHeader(HTTP_HEADER_IF_VERSION_MATCH).orElse(null), "42");

    // APP_SOURCE should be stamped on system metadata.
    assertEquals(
        firstItem.getSystemMetadata().getProperties().get(APP_SOURCE), SYSTEM_UPDATE_SOURCE);

    // Mid-sweep progress checkpoint should be IN_PROGRESS with the last urn.
    verify(upgrade)
        .setUpgradeResult(
            any(),
            any(Urn.class),
            any(),
            eq(DataHubUpgradeState.IN_PROGRESS),
            eq(Map.of(LAST_URN_KEY, viewUrn.toString())));
  }

  @Test
  public void testExecutableHonorsBatchDelayMs() {
    Urn viewUrn = UrnUtils.getUrn("urn:li:dataHubView:test-delay-1");
    SystemAspect prepared = newMockViewSystemAspectWithLegacyValue(viewUrn, null);
    setupStaticEntityUtilsToReturn(prepared);

    EbeanAspectV2 ebeanAspect = mock(EbeanAspectV2.class);
    PartitionedStream<EbeanAspectV2> mockStream = mock(PartitionedStream.class);
    when(mockStream.partition(org.mockito.ArgumentMatchers.anyInt()))
        .thenReturn(Stream.of(Stream.of(ebeanAspect)));
    when(aspectDao.streamAspectBatches(any(RestoreIndicesArgs.class))).thenReturn(mockStream);
    when(upgrade.getUpgradeResult(any(), any(Urn.class), any())).thenReturn(Optional.empty());

    CriterionFilterAspectsBlockingStep step =
        new CriterionFilterAspectsBlockingStep(
            OP_CONTEXT,
            entityService,
            aspectDao,
            UPGRADE_VERSION,
            /* batchSize */ 100,
            /* batchDelayMs */ 1, // 1ms exercises sleep path without slowing CI
            /* limit */ 0);

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  // ── prepareSystemAspectForBlockingIngest edge case ────────────────────────

  @Test
  public void testPrepareReturnsOriginalWhenRecordTemplateIsNull() {
    SystemAspect sa = mock(SystemAspect.class);
    when(sa.getAspectName()).thenReturn(DATAHUB_VIEW_INFO_ASPECT_NAME);
    when(sa.getRecordTemplate()).thenReturn(null);
    org.testng.Assert.assertSame(
        CriterionFilterAspectsBlockingStep.prepareSystemAspectForBlockingIngest(sa), sa);
  }

  // ── private helpers covered via reflection ────────────────────────────────

  @Test
  public void testVersionHeadersNullReturnsEmptyMap() throws Exception {
    Method m =
        CriterionFilterAspectsBlockingStep.class.getDeclaredMethod("versionHeaders", String.class);
    m.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, String> result = (Map<String, String>) m.invoke(null, (String) null);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testVersionHeadersSetsIfVersionMatchHeader() throws Exception {
    Method m =
        CriterionFilterAspectsBlockingStep.class.getDeclaredMethod("versionHeaders", String.class);
    m.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, String> result = (Map<String, String>) m.invoke(null, "123");
    assertEquals(result.get(HTTP_HEADER_IF_VERSION_MATCH), "123");
  }

  @Test
  public void testWithAppSourceNullInputCreatesSystemMetadataWithAppSource() throws Exception {
    Method m =
        CriterionFilterAspectsBlockingStep.class.getDeclaredMethod(
            "withAppSource", SystemMetadata.class);
    m.setAccessible(true);

    SystemMetadata result = (SystemMetadata) m.invoke(null, (SystemMetadata) null);
    assertNotNull(result);
    assertEquals(result.getProperties().get(APP_SOURCE), SYSTEM_UPDATE_SOURCE);
  }

  @Test
  public void testWithAppSourceCopiesExistingPropertiesAndStampsAppSource() throws Exception {
    Method m =
        CriterionFilterAspectsBlockingStep.class.getDeclaredMethod(
            "withAppSource", SystemMetadata.class);
    m.setAccessible(true);

    SystemMetadata input = new SystemMetadata();
    StringMap props = new StringMap();
    props.put("existingKey", "existingValue");
    input.setProperties(props);

    SystemMetadata result = (SystemMetadata) m.invoke(null, input);

    assertEquals(result.getProperties().get("existingKey"), "existingValue");
    assertEquals(result.getProperties().get(APP_SOURCE), SYSTEM_UPDATE_SOURCE);
    // The original input must not be mutated.
    assertNull(input.getProperties().get(APP_SOURCE));
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  private CriterionFilterAspectsBlockingStep buildStep() {
    return new CriterionFilterAspectsBlockingStep(
        OP_CONTEXT, entityService, aspectDao, UPGRADE_VERSION, 100, 0, 0);
  }

  /**
   * Build a {@link SystemAspect} mock that mimics how the step's {@link
   * CriterionFilterAspectsBlockingStep#prepareSystemAspectForBlockingIngest} chain produces an
   * already-sanitized aspect. We bypass running the static prepare method against a mock and
   * instead have {@link SystemAspect#copy()} → {@code setRecordTemplate(...)} return the same mock
   * with all required fields wired up for downstream {@link
   * com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl#build} validation.
   */
  private SystemAspect newMockViewSystemAspectWithLegacyValue(Urn urn, String aspectVersion) {
    EntitySpec entitySpec = OP_CONTEXT.getEntityRegistry().getEntitySpec(DATAHUB_VIEW_ENTITY_NAME);
    AspectSpec aspectSpec = entitySpec.getAspectSpec(DATAHUB_VIEW_INFO_ASPECT_NAME);

    DataHubViewInfo viewInfo = newViewInfoWithLegacyValue();
    AuditStamp auditStamp =
        new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:tester")).setTime(1L);
    SystemMetadata systemMetadata = new SystemMetadata();
    if (aspectVersion != null) {
      systemMetadata.setVersion(aspectVersion);
    }

    SystemAspect sa = mock(SystemAspect.class);
    when(sa.getUrn()).thenReturn(urn);
    when(sa.getAspectName()).thenReturn(DATAHUB_VIEW_INFO_ASPECT_NAME);
    when(sa.getEntitySpec()).thenReturn(entitySpec);
    when(sa.getAspectSpec()).thenReturn(aspectSpec);
    when(sa.getRecordTemplate()).thenReturn(viewInfo);
    when(sa.getAuditStamp()).thenReturn(auditStamp);
    when(sa.getSystemMetadata()).thenReturn(systemMetadata);
    when(sa.copy()).thenReturn(sa);
    when(sa.setRecordTemplate(any())).thenReturn(sa);
    return sa;
  }

  /**
   * Produces a valid {@link DataHubViewInfo} but injects a legacy {@code criterion.value} string
   * directly into the underlying DataMap so the prepare step's sanitization runs end-to-end.
   */
  private static DataHubViewInfo newViewInfoWithLegacyValue() {
    DataHubViewInfo info =
        new DataHubViewInfo()
            .setName("test")
            .setType(DataHubViewType.PERSONAL)
            .setDescription("test view")
            .setCreated(
                new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:tester")).setTime(0L))
            .setLastModified(
                new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:tester")).setTime(0L))
            .setDefinition(
                new DataHubViewDefinition()
                    .setEntityTypes(new StringArray())
                    .setFilter(new Filter()));

    DataMap criterion = new DataMap();
    criterion.put("field", "platform");
    criterion.put("value", "snowflake");
    DataList criteria = new DataList();
    criteria.add(criterion);
    DataMap filter = new DataMap();
    filter.put("criteria", criteria);
    info.data().getDataMap("definition").put("filter", filter);
    return info;
  }

  private void setupStaticEntityUtilsToReturn(SystemAspect prepared) {
    entityUtilsMock = mockStatic(EntityUtils.class);
    entityUtilsMock
        .when(() -> EntityUtils.toSystemAspectFromEbeanAspects(any(), any()))
        .thenReturn(List.of(prepared));
  }

  private static DataHubUpgradeResult succeededResult() {
    return new DataHubUpgradeResult().setState(DataHubUpgradeState.SUCCEEDED);
  }

  private static DataHubUpgradeResult abortedResult() {
    return new DataHubUpgradeResult().setState(DataHubUpgradeState.ABORTED);
  }
}
