package com.linkedin.datahub.upgrade.assertions;

import static com.linkedin.metadata.Constants.ASSERTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.ASSERTION_NOTE_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionNote;
import com.linkedin.assertion.AssertionType;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.system.assertions.MigrateAssertionNoteToAspectStep;
import com.linkedin.datahub.upgrade.system.assertions.MigrateAssertionNoteToAspectStep.MigrationCounts;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MigrateAssertionNoteToAspectStepTest {

  @Mock private OperationContext mockOpContext;
  @Mock private EntityService<?> mockEntityService;
  @Mock private AspectDao mockAspectDao;
  @Mock private RetrieverContext mockRetrieverContext;

  private MigrateAssertionNoteToAspectStep step;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    step =
        new MigrateAssertionNoteToAspectStep(
            mockOpContext, mockEntityService, mockAspectDao, 10, 0, 0);
    when(mockOpContext.getRetrieverContext()).thenReturn(mockRetrieverContext);
  }

  // ---- structural tests ----

  @Test
  public void testId() {
    assertEquals(step.id(), MigrateAssertionNoteToAspectStep.STEP_ID);
  }

  @Test
  public void testGetUrnLike() {
    assertEquals(step.getUrnLike(), "urn:li:assertion:%");
  }

  // ---- skip() tests ----

  @Test
  public void testSkip_noHistory() {
    UpgradeContext ctx = mockContext(Optional.empty());
    assertFalse(step.skip(ctx));
  }

  @Test
  public void testSkip_succeededState() {
    UpgradeContext ctx = mockContext(Optional.of(stateResult(DataHubUpgradeState.SUCCEEDED)));
    assertTrue(step.skip(ctx));
  }

  @Test
  public void testSkip_abortedState() {
    UpgradeContext ctx = mockContext(Optional.of(stateResult(DataHubUpgradeState.ABORTED)));
    assertTrue(step.skip(ctx));
  }

  @Test
  public void testSkip_inProgressState() {
    // IN_PROGRESS means the previous run was interrupted; must not skip so it can resume.
    UpgradeContext ctx = mockContext(Optional.of(stateResult(DataHubUpgradeState.IN_PROGRESS)));
    assertFalse(step.skip(ctx));
  }

  // ---- executable() plumbing tests ----

  /** Empty stream → SUCCEEDED immediately with no migration writes. */
  @Test
  public void testExecutable_emptyStream() {
    UpgradeContext ctx = mockContextWithReport(Optional.empty());
    mockEmptyStream();

    UpgradeStepResult result = step.executable().apply(ctx);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  /** SQL args are configured with the correct aspect name, batch size, and URN filter. */
  @Test
  public void testExecutable_streamArgsConfigured() {
    mockContextWithReport(Optional.empty());
    UpgradeContext ctx = mockContextWithReport(Optional.empty());
    mockEmptyStream();

    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);

    step.executable().apply(ctx);

    verify(mockAspectDao)
        .streamAspectBatches(any(OperationContext.class), argsCaptor.capture(), any());
    RestoreIndicesArgs args = argsCaptor.getValue();
    assertEquals(args.aspectNames().get(0), ASSERTION_INFO_ASPECT_NAME);
    assertEquals(args.batchSize(), 10);
    assertEquals(args.urnLike(), "urn:li:assertion:%");
  }

  // ---- buildMigrationProposals() unit tests ----
  // These test the core filtering logic in isolation without needing to mock static methods
  // or set up the full SQL streaming path.

  /** An assertion WITH a note and NO existing assertionNote → should produce a proposal. */
  @Test
  public void testBuildMigrationProposals_writesNoteWhenMissing() throws URISyntaxException {
    SystemAspect infoAspect = mockInfoAspect("urn:li:assertion:a1", buildInfoWithNote("fix this"));

    MigrationCounts counts = new MigrationCounts(new AtomicLong(), new AtomicLong());
    List<MetadataChangeProposal> proposals =
        step.buildMigrationProposals(List.of(infoAspect), Map.of(), counts);

    assertEquals(proposals.size(), 1);
    assertEquals(proposals.get(0).getAspectName(), ASSERTION_NOTE_ASPECT_NAME);
    assertEquals(proposals.get(0).getEntityUrn().toString(), "urn:li:assertion:a1");
    assertEquals(counts.noNote.get(), 0L);
    assertEquals(counts.skipped.get(), 0L);
  }

  /** An assertion WITH a note that ALREADY HAS assertionNote → must not be overwritten. */
  @Test
  public void testBuildMigrationProposals_skipsWhenNoteAlreadyExists() throws URISyntaxException {
    String urnStr = "urn:li:assertion:a2";
    SystemAspect infoAspect = mockInfoAspect(urnStr, buildInfoWithNote("old note"));
    SystemAspect existingNoteAspect = mock(SystemAspect.class);

    Map<String, Map<String, SystemAspect>> existing =
        Map.of(urnStr, Map.of(ASSERTION_NOTE_ASPECT_NAME, existingNoteAspect));

    MigrationCounts counts = new MigrationCounts(new AtomicLong(), new AtomicLong());
    List<MetadataChangeProposal> proposals =
        step.buildMigrationProposals(List.of(infoAspect), existing, counts);

    assertTrue(proposals.isEmpty());
    assertEquals(counts.skipped.get(), 1L);
    assertEquals(counts.noNote.get(), 0L);
  }

  /** An assertion WITHOUT a note → nothing to migrate. */
  @Test
  public void testBuildMigrationProposals_skipsWhenNoNote() throws URISyntaxException {
    AssertionInfo infoWithoutNote = new AssertionInfo().setType(AssertionType.DATASET);
    SystemAspect infoAspect = mockInfoAspect("urn:li:assertion:a3", infoWithoutNote);

    MigrationCounts counts = new MigrationCounts(new AtomicLong(), new AtomicLong());
    List<MetadataChangeProposal> proposals =
        step.buildMigrationProposals(List.of(infoAspect), Map.of(), counts);

    assertTrue(proposals.isEmpty());
    assertEquals(counts.noNote.get(), 1L);
    assertEquals(counts.skipped.get(), 0L);
  }

  /** Mixed batch: one with a note, one already migrated, one with no note. */
  @Test
  public void testBuildMigrationProposals_mixedBatch() throws URISyntaxException {
    String urn1 = "urn:li:assertion:b1"; // needs migration
    String urn2 = "urn:li:assertion:b2"; // already has assertionNote
    String urn3 = "urn:li:assertion:b3"; // no note at all

    SystemAspect aspect1 = mockInfoAspect(urn1, buildInfoWithNote("note content"));
    SystemAspect aspect2 = mockInfoAspect(urn2, buildInfoWithNote("existing note"));
    SystemAspect aspect3 = mockInfoAspect(urn3, new AssertionInfo().setType(AssertionType.DATASET));

    SystemAspect existingAspect2 = mock(SystemAspect.class);
    Map<String, Map<String, SystemAspect>> existing =
        Map.of(urn2, Map.of(ASSERTION_NOTE_ASPECT_NAME, existingAspect2));

    MigrationCounts counts = new MigrationCounts(new AtomicLong(), new AtomicLong());
    List<MetadataChangeProposal> proposals =
        step.buildMigrationProposals(List.of(aspect1, aspect2, aspect3), existing, counts);

    assertEquals(proposals.size(), 1);
    assertEquals(proposals.get(0).getEntityUrn().toString(), urn1);
    assertEquals(counts.noNote.get(), 1L);
    assertEquals(counts.skipped.get(), 1L);
  }

  // ---- fault tolerance test ----

  // ---- helpers ----

  private UpgradeContext mockContext(Optional<DataHubUpgradeResult> result) {
    UpgradeContext ctx = mock(UpgradeContext.class);
    Upgrade upgrade = mock(Upgrade.class);
    when(ctx.upgrade()).thenReturn(upgrade);
    when(upgrade.getUpgradeResult(any(), any(), any())).thenReturn(result);
    return ctx;
  }

  private UpgradeContext mockContextWithReport(Optional<DataHubUpgradeResult> result) {
    UpgradeContext ctx = mockContext(result);
    UpgradeReport report = mock(UpgradeReport.class);
    when(ctx.report()).thenReturn(report);
    return ctx;
  }

  private void mockEmptyStream() {
    @SuppressWarnings("unchecked")
    PartitionedStream<EbeanAspectV2> mockStream = mock(PartitionedStream.class);
    when(mockAspectDao.streamAspectBatches(
            any(OperationContext.class), any(RestoreIndicesArgs.class), any()))
        .thenAnswer(
            inv ->
                ((java.util.function.Function<PartitionedStream<EbeanAspectV2>, Object>)
                        inv.getArgument(2))
                    .apply(mockStream));
    when(mockStream.partition(anyInt())).thenReturn(Stream.empty());
  }

  private static DataHubUpgradeResult stateResult(DataHubUpgradeState state) {
    DataHubUpgradeResult r = mock(DataHubUpgradeResult.class);
    when(r.getState()).thenReturn(state);
    return r;
  }

  private static AssertionInfo buildInfoWithNote(String content) {
    return new AssertionInfo()
        .setType(AssertionType.DATASET)
        .setNote(
            new AssertionNote()
                .setContent(content)
                .setLastModified(
                    new AuditStamp()
                        .setTime(0L)
                        .setActor(UrnUtils.getUrn("urn:li:corpuser:test"))));
  }

  private static SystemAspect mockInfoAspect(String urnStr, AssertionInfo info)
      throws URISyntaxException {
    SystemAspect aspect = mock(SystemAspect.class);
    RecordTemplate record = mock(RecordTemplate.class);
    when(aspect.getUrn()).thenReturn(Urn.createFromString(urnStr));
    when(aspect.getRecordTemplate()).thenReturn(record);
    when(record.data()).thenReturn(info.data());
    return aspect;
  }
}
