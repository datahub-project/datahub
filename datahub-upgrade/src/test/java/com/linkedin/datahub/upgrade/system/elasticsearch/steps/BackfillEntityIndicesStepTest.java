package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.loadindices.AspectIndexLoader;
import com.linkedin.datahub.upgrade.loadindices.LoadIndicesArgs;
import com.linkedin.datahub.upgrade.loadindices.LoadIndicesResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.BackfillHeaders;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3IndexKeys;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState.BackfillCheckpoint;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState.Status;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.annotation.TxIsolation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BackfillEntityIndicesStepTest {

  private static final String UPGRADE_VERSION = "0.14.0-0";

  @Mock private EntityService<?> entityService;
  @Mock private ESIndexBuilder indexBuilder;
  @Mock private ElasticSearchIndexed indexedService;
  @Mock private UpgradeContext upgradeContext;
  @Mock private UpgradeReport upgradeReport;
  @Mock private Upgrade upgrade;
  @Mock private AspectIndexLoader loader;
  @Mock private UpdateIndicesService updateIndicesService;

  private OperationContext opContext;
  private String alias;
  private List<String> entityNames;
  private final AtomicReference<DataHubUpgradeResult> storedPhase1 = new AtomicReference<>();

  @BeforeMethod
  public void setup() throws Exception {
    MockitoAnnotations.openMocks(this);
    IndexUtils.clearReindexConfigCache();
    opContext = TestOperationContexts.systemContextNoValidate();

    String key = V3IndexKeys.resolve(opContext.getEntityRegistry().getEntitySpec("dataset"));
    alias = opContext.getSearchContext().getIndexConvention().getEntityIndexNameV3(opContext, key);
    entityNames =
        V3IndexKeys.entitySpecsForKey(opContext.getEntityRegistry(), key).stream()
            .filter(BackfillEntityIndicesStep::producesSearchDocuments)
            .map(EntitySpec::getName)
            .sorted(Comparator.naturalOrder())
            .collect(Collectors.toList());
    assertEquals(entityNames.size(), 1, "one index per entity type: the fill walks exactly one");

    when(upgradeContext.opContext()).thenReturn(opContext);
    when(upgradeContext.upgrade()).thenReturn(upgrade);
    when(upgradeContext.report()).thenReturn(upgradeReport);
    when(upgrade.getUpgradeResult(any(), any(), any()))
        .thenAnswer(invocation -> Optional.ofNullable(storedPhase1.get()));
    // Persisted state accumulates the way it does against a real store: every merge reads back
    // the previous write.
    when(entityService.getLatestEnvelopedAspect(any(), any(), any(), any()))
        .thenAnswer(
            invocation ->
                storedPhase1.get() == null
                    ? null
                    : new EnvelopedAspect().setValue(new Aspect(storedPhase1.get().data())));
    when(entityService.ingestProposal(any(), any(), any(), anyBoolean()))
        .thenAnswer(
            invocation -> {
              MetadataChangeProposal proposal = invocation.getArgument(1);
              storedPhase1.set(
                  GenericRecordUtils.deserializeAspect(
                      proposal.getAspect().getValue(),
                      proposal.getAspect().getContentType(),
                      DataHubUpgradeResult.class));
              return mock(IngestResult.class);
            });

    ReindexConfig config = mock(ReindexConfig.class);
    when(config.name()).thenReturn(alias);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(config));
    when(indexedService.getIndexBuilder()).thenReturn(indexBuilder);
    when(indexedService.getIndexBuilder(anyString())).thenReturn(indexBuilder);
    when(indexBuilder.resumedReindexMatchesTarget(
            any(OperationContext.class), any(ReindexConfig.class), anyString()))
        .thenReturn(true);
    // The live name is an alias with a backing index unless a test says otherwise.
    when(indexBuilder.getBackingIndices(any(OperationContext.class), anyString()))
        .thenReturn(Set.of(alias + "_backing"));

    when(loader.load(any(), any(), any(), any(), any())).thenAnswer(this::fakeLoad);
  }

  /** Every entity type yields one batch of 5 aspects over 3 URNs. */
  private LoadIndicesResult fakeLoad(InvocationOnMock invocation) {
    LoadIndicesArgs args = invocation.getArgument(1);
    AspectIndexLoader.BatchListener listener = invocation.getArgument(3);
    if (listener != null) {
      listener.onBatch(args.urnLike.replace("%", "last"), 5, 3);
    }
    LoadIndicesResult result = new LoadIndicesResult();
    result.rowsProcessed = 5;
    result.urnsProcessed = 3;
    return result;
  }

  private BackfillEntityIndicesStep createStep(BuildIndicesConfiguration config) {
    return new BackfillEntityIndicesStep(
        opContext,
        entityService,
        List.of(indexedService),
        Set.of(),
        UPGRADE_VERSION,
        config,
        loader,
        updateIndicesService);
  }

  private void seedPhase1(Map<String, String> state) {
    storedPhase1.set(
        new DataHubUpgradeResult()
            .setState(DataHubUpgradeState.IN_PROGRESS)
            .setResult(new StringMap(state)));
  }

  private Map<String, String> persistedState() {
    return storedPhase1.get().getResult();
  }

  @Test
  public void testDefaultPageSizeKeepsAComfortableMarginOverTheLargestEntity() {
    // Adding aspects to an entity type erodes this margin; when it fails, raise the default page
    // size (application.yaml and DEFAULT_BATCH_SIZE) rather than relaxing the multiple.
    int largest = BackfillEntityIndicesStep.largestEntityAspectCount(opContext.getEntityRegistry());
    assertTrue(largest > 0);
    assertTrue(
        BackfillEntityIndicesStep.DEFAULT_BATCH_SIZE
            >= BackfillEntityIndicesStep.PAGE_SIZE_ENTITY_MULTIPLE * largest,
        "default page size "
            + BackfillEntityIndicesStep.DEFAULT_BATCH_SIZE
            + " holds fewer than "
            + BackfillEntityIndicesStep.PAGE_SIZE_ENTITY_MULTIPLE
            + " entities of the largest type ("
            + largest
            + " aspects)");
  }

  @Test
  public void testEntityTypesWithoutSearchableAspectsAreNotFilled() {
    // The default search group holds system entity types that never produce a search document;
    // streaming them would inflate the document count the alias swap is validated against.
    String defaultKey =
        V3IndexKeys.resolve(opContext.getEntityRegistry().getEntitySpec("dataHubUpgrade"));
    Collection<EntitySpec> all =
        V3IndexKeys.entitySpecsForKey(opContext.getEntityRegistry(), defaultKey);
    List<String> skipped =
        all.stream()
            .filter(spec -> !BackfillEntityIndicesStep.producesSearchDocuments(spec))
            .map(EntitySpec::getName)
            .collect(Collectors.toList());
    assertTrue(skipped.contains("dataHubUpgrade"), "expected a document-less type, got " + skipped);
    assertTrue(
        BackfillEntityIndicesStep.producesSearchDocuments(
            opContext.getEntityRegistry().getEntitySpec("dataset")));
  }

  @Test
  public void testPageSizeThatCannotHoldOneEntityFailsBeforeAnyFill() {
    seedPhase1(IncrementalReindexState.setBackfillPending(null, alias, alias, 1000L));
    BuildIndicesConfiguration config = new BuildIndicesConfiguration();
    config.setBackfillBatchSize(1);

    UpgradeStepResult result = createStep(config).executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(loader, never()).load(any(), any(), any(), any(), any());
  }

  private List<LoadIndicesArgs> loadArgs() {
    ArgumentCaptor<LoadIndicesArgs> captor = ArgumentCaptor.forClass(LoadIndicesArgs.class);
    verify(loader, atLeast(0)).load(any(), captor.capture(), any(), any(), any());
    return new ArrayList<>(captor.getAllValues());
  }

  @Test
  public void testNothingToFillSucceedsWithoutTouchingTheLoader() throws Exception {
    seedPhase1(
        IncrementalReindexState.setPhase1State(
            null, "datasetindex_v2", "next", "old", 1L, 2L, null, false, Status.COMPLETED));

    UpgradeStepResult result = createStep(null).executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(loader, never()).load(any(), any(), any(), any(), any());
  }

  @Test
  public void testInPlaceFillWalksEveryEntityType() throws Exception {
    seedPhase1(IncrementalReindexState.setBackfillPending(null, alias, alias, 1000L));

    UpgradeStepResult result = createStep(null).executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    List<LoadIndicesArgs> runs = loadArgs();
    assertEquals(
        runs.stream().map(args -> args.urnLike).collect(Collectors.toList()),
        entityNames.stream().map(name -> "urn:li:" + name + ":%").collect(Collectors.toList()));
    for (LoadIndicesArgs args : runs) {
      assertEquals(args.v3TargetIndex, alias);
      assertEquals(args.v3BackfillMode, BackfillHeaders.MODE_BASELINE);
      assertEquals(args.batchSize, BackfillEntityIndicesStep.DEFAULT_BATCH_SIZE);
      assertEquals(args.txIsolation, TxIsolation.READ_COMMITED);
      assertNull(args.deadlineEpochMs);
    }

    Map<String, String> state = persistedState();
    assertEquals(IncrementalReindexState.getStatus(state, alias), Optional.of(Status.COMPLETED));
    assertTrue(
        state.containsKey(
            IncrementalReindexState.key(alias, IncrementalReindexState.BACKFILL_COMPLETE_TIME)));
    assertEquals(
        IncrementalReindexState.getBackfillUrnsProcessed(state, alias),
        Optional.of(3L * entityNames.size()));
    assertEquals(
        IncrementalReindexState.getBackfillCheckpoint(state, alias).get().processedCount(),
        5L * entityNames.size());
    // In place: the alias already points at the filled index, nothing to swap.
    verify(indexBuilder, never()).swapAlias(any(OperationContext.class), anyString(), anyString());
  }

  @Test
  public void testResumeStartsAtTheCheckpointedUrn() throws Exception {
    String entity = entityNames.get(0);
    String cursorUrn = "urn:li:" + entity + ":resume-here";
    Map<String, String> state =
        IncrementalReindexState.setBackfillPending(null, alias, alias, 1000L);
    state =
        IncrementalReindexState.setBackfillCheckpoint(
            state, alias, new BackfillCheckpoint(entity, cursorUrn, 40L, 0L));
    state = IncrementalReindexState.setBackfillUrnsProcessed(state, alias, 12L);
    seedPhase1(state);

    UpgradeStepResult result = createStep(null).executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    List<LoadIndicesArgs> runs = loadArgs();
    assertEquals(runs.size(), 1);
    assertEquals(runs.get(0).urnLike, "urn:li:" + entity + ":%");
    assertEquals(runs.get(0).lastUrn, cursorUrn);
    Map<String, String> persisted = persistedState();
    assertEquals(
        IncrementalReindexState.getBackfillUrnsProcessed(persisted, alias),
        Optional.of(12L + 3L * runs.size()));
    assertEquals(
        IncrementalReindexState.getBackfillCheckpoint(persisted, alias).get().processedCount(),
        40L + 5L * runs.size());
  }

  @Test
  public void testRebuildSwapsTheAliasOntoTheNextIndexAfterTheFill() throws Exception {
    String next = alias + "_0_14_0-0_1679000000000";
    seedPhase1(IncrementalReindexState.setBackfillPending(null, alias, next, 1000L));

    UpgradeStepResult result = createStep(null).executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    for (LoadIndicesArgs args : loadArgs()) {
      assertEquals(args.v3TargetIndex, next);
    }
    // The destination is verified before the first write and again before it goes live; the swap
    // itself carries no document count.
    verify(indexBuilder, times(2))
        .resumedReindexMatchesTarget(eq(opContext), any(ReindexConfig.class), eq(next));
    verify(indexBuilder).swapAlias(opContext, alias, next);
    // The mark stays incomplete until catch-up has replayed the window; catch-up sets it.
    verify(indexBuilder, never()).markMigrationComplete(any(OperationContext.class), anyString());
    verify(indexBuilder, never()).getCount(any(OperationContext.class), anyString());
    verify(indexBuilder, never())
        .validateAndSwapAlias(any(OperationContext.class), anyString(), anyString(), anyLong());
    assertTrue(
        persistedState()
            .containsKey(
                IncrementalReindexState.key(
                    alias, IncrementalReindexState.BACKFILL_COMPLETE_TIME)));
  }

  @Test
  public void testRebuildOfAConcreteIndexRetainsItBeforeTheSwap() throws Exception {
    // A V3 index created before indices were aliased owns the convention name; the swap must
    // delete it to give the alias the name, so a copy is kept and recorded for rollback first.
    String next = alias + "_0_14_0-0_1679000000000";
    String retained = alias + "_0_14_0-0_1679000001000";
    seedPhase1(IncrementalReindexState.setBackfillPending(null, alias, next, 1000L));
    when(indexBuilder.getBackingIndices(any(OperationContext.class), eq(alias)))
        .thenReturn(Set.of());
    ESIndexBuilder.IncrementalReindexResult copy =
        new ESIndexBuilder.IncrementalReindexResult(
            retained, 1679000001000L, "copy-task", false, 1, 42L, Map.of());
    when(indexBuilder.startRollbackCopy(
            eq(opContext), any(ReindexConfig.class), eq(UPGRADE_VERSION)))
        .thenReturn(copy);

    UpgradeStepResult result = createStep(null).executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    InOrder inOrder = inOrder(indexBuilder);
    inOrder
        .verify(indexBuilder)
        .startRollbackCopy(eq(opContext), any(ReindexConfig.class), eq(UPGRADE_VERSION));
    inOrder
        .verify(indexBuilder)
        .finishRollbackCopy(eq(opContext), any(ReindexConfig.class), eq(copy));
    inOrder.verify(indexBuilder).swapAlias(opContext, alias, next);
    verify(indexBuilder, never()).deleteActionWithRetry(any(OperationContext.class), anyString());
    Map<String, String> state = persistedState();
    assertEquals(
        IncrementalReindexState.get(state, alias, IncrementalReindexState.OLD_BACKING_INDEX_NAME),
        Optional.of(retained));
    assertEquals(
        IncrementalReindexState.getStatus(state, alias),
        Optional.of(IncrementalReindexState.Status.COMPLETED));
  }

  @Test
  public void testFailedRollbackCopyIsDeletedAndFailsTheRow() throws Exception {
    String next = alias + "_0_14_0-0_1679000000000";
    String retained = alias + "_0_14_0-0_1679000001000";
    seedPhase1(IncrementalReindexState.setBackfillPending(null, alias, next, 1000L));
    when(indexBuilder.getBackingIndices(any(OperationContext.class), eq(alias)))
        .thenReturn(Set.of());
    ESIndexBuilder.IncrementalReindexResult copy =
        new ESIndexBuilder.IncrementalReindexResult(
            retained, 1679000001000L, "copy-task", false, 1, 42L, Map.of());
    when(indexBuilder.startRollbackCopy(
            eq(opContext), any(ReindexConfig.class), eq(UPGRADE_VERSION)))
        .thenReturn(copy);
    doThrow(new IllegalStateException("copy did not complete"))
        .when(indexBuilder)
        .finishRollbackCopy(eq(opContext), any(ReindexConfig.class), eq(copy));

    UpgradeStepResult result = createStep(null).executable().apply(upgradeContext);

    // The copy's reindex is stopped before the copy goes, the row no longer names it, and FAILED is
    // re-planned.
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    InOrder cleanup = inOrder(indexBuilder);
    cleanup.verify(indexBuilder).cancelRollbackCopy(opContext, copy);
    cleanup.verify(indexBuilder).deleteActionWithRetry(opContext, retained);
    verify(indexBuilder, never()).swapAlias(any(OperationContext.class), anyString(), anyString());
    Map<String, String> state = persistedState();
    assertEquals(IncrementalReindexState.getStatus(state, alias), Optional.of(Status.FAILED));
    assertFalse(
        state.containsKey(
            IncrementalReindexState.key(alias, IncrementalReindexState.OLD_BACKING_INDEX_NAME)));
  }

  @Test
  public void testACopyWhoseReindexCannotBeConfirmedStoppedIsLeftInPlace() throws Exception {
    // Deleting an index under a live writer only has the writer recreate it, so an unconfirmed
    // cancel leaves the copy for cleanup; the row still fails and no longer names it.
    String next = alias + "_0_14_0-0_1679000000000";
    String retained = alias + "_0_14_0-0_1679000001000";
    seedPhase1(IncrementalReindexState.setBackfillPending(null, alias, next, 1000L));
    when(indexBuilder.getBackingIndices(any(OperationContext.class), eq(alias)))
        .thenReturn(Set.of());
    ESIndexBuilder.IncrementalReindexResult copy =
        new ESIndexBuilder.IncrementalReindexResult(
            retained, 1679000001000L, "copy-task", false, 1, 42L, Map.of());
    when(indexBuilder.startRollbackCopy(
            eq(opContext), any(ReindexConfig.class), eq(UPGRADE_VERSION)))
        .thenReturn(copy);
    doThrow(new IllegalStateException("copy did not complete"))
        .when(indexBuilder)
        .finishRollbackCopy(eq(opContext), any(ReindexConfig.class), eq(copy));
    doThrow(new IllegalStateException("task not confirmed stopped"))
        .when(indexBuilder)
        .cancelRollbackCopy(opContext, copy);

    UpgradeStepResult result = createStep(null).executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(indexBuilder, never()).deleteActionWithRetry(any(OperationContext.class), anyString());
    Map<String, String> state = persistedState();
    assertEquals(IncrementalReindexState.getStatus(state, alias), Optional.of(Status.FAILED));
    assertFalse(
        state.containsKey(
            IncrementalReindexState.key(alias, IncrementalReindexState.OLD_BACKING_INDEX_NAME)));
  }

  @Test
  public void testResumedRebuildLeavesThePartialCopyAPreviousRunLeftAndCopiesAfresh()
      throws Exception {
    // The previous run recorded its copy and died while it was in flight, perhaps with its reindex
    // still writing; this run leaves that copy for cleanup and takes a fresh one.
    String next = alias + "_0_14_0-0_1679000000000";
    String stale = alias + "_0_14_0-0_1678000000000";
    String retained = alias + "_0_14_0-0_1679000001000";
    seedPhase1(
        IncrementalReindexState.setOldBackingIndexName(
            IncrementalReindexState.setBackfillPending(null, alias, next, 1000L), alias, stale));
    when(indexBuilder.getBackingIndices(any(OperationContext.class), eq(alias)))
        .thenReturn(Set.of());
    ESIndexBuilder.IncrementalReindexResult copy =
        new ESIndexBuilder.IncrementalReindexResult(
            retained, 1679000001000L, "copy-task", false, 1, 42L, Map.of());
    when(indexBuilder.startRollbackCopy(
            eq(opContext), any(ReindexConfig.class), eq(UPGRADE_VERSION)))
        .thenReturn(copy);

    UpgradeStepResult result = createStep(null).executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder, never()).deleteActionWithRetry(any(OperationContext.class), anyString());
    InOrder inOrder = inOrder(indexBuilder);
    inOrder
        .verify(indexBuilder)
        .startRollbackCopy(eq(opContext), any(ReindexConfig.class), eq(UPGRADE_VERSION));
    inOrder.verify(indexBuilder).swapAlias(opContext, alias, next);
    assertEquals(
        IncrementalReindexState.get(
            persistedState(), alias, IncrementalReindexState.OLD_BACKING_INDEX_NAME),
        Optional.of(retained));
  }

  @Test
  public void testRebuildOfAnAliasedIndexIsNotCopied() throws Exception {
    // Phase 1 recorded the aliased index's old backing index; the swap leaves it in place.
    String next = alias + "_0_14_0-0_1679000000000";
    seedPhase1(IncrementalReindexState.setBackfillPending(null, alias, next, 1000L));

    UpgradeStepResult result = createStep(null).executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder, never())
        .startRollbackCopy(any(OperationContext.class), any(ReindexConfig.class), anyString());
    verify(indexBuilder).swapAlias(opContext, alias, next);
  }

  @Test
  public void testRebuildWhoseNextIndexLostItsMappingsIsFailedBeforeAnyWrite() throws Exception {
    String next = alias + "_0_14_0-0_1679000000000";
    seedPhase1(IncrementalReindexState.setBackfillPending(null, alias, next, 1000L));
    when(indexBuilder.resumedReindexMatchesTarget(
            any(OperationContext.class), any(ReindexConfig.class), eq(next)))
        .thenReturn(false);

    UpgradeStepResult result = createStep(null).executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(loader, never()).load(any(), any(), any(), any(), any());
    assertEquals(
        IncrementalReindexState.getStatus(persistedState(), alias), Optional.of(Status.FAILED));
  }

  @Test
  public void testDestinationLostBeforeTheSwapFailsTheRowInsteadOfMovingTheAlias()
      throws Exception {
    // The next index passed verification before the first write but not before the swap: deleted
    // mid-fill and recreated by dynamic mapping under the writes that followed. It holds documents,
    // so a count would have swapped it in with the wrong mappings.
    String next = alias + "_0_14_0-0_1679000000000";
    seedPhase1(IncrementalReindexState.setBackfillPending(null, alias, next, 1000L));
    when(indexBuilder.resumedReindexMatchesTarget(
            any(OperationContext.class), any(ReindexConfig.class), eq(next)))
        .thenReturn(true, false);

    UpgradeStepResult result = createStep(null).executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(indexBuilder, never()).swapAlias(any(OperationContext.class), anyString(), anyString());
    Map<String, String> state = persistedState();
    assertEquals(IncrementalReindexState.getStatus(state, alias), Optional.of(Status.FAILED));
    assertFalse(
        state.containsKey(
            IncrementalReindexState.key(alias, IncrementalReindexState.BACKFILL_COMPLETE_TIME)));
  }

  @Test
  public void testResumedRebuildSwapsWithoutCountingTheReReadEntity() throws Exception {
    // A run interrupted mid-type leaves its cursor on the last entity written; the resume re-reads
    // that entity and writes it again. The
    // entities-read figure therefore runs one ahead of the index per resume, and the swap must not
    // depend on it.
    String next = alias + "_0_14_0-0_1679000000000";
    String firstEntity = entityNames.get(0);
    String cursorUrn = "urn:li:" + firstEntity + ":last-written";
    Map<String, String> state =
        IncrementalReindexState.setBackfillPending(null, alias, next, 1000L);
    state =
        IncrementalReindexState.setBackfillCheckpoint(
            state, alias, new BackfillCheckpoint(firstEntity, cursorUrn, 5L, 0L));
    state = IncrementalReindexState.setBackfillUrnsProcessed(state, alias, 3L);
    seedPhase1(state);

    UpgradeStepResult result = createStep(null).executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    assertEquals(loadArgs().get(0).lastUrn, cursorUrn);
    verify(indexBuilder).swapAlias(opContext, alias, next);
    verify(indexBuilder, never()).getCount(any(OperationContext.class), anyString());
    // Reported as what it is: entities read across runs, the re-read included.
    assertEquals(
        IncrementalReindexState.getBackfillUrnsProcessed(persistedState(), alias),
        Optional.of(3L + 3L * entityNames.size()));
  }

  @Test
  public void testBatchThatDoesNotNameTheFillTargetIsRefusedBeforeItIsWritten() throws Exception {
    // The target header is how the strategy is told to write into the next index rather than the
    // alias, and it writes the alias without one. A loader that stopped stamping it would fill the
    // live index in silence, so the contract is checked where the batches are handed over.
    String next = alias + "_0_14_0-0_1679000000000";
    seedPhase1(IncrementalReindexState.setBackfillPending(null, alias, next, 1000L));
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,fill,PROD)");
    MetadataChangeLog stamped = new MetadataChangeLog().setEntityUrn(urn);
    StringMap headers = new StringMap();
    headers.put(BackfillHeaders.TARGET_INDEX_HEADER_NAME, next);
    stamped.setHeaders(headers);
    MetadataChangeLog unstamped = new MetadataChangeLog().setEntityUrn(urn);
    when(loader.load(any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              AspectIndexLoader.BatchWriter writer = invocation.getArgument(4);
              writer.write(opContext, List.of(stamped));
              writer.write(opContext, List.of(unstamped));
              return new LoadIndicesResult();
            });

    UpgradeStepResult result = createStep(null).executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(updateIndicesService, times(1)).fillSearchIndices(any(), any());
    verify(indexBuilder, never()).swapAlias(any(OperationContext.class), anyString(), anyString());
  }

  @Test
  public void testDeadlineLeavesTheCursorForTheNextRun() throws Exception {
    seedPhase1(IncrementalReindexState.setBackfillPending(null, alias, alias, 1000L));
    String firstEntity = entityNames.get(0);
    String stoppedAt = "urn:li:" + firstEntity + ":stopped";
    doThrow(new AspectIndexLoader.DeadlineExceeded(stoppedAt, 5))
        .when(loader)
        .load(any(), any(), any(), any(), any());
    BuildIndicesConfiguration config = new BuildIndicesConfiguration();
    config.setMaxBackfillHours(1);
    config.setBackfillBatchSize(250);

    UpgradeStepResult result = createStep(config).executable().apply(upgradeContext);

    // A budget pause is a successful bounded run; failing it would end the whole flattened
    // non-blocking job. The row, not the step result, says the fill is unfinished.
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    LoadIndicesArgs args = loadArgs().get(0);
    assertEquals(args.batchSize, 250);
    assertNull(args.aspectNames, "the fill streams every stored aspect of each entity");
    assertTrue(args.deadlineEpochMs > System.currentTimeMillis());
    Map<String, String> state = persistedState();
    assertEquals(
        IncrementalReindexState.getStatus(state, alias), Optional.of(Status.BACKFILL_IN_PROGRESS));
    assertFalse(
        state.containsKey(
            IncrementalReindexState.key(alias, IncrementalReindexState.BACKFILL_COMPLETE_TIME)));
  }

  @Test
  public void testCursorIsPersistedEveryFewThousandRowsAndFlushedWhenTheRunStops()
      throws Exception {
    seedPhase1(IncrementalReindexState.setBackfillPending(null, alias, alias, 1000L));
    String firstEntity = entityNames.get(0);
    long rowsPerBatch = BackfillEntityIndicesStep.CHECKPOINT_EVERY_ROWS / 2;
    int batches = 7;
    when(loader.load(any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              AspectIndexLoader.BatchListener listener = invocation.getArgument(3);
              for (int i = 1; i <= batches; i++) {
                listener.onBatch("urn:li:" + firstEntity + ":" + i, rowsPerBatch * i, i);
              }
              throw new AspectIndexLoader.DeadlineExceeded(
                  "urn:li:" + firstEntity + ":" + batches, rowsPerBatch * batches);
            });

    UpgradeStepResult result = createStep(null).executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    BackfillCheckpoint checkpoint =
        IncrementalReindexState.getBackfillCheckpoint(persistedState(), alias).get();
    assertEquals(checkpoint.cursorEntity(), firstEntity);
    assertEquals(checkpoint.cursorUrn(), "urn:li:" + firstEntity + ":" + batches);
    assertEquals(checkpoint.processedCount(), rowsPerBatch * batches);
    assertEquals(
        IncrementalReindexState.getBackfillUrnsProcessed(persistedState(), alias),
        Optional.of((long) batches));
    // Status move, one write per full cadence interval, and the flush on exit.
    int cadenceWrites =
        (int) (rowsPerBatch * batches / BackfillEntityIndicesStep.CHECKPOINT_EVERY_ROWS);
    verify(entityService, times(1 + cadenceWrites + 1))
        .ingestProposal(any(), any(), any(), anyBoolean());
  }

  @Test
  public void testCompletedFillIsNotRepeatedOnRerun() throws Exception {
    Map<String, String> state =
        IncrementalReindexState.setBackfillPending(null, alias, alias, 1000L);
    state = IncrementalReindexState.setBackfillCompleteTime(state, alias, 2000L);
    state = IncrementalReindexState.setPhase1Completed(state, alias);
    seedPhase1(state);

    UpgradeStepResult result = createStep(null).executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(loader, never()).load(any(), any(), any(), any(), any());
  }

  @Test
  public void testAFailedRowDoesNotFailTheFillStep() throws Exception {
    // A V2 _reindex failure is not a fill. Failing this step would end the run before catch-up
    // replays indices that already completed.
    Map<String, String> state =
        IncrementalReindexState.setPhase1State(
            null, alias, alias, null, 1000L, 0L, null, false, Status.FAILED);
    seedPhase1(state);

    UpgradeStepResult result = createStep(null).executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(loader, never()).load(any(), any(), any(), any(), any());
    verify(upgrade, never()).setUpgradeResult(any(), any(), any(), any(), any());
    assertEquals(
        IncrementalReindexState.getStatus(persistedState(), alias), Optional.of(Status.FAILED));
  }
}
