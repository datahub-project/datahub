package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.CORP_USER_EDITABLE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_PROFILE_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.entity.CeilingDeleteH2Harness.editable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Ceiling-bounded deletes on H2 behind the real EntityServiceImpl, MCLs captured (layers 2-3). */
public class EbeanCeilingDeleteTest {
  private static final String EDITABLE = CORP_USER_EDITABLE_INFO_ASPECT_NAME;

  private CeilingDeleteH2Harness h;
  private EntityServiceImpl entityService;
  private EbeanAspectDao aspectDao;
  private EventProducer producer;
  private OperationContext opContext;

  @DataProvider(name = "lockingModes")
  public Object[][] lockingModes() {
    return new Object[][] {{false}, {true}};
  }

  @BeforeMethod
  public void setup() {
    use(false);
  }

  private void use(boolean optimisticLocking) {
    h = CeilingDeleteH2Harness.build(optimisticLocking);
    entityService = h.entityService;
    aspectDao = h.aspectDao;
    producer = h.producer;
    opContext = h.opContext;
  }

  @Test(dataProvider = "lockingModes")
  public void historyRowsAreNumberedByTheLogicalVersionTheyHadAsLatest(boolean optimisticLocking) {
    use(optimisticLocking);
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:ceiling-numbering-" + optimisticLocking);
    h.upsert(urn, EDITABLE, editable("v1"), 1_000L);
    h.upsert(urn, EDITABLE, editable("v2"), 2_000L);
    h.upsert(urn, EDITABLE, editable("v3"), 3_000L);

    assertEquals(h.storedVersion(urn, EDITABLE, 0L), 3L);
    assertEquals(h.storedVersion(urn, EDITABLE, 1L), 1L);
    assertEquals(h.storedVersion(urn, EDITABLE, 2L), 2L);
    assertNull(aspectDao.getAspect(opContext, urn.toString(), EDITABLE, 3L));
  }

  @Test
  public void aNoOpWriteKeepsTheVersionAndWritesNoHistory() {
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:ceiling-noop");
    h.upsert(urn, EDITABLE, editable("same"), 1_000L);
    h.upsert(urn, EDITABLE, editable("same"), 2_000L);

    assertEquals(h.storedVersion(urn, EDITABLE, 0L), 1L);
    assertNull(aspectDao.getAspect(opContext, urn.toString(), EDITABLE, 1L));
  }

  private DeleteCeiling capture(Urn urn, long capturedAtMillis) {
    return entityService.captureDeleteCeiling(opContext, urn, capturedAtMillis).orElseThrow();
  }

  @Test
  public void capturesEveryLatestVersionButNotTheKeyAndNothingForAnAbsentUrn() {
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:ceiling-capture");
    h.upsert(urn, STATUS_ASPECT_NAME, new Status().setRemoved(true), 1_000L);
    h.upsert(urn, EDITABLE, editable("v1"), 1_000L);
    h.upsert(urn, EDITABLE, editable("v2"), 2_000L);

    final DeleteCeiling ceiling = capture(urn, 2_500L);

    assertEquals(ceiling.aspectVersions().get(STATUS_ASPECT_NAME), Long.valueOf(1L));
    assertEquals(ceiling.aspectVersions().get(EDITABLE), Long.valueOf(2L));
    assertFalse(ceiling.aspectVersions().containsKey(opContext.getKeyAspectName(urn)));
    assertEquals(ceiling.keyCreatedMillis(), 1_000L);
    assertEquals(ceiling.capturedAtMillis(), 2_500L);
    assertTrue(
        entityService
            .captureDeleteCeiling(
                opContext, UrnUtils.getUrn("urn:li:corpuser:ceiling-capture-absent"), 2_500L)
            .isEmpty());
  }

  /** I2, I4, I5 (DELETED): history included, one key DELETE MCL, no per-aspect DELETE MCL. */
  @Test
  public void deletesTheWholeEntityWhenNothingIsNewerThanTheCeiling() {
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:ceiling-whole");
    h.upsert(urn, STATUS_ASPECT_NAME, new Status().setRemoved(true), 1_000L);
    h.upsert(urn, EDITABLE, editable("v1"), 1_000L);
    h.upsert(urn, EDITABLE, editable("v2"), 2_000L);
    final DeleteCeiling ceiling = capture(urn, 2_500L);
    clearInvocations(producer);

    final RollbackRunResult result = entityService.deleteUrn(opContext, urn, ceiling);

    assertEquals(result.getConditionalDeleteOutcome(), ConditionalDeleteOutcome.DELETED);
    assertTrue(entityService.captureDeleteCeiling(opContext, urn, 3_000L).isEmpty());
    assertNull(aspectDao.getAspect(opContext, urn.toString(), EDITABLE, 1L));
    verify(producer, times(1))
        .produceMetadataChangeLog(any(), eq(urn), any(), argThat(mcl -> h.isKeyDelete(mcl, urn)));
    verify(producer, never())
        .produceMetadataChangeLog(
            any(),
            any(),
            any(),
            argThat(
                mcl ->
                    CeilingDeleteH2Harness.isAspectDelete(mcl, EDITABLE)
                        || CeilingDeleteH2Harness.isAspectDelete(mcl, STATUS_ASPECT_NAME)));
  }

  /** I1, I2, I4, I5 (PARTIAL): the advanced aspect keeps its latest, loses history <= ceiling. */
  @Test
  public void keepsAnAspectThatAdvancedPastItsCeilingAndDeletesTheRest() {
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:ceiling-advanced");
    h.upsert(urn, STATUS_ASPECT_NAME, new Status().setRemoved(true), 1_000L);
    h.upsert(urn, EDITABLE, editable("v1"), 1_000L);
    h.upsert(urn, EDITABLE, editable("v2"), 2_000L);
    final DeleteCeiling ceiling = capture(urn, 2_500L);
    // Ingestion writes the aspect again after the request was captured.
    h.upsert(urn, EDITABLE, editable("v3"), 3_000L);
    clearInvocations(producer);

    final RollbackRunResult result = entityService.deleteUrn(opContext, urn, ceiling);

    assertEquals(result.getConditionalDeleteOutcome(), ConditionalDeleteOutcome.PARTIAL);
    assertNull(entityService.getLatestAspect(opContext, urn, STATUS_ASPECT_NAME));
    assertEquals(
        new CorpUserEditableInfo(entityService.getLatestAspect(opContext, urn, EDITABLE).data())
            .getAboutMe(),
        "v3");
    assertEquals(h.storedVersion(urn, EDITABLE, 0L), 3L);
    // As if the delete ran at capture time and v3 came after it: no history the request saw.
    assertNull(aspectDao.getAspect(opContext, urn.toString(), EDITABLE, 1L));
    assertNull(aspectDao.getAspect(opContext, urn.toString(), EDITABLE, 2L));
    assertTrue(entityService.captureDeleteCeiling(opContext, urn, 4_000L).isPresent());
    verify(producer, times(1))
        .produceMetadataChangeLog(
            any(),
            eq(urn),
            any(),
            argThat(mcl -> CeilingDeleteH2Harness.isAspectDelete(mcl, STATUS_ASPECT_NAME)));
    verify(producer, never())
        .produceMetadataChangeLog(
            any(),
            any(),
            any(),
            argThat(
                mcl ->
                    h.isKeyDelete(mcl, urn)
                        || CeilingDeleteH2Harness.isAspectDelete(mcl, EDITABLE)));
  }

  /** I3, I4: an aspect first written after the capture keeps the entity alive. */
  @Test
  public void keepsAnAspectCreatedAfterTheCapture() {
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:ceiling-created-after");
    h.upsert(urn, STATUS_ASPECT_NAME, new Status().setRemoved(true), 1_000L);
    final DeleteCeiling ceiling = capture(urn, 1_500L);
    h.upsert(urn, EDITABLE, editable("new"), 2_000L);
    clearInvocations(producer);

    final RollbackRunResult result = entityService.deleteUrn(opContext, urn, ceiling);

    assertEquals(result.getConditionalDeleteOutcome(), ConditionalDeleteOutcome.PARTIAL);
    assertNull(entityService.getLatestAspect(opContext, urn, STATUS_ASPECT_NAME));
    assertEquals(
        new CorpUserEditableInfo(entityService.getLatestAspect(opContext, urn, EDITABLE).data())
            .getAboutMe(),
        "new");
    assertTrue(entityService.captureDeleteCeiling(opContext, urn, 3_000L).isPresent());
    verify(producer, never())
        .produceMetadataChangeLog(
            any(),
            any(),
            any(),
            argThat(
                mcl ->
                    h.isKeyDelete(mcl, urn)
                        || CeilingDeleteH2Harness.isAspectDelete(mcl, EDITABLE)));
  }

  /** Versions restart at 1 after a hard delete: only the key creation time tells them apart. */
  @Test
  public void recreatedEntityIsReportedAlreadyDeletedAndLeftAlone() {
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:ceiling-recreated");
    h.upsert(urn, STATUS_ASPECT_NAME, new Status().setRemoved(true), 1_000L);
    final DeleteCeiling ceiling = capture(urn, 1_500L);
    entityService.deleteUrn(opContext, urn);
    h.upsert(urn, STATUS_ASPECT_NAME, new Status().setRemoved(false), 5_000L);
    assertEquals(capture(urn, 6_000L).aspectVersions(), ceiling.aspectVersions());
    clearInvocations(producer);

    final RollbackRunResult result = entityService.deleteUrn(opContext, urn, ceiling);

    assertEquals(result.getConditionalDeleteOutcome(), ConditionalDeleteOutcome.ALREADY_DELETED);
    assertTrue(result.getRowsRolledBack().isEmpty());
    assertFalse(
        new Status(entityService.getLatestAspect(opContext, urn, STATUS_ASPECT_NAME).data())
            .isRemoved());
    verify(producer, never()).produceMetadataChangeLog(any(), any(), any(), any());
  }

  /** I7. */
  @Test
  public void repeatingACommittedDeleteReportsAlreadyDeletedAndWritesNothing() {
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:ceiling-repeat");
    h.upsert(urn, STATUS_ASPECT_NAME, new Status().setRemoved(true), 1_000L);
    final DeleteCeiling ceiling = capture(urn, 1_500L);
    assertEquals(
        entityService.deleteUrn(opContext, urn, ceiling).getConditionalDeleteOutcome(),
        ConditionalDeleteOutcome.DELETED);
    clearInvocations(producer, aspectDao);

    final RollbackRunResult again = entityService.deleteUrn(opContext, urn, ceiling);

    assertEquals(again.getConditionalDeleteOutcome(), ConditionalDeleteOutcome.ALREADY_DELETED);
    assertTrue(again.getRowsRolledBack().isEmpty());
    verify(aspectDao, never()).deleteUrn(any(), any(), any());
    verify(aspectDao, never()).deleteAspectVersionRange(any(), any(), any(), anyLong(), anyLong());
    verify(producer, never()).produceMetadataChangeLog(any(), any(), any(), any());
  }

  /** I8 at this layer: the capture writes nothing and flags a version above the latest. */
  @Test
  public void aCallerVersionAboveTheLatestIsAMismatchAndNothingIsWritten() {
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:ceiling-caller-version");
    h.upsert(urn, STATUS_ASPECT_NAME, new Status().setRemoved(true), 1_000L);
    clearInvocations(producer, aspectDao);

    final DeleteCeiling ceiling = capture(urn, 1_500L);

    assertEquals(
        ceiling.callerMismatches(Map.of(STATUS_ASPECT_NAME, 2L)), Set.of(STATUS_ASPECT_NAME));
    assertTrue(ceiling.callerMismatches(Map.of(STATUS_ASPECT_NAME, 1L)).isEmpty());
    verify(aspectDao, never()).deleteUrn(any(), any(), any());
    verify(aspectDao, never()).deleteAspectVersionRange(any(), any(), any(), anyLong(), anyLong());
    verify(producer, never()).produceMetadataChangeLog(any(), any(), any(), any());
  }

  /**
   * I2, I4, I5 for an entity that is only its key: the ceiling lists no aspect, and an empty ceiling
   * must still mean "delete", not "nothing to delete".
   */
  @Test
  public void aKeyOnlyEntityIsDeletedWithItsKeyDeleteEvent() {
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:ceiling-key-only");
    h.upsert(
        urn,
        opContext.getKeyAspectName(urn),
        new CorpUserKey().setUsername("ceiling-key-only"),
        1_000L);
    final DeleteCeiling ceiling = capture(urn, 1_500L);
    assertTrue(
        ceiling.aspectVersions().isEmpty(),
        "precondition: only the key exists; got " + ceiling.aspectVersions());
    clearInvocations(producer);

    final RollbackRunResult result = entityService.deleteUrn(opContext, urn, ceiling);

    assertEquals(result.getConditionalDeleteOutcome(), ConditionalDeleteOutcome.DELETED);
    assertTrue(entityService.captureDeleteCeiling(opContext, urn, 2_000L).isEmpty());
    verify(producer, times(1))
        .produceMetadataChangeLog(any(), eq(urn), any(), argThat(mcl -> h.isKeyDelete(mcl, urn)));
  }

  /**
   * I1, I2, I4 on rows written before {@code systemMetadata.version} and {@code aspectCreated}
   * existed: the latest counts as version 1, the key's creation time comes from its {@code
   * createdon}, and capture and delete must agree on both, or a legacy entity is never deleted.
   */
  @Test
  public void legacyRowsWithoutAStoredVersionAreDeletedWithTheirHistory() {
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:ceiling-legacy");
    saveLegacyRow(
        urn,
        opContext.getKeyAspectName(urn),
        0L,
        new CorpUserKey().setUsername("ceiling-legacy"),
        1_000L);
    saveLegacyRow(urn, STATUS_ASPECT_NAME, 1L, new Status().setRemoved(false), 1_000L);
    saveLegacyRow(urn, STATUS_ASPECT_NAME, 0L, new Status().setRemoved(true), 2_000L);
    final DeleteCeiling ceiling = capture(urn, 2_500L);
    assertEquals(ceiling.aspectVersions(), Map.of(STATUS_ASPECT_NAME, 1L));
    assertEquals(ceiling.keyCreatedMillis(), 1_000L);
    clearInvocations(producer);

    final RollbackRunResult result = entityService.deleteUrn(opContext, urn, ceiling);

    assertEquals(result.getConditionalDeleteOutcome(), ConditionalDeleteOutcome.DELETED);
    assertTrue(entityService.captureDeleteCeiling(opContext, urn, 3_000L).isEmpty());
    assertNull(aspectDao.getAspect(opContext, urn.toString(), STATUS_ASPECT_NAME, 1L));
    verify(producer, times(1))
        .produceMetadataChangeLog(any(), eq(urn), any(), argThat(mcl -> h.isKeyDelete(mcl, urn)));
  }

  /** A row as pre-versioning code wrote it: no systemMetadata.version, no aspectCreated. */
  private void saveLegacyRow(
      Urn urn, String aspectName, long rowVersion, RecordTemplate value, long createdOnMillis) {
    h.server.save(
        new EbeanAspectV2(
            urn.toString(),
            aspectName,
            rowVersion,
            RecordUtils.toJsonString(value),
            new Timestamp(createdOnMillis),
            "urn:li:corpuser:datahub",
            null,
            RecordUtils.toJsonString(new SystemMetadata().setRunId("legacy-run"))));
  }

  @Test
  public void typeWithoutStatusDeletesOnTheCeilingAlone() {
    final Urn urn = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:ceiling-no-status");
    assertFalse(
        opContext
            .getEntityRegistry()
            .getEntitySpec(urn.getEntityType())
            .hasAspect(STATUS_ASPECT_NAME));
    h.upsert(
        urn,
        EXECUTION_REQUEST_INPUT_ASPECT_NAME,
        new ExecutionRequestInput()
            .setTask("RUN_INGEST")
            .setArgs(new StringMap())
            .setExecutorId("default")
            .setSource(new ExecutionRequestSource().setType("MANUAL"))
            .setRequestedAt(1_000L),
        1_000L);

    final RollbackRunResult result = entityService.deleteUrn(opContext, urn, capture(urn, 1_500L));

    assertEquals(result.getConditionalDeleteOutcome(), ConditionalDeleteOutcome.DELETED);
    assertTrue(entityService.captureDeleteCeiling(opContext, urn, 2_000L).isEmpty());
  }

  @Test
  public void rejectsACeilingThatListsTheKeyAspect() {
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:ceiling-key-listed");
    h.upsert(urn, STATUS_ASPECT_NAME, new Status().setRemoved(true), 1_000L);
    final DeleteCeiling captured = capture(urn, 1_500L);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            entityService.deleteUrn(
                opContext,
                urn,
                new DeleteCeiling(
                    Map.of(opContext.getKeyAspectName(urn), 1L),
                    captured.keyCreatedMillis(),
                    1_500L)));
    assertTrue(entityService.captureDeleteCeiling(opContext, urn, 2_000L).isPresent());
  }

  /** Lock order (Review Focus 7) and the structural half of I6. */
  @Test(dataProvider = "lockingModes")
  public void ceilingDeleteLocksTheKeyThenEveryLatestRowBeforeDeleting(boolean optimisticLocking) {
    use(optimisticLocking);
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:ceiling-lock-order-" + optimisticLocking);
    h.upsert(urn, STATUS_ASPECT_NAME, new Status().setRemoved(true), 1_000L);
    final DeleteCeiling ceiling = capture(urn, 1_500L);
    clearInvocations(aspectDao);

    assertEquals(
        entityService.deleteUrn(opContext, urn, ceiling).getConditionalDeleteOutcome(),
        ConditionalDeleteOutcome.DELETED);

    final InOrder order = inOrder(aspectDao);
    order
        .verify(aspectDao)
        .getLatestAspectForDecision(any(), eq(urn.toString()), eq(opContext.getKeyAspectName(urn)));
    order.verify(aspectDao).getLatestAspectsForDecision(any(), eq(urn));
    order.verify(aspectDao).deleteUrn(any(), any(), eq(urn.toString()));
  }

  /** Review Focus 6: both outcomes write only through the transaction that holds the locks. */
  @Test(dataProvider = "lockingModes")
  public void ceilingDeletesOpenNoNestedTransaction(boolean optimisticLocking) {
    use(optimisticLocking);
    final Urn whole = UrnUtils.getUrn("urn:li:corpuser:ceiling-nesting-whole-" + optimisticLocking);
    final Urn partial =
        UrnUtils.getUrn("urn:li:corpuser:ceiling-nesting-partial-" + optimisticLocking);
    h.upsert(whole, STATUS_ASPECT_NAME, new Status().setRemoved(true), 1_000L);
    h.upsert(partial, STATUS_ASPECT_NAME, new Status().setRemoved(true), 1_000L);
    final DeleteCeiling wholeCeiling = capture(whole, 1_500L);
    final DeleteCeiling partialCeiling = capture(partial, 1_500L);
    h.upsert(partial, EDITABLE, editable("after"), 2_000L);
    final AtomicInteger maxDepth = h.trackTransactionNesting();

    assertEquals(
        entityService.deleteUrn(opContext, whole, wholeCeiling).getConditionalDeleteOutcome(),
        ConditionalDeleteOutcome.DELETED);
    assertEquals(
        entityService.deleteUrn(opContext, partial, partialCeiling).getConditionalDeleteOutcome(),
        ConditionalDeleteOutcome.PARTIAL);

    assertEquals(maxDepth.get(), 1);
  }

  /** Control: the probe sees nesting (the unconditional key soft delete ingests status inside). */
  @Test
  public void theNestingProbeSeesANestedTransaction() {
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:ceiling-nesting-control");
    h.upsert(urn, STATUS_ASPECT_NAME, new Status().setRemoved(false), 1_000L);
    final AtomicInteger maxDepth = h.trackTransactionNesting();

    entityService.deleteAspectWithoutMCL(
        opContext, urn.toString(), opContext.getKeyAspectName(urn), Map.of(), false);

    assertEquals(maxDepth.get(), 2);
  }

  /** R2: the existing delete never takes the decision locks. */
  @Test
  public void unconditionalDeleteKeepsTheUnlockedRead() {
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:ceiling-unconditional");
    h.upsert(urn, STATUS_ASPECT_NAME, new Status().setRemoved(true), 1_000L);
    clearInvocations(aspectDao);

    entityService.deleteUrn(opContext, urn);

    verify(aspectDao, never()).getLatestAspectForDecision(any(), any(), any());
    verify(aspectDao, never()).getLatestAspectsForDecision(any(), any());
    assertTrue(entityService.captureDeleteCeiling(opContext, urn, 2_000L).isEmpty());
  }

  /** Today's soft-delete-first rule for a user's hard delete of a structured property. */
  @Test
  public void activeStructuredPropertyNeedsASoftDeleteFirstForAUser() {
    final Urn property = UrnUtils.getUrn("urn:li:structuredProperty:ceiling-guarded");
    h.upsert(property, STATUS_ASPECT_NAME, new Status().setRemoved(false), 1_000L);
    final OperationContext user =
        TestOperationContexts.userContextNoSearchAuthorization(opContext.getEntityRegistry());
    final DeleteCeiling ceiling = capture(property, 1_500L);

    assertThrows(
        IllegalArgumentException.class, () -> entityService.deleteUrn(user, property, ceiling));
    assertTrue(entityService.captureDeleteCeiling(opContext, property, 2_000L).isPresent());

    h.upsert(property, STATUS_ASPECT_NAME, new Status().setRemoved(true), 3_000L);
    entityService.validateHardDelete(user, property);
  }

  /** The in-transaction class captures the side-effect pre-image from its own locked read. */
  @Test
  public void entityDeleteCapturesTheSideEffectPreImageFromTheLockedRows() {
    final Urn dataProduct = UrnUtils.getUrn("urn:li:dataProduct:ceiling-preimage");
    h.upsert(
        dataProduct,
        DATA_PRODUCT_PROPERTIES_ASPECT_NAME,
        new DataProductProperties().setName("dp"),
        1_000L);
    final DeleteCeiling ceiling = capture(dataProduct, 1_500L);

    final AtomicReference<CeilingDeleteTransaction.Result> result = new AtomicReference<>();
    aspectDao.runInTransactionWithRetry(
        opContext,
        txContext -> {
          result.set(
              new CeilingDeleteTransaction(aspectDao)
                  .deleteEntity(opContext, txContext, dataProduct, ceiling));
          return TransactionResult.commit("");
        },
        0);

    assertEquals(result.get().outcome(), ConditionalDeleteOutcome.DELETED);
    assertEquals(
        ((DataProductProperties)
                result
                    .get()
                    .keyDeleteSideEffectPreImages()
                    .get(DATA_PRODUCT_PROPERTIES_ASPECT_NAME))
            .getName(),
        "dp");
  }

  @Test
  public void keyDeleteSideEffectMclsPutPreImagesBeforeTheKeyDelete() {
    final Urn dataProduct = UrnUtils.getUrn("urn:li:dataProduct:ceiling-mcls");
    final String keyAspectName = opContext.getKeyAspectName(dataProduct);
    final RollbackResult keyDelete =
        new RollbackResult(
            dataProduct,
            dataProduct.getEntityType(),
            keyAspectName,
            EntityKeyUtils.convertUrnToEntityKey(
                dataProduct,
                opContext
                    .getEntityRegistry()
                    .getEntitySpec(dataProduct.getEntityType())
                    .getKeyAspectSpec()),
            null,
            null,
            null,
            ChangeType.DELETE,
            true,
            0);

    final List<MetadataChangeLog> mcls =
        EntityServiceImpl.buildKeyDeleteSideEffectMcls(
            dataProduct,
            new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:ceiling")).setTime(3_000L),
            Map.<String, RecordTemplate>of(
                DATA_PRODUCT_PROPERTIES_ASPECT_NAME, new DataProductProperties().setName("dp")),
            keyDelete);

    assertEquals(mcls.size(), 2);
    assertEquals(mcls.get(0).getAspectName(), DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
    assertEquals(mcls.get(0).getChangeType(), ChangeType.DELETE);
    assertTrue(mcls.get(0).hasPreviousAspectValue());
    assertEquals(mcls.get(1).getAspectName(), keyAspectName);
    assertEquals(mcls.get(1).getChangeType(), ChangeType.DELETE);
  }

  /**
   * Decision 3 (2026-10-07): an aspect hard-deleted and written again after the capture restarts at
   * version 1, at or below its ceiling; its aspectCreated after the capture keeps it.
   */
  @Test
  public void keepsAnAspectRecreatedAfterTheCaptureEvenAtOrBelowItsCeiling() {
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:ceiling-aspect-recreated");
    h.upsert(urn, STATUS_ASPECT_NAME, new Status().setRemoved(true), 1_000L);
    h.upsert(urn, EDITABLE, editable("original"), 1_000L);
    final DeleteCeiling ceiling = capture(urn, 1_500L);
    entityService.deleteAspectWithoutMCL(opContext, urn.toString(), EDITABLE, Map.of(), true);
    h.upsert(urn, EDITABLE, editable("reborn"), 2_000L);
    assertEquals(h.storedVersion(urn, EDITABLE, 0L), 1L);
    clearInvocations(producer);

    final RollbackRunResult result = entityService.deleteUrn(opContext, urn, ceiling);

    assertEquals(result.getConditionalDeleteOutcome(), ConditionalDeleteOutcome.PARTIAL);
    assertNull(entityService.getLatestAspect(opContext, urn, STATUS_ASPECT_NAME));
    assertEquals(
        new CorpUserEditableInfo(entityService.getLatestAspect(opContext, urn, EDITABLE).data())
            .getAboutMe(),
        "reborn");
    verify(producer, never())
        .produceMetadataChangeLog(
            any(),
            any(),
            any(),
            argThat(
                mcl ->
                    h.isKeyDelete(mcl, urn)
                        || CeilingDeleteH2Harness.isAspectDelete(mcl, EDITABLE)));
  }

  @Test
  public void aspectCeilingDeletesTheAspectWhenItsLatestIsAtOrBelowTheCeiling() {
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:aspect-ceiling-whole");
    h.upsert(urn, EDITABLE, editable("v1"), 1_000L);
    h.upsert(urn, EDITABLE, editable("v2"), 2_000L);
    clearInvocations(producer);

    assertEquals(
        entityService.deleteAspectUpToVersion(opContext, urn, EDITABLE, 2L),
        ConditionalDeleteOutcome.DELETED);

    assertNull(entityService.getLatestAspect(opContext, urn, EDITABLE));
    assertNull(aspectDao.getAspect(opContext, urn.toString(), EDITABLE, 1L));
    verify(producer, times(1))
        .produceMetadataChangeLog(
            any(),
            eq(urn),
            any(),
            argThat(mcl -> CeilingDeleteH2Harness.isAspectDelete(mcl, EDITABLE)));
  }

  @Test
  public void aspectCeilingKeepsANewerLatestAndDeletesOnlyOlderHistory() {
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:aspect-ceiling-partial");
    h.upsert(urn, EDITABLE, editable("v1"), 1_000L);
    h.upsert(urn, EDITABLE, editable("v2"), 2_000L);
    h.upsert(urn, EDITABLE, editable("v3"), 3_000L);
    clearInvocations(producer);

    assertEquals(
        entityService.deleteAspectUpToVersion(opContext, urn, EDITABLE, 2L),
        ConditionalDeleteOutcome.PARTIAL);

    assertEquals(h.storedVersion(urn, EDITABLE, 0L), 3L);
    assertNull(aspectDao.getAspect(opContext, urn.toString(), EDITABLE, 1L));
    assertNull(aspectDao.getAspect(opContext, urn.toString(), EDITABLE, 2L));
    verify(producer, never()).produceMetadataChangeLog(any(), any(), any(), any());
  }

  @Test
  public void aspectCeilingOfAnAbsentAspectIsAlreadyDeleted() {
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:aspect-ceiling-absent");
    h.upsert(urn, STATUS_ASPECT_NAME, new Status().setRemoved(false), 1_000L);
    clearInvocations(producer);

    assertEquals(
        entityService.deleteAspectUpToVersion(opContext, urn, EDITABLE, 5L),
        ConditionalDeleteOutcome.ALREADY_DELETED);
    verify(producer, never()).produceMetadataChangeLog(any(), any(), any(), any());
  }

  @Test
  public void aspectCeilingRejectsTheKeyATimeseriesAspectAndAVersionBelowOne() {
    final Urn user = UrnUtils.getUrn("urn:li:corpuser:aspect-ceiling-rejects");
    final Urn dataset =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,aspect_ceiling_rejects,PROD)");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            entityService.deleteAspectUpToVersion(
                opContext, user, opContext.getKeyAspectName(user), 1L));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            entityService.deleteAspectUpToVersion(
                opContext, dataset, DATASET_PROFILE_ASPECT_NAME, 1L));
    assertThrows(
        IllegalArgumentException.class,
        () -> entityService.deleteAspectUpToVersion(opContext, user, EDITABLE, 0L));
  }

  @Test(dataProvider = "lockingModes")
  public void aspectCeilingDeleteLocksOnlyTheTargetRowAndNestsNothing(boolean optimisticLocking) {
    use(optimisticLocking);
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:aspect-ceiling-lock-" + optimisticLocking);
    h.upsert(urn, EDITABLE, editable("v1"), 1_000L);
    clearInvocations(aspectDao);
    final AtomicInteger maxDepth = h.trackTransactionNesting();

    assertEquals(
        entityService.deleteAspectUpToVersion(opContext, urn, EDITABLE, 1L),
        ConditionalDeleteOutcome.DELETED);

    verify(aspectDao).getLatestAspectForDecision(any(), eq(urn.toString()), eq(EDITABLE));
    // No key lock: a key-first writer can never hold this row while waiting on one we hold.
    verify(aspectDao, never())
        .getLatestAspectForDecision(any(), eq(urn.toString()), eq(opContext.getKeyAspectName(urn)));
    verify(aspectDao, never()).getLatestAspectsForDecision(any(), any());
    assertEquals(maxDepth.get(), 1);
  }

  @Test
  public void reemitProducesTheKeyDeleteOnlyWhenTheKeyIsGone() {
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:ceiling-reemit");
    h.upsert(urn, STATUS_ASPECT_NAME, new Status().setRemoved(true), 1_000L);
    clearInvocations(producer);
    assertThrows(
        IllegalStateException.class, () -> entityService.reemitKeyDeleteMcl(opContext, urn));
    verify(producer, never()).produceMetadataChangeLog(any(), any(), any(), any());

    entityService.deleteUrn(opContext, urn, capture(urn, 1_500L));
    clearInvocations(producer);
    entityService.reemitKeyDeleteMcl(opContext, urn);

    verify(producer, times(1))
        .produceMetadataChangeLog(
            any(),
            eq(urn),
            any(),
            argThat(mcl -> h.isKeyDelete(mcl, urn) && mcl.hasPreviousAspectValue()));
  }
}
