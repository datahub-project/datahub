package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.ASSERTION_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ASSERTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.ASSERTION_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.ASSERTION_RUN_EVENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.ASSERTION_RUN_SUMMARY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.datahub.authorization.EntitySpec;
import com.datahub.context.OperationFingerprint;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.assertion.AssertionRunSummary;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessAssertionScheduleType;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.assertion.VolumeAssertionType;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationSubType;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.entity.ebean.batch.ProposedItem;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AssertionAuthorizationValidatorTest {

  private static final Urn ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test-assertion");
  private static final Urn DATASET_A =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,my_db.table_a,PROD)");
  private static final Urn DATASET_B =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,my_db.table_b,PROD)");
  private static final Urn DATASET_A_DEV =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,my_db.table_a,DEV)");
  private static final Urn DATASET_A_UPPER =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,MY_DB.TABLE_A,PROD)");
  private static final DisjunctivePrivilegeGroup EDIT_ASSERTIONS_PRIVILEGES =
      new DisjunctivePrivilegeGroup(
          ImmutableList.of(
              new ConjunctivePrivilegeGroup(
                  ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType())),
              new ConjunctivePrivilegeGroup(
                  ImmutableList.of(PoliciesConfig.EDIT_ENTITY_ASSERTIONS_PRIVILEGE.getType()))));

  private AssertionAuthorizationValidator validator;
  private AuthorizationSession session;
  private AspectRetriever aspectRetriever;
  private MockedStatic<AuthUtil> authUtilMockedStatic;

  @BeforeMethod
  public void setup() {
    authUtilMockedStatic = Mockito.mockStatic(AuthUtil.class);
    validator = new AssertionAuthorizationValidator();
    validator.setConfig(
        AspectPluginConfig.builder()
            .className(AssertionAuthorizationValidator.class.getName())
            .enabled(true)
            .supportedOperations(List.of("UPSERT", "PATCH", "DELETE"))
            .supportedEntityAspectNames(
                List.of(
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName(ASSERTION_ENTITY_NAME)
                        .aspectName(ASSERTION_KEY_ASPECT_NAME)
                        .build(),
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName(ASSERTION_ENTITY_NAME)
                        .aspectName(ASSERTION_INFO_ASPECT_NAME)
                        .build(),
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName(ASSERTION_ENTITY_NAME)
                        .aspectName(ASSERTION_RUN_EVENT_ASPECT_NAME)
                        .build(),
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName(ASSERTION_ENTITY_NAME)
                        .aspectName(ASSERTION_RUN_SUMMARY_ASPECT_NAME)
                        .build(),
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName(ASSERTION_ENTITY_NAME)
                        .aspectName(STATUS_ASPECT_NAME)
                        .build()))
            .build());
    session = mock(AuthorizationSession.class);
    aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getEntityRegistry()).thenReturn(new TestEntityRegistry());
    mockPersisted(null);
    mockDeleted();
  }

  @AfterMethod
  public void tearDown() {
    authUtilMockedStatic.close();
  }

  @Test
  public void testCreateAllowedWithEditAssertionsOnAssertee() {
    grantEditAssertionsOn(DATASET_A);

    Assert.assertTrue(
        validate(OperationFingerprint.EMPTY, upsert(freshnessOn(DATASET_A))).isEmpty());
  }

  @Test
  public void testCreateDeniedWithoutEditAssertionsOnAssertee() {
    // Holding privileges on a different dataset (or on the assertion itself) is not enough.
    grantEditAssertionsOn(DATASET_B);

    List<AspectValidationException> failures =
        validate(OperationFingerprint.EMPTY, upsert(freshnessOn(DATASET_A)));

    Assert.assertEquals(failures.size(), 1);
    Assert.assertEquals(failures.get(0).getEntityUrn(), ASSERTION_URN);
  }

  @Test
  public void testSpoofedEntityUrnRequiresPrivilegeOnBothTargets() {
    grantEditAssertionsOn(DATASET_A);
    AssertionInfo info = freshnessOn(DATASET_A).setEntityUrn(DATASET_B);

    Assert.assertFalse(validate(OperationFingerprint.EMPTY, upsert(info)).isEmpty());
  }

  @Test
  public void testUpdateKeepingSameAssertee_noAssertedEntityCheck() {
    // EDIT_ENTITY on the assertion itself (enforced by the API gate) keeps working for edits that
    // don't change which entity the assertion is attached to.
    mockPersisted(freshnessOn(DATASET_A).setEntityUrn(DATASET_A));

    Assert.assertTrue(
        validate(OperationFingerprint.EMPTY, upsert(freshnessOn(DATASET_A).setDescription("x")))
            .isEmpty());
  }

  @Test
  public void testRetargetRequiresPrivilegeOnBothEntities() {
    mockPersisted(freshnessOn(DATASET_A));
    grantEditAssertionsOn(DATASET_B);

    Assert.assertFalse(
        validate(OperationFingerprint.EMPTY, upsert(freshnessOn(DATASET_B))).isEmpty());

    grantEditAssertionsOn(DATASET_A);
    Assert.assertTrue(
        validate(OperationFingerprint.EMPTY, upsert(freshnessOn(DATASET_B))).isEmpty());
  }

  @Test
  public void testSystemActorSkipsCheck() {
    OperationFingerprint systemFingerprint = mock(OperationFingerprint.class);
    when(systemFingerprint.isSystemAuth()).thenReturn(true);

    Assert.assertTrue(validate(systemFingerprint, upsert(freshnessOn(DATASET_A))).isEmpty());
  }

  @Test
  public void testPatchRetargetRequiresPrivilegeOnNewTarget() {
    mockPersisted(freshnessOn(DATASET_A));
    grantEditAssertionsOn(DATASET_A);

    Assert.assertFalse(
        validate(
                OperationFingerprint.EMPTY,
                genericPatch("/freshnessAssertion/entity", DATASET_B.toString()))
            .isEmpty());

    grantEditAssertionsOn(DATASET_B);
    Assert.assertTrue(
        validate(
                OperationFingerprint.EMPTY,
                genericPatch("/freshnessAssertion/entity", DATASET_B.toString()))
            .isEmpty());
  }

  @Test
  public void testSameBatchWritesAppliedInOrder() {
    // The PATCH applies on top of the upsert before it, not on the stored value, so it moves the
    // volume sub-record the upsert created to DATASET_B.
    mockPersisted(freshnessOn(DATASET_A));
    grantEditAssertionsOn(DATASET_A);
    BatchItem upsert = upsert(volumeOn(DATASET_A));
    BatchItem patch = genericPatch("/volumeAssertion/entity", DATASET_B.toString());

    List<AspectValidationException> failures = validate(OperationFingerprint.EMPTY, upsert, patch);
    Assert.assertEquals(failures.size(), 1);
    Assert.assertSame(failures.get(0).getItem(), patch);

    grantEditAssertionsOn(DATASET_B);
    Assert.assertTrue(validate(OperationFingerprint.EMPTY, upsert, patch).isEmpty());
  }

  @Test
  public void testSiblingSubRecordRequiresPrivilege() {
    // type selects the freshness sub-record, but a populated volume sub-record is indexed as an
    // Asserts edge too, so it lists the assertion on DATASET_B.
    grantEditAssertionsOn(DATASET_A);
    AssertionInfo info =
        freshnessOn(DATASET_A)
            .setVolumeAssertion(
                new VolumeAssertionInfo()
                    .setType(VolumeAssertionType.ROW_COUNT_TOTAL)
                    .setEntity(DATASET_B));

    Assert.assertFalse(validate(OperationFingerprint.EMPTY, upsert(info)).isEmpty());

    grantEditAssertionsOn(DATASET_B);
    Assert.assertTrue(validate(OperationFingerprint.EMPTY, upsert(info)).isEmpty());
  }

  @Test
  public void testSchemaFieldTargetsCheckedOnParentDataset() {
    grantEditAssertionsOn(DATASET_A);
    AssertionInfo info =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(
                new DatasetAssertionInfo()
                    .setDataset(DATASET_A)
                    .setScope(DatasetAssertionScope.DATASET_COLUMN)
                    .setFields(
                        new UrnArray(
                            UrnUtils.getUrn("urn:li:schemaField:(" + DATASET_B + ",col_a)"))));

    Assert.assertFalse(validate(OperationFingerprint.EMPTY, upsert(info)).isEmpty());

    grantEditAssertionsOn(DATASET_B);
    Assert.assertTrue(validate(OperationFingerprint.EMPTY, upsert(info)).isEmpty());
  }

  @Test
  public void testPrivilegeOnAssertionItselfDoesNotAttach() {
    grantEditAssertionsOn(ASSERTION_URN);

    Assert.assertFalse(
        validate(OperationFingerprint.EMPTY, upsert(freshnessOn(DATASET_A))).isEmpty());
  }

  @Test
  public void testRunEventWithoutAsserteeRejected() {
    mockPersisted(freshnessOn(DATASET_A));
    grantEditAssertionsOn(DATASET_A);
    AssertionRunEvent runEvent =
        new AssertionRunEvent()
            .setTimestampMillis(0L)
            .setRunId("run")
            .setAssertionUrn(ASSERTION_URN)
            .setStatus(AssertionRunStatus.COMPLETE);
    BatchItem item =
        TestMCP.ofOneUpsertItem(ASSERTION_URN, runEvent, new TestEntityRegistry()).stream()
            .findFirst()
            .get();

    Assert.assertEquals(validate(OperationFingerprint.EMPTY, item).size(), 1);
  }

  @Test
  public void testPatchKeepingTarget_noAssertedEntityCheck() {
    mockPersisted(freshnessOn(DATASET_A));

    Assert.assertTrue(
        validate(OperationFingerprint.EMPTY, genericPatch("/description", "updated")).isEmpty());
  }

  @Test
  public void testProposedItemPatchKeepingTarget_noAssertedEntityCheck() {
    // Alternate MCP validation hands validators ProposedItems, whose PATCH payload is a JSON patch
    // rather than an aspect. Rewriting the sub-record with the same target changes nothing that
    // needs the asserted-entity privilege.
    mockPersisted(datasetAssertionOn(DATASET_A));

    Assert.assertTrue(
        validate(OperationFingerprint.EMPTY, proposedItem(datasetAssertionPatch(DATASET_A)))
            .isEmpty());
  }

  @Test
  public void testProposedItemPatchRetargetRequiresPrivilegeOnNewTarget() {
    mockPersisted(datasetAssertionOn(DATASET_A));
    grantEditAssertionsOn(DATASET_A);
    BatchItem patch = proposedItem(datasetAssertionPatch(DATASET_B));

    Assert.assertEquals(validate(OperationFingerprint.EMPTY, patch).size(), 1);

    grantEditAssertionsOn(DATASET_B);
    Assert.assertTrue(validate(OperationFingerprint.EMPTY, patch).isEmpty());
  }

  @Test
  public void testRunEventForMismatchedAsseteeAllowedWithPrivilegeOnBoth() {
    // dbt can report results whose assertee differs from the target by environment or URN case.
    mockPersisted(freshnessOn(DATASET_A));
    grantEditAssertionsOn(DATASET_A);
    grantEditAssertionsOn(DATASET_A_DEV);
    grantEditAssertionsOn(DATASET_A_UPPER);

    Assert.assertTrue(validate(OperationFingerprint.EMPTY, runEventOn(DATASET_A_DEV)).isEmpty());
    Assert.assertTrue(validate(OperationFingerprint.EMPTY, runEventOn(DATASET_A_UPPER)).isEmpty());
  }

  @Test
  public void testRunEventDeniedWithPrivilegeOnAsseteeOnly() {
    mockPersisted(freshnessOn(DATASET_A));
    grantEditAssertionsOn(DATASET_B);

    List<AspectValidationException> failures =
        validate(OperationFingerprint.EMPTY, runEventOn(DATASET_B));

    Assert.assertEquals(failures.size(), 1);
    Assert.assertEquals(failures.get(0).getSubType(), ValidationSubType.AUTHORIZATION);
  }

  @Test
  public void testRunEventDeniedWithPrivilegeOnTargetOnly() {
    mockPersisted(freshnessOn(DATASET_A));
    grantEditAssertionsOn(DATASET_A);

    List<AspectValidationException> failures =
        validate(OperationFingerprint.EMPTY, runEventOn(DATASET_B));

    Assert.assertEquals(failures.size(), 1);
    Assert.assertEquals(failures.get(0).getSubType(), ValidationSubType.AUTHORIZATION);
  }

  @Test
  public void testSameBatchInfoAndRunEventForNewTarget() {
    grantEditAssertionsOn(DATASET_A);

    Assert.assertTrue(
        validate(OperationFingerprint.EMPTY, upsert(freshnessOn(DATASET_A)), runEventOn(DATASET_A))
            .isEmpty());
  }

  @Test
  public void testSameBatchRunEventNeedsPrivilegeOnNewTarget() {
    // The run event is checked against the target the upsert before it sets, not the stored one.
    mockPersisted(freshnessOn(DATASET_B));
    grantEditAssertionsOn(DATASET_B);

    List<AspectValidationException> failures =
        validate(OperationFingerprint.EMPTY, upsert(freshnessOn(DATASET_A)), runEventOn(DATASET_B));

    Assert.assertEquals(
        failures.stream().map(AspectValidationException::getAspectName).toList(),
        List.of(ASSERTION_INFO_ASPECT_NAME, ASSERTION_RUN_EVENT_ASPECT_NAME));
  }

  @Test
  public void testRetargetWithRunEventAllowedWithPrivilegeOnBothEntities() {
    mockPersisted(freshnessOn(DATASET_A));
    grantEditAssertionsOn(DATASET_A);
    grantEditAssertionsOn(DATASET_B);

    Assert.assertTrue(
        validate(OperationFingerprint.EMPTY, upsert(freshnessOn(DATASET_B)), runEventOn(DATASET_B))
            .isEmpty());
  }

  @Test
  public void testRunEventDeniedWithoutEditAssertionsOnAssertee() {
    // The generic privilege on the assertion (checked by the API gate) does not cover the assertee.
    mockPersisted(freshnessOn(DATASET_A));

    List<AspectValidationException> failures =
        validate(OperationFingerprint.EMPTY, runEventOn(DATASET_A));

    Assert.assertEquals(failures.size(), 1);
    Assert.assertEquals(failures.get(0).getAspectName(), ASSERTION_RUN_EVENT_ASPECT_NAME);
  }

  @Test
  public void testRunEventWithoutTargetStillNeedsPrivilegeOnAssertee() {
    // With no assertionInfo the API gate requires the privilege on the assertion itself; the
    // assertee, which receives the notifications, is checked here as well.
    Assert.assertFalse(validate(OperationFingerprint.EMPTY, runEventOn(DATASET_A)).isEmpty());

    grantEditAssertionsOn(DATASET_A);
    Assert.assertTrue(validate(OperationFingerprint.EMPTY, runEventOn(DATASET_A)).isEmpty());
  }

  @Test
  public void testRunEventDatedInTheFutureRejected() {
    mockPersisted(freshnessOn(DATASET_A));
    grantEditAssertionsOn(DATASET_A);
    long now = System.currentTimeMillis();

    List<AspectValidationException> failures =
        validate(
            OperationFingerprint.EMPTY, runEventOn(DATASET_A, now + Duration.ofDays(1).toMillis()));
    Assert.assertEquals(failures.size(), 1);
    Assert.assertNotEquals(failures.get(0).getSubType(), ValidationSubType.AUTHORIZATION);

    Assert.assertTrue(validate(OperationFingerprint.EMPTY, runEventOn(DATASET_A, now)).isEmpty());
  }

  @Test
  public void testSoftDeleteNeedsPrivilegeOnTarget() {
    mockPersisted(freshnessOn(DATASET_A));
    grantEditAssertionsOn(DATASET_B);
    BatchItem softDelete = statusUpsert(true);

    Assert.assertFalse(validate(OperationFingerprint.EMPTY, softDelete).isEmpty());

    grantEditAssertionsOn(DATASET_A);
    Assert.assertTrue(validate(OperationFingerprint.EMPTY, softDelete).isEmpty());
  }

  @Test
  public void testRestoreNeedsPrivilegeOnTarget() {
    mockPersisted(freshnessOn(DATASET_A));
    mockPersistedStatus(new Status().setRemoved(true));

    Assert.assertFalse(validate(OperationFingerprint.EMPTY, statusUpsert(false)).isEmpty());
    Assert.assertFalse(validate(OperationFingerprint.EMPTY, delete(STATUS_ASPECT_NAME)).isEmpty());

    grantEditAssertionsOn(DATASET_A);
    Assert.assertTrue(validate(OperationFingerprint.EMPTY, statusUpsert(false)).isEmpty());
  }

  @Test
  public void testSoftDeleteAndRestoreSkipTargetThatNoLongerExists() {
    mockPersisted(freshnessOn(DATASET_A));
    mockDeleted(DATASET_A);

    Assert.assertTrue(validate(OperationFingerprint.EMPTY, statusUpsert(true)).isEmpty());

    mockPersistedStatus(new Status().setRemoved(true));
    Assert.assertTrue(validate(OperationFingerprint.EMPTY, statusUpsert(false)).isEmpty());
  }

  @Test
  public void testStatusWriteThatKeepsRemovedNotChecked() {
    // Ingestion writes status removed=false for every entity it emits; that changes nothing.
    mockPersisted(freshnessOn(DATASET_A));

    Assert.assertTrue(validate(OperationFingerprint.EMPTY, statusUpsert(false)).isEmpty());
  }

  @Test
  public void testRunSummaryNeedsPrivilegeOnTargetAndPastTimestamps() {
    mockPersisted(freshnessOn(DATASET_A));
    long now = System.currentTimeMillis();
    BatchItem summary = runSummaryUpsert(now);

    Assert.assertFalse(validate(OperationFingerprint.EMPTY, summary).isEmpty());

    grantEditAssertionsOn(DATASET_A);
    Assert.assertTrue(validate(OperationFingerprint.EMPTY, summary).isEmpty());
    Assert.assertFalse(
        validate(OperationFingerprint.EMPTY, runSummaryUpsert(now + Duration.ofDays(1).toMillis()))
            .isEmpty());
  }

  @Test
  public void testUnreadableRunEventPatchDeniedNotFailed() {
    // Under alternate MCP validation the item carries the raw PATCH payload.
    mockPersisted(freshnessOn(DATASET_A));
    grantEditAssertionsOn(DATASET_A);
    MetadataChangeProposal patch =
        genericPatchProposal(List.of(patchOp("/status", AssertionRunStatus.COMPLETE.name())))
            .setAspectName(ASSERTION_RUN_EVENT_ASPECT_NAME);

    List<AspectValidationException> failures =
        validate(OperationFingerprint.EMPTY, proposedItem(patch));

    Assert.assertEquals(failures.size(), 1);
    Assert.assertEquals(failures.get(0).getAspectName(), ASSERTION_RUN_EVENT_ASPECT_NAME);
  }

  @Test
  public void testDeleteDeniedWithoutEditAssertionsOnTarget() {
    mockPersisted(freshnessOn(DATASET_A));
    grantEditAssertionsOn(DATASET_B);

    Assert.assertFalse(
        validate(OperationFingerprint.EMPTY, delete(ASSERTION_KEY_ASPECT_NAME)).isEmpty());
    Assert.assertFalse(
        validate(OperationFingerprint.EMPTY, delete(ASSERTION_INFO_ASPECT_NAME)).isEmpty());
  }

  @Test
  public void testDeleteAllowedWithEditAssertionsOnTarget() {
    mockPersisted(freshnessOn(DATASET_A));
    grantEditAssertionsOn(DATASET_A);

    Assert.assertTrue(
        validate(OperationFingerprint.EMPTY, delete(ASSERTION_KEY_ASPECT_NAME)).isEmpty());
    Assert.assertTrue(
        validate(OperationFingerprint.EMPTY, delete(ASSERTION_INFO_ASPECT_NAME)).isEmpty());
  }

  @Test
  public void testDeleteSkipsDeletedTargetButChecksLiveOnes() {
    // Reference cleanup after a hard delete: the asset is gone, so its policy fields (domain,
    // owners) cannot be resolved and the privilege on it cannot be checked.
    mockPersisted(freshnessOn(DATASET_A));
    mockDeleted(DATASET_A);

    Assert.assertTrue(
        validate(OperationFingerprint.EMPTY, delete(ASSERTION_KEY_ASPECT_NAME)).isEmpty());
    Assert.assertTrue(
        validate(OperationFingerprint.EMPTY, delete(ASSERTION_INFO_ASPECT_NAME)).isEmpty());

    mockPersisted(
        freshnessOn(DATASET_A).setVolumeAssertion(volumeOn(DATASET_B).getVolumeAssertion()));
    List<AspectValidationException> failures =
        validate(OperationFingerprint.EMPTY, delete(ASSERTION_KEY_ASPECT_NAME));
    Assert.assertEquals(failures.size(), 1);
    Assert.assertTrue(failures.get(0).getMessage().contains(DATASET_B.toString()));
    Assert.assertFalse(failures.get(0).getMessage().contains(DATASET_A.toString()));
  }

  @Test
  public void testDetachFromDeletedTargetNeedsNoPrivilegeOnIt() {
    // Stored targets: A (freshness) and B (volume). The upsert keeps B and drops A.
    mockPersisted(
        freshnessOn(DATASET_A).setVolumeAssertion(volumeOn(DATASET_B).getVolumeAssertion()));
    TestMCP detach = upsert(volumeOn(DATASET_B));

    Assert.assertFalse(validate(OperationFingerprint.EMPTY, detach).isEmpty());

    mockDeleted(DATASET_A);
    Assert.assertTrue(validate(OperationFingerprint.EMPTY, detach).isEmpty());
  }

  @Test
  public void testAttachToMissingTargetStillChecked() {
    mockDeleted(DATASET_A);

    Assert.assertFalse(
        validate(OperationFingerprint.EMPTY, upsert(freshnessOn(DATASET_A))).isEmpty());
  }

  @Test
  public void testDeleteWithoutAssertionInfo_noAssertedEntityCheck() {
    // Matches DeleteAssertionResolver: with no assertionInfo only the API gate applies.
    Assert.assertTrue(
        validate(OperationFingerprint.EMPTY, delete(ASSERTION_KEY_ASPECT_NAME)).isEmpty());
  }

  @Test
  public void testSystemActorSkipsDeleteCheck() {
    mockPersisted(freshnessOn(DATASET_A));
    OperationFingerprint systemFingerprint = mock(OperationFingerprint.class);
    when(systemFingerprint.isSystemAuth()).thenReturn(true);

    Assert.assertTrue(validate(systemFingerprint, delete(ASSERTION_KEY_ASPECT_NAME)).isEmpty());
  }

  private void grantEditAssertionsOn(Urn entityUrn) {
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(session),
                    eq(EDIT_ASSERTIONS_PRIVILEGES),
                    eq(new EntitySpec(entityUrn.getEntityType(), entityUrn.toString()))))
        .thenReturn(true);
  }

  private void mockPersisted(AssertionInfo info) {
    when(aspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(ASSERTION_URN)), eq(Set.of(ASSERTION_INFO_ASPECT_NAME))))
        .thenReturn(
            info == null
                ? Map.of()
                : Map.of(
                    ASSERTION_URN, Map.of(ASSERTION_INFO_ASPECT_NAME, new Aspect(info.data()))));
  }

  private static AssertionInfo freshnessOn(Urn entityUrn) {
    return new AssertionInfo()
        .setType(AssertionType.FRESHNESS)
        .setFreshnessAssertion(
            new FreshnessAssertionInfo()
                .setType(FreshnessAssertionType.DATASET_CHANGE)
                .setEntity(entityUrn)
                .setSchedule(
                    new FreshnessAssertionSchedule()
                        .setType(FreshnessAssertionScheduleType.FIXED_INTERVAL)))
        .setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));
  }

  private static AssertionInfo datasetAssertionOn(Urn datasetUrn) {
    return new AssertionInfo()
        .setType(AssertionType.DATASET)
        .setCustomProperties(new StringMap(Map.of("suite", "initial")))
        .setDatasetAssertion(
            new DatasetAssertionInfo()
                .setDataset(datasetUrn)
                .setScope(DatasetAssertionScope.DATASET_ROWS)
                .setOperator(AssertionStdOperator.BETWEEN));
  }

  /**
   * A generic PATCH that rewrites {@code type} and the whole dataset sub-record (pointing it at
   * {@code datasetUrn}) and adds a custom property, as clients that re-emit a definition send it.
   */
  private static MetadataChangeProposal datasetAssertionPatch(Urn datasetUrn) {
    return genericPatchProposal(
        List.of(
            patchOp("/type", AssertionType.DATASET.name()),
            patchOp(
                "/datasetAssertion",
                Map.of(
                    "dataset", datasetUrn.toString(),
                    "scope", DatasetAssertionScope.DATASET_SCHEMA.name(),
                    "operator", AssertionStdOperator.EQUAL_TO.name())),
            patchOp("/customProperties/suite", "updated")));
  }

  private static GenericJsonPatch.PatchOp patchOp(String path, Object value) {
    GenericJsonPatch.PatchOp op = new GenericJsonPatch.PatchOp();
    op.setOp("add");
    op.setPath(path);
    op.setValue(value);
    return op;
  }

  private static MetadataChangeProposal genericPatchProposal(List<GenericJsonPatch.PatchOp> ops) {
    return new MetadataChangeProposal()
        .setEntityUrn(ASSERTION_URN)
        .setEntityType(ASSERTION_ENTITY_NAME)
        .setAspectName(ASSERTION_INFO_ASPECT_NAME)
        .setChangeType(ChangeType.PATCH)
        .setAspect(
            GenericRecordUtils.serializePatch(
                GenericJsonPatch.builder().patch(ops).forceGenericPatch(true).build(),
                new ObjectMapper()));
  }

  private static BatchItem proposedItem(MetadataChangeProposal mcp) {
    return ProposedItem.builder()
        .build(mcp, AuditStampUtils.createDefaultAuditStamp(), new TestEntityRegistry());
  }

  private static AssertionInfo volumeOn(Urn entityUrn) {
    return new AssertionInfo()
        .setType(AssertionType.VOLUME)
        .setVolumeAssertion(
            new VolumeAssertionInfo()
                .setType(VolumeAssertionType.ROW_COUNT_TOTAL)
                .setEntity(entityUrn))
        .setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));
  }

  private static BatchItem genericPatch(String path, String value) {
    return patchItem(genericPatchProposal(List.of(patchOp(path, value))));
  }

  private static BatchItem patchItem(MetadataChangeProposal mcp) {
    return PatchItemImpl.builder()
        .build(mcp, AuditStampUtils.createDefaultAuditStamp(), new TestEntityRegistry());
  }

  private static BatchItem runEventOn(Urn asserteeUrn) {
    return runEventOn(asserteeUrn, 0L);
  }

  private static BatchItem runEventOn(Urn asserteeUrn, long timestampMillis) {
    AssertionRunEvent runEvent =
        new AssertionRunEvent()
            .setTimestampMillis(timestampMillis)
            .setRunId("run")
            .setAssertionUrn(ASSERTION_URN)
            .setAsserteeUrn(asserteeUrn)
            .setStatus(AssertionRunStatus.COMPLETE);
    return TestMCP.ofOneUpsertItem(ASSERTION_URN, runEvent, new TestEntityRegistry()).stream()
        .findFirst()
        .get();
  }

  private static BatchItem statusUpsert(boolean removed) {
    return TestMCP.ofOneUpsertItem(
            ASSERTION_URN, new Status().setRemoved(removed), new TestEntityRegistry())
        .stream()
        .findFirst()
        .get();
  }

  private static BatchItem runSummaryUpsert(long lastFailedAtMillis) {
    return TestMCP.ofOneUpsertItem(
            ASSERTION_URN,
            new AssertionRunSummary().setLastFailedAtMillis(lastFailedAtMillis),
            new TestEntityRegistry())
        .stream()
        .findFirst()
        .get();
  }

  /** Every entity exists except {@code deleted}. */
  private void mockDeleted(Urn... deleted) {
    Set<Urn> gone = Set.of(deleted);
    // doAnswer, so re-stubbing does not invoke the previous answer.
    Mockito.doAnswer(
            invocation -> {
              Set<Urn> urns = invocation.getArgument(1);
              return urns.stream()
                  .collect(Collectors.toMap(urn -> urn, urn -> !gone.contains(urn)));
            })
        .when(aspectRetriever)
        .entityExists(any(), any());
  }

  private void mockPersistedStatus(Status status) {
    when(aspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(ASSERTION_URN)), eq(Set.of(STATUS_ASPECT_NAME))))
        .thenReturn(Map.of(ASSERTION_URN, Map.of(STATUS_ASPECT_NAME, new Aspect(status.data()))));
  }

  private static BatchItem delete(String aspectName) {
    com.linkedin.metadata.models.EntitySpec entitySpec =
        new TestEntityRegistry().getEntitySpec(ASSERTION_ENTITY_NAME);
    return TestMCP.builder()
        .urn(ASSERTION_URN)
        .changeType(ChangeType.DELETE)
        .entitySpec(entitySpec)
        .aspectSpec(entitySpec.getAspectSpec(aspectName))
        .build();
  }

  private static TestMCP upsert(AssertionInfo info) {
    return TestMCP.ofOneUpsertItem(ASSERTION_URN, info, new TestEntityRegistry()).stream()
        .map(i -> (TestMCP) i)
        .findFirst()
        .get();
  }

  private List<AspectValidationException> validate(
      OperationFingerprint fingerprint, BatchItem... items) {
    return validator
        .validateProposedAspectsWithAuth(
            fingerprint,
            List.of(items),
            TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever)
                .getRetrieverContext(),
            session)
        .collect(Collectors.toList());
  }
}
