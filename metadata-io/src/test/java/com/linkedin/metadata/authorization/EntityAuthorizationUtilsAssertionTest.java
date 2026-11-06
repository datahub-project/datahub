package com.linkedin.metadata.authorization;

import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.datahub.authorization.EntitySpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessAssertionScheduleType;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.assertion.VolumeAssertionType;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ReadPreference;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Assertion writes through the API gate: the asserted-entity fallback for denied callers. */
public class EntityAuthorizationUtilsAssertionTest {

  private static final Urn ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:facade-assertion");
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,facade,PROD)");
  private static final Urn OTHER_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,other_table,PROD)");
  private static final Urn DEV_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,facade,DEV)");
  private static final DisjunctivePrivilegeGroup EDIT_ASSERTIONS_PRIVILEGES =
      new DisjunctivePrivilegeGroup(
          ImmutableList.of(
              new ConjunctivePrivilegeGroup(
                  ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType())),
              new ConjunctivePrivilegeGroup(
                  ImmutableList.of(PoliciesConfig.EDIT_ENTITY_ASSERTIONS_PRIVILEGE.getType()))));

  private final EntityRegistry registry = new TestEntityRegistry();
  private OperationContext opContext;
  private AspectRetriever aspectRetriever;
  private MockedStatic<AuthUtil> authUtilMock;

  @BeforeMethod
  public void setUp() {
    opContext = mock(OperationContext.class);
    aspectRetriever = mock(AspectRetriever.class);
    when(opContext.getAspectRetriever()).thenReturn(aspectRetriever);
    when(opContext.isSystemAuth()).thenReturn(false);
    when(opContext.withReadPreference(ReadPreference.PRIMARY)).thenReturn(opContext);
    when(opContext.getAuditStamp()).thenReturn(AuditStampUtils.createDefaultAuditStamp());
    when(aspectRetriever.getEntityRegistry()).thenReturn(registry);
    authUtilMock = Mockito.mockStatic(AuthUtil.class);
    authUtilMock.when(AuthUtil::isRestApiAuthorizationEnabled).thenReturn(true);
  }

  @AfterMethod
  public void tearDown() {
    authUtilMock.close();
  }

  @Test
  public void testFallsBackToEditAssertionsOnProposedAssertee() {
    MetadataChangeProposal proposal =
        upsert(Constants.ASSERTION_INFO_ASPECT_NAME, freshnessOn(DATASET_URN));
    denyOnAssertion(false);

    assertEquals(ingest(proposal), List.of(Pair.of(proposal, 403)));

    grantEditAssertionsOn(DATASET_URN);
    assertEquals(ingest(proposal), List.of(Pair.of(proposal, 200)));
  }

  @Test
  public void testUpsertRetargetNeedsBothEntities() {
    mockPersisted(freshnessOn(DATASET_URN));
    MetadataChangeProposal proposal =
        upsert(Constants.ASSERTION_INFO_ASPECT_NAME, freshnessOn(OTHER_DATASET_URN));
    denyOnAssertion(true);
    grantEditAssertionsOn(OTHER_DATASET_URN);

    assertEquals(ingest(proposal), List.of(Pair.of(proposal, 403)));

    grantEditAssertionsOn(DATASET_URN);
    assertEquals(ingest(proposal), List.of(Pair.of(proposal, 200)));
  }

  @Test
  public void testSiblingSubRecordIsATarget() {
    // type selects the freshness sub-record, but the volume sub-record is indexed as Asserts too.
    AssertionInfo info =
        freshnessOn(DATASET_URN)
            .setVolumeAssertion(
                new VolumeAssertionInfo()
                    .setType(VolumeAssertionType.ROW_COUNT_TOTAL)
                    .setEntity(OTHER_DATASET_URN));
    MetadataChangeProposal proposal = upsert(Constants.ASSERTION_INFO_ASPECT_NAME, info);
    denyOnAssertion(false);
    grantEditAssertionsOn(DATASET_URN);

    assertEquals(ingest(proposal), List.of(Pair.of(proposal, 403)));

    grantEditAssertionsOn(OTHER_DATASET_URN);
    assertEquals(ingest(proposal), List.of(Pair.of(proposal, 200)));
  }

  @Test
  public void testStatusBeforeInfoDenied() {
    // status/dataPlatformInstance do not attach an assertion; without assertionInfo there is no
    // entity to authorize against, so a caller denied on the assertion cannot write them.
    MetadataChangeProposal proposal =
        upsert(Constants.STATUS_ASPECT_NAME, new Status().setRemoved(true));
    denyOnAssertion(false);
    grantEditAssertionsOn(DATASET_URN);

    assertEquals(ingest(proposal), List.of(Pair.of(proposal, 403)));
  }

  @Test
  public void testPlatformInstanceAllowedWithInfoInSameRequest() {
    MetadataChangeProposal platform =
        upsert(
            Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
            new DataPlatformInstance().setPlatform(UrnUtils.getUrn("urn:li:dataPlatform:dbt")));
    MetadataChangeProposal info =
        upsert(Constants.ASSERTION_INFO_ASPECT_NAME, freshnessOn(DATASET_URN));
    denyOnAssertion(false);
    grantEditAssertionsOn(DATASET_URN);

    assertEquals(ingest(platform, info), List.of(Pair.of(platform, 200), Pair.of(info, 200)));
  }

  @Test
  public void testPatchRetargetNeedsBothEntities() {
    mockPersisted(freshnessOn(DATASET_URN));
    MetadataChangeProposal patch = retargetPatch(OTHER_DATASET_URN);
    denyOnAssertion(true);
    grantEditAssertionsOn(OTHER_DATASET_URN);

    assertEquals(ingest(patch), List.of(Pair.of(patch, 403)));

    grantEditAssertionsOn(DATASET_URN);
    assertEquals(ingest(patch), List.of(Pair.of(patch, 200)));
  }

  @Test
  public void testPatchAppliesOnTopOfEarlierWriteInSameRequest() {
    // Stored: freshness on DATASET_URN. The upsert switches to volume on DATASET_URN, then the
    // PATCH moves the volume sub-record to OTHER_DATASET_URN. Against the stored value alone the
    // PATCH would change nothing that counts; applied in order it re-targets the assertion.
    mockPersisted(freshnessOn(DATASET_URN));
    MetadataChangeProposal upsert =
        upsert(Constants.ASSERTION_INFO_ASPECT_NAME, volumeOn(DATASET_URN));
    MetadataChangeProposal patch = genericPatch("/volumeAssertion/entity", OTHER_DATASET_URN);
    denyOnAssertion(true);
    grantEditAssertionsOn(DATASET_URN);

    assertEquals(ingest(upsert, patch), List.of(Pair.of(upsert, 403), Pair.of(patch, 403)));

    grantEditAssertionsOn(OTHER_DATASET_URN);
    assertEquals(ingest(upsert, patch), List.of(Pair.of(upsert, 200), Pair.of(patch, 200)));
  }

  @Test
  public void testDeleteProposalNeedsPrivilegeOnPersistedTarget() {
    mockPersisted(freshnessOn(DATASET_URN));
    MetadataChangeProposal delete =
        new MetadataChangeProposal()
            .setEntityType(Constants.ASSERTION_ENTITY_NAME)
            .setEntityUrn(ASSERTION_URN)
            .setChangeType(ChangeType.DELETE)
            .setAspectName(Constants.ASSERTION_INFO_ASPECT_NAME);
    denyOnAssertion(true);

    assertEquals(ingest(delete), List.of(Pair.of(delete, 403)));

    grantEditAssertionsOn(DATASET_URN);
    assertEquals(ingest(delete), List.of(Pair.of(delete, 200)));
  }

  @Test
  public void testRunEventWithoutTargetNeedsPrivilegeOnAssertion() {
    // Without assertionInfo there is no asset to authorize against: Edit Assertions on the asset
    // a result names would let a caller plant results on an assertion another asset defines later.
    MetadataChangeProposal proposal = runEvent(DATASET_URN);
    denyOnAssertion(false);
    grantEditAssertionsOn(DATASET_URN);

    assertEquals(ingest(proposal), List.of(Pair.of(proposal, 403)));
  }

  @Test
  public void testRunEventForOtherAsseteeNeedsPrivilegeOnIt() {
    mockPersisted(freshnessOn(DATASET_URN));
    MetadataChangeProposal proposal = runEvent(OTHER_DATASET_URN);
    denyOnAssertion(true);
    grantEditAssertionsOn(DATASET_URN);

    assertEquals(ingest(proposal), List.of(Pair.of(proposal, 403)));
  }

  @Test
  public void testRunEventForMismatchedAsseteeAllowedWithPrivilegeOnBoth() {
    mockPersisted(freshnessOn(DATASET_URN));
    MetadataChangeProposal proposal = runEvent(DEV_DATASET_URN);
    denyOnAssertion(true);
    grantEditAssertionsOn(DATASET_URN);
    grantEditAssertionsOn(DEV_DATASET_URN);

    assertEquals(ingest(proposal), List.of(Pair.of(proposal, 200)));
  }

  @Test
  public void testMalformedSchemaFieldDeniedNotFailed() {
    // The gate runs before URN validation, so a schema field without a parent URN must not throw.
    Urn malformed = UrnUtils.getUrn("urn:li:schemaField:(notaurn,col_a)");
    mockPersisted(freshnessOn(DATASET_URN));
    MetadataChangeProposal runEvent = runEvent(malformed);
    MetadataChangeProposal info =
        upsert(
            Constants.ASSERTION_INFO_ASPECT_NAME, freshnessOn(DATASET_URN).setEntityUrn(malformed));
    denyOnAssertion(true);
    grantEditAssertionsOn(DATASET_URN);

    assertEquals(ingest(runEvent), List.of(Pair.of(runEvent, 403)));
    assertEquals(ingest(info), List.of(Pair.of(info, 403)));
  }

  @Test
  public void testRunEventWithoutAsserteeDeniedNotFailed() {
    MetadataChangeProposal proposal =
        upsert(
            Constants.ASSERTION_RUN_EVENT_ASPECT_NAME,
            new AssertionRunEvent()
                .setTimestampMillis(0L)
                .setRunId("run")
                .setAssertionUrn(ASSERTION_URN)
                .setStatus(AssertionRunStatus.COMPLETE));
    denyOnAssertion(false);
    grantEditAssertionsOn(DATASET_URN);

    assertEquals(ingest(proposal), List.of(Pair.of(proposal, 403)));
  }

  @Test
  public void testEntityUrnsDeleteUsesPersistedAssertee() {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAPIAuthorizedEntityUrns(
                    opContext, ApiOperation.DELETE, List.of(ASSERTION_URN)))
        .thenReturn(false);
    mockPersisted(freshnessOn(DATASET_URN));

    assertFalse(
        EntityAuthorizationUtils.isAPIAuthorizedEntityUrns(
            opContext, ApiOperation.DELETE, List.of(ASSERTION_URN)));

    grantEditAssertionsOn(DATASET_URN);
    assertTrue(
        EntityAuthorizationUtils.isAPIAuthorizedEntityUrns(
            opContext, ApiOperation.DELETE, List.of(ASSERTION_URN)));
  }

  @Test
  public void testRunHistoryDeleteNeedsPrivilegeOnPersistedTarget() {
    mockPersisted(freshnessOn(DATASET_URN));
    when(aspectRetriever.entityExists(opContext, Set.of(DATASET_URN)))
        .thenReturn(Map.of(DATASET_URN, true));

    assertFalse(
        AssertionAuthorizationUtils.isAuthorizedToDeleteRunHistory(
            opContext, List.of(ASSERTION_URN)));
    assertTrue(
        AssertionAuthorizationUtils.isAuthorizedToDeleteRunHistory(
            opContext, List.of(DATASET_URN)));

    grantEditAssertionsOn(DATASET_URN);
    assertTrue(
        AssertionAuthorizationUtils.isAuthorizedToDeleteRunHistory(
            opContext, List.of(ASSERTION_URN)));
    // Targets are read from the primary, so a lagging replica cannot hide one.
    verify(opContext, atLeastOnce()).withReadPreference(ReadPreference.PRIMARY);
  }

  @Test
  public void testRunHistoryDeleteSkipsTargetThatNoLongerExists() {
    // After the asset is hard deleted its policy fields cannot be resolved, as in the validator.
    mockPersisted(freshnessOn(DATASET_URN));
    when(aspectRetriever.entityExists(opContext, Set.of(DATASET_URN)))
        .thenReturn(Map.of(DATASET_URN, false));

    assertTrue(
        AssertionAuthorizationUtils.isAuthorizedToDeleteRunHistory(
            opContext, List.of(ASSERTION_URN)));
  }

  private List<Pair<MetadataChangeProposal, Integer>> ingest(MetadataChangeProposal... mcps) {
    return EntityAuthorizationUtils.isAPIAuthorizedIngest(opContext, registry, List.of(mcps));
  }

  /** Denies every write on the assertion itself, so only the asserted-entity fallback can allow. */
  @SuppressWarnings("unchecked")
  private void denyOnAssertion(boolean exists) {
    when(aspectRetriever.entityExists(opContext, Set.of(ASSERTION_URN)))
        .thenReturn(Map.of(ASSERTION_URN, exists));
    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedUrns(eq(opContext), eq(ENTITY), anySet(), any()))
        .thenAnswer(
            invocation ->
                ((Set<Pair<ChangeType, Urn>>) invocation.getArgument(2))
                    .stream().collect(Collectors.toMap(Function.identity(), key -> 403)));
  }

  private void grantEditAssertionsOn(Urn entityUrn) {
    authUtilMock
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(opContext),
                    eq(EDIT_ASSERTIONS_PRIVILEGES),
                    eq(new EntitySpec(entityUrn.getEntityType(), entityUrn.toString()))))
        .thenReturn(true);
  }

  private void mockPersisted(AssertionInfo info) {
    when(aspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(ASSERTION_URN)), eq(Set.of(Constants.ASSERTION_INFO_ASPECT_NAME))))
        .thenReturn(
            Map.of(
                ASSERTION_URN,
                Map.of(Constants.ASSERTION_INFO_ASPECT_NAME, new Aspect(info.data()))));
  }

  private static MetadataChangeProposal upsert(String aspectName, RecordTemplate aspect) {
    return new MetadataChangeProposal()
        .setEntityType(Constants.ASSERTION_ENTITY_NAME)
        .setEntityUrn(ASSERTION_URN)
        .setChangeType(ChangeType.UPSERT)
        .setAspectName(aspectName)
        .setAspect(GenericRecordUtils.serializeAspect(aspect));
  }

  private static MetadataChangeProposal runEvent(Urn asserteeUrn) {
    return upsert(
        Constants.ASSERTION_RUN_EVENT_ASPECT_NAME,
        new AssertionRunEvent()
            .setTimestampMillis(0L)
            .setRunId("run")
            .setAssertionUrn(ASSERTION_URN)
            .setAsserteeUrn(asserteeUrn)
            .setStatus(AssertionRunStatus.COMPLETE));
  }

  private static MetadataChangeProposal retargetPatch(Urn entityUrn) {
    return genericPatch("/freshnessAssertion/entity", entityUrn);
  }

  private static MetadataChangeProposal genericPatch(String path, Urn value) {
    GenericJsonPatch.PatchOp op = new GenericJsonPatch.PatchOp();
    op.setOp("add");
    op.setPath(path);
    op.setValue(value.toString());
    return new MetadataChangeProposal()
        .setEntityType(Constants.ASSERTION_ENTITY_NAME)
        .setEntityUrn(ASSERTION_URN)
        .setChangeType(ChangeType.PATCH)
        .setAspectName(Constants.ASSERTION_INFO_ASPECT_NAME)
        .setAspect(
            GenericRecordUtils.serializePatch(
                GenericJsonPatch.builder().patch(List.of(op)).forceGenericPatch(true).build(),
                new ObjectMapper()));
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

  private static AssertionInfo volumeOn(Urn entityUrn) {
    return new AssertionInfo()
        .setType(AssertionType.VOLUME)
        .setVolumeAssertion(
            new VolumeAssertionInfo()
                .setType(VolumeAssertionType.ROW_COUNT_TOTAL)
                .setEntity(entityUrn))
        .setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));
  }
}
