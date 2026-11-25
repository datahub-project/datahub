package com.linkedin.metadata.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayMap;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.hooks.OwnershipOwnerTypes;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.EbeanSystemAspect;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.semantic.SemanticNoOpComparator;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * A write hook can complete a proposal after the pre-merge comparison. These tests run the real
 * {@link OwnershipOwnerTypes} hook between the pre-merge and persist-time passes of {@link
 * EntityServiceImpl#applyUpsert}, as production does, and check the version the write persists.
 */
public class WriteHookRematchEntityServiceTest {
  private static final Urn PLATFORM_TEAM = UrnUtils.getUrn("urn:li:corpGroup:data-platform");
  private static final Urn OTHER_TEAM = UrnUtils.getUrn("urn:li:corpGroup:analytics");
  private static final String DEVELOPER_TYPE = "urn:li:ownershipType:__system__developer";

  private OperationContext opContext;
  private EntityRegistry registry;
  private Urn dataFlowUrn;
  private OwnershipOwnerTypes ownerTypesHook;
  private AuditStamp storedAudit;
  private AuditStamp incomingAudit;

  @BeforeClass
  public void setup() {
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    registry = opContext.getEntityRegistry();
    dataFlowUrn = UrnUtils.getUrn("urn:li:dataFlow:(airflow,example_dag,PROD)");
    storedAudit = new AuditStamp().setTime(1_000L).setActor(new CorpuserUrn("stored"));
    incomingAudit = new AuditStamp().setTime(5_000L).setActor(new CorpuserUrn("airflow"));
    ownerTypesHook =
        new OwnershipOwnerTypes()
            .setConfig(
                AspectPluginConfig.builder()
                    .className(OwnershipOwnerTypes.class.getName())
                    .enabled(true)
                    .supportedOperations(List.of("UPSERT"))
                    .supportedEntityAspectNames(
                        List.of(
                            AspectPluginConfig.EntityAspectName.builder()
                                .entityName("*")
                                .aspectName("ownership")
                                .build()))
                    .build());
  }

  @Test
  public void resendCompletedByAHookKeepsTheStoredVersion() throws Exception {
    Ownership stored = withOwnerTypes(ownership(PLATFORM_TEAM), PLATFORM_TEAM);
    WriteAttempt write = write(stored, ownership(PLATFORM_TEAM));

    // Before hooks the proposal lacks ownerTypes, so the pre-merge pass sees a change.
    applyUpsert(write);
    assertEquals(write.latest.getSystemMetadata().getVersion(), "2");

    assertTrue(runHook(write.change));
    assertTrue(DataTemplateUtil.areEqual(write.change.getRecordTemplate(), stored));

    SystemAspect persisted = applyUpsert(write);
    assertEquals(persisted.getSystemMetadata().getVersion(), "1");
    assertEquals(write.change.getSystemMetadata().getVersion(), "1");
    assertEquals(write.change.getNextAspectVersion(), 1L);
    assertTrue(DataTemplateUtil.areEqual(persisted.getRecordTemplate(), stored));
    assertEquals(persisted.getAuditStamp(), storedAudit);
    assertEquals(persisted.getSystemMetadata().getRunId(), "run-2");
    assertEquals(persisted.getSystemMetadata().getLastRunId(), "run-1");
    assertEquals(persisted.getSystemMetadata().getLastObserved().longValue(), 5_000L);
    assertFalse(write.change.isSemanticNoOp());

    // The version-0 row is refreshed for lastObserved, but no history row is written.
    AspectDao dao = mock(AspectDao.class);
    doCallRealMethod()
        .when(dao)
        .saveLatestAspect(any(), any(), any(SystemAspect.class), any(SystemAspect.class), anyInt());
    dao.saveLatestAspect(opContext, null, write.latest, persisted, 20);
    verify(dao, never()).insertAspect(any(), any(), any(SystemAspect.class), anyLong());
    verify(dao, times(1)).updateAspect(any(), any(), eq(persisted));
  }

  @Test
  public void realOwnerChangeStillVersionsAfterTheHook() throws Exception {
    Ownership stored = withOwnerTypes(ownership(PLATFORM_TEAM), PLATFORM_TEAM);
    WriteAttempt write = write(stored, ownership(OTHER_TEAM));

    applyUpsert(write);
    assertTrue(runHook(write.change));
    SystemAspect persisted = applyUpsert(write);

    assertEquals(persisted.getSystemMetadata().getVersion(), "2");
    assertEquals(persisted.getAuditStamp(), incomingAudit);
    assertTrue(
        DataTemplateUtil.areEqual(
            persisted.getRecordTemplate(), withOwnerTypes(ownership(OTHER_TEAM), OTHER_TEAM)));

    AspectDao dao = mock(AspectDao.class);
    doCallRealMethod()
        .when(dao)
        .saveLatestAspect(any(), any(), any(SystemAspect.class), any(SystemAspect.class), anyInt());
    dao.saveLatestAspect(opContext, null, write.latest, persisted, 20);
    // The stored body is archived under its own version before version 0 takes version 2.
    verify(dao, times(1)).insertAspect(any(), any(), any(SystemAspect.class), eq(1L));
  }

  @Test
  public void completeResendKeepsTheStoredVersionWithoutAHook() throws Exception {
    Ownership stored = withOwnerTypes(ownership(PLATFORM_TEAM), PLATFORM_TEAM);
    WriteAttempt write = write(stored, withOwnerTypes(ownership(PLATFORM_TEAM), PLATFORM_TEAM));

    applyUpsert(write);
    assertEquals(write.latest.getSystemMetadata().getVersion(), "1");
    assertFalse(runHook(write.change));
    assertEquals(applyUpsert(write).getSystemMetadata().getVersion(), "1");
  }

  @Test
  public void secondHookCompletedResendInABatchStillLandsItsRunMetadata() throws Exception {
    Ownership stored = withOwnerTypes(ownership(PLATFORM_TEAM), PLATFORM_TEAM);
    WriteAttempt first = write(stored, ownership(PLATFORM_TEAM));
    ChangeMCP second = change(ownership(PLATFORM_TEAM), "run-3", 7_000L, 3L);

    // Pre-merge: the first write advances the row to 2 and the second matches that body.
    applyUpsert(first);
    EntityServiceImpl.applyUpsert(
        second, first.latest, List.of(), null, opContext, SemanticNoOpComparator.disabled());
    assertEquals(first.change.getSystemMetadata().getVersion(), "2");
    assertEquals(second.getSystemMetadata().getVersion(), "2");

    assertTrue(runHook(first.change));
    assertTrue(runHook(second));

    // Persist in batch order, as the legacy and scoped loops do.
    assertFalse(EntityServiceImpl.isObsoleteWrite(first.latest, first.change));
    applyUpsert(first);
    assertEquals(first.latest.getSystemMetadata().getVersion(), "1");

    assertFalse(EntityServiceImpl.isObsoleteWrite(first.latest, second));
    SystemAspect persisted =
        EntityServiceImpl.applyUpsert(
            second, first.latest, List.of(), null, opContext, SemanticNoOpComparator.disabled());
    assertEquals(persisted.getSystemMetadata().getVersion(), "1");
    assertEquals(second.getSystemMetadata().getVersion(), "1");
    assertTrue(DataTemplateUtil.areEqual(persisted.getRecordTemplate(), stored));
    assertEquals(persisted.getSystemMetadata().getRunId(), "run-3");
    assertEquals(persisted.getSystemMetadata().getLastObserved().longValue(), 7_000L);
  }

  @Test
  public void writeOvertakenByALaterVersionIsObsolete() throws Exception {
    Ownership stored = withOwnerTypes(ownership(PLATFORM_TEAM), PLATFORM_TEAM);
    WriteAttempt first = write(stored, ownership(OTHER_TEAM));
    ChangeMCP second = change(ownership(PLATFORM_TEAM), "run-3", 7_000L, 3L);

    applyUpsert(first);
    EntityServiceImpl.applyUpsert(
        second, first.latest, List.of(), null, opContext, SemanticNoOpComparator.disabled());
    assertEquals(first.latest.getSystemMetadata().getVersion(), "3");

    assertTrue(EntityServiceImpl.isObsoleteWrite(first.latest, first.change));
    assertFalse(EntityServiceImpl.isObsoleteWrite(first.latest, second));
  }

  private SystemAspect applyUpsert(WriteAttempt write) {
    return EntityServiceImpl.applyUpsert(
        write.change, write.latest, List.of(), null, opContext, SemanticNoOpComparator.disabled());
  }

  /** Returns whether the hook changed the proposal. */
  private boolean runHook(ChangeMCP change) {
    return ownerTypesHook
        .applyWriteMutation(opContext, List.of(change), opContext.getRetrieverContext())
        .map(Pair::getSecond)
        .reduce(false, Boolean::logicalOr);
  }

  private WriteAttempt write(Ownership stored, Ownership incoming) {
    SystemMetadata initial = new SystemMetadata();
    initial.setRunId("run-1");
    initial.setVersion("1");
    initial.setLastObserved(1_000L);
    EbeanAspectV2 row =
        new EbeanAspectV2(
            dataFlowUrn.toString(),
            "ownership",
            0L,
            RecordUtils.toJsonString(stored),
            new Timestamp(storedAudit.getTime()),
            storedAudit.getActor().toString(),
            null,
            RecordUtils.toJsonString(initial));
    SystemAspect latest = EbeanSystemAspect.builder().forUpdate(row, registry);
    return new WriteAttempt(latest, change(incoming, "run-2", 5_000L, 2L));
  }

  private ChangeMCP change(
      Ownership incoming, String runId, long lastObserved, long nextAspectVersion) {
    SystemMetadata incomingMetadata = new SystemMetadata();
    incomingMetadata.setRunId(runId);
    incomingMetadata.setLastObserved(lastObserved);
    return ChangeItemImpl.builder()
        .urn(dataFlowUrn)
        .aspectName("ownership")
        .recordTemplate(incoming)
        .systemMetadata(incomingMetadata)
        .auditStamp(incomingAudit)
        .nextAspectVersion(nextAspectVersion)
        .build(opContext.getAspectRetriever());
  }

  private record WriteAttempt(SystemAspect latest, ChangeMCP change) {}

  /** Ownership as connectors send it: owners only, no ownerTypes. */
  private static Ownership ownership(Urn owner) {
    return new Ownership()
        .setOwners(
            new OwnerArray(
                new Owner()
                    .setOwner(owner)
                    .setType(OwnershipType.DEVELOPER)
                    .setSource(new OwnershipSource().setType(OwnershipSourceType.SERVICE))))
        .setLastModified(new AuditStamp().setTime(0L).setActor(new CorpuserUrn("airflow")));
  }

  /** Ownership as GMS stores it, with the ownerTypes the hook derives. */
  private static Ownership withOwnerTypes(Ownership ownership, Urn owner) {
    return ownership.setOwnerTypes(
        new UrnArrayMap(Map.of(DEVELOPER_TYPE, new UrnArray(List.of(owner)))));
  }
}
