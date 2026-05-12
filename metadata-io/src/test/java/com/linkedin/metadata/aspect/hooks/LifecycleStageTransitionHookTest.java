package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.lifecycle.LifecycleStageSettings;
import com.linkedin.lifecycle.LifecycleStageTransitionPolicy;
import com.linkedin.lifecycle.LifecycleStageTypeInfo;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LifecycleStageTransitionHookTest {

  private static final Urn TEST_ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:postgres,test.table,PROD)");
  private static final Urn PROPOSED_STAGE = UrnUtils.getUrn("urn:li:lifecycleStageType:PROPOSED");
  private static final Urn CERTIFIED_STAGE = UrnUtils.getUrn("urn:li:lifecycleStageType:CERTIFIED");
  private static final Urn ARCHIVED_STAGE = UrnUtils.getUrn("urn:li:lifecycleStageType:ARCHIVED");

  private static final AspectPluginConfig HOOK_CONFIG =
      AspectPluginConfig.builder()
          .className(LifecycleStageTransitionHook.class.getName())
          .enabled(true)
          .supportedOperations(List.of("UPSERT"))
          .supportedEntityAspectNames(
              List.of(
                  AspectPluginConfig.EntityAspectName.builder()
                      .entityName("*")
                      .aspectName(STATUS_ASPECT_NAME)
                      .build()))
          .build();

  private CachingAspectRetriever mockAspectRetriever;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    mockAspectRetriever = mock(CachingAspectRetriever.class);
    retrieverContext =
        RetrieverContext.builder()
            .searchRetriever(mock(SearchRetriever.class))
            .cachingAspectRetriever(mockAspectRetriever)
            .graphRetriever(GraphRetriever.EMPTY)
            .build();
    // Default: return null for any stage info lookup (no policy → open entry)
    when(mockAspectRetriever.getLatestAspectObject(any(), eq("lifecycleStageTypeInfo")))
        .thenReturn(null);
  }

  @Test
  public void testAllowedTransition_noPolicyOnDestination() {
    // No transition policy on PROPOSED → any prior stage can enter; Status unchanged
    when(mockAspectRetriever.getLatestAspectObject(
            eq(PROPOSED_STAGE), eq("lifecycleStageTypeInfo")))
        .thenReturn(makeStageAspect(true, null));

    Status proposedStatus = makeStatus(PROPOSED_STAGE);
    ChangeMCP item = makeChangeMCP(proposedStatus, null);

    List<Pair<ChangeMCP, Boolean>> result =
        buildHook().writeMutation(List.of(item), retrieverContext).toList();

    assertEquals(result.size(), 1);
    assertFalse(result.get(0).getSecond(), "No mutation expected for allowed transition");
    assertTrue(proposedStatus.hasLifecycleStage(), "Status should retain the new stage");
  }

  @Test
  public void testAllowedTransition_currentStageInAllowedList() {
    // CERTIFIED allows entry from PROPOSED
    LifecycleStageTransitionPolicy policy = new LifecycleStageTransitionPolicy();
    policy.setAllowedPreviousStages(new com.linkedin.common.UrnArray(List.of(PROPOSED_STAGE)));
    when(mockAspectRetriever.getLatestAspectObject(
            eq(CERTIFIED_STAGE), eq("lifecycleStageTypeInfo")))
        .thenReturn(makeStageAspect(false, policy));

    // Proposing CERTIFIED; entity is currently in PROPOSED
    Status proposedStatus = makeStatus(CERTIFIED_STAGE);
    Status previousStatus = makeStatus(PROPOSED_STAGE);
    ChangeMCP item = makeChangeMCP(proposedStatus, previousStatus);

    List<Pair<ChangeMCP, Boolean>> result =
        buildHook().writeMutation(List.of(item), retrieverContext).toList();

    assertEquals(result.size(), 1);
    assertFalse(result.get(0).getSecond(), "PROPOSED → CERTIFIED should be allowed, no mutation");
    assertEquals(proposedStatus.getLifecycleStage(), CERTIFIED_STAGE);
  }

  @Test
  public void testBlockedTransition_currentStageNotInAllowedList() {
    // CERTIFIED only allows entry from PROPOSED, not from ARCHIVED → revert
    LifecycleStageTransitionPolicy policy = new LifecycleStageTransitionPolicy();
    policy.setAllowedPreviousStages(new com.linkedin.common.UrnArray(List.of(PROPOSED_STAGE)));
    when(mockAspectRetriever.getLatestAspectObject(
            eq(CERTIFIED_STAGE), eq("lifecycleStageTypeInfo")))
        .thenReturn(makeStageAspect(false, policy));

    // Proposing CERTIFIED; entity is currently in ARCHIVED (not allowed)
    Status proposedStatus = makeStatus(CERTIFIED_STAGE);
    Status previousStatus = makeStatus(ARCHIVED_STAGE);
    ChangeMCP item = makeChangeMCP(proposedStatus, previousStatus);

    List<Pair<ChangeMCP, Boolean>> result =
        buildHook().writeMutation(List.of(item), retrieverContext).toList();

    assertEquals(result.size(), 1);
    assertTrue(result.get(0).getSecond(), "ARCHIVED → CERTIFIED should be blocked (mutation)");
    // Reverted: lifecycleStage restored to ARCHIVED
    assertEquals(
        proposedStatus.getLifecycleStage(),
        ARCHIVED_STAGE,
        "Stage should be reverted to previous value");
  }

  @Test
  public void testBlockedTransition_emptyAllowedList() {
    // ARCHIVED has empty allowedPreviousStages → unreachable; revert to null
    LifecycleStageTransitionPolicy policy = new LifecycleStageTransitionPolicy();
    policy.setAllowedPreviousStages(new com.linkedin.common.UrnArray());
    when(mockAspectRetriever.getLatestAspectObject(
            eq(ARCHIVED_STAGE), eq("lifecycleStageTypeInfo")))
        .thenReturn(makeStageAspect(true, policy));

    // Proposing ARCHIVED from the null/active state
    Status proposedStatus = makeStatus(ARCHIVED_STAGE);
    ChangeMCP item = makeChangeMCP(proposedStatus, null); // no previous stage

    List<Pair<ChangeMCP, Boolean>> result =
        buildHook().writeMutation(List.of(item), retrieverContext).toList();

    assertEquals(result.size(), 1);
    assertTrue(result.get(0).getSecond(), "Unreachable stage should be blocked (mutation)");
    // Reverted to null/active: lifecycleStage field removed
    assertFalse(
        proposedStatus.hasLifecycleStage(), "Stage should be cleared (reverted to null/active)");
  }

  @Test
  public void testAllowedTransition_fromNoStageViaSentinel() {
    // PROPOSED allows entry only from __NONE__ (default active state)
    Urn noneSentinel = UrnUtils.getUrn("urn:li:lifecycleStageType:__NONE__");
    LifecycleStageTransitionPolicy policy = new LifecycleStageTransitionPolicy();
    policy.setAllowedPreviousStages(new com.linkedin.common.UrnArray(List.of(noneSentinel)));
    when(mockAspectRetriever.getLatestAspectObject(
            eq(PROPOSED_STAGE), eq("lifecycleStageTypeInfo")))
        .thenReturn(makeStageAspect(true, policy));

    // No current stage → matches __NONE__ sentinel → allowed
    Status proposedStatus = makeStatus(PROPOSED_STAGE);
    ChangeMCP item = makeChangeMCP(proposedStatus, null);

    List<Pair<ChangeMCP, Boolean>> result =
        buildHook().writeMutation(List.of(item), retrieverContext).toList();

    assertEquals(result.size(), 1);
    assertFalse(
        result.get(0).getSecond(), "(none) → PROPOSED via __NONE__ sentinel should be allowed");
    assertTrue(proposedStatus.hasLifecycleStage());
  }

  @Test
  public void testClearingStageAlwaysAllowed() {
    // Clearing the stage (proposing null) is always allowed regardless of policy
    Status proposedStatus = new Status();
    proposedStatus.setRemoved(false);
    // No lifecycleStage set (clearing)
    Status previousStatus = makeStatus(PROPOSED_STAGE);
    ChangeMCP item = makeChangeMCP(proposedStatus, previousStatus);

    List<Pair<ChangeMCP, Boolean>> result =
        buildHook().writeMutation(List.of(item), retrieverContext).toList();

    assertEquals(result.size(), 1);
    assertFalse(result.get(0).getSecond(), "Clearing lifecycle stage should never mutate");
    assertFalse(
        proposedStatus.hasLifecycleStage(), "Status should have no lifecycleStage after clear");
  }

  @Test
  public void testSameStageNoOp() {
    // Entity already in PROPOSED, proposing PROPOSED again → no-op, no mutation
    Status proposedStatus = makeStatus(PROPOSED_STAGE);
    Status previousStatus = makeStatus(PROPOSED_STAGE);
    ChangeMCP item = makeChangeMCP(proposedStatus, previousStatus);

    List<Pair<ChangeMCP, Boolean>> result =
        buildHook().writeMutation(List.of(item), retrieverContext).toList();

    assertEquals(result.size(), 1);
    assertFalse(result.get(0).getSecond(), "Re-applying same stage should not mutate");
    assertEquals(proposedStatus.getLifecycleStage(), PROPOSED_STAGE);
  }

  @Test
  public void testNonStatusAspect_passThrough() {
    // Non-status aspects are ignored by the hook
    ChangeMCP item = mock(ChangeMCP.class);
    when(item.getAspectName()).thenReturn("glossaryTermInfo");

    List<Pair<ChangeMCP, Boolean>> result =
        buildHook().writeMutation(List.of(item), retrieverContext).toList();

    assertEquals(result.size(), 1);
    assertFalse(result.get(0).getSecond(), "Non-status aspect should not be mutated");
  }

  @Test
  public void testRevertToNullWhenNoPreviousAspect() {
    // Invalid transition from null → blocked stage; revert clears the field
    LifecycleStageTransitionPolicy policy = new LifecycleStageTransitionPolicy();
    policy.setAllowedPreviousStages(
        new com.linkedin.common.UrnArray(List.of(PROPOSED_STAGE))); // only PROPOSED allowed
    when(mockAspectRetriever.getLatestAspectObject(
            eq(CERTIFIED_STAGE), eq("lifecycleStageTypeInfo")))
        .thenReturn(makeStageAspect(false, policy));

    // Proposing CERTIFIED from null/active (not in allowed list) → revert to null
    Status proposedStatus = makeStatus(CERTIFIED_STAGE);
    ChangeMCP item = makeChangeMCP(proposedStatus, null);

    List<Pair<ChangeMCP, Boolean>> result =
        buildHook().writeMutation(List.of(item), retrieverContext).toList();

    assertTrue(result.get(0).getSecond(), "Should be mutated (reverted)");
    assertFalse(proposedStatus.hasLifecycleStage(), "Should revert to null/active");
  }

  // ── Helpers ─────────────────────────────────────────────────────────────────

  private LifecycleStageTransitionHook buildHook() {
    LifecycleStageTransitionHook hook = new LifecycleStageTransitionHook();
    hook.setConfig(HOOK_CONFIG);
    return hook;
  }

  private static Status makeStatus(Urn lifecycleStageUrn) {
    Status status = new Status();
    status.setRemoved(false);
    if (lifecycleStageUrn != null) {
      status.setLifecycleStage(lifecycleStageUrn);
    }
    return status;
  }

  /** Creates a mock ChangeMCP for the Status aspect with the given proposed/previous values. */
  private static ChangeMCP makeChangeMCP(Status proposed, Status previous) {
    ChangeMCP item = mock(ChangeMCP.class);
    when(item.getAspectName()).thenReturn(STATUS_ASPECT_NAME);
    when(item.getUrn()).thenReturn(TEST_ENTITY_URN);
    when(item.getAspect(Status.class)).thenReturn(proposed);
    when(item.getPreviousAspect(Status.class)).thenReturn(previous);
    return item;
  }

  /** Serializes a LifecycleStageTypeInfo into the Aspect wrapper used by getLatestAspectObject. */
  private static Aspect makeStageAspect(
      boolean hideInSearch, LifecycleStageTransitionPolicy policy) {
    LifecycleStageSettings settings = new LifecycleStageSettings();
    settings.setHideInSearch(hideInSearch);

    AuditStamp stamp = new AuditStamp();
    stamp.setTime(System.currentTimeMillis());
    stamp.setActor(UrnUtils.getUrn("urn:li:corpuser:system"));

    LifecycleStageTypeInfo info = new LifecycleStageTypeInfo();
    info.setName("Test Stage");
    info.setSettings(settings);
    info.setCreated(stamp);
    info.setLastModified(stamp);
    if (policy != null) {
      info.setTransitionPolicy(policy);
    }
    return new Aspect(info.data());
  }
}
