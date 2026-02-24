package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.assertion.AssertionActions;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.assertion.rule.AssertionAssignmentRuleFilter;
import com.linkedin.assertion.rule.AssertionAssignmentRuleInfo;
import com.linkedin.assertion.rule.FreshnessAssertionAssignmentRuleConfig;
import com.linkedin.assertion.rule.SubscriptionAssignmentRuleConfig;
import com.linkedin.assertion.rule.VolumeAssertionAssignmentRuleConfig;
import com.linkedin.common.CronSchedule;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.util.AssertionAssignmentRuleUtils;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.subscription.EntityChangeDetails;
import com.linkedin.subscription.EntityChangeDetailsArray;
import com.linkedin.subscription.EntityChangeType;
import com.linkedin.subscription.EntityChangeTypeArray;
import com.linkedin.subscription.SubscriptionInfo;
import com.linkedin.subscription.SubscriptionManagedBy;
import com.linkedin.subscription.SubscriptionManagedByArray;
import com.linkedin.subscription.SubscriptionTypeArray;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AssertionAssignmentRuleServiceTest {

  private static final Urn RULE_URN = UrnUtils.getUrn("urn:li:assertionAssignmentRule:test123");
  private static final Urn ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table1,PROD)");
  private static final Urn ENTITY_URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table2,PROD)");
  private static final Urn ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:existing-assertion");
  private static final Urn SUBSCRIBER_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final Urn SUBSCRIPTION_URN = UrnUtils.getUrn("urn:li:subscription:test-sub");

  private SystemEntityClient mockEntityClient;
  private AssertionService mockAssertionService;
  private MonitorService mockMonitorService;
  private SubscriptionService mockSubscriptionService;
  private OperationContext opContext;
  private AssertionAssignmentRuleService service;
  private AssertionAssignmentRuleInfo ruleInfo;

  @BeforeMethod
  public void setup() {
    mockEntityClient = mock(SystemEntityClient.class);
    OpenApiClient mockOpenApiClient = mock(OpenApiClient.class);
    mockAssertionService = mock(AssertionService.class);
    mockMonitorService = mock(MonitorService.class);
    mockSubscriptionService = mock(SubscriptionService.class);

    AspectRetriever mockAspectRetriever = mock(AspectRetriever.class);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization(mockAspectRetriever);

    service =
        new AssertionAssignmentRuleService(
            mockEntityClient,
            mockOpenApiClient,
            new ObjectMapper(),
            mockAssertionService,
            mockMonitorService,
            mockSubscriptionService);

    Filter filter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                CriterionUtils.buildCriterion(
                                    "platform",
                                    Condition.EQUAL,
                                    "urn:li:dataPlatform:snowflake")))));

    ruleInfo =
        new AssertionAssignmentRuleInfo()
            .setName("test rule")
            .setEntityFilter(new AssertionAssignmentRuleFilter().setFilter(filter));
  }

  // ---------------------------------------------------------------------------
  // Automation CRUD
  // ---------------------------------------------------------------------------

  @Test
  public void testUpsertAutomation() throws Exception {
    service.upsertAssertionAssignmentRuleAutomation(opContext, RULE_URN, ruleInfo);

    ArgumentCaptor<List<MetadataChangeProposal>> captor = ArgumentCaptor.forClass(List.class);
    verify(mockEntityClient, times(1))
        .batchIngestProposals(any(OperationContext.class), captor.capture(), anyBoolean());

    List<MetadataChangeProposal> proposals = captor.getValue();
    assertEquals(proposals.size(), 1);
    assertEquals(proposals.get(0).getAspectName(), TEST_INFO_ASPECT_NAME);
  }

  @Test
  public void testGetRuleInfo() throws Exception {
    EntityResponse response = new EntityResponse();
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        ASSERTION_ASSIGNMENT_RULE_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(ruleInfo.data())));
    response.setAspects(aspectMap);

    when(mockEntityClient.getV2(
            any(OperationContext.class),
            eq(ASSERTION_ASSIGNMENT_RULE_ENTITY_NAME),
            eq(RULE_URN),
            anySet()))
        .thenReturn(response);

    AssertionAssignmentRuleInfo result = service.getRuleInfo(opContext, RULE_URN);

    assertNotNull(result);
    assertEquals(result.getName(), "test rule");
  }

  @Test
  public void testGetRuleInfoNotFound() throws Exception {
    when(mockEntityClient.getV2(
            any(OperationContext.class),
            eq(ASSERTION_ASSIGNMENT_RULE_ENTITY_NAME),
            eq(RULE_URN),
            anySet()))
        .thenReturn(null);

    AssertionAssignmentRuleInfo result = service.getRuleInfo(opContext, RULE_URN);

    assertNull(result);
  }

  // ---------------------------------------------------------------------------
  // Managed assertion upsert
  // ---------------------------------------------------------------------------

  @Test
  public void testUpsertManagedFreshnessAssertion_new_createsAssertionAndMonitor()
      throws Exception {
    Urn expectedUrn =
        AssertionAssignmentRuleUtils.createAssertionUrnForRule(
            RULE_URN, ENTITY_URN, AssertionType.FRESHNESS);
    when(mockEntityClient.exists(any(OperationContext.class), eq(expectedUrn))).thenReturn(false);
    when(mockAssertionService.upsertDatasetFreshnessAssertion(
            any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(expectedUrn);
    when(mockMonitorService.createAssertionMonitor(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(UrnUtils.getUrn("urn:li:monitor:m1"));

    FreshnessAssertionAssignmentRuleConfig config =
        new FreshnessAssertionAssignmentRuleConfig().setEnabled(true);

    int newMonitors =
        service.upsertManagedFreshnessAssertion(opContext, ENTITY_URN, RULE_URN, config);

    assertEquals(newMonitors, 1);
    verify(mockAssertionService)
        .upsertDatasetFreshnessAssertion(
            any(),
            eq(expectedUrn),
            eq(ENTITY_URN),
            any(),
            any(),
            any(),
            any(),
            any(AssertionSource.class),
            eq(METADATA_TESTS_SOURCE));
    verify(mockMonitorService)
        .createAssertionMonitor(
            any(),
            eq(ENTITY_URN),
            eq(expectedUrn),
            any(CronSchedule.class),
            any(AssertionEvaluationParameters.class),
            any(),
            eq(METADATA_TESTS_SOURCE));
    verify(mockAssertionService, never()).generateAssertionUrn();
  }

  @Test
  public void testUpsertManagedFreshnessAssertion_existing_updatesWithoutNewMonitor()
      throws Exception {
    Urn expectedUrn =
        AssertionAssignmentRuleUtils.createAssertionUrnForRule(
            RULE_URN, ENTITY_URN, AssertionType.FRESHNESS);
    when(mockEntityClient.exists(any(OperationContext.class), eq(expectedUrn))).thenReturn(true);
    when(mockAssertionService.upsertDatasetFreshnessAssertion(
            any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(expectedUrn);

    FreshnessAssertionAssignmentRuleConfig config =
        new FreshnessAssertionAssignmentRuleConfig().setEnabled(true);

    int newMonitors =
        service.upsertManagedFreshnessAssertion(opContext, ENTITY_URN, RULE_URN, config);

    assertEquals(newMonitors, 0);
    verify(mockAssertionService)
        .upsertDatasetFreshnessAssertion(
            any(), eq(expectedUrn), eq(ENTITY_URN), any(), any(), any(), any(), any(), any());
    verifyNoInteractions(mockMonitorService);
  }

  @Test
  public void testUpsertManagedVolumeAssertion_new_createsAssertionAndMonitor() throws Exception {
    Urn expectedUrn =
        AssertionAssignmentRuleUtils.createAssertionUrnForRule(
            RULE_URN, ENTITY_URN, AssertionType.VOLUME);
    when(mockEntityClient.exists(any(OperationContext.class), eq(expectedUrn))).thenReturn(false);
    when(mockAssertionService.upsertDatasetVolumeAssertion(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(expectedUrn);
    when(mockMonitorService.createAssertionMonitor(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(UrnUtils.getUrn("urn:li:monitor:m1"));

    VolumeAssertionAssignmentRuleConfig config =
        new VolumeAssertionAssignmentRuleConfig().setEnabled(true);

    int newMonitors = service.upsertManagedVolumeAssertion(opContext, ENTITY_URN, RULE_URN, config);

    assertEquals(newMonitors, 1);
    verify(mockAssertionService)
        .upsertDatasetVolumeAssertion(
            any(),
            eq(expectedUrn),
            eq(ENTITY_URN),
            any(),
            any(VolumeAssertionInfo.class),
            any(),
            any(AssertionSource.class),
            eq(METADATA_TESTS_SOURCE));
    verify(mockMonitorService)
        .createAssertionMonitor(
            any(),
            eq(ENTITY_URN),
            eq(expectedUrn),
            any(CronSchedule.class),
            any(AssertionEvaluationParameters.class),
            any(),
            eq(METADATA_TESTS_SOURCE));
    verify(mockAssertionService, never()).generateAssertionUrn();
  }

  @Test
  public void testUpsertManagedAssertion_monitorLimitExceeded_cleansUpOrphan() throws Exception {
    Urn expectedUrn =
        AssertionAssignmentRuleUtils.createAssertionUrnForRule(
            RULE_URN, ENTITY_URN, AssertionType.FRESHNESS);
    when(mockEntityClient.exists(any(OperationContext.class), eq(expectedUrn))).thenReturn(false);
    when(mockAssertionService.upsertDatasetFreshnessAssertion(
            any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(expectedUrn);
    when(mockMonitorService.createAssertionMonitor(any(), any(), any(), any(), any(), any(), any()))
        .thenThrow(
            new RuntimeException(
                AcrylConstants.MONITOR_LIMIT_EXCEEDED_ERROR_MESSAGE_PREFIX
                    + " Max 1000 monitors."));

    FreshnessAssertionAssignmentRuleConfig config =
        new FreshnessAssertionAssignmentRuleConfig().setEnabled(true);

    assertThrows(
        RuntimeException.class,
        () -> service.upsertManagedFreshnessAssertion(opContext, ENTITY_URN, RULE_URN, config));

    verify(mockAssertionService).tryDeleteAssertionReferences(any(), eq(expectedUrn));
    verify(mockAssertionService).tryDeleteAssertion(any(), eq(expectedUrn));
  }

  // ---------------------------------------------------------------------------
  // Managed assertion removal (batch)
  // ---------------------------------------------------------------------------

  @Test
  public void testRemoveManagedAssertionsForEntities_deletesDeterministicUrns() throws Exception {
    Urn freshnessUrn =
        AssertionAssignmentRuleUtils.createAssertionUrnForRule(
            RULE_URN, ENTITY_URN, AssertionType.FRESHNESS);
    Urn volumeUrn =
        AssertionAssignmentRuleUtils.createAssertionUrnForRule(
            RULE_URN, ENTITY_URN, AssertionType.VOLUME);

    // Phase 1: both deterministic URNs exist
    when(mockEntityClient.exists(any(OperationContext.class), eq(freshnessUrn))).thenReturn(true);
    when(mockEntityClient.exists(any(OperationContext.class), eq(volumeUrn))).thenReturn(true);

    // Phase 2: search sweep returns no orphans
    stubEmptySearchSweep();

    service.removeManagedAssertionsForEntities(opContext, RULE_URN, List.of(ENTITY_URN));

    verify(mockAssertionService).tryDeleteAssertionReferences(any(), eq(freshnessUrn));
    verify(mockAssertionService).tryDeleteAssertion(any(), eq(freshnessUrn));
    verify(mockAssertionService).tryDeleteAssertionReferences(any(), eq(volumeUrn));
    verify(mockAssertionService).tryDeleteAssertion(any(), eq(volumeUrn));
  }

  @Test
  public void testRemoveManagedAssertionsForEntities_cleansUpOrphans() throws Exception {
    Urn freshnessUrn =
        AssertionAssignmentRuleUtils.createAssertionUrnForRule(
            RULE_URN, ENTITY_URN, AssertionType.FRESHNESS);
    Urn volumeUrn =
        AssertionAssignmentRuleUtils.createAssertionUrnForRule(
            RULE_URN, ENTITY_URN, AssertionType.VOLUME);
    Urn orphanedUrn = UrnUtils.getUrn("urn:li:assertion:legacy-orphan");

    // Phase 1: only freshness exists
    when(mockEntityClient.exists(any(OperationContext.class), eq(freshnessUrn))).thenReturn(true);
    when(mockEntityClient.exists(any(OperationContext.class), eq(volumeUrn))).thenReturn(false);

    // Phase 2: search sweep finds the orphan and the already-deleted freshness URN
    SearchResult searchResult = new SearchResult();
    searchResult.setNumEntities(2);
    searchResult.setEntities(
        new SearchEntityArray(
            new SearchEntity().setEntity(freshnessUrn), new SearchEntity().setEntity(orphanedUrn)));
    when(mockEntityClient.filter(
            any(), eq(ASSERTION_ENTITY_NAME), any(), any(), anyInt(), anyInt()))
        .thenReturn(searchResult);

    service.removeManagedAssertionsForEntities(opContext, RULE_URN, List.of(ENTITY_URN));

    // freshnessUrn deleted in phase 1 only (not again in phase 2)
    verify(mockAssertionService, times(1)).tryDeleteAssertionReferences(any(), eq(freshnessUrn));
    verify(mockAssertionService, times(1)).tryDeleteAssertion(any(), eq(freshnessUrn));
    // orphan cleaned up in phase 2
    verify(mockAssertionService).tryDeleteAssertionReferences(any(), eq(orphanedUrn));
    verify(mockAssertionService).tryDeleteAssertion(any(), eq(orphanedUrn));
  }

  @Test
  public void testRemoveManagedAssertionsForEntities_noAssertions_noOp() throws Exception {
    Urn freshnessUrn =
        AssertionAssignmentRuleUtils.createAssertionUrnForRule(
            RULE_URN, ENTITY_URN, AssertionType.FRESHNESS);
    Urn volumeUrn =
        AssertionAssignmentRuleUtils.createAssertionUrnForRule(
            RULE_URN, ENTITY_URN, AssertionType.VOLUME);

    // Phase 1: neither exists
    when(mockEntityClient.exists(any(OperationContext.class), eq(freshnessUrn))).thenReturn(false);
    when(mockEntityClient.exists(any(OperationContext.class), eq(volumeUrn))).thenReturn(false);

    // Phase 2: search sweep returns nothing
    stubEmptySearchSweep();

    service.removeManagedAssertionsForEntities(opContext, RULE_URN, List.of(ENTITY_URN));

    verify(mockAssertionService, never()).tryDeleteAssertion(any(), any());
  }

  @Test
  public void testRemoveManagedAssertionsForEntities_multipleEntities() throws Exception {
    // Stub all deterministic URNs as existing
    for (Urn entityUrn : List.of(ENTITY_URN, ENTITY_URN_2)) {
      for (AssertionType type : List.of(AssertionType.FRESHNESS, AssertionType.VOLUME)) {
        Urn assertionUrn =
            AssertionAssignmentRuleUtils.createAssertionUrnForRule(RULE_URN, entityUrn, type);
        when(mockEntityClient.exists(any(OperationContext.class), eq(assertionUrn)))
            .thenReturn(true);
      }
    }

    // Phase 2: search sweep returns no orphans
    stubEmptySearchSweep();

    service.removeManagedAssertionsForEntities(
        opContext, RULE_URN, List.of(ENTITY_URN, ENTITY_URN_2));

    // 2 entities × 2 types = 4 exists checks
    verify(mockEntityClient, times(4)).exists(any(OperationContext.class), any(Urn.class));
    // 4 deletions (2 entities × 2 types)
    verify(mockAssertionService, times(4)).tryDeleteAssertionReferences(any(), any());
    verify(mockAssertionService, times(4)).tryDeleteAssertion(any(), any());
    // Single batch search call with IN condition
    verify(mockEntityClient, times(1))
        .filter(any(), eq(ASSERTION_ENTITY_NAME), any(), any(), anyInt(), anyInt());
  }

  // ---------------------------------------------------------------------------
  // Batch search (findAllManagedAssertionsForEntities)
  // ---------------------------------------------------------------------------

  @Test
  public void testFindAllManagedAssertionsForEntities_batchSearch() throws Exception {
    Urn assertion1 = UrnUtils.getUrn("urn:li:assertion:a1");
    Urn assertion2 = UrnUtils.getUrn("urn:li:assertion:a2");

    SearchResult searchResult = new SearchResult();
    searchResult.setNumEntities(2);
    searchResult.setEntities(
        new SearchEntityArray(
            new SearchEntity().setEntity(assertion1), new SearchEntity().setEntity(assertion2)));

    ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
    when(mockEntityClient.filter(
            any(), eq(ASSERTION_ENTITY_NAME), filterCaptor.capture(), any(), anyInt(), anyInt()))
        .thenReturn(searchResult);

    List<Urn> result =
        service.findAllManagedAssertionsForEntities(
            opContext, RULE_URN, List.of(ENTITY_URN, ENTITY_URN_2));

    assertEquals(result.size(), 2);
    assertTrue(result.contains(assertion1));
    assertTrue(result.contains(assertion2));

    // Verify filter uses Condition.IN for entity field
    Filter capturedFilter = filterCaptor.getValue();
    CriterionArray criteria = capturedFilter.getOr().get(0).getAnd();
    assertEquals(criteria.size(), 2);
    // Find the entity criterion (the one with IN condition)
    boolean foundInCondition = false;
    for (var criterion : criteria) {
      if ("entity".equals(criterion.getField())) {
        assertEquals(criterion.getCondition(), Condition.IN);
        foundInCondition = true;
      }
    }
    assertTrue(foundInCondition, "Expected entity criterion with Condition.IN");
  }

  // ---------------------------------------------------------------------------
  // Subscription sync
  // ---------------------------------------------------------------------------

  @Test
  public void testSyncSubscriptionsForEntity_createsNewSubscription() {
    SubscriptionAssignmentRuleConfig subConfig = new SubscriptionAssignmentRuleConfig();
    subConfig.setSubscribers(new UrnArray(SUBSCRIBER_URN));
    subConfig.setEntityChangeTypes(new EntityChangeTypeArray(EntityChangeType.ASSERTION_FAILED));

    when(mockSubscriptionService.getSubscription(any(), eq(ENTITY_URN), eq(SUBSCRIBER_URN)))
        .thenReturn(null);

    service.syncSubscriptionsForEntity(opContext, ENTITY_URN, RULE_URN, subConfig);

    verify(mockSubscriptionService)
        .createSubscription(
            any(),
            eq(SUBSCRIBER_URN),
            eq(ENTITY_URN),
            any(SubscriptionTypeArray.class),
            any(EntityChangeDetailsArray.class),
            isNull(),
            any(SubscriptionManagedByArray.class));
    verify(mockSubscriptionService, never()).updateSubscriptionInfo(any(), any(), any());
  }

  @Test
  public void testSyncSubscriptionsForEntity_mergesExistingSubscription() {
    SubscriptionAssignmentRuleConfig subConfig = new SubscriptionAssignmentRuleConfig();
    subConfig.setSubscribers(new UrnArray(SUBSCRIBER_URN));
    subConfig.setEntityChangeTypes(new EntityChangeTypeArray(EntityChangeType.ASSERTION_FAILED));

    SubscriptionInfo existingInfo =
        new SubscriptionInfo()
            .setActorUrn(SUBSCRIBER_URN)
            .setEntityUrn(ENTITY_URN)
            .setEntityChangeTypes(
                new EntityChangeDetailsArray(
                    new EntityChangeDetails()
                        .setEntityChangeType(EntityChangeType.INCIDENT_RAISED)));
    when(mockSubscriptionService.getSubscription(any(), eq(ENTITY_URN), eq(SUBSCRIBER_URN)))
        .thenReturn(Map.entry(SUBSCRIPTION_URN, existingInfo));

    service.syncSubscriptionsForEntity(opContext, ENTITY_URN, RULE_URN, subConfig);

    verify(mockSubscriptionService, never())
        .createSubscription(any(), any(), any(), any(), any(), any(), any());
    verify(mockSubscriptionService)
        .updateSubscriptionInfo(
            any(), eq(SUBSCRIPTION_URN), argThat(info -> info.getEntityChangeTypes().size() == 2));
  }

  // ---------------------------------------------------------------------------
  // Helper method unit tests
  // ---------------------------------------------------------------------------

  @Test
  public void testMergeEntityChangeDetails_preservesExistingAndAddsNew() {
    EntityChangeDetailsArray existing =
        new EntityChangeDetailsArray(
            new EntityChangeDetails().setEntityChangeType(EntityChangeType.INCIDENT_RAISED),
            new EntityChangeDetails().setEntityChangeType(EntityChangeType.ASSERTION_FAILED));
    EntityChangeDetailsArray incoming =
        new EntityChangeDetailsArray(
            new EntityChangeDetails().setEntityChangeType(EntityChangeType.ASSERTION_FAILED),
            new EntityChangeDetails().setEntityChangeType(EntityChangeType.TAG_ADDED));

    EntityChangeDetailsArray merged =
        AssertionAssignmentRuleService.mergeEntityChangeDetails(existing, incoming);

    assertEquals(3, merged.size());
  }

  @Test
  public void testBuildAssertionActions_withNulls() {
    AssertionActions actions = AssertionAssignmentRuleService.buildAssertionActions(null, null);
    assertNotNull(actions.getOnSuccess());
    assertNotNull(actions.getOnFailure());
    assertEquals(0, actions.getOnSuccess().size());
    assertEquals(0, actions.getOnFailure().size());
  }

  @Test
  public void testUpdateManagedBy_addsNewEntry() {
    SubscriptionInfo info = new SubscriptionInfo();
    EntityChangeTypeArray types = new EntityChangeTypeArray(EntityChangeType.ASSERTION_FAILED);
    AssertionAssignmentRuleService.updateManagedBy(info, RULE_URN, types);

    assertTrue(info.hasManagedBy());
    assertEquals(1, info.getManagedBy().size());
    assertEquals(RULE_URN, info.getManagedBy().get(0).getSourceEntity());
  }

  @Test
  public void testUpdateManagedBy_updatesExistingEntry() {
    SubscriptionInfo info = new SubscriptionInfo();
    SubscriptionManagedByArray existing = new SubscriptionManagedByArray();
    existing.add(
        new SubscriptionManagedBy()
            .setSourceEntity(RULE_URN)
            .setLastAppliedChangeTypes(
                new EntityChangeTypeArray(EntityChangeType.INCIDENT_RAISED)));
    info.setManagedBy(existing);

    EntityChangeTypeArray newTypes = new EntityChangeTypeArray(EntityChangeType.ASSERTION_FAILED);
    AssertionAssignmentRuleService.updateManagedBy(info, RULE_URN, newTypes);

    assertEquals(1, info.getManagedBy().size());
    assertEquals(1, info.getManagedBy().get(0).getLastAppliedChangeTypes().size());
    assertEquals(
        EntityChangeType.ASSERTION_FAILED,
        info.getManagedBy().get(0).getLastAppliedChangeTypes().get(0));
  }

  // ---------------------------------------------------------------------------
  // Test helpers
  // ---------------------------------------------------------------------------

  private void stubEmptySearchSweep() throws Exception {
    SearchResult emptyResult = new SearchResult();
    emptyResult.setNumEntities(0);
    emptyResult.setEntities(new SearchEntityArray());
    when(mockEntityClient.filter(
            any(), eq(ASSERTION_ENTITY_NAME), any(), any(), anyInt(), anyInt()))
        .thenReturn(emptyResult);
  }
}
