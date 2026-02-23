package com.linkedin.metadata.test.action.assertion;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.assertion.rule.AssertionAssignmentRuleInfo;
import com.linkedin.assertion.rule.AssertionAssignmentRuleMode;
import com.linkedin.assertion.rule.FreshnessAssertionAssignmentRuleConfig;
import com.linkedin.assertion.rule.SubscriptionAssignmentRuleConfig;
import com.linkedin.assertion.rule.VolumeAssertionAssignmentRuleConfig;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.AssertionAssignmentRuleService;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import com.linkedin.subscription.EntityChangeType;
import com.linkedin.subscription.EntityChangeTypeArray;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.List;
import org.testng.annotations.Test;

public class UpsertAssertionAssignmentRuleActionTest {

  private static final int MAX_MONITORS = 1000;
  private static final Urn RULE_URN = UrnUtils.getUrn("urn:li:assertionAssignmentRule:test-rule");
  private static final Urn ENTITY_URN_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table1,PROD)");
  private static final Urn ENTITY_URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table2,PROD)");

  private ActionParameters buildParams() {
    return new ActionParameters(ImmutableMap.of("ruleUrn", ImmutableList.of(RULE_URN.toString())));
  }

  @Test
  public void testValidate_missingRuleUrn_throws() {
    UpsertAssertionAssignmentRuleAction action =
        new UpsertAssertionAssignmentRuleAction(
            mock(AssertionAssignmentRuleService.class), MAX_MONITORS);
    assertThrows(
        InvalidActionParamsException.class,
        () -> action.validate(new ActionParameters(new HashMap<>())));
  }

  @Test
  public void testValidate_validParams_noException() {
    UpsertAssertionAssignmentRuleAction action =
        new UpsertAssertionAssignmentRuleAction(
            mock(AssertionAssignmentRuleService.class), MAX_MONITORS);
    action.validate(buildParams());
  }

  @Test
  public void testApply_ruleNotFound_noServiceCalls() throws Exception {
    AssertionAssignmentRuleService ruleService = mock(AssertionAssignmentRuleService.class);
    when(ruleService.getRuleInfo(any(), eq(RULE_URN))).thenReturn(null);

    UpsertAssertionAssignmentRuleAction action =
        new UpsertAssertionAssignmentRuleAction(ruleService, MAX_MONITORS);
    action.apply(mock(OperationContext.class), List.of(ENTITY_URN_1), buildParams());

    verify(ruleService, never()).upsertManagedFreshnessAssertion(any(), any(), any(), any());
    verify(ruleService, never()).upsertManagedVolumeAssertion(any(), any(), any(), any());
    verify(ruleService, never()).syncSubscriptionsForEntity(any(), any(), any(), any());
  }

  @Test
  public void testApply_ruleDisabled_noServiceCalls() throws Exception {
    AssertionAssignmentRuleService ruleService = mock(AssertionAssignmentRuleService.class);
    AssertionAssignmentRuleInfo ruleInfo = new AssertionAssignmentRuleInfo();
    ruleInfo.setMode(AssertionAssignmentRuleMode.DISABLED);
    when(ruleService.getRuleInfo(any(), eq(RULE_URN))).thenReturn(ruleInfo);

    UpsertAssertionAssignmentRuleAction action =
        new UpsertAssertionAssignmentRuleAction(ruleService, MAX_MONITORS);
    action.apply(mock(OperationContext.class), List.of(ENTITY_URN_1), buildParams());

    verify(ruleService, never()).upsertManagedFreshnessAssertion(any(), any(), any(), any());
  }

  @Test
  public void testApply_freshnessConfig_delegatesToService() throws Exception {
    AssertionAssignmentRuleService ruleService = mock(AssertionAssignmentRuleService.class);

    FreshnessAssertionAssignmentRuleConfig freshnessConfig =
        new FreshnessAssertionAssignmentRuleConfig().setEnabled(true);
    AssertionAssignmentRuleInfo ruleInfo =
        new AssertionAssignmentRuleInfo()
            .setMode(AssertionAssignmentRuleMode.ENABLED)
            .setFreshnessConfig(freshnessConfig);
    when(ruleService.getRuleInfo(any(), eq(RULE_URN))).thenReturn(ruleInfo);
    when(ruleService.countActiveMonitors(any())).thenReturn(0);
    when(ruleService.upsertManagedFreshnessAssertion(
            any(), eq(ENTITY_URN_1), eq(RULE_URN), eq(freshnessConfig)))
        .thenReturn(1);

    UpsertAssertionAssignmentRuleAction action =
        new UpsertAssertionAssignmentRuleAction(ruleService, MAX_MONITORS);
    action.apply(mock(OperationContext.class), List.of(ENTITY_URN_1), buildParams());

    verify(ruleService)
        .upsertManagedFreshnessAssertion(
            any(), eq(ENTITY_URN_1), eq(RULE_URN), eq(freshnessConfig));
  }

  @Test
  public void testApply_volumeConfig_delegatesToService() throws Exception {
    AssertionAssignmentRuleService ruleService = mock(AssertionAssignmentRuleService.class);

    VolumeAssertionAssignmentRuleConfig volumeConfig =
        new VolumeAssertionAssignmentRuleConfig().setEnabled(true);
    AssertionAssignmentRuleInfo ruleInfo =
        new AssertionAssignmentRuleInfo()
            .setMode(AssertionAssignmentRuleMode.ENABLED)
            .setVolumeConfig(volumeConfig);
    when(ruleService.getRuleInfo(any(), eq(RULE_URN))).thenReturn(ruleInfo);
    when(ruleService.countActiveMonitors(any())).thenReturn(0);
    when(ruleService.upsertManagedVolumeAssertion(
            any(), eq(ENTITY_URN_1), eq(RULE_URN), eq(volumeConfig)))
        .thenReturn(1);

    UpsertAssertionAssignmentRuleAction action =
        new UpsertAssertionAssignmentRuleAction(ruleService, MAX_MONITORS);
    action.apply(mock(OperationContext.class), List.of(ENTITY_URN_1), buildParams());

    verify(ruleService)
        .upsertManagedVolumeAssertion(any(), eq(ENTITY_URN_1), eq(RULE_URN), eq(volumeConfig));
  }

  @Test
  public void testApply_subscriptionConfig_delegatesToService() throws Exception {
    AssertionAssignmentRuleService ruleService = mock(AssertionAssignmentRuleService.class);

    Urn subscriberUrn = UrnUtils.getUrn("urn:li:corpuser:testUser");
    SubscriptionAssignmentRuleConfig subConfig = new SubscriptionAssignmentRuleConfig();
    subConfig.setSubscribers(new com.linkedin.common.UrnArray(subscriberUrn));
    subConfig.setEntityChangeTypes(new EntityChangeTypeArray(EntityChangeType.ASSERTION_FAILED));
    AssertionAssignmentRuleInfo ruleInfo =
        new AssertionAssignmentRuleInfo()
            .setMode(AssertionAssignmentRuleMode.ENABLED)
            .setSubscriptionConfig(subConfig);
    when(ruleService.getRuleInfo(any(), eq(RULE_URN))).thenReturn(ruleInfo);
    when(ruleService.countActiveMonitors(any())).thenReturn(0);

    UpsertAssertionAssignmentRuleAction action =
        new UpsertAssertionAssignmentRuleAction(ruleService, MAX_MONITORS);
    action.apply(mock(OperationContext.class), List.of(ENTITY_URN_1), buildParams());

    verify(ruleService)
        .syncSubscriptionsForEntity(any(), eq(ENTITY_URN_1), eq(RULE_URN), eq(subConfig));
  }

  @Test
  public void testApply_monitorLimitReached_skipsRemainingEntities() throws Exception {
    AssertionAssignmentRuleService ruleService = mock(AssertionAssignmentRuleService.class);

    FreshnessAssertionAssignmentRuleConfig freshnessConfig =
        new FreshnessAssertionAssignmentRuleConfig().setEnabled(true);
    AssertionAssignmentRuleInfo ruleInfo =
        new AssertionAssignmentRuleInfo()
            .setMode(AssertionAssignmentRuleMode.ENABLED)
            .setFreshnessConfig(freshnessConfig);
    when(ruleService.getRuleInfo(any(), eq(RULE_URN))).thenReturn(ruleInfo);
    // Already at max monitors
    when(ruleService.countActiveMonitors(any())).thenReturn(MAX_MONITORS);

    UpsertAssertionAssignmentRuleAction action =
        new UpsertAssertionAssignmentRuleAction(ruleService, MAX_MONITORS);
    action.apply(mock(OperationContext.class), List.of(ENTITY_URN_1, ENTITY_URN_2), buildParams());

    // No entities should be processed
    verify(ruleService, never()).upsertManagedFreshnessAssertion(any(), any(), any(), any());
  }

  @Test
  public void testApply_perEntityErrorIsolation() throws Exception {
    AssertionAssignmentRuleService ruleService = mock(AssertionAssignmentRuleService.class);

    FreshnessAssertionAssignmentRuleConfig freshnessConfig =
        new FreshnessAssertionAssignmentRuleConfig().setEnabled(true);
    AssertionAssignmentRuleInfo ruleInfo =
        new AssertionAssignmentRuleInfo()
            .setMode(AssertionAssignmentRuleMode.ENABLED)
            .setFreshnessConfig(freshnessConfig);
    when(ruleService.getRuleInfo(any(), eq(RULE_URN))).thenReturn(ruleInfo);
    when(ruleService.countActiveMonitors(any())).thenReturn(0);

    // First entity fails
    when(ruleService.upsertManagedFreshnessAssertion(
            any(), eq(ENTITY_URN_1), eq(RULE_URN), eq(freshnessConfig)))
        .thenThrow(new RuntimeException("Search failed for entity 1"));
    // Second entity succeeds
    when(ruleService.upsertManagedFreshnessAssertion(
            any(), eq(ENTITY_URN_2), eq(RULE_URN), eq(freshnessConfig)))
        .thenReturn(1);

    UpsertAssertionAssignmentRuleAction action =
        new UpsertAssertionAssignmentRuleAction(ruleService, MAX_MONITORS);
    action.apply(mock(OperationContext.class), List.of(ENTITY_URN_1, ENTITY_URN_2), buildParams());

    // Second entity should still be processed
    verify(ruleService)
        .upsertManagedFreshnessAssertion(
            any(), eq(ENTITY_URN_2), eq(RULE_URN), eq(freshnessConfig));
  }

  @Test
  public void testApply_multipleEntities_bothProcessed() throws Exception {
    AssertionAssignmentRuleService ruleService = mock(AssertionAssignmentRuleService.class);

    FreshnessAssertionAssignmentRuleConfig freshnessConfig =
        new FreshnessAssertionAssignmentRuleConfig().setEnabled(true);
    AssertionAssignmentRuleInfo ruleInfo =
        new AssertionAssignmentRuleInfo()
            .setMode(AssertionAssignmentRuleMode.ENABLED)
            .setFreshnessConfig(freshnessConfig);
    when(ruleService.getRuleInfo(any(), eq(RULE_URN))).thenReturn(ruleInfo);
    when(ruleService.countActiveMonitors(any())).thenReturn(0);
    when(ruleService.upsertManagedFreshnessAssertion(
            any(), any(), eq(RULE_URN), eq(freshnessConfig)))
        .thenReturn(1);

    UpsertAssertionAssignmentRuleAction action =
        new UpsertAssertionAssignmentRuleAction(ruleService, MAX_MONITORS);
    action.apply(mock(OperationContext.class), List.of(ENTITY_URN_1, ENTITY_URN_2), buildParams());

    verify(ruleService)
        .upsertManagedFreshnessAssertion(
            any(), eq(ENTITY_URN_1), eq(RULE_URN), eq(freshnessConfig));
    verify(ruleService)
        .upsertManagedFreshnessAssertion(
            any(), eq(ENTITY_URN_2), eq(RULE_URN), eq(freshnessConfig));
  }
}
