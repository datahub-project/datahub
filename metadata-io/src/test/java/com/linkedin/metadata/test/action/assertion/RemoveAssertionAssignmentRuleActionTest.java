package com.linkedin.metadata.test.action.assertion;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.AssertionAssignmentRuleService;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class RemoveAssertionAssignmentRuleActionTest {

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
    RemoveAssertionAssignmentRuleAction action =
        new RemoveAssertionAssignmentRuleAction(mock(AssertionAssignmentRuleService.class));
    assertThrows(
        InvalidActionParamsException.class,
        () -> action.validate(new ActionParameters(new HashMap<>())));
  }

  @Test
  public void testValidate_validParams_noException() {
    RemoveAssertionAssignmentRuleAction action =
        new RemoveAssertionAssignmentRuleAction(mock(AssertionAssignmentRuleService.class));
    action.validate(buildParams());
  }

  @Test
  public void testApply_delegatesToService() throws Exception {
    AssertionAssignmentRuleService ruleService = mock(AssertionAssignmentRuleService.class);

    RemoveAssertionAssignmentRuleAction action =
        new RemoveAssertionAssignmentRuleAction(ruleService);
    action.apply(mock(OperationContext.class), List.of(ENTITY_URN_1, ENTITY_URN_2), buildParams());

    verify(ruleService)
        .removeManagedAssertionsForEntities(
            any(), eq(RULE_URN), eq(List.of(ENTITY_URN_1, ENTITY_URN_2)));
  }

  @Test
  public void testApply_retriesOnTransientFailure() throws Exception {
    AssertionAssignmentRuleService ruleService = mock(AssertionAssignmentRuleService.class);
    doThrow(new RuntimeException("Transient"))
        .doNothing()
        .when(ruleService)
        .removeManagedAssertionsForEntities(any(), any(), any());

    RemoveAssertionAssignmentRuleAction action =
        new RemoveAssertionAssignmentRuleAction(ruleService);
    action.apply(mock(OperationContext.class), List.of(ENTITY_URN_1), buildParams());

    verify(ruleService, times(2)).removeManagedAssertionsForEntities(any(), any(), any());
  }

  @Test
  public void testApply_exhaustsRetriesWithoutThrowing() throws Exception {
    AssertionAssignmentRuleService ruleService = mock(AssertionAssignmentRuleService.class);
    // All attempts fail persistently
    doThrow(new RuntimeException("Persistent"))
        .when(ruleService)
        .removeManagedAssertionsForEntities(any(), any(), any());

    RemoveAssertionAssignmentRuleAction action =
        new RemoveAssertionAssignmentRuleAction(ruleService);

    // Should not throw — exhausts retries and swallows the error
    assertDoesNotThrow(
        () -> action.apply(mock(OperationContext.class), List.of(ENTITY_URN_1), buildParams()));

    // Retried MAX_RETRIES times
    verify(ruleService, times(RemoveAssertionAssignmentRuleAction.MAX_RETRIES))
        .removeManagedAssertionsForEntities(any(), eq(RULE_URN), any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testApply_batchesBySize() throws Exception {
    AssertionAssignmentRuleService ruleService = mock(AssertionAssignmentRuleService.class);

    // Generate BATCH_SIZE + 10 URNs to trigger 2 batches
    int totalUrns = RemoveAssertionAssignmentRuleAction.BATCH_SIZE + 10;
    List<Urn> urns =
        IntStream.range(0, totalUrns)
            .mapToObj(
                i ->
                    UrnUtils.getUrn(
                        String.format(
                            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table%d,PROD)", i)))
            .collect(Collectors.toList());

    RemoveAssertionAssignmentRuleAction action =
        new RemoveAssertionAssignmentRuleAction(ruleService);
    action.apply(mock(OperationContext.class), urns, buildParams());

    ArgumentCaptor<List<Urn>> batchCaptor = ArgumentCaptor.forClass(List.class);
    verify(ruleService, times(2))
        .removeManagedAssertionsForEntities(any(), eq(RULE_URN), batchCaptor.capture());

    List<List<Urn>> batches = batchCaptor.getAllValues();
    assertEquals(batches.size(), 2);
    assertEquals(batches.get(0).size(), RemoveAssertionAssignmentRuleAction.BATCH_SIZE);
    assertEquals(batches.get(1).size(), 10);
  }
}
