package com.linkedin.metadata.kafka.hook.rule;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.assertion.rule.AssertionAssignmentRuleFilter;
import com.linkedin.assertion.rule.AssertionAssignmentRuleInfo;
import com.linkedin.assertion.rule.AssertionAssignmentRuleMode;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.key.AssertionAssignmentRuleKey;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.service.AssertionAssignmentRuleService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class AssertionAssignmentRuleHookTest {

  private static final Urn TEST_RULE_URN =
      UrnUtils.getUrn("urn:li:assertionAssignmentRule:test-rule");

  @Test
  public void testInvokeNotEnabled() throws Exception {
    AssertionAssignmentRuleService service = Mockito.mock(AssertionAssignmentRuleService.class);
    AssertionAssignmentRuleHook hook = new AssertionAssignmentRuleHook(service, false);
    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_RULE_URN,
            ASSERTION_ASSIGNMENT_RULE_INFO_ASPECT_NAME,
            ChangeType.UPSERT,
            buildRuleInfo());
    hook.invoke(event);
    Mockito.verifyNoInteractions(service);
  }

  @Test
  public void testInvokeNotEligibleChange() throws Exception {
    AssertionAssignmentRuleService service = Mockito.mock(AssertionAssignmentRuleService.class);
    AssertionAssignmentRuleHook hook = new AssertionAssignmentRuleHook(service, true);

    // Case 1: Key aspect with UPSERT (not a delete)
    MetadataChangeLog event1 =
        buildMetadataChangeLog(
            TEST_RULE_URN,
            ASSERTION_ASSIGNMENT_RULE_KEY_ASPECT_NAME,
            ChangeType.UPSERT,
            new AssertionAssignmentRuleKey().setId("test"));
    hook.invoke(event1);
    Mockito.verifyNoInteractions(service);

    // Case 2: Info aspect with DELETE (not a supported update type)
    MetadataChangeLog event2 =
        buildMetadataChangeLog(
            TEST_RULE_URN,
            ASSERTION_ASSIGNMENT_RULE_INFO_ASPECT_NAME,
            ChangeType.DELETE,
            buildRuleInfo());
    hook.invoke(event2);
    Mockito.verifyNoInteractions(service);
  }

  @Test
  public void testInvokeRuleInfoUpsert() throws Exception {
    AssertionAssignmentRuleService service = Mockito.mock(AssertionAssignmentRuleService.class);
    AssertionAssignmentRuleHook hook = new AssertionAssignmentRuleHook(service, true);
    AssertionAssignmentRuleInfo ruleInfo = buildRuleInfo();
    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_RULE_URN, ASSERTION_ASSIGNMENT_RULE_INFO_ASPECT_NAME, ChangeType.UPSERT, ruleInfo);
    hook.invoke(event);
    Mockito.verify(service, Mockito.times(1))
        .upsertAssertionAssignmentRuleAutomation(
            Mockito.nullable(OperationContext.class),
            Mockito.eq(TEST_RULE_URN),
            Mockito.any(AssertionAssignmentRuleInfo.class));
  }

  @Test
  public void testInvokeRuleInfoCreate() throws Exception {
    AssertionAssignmentRuleService service = Mockito.mock(AssertionAssignmentRuleService.class);
    AssertionAssignmentRuleHook hook = new AssertionAssignmentRuleHook(service, true);
    AssertionAssignmentRuleInfo ruleInfo = buildRuleInfo();
    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_RULE_URN, ASSERTION_ASSIGNMENT_RULE_INFO_ASPECT_NAME, ChangeType.CREATE, ruleInfo);
    hook.invoke(event);
    Mockito.verify(service, Mockito.times(1))
        .upsertAssertionAssignmentRuleAutomation(
            Mockito.nullable(OperationContext.class),
            Mockito.eq(TEST_RULE_URN),
            Mockito.any(AssertionAssignmentRuleInfo.class));
  }

  @Test
  public void testInvokeRuleInfoPatch() throws Exception {
    AssertionAssignmentRuleService service = Mockito.mock(AssertionAssignmentRuleService.class);
    AssertionAssignmentRuleHook hook = new AssertionAssignmentRuleHook(service, true);
    AssertionAssignmentRuleInfo ruleInfo = buildRuleInfo();
    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_RULE_URN, ASSERTION_ASSIGNMENT_RULE_INFO_ASPECT_NAME, ChangeType.PATCH, ruleInfo);
    hook.invoke(event);
    Mockito.verify(service, Mockito.times(1))
        .upsertAssertionAssignmentRuleAutomation(
            Mockito.nullable(OperationContext.class),
            Mockito.eq(TEST_RULE_URN),
            Mockito.any(AssertionAssignmentRuleInfo.class));
  }

  @Test
  public void testInvokeRuleDeleted() throws Exception {
    AssertionAssignmentRuleService service = Mockito.mock(AssertionAssignmentRuleService.class);
    AssertionAssignmentRuleHook hook = new AssertionAssignmentRuleHook(service, true);
    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_RULE_URN,
            ASSERTION_ASSIGNMENT_RULE_KEY_ASPECT_NAME,
            ChangeType.DELETE,
            new AssertionAssignmentRuleKey().setId("test"));
    hook.invoke(event);
    Mockito.verify(service, Mockito.times(1))
        .removeAssertionAssignmentRuleAutomation(
            Mockito.nullable(OperationContext.class), Mockito.eq(TEST_RULE_URN));
  }

  private static AssertionAssignmentRuleInfo buildRuleInfo() {
    return new AssertionAssignmentRuleInfo()
        .setName("Test Rule")
        .setMode(AssertionAssignmentRuleMode.ENABLED)
        .setEntityFilter(new AssertionAssignmentRuleFilter().setFilter(new Filter()));
  }

  private static MetadataChangeLog buildMetadataChangeLog(
      Urn urn, String aspectName, ChangeType changeType, RecordTemplate aspect) {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityUrn(urn);
    event.setEntityType(ASSERTION_ASSIGNMENT_RULE_ENTITY_NAME);
    event.setAspectName(aspectName);
    event.setChangeType(changeType);
    event.setAspect(GenericRecordUtils.serializeAspect(aspect));
    return event;
  }
}
