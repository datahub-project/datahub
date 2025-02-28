package com.linkedin.metadata.kafka.hook.form;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.form.DynamicFormAssignment;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormPrompt;
import com.linkedin.form.FormPromptArray;
import com.linkedin.form.FormPromptType;
import com.linkedin.form.FormState;
import com.linkedin.form.FormStatus;
import com.linkedin.form.FormType;
import com.linkedin.form.StructuredPropertyParams;
import com.linkedin.metadata.key.FormKey;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nullable;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class FormAssignmentHookTest {

  private static final Urn TEST_FORM_URN = UrnUtils.getUrn("urn:li:form:test");
  private static final String TEST_FORM_PROMPT_ID_1 = "test-id";
  private static final String TEST_FORM_PROMPT_ID_2 = "test-id-2";
  private static final Urn TEST_PROPERTY_URN = UrnUtils.getUrn("urn:li:structuredProperty:test.id");
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void testInvokeNotEnabled() throws Exception {
    FormService service = mockFormService();
    FormAssignmentHook hook = new FormAssignmentHook(service, false);
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_FORM_URN,
            FORM_INFO_ASPECT_NAME,
            ChangeType.UPSERT,
            mockFormInfo(
                ImmutableList.of(
                    new FormPrompt()
                        .setId(TEST_FORM_PROMPT_ID_1)
                        .setType(FormPromptType.STRUCTURED_PROPERTY)
                        .setRequired(true)
                        .setStructuredPropertyParams(
                            new StructuredPropertyParams().setUrn(TEST_PROPERTY_URN)))));
    hook.invoke(event);
    Mockito.verifyNoInteractions(service);
  }

  @Test
  public void testInvokeNotEligibleChange() throws Exception {
    FormService service = mockFormService();
    FormAssignmentHook hook = new FormAssignmentHook(service, true);

    // Case 1: Bad aspect
    final MetadataChangeLog event1 =
        buildMetadataChangeLog(
            TEST_FORM_URN, FORM_KEY_ASPECT_NAME, ChangeType.UPSERT, new FormKey().setId("test"));
    hook.invoke(event1);
    Mockito.verifyNoInteractions(service);

    // Case 2: Bad change type
    final MetadataChangeLog event2 =
        buildMetadataChangeLog(
            TEST_FORM_URN,
            FORM_INFO_ASPECT_NAME,
            ChangeType.DELETE,
            mockFormInfo(
                ImmutableList.of(
                    new FormPrompt()
                        .setId(TEST_FORM_PROMPT_ID_1)
                        .setType(FormPromptType.STRUCTURED_PROPERTY)
                        .setRequired(true)
                        .setStructuredPropertyParams(
                            new StructuredPropertyParams().setUrn(TEST_PROPERTY_URN)))));
    hook.invoke(event2);
    Mockito.verifyNoInteractions(service);
  }

  @Test
  public void testInvokeFormInfoNoPrevious() throws Exception {
    FormService service = mockFormService();
    FormAssignmentHook hook = new FormAssignmentHook(service, true);
    FormPrompt testPrompt1 =
        new FormPrompt()
            .setId(TEST_FORM_PROMPT_ID_1)
            .setTitle("Test")
            .setType(FormPromptType.STRUCTURED_PROPERTY)
            .setStructuredPropertyParams(new StructuredPropertyParams().setUrn(TEST_PROPERTY_URN))
            .setRequired(true);
    FormPrompt testPrompt2 =
        new FormPrompt()
            .setId(TEST_FORM_PROMPT_ID_2)
            .setTitle("Test 2")
            .setType(FormPromptType.STRUCTURED_PROPERTY)
            .setStructuredPropertyParams(new StructuredPropertyParams().setUrn(TEST_PROPERTY_URN))
            .setRequired(true);
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_FORM_URN,
            FORM_INFO_ASPECT_NAME,
            ChangeType.UPSERT,
            mockFormInfo(ImmutableList.of(testPrompt1, testPrompt2)),
            null);
    hook.invoke(event);
    Mockito.verify(service, Mockito.times(1))
        .upsertFormPromptCompletionAutomation(
            nullable(OperationContext.class),
            Mockito.eq(TEST_FORM_URN),
            Mockito.eq(ImmutableList.of(testPrompt1, testPrompt2)));
    Mockito.verify(service, Mockito.times(0))
        .removeFormPromptCompletionAutomation(
            nullable(OperationContext.class), Mockito.eq(TEST_FORM_URN), any(FormPrompt.class));
  }

  @Test
  public void testInvokeFormInfoHasPrevious() throws Exception {
    FormService service = mockFormService();
    FormAssignmentHook hook = new FormAssignmentHook(service, true);

    FormPrompt prevPrompt1 =
        new FormPrompt()
            .setId("prev-prompt")
            .setTitle("Prev Test")
            .setType(FormPromptType.STRUCTURED_PROPERTY)
            .setStructuredPropertyParams(new StructuredPropertyParams().setUrn(TEST_PROPERTY_URN))
            .setRequired(true);

    FormPrompt newPrompt1 =
        new FormPrompt()
            .setId(TEST_FORM_PROMPT_ID_1)
            .setTitle("Test")
            .setType(FormPromptType.STRUCTURED_PROPERTY)
            .setStructuredPropertyParams(new StructuredPropertyParams().setUrn(TEST_PROPERTY_URN))
            .setRequired(true);

    FormPrompt newPrompt2 =
        new FormPrompt()
            .setId(TEST_FORM_PROMPT_ID_2)
            .setTitle("Test 2")
            .setType(FormPromptType.STRUCTURED_PROPERTY)
            .setStructuredPropertyParams(new StructuredPropertyParams().setUrn(TEST_PROPERTY_URN))
            .setRequired(true);

    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_FORM_URN,
            FORM_INFO_ASPECT_NAME,
            ChangeType.UPSERT,
            mockFormInfo(ImmutableList.of(newPrompt1, newPrompt2)),
            mockFormInfo(ImmutableList.of(prevPrompt1)));
    hook.invoke(event);
    Mockito.verify(service, Mockito.times(1))
        .upsertFormPromptCompletionAutomation(
            nullable(OperationContext.class),
            Mockito.eq(TEST_FORM_URN),
            Mockito.eq(ImmutableList.of(newPrompt1, newPrompt2)));
    Mockito.verify(service, Mockito.times(1))
        .removeFormPromptCompletionAutomation(
            nullable(OperationContext.class), Mockito.eq(TEST_FORM_URN), Mockito.eq(prevPrompt1));
  }

  @Test
  public void testInvokeDynamicFilters() throws Exception {
    FormService service = mockFormService();
    FormAssignmentHook hook = new FormAssignmentHook(service, true);
    final Filter newFilter =
        buildFilter(
            ImmutableList.of(CriterionUtils.buildCriterion("platform", Condition.EQUAL, "Test")));
    final DynamicFormAssignment newFormFilters = mockDynamicFilterAssignment(newFilter);
    final MetadataChangeLog event1 =
        buildMetadataChangeLog(
            TEST_FORM_URN, DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME, ChangeType.UPSERT, newFormFilters);
    hook.invoke(event1);
    Mockito.verify(service, Mockito.times(1))
        .upsertFormAssignmentAutomation(
            nullable(OperationContext.class),
            Mockito.eq(TEST_FORM_URN),
            Mockito.eq(newFormFilters));
  }

  @Test
  public void testNotPublished() {
    FormService service = mockFormService();
    FormAssignmentHook hook = new FormAssignmentHook(service, true);
    FormPrompt testPrompt1 =
        new FormPrompt()
            .setId(TEST_FORM_PROMPT_ID_1)
            .setTitle("Test")
            .setType(FormPromptType.STRUCTURED_PROPERTY)
            .setStructuredPropertyParams(new StructuredPropertyParams().setUrn(TEST_PROPERTY_URN))
            .setRequired(true);
    FormPrompt testPrompt2 =
        new FormPrompt()
            .setId(TEST_FORM_PROMPT_ID_2)
            .setTitle("Test 2")
            .setType(FormPromptType.STRUCTURED_PROPERTY)
            .setStructuredPropertyParams(new StructuredPropertyParams().setUrn(TEST_PROPERTY_URN))
            .setRequired(true);
    final FormStatus formStatus = new FormStatus();
    formStatus.setState(FormState.UNPUBLISHED);
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_FORM_URN,
            FORM_INFO_ASPECT_NAME,
            ChangeType.UPSERT,
            mockFormInfo(ImmutableList.of(testPrompt1, testPrompt2), formStatus),
            mockFormInfo(ImmutableList.of(testPrompt1, testPrompt2)));
    hook.invoke(event);
    Mockito.verify(service, Mockito.times(2))
        .removeFormPromptCompletionAutomation(
            nullable(OperationContext.class), Mockito.eq(TEST_FORM_URN), any(FormPrompt.class));
  }

  private FormInfo mockFormInfo(final List<FormPrompt> prompts) {
    return mockFormInfo(prompts, null);
  }

  private FormInfo mockFormInfo(final List<FormPrompt> prompts, @Nullable FormStatus status) {
    FormInfo formInfo = new FormInfo();
    formInfo.setName("Test");
    formInfo.setType(FormType.VERIFICATION);
    formInfo.setPrompts(new FormPromptArray(prompts));
    if (status != null) {
      formInfo.setStatus(status);
    }
    return formInfo;
  }

  private DynamicFormAssignment mockDynamicFilterAssignment(final Filter filter) {
    DynamicFormAssignment dynamicFormAssignment = new DynamicFormAssignment();
    dynamicFormAssignment.setFilter(filter);
    return dynamicFormAssignment;
  }

  private Filter buildFilter(final List<Criterion> criteria) {
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                ImmutableList.of(new ConjunctiveCriterion().setAnd(new CriterionArray(criteria)))));
  }

  private FormService mockFormService() {
    return Mockito.mock(FormService.class);
  }

  private MetadataChangeLog buildMetadataChangeLog(
      Urn urn, String aspectName, ChangeType changeType, RecordTemplate aspect) throws Exception {
    return buildMetadataChangeLog(urn, aspectName, changeType, aspect, null);
  }

  private MetadataChangeLog buildMetadataChangeLog(
      Urn urn,
      String aspectName,
      ChangeType changeType,
      RecordTemplate aspect,
      RecordTemplate prevAspect) {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityUrn(urn);
    event.setEntityType(FORM_ENTITY_NAME);
    event.setAspectName(aspectName);
    event.setChangeType(changeType);
    event.setAspect(GenericRecordUtils.serializeAspect(aspect));
    if (prevAspect != null) {
      event.setPreviousAspectValue(GenericRecordUtils.serializeAspect(prevAspect));
    }
    return event;
  }
}
