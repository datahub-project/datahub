package com.linkedin.metadata.kafka.hook.form;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.FormAssociation;
import com.linkedin.common.FormAssociationArray;
import com.linkedin.common.FormPromptAssociation;
import com.linkedin.common.FormPromptAssociationArray;
import com.linkedin.common.Forms;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormPrompt;
import com.linkedin.form.FormPromptArray;
import com.linkedin.form.FormPromptType;
import com.linkedin.form.FormType;
import com.linkedin.form.StructuredPropertyParams;
import com.linkedin.metadata.key.FormKey;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class FormCompletionHookTest {

  private static final Urn TEST_DATASET_URN = UrnUtils.getUrn("urn:li:dataset:(test,test,test)");
  private static final Urn TEST_FORM_URN = UrnUtils.getUrn("urn:li:form:test");
  private static final String TEST_FORM_PROMPT_ID_1 = "test-id";
  private static final Urn TEST_PROPERTY_URN = UrnUtils.getUrn("urn:li:structuredProperty:test.id");

  @Test
  public void testInvokeNotEnabled() throws Exception {
    FormService service = mockFormService(true);
    SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    FormCompletionHook hook = new FormCompletionHook(service, false, mockClient);
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_DATASET_URN, FORMS_ASPECT_NAME, ChangeType.UPSERT, mockForms(false, true));
    hook.invoke(event);
    Mockito.verifyNoInteractions(service);
    Mockito.verifyNoInteractions(mockClient);
  }

  @Test
  public void testInvokeNotEligibleChange() throws Exception {
    FormService service = mockFormService(true);
    SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    FormCompletionHook hook = new FormCompletionHook(service, true, mockClient);

    // Case 1: Bad aspect
    final MetadataChangeLog event1 =
        buildMetadataChangeLog(
            TEST_DATASET_URN, FORM_KEY_ASPECT_NAME, ChangeType.UPSERT, new FormKey().setId("test"));
    hook.invoke(event1);
    Mockito.verifyNoInteractions(service);
    Mockito.verifyNoInteractions(mockClient);

    // Case 2: Bad change type
    final MetadataChangeLog event2 =
        buildMetadataChangeLog(
            TEST_DATASET_URN, FORMS_ASPECT_NAME, ChangeType.DELETE, mockForms(false, true));
    hook.invoke(event2);
    Mockito.verifyNoInteractions(service);
    Mockito.verifyNoInteractions(mockClient);
  }

  @Test
  public void testInvokeSetFormComplete() throws Exception {
    FormService service = mockFormService(true);
    SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    FormCompletionHook hook = new FormCompletionHook(service, true, mockClient);
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_DATASET_URN, FORMS_ASPECT_NAME, ChangeType.UPSERT, mockForms(false, true));
    hook.invoke(event);

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            nullable(OperationContext.class),
            Mockito.any(MetadataChangeProposal.class),
            Mockito.eq(true));
  }

  @Test
  public void testInvokeNoActionWithCompleteForm() throws Exception {
    // in this test, the form is complete and the prompt is complete so we don't need to take any
    // action
    FormService service = mockFormService(true);
    SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    FormCompletionHook hook = new FormCompletionHook(service, true, mockClient);
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_DATASET_URN, FORMS_ASPECT_NAME, ChangeType.UPSERT, mockForms(true, true));
    hook.invoke(event);

    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(
            nullable(OperationContext.class),
            Mockito.any(MetadataChangeProposal.class),
            Mockito.eq(true));
  }

  @Test
  public void testInvokeNoActionWithIncompleteForm() throws Exception {
    // in this test, the form is incomplete and the prompt is incomplete so we don't need to take
    // any action
    FormService service = mockFormService(true);
    SystemEntityClient mockClient = Mockito.mock(SystemEntityClient.class);
    FormCompletionHook hook = new FormCompletionHook(service, true, mockClient);
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_DATASET_URN, FORMS_ASPECT_NAME, ChangeType.UPSERT, mockForms(false, false));
    hook.invoke(event);

    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(
            nullable(OperationContext.class),
            Mockito.any(MetadataChangeProposal.class),
            Mockito.eq(true));
  }

  private FormInfo mockFormInfo(boolean isPromptRequired) {
    FormInfo formInfo = new FormInfo();
    formInfo.setName("Test");
    formInfo.setType(FormType.VERIFICATION);
    formInfo.setPrompts(
        new FormPromptArray(
            ImmutableList.of(
                new FormPrompt()
                    .setId(TEST_FORM_PROMPT_ID_1)
                    .setType(FormPromptType.STRUCTURED_PROPERTY)
                    .setRequired(isPromptRequired)
                    .setStructuredPropertyParams(
                        new StructuredPropertyParams().setUrn(TEST_PROPERTY_URN)))));
    return formInfo;
  }

  private FormAssociation createFormAssociation(boolean isPromptComplete) {
    FormAssociation formAssociation = new FormAssociation();
    formAssociation.setUrn(TEST_FORM_URN);
    FormPromptAssociationArray formPromptAssociations = new FormPromptAssociationArray();
    FormPromptAssociation promptAssociation = new FormPromptAssociation();
    promptAssociation.setId(TEST_FORM_PROMPT_ID_1);
    formPromptAssociations.add(promptAssociation);

    if (isPromptComplete) {
      formAssociation.setCompletedPrompts(formPromptAssociations);
      formAssociation.setIncompletePrompts(new FormPromptAssociationArray());
    } else {
      formAssociation.setCompletedPrompts(new FormPromptAssociationArray());
      formAssociation.setIncompletePrompts(formPromptAssociations);
    }

    return formAssociation;
  }

  private Forms mockForms(boolean isFormComplete, boolean isPromptComplete) {
    Forms forms = new Forms();
    FormAssociationArray formAssociations = new FormAssociationArray();
    FormAssociation formAssociation = createFormAssociation(isPromptComplete);
    formAssociations.add(formAssociation);

    if (isFormComplete) {
      forms.setCompletedForms(formAssociations);
      forms.setIncompleteForms(new FormAssociationArray());
    } else {
      forms.setCompletedForms(new FormAssociationArray());
      forms.setIncompleteForms(formAssociations);
    }

    return forms;
  }

  private FormService mockFormService(boolean isPromptRequired) {
    FormService mockService = Mockito.mock(FormService.class);
    Map<Urn, FormInfo> response = new HashMap<>();
    response.put(TEST_FORM_URN, mockFormInfo(isPromptRequired));
    Mockito.when(mockService.batchFetchForms(any(), eq(ImmutableSet.of(TEST_FORM_URN))))
        .thenReturn(response);
    Mockito.doCallRealMethod().when(mockService).isFormCompleted(any(), any());
    return mockService;
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
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(aspectName);
    event.setChangeType(changeType);
    event.setAspect(GenericRecordUtils.serializeAspect(aspect));
    if (prevAspect != null) {
      event.setPreviousAspectValue(GenericRecordUtils.serializeAspect(prevAspect));
    }
    return event;
  }
}
