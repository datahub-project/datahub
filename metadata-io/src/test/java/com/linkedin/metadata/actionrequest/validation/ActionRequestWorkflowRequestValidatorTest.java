package com.linkedin.metadata.actionrequest.validation;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestParams;
import com.linkedin.actionworkflow.ActionWorkflowCategory;
import com.linkedin.actionworkflow.ActionWorkflowFieldArray;
import com.linkedin.actionworkflow.ActionWorkflowFormRequest;
import com.linkedin.actionworkflow.ActionWorkflowFormRequestField;
import com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray;
import com.linkedin.actionworkflow.ActionWorkflowInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.PropertyValue;
import com.linkedin.structured.PropertyValueArray;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ActionRequestWorkflowRequestValidatorTest {

  private static final Urn TEST_WORKFLOW_URN =
      UrnUtils.getUrn("urn:li:actionWorkflow:test-workflow");
  private static final Urn TEST_ACTION_REQUEST_URN =
      UrnUtils.getUrn("urn:li:actionRequest:test-request");

  @Mock private AspectRetriever mockAspectRetriever;
  @Mock private RetrieverContext mockRetrieverContext;

  private ActionRequestWorkflowRequestValidator validator;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    validator = new ActionRequestWorkflowRequestValidator();

    // Setup retriever context mock
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
  }

  /*--------------------------------------------------------------------------
   *                      VALIDATION SUCCESS TESTS
   *------------------------------------------------------------------------*/

  @Test
  public void testValidateWorkflowRequestSuccess() throws Exception {
    // GIVEN
    ChangeMCP changeMCP = createValidChangeMCP();
    ActionWorkflowInfo workflowInfo = createValidWorkflowInfo();

    when(mockAspectRetriever.getLatestAspectObject(
            eq(TEST_WORKFLOW_URN), eq(ACTION_WORKFLOW_INFO_ASPECT_NAME)))
        .thenReturn(createAspectFromWorkflowInfo(workflowInfo));

    // WHEN
    Stream<AspectValidationException> result =
        validator.validateWorkflowRequestUpserts(
            Collections.singletonList(changeMCP), mockRetrieverContext);

    // THEN
    List<AspectValidationException> exceptions = result.toList();
    assertTrue(exceptions.isEmpty(), "No validation exceptions should be thrown for valid request");
    verify(mockAspectRetriever)
        .getLatestAspectObject(eq(TEST_WORKFLOW_URN), eq(ACTION_WORKFLOW_INFO_ASPECT_NAME));
  }

  @Test
  public void testValidateNonWorkflowRequest() throws Exception {
    // GIVEN
    ChangeMCP changeMCP = createNonWorkflowRequestChangeMCP();

    // WHEN
    Stream<AspectValidationException> result =
        validator.validateWorkflowRequestUpserts(
            Collections.singletonList(changeMCP), mockRetrieverContext);

    // THEN
    List<AspectValidationException> exceptions = result.toList();
    assertTrue(exceptions.isEmpty(), "No validation should occur for non-workflow requests");
    verify(mockAspectRetriever, never()).getLatestAspectObject(any(), any());
  }

  /*--------------------------------------------------------------------------
   *                      VALIDATION FAILURE TESTS
   *------------------------------------------------------------------------*/

  @Test
  public void testValidateWorkflowRequestEmptyFields() throws Exception {
    // GIVEN
    ChangeMCP changeMCP = createChangeMCPWithEmptyFields();
    ActionWorkflowInfo workflowInfo = createWorkflowInfoWithRequiredFields();

    when(mockAspectRetriever.getLatestAspectObject(
            eq(TEST_WORKFLOW_URN), eq(ACTION_WORKFLOW_INFO_ASPECT_NAME)))
        .thenReturn(createAspectFromWorkflowInfo(workflowInfo));

    // WHEN
    Stream<AspectValidationException> result =
        validator.validateWorkflowRequestUpserts(
            Collections.singletonList(changeMCP), mockRetrieverContext);

    // THEN
    List<AspectValidationException> exceptions = result.toList();
    assertFalse(exceptions.isEmpty(), "Should have validation exceptions for empty fields");
    assertTrue(exceptions.get(0).getMessage().contains("must have at least one field"));
  }

  @Test
  public void testValidateWorkflowRequestInvalidFieldId() throws Exception {
    // GIVEN
    ChangeMCP changeMCP = createChangeMCPWithInvalidFieldId();
    ActionWorkflowInfo workflowInfo = createValidWorkflowInfo();

    when(mockAspectRetriever.getLatestAspectObject(
            eq(TEST_WORKFLOW_URN), eq(ACTION_WORKFLOW_INFO_ASPECT_NAME)))
        .thenReturn(createAspectFromWorkflowInfo(workflowInfo));

    // WHEN
    Stream<AspectValidationException> result =
        validator.validateWorkflowRequestUpserts(
            Collections.singletonList(changeMCP), mockRetrieverContext);

    // THEN
    List<AspectValidationException> exceptions = result.toList();
    assertFalse(exceptions.isEmpty(), "Should have validation exceptions for invalid field ID");
    assertTrue(exceptions.get(0).getMessage().contains("not defined in workflow"));
  }

  @Test
  public void testValidateWorkflowRequestMissingRequiredField() throws Exception {
    // GIVEN
    ChangeMCP changeMCP = createChangeMCPMissingRequiredField();
    ActionWorkflowInfo workflowInfo = createWorkflowInfoWithMultipleFields();

    when(mockAspectRetriever.getLatestAspectObject(
            eq(TEST_WORKFLOW_URN), eq(ACTION_WORKFLOW_INFO_ASPECT_NAME)))
        .thenReturn(createAspectFromWorkflowInfo(workflowInfo));

    // WHEN
    Stream<AspectValidationException> result =
        validator.validateWorkflowRequestUpserts(
            Collections.singletonList(changeMCP), mockRetrieverContext);

    // THEN
    List<AspectValidationException> exceptions = result.toList();
    assertFalse(
        exceptions.isEmpty(), "Should have validation exceptions for missing required field");
    assertTrue(exceptions.get(0).getMessage().contains("missing from workflow request"));
  }

  @Test
  public void testValidateWorkflowRequestInvalidAllowedValue() throws Exception {
    // GIVEN
    ChangeMCP changeMCP = createChangeMCPWithInvalidAllowedValue();
    ActionWorkflowInfo workflowInfo = createWorkflowInfoWithAllowedValues();

    when(mockAspectRetriever.getLatestAspectObject(
            eq(TEST_WORKFLOW_URN), eq(ACTION_WORKFLOW_INFO_ASPECT_NAME)))
        .thenReturn(createAspectFromWorkflowInfo(workflowInfo));

    // WHEN
    Stream<AspectValidationException> result =
        validator.validateWorkflowRequestUpserts(
            Collections.singletonList(changeMCP), mockRetrieverContext);

    // THEN
    List<AspectValidationException> exceptions = result.toList();
    assertFalse(
        exceptions.isEmpty(), "Should have validation exceptions for invalid allowed value");
    assertTrue(exceptions.get(0).getMessage().contains("not in the list of allowed values"));
  }

  /*--------------------------------------------------------------------------
   *                      ERROR HANDLING TESTS
   *------------------------------------------------------------------------*/

  @Test
  public void testValidateWorkflowRequestNotFound() throws Exception {
    // GIVEN
    ChangeMCP changeMCP = createValidChangeMCP();

    when(mockAspectRetriever.getLatestAspectObject(
            eq(TEST_WORKFLOW_URN), eq(ACTION_WORKFLOW_INFO_ASPECT_NAME)))
        .thenReturn(null);

    // WHEN
    Stream<AspectValidationException> result =
        validator.validateWorkflowRequestUpserts(
            Collections.singletonList(changeMCP), mockRetrieverContext);

    // THEN
    List<AspectValidationException> exceptions = result.toList();
    assertFalse(exceptions.isEmpty(), "Should have validation exceptions when workflow not found");
    assertTrue(exceptions.get(0).getMessage().contains("Workflow definition not found"));
  }

  @Test
  public void testValidateWorkflowRequestServiceThrowsException() throws Exception {
    // GIVEN
    ChangeMCP changeMCP = createValidChangeMCP();

    when(mockAspectRetriever.getLatestAspectObject(
            eq(TEST_WORKFLOW_URN), eq(ACTION_WORKFLOW_INFO_ASPECT_NAME)))
        .thenThrow(new RuntimeException("Aspect retrieval failed"));

    // WHEN
    Stream<AspectValidationException> result =
        validator.validateWorkflowRequestUpserts(
            Collections.singletonList(changeMCP), mockRetrieverContext);

    // THEN
    List<AspectValidationException> exceptions = result.toList();
    assertFalse(exceptions.isEmpty(), "Should have validation exceptions when retrieval throws");
    assertTrue(exceptions.get(0).getMessage().contains("Failed to validate workflow request"));
  }

  @Test
  public void testValidateMultipleChangeMCPs() throws Exception {
    // GIVEN
    ChangeMCP validChangeMCP = createValidChangeMCP();
    ChangeMCP invalidChangeMCP = createChangeMCPWithInvalidFieldId();
    ActionWorkflowInfo workflowInfo = createValidWorkflowInfo();

    when(mockAspectRetriever.getLatestAspectObject(
            eq(TEST_WORKFLOW_URN), eq(ACTION_WORKFLOW_INFO_ASPECT_NAME)))
        .thenReturn(createAspectFromWorkflowInfo(workflowInfo));

    // WHEN
    Stream<AspectValidationException> result =
        validator.validateWorkflowRequestUpserts(
            List.of(validChangeMCP, invalidChangeMCP), mockRetrieverContext);

    // THEN
    List<AspectValidationException> exceptions = result.toList();
    assertEquals(exceptions.size(), 1, "Should have exactly one validation exception");
    assertTrue(exceptions.get(0).getMessage().contains("not defined in workflow"));
  }

  @Test
  public void testValidatePreCommitAspectsFiltering() throws Exception {
    // GIVEN
    ChangeMCP actionRequestMCP = createValidChangeMCP();
    ChangeMCP otherMCP = mock(ChangeMCP.class);
    when(otherMCP.getAspectName()).thenReturn("someOtherAspect");

    ActionWorkflowInfo workflowInfo = createValidWorkflowInfo();
    when(mockAspectRetriever.getLatestAspectObject(
            eq(TEST_WORKFLOW_URN), eq(ACTION_WORKFLOW_INFO_ASPECT_NAME)))
        .thenReturn(createAspectFromWorkflowInfo(workflowInfo));

    // WHEN
    Stream<AspectValidationException> result =
        validator.validatePreCommitAspects(
            List.of(actionRequestMCP, otherMCP), mockRetrieverContext);

    // THEN
    List<AspectValidationException> exceptions = result.toList();
    assertTrue(
        exceptions.isEmpty(), "Should only validate ACTION_REQUEST_INFO_ASPECT_NAME aspects");

    // Verify only called once for the actionRequest aspect, not the other one
    verify(mockAspectRetriever, times(1))
        .getLatestAspectObject(eq(TEST_WORKFLOW_URN), eq(ACTION_WORKFLOW_INFO_ASPECT_NAME));
  }

  /*--------------------------------------------------------------------------
   *                      HELPER METHODS
   *------------------------------------------------------------------------*/

  private ChangeMCP createValidChangeMCP() {
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType("WORKFLOW_FORM_REQUEST");

    ActionWorkflowFormRequest workflowRequest = new ActionWorkflowFormRequest();
    workflowRequest.setWorkflow(TEST_WORKFLOW_URN);
    workflowRequest.setCategory(ActionWorkflowCategory.ACCESS);

    // Create valid field
    ActionWorkflowFormRequestField field = new ActionWorkflowFormRequestField();
    field.setId("reason");
    PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();
    values.add(PrimitivePropertyValue.create("Test reason"));
    field.setValues(values);

    ActionWorkflowFormRequestFieldArray fields = new ActionWorkflowFormRequestFieldArray();
    fields.add(field);
    workflowRequest.setFields(fields);

    ActionRequestParams params = new ActionRequestParams();
    params.setWorkflowFormRequest(workflowRequest);
    actionRequestInfo.setParams(params);

    return createChangeMCP(actionRequestInfo);
  }

  private ChangeMCP createNonWorkflowRequestChangeMCP() {
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType("OTHER_REQUEST_TYPE");
    // No workflow request params

    return createChangeMCP(actionRequestInfo);
  }

  private ChangeMCP createChangeMCPWithEmptyFields() {
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType("WORKFLOW_REQUEST");

    ActionWorkflowFormRequest workflowRequest = new ActionWorkflowFormRequest();
    workflowRequest.setWorkflow(TEST_WORKFLOW_URN);
    workflowRequest.setCategory(ActionWorkflowCategory.ACCESS);
    workflowRequest.setFields(new ActionWorkflowFormRequestFieldArray()); // Empty fields

    ActionRequestParams params = new ActionRequestParams();
    params.setWorkflowFormRequest(workflowRequest);
    actionRequestInfo.setParams(params);

    return createChangeMCP(actionRequestInfo);
  }

  private ChangeMCP createChangeMCPWithInvalidFieldId() {
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType("WORKFLOW_REQUEST");

    ActionWorkflowFormRequest workflowRequest = new ActionWorkflowFormRequest();
    workflowRequest.setWorkflow(TEST_WORKFLOW_URN);
    workflowRequest.setCategory(ActionWorkflowCategory.ACCESS);

    // Create field with invalid ID
    ActionWorkflowFormRequestField field = new ActionWorkflowFormRequestField();
    field.setId("invalid_field_id");
    PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();
    values.add(PrimitivePropertyValue.create("Test value"));
    field.setValues(values);

    ActionWorkflowFormRequestFieldArray fields = new ActionWorkflowFormRequestFieldArray();
    fields.add(field);
    workflowRequest.setFields(fields);

    ActionRequestParams params = new ActionRequestParams();
    params.setWorkflowFormRequest(workflowRequest);
    actionRequestInfo.setParams(params);

    return createChangeMCP(actionRequestInfo);
  }

  private ChangeMCP createChangeMCPMissingRequiredField() {
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType("WORKFLOW_REQUEST");

    ActionWorkflowFormRequest workflowRequest = new ActionWorkflowFormRequest();
    workflowRequest.setWorkflow(TEST_WORKFLOW_URN);
    workflowRequest.setCategory(ActionWorkflowCategory.ACCESS);

    // Only provide optional field, missing required field
    ActionWorkflowFormRequestField field = new ActionWorkflowFormRequestField();
    field.setId("optional_field");
    PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();
    values.add(PrimitivePropertyValue.create("Optional value"));
    field.setValues(values);

    ActionWorkflowFormRequestFieldArray fields = new ActionWorkflowFormRequestFieldArray();
    fields.add(field);
    workflowRequest.setFields(fields);

    ActionRequestParams params = new ActionRequestParams();
    params.setWorkflowFormRequest(workflowRequest);
    actionRequestInfo.setParams(params);

    return createChangeMCP(actionRequestInfo);
  }

  private ChangeMCP createChangeMCPWithInvalidAllowedValue() {
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType("WORKFLOW_REQUEST");

    ActionWorkflowFormRequest workflowRequest = new ActionWorkflowFormRequest();
    workflowRequest.setWorkflow(TEST_WORKFLOW_URN);
    workflowRequest.setCategory(ActionWorkflowCategory.ACCESS);

    // Create field with invalid allowed value
    ActionWorkflowFormRequestField field = new ActionWorkflowFormRequestField();
    field.setId("classification");
    PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();
    values.add(PrimitivePropertyValue.create("INVALID_VALUE"));
    field.setValues(values);

    ActionWorkflowFormRequestFieldArray fields = new ActionWorkflowFormRequestFieldArray();
    fields.add(field);
    workflowRequest.setFields(fields);

    ActionRequestParams params = new ActionRequestParams();
    params.setWorkflowFormRequest(workflowRequest);
    actionRequestInfo.setParams(params);

    return createChangeMCP(actionRequestInfo);
  }

  private ChangeMCP createChangeMCP(ActionRequestInfo actionRequestInfo) {
    ChangeMCP changeMCP = mock(ChangeMCP.class);
    when(changeMCP.getAspectName()).thenReturn(ACTION_REQUEST_INFO_ASPECT_NAME);
    when(changeMCP.getAspect(ActionRequestInfo.class)).thenReturn(actionRequestInfo);
    when(changeMCP.getUrn()).thenReturn(TEST_ACTION_REQUEST_URN);
    return changeMCP;
  }

  private ActionWorkflowInfo createValidWorkflowInfo() {
    ActionWorkflowInfo workflowInfo = new ActionWorkflowInfo();
    workflowInfo.setName("Test Workflow");
    workflowInfo.setCategory(ActionWorkflowCategory.ACCESS);
    workflowInfo.setDescription("Test workflow");

    // Create trigger with form
    com.linkedin.actionworkflow.ActionWorkflowTrigger trigger =
        new com.linkedin.actionworkflow.ActionWorkflowTrigger();
    trigger.setType(com.linkedin.actionworkflow.ActionWorkflowTriggerType.FORM_SUBMITTED);

    com.linkedin.actionworkflow.ActionWorkflowForm form =
        new com.linkedin.actionworkflow.ActionWorkflowForm();

    ActionWorkflowFieldArray fields = new ActionWorkflowFieldArray();

    // Required reason field
    com.linkedin.actionworkflow.ActionWorkflowField reasonField =
        new com.linkedin.actionworkflow.ActionWorkflowField();
    reasonField.setId("reason");
    reasonField.setName("Reason");
    reasonField.setDescription("Reason for access");
    reasonField.setValueType(UrnUtils.getUrn("urn:li:structured:propertyType:string"));
    reasonField.setCardinality(com.linkedin.structured.PropertyCardinality.SINGLE);
    reasonField.setRequired(true);
    fields.add(reasonField);

    form.setFields(fields);
    trigger.setForm(form);
    workflowInfo.setTrigger(trigger);

    return workflowInfo;
  }

  private ActionWorkflowInfo createWorkflowInfoWithRequiredFields() {
    return createValidWorkflowInfo(); // Already has required fields
  }

  private ActionWorkflowInfo createWorkflowInfoWithMultipleFields() {
    ActionWorkflowInfo workflowInfo = new ActionWorkflowInfo();
    workflowInfo.setName("Test Workflow With Multiple Fields");
    workflowInfo.setCategory(ActionWorkflowCategory.ACCESS);
    workflowInfo.setDescription("Test workflow with multiple fields");

    // Create trigger with form
    com.linkedin.actionworkflow.ActionWorkflowTrigger trigger =
        new com.linkedin.actionworkflow.ActionWorkflowTrigger();
    trigger.setType(com.linkedin.actionworkflow.ActionWorkflowTriggerType.FORM_SUBMITTED);

    com.linkedin.actionworkflow.ActionWorkflowForm form =
        new com.linkedin.actionworkflow.ActionWorkflowForm();

    ActionWorkflowFieldArray fields = new ActionWorkflowFieldArray();

    // Required field
    com.linkedin.actionworkflow.ActionWorkflowField requiredField =
        new com.linkedin.actionworkflow.ActionWorkflowField();
    requiredField.setId("required_field");
    requiredField.setName("Required Field");
    requiredField.setDescription("A required field");
    requiredField.setValueType(UrnUtils.getUrn("urn:li:structured:propertyType:string"));
    requiredField.setCardinality(com.linkedin.structured.PropertyCardinality.SINGLE);
    requiredField.setRequired(true);
    fields.add(requiredField);

    // Optional field
    com.linkedin.actionworkflow.ActionWorkflowField optionalField =
        new com.linkedin.actionworkflow.ActionWorkflowField();
    optionalField.setId("optional_field");
    optionalField.setName("Optional Field");
    optionalField.setDescription("An optional field");
    optionalField.setValueType(UrnUtils.getUrn("urn:li:structured:propertyType:string"));
    optionalField.setCardinality(com.linkedin.structured.PropertyCardinality.SINGLE);
    optionalField.setRequired(false);
    fields.add(optionalField);

    form.setFields(fields);
    trigger.setForm(form);
    workflowInfo.setTrigger(trigger);

    return workflowInfo;
  }

  private ActionWorkflowInfo createWorkflowInfoWithAllowedValues() {
    ActionWorkflowInfo workflowInfo = new ActionWorkflowInfo();
    workflowInfo.setName("Test Workflow With Allowed Values");
    workflowInfo.setCategory(ActionWorkflowCategory.CUSTOM);
    workflowInfo.setDescription("Test workflow with allowed values");

    // Create trigger with form
    com.linkedin.actionworkflow.ActionWorkflowTrigger trigger =
        new com.linkedin.actionworkflow.ActionWorkflowTrigger();
    trigger.setType(com.linkedin.actionworkflow.ActionWorkflowTriggerType.FORM_SUBMITTED);

    com.linkedin.actionworkflow.ActionWorkflowForm form =
        new com.linkedin.actionworkflow.ActionWorkflowForm();

    ActionWorkflowFieldArray fields = new ActionWorkflowFieldArray();

    com.linkedin.actionworkflow.ActionWorkflowField classificationField =
        new com.linkedin.actionworkflow.ActionWorkflowField();
    classificationField.setId("classification");
    classificationField.setName("Classification Level");
    classificationField.setDescription("Classification with allowed values");
    classificationField.setValueType(UrnUtils.getUrn("urn:li:structured:propertyType:string"));
    classificationField.setCardinality(com.linkedin.structured.PropertyCardinality.SINGLE);
    classificationField.setRequired(true);

    // Set allowed values
    PropertyValueArray allowedValues = new PropertyValueArray();
    allowedValues.add(new PropertyValue().setValue(PrimitivePropertyValue.create("PUBLIC")));
    allowedValues.add(new PropertyValue().setValue(PrimitivePropertyValue.create("INTERNAL")));
    allowedValues.add(new PropertyValue().setValue(PrimitivePropertyValue.create("CONFIDENTIAL")));
    classificationField.setAllowedValues(allowedValues);

    fields.add(classificationField);
    form.setFields(fields);
    trigger.setForm(form);
    workflowInfo.setTrigger(trigger);

    return workflowInfo;
  }

  private com.linkedin.entity.Aspect createAspectFromWorkflowInfo(ActionWorkflowInfo workflowInfo) {
    return new com.linkedin.entity.Aspect(workflowInfo.data());
  }
}
