package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestParams;
import com.linkedin.actionworkflow.ActionWorkflowFormRequest;
import com.linkedin.actionworkflow.ActionWorkflowFormRequestField;
import com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray;
import com.linkedin.actionworkflow.ActionWorkflowRequestStepState;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.UserService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class WorkflowFormRequestStepCompletionChangeEventGeneratorTest {

  private WorkflowFormRequestStepCompletionChangeEventGenerator generator;
  private UserService mockUserService;
  private OperationContext mockOperationContext;

  private static final Urn TEST_ACTION_REQUEST_URN =
      UrnUtils.getUrn("urn:li:actionRequest:test-request");
  private static final Urn TEST_WORKFLOW_URN =
      UrnUtils.getUrn("urn:li:actionWorkflow:data-access-request");
  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:johnjoyce");
  private static final String TEST_ACTOR_EMAIL = "john.joyce@datahub.com";
  private static final long TEST_TIMESTAMP = 1234567890L;

  @BeforeMethod
  public void setup() throws Exception {
    mockUserService = mock(UserService.class);
    mockOperationContext = mock(OperationContext.class);

    // Mock user service to return user email
    when(mockUserService.getUserEmail(any(OperationContext.class), eq(TEST_ACTOR_URN)))
        .thenReturn(TEST_ACTOR_EMAIL);

    generator =
        new WorkflowFormRequestStepCompletionChangeEventGenerator(
            mockUserService, mockOperationContext);
  }

  @Test
  public void testConstructor() {
    // Test constructor with valid parameters
    assertNotNull(generator);

    // Test constructor with null parameters
    WorkflowFormRequestStepCompletionChangeEventGenerator nullGenerator =
        new WorkflowFormRequestStepCompletionChangeEventGenerator(null, null);
    assertNotNull(nullGenerator);
  }

  @Test
  public void testGetChangeEventsForWorkflowRequest() {
    // Create test workflow request
    ActionRequestInfo actionRequestInfo = createTestWorkflowActionRequestInfo();
    Aspect<ActionRequestInfo> fromAspect = new Aspect<>(null, null);
    Aspect<ActionRequestInfo> toAspect = new Aspect<>(actionRequestInfo, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestInfo",
            fromAspect,
            toAspect,
            auditStamp);

    // Verify event
    assertEquals(changeEvents.size(), 1);
    ChangeEvent event = changeEvents.get(0);

    assertEquals(event.getCategory(), ChangeCategory.LIFECYCLE);
    assertEquals(event.getOperation(), ChangeOperation.CREATE);
    assertEquals(event.getEntityUrn(), TEST_ACTION_REQUEST_URN.toString());
    assertEquals(event.getAuditStamp(), auditStamp);

    // Verify parameters
    Map<String, Object> parameters = event.getParameters();
    assertEquals(parameters.get("actionRequestType"), ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    assertEquals(parameters.get("workflowUrn"), TEST_WORKFLOW_URN.toString());
    assertEquals(parameters.get("workflowId"), "data-access-request");
    assertEquals(parameters.get("actorUrn"), TEST_ACTOR_URN.toString());
    assertEquals(parameters.get("actorEmail"), TEST_ACTOR_EMAIL);
    assertTrue(parameters.containsKey("fields"));
  }

  @Test
  public void testGetChangeEventsForNonWorkflowRequest() {
    // Create a non-workflow action request
    ActionRequestInfo nonWorkflowActionRequestInfo = new ActionRequestInfo();
    nonWorkflowActionRequestInfo.setType("TAG_ASSOCIATION");
    nonWorkflowActionRequestInfo.setCreatedBy(TEST_ACTOR_URN);
    nonWorkflowActionRequestInfo.setCreated(TEST_TIMESTAMP);

    Aspect<ActionRequestInfo> fromAspect = new Aspect<>(null, null);
    Aspect<ActionRequestInfo> toAspect = new Aspect<>(nonWorkflowActionRequestInfo, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestInfo",
            fromAspect,
            toAspect,
            auditStamp);

    // Should return empty list for non-workflow requests
    assertTrue(changeEvents.isEmpty());
  }

  @Test
  public void testGetChangeEventsWithNullToAspect() {
    Aspect<ActionRequestInfo> fromAspect = new Aspect<>(null, null);
    Aspect<ActionRequestInfo> toAspect = new Aspect<>(null, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestInfo",
            fromAspect,
            toAspect,
            auditStamp);

    // Should return empty list
    assertTrue(changeEvents.isEmpty());
  }

  @Test
  public void testComputeWorkflowRequestDiffsWithEqualAspects() {
    // Create identical workflow requests
    ActionRequestInfo actionRequestInfo1 = createTestWorkflowActionRequestInfo();
    ActionRequestInfo actionRequestInfo2 = createTestWorkflowActionRequestInfo();

    Aspect<ActionRequestInfo> fromAspect = new Aspect<>(actionRequestInfo1, null);
    Aspect<ActionRequestInfo> toAspect = new Aspect<>(actionRequestInfo2, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestInfo",
            fromAspect,
            toAspect,
            auditStamp);

    // Should return empty list for equal aspects
    assertTrue(changeEvents.isEmpty());
  }

  @Test
  public void testStepCompletionEvent() {
    // Create previous and new workflow requests with different step states
    ActionRequestInfo previousActionRequestInfo = createTestWorkflowActionRequestInfo();
    ActionRequestInfo newActionRequestInfo = createTestWorkflowActionRequestInfo();

    // Change the step state to simulate step completion
    ActionWorkflowRequestStepState newStepState = new ActionWorkflowRequestStepState();
    newStepState.setStepId("data_steward_approval");
    newActionRequestInfo.getParams().getWorkflowFormRequest().setStepState(newStepState);

    Aspect<ActionRequestInfo> fromAspect = new Aspect<>(previousActionRequestInfo, null);
    Aspect<ActionRequestInfo> toAspect = new Aspect<>(newActionRequestInfo, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestInfo",
            fromAspect,
            toAspect,
            auditStamp);

    // Verify event
    assertEquals(changeEvents.size(), 1);
    ChangeEvent event = changeEvents.get(0);

    assertEquals(event.getCategory(), ChangeCategory.LIFECYCLE);
    assertEquals(event.getOperation(), ChangeOperation.MODIFY);

    // Verify step completion parameters
    Map<String, Object> parameters = event.getParameters();
    assertEquals(parameters.get("stepId"), "data_steward_approval");
    assertEquals(parameters.get("stepResult"), "APPROVED");
  }

  @Test
  public void testIsStepCompletionWithSameStepId() {
    // Create workflow requests with same step state
    ActionRequestInfo actionRequestInfo1 = createTestWorkflowActionRequestInfo();
    ActionRequestInfo actionRequestInfo2 = createTestWorkflowActionRequestInfo();

    Aspect<ActionRequestInfo> fromAspect = new Aspect<>(actionRequestInfo1, null);
    Aspect<ActionRequestInfo> toAspect = new Aspect<>(actionRequestInfo2, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestInfo",
            fromAspect,
            toAspect,
            auditStamp);

    // Should return empty list since step IDs are the same
    assertTrue(changeEvents.isEmpty());
  }

  @Test
  public void testIsStepCompletionWithMissingStepState() {
    // Create workflow requests without step state
    ActionRequestInfo actionRequestInfo1 = createTestWorkflowActionRequestInfo();
    ActionRequestInfo actionRequestInfo2 = createTestWorkflowActionRequestInfo();

    // Remove step state from both
    actionRequestInfo1.getParams().getWorkflowFormRequest().removeStepState();
    actionRequestInfo2.getParams().getWorkflowFormRequest().removeStepState();

    Aspect<ActionRequestInfo> fromAspect = new Aspect<>(actionRequestInfo1, null);
    Aspect<ActionRequestInfo> toAspect = new Aspect<>(actionRequestInfo2, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestInfo",
            fromAspect,
            toAspect,
            auditStamp);

    // Should return empty list since both have no step state
    assertTrue(changeEvents.isEmpty());
  }

  @Test
  public void testBuildWorkflowRequestParametersWithMinimalData() {
    // Create minimal workflow request
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType(ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    actionRequestInfo.setCreatedBy(TEST_ACTOR_URN);
    actionRequestInfo.setCreated(TEST_TIMESTAMP);

    ActionWorkflowFormRequest workflowRequest = new ActionWorkflowFormRequest();
    workflowRequest.setWorkflow(TEST_WORKFLOW_URN);

    ActionRequestParams params = new ActionRequestParams();
    params.setWorkflowFormRequest(workflowRequest);
    actionRequestInfo.setParams(params);

    Aspect<ActionRequestInfo> fromAspect = new Aspect<>(null, null);
    Aspect<ActionRequestInfo> toAspect = new Aspect<>(actionRequestInfo, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestInfo",
            fromAspect,
            toAspect,
            auditStamp);

    // Verify event
    assertEquals(changeEvents.size(), 1);
    ChangeEvent event = changeEvents.get(0);

    Map<String, Object> parameters = event.getParameters();
    assertEquals(parameters.get("actionRequestType"), ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    assertEquals(parameters.get("workflowUrn"), TEST_WORKFLOW_URN.toString());
    assertEquals(parameters.get("workflowId"), "data-access-request");
    assertEquals(parameters.get("actorUrn"), TEST_ACTOR_URN.toString());
    assertEquals(parameters.get("actorEmail"), TEST_ACTOR_EMAIL);
    assertFalse(parameters.containsKey("stepId"));
    assertFalse(parameters.containsKey("stepResult"));
  }

  @Test
  public void testBuildWorkflowRequestParametersWithMissingWorkflow() {
    // Create workflow request without workflow URN
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType(ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    actionRequestInfo.setCreatedBy(TEST_ACTOR_URN);
    actionRequestInfo.setCreated(TEST_TIMESTAMP);

    ActionWorkflowFormRequest workflowRequest = new ActionWorkflowFormRequest();
    // Don't set workflow URN

    ActionRequestParams params = new ActionRequestParams();
    params.setWorkflowFormRequest(workflowRequest);
    actionRequestInfo.setParams(params);

    Aspect<ActionRequestInfo> fromAspect = new Aspect<>(null, null);
    Aspect<ActionRequestInfo> toAspect = new Aspect<>(actionRequestInfo, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestInfo",
            fromAspect,
            toAspect,
            auditStamp);

    // Verify event
    assertEquals(changeEvents.size(), 1);
    ChangeEvent event = changeEvents.get(0);

    Map<String, Object> parameters = event.getParameters();
    assertEquals(parameters.get("actionRequestType"), ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    assertFalse(parameters.containsKey("workflowUrn"));
    assertFalse(parameters.containsKey("workflowId"));
  }

  @Test
  public void testBuildWorkflowRequestParametersWithMissingParams() {
    // Create workflow request without params
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType(ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    actionRequestInfo.setCreatedBy(TEST_ACTOR_URN);
    actionRequestInfo.setCreated(TEST_TIMESTAMP);
    // Don't set params

    Aspect<ActionRequestInfo> fromAspect = new Aspect<>(null, null);
    Aspect<ActionRequestInfo> toAspect = new Aspect<>(actionRequestInfo, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestInfo",
            fromAspect,
            toAspect,
            auditStamp);

    // Verify event
    assertEquals(changeEvents.size(), 1);
    ChangeEvent event = changeEvents.get(0);

    Map<String, Object> parameters = event.getParameters();
    assertEquals(parameters.get("actionRequestType"), ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    assertFalse(parameters.containsKey("workflowUrn"));
    assertFalse(parameters.containsKey("fields"));
  }

  @Test
  public void testBuildWorkflowRequestParametersWithMissingCreatedBy() {
    // Create workflow request without createdBy
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType(ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    actionRequestInfo.setCreated(TEST_TIMESTAMP);
    // Don't set createdBy

    ActionWorkflowFormRequest workflowRequest = new ActionWorkflowFormRequest();
    workflowRequest.setWorkflow(TEST_WORKFLOW_URN);

    ActionRequestParams params = new ActionRequestParams();
    params.setWorkflowFormRequest(workflowRequest);
    actionRequestInfo.setParams(params);

    Aspect<ActionRequestInfo> fromAspect = new Aspect<>(null, null);
    Aspect<ActionRequestInfo> toAspect = new Aspect<>(actionRequestInfo, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestInfo",
            fromAspect,
            toAspect,
            auditStamp);

    // Verify event
    assertEquals(changeEvents.size(), 1);
    ChangeEvent event = changeEvents.get(0);

    Map<String, Object> parameters = event.getParameters();
    assertEquals(parameters.get("actionRequestType"), ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    assertFalse(parameters.containsKey("actorUrn"));
    assertFalse(parameters.containsKey("actorEmail"));
  }

  @Test
  public void testUserEmailResolutionFailure() throws Exception {
    // Mock user service to throw exception
    when(mockUserService.getUserEmail(any(OperationContext.class), eq(TEST_ACTOR_URN)))
        .thenThrow(new RuntimeException("User service error"));

    ActionRequestInfo actionRequestInfo = createTestWorkflowActionRequestInfo();
    Aspect<ActionRequestInfo> fromAspect = new Aspect<>(null, null);
    Aspect<ActionRequestInfo> toAspect = new Aspect<>(actionRequestInfo, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestInfo",
            fromAspect,
            toAspect,
            auditStamp);

    // Verify event is still generated but without email
    assertEquals(changeEvents.size(), 1);
    ChangeEvent event = changeEvents.get(0);

    Map<String, Object> parameters = event.getParameters();
    assertEquals(parameters.get("actorUrn"), TEST_ACTOR_URN.toString());
    assertFalse(parameters.containsKey("actorEmail"));
  }

  @Test
  public void testUserEmailResolutionReturnsNull() throws Exception {
    // Mock user service to return null
    when(mockUserService.getUserEmail(any(OperationContext.class), eq(TEST_ACTOR_URN)))
        .thenReturn(null);

    ActionRequestInfo actionRequestInfo = createTestWorkflowActionRequestInfo();
    Aspect<ActionRequestInfo> fromAspect = new Aspect<>(null, null);
    Aspect<ActionRequestInfo> toAspect = new Aspect<>(actionRequestInfo, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestInfo",
            fromAspect,
            toAspect,
            auditStamp);

    // Verify event is still generated but without email
    assertEquals(changeEvents.size(), 1);
    ChangeEvent event = changeEvents.get(0);

    Map<String, Object> parameters = event.getParameters();
    assertEquals(parameters.get("actorUrn"), TEST_ACTOR_URN.toString());
    assertFalse(parameters.containsKey("actorEmail"));
  }

  @Test
  public void testConvertPrimitiveValueToStringWithStringValue() {
    ActionRequestInfo actionRequestInfo = createTestWorkflowActionRequestInfo();
    Aspect<ActionRequestInfo> fromAspect = new Aspect<>(null, null);
    Aspect<ActionRequestInfo> toAspect = new Aspect<>(actionRequestInfo, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestInfo",
            fromAspect,
            toAspect,
            auditStamp);

    // Verify event with string field values
    assertEquals(changeEvents.size(), 1);
    ChangeEvent event = changeEvents.get(0);

    Map<String, Object> parameters = event.getParameters();
    assertTrue(parameters.containsKey("fields"));
    // The fields parameter should contain the string values
    String fieldsJson = (String) parameters.get("fields");
    assertTrue(fieldsJson.contains("urn:li:dataset"));
    assertTrue(fieldsJson.contains("I need access"));
  }

  @Test
  public void testConvertPrimitiveValueToStringWithDoubleValue() {
    // Create workflow request with double field value
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType(ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    actionRequestInfo.setCreatedBy(TEST_ACTOR_URN);
    actionRequestInfo.setCreated(TEST_TIMESTAMP);

    ActionWorkflowFormRequest workflowRequest = new ActionWorkflowFormRequest();
    workflowRequest.setWorkflow(TEST_WORKFLOW_URN);

    ActionWorkflowRequestStepState stepState = new ActionWorkflowRequestStepState();
    stepState.setStepId("owner_approval");
    workflowRequest.setStepState(stepState);

    // Add double field
    ActionWorkflowFormRequestField doubleField = new ActionWorkflowFormRequestField();
    doubleField.setId("priority");
    PrimitivePropertyValue doubleValue = new PrimitivePropertyValue();
    doubleValue.setDouble(1.5);
    doubleField.setValues(new PrimitivePropertyValueArray(doubleValue));

    workflowRequest.setFields(new ActionWorkflowFormRequestFieldArray(doubleField));

    ActionRequestParams params = new ActionRequestParams();
    params.setWorkflowFormRequest(workflowRequest);
    actionRequestInfo.setParams(params);

    Aspect<ActionRequestInfo> fromAspect = new Aspect<>(null, null);
    Aspect<ActionRequestInfo> toAspect = new Aspect<>(actionRequestInfo, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestInfo",
            fromAspect,
            toAspect,
            auditStamp);

    // Verify event with double field value
    assertEquals(changeEvents.size(), 1);
    ChangeEvent event = changeEvents.get(0);

    Map<String, Object> parameters = event.getParameters();
    assertTrue(parameters.containsKey("fields"));
    String fieldsJson = (String) parameters.get("fields");
    assertTrue(fieldsJson.contains("1.5"));
  }

  @Test
  public void testWorkflowRequestFieldsWithEmptyValues() {
    // Create workflow request with empty field values
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType(ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    actionRequestInfo.setCreatedBy(TEST_ACTOR_URN);
    actionRequestInfo.setCreated(TEST_TIMESTAMP);

    ActionWorkflowFormRequest workflowRequest = new ActionWorkflowFormRequest();
    workflowRequest.setWorkflow(TEST_WORKFLOW_URN);

    ActionWorkflowRequestStepState stepState = new ActionWorkflowRequestStepState();
    stepState.setStepId("owner_approval");
    workflowRequest.setStepState(stepState);

    // Add field with empty values
    ActionWorkflowFormRequestField emptyField = new ActionWorkflowFormRequestField();
    emptyField.setId("empty_field");
    emptyField.setValues(new PrimitivePropertyValueArray());

    workflowRequest.setFields(new ActionWorkflowFormRequestFieldArray(emptyField));

    ActionRequestParams params = new ActionRequestParams();
    params.setWorkflowFormRequest(workflowRequest);
    actionRequestInfo.setParams(params);

    Aspect<ActionRequestInfo> fromAspect = new Aspect<>(null, null);
    Aspect<ActionRequestInfo> toAspect = new Aspect<>(actionRequestInfo, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestInfo",
            fromAspect,
            toAspect,
            auditStamp);

    // Verify event still generates with empty fields
    assertEquals(changeEvents.size(), 1);
    ChangeEvent event = changeEvents.get(0);

    Map<String, Object> parameters = event.getParameters();
    assertTrue(parameters.containsKey("fields"));
    String fieldsJson = (String) parameters.get("fields");
    assertEquals(fieldsJson, "{}"); // Empty object since no valid fields
  }

  @Test
  public void testWorkflowRequestFieldsWithNullValues() {
    // Create workflow request with null field values
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType(ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    actionRequestInfo.setCreatedBy(TEST_ACTOR_URN);
    actionRequestInfo.setCreated(TEST_TIMESTAMP);

    ActionWorkflowFormRequest workflowRequest = new ActionWorkflowFormRequest();
    workflowRequest.setWorkflow(TEST_WORKFLOW_URN);

    ActionWorkflowRequestStepState stepState = new ActionWorkflowRequestStepState();
    stepState.setStepId("owner_approval");
    workflowRequest.setStepState(stepState);

    // Add field with null values
    ActionWorkflowFormRequestField nullField = new ActionWorkflowFormRequestField();
    nullField.setId("null_field");
    // Don't set values

    workflowRequest.setFields(new ActionWorkflowFormRequestFieldArray(nullField));

    ActionRequestParams params = new ActionRequestParams();
    params.setWorkflowFormRequest(workflowRequest);
    actionRequestInfo.setParams(params);

    Aspect<ActionRequestInfo> fromAspect = new Aspect<>(null, null);
    Aspect<ActionRequestInfo> toAspect = new Aspect<>(actionRequestInfo, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestInfo",
            fromAspect,
            toAspect,
            auditStamp);

    // Verify event still generates with null fields
    assertEquals(changeEvents.size(), 1);
    ChangeEvent event = changeEvents.get(0);

    Map<String, Object> parameters = event.getParameters();
    assertTrue(parameters.containsKey("fields"));
    String fieldsJson = (String) parameters.get("fields");
    assertEquals(fieldsJson, "{}"); // Empty object since no valid fields
  }

  private ActionRequestInfo createTestWorkflowActionRequestInfo() {
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType(ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    actionRequestInfo.setCreatedBy(TEST_ACTOR_URN);
    actionRequestInfo.setCreated(TEST_TIMESTAMP);

    // Create workflow request
    ActionWorkflowFormRequest workflowRequest = new ActionWorkflowFormRequest();
    workflowRequest.setWorkflow(TEST_WORKFLOW_URN);

    // Add step state
    ActionWorkflowRequestStepState stepState = new ActionWorkflowRequestStepState();
    stepState.setStepId("owner_approval");
    workflowRequest.setStepState(stepState);

    // Add test fields
    ActionWorkflowFormRequestField datasetField = new ActionWorkflowFormRequestField();
    datasetField.setId("dataset");
    PrimitivePropertyValue datasetValue = new PrimitivePropertyValue();
    datasetValue.setString("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)");
    datasetField.setValues(new PrimitivePropertyValueArray(datasetValue));

    ActionWorkflowFormRequestField justificationField = new ActionWorkflowFormRequestField();
    justificationField.setId("justification");
    PrimitivePropertyValue justificationValue = new PrimitivePropertyValue();
    justificationValue.setString("I need access because I am trying to create a new dashboard");
    justificationField.setValues(new PrimitivePropertyValueArray(justificationValue));

    workflowRequest.setFields(
        new ActionWorkflowFormRequestFieldArray(datasetField, justificationField));

    // Create params
    ActionRequestParams params = new ActionRequestParams();
    params.setWorkflowFormRequest(workflowRequest);
    actionRequestInfo.setParams(params);

    return actionRequestInfo;
  }
}
