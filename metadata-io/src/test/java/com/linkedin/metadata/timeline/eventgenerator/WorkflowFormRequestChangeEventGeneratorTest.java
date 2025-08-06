package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_COMPLETE;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestParams;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.actionworkflow.ActionWorkflowFormRequest;
import com.linkedin.actionworkflow.ActionWorkflowFormRequestField;
import com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray;
import com.linkedin.actionworkflow.ActionWorkflowRequestStepState;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.ActionWorkflowService;
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

public class WorkflowFormRequestChangeEventGeneratorTest {

  private ActionRequestInfoChangeEventGenerator infoGenerator;
  private ActionRequestStatusChangeEventGenerator statusGenerator;
  private UserService mockUserService;
  private OperationContext mockOperationContext;
  private ActionWorkflowService mockActionWorkflowService;
  private SystemEntityClient mockSystemEntityClient;

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
    mockActionWorkflowService = mock(ActionWorkflowService.class);
    mockSystemEntityClient = mock(SystemEntityClient.class);

    // Mock user service to return user email
    when(mockUserService.getUserEmail(any(OperationContext.class), eq(TEST_ACTOR_URN)))
        .thenReturn(TEST_ACTOR_EMAIL);

    // Create generators with workflow dependencies - they will delegate to workflow-specific logic
    infoGenerator =
        new ActionRequestInfoChangeEventGenerator(
            mockUserService, mockOperationContext, mockSystemEntityClient);
    statusGenerator =
        new ActionRequestStatusChangeEventGenerator(
            mockUserService,
            mockOperationContext,
            mockActionWorkflowService,
            mockSystemEntityClient);
  }

  @Test
  public void testWorkflowRequestInfoCreateEvent() {
    // Create test workflow request
    ActionRequestInfo actionRequestInfo = createTestWorkflowActionRequestInfo();
    Aspect<ActionRequestInfo> fromAspect = new Aspect<>(null, null);
    Aspect<ActionRequestInfo> toAspect = new Aspect<>(actionRequestInfo, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        infoGenerator.getChangeEvents(
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
  public void testWorkflowRequestInfoStepCompleteEvent() {
    // Create previous and new workflow requests with different step states
    ActionRequestInfo previousActionRequestInfo = createTestWorkflowActionRequestInfo();
    ActionRequestInfo newActionRequestInfo = createTestWorkflowActionRequestInfo();

    // Change the step state to simulate step completion
    ActionWorkflowRequestStepState prevStepState = new ActionWorkflowRequestStepState();
    prevStepState.setStepId("data_steward_approval");
    previousActionRequestInfo.getParams().getWorkflowFormRequest().setStepState(prevStepState);

    Aspect<ActionRequestInfo> fromAspect = new Aspect<>(previousActionRequestInfo, null);
    Aspect<ActionRequestInfo> toAspect = new Aspect<>(newActionRequestInfo, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        infoGenerator.getChangeEvents(
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

    // Verify step completion parameters - updated based on your changes
    Map<String, Object> parameters = event.getParameters();
    assertFalse(parameters.containsKey("operation")); // No longer setting operation override
    assertEquals(parameters.get("stepId"), "data_steward_approval");
    assertEquals(parameters.get("stepResult"), "ACCEPTED"); // Updated parameter name and value
  }

  @Test
  public void testWorkflowRequestStatusCompleteEvent() throws Exception {
    // Mock the ActionWorkflowService to return the action request info
    ActionRequestInfo actionRequestInfo = createTestWorkflowActionRequestInfo();
    when(mockActionWorkflowService.getActionRequestInfo(
            any(OperationContext.class), eq(TEST_ACTION_REQUEST_URN)))
        .thenReturn(actionRequestInfo);

    // Create status change from PENDING to COMPLETE
    ActionRequestStatus previousStatus = new ActionRequestStatus();
    previousStatus.setStatus("PENDING");

    ActionRequestStatus newStatus = new ActionRequestStatus();
    newStatus.setStatus(ACTION_REQUEST_STATUS_COMPLETE);
    newStatus.setResult(ACTION_REQUEST_RESULT_ACCEPTED);

    Aspect<ActionRequestStatus> fromAspect = new Aspect<>(previousStatus, null);
    Aspect<ActionRequestStatus> toAspect = new Aspect<>(newStatus, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        statusGenerator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestStatus",
            fromAspect,
            toAspect,
            auditStamp);

    // Verify event
    assertEquals(changeEvents.size(), 1);
    ChangeEvent event = changeEvents.get(0);

    assertEquals(event.getCategory(), ChangeCategory.LIFECYCLE);
    assertEquals(event.getOperation(), ChangeOperation.COMPLETED);

    // Verify parameters - updated based on your changes
    Map<String, Object> parameters = event.getParameters();
    assertEquals(parameters.get("actionRequestType"), ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    assertEquals(parameters.get("operation"), "COMPLETE");
    assertEquals(parameters.get("result"), ACTION_REQUEST_RESULT_ACCEPTED); // No longer lowercased
    assertEquals(parameters.get("workflowUrn"), TEST_WORKFLOW_URN.toString());
    assertEquals(parameters.get("workflowId"), "data-access-request");
    assertEquals(parameters.get("actorUrn"), TEST_ACTOR_URN.toString());
    assertEquals(parameters.get("actorEmail"), TEST_ACTOR_EMAIL);
  }

  @Test
  public void testNonWorkflowRequestFallback() {
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
        infoGenerator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestInfo",
            fromAspect,
            toAspect,
            auditStamp);

    // Verify it falls back to parent implementation
    assertEquals(changeEvents.size(), 1);
    ChangeEvent event = changeEvents.get(0);

    // Should still generate an event but without workflow-specific parameters
    Map<String, Object> parameters = event.getParameters();
    assertFalse(parameters.containsKey("workflowUrn"));
    assertFalse(parameters.containsKey("workflowId"));
    assertFalse(parameters.containsKey("fields"));
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
