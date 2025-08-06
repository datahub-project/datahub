package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_RESULT_REJECTED;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_COMPLETE;
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
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.actionworkflow.ActionWorkflowFormRequest;
import com.linkedin.actionworkflow.ActionWorkflowFormRequestField;
import com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray;
import com.linkedin.actionworkflow.ActionWorkflowRequestAccess;
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

public class WorkflowFormRequestCompletionChangeEventGeneratorTest {

  private WorkflowFormRequestCompletionChangeEventGenerator generator;
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

    // Mock ActionWorkflowService to return test ActionRequestInfo
    ActionRequestInfo actionRequestInfo = createTestWorkflowActionRequestInfo();
    when(mockActionWorkflowService.getActionRequestInfo(
            any(OperationContext.class), eq(TEST_ACTION_REQUEST_URN)))
        .thenReturn(actionRequestInfo);

    generator =
        new WorkflowFormRequestCompletionChangeEventGenerator(
            mockUserService,
            mockOperationContext,
            mockActionWorkflowService,
            mockSystemEntityClient);
  }

  @Test
  public void testConstructor() {
    // Test constructor with valid parameters
    assertNotNull(generator);

    // Test constructor with null parameters
    WorkflowFormRequestCompletionChangeEventGenerator nullGenerator =
        new WorkflowFormRequestCompletionChangeEventGenerator(null, null, null, null);
    assertNotNull(nullGenerator);
  }

  @Test
  public void testGetChangeEventsForWorkflowRequestStatusComplete() throws Exception {
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
        generator.getChangeEvents(
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
    assertEquals(event.getEntityUrn(), TEST_ACTION_REQUEST_URN.toString());
    assertEquals(event.getAuditStamp(), auditStamp);

    // Verify parameters
    Map<String, Object> parameters = event.getParameters();
    assertEquals(parameters.get("actionRequestType"), ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    assertEquals(parameters.get("operation"), "COMPLETE");
    assertEquals(parameters.get("result"), ACTION_REQUEST_RESULT_ACCEPTED);
    assertEquals(parameters.get("workflowUrn"), TEST_WORKFLOW_URN.toString());
    assertEquals(parameters.get("workflowId"), "data-access-request");
    assertEquals(parameters.get("actorUrn"), TEST_ACTOR_URN.toString());
    assertEquals(parameters.get("actorEmail"), TEST_ACTOR_EMAIL);
    assertTrue(parameters.containsKey("fields"));
  }

  @Test
  public void testGetChangeEventsForWorkflowRequestStatusCompleteWithRejectedResult()
      throws Exception {
    // Create status change from PENDING to COMPLETE with REJECTED result
    ActionRequestStatus previousStatus = new ActionRequestStatus();
    previousStatus.setStatus("PENDING");

    ActionRequestStatus newStatus = new ActionRequestStatus();
    newStatus.setStatus(ACTION_REQUEST_STATUS_COMPLETE);
    newStatus.setResult(ACTION_REQUEST_RESULT_REJECTED);

    Aspect<ActionRequestStatus> fromAspect = new Aspect<>(previousStatus, null);
    Aspect<ActionRequestStatus> toAspect = new Aspect<>(newStatus, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        generator.getChangeEvents(
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

    // Verify parameters
    Map<String, Object> parameters = event.getParameters();
    assertEquals(parameters.get("result"), ACTION_REQUEST_RESULT_REJECTED);
  }

  @Test
  public void testGetChangeEventsForNonCompleteStatus() throws Exception {
    // Create status change from PENDING to IN_PROGRESS (not complete)
    ActionRequestStatus previousStatus = new ActionRequestStatus();
    previousStatus.setStatus("PENDING");

    ActionRequestStatus newStatus = new ActionRequestStatus();
    newStatus.setStatus("IN_PROGRESS");

    Aspect<ActionRequestStatus> fromAspect = new Aspect<>(previousStatus, null);
    Aspect<ActionRequestStatus> toAspect = new Aspect<>(newStatus, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestStatus",
            fromAspect,
            toAspect,
            auditStamp);

    // Should return empty list for non-complete status
    assertTrue(changeEvents.isEmpty());
  }

  @Test
  public void testGetChangeEventsForNonWorkflowRequest() throws Exception {
    // Mock ActionWorkflowService to return non-workflow ActionRequestInfo
    ActionRequestInfo nonWorkflowActionRequestInfo = new ActionRequestInfo();
    nonWorkflowActionRequestInfo.setType("TAG_ASSOCIATION");
    nonWorkflowActionRequestInfo.setCreatedBy(TEST_ACTOR_URN);
    nonWorkflowActionRequestInfo.setCreated(TEST_TIMESTAMP);

    when(mockActionWorkflowService.getActionRequestInfo(
            any(OperationContext.class), eq(TEST_ACTION_REQUEST_URN)))
        .thenReturn(nonWorkflowActionRequestInfo);

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
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestStatus",
            fromAspect,
            toAspect,
            auditStamp);

    // Should return empty list for non-workflow requests
    assertTrue(changeEvents.isEmpty());
  }

  @Test
  public void testGetChangeEventsWithActionWorkflowServiceReturnsNull() throws Exception {
    // Mock ActionWorkflowService to return null
    when(mockActionWorkflowService.getActionRequestInfo(
            any(OperationContext.class), eq(TEST_ACTION_REQUEST_URN)))
        .thenReturn(null);

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
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestStatus",
            fromAspect,
            toAspect,
            auditStamp);

    // Should return empty list when ActionRequestInfo is null
    assertTrue(changeEvents.isEmpty());
  }

  @Test
  public void testGetChangeEventsWithActionWorkflowServiceThrowsException() throws Exception {
    // Mock ActionWorkflowService to throw exception
    when(mockActionWorkflowService.getActionRequestInfo(
            any(OperationContext.class), eq(TEST_ACTION_REQUEST_URN)))
        .thenThrow(new RuntimeException("Service error"));

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
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestStatus",
            fromAspect,
            toAspect,
            auditStamp);

    // Should return empty list when service throws exception
    assertTrue(changeEvents.isEmpty());
  }

  @Test
  public void testGetChangeEventsWithNullToAspect() throws Exception {
    Aspect<ActionRequestStatus> fromAspect = new Aspect<>(null, null);
    Aspect<ActionRequestStatus> toAspect = new Aspect<>(null, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestStatus",
            fromAspect,
            toAspect,
            auditStamp);

    // Should return empty list when new aspect is null
    assertTrue(changeEvents.isEmpty());
  }

  @Test
  public void testGetChangeEventsWithEqualStatuses() throws Exception {
    // Create identical statuses
    ActionRequestStatus status1 = new ActionRequestStatus();
    status1.setStatus(ACTION_REQUEST_STATUS_COMPLETE);
    status1.setResult(ACTION_REQUEST_RESULT_ACCEPTED);

    ActionRequestStatus status2 = new ActionRequestStatus();
    status2.setStatus(ACTION_REQUEST_STATUS_COMPLETE);
    status2.setResult(ACTION_REQUEST_RESULT_ACCEPTED);

    Aspect<ActionRequestStatus> fromAspect = new Aspect<>(status1, null);
    Aspect<ActionRequestStatus> toAspect = new Aspect<>(status2, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestStatus",
            fromAspect,
            toAspect,
            auditStamp);

    // Should return empty list when statuses are equal
    assertTrue(changeEvents.isEmpty());
  }

  @Test
  public void testBuildWorkflowRequestStatusParametersWithMinimalData() throws Exception {
    // Create minimal workflow request without fields
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType(ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    actionRequestInfo.setCreatedBy(TEST_ACTOR_URN);
    actionRequestInfo.setCreated(TEST_TIMESTAMP);

    ActionWorkflowFormRequest workflowRequest = new ActionWorkflowFormRequest();
    workflowRequest.setWorkflow(TEST_WORKFLOW_URN);

    ActionRequestParams params = new ActionRequestParams();
    params.setWorkflowFormRequest(workflowRequest);
    actionRequestInfo.setParams(params);

    when(mockActionWorkflowService.getActionRequestInfo(
            any(OperationContext.class), eq(TEST_ACTION_REQUEST_URN)))
        .thenReturn(actionRequestInfo);

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
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestStatus",
            fromAspect,
            toAspect,
            auditStamp);

    // Verify event
    assertEquals(changeEvents.size(), 1);
    ChangeEvent event = changeEvents.get(0);

    Map<String, Object> parameters = event.getParameters();
    assertEquals(parameters.get("actionRequestType"), ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    assertEquals(parameters.get("operation"), "COMPLETE");
    assertEquals(parameters.get("result"), ACTION_REQUEST_RESULT_ACCEPTED);
    assertEquals(parameters.get("workflowUrn"), TEST_WORKFLOW_URN.toString());
    assertEquals(parameters.get("workflowId"), "data-access-request");
    assertEquals(parameters.get("actorUrn"), TEST_ACTOR_URN.toString());
    assertEquals(parameters.get("actorEmail"), TEST_ACTOR_EMAIL);
    assertFalse(parameters.containsKey("fields"));
  }

  @Test
  public void testBuildWorkflowRequestStatusParametersWithMissingWorkflow() throws Exception {
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

    when(mockActionWorkflowService.getActionRequestInfo(
            any(OperationContext.class), eq(TEST_ACTION_REQUEST_URN)))
        .thenReturn(actionRequestInfo);

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
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestStatus",
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
  public void testBuildWorkflowRequestStatusParametersWithMissingParams() throws Exception {
    // Create workflow request without params
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType(ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    actionRequestInfo.setCreatedBy(TEST_ACTOR_URN);
    actionRequestInfo.setCreated(TEST_TIMESTAMP);
    // Don't set params

    when(mockActionWorkflowService.getActionRequestInfo(
            any(OperationContext.class), eq(TEST_ACTION_REQUEST_URN)))
        .thenReturn(actionRequestInfo);

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
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestStatus",
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
  public void testBuildWorkflowRequestStatusParametersWithMissingCreatedBy() throws Exception {
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

    when(mockActionWorkflowService.getActionRequestInfo(
            any(OperationContext.class), eq(TEST_ACTION_REQUEST_URN)))
        .thenReturn(actionRequestInfo);

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
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestStatus",
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
  public void testBuildWorkflowRequestStatusParametersWithMissingResult() throws Exception {
    ActionRequestStatus previousStatus = new ActionRequestStatus();
    previousStatus.setStatus("PENDING");

    ActionRequestStatus newStatus = new ActionRequestStatus();
    newStatus.setStatus(ACTION_REQUEST_STATUS_COMPLETE);
    // Don't set result

    Aspect<ActionRequestStatus> fromAspect = new Aspect<>(previousStatus, null);
    Aspect<ActionRequestStatus> toAspect = new Aspect<>(newStatus, null);

    AuditStamp auditStamp = new AuditStamp().setTime(TEST_TIMESTAMP).setActor(TEST_ACTOR_URN);

    // Generate change events
    List<ChangeEvent> changeEvents =
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestStatus",
            fromAspect,
            toAspect,
            auditStamp);

    // Verify event
    assertEquals(changeEvents.size(), 1);
    ChangeEvent event = changeEvents.get(0);

    Map<String, Object> parameters = event.getParameters();
    assertEquals(parameters.get("actionRequestType"), ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    assertFalse(parameters.containsKey("result"));
  }

  @Test
  public void testUserEmailResolutionFailure() throws Exception {
    // Mock user service to throw exception
    when(mockUserService.getUserEmail(any(OperationContext.class), eq(TEST_ACTOR_URN)))
        .thenThrow(new RuntimeException("User service error"));

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
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestStatus",
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
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestStatus",
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
  public void testConvertPrimitiveValueToStringWithDoubleValue() throws Exception {
    // Create workflow request with double field value
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType(ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    actionRequestInfo.setCreatedBy(TEST_ACTOR_URN);
    actionRequestInfo.setCreated(TEST_TIMESTAMP);

    ActionWorkflowFormRequest workflowRequest = new ActionWorkflowFormRequest();
    workflowRequest.setWorkflow(TEST_WORKFLOW_URN);

    // Add double field
    ActionWorkflowFormRequestField doubleField = new ActionWorkflowFormRequestField();
    doubleField.setId("priority");
    PrimitivePropertyValue doubleValue = new PrimitivePropertyValue();
    doubleValue.setDouble(2.5);
    doubleField.setValues(new PrimitivePropertyValueArray(doubleValue));

    workflowRequest.setFields(new ActionWorkflowFormRequestFieldArray(doubleField));

    ActionRequestParams params = new ActionRequestParams();
    params.setWorkflowFormRequest(workflowRequest);
    actionRequestInfo.setParams(params);

    when(mockActionWorkflowService.getActionRequestInfo(
            any(OperationContext.class), eq(TEST_ACTION_REQUEST_URN)))
        .thenReturn(actionRequestInfo);

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
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestStatus",
            fromAspect,
            toAspect,
            auditStamp);

    // Verify event with double field value
    assertEquals(changeEvents.size(), 1);
    ChangeEvent event = changeEvents.get(0);

    Map<String, Object> parameters = event.getParameters();
    assertTrue(parameters.containsKey("fields"));
    String fieldsJson = (String) parameters.get("fields");
    assertTrue(fieldsJson.contains("2.5"));
  }

  @Test
  public void testWorkflowRequestFieldsWithEmptyValues() throws Exception {
    // Create workflow request with empty field values
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType(ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    actionRequestInfo.setCreatedBy(TEST_ACTOR_URN);
    actionRequestInfo.setCreated(TEST_TIMESTAMP);

    ActionWorkflowFormRequest workflowRequest = new ActionWorkflowFormRequest();
    workflowRequest.setWorkflow(TEST_WORKFLOW_URN);

    // Add field with empty values
    ActionWorkflowFormRequestField emptyField = new ActionWorkflowFormRequestField();
    emptyField.setId("empty_field");
    emptyField.setValues(new PrimitivePropertyValueArray());

    workflowRequest.setFields(new ActionWorkflowFormRequestFieldArray(emptyField));

    ActionRequestParams params = new ActionRequestParams();
    params.setWorkflowFormRequest(workflowRequest);
    actionRequestInfo.setParams(params);

    when(mockActionWorkflowService.getActionRequestInfo(
            any(OperationContext.class), eq(TEST_ACTION_REQUEST_URN)))
        .thenReturn(actionRequestInfo);

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
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestStatus",
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
  public void testWorkflowCompletionWithExpiresAt() throws Exception {
    // Create workflow request with access information and expiration
    ActionRequestInfo actionRequestInfo = createTestWorkflowActionRequestInfo();

    // Add access information with expiresAt
    ActionWorkflowRequestAccess accessInfo = new ActionWorkflowRequestAccess();
    accessInfo.setExpiresAt(1234567890123L); // Example timestamp
    actionRequestInfo.getParams().getWorkflowFormRequest().setAccess(accessInfo);

    when(mockActionWorkflowService.getActionRequestInfo(
            any(OperationContext.class), eq(TEST_ACTION_REQUEST_URN)))
        .thenReturn(actionRequestInfo);

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
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestStatus",
            fromAspect,
            toAspect,
            auditStamp);

    // Verify event
    assertEquals(changeEvents.size(), 1);
    ChangeEvent event = changeEvents.get(0);

    // Verify expiresAtMs parameter is included
    Map<String, Object> parameters = event.getParameters();
    assertEquals(parameters.get("expiresAtMs"), 1234567890123L);
    assertEquals(parameters.get("actionRequestType"), ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    assertEquals(parameters.get("result"), ACTION_REQUEST_RESULT_ACCEPTED);
  }

  @Test
  public void testWorkflowCompletionWithoutExpiresAt() throws Exception {
    // Create workflow request without access information
    ActionRequestInfo actionRequestInfo = createTestWorkflowActionRequestInfo();
    // Don't add access information

    when(mockActionWorkflowService.getActionRequestInfo(
            any(OperationContext.class), eq(TEST_ACTION_REQUEST_URN)))
        .thenReturn(actionRequestInfo);

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
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestStatus",
            fromAspect,
            toAspect,
            auditStamp);

    // Verify event
    assertEquals(changeEvents.size(), 1);
    ChangeEvent event = changeEvents.get(0);

    // Verify expiresAtMs parameter is not included
    Map<String, Object> parameters = event.getParameters();
    assertFalse(parameters.containsKey("expiresAtMs"));
    assertEquals(parameters.get("actionRequestType"), ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    assertEquals(parameters.get("result"), ACTION_REQUEST_RESULT_ACCEPTED);
  }

  @Test
  public void testWorkflowCompletionWithAccessButNoExpiresAt() throws Exception {
    // Create workflow request with access information but no expiresAt
    ActionRequestInfo actionRequestInfo = createTestWorkflowActionRequestInfo();

    // Add access information without expiresAt
    ActionWorkflowRequestAccess accessInfo = new ActionWorkflowRequestAccess();
    // Don't set expiresAt
    actionRequestInfo.getParams().getWorkflowFormRequest().setAccess(accessInfo);

    when(mockActionWorkflowService.getActionRequestInfo(
            any(OperationContext.class), eq(TEST_ACTION_REQUEST_URN)))
        .thenReturn(actionRequestInfo);

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
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestStatus",
            fromAspect,
            toAspect,
            auditStamp);

    // Verify event
    assertEquals(changeEvents.size(), 1);
    ChangeEvent event = changeEvents.get(0);

    // Verify expiresAtMs parameter is not included when expiresAt is not set
    Map<String, Object> parameters = event.getParameters();
    assertFalse(parameters.containsKey("expiresAtMs"));
    assertEquals(parameters.get("actionRequestType"), ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    assertEquals(parameters.get("result"), ACTION_REQUEST_RESULT_ACCEPTED);
  }

  @Test
  public void testWorkflowCompletionWithEntityContext() throws Exception {
    // Create workflow request with resource URN
    ActionRequestInfo actionRequestInfo = createTestWorkflowActionRequestInfo();
    actionRequestInfo.setResource(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)");
    actionRequestInfo.setResourceType("dataset");

    when(mockActionWorkflowService.getActionRequestInfo(
            any(OperationContext.class), eq(TEST_ACTION_REQUEST_URN)))
        .thenReturn(actionRequestInfo);

    // Mock EntityNameProvider responses for dataset URN
    Urn datasetUrn =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)");

    // Setup EntityNameProvider mocks using reflection or by directly mocking the methods
    // Note: Since EntityNameProvider is created internally, we need to mock SystemEntityClient
    // responses
    // that EntityNameProvider would use

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
        generator.getChangeEvents(
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

    // Verify entity context parameters
    Map<String, Object> parameters = event.getParameters();
    assertEquals(parameters.get("entityUrn"), datasetUrn.toString());
    assertEquals(parameters.get("entityType"), "dataset");
    assertEquals(parameters.get("actionRequestType"), ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    assertEquals(parameters.get("result"), ACTION_REQUEST_RESULT_ACCEPTED);

    // The entityName, qualifiedEntityName, and entityPlatformName will be present
    // if EntityNameProvider returns non-null values (this depends on the mock setup)
    assertTrue(parameters.containsKey("entityUrn"));
  }

  @Test
  public void testWorkflowCompletionWithoutResource() throws Exception {
    // Create workflow request without resource URN
    ActionRequestInfo actionRequestInfo = createTestWorkflowActionRequestInfo();
    // Don't set resource

    when(mockActionWorkflowService.getActionRequestInfo(
            any(OperationContext.class), eq(TEST_ACTION_REQUEST_URN)))
        .thenReturn(actionRequestInfo);

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
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestStatus",
            fromAspect,
            toAspect,
            auditStamp);

    // Verify event
    assertEquals(changeEvents.size(), 1);
    ChangeEvent event = changeEvents.get(0);

    // Verify that no entity context parameters are present when resource is not set
    Map<String, Object> parameters = event.getParameters();
    assertFalse(parameters.containsKey("entityUrn"));
    assertFalse(parameters.containsKey("entityName"));
    assertFalse(parameters.containsKey("entityType"));
    assertFalse(parameters.containsKey("qualifiedEntityName"));
    assertFalse(parameters.containsKey("entityPlatformName"));

    // But other parameters should still be present
    assertEquals(parameters.get("actionRequestType"), ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    assertEquals(parameters.get("result"), ACTION_REQUEST_RESULT_ACCEPTED);
  }

  @Test
  public void testWorkflowCompletionWithInvalidResource() throws Exception {
    // Create workflow request with invalid resource URN
    ActionRequestInfo actionRequestInfo = createTestWorkflowActionRequestInfo();
    actionRequestInfo.setResource("invalid-urn-format");
    actionRequestInfo.setResourceType("dataset");

    when(mockActionWorkflowService.getActionRequestInfo(
            any(OperationContext.class), eq(TEST_ACTION_REQUEST_URN)))
        .thenReturn(actionRequestInfo);

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
        generator.getChangeEvents(
            TEST_ACTION_REQUEST_URN,
            "actionRequest",
            "actionRequestStatus",
            fromAspect,
            toAspect,
            auditStamp);

    // Verify event is still generated but without entity context
    assertEquals(changeEvents.size(), 1);
    ChangeEvent event = changeEvents.get(0);

    // Verify that no entity context parameters are present when resource URN is invalid
    Map<String, Object> parameters = event.getParameters();
    assertFalse(parameters.containsKey("entityUrn"));
    assertFalse(parameters.containsKey("entityName"));
    assertFalse(parameters.containsKey("entityType"));
    assertFalse(parameters.containsKey("qualifiedEntityName"));
    assertFalse(parameters.containsKey("entityPlatformName"));

    // But other parameters should still be present
    assertEquals(parameters.get("actionRequestType"), ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    assertEquals(parameters.get("result"), ACTION_REQUEST_RESULT_ACCEPTED);
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
