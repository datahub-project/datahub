package com.linkedin.metadata.kafka.hook.notification.workflowrequest;

import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_COMPLETE;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST;
import static com.linkedin.metadata.Constants.ACTION_REQUEST_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.ACTION_REQUEST_STATUS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.notification.NotificationScenarioType;
import com.datahub.notification.recipient.NotificationRecipientBuilder;
import com.datahub.notification.recipient.NotificationRecipientBuilders;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestParams;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.actionworkflow.ActionWorkflowCategory;
import com.linkedin.actionworkflow.ActionWorkflowEntrypointArray;
import com.linkedin.actionworkflow.ActionWorkflowFieldArray;
import com.linkedin.actionworkflow.ActionWorkflowForm;
import com.linkedin.actionworkflow.ActionWorkflowFormRequest;
import com.linkedin.actionworkflow.ActionWorkflowFormRequestField;
import com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray;
import com.linkedin.actionworkflow.ActionWorkflowInfo;
import com.linkedin.actionworkflow.ActionWorkflowRequestStepState;
import com.linkedin.actionworkflow.ActionWorkflowStepArray;
import com.linkedin.actionworkflow.ActionWorkflowTrigger;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.service.ActionWorkflowService;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.service.UserService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.Collections;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class WorkflowRequestNotificationGeneratorTest {

  private static final String WORKFLOW_REQUEST_URN = "urn:li:actionRequest:test-workflow-request";
  private static final String WORKFLOW_URN = "urn:li:actionWorkflow:test-workflow";
  private static final String ENTITY_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,test,PROD)";
  private static final String ACTOR_URN = "urn:li:corpuser:test-actor";
  private static final String CREATOR_URN = "urn:li:corpuser:test-creator";
  private static final String USER1_URN = "urn:li:corpuser:user1";
  private static final String GROUP_URN = "urn:li:corpGroup:test-group";
  private static final String ROLE_URN = "urn:li:dataHubRole:test-role";

  private WorkflowRequestNotificationGenerator _generator;
  private OperationContext _operationContext;
  private EventProducer _eventProducer;
  private SystemEntityClient _entityClient;
  private GraphClient _graphClient;
  private SettingsService _settingsService;
  private NotificationRecipientBuilders _recipientBuilders;
  private ActionWorkflowService _actionWorkflowService;
  private UserService _userService;

  @BeforeMethod
  public void setup() {
    _operationContext = mock(OperationContext.class);
    _eventProducer = mock(EventProducer.class);
    _entityClient = mock(SystemEntityClient.class);
    _graphClient = mock(GraphClient.class);
    _settingsService = mock(SettingsService.class);
    _recipientBuilders = mock(NotificationRecipientBuilders.class);
    _actionWorkflowService = mock(ActionWorkflowService.class);
    _userService = mock(UserService.class);

    // Mock NotificationRecipientBuilder to return at least one recipient
    NotificationRecipientBuilder mockBuilder = mock(NotificationRecipientBuilder.class);
    NotificationRecipient mockRecipient = new NotificationRecipient();
    mockRecipient.setId("test-user@example.com");
    when(mockBuilder.buildActorRecipients(any(), any(), any()))
        .thenReturn(Arrays.asList(mockRecipient));
    when(_recipientBuilders.listBuilders()).thenReturn(Arrays.asList(mockBuilder));

    _generator =
        new WorkflowRequestNotificationGenerator(
            _operationContext,
            _eventProducer,
            _entityClient,
            _graphClient,
            _settingsService,
            _recipientBuilders,
            _actionWorkflowService,
            _userService);
  }

  @Test
  public void testIsEligibleForCustomRecipients() {
    assertTrue(
        _generator.isEligibleForCustomRecipients(
            NotificationScenarioType.NEW_ACTION_WORKFLOW_FORM_REQUEST));
    assertTrue(
        _generator.isEligibleForCustomRecipients(
            NotificationScenarioType.REQUESTER_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE));
    assertFalse(_generator.isEligibleForCustomRecipients(NotificationScenarioType.NEW_PROPOSAL));
  }

  @Test
  public void testIsEligibleForProcessingActionRequestInfo() {
    // Test eligible workflow request
    ActionRequestInfo workflowRequestInfo = new ActionRequestInfo();
    workflowRequestInfo.setType(ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    assertTrue(_generator.isEligibleForProcessingActionRequestInfo(workflowRequestInfo));

    // Test non-workflow request
    ActionRequestInfo otherRequestInfo = new ActionRequestInfo();
    otherRequestInfo.setType("TAG_PROPOSAL");
    assertFalse(_generator.isEligibleForProcessingActionRequestInfo(otherRequestInfo));
  }

  @Test
  public void testNewWorkflowRequestNotificationIsSent() throws Exception {
    // Mock essential dependencies
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();
    when(_actionWorkflowService.getActionWorkflow(any(), any())).thenReturn(workflowInfo);

    // Mock entity existence check
    when(_entityClient.exists(any(), any(), anyBoolean())).thenReturn(true);

    // Mock UserService to return at least one user so notification gets sent
    when(_userService.resolveGroupUsers(any(), any()))
        .thenReturn(Arrays.asList(UrnUtils.getUrn(USER1_URN)));
    when(_userService.resolveRoleUsers(any(), any())).thenReturn(Collections.emptyList());

    // Create and send event
    ActionRequestInfo actionRequestInfo = createTestActionRequestInfo();
    MetadataChangeLog event = createNewWorkflowRequestEvent(actionRequestInfo);

    _generator.generate(event);

    // Verify notification was sent by checking if EventProducer was called
    verify(_eventProducer, times(1)).producePlatformEvent(anyString(), any(), any());
  }

  @Test
  public void testStepCompletionNotificationIsSent() throws Exception {
    // Mock essential dependencies
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();
    when(_actionWorkflowService.getActionWorkflow(any(), any())).thenReturn(workflowInfo);

    // Mock entity existence check
    when(_entityClient.exists(any(), any(), anyBoolean())).thenReturn(true);

    // Mock UserService to return at least one user so notification gets sent
    when(_userService.resolveGroupUsers(any(), any()))
        .thenReturn(Arrays.asList(UrnUtils.getUrn(USER1_URN)));
    when(_userService.resolveRoleUsers(any(), any())).thenReturn(Collections.emptyList());

    // Create and send step completion event
    ActionRequestInfo actionRequestInfo = createTestActionRequestInfo();
    MetadataChangeLog event = createStepCompletionEvent(actionRequestInfo);

    _generator.generate(event);

    // Verify notification was sent by checking if EventProducer was called
    verify(_eventProducer, times(1)).producePlatformEvent(anyString(), any(), any());
  }

  @Test
  public void testStatusChangeNotificationIsSent() throws Exception {
    // Mock essential dependencies
    ActionRequestInfo actionRequestInfo = createTestActionRequestInfo();
    ActionWorkflowInfo workflowInfo = createTestWorkflowInfo();
    when(_actionWorkflowService.getActionRequestInfo(any(), any())).thenReturn(actionRequestInfo);
    when(_actionWorkflowService.getActionWorkflow(any(), any())).thenReturn(workflowInfo);

    // Mock entity existence check
    when(_entityClient.exists(any(), any(), anyBoolean())).thenReturn(true);

    // Mock UserService to return at least one user so notification gets sent
    when(_userService.resolveGroupUsers(any(), any()))
        .thenReturn(Arrays.asList(UrnUtils.getUrn(USER1_URN)));
    when(_userService.resolveRoleUsers(any(), any())).thenReturn(Collections.emptyList());

    // Create and send status change event
    ActionRequestStatus completeStatus = createTestCompleteStatus();
    MetadataChangeLog event = createStatusChangeEvent(completeStatus);

    _generator.generate(event);

    // Verify notification was sent by checking if EventProducer was called
    verify(_eventProducer, times(1)).producePlatformEvent(anyString(), any(), any());
  }

  // Helper methods to create test data
  private ActionRequestInfo createTestActionRequestInfo() {
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType(ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    info.setAssignedUsers(new UrnArray(Arrays.asList(UrnUtils.getUrn(USER1_URN))));
    info.setAssignedGroups(new UrnArray(Arrays.asList(UrnUtils.getUrn(GROUP_URN))));
    info.setAssignedRoles(new UrnArray(Arrays.asList(UrnUtils.getUrn(ROLE_URN))));
    info.setResource(ENTITY_URN);
    info.setCreatedBy(UrnUtils.getUrn(CREATOR_URN));
    info.setCreated(System.currentTimeMillis());

    // Create workflow request
    ActionWorkflowFormRequest workflowRequest = new ActionWorkflowFormRequest();
    workflowRequest.setWorkflow(UrnUtils.getUrn(WORKFLOW_URN));
    workflowRequest.setCategory(ActionWorkflowCategory.ACCESS);

    // Add step state
    ActionWorkflowRequestStepState stepState = new ActionWorkflowRequestStepState();
    stepState.setStepId("step-1");
    workflowRequest.setStepState(stepState);

    // Add test fields
    ActionWorkflowFormRequestField field = new ActionWorkflowFormRequestField();
    field.setId("test-field");
    PrimitivePropertyValue value = new PrimitivePropertyValue();
    value.setString("test-value");
    field.setValues(new PrimitivePropertyValueArray(Arrays.asList(value)));
    workflowRequest.setFields(new ActionWorkflowFormRequestFieldArray(Arrays.asList(field)));

    ActionRequestParams params = new ActionRequestParams();
    params.setWorkflowFormRequest(workflowRequest);
    info.setParams(params);

    return info;
  }

  private ActionWorkflowInfo createTestWorkflowInfo() {
    ActionWorkflowInfo info = new ActionWorkflowInfo();
    info.setName("Test Workflow");
    info.setCategory(ActionWorkflowCategory.ACCESS);
    info.setDescription("Test workflow description");
    info.setSteps(new ActionWorkflowStepArray());
    info.setTrigger(
        new ActionWorkflowTrigger()
            .setForm(
                new ActionWorkflowForm()
                    .setEntrypoints(new ActionWorkflowEntrypointArray())
                    .setFields(new ActionWorkflowFieldArray())));

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(UrnUtils.getUrn(CREATOR_URN));
    info.setCreated(auditStamp);
    info.setLastModified(auditStamp);

    return info;
  }

  private ActionRequestStatus createTestCompleteStatus() {
    ActionRequestStatus status = new ActionRequestStatus();
    status.setStatus(ACTION_REQUEST_STATUS_COMPLETE);
    status.setResult(ACTION_REQUEST_RESULT_ACCEPTED);
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(UrnUtils.getUrn(ACTOR_URN));
    status.setLastModified(auditStamp);
    return status;
  }

  private MetadataChangeLog createNewWorkflowRequestEvent(ActionRequestInfo actionRequestInfo) {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityUrn(UrnUtils.getUrn(WORKFLOW_REQUEST_URN));
    event.setAspectName(ACTION_REQUEST_INFO_ASPECT_NAME);
    event.setChangeType(ChangeType.CREATE);

    GenericAspect serializedAspect = GenericRecordUtils.serializeAspect(actionRequestInfo);
    event.setAspect(serializedAspect);

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(UrnUtils.getUrn(ACTOR_URN));
    event.setCreated(auditStamp);

    return event;
  }

  private MetadataChangeLog createStepCompletionEvent(ActionRequestInfo actionRequestInfo) {
    ActionRequestInfo previousInfo = createTestActionRequestInfo();
    ActionRequestInfo currentInfo = createTestActionRequestInfo();

    // Change step ID to simulate step completion
    currentInfo.getParams().getWorkflowFormRequest().getStepState().setStepId("step-2");

    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityUrn(UrnUtils.getUrn(WORKFLOW_REQUEST_URN));
    event.setAspectName(ACTION_REQUEST_INFO_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);

    // Set previous and current aspects
    event.setPreviousAspectValue(GenericRecordUtils.serializeAspect(previousInfo));
    event.setAspect(GenericRecordUtils.serializeAspect(currentInfo));

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(UrnUtils.getUrn(ACTOR_URN));
    event.setCreated(auditStamp);

    return event;
  }

  private MetadataChangeLog createStatusChangeEvent(ActionRequestStatus status) {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityUrn(UrnUtils.getUrn(WORKFLOW_REQUEST_URN));
    event.setAspectName(ACTION_REQUEST_STATUS_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);

    // Set a previous status to simulate change
    ActionRequestStatus previousStatus = new ActionRequestStatus();
    previousStatus.setStatus("PENDING");
    AuditStamp previousAuditStamp = new AuditStamp();
    previousAuditStamp.setTime(System.currentTimeMillis() - 1000);
    previousAuditStamp.setActor(UrnUtils.getUrn(ACTOR_URN));
    previousStatus.setLastModified(previousAuditStamp);

    event.setPreviousAspectValue(GenericRecordUtils.serializeAspect(previousStatus));
    event.setAspect(GenericRecordUtils.serializeAspect(status));

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(UrnUtils.getUrn(ACTOR_URN));
    event.setCreated(auditStamp);

    return event;
  }
}
