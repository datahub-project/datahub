package com.linkedin.metadata.timeline.eventgenerator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.*;

import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestParams;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.actionworkflow.ActionWorkflowFormRequest;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.ActionWorkflowService;
import com.linkedin.metadata.service.UserService;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.List;
import org.testng.annotations.Test;

public class ActionRequestStatusChangeEventGeneratorTest {

  @Test
  public void testPendingStatus() throws Exception {
    ActionRequestStatusChangeEventGenerator generator =
        new ActionRequestStatusChangeEventGenerator();

    Urn urn = Urn.createFromString("urn:li:actionRequest:test");
    String entity = "actionRequest";
    String aspect = "actionRequestStatus";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    ActionRequestStatus status = new ActionRequestStatus();
    status.setStatus("PENDING");
    status.setLastModified(auditStamp);

    Aspect<ActionRequestStatus> from = new Aspect<>(null, new SystemMetadata());
    Aspect<ActionRequestStatus> to = new Aspect<>(status, new SystemMetadata());

    List<ChangeEvent> events = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(1, events.size());
    ChangeEvent event = events.get(0);
    assertEquals(ChangeOperation.PENDING, event.getOperation());
    assertEquals("PENDING", event.getParameters().get("actionRequestStatus"));
  }

  @Test
  public void testWorkflowFormDelegation() throws Exception {
    // Mock dependencies
    UserService mockUserService = mock(UserService.class);
    OperationContext mockOperationContext = mock(OperationContext.class);
    ActionWorkflowService mockWorkflowService = mock(ActionWorkflowService.class);
    SystemEntityClient mockSystemEntityClient = mock(SystemEntityClient.class);

    when(mockUserService.getUserEmail(any(OperationContext.class), any(Urn.class)))
        .thenReturn("test@example.com");

    // Create generator with workflow dependencies
    ActionRequestStatusChangeEventGenerator generator =
        new ActionRequestStatusChangeEventGenerator(
            mockUserService, mockOperationContext, mockWorkflowService, mockSystemEntityClient);

    Urn urn = Urn.createFromString("urn:li:actionRequest:test");
    String entity = "actionRequest";
    String aspect = "actionRequestStatus";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:test"))
            .setTime(1683829509553L);

    // Create workflow request info that the workflow service will return
    ActionRequestInfo workflowRequestInfo = createWorkflowFormActionRequestInfo();
    when(mockWorkflowService.getActionRequestInfo(any(OperationContext.class), eq(urn)))
        .thenReturn(workflowRequestInfo);

    // Create status change to "COMPLETED"
    ActionRequestStatus status = new ActionRequestStatus();
    status.setStatus("COMPLETED");
    status.setResult("ACCEPTED");
    status.setLastModified(auditStamp);

    Aspect<ActionRequestStatus> from = new Aspect<>(null, new SystemMetadata());
    Aspect<ActionRequestStatus> to = new Aspect<>(status, new SystemMetadata());

    List<ChangeEvent> events = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    // Should get workflow-specific event (COMPLETED operation)
    assertEquals(1, events.size());
    ChangeEvent event = events.get(0);
    assertEquals(ChangeOperation.COMPLETED, event.getOperation());
    assertTrue(event.getParameters().containsKey("workflowId"));
    assertTrue(event.getParameters().containsKey("actorEmail"));
    assertEquals("test@example.com", event.getParameters().get("actorEmail"));
  }

  @Test
  public void testNonWorkflowFormDelegation() throws Exception {
    // Mock dependencies
    UserService mockUserService = mock(UserService.class);
    OperationContext mockOperationContext = mock(OperationContext.class);
    ActionWorkflowService mockWorkflowService = mock(ActionWorkflowService.class);
    SystemEntityClient mockSystemEntityClient = mock(SystemEntityClient.class);

    // Create generator with workflow dependencies
    ActionRequestStatusChangeEventGenerator generator =
        new ActionRequestStatusChangeEventGenerator(
            mockUserService, mockOperationContext, mockWorkflowService, mockSystemEntityClient);

    Urn urn = Urn.createFromString("urn:li:actionRequest:test");
    String entity = "actionRequest";
    String aspect = "actionRequestStatus";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:test"))
            .setTime(1683829509553L);

    // Return null to indicate this is not a workflow request
    when(mockWorkflowService.getActionRequestInfo(any(OperationContext.class), eq(urn)))
        .thenReturn(null);

    // Create status change to "PENDING" (this should work with original logic)
    ActionRequestStatus status = new ActionRequestStatus();
    status.setStatus("PENDING");
    status.setLastModified(auditStamp);

    Aspect<ActionRequestStatus> from = new Aspect<>(null, new SystemMetadata());
    Aspect<ActionRequestStatus> to = new Aspect<>(status, new SystemMetadata());

    List<ChangeEvent> events = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    // Should get original logic event (PENDING -> CREATE)
    assertEquals(1, events.size());
    ChangeEvent event = events.get(0);
    assertEquals(ChangeOperation.PENDING, event.getOperation());
    assertEquals("PENDING", event.getParameters().get("actionRequestStatus"));
  }

  private ActionRequestInfo createWorkflowFormActionRequestInfo() throws URISyntaxException {
    ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType("WORKFLOW_FORM_REQUEST");
    // Use the existing creator method pattern but for workflow
    actionRequestInfo.setCreatedBy(Urn.createFromString("urn:li:corpuser:test"));
    actionRequestInfo.setCreated(1683829509553L);

    ActionRequestParams params = new ActionRequestParams();
    ActionWorkflowFormRequest workflowRequest = new ActionWorkflowFormRequest();
    // Set the workflow field using the existing Urn pattern
    workflowRequest.setWorkflow(Urn.createFromString("urn:li:actionWorkflow:test-workflow"));

    params.setWorkflowFormRequest(workflowRequest);
    actionRequestInfo.setParams(params);

    return actionRequestInfo;
  }
}
