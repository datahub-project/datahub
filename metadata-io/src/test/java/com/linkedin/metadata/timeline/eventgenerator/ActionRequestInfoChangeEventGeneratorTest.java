package com.linkedin.metadata.timeline.eventgenerator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.*;

import com.linkedin.actionrequest.*;
import com.linkedin.actionworkflow.ActionWorkflowFormRequest;
import com.linkedin.common.*;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.UserService;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

public class ActionRequestInfoChangeEventGeneratorTest {

  @Test
  public void testCreateStructuredPropertyProposal() throws Exception {
    ActionRequestInfoChangeEventGenerator test = new ActionRequestInfoChangeEventGenerator();

    Urn urn = Urn.createFromString("urn:li:actionRequest:test");
    String entity = "actionRequest";
    String aspect = "actionRequestInfo";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    ActionRequestInfo actionRequestInfo =
        createStructuredPropertyActionRequestInfo(
            "urn:li:structuredProperty:test", Arrays.asList("test", "testing", "onemore"));

    Aspect<ActionRequestInfo> from = new Aspect<>(null, new SystemMetadata());
    Aspect<ActionRequestInfo> to = new Aspect<>(actionRequestInfo, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(1, actual.size());
    assertEquals("CREATE", actual.get(0).getOperation().toString());
    assertEquals(
        "[{\"propertyUrn\":\"urn:li:structuredProperty:test\",\"values\":[\"test\",\"testing\",\"onemore\"]}]",
        actual.get(0).getParameters().get("structuredProperties"));
  }

  private ActionRequestInfo createStructuredPropertyActionRequestInfo(
      String propertyUrn, List<String> values) throws Exception {
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType("STRUCTURED_PROPERTY_ASSOCIATION");

    StructuredPropertyProposal proposal = new StructuredPropertyProposal();
    StructuredPropertyValueAssignmentArray array = new StructuredPropertyValueAssignmentArray();

    StructuredPropertyValueAssignment assignment = new StructuredPropertyValueAssignment();
    assignment.setPropertyUrn(Urn.createFromString(propertyUrn));

    PrimitivePropertyValueArray propValues = new PrimitivePropertyValueArray();
    for (String value : values) {
      propValues.add(PrimitivePropertyValue.create(value));
    }
    assignment.setValues(propValues);

    array.add(assignment);
    proposal.setStructuredPropertyValues(array);

    ActionRequestParams params = new ActionRequestParams();
    params.setStructuredPropertyProposal(proposal);
    info.setParams(params);

    return info;
  }

  @Test
  public void testCreateDomainProposal() throws Exception {
    ActionRequestInfoChangeEventGenerator test = new ActionRequestInfoChangeEventGenerator();

    Urn urn = Urn.createFromString("urn:li:actionRequest:test");
    String entity = "actionRequest";
    String aspect = "actionRequestInfo";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    ActionRequestInfo actionRequestInfo =
        createDomainActionRequestInfo("urn:li:domain:engineering");

    Aspect<ActionRequestInfo> from = new Aspect<>(null, new SystemMetadata());
    Aspect<ActionRequestInfo> to = new Aspect<>(actionRequestInfo, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(1, actual.size());
    assertEquals("CREATE", actual.get(0).getOperation().toString());
    assertEquals("[\"urn:li:domain:engineering\"]", actual.get(0).getParameters().get("domains"));
  }

  @Test
  public void testCreateOwnerProposal() throws Exception {
    ActionRequestInfoChangeEventGenerator test = new ActionRequestInfoChangeEventGenerator();

    Urn urn = Urn.createFromString("urn:li:actionRequest:test");
    String entity = "actionRequest";
    String aspect = "actionRequestInfo";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    ActionRequestInfo actionRequestInfo =
        createOwnerActionRequestInfo(
            Arrays.asList(
                new Pair<>(
                    "urn:li:corpuser:admin",
                    new Pair<>("TECHNICAL_OWNER", "urn:li:ownershipType:technicalOwner")),
                new Pair<>(
                    "urn:li:corpGroup:my_team",
                    new Pair<>("BUSINESS_OWNER", "urn:li:ownershipType:businessOwner"))));

    Aspect<ActionRequestInfo> from = new Aspect<>(null, new SystemMetadata());
    Aspect<ActionRequestInfo> to = new Aspect<>(actionRequestInfo, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(1, actual.size());
    assertEquals("CREATE", actual.get(0).getOperation().toString());
    assertEquals(
        "[{\"type\":\"TECHNICAL_OWNER\",\"typeUrn\":\"urn:li:ownershipType:technicalOwner\",\"ownerUrn\":\"urn:li:corpuser:admin\"},"
            + "{\"type\":\"BUSINESS_OWNER\",\"typeUrn\":\"urn:li:ownershipType:businessOwner\",\"ownerUrn\":\"urn:li:corpGroup:my_team\"}]",
        actual.get(0).getParameters().get("owners"));
  }

  @Test
  public void testWorkflowFormRequestDelegation() throws Exception {
    // Setup mocks
    UserService mockUserService = mock(UserService.class);
    OperationContext mockOperationContext = mock(OperationContext.class);
    SystemEntityClient mockSystemClient = mock(SystemEntityClient.class);

    // Mock user service to return user email for the test URN
    Urn actorUrn = Urn.createFromString("urn:li:corpuser:test");
    when(mockUserService.getUserEmail(any(OperationContext.class), eq(actorUrn)))
        .thenReturn("john@example.com");

    // Create generator with workflow dependencies
    ActionRequestInfoChangeEventGenerator generator =
        new ActionRequestInfoChangeEventGenerator(
            mockUserService, mockOperationContext, mockSystemClient);

    Urn urn = Urn.createFromString("urn:li:actionRequest:test");
    String entity = "actionRequest";
    String aspect = "actionRequestInfo";
    AuditStamp auditStamp = new AuditStamp().setActor(actorUrn).setTime(1683829509553L);

    // Create workflow request
    ActionRequestInfo workflowRequest = createWorkflowFormRequestInfo();

    Aspect<ActionRequestInfo> from = new Aspect<>(null, new SystemMetadata());
    Aspect<ActionRequestInfo> to = new Aspect<>(workflowRequest, new SystemMetadata());

    // Generate change events
    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    // Verify workflow-specific event was generated
    assertEquals(1, actual.size());
    ChangeEvent event = actual.get(0);
    assertEquals(ChangeOperation.CREATE, event.getOperation());

    // Verify workflow-specific parameters are present
    assertTrue(event.getParameters().containsKey("workflowUrn"));
    assertTrue(event.getParameters().containsKey("workflowId"));
    assertTrue(event.getParameters().containsKey("actorEmail"));
    assertEquals("john@example.com", event.getParameters().get("actorEmail"));
  }

  @Test
  public void testNonWorkflowRequestFallback() throws Exception {
    // Setup mocks
    UserService mockUserService = mock(UserService.class);
    OperationContext mockOperationContext = mock(OperationContext.class);
    SystemEntityClient mockSystemClient = mock(SystemEntityClient.class);

    // Create generator with workflow dependencies
    ActionRequestInfoChangeEventGenerator generator =
        new ActionRequestInfoChangeEventGenerator(
            mockUserService, mockOperationContext, mockSystemClient);

    Urn urn = Urn.createFromString("urn:li:actionRequest:test");
    String entity = "actionRequest";
    String aspect = "actionRequestInfo";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    // Create non-workflow request (domain proposal)
    ActionRequestInfo domainRequest = createDomainActionRequestInfo("urn:li:domain:engineering");

    Aspect<ActionRequestInfo> from = new Aspect<>(null, new SystemMetadata());
    Aspect<ActionRequestInfo> to = new Aspect<>(domainRequest, new SystemMetadata());

    // Generate change events
    List<ChangeEvent> actual = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    // Verify original logic was used (no workflow-specific parameters)
    assertEquals(1, actual.size());
    ChangeEvent event = actual.get(0);
    assertEquals(ChangeOperation.CREATE, event.getOperation());

    // Verify workflow-specific parameters are NOT present
    assertFalse(event.getParameters().containsKey("workflowUrn"));
    assertFalse(event.getParameters().containsKey("workflowId"));
    assertFalse(event.getParameters().containsKey("actorEmail"));

    // Verify domain-specific parameters ARE present (original logic)
    assertTrue(event.getParameters().containsKey("domains"));
  }

  @Test
  public void testWorkflowFormDelegation() throws Exception {
    // Mock dependencies
    UserService mockUserService = mock(UserService.class);
    OperationContext mockOperationContext = mock(OperationContext.class);
    SystemEntityClient mockSystemClient = mock(SystemEntityClient.class);

    when(mockUserService.getUserEmail(any(OperationContext.class), any(Urn.class)))
        .thenReturn("test@example.com");

    // Create generator with workflow dependencies
    ActionRequestInfoChangeEventGenerator generator =
        new ActionRequestInfoChangeEventGenerator(
            mockUserService, mockOperationContext, mockSystemClient);

    Urn urn = Urn.createFromString("urn:li:actionRequest:test");
    String entity = "actionRequest";
    String aspect = "actionRequestInfo";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:test"))
            .setTime(1683829509553L);

    // Create workflow request info
    ActionRequestInfo workflowRequestInfo = createWorkflowFormRequestInfo();

    Aspect<ActionRequestInfo> from = new Aspect<>(null, new SystemMetadata());
    Aspect<ActionRequestInfo> to = new Aspect<>(workflowRequestInfo, new SystemMetadata());

    List<ChangeEvent> events = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    // Should get workflow-specific event (CREATE operation)
    assertEquals(1, events.size());
    ChangeEvent event = events.get(0);
    assertEquals(ChangeOperation.CREATE, event.getOperation());
    assertTrue(event.getParameters().containsKey("workflowId"));
    assertTrue(event.getParameters().containsKey("actorEmail"));
    assertEquals("test@example.com", event.getParameters().get("actorEmail"));
  }

  @Test
  public void testNonWorkflowFormDelegation() throws Exception {
    // Mock dependencies
    UserService mockUserService = mock(UserService.class);
    OperationContext mockOperationContext = mock(OperationContext.class);
    SystemEntityClient mockSystemClient = mock(SystemEntityClient.class);

    // Create generator with workflow dependencies
    ActionRequestInfoChangeEventGenerator generator =
        new ActionRequestInfoChangeEventGenerator(
            mockUserService, mockOperationContext, mockSystemClient);

    Urn urn = Urn.createFromString("urn:li:actionRequest:test");
    String entity = "actionRequest";
    String aspect = "actionRequestInfo";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:test"))
            .setTime(1683829509553L);

    // Create non-workflow request info
    ActionRequestInfo nonWorkflowInfo =
        createStructuredPropertyActionRequestInfo(
            "urn:li:structuredProperty:test", Arrays.asList("test"));

    Aspect<ActionRequestInfo> from = new Aspect<>(null, new SystemMetadata());
    Aspect<ActionRequestInfo> to = new Aspect<>(nonWorkflowInfo, new SystemMetadata());

    List<ChangeEvent> events = generator.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    // Should get original logic event (CREATE operation)
    assertEquals(1, events.size());
    ChangeEvent event = events.get(0);
    assertEquals(ChangeOperation.CREATE, event.getOperation());
    // Should NOT have workflow-specific parameters
    assertFalse(event.getParameters().containsKey("workflowId"));
    assertFalse(event.getParameters().containsKey("actorEmail"));
  }

  private ActionRequestInfo createWorkflowFormRequestInfo() throws Exception {
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

  private ActionRequestInfo createDomainActionRequestInfo(String domainUrn) throws Exception {
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType("DOMAIN_ASSOCIATION");

    DomainProposal proposal = new DomainProposal();
    UrnArray domains = new UrnArray();
    domains.add(Urn.createFromString(domainUrn));
    proposal.setDomains(domains);

    ActionRequestParams params = new ActionRequestParams();
    params.setDomainProposal(proposal);
    info.setParams(params);

    return info;
  }

  private ActionRequestInfo createOwnerActionRequestInfo(
      List<Pair<String, Pair<String, String>>> ownerInfo) throws Exception {
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType("OWNER_ASSOCIATION");

    OwnerProposal proposal = new OwnerProposal();
    OwnerArray owners = new OwnerArray();

    for (Pair<String, Pair<String, String>> pair : ownerInfo) {
      Owner owner = new Owner();
      owner.setOwner(Urn.createFromString(pair.getFirst()));
      owner.setType(Enum.valueOf(OwnershipType.class, pair.getSecond().getFirst()));
      owner.setTypeUrn(Urn.createFromString(pair.getSecond().getSecond()));
      owners.add(owner);
    }

    proposal.setOwners(owners);

    ActionRequestParams params = new ActionRequestParams();
    params.setOwnerProposal(proposal);
    info.setParams(params);

    return info;
  }
}
