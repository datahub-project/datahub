package com.datahub.notification;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Set;
import org.testng.annotations.Test;

@Test
public class NotificationTemplateTypeTest {

  @Test
  public void testBroadcastNewActionWorkflowRequestTemplateType() {
    // Test the new workflow request template type
    NotificationTemplateType templateType =
        NotificationTemplateType.BROADCAST_NEW_ACTION_WORKFLOW_FORM_REQUEST;

    // Verify required parameters
    Set<String> requiredParams = templateType.getRequiredParameters();
    assertEquals(requiredParams.size(), 4);
    assertTrue(requiredParams.contains("workflowName"));
    assertTrue(requiredParams.contains("workflowUrn"));
    assertTrue(requiredParams.contains("actorUrn"));
    assertTrue(requiredParams.contains("actorName"));

    // Verify optional parameters
    Set<String> optionalParams = templateType.getOptionalFields();
    assertEquals(optionalParams.size(), 7);
    assertTrue(optionalParams.contains("entityName"));
    assertTrue(optionalParams.contains("entityType"));
    assertTrue(optionalParams.contains("entityPath"));
    assertTrue(optionalParams.contains("entityPlatform"));
    assertTrue(optionalParams.contains("workflowType"));
    assertTrue(optionalParams.contains("customWorkflowType"));
    assertTrue(optionalParams.contains("fields"));
  }

  @Test
  public void testBroadcastActionWorkflowRequestStatusChangeTemplateType() {
    // Test the workflow request status change template type
    NotificationTemplateType templateType =
        NotificationTemplateType.BROADCAST_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE;

    // Verify required parameters
    Set<String> requiredParams = templateType.getRequiredParameters();
    assertEquals(requiredParams.size(), 7);
    assertTrue(requiredParams.contains("workflowName"));
    assertTrue(requiredParams.contains("workflowUrn"));
    assertTrue(requiredParams.contains("actorUrn"));
    assertTrue(requiredParams.contains("actorName"));
    assertTrue(requiredParams.contains("creatorUrn"));
    assertTrue(requiredParams.contains("creatorName"));
    assertTrue(requiredParams.contains("result"));

    // Verify optional parameters
    Set<String> optionalParams = templateType.getOptionalFields();
    assertEquals(optionalParams.size(), 8);
    assertTrue(optionalParams.contains("entityName"));
    assertTrue(optionalParams.contains("entityType"));
    assertTrue(optionalParams.contains("entityPath"));
    assertTrue(optionalParams.contains("entityPlatform"));
    assertTrue(optionalParams.contains("workflowType"));
    assertTrue(optionalParams.contains("customWorkflowType"));
    assertTrue(optionalParams.contains("fields"));
    assertTrue(optionalParams.contains("note"));
  }

  @Test
  public void testWorkflowRequestTemplateTypesExist() {
    // Verify that the workflow request template types exist in the enum
    boolean foundNewWorkflowRequest = false;
    boolean foundStatusChange = false;

    for (NotificationTemplateType templateType : NotificationTemplateType.values()) {
      if (templateType == NotificationTemplateType.BROADCAST_NEW_ACTION_WORKFLOW_FORM_REQUEST) {
        foundNewWorkflowRequest = true;
      }
      if (templateType
          == NotificationTemplateType.BROADCAST_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE) {
        foundStatusChange = true;
      }
    }

    assertTrue(
        foundNewWorkflowRequest,
        "BROADCAST_NEW_ACTION_WORKFLOW_FORM_REQUEST template type should exist");
    assertTrue(
        foundStatusChange,
        "BROADCAST_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE template type should exist");
  }

  @Test
  public void testAllTemplateTypesHaveRequiredParameters() {
    // Test that all template types have some required parameters (except CUSTOM)
    for (NotificationTemplateType templateType : NotificationTemplateType.values()) {
      if (templateType != NotificationTemplateType.CUSTOM) {
        assertTrue(
            templateType.getRequiredParameters().size() > 0,
            "Template type " + templateType.name() + " should have required parameters");
      }
    }
  }

  @Test
  public void testWorkflowRequestTemplateParameterConsistency() {
    // Test that workflow request template types have consistent parameter naming
    NotificationTemplateType newRequest =
        NotificationTemplateType.BROADCAST_NEW_ACTION_WORKFLOW_FORM_REQUEST;
    NotificationTemplateType statusChange =
        NotificationTemplateType.BROADCAST_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE;

    // Both should have common required parameters
    Set<String> newRequestRequired = newRequest.getRequiredParameters();
    Set<String> statusChangeRequired = statusChange.getRequiredParameters();

    assertTrue(newRequestRequired.contains("workflowName"));
    assertTrue(newRequestRequired.contains("workflowUrn"));
    assertTrue(newRequestRequired.contains("actorUrn"));
    assertTrue(newRequestRequired.contains("actorName"));

    assertTrue(statusChangeRequired.contains("workflowName"));
    assertTrue(statusChangeRequired.contains("workflowUrn"));
    assertTrue(statusChangeRequired.contains("actorUrn"));
    assertTrue(statusChangeRequired.contains("actorName"));

    // Status change should have additional required parameters
    assertTrue(statusChangeRequired.contains("creatorUrn"));
    assertTrue(statusChangeRequired.contains("creatorName"));
    assertTrue(statusChangeRequired.contains("result"));

    // Both should have common optional parameters (excluding the additional "note" parameter in
    // status change)
    Set<String> newRequestOptional = newRequest.getOptionalFields();
    Set<String> statusChangeOptional = statusChange.getOptionalFields();

    // Check that all parameters from newRequest are in statusChange
    for (String param : newRequestOptional) {
      assertTrue(
          statusChangeOptional.contains(param),
          "Status change template should contain optional parameter: " + param);
    }

    // Status change should have one additional optional parameter: "note"
    assertTrue(statusChangeOptional.contains("note"));
    assertEquals(
        statusChangeOptional.size(),
        newRequestOptional.size() + 1,
        "Status change template should have one additional optional parameter compared to new request template");
  }

  @Test
  public void testWorkflowRequestTemplateParameterTypes() {
    // Verify that the workflow request template types follow the expected parameter patterns
    NotificationTemplateType newRequest =
        NotificationTemplateType.BROADCAST_NEW_ACTION_WORKFLOW_FORM_REQUEST;
    NotificationTemplateType statusChange =
        NotificationTemplateType.BROADCAST_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE;

    // Check that workflow-specific parameters are included
    assertTrue(newRequest.getRequiredParameters().contains("workflowName"));
    assertTrue(newRequest.getRequiredParameters().contains("workflowUrn"));
    assertTrue(newRequest.getOptionalFields().contains("workflowType"));
    assertTrue(newRequest.getOptionalFields().contains("customWorkflowType"));

    assertTrue(statusChange.getRequiredParameters().contains("workflowName"));
    assertTrue(statusChange.getRequiredParameters().contains("workflowUrn"));
    assertTrue(statusChange.getOptionalFields().contains("workflowType"));
    assertTrue(statusChange.getOptionalFields().contains("customWorkflowType"));

    // Check that entity-specific parameters are optional
    assertTrue(newRequest.getOptionalFields().contains("entityName"));
    assertTrue(newRequest.getOptionalFields().contains("entityType"));
    assertTrue(newRequest.getOptionalFields().contains("entityPath"));
    assertTrue(newRequest.getOptionalFields().contains("entityPlatform"));

    assertTrue(statusChange.getOptionalFields().contains("entityName"));
    assertTrue(statusChange.getOptionalFields().contains("entityType"));
    assertTrue(statusChange.getOptionalFields().contains("entityPath"));
    assertTrue(statusChange.getOptionalFields().contains("entityPlatform"));

    // Check that fields parameter is optional
    assertTrue(newRequest.getOptionalFields().contains("fields"));
    assertTrue(statusChange.getOptionalFields().contains("fields"));
  }

  @Test
  public void testSupportLoginTemplateType() {
    // Test the support login template type
    NotificationTemplateType templateType = NotificationTemplateType.SUPPORT_LOGIN;

    // Verify required parameters
    Set<String> requiredParams = templateType.getRequiredParameters();
    assertEquals(requiredParams.size(), 3);
    assertTrue(requiredParams.contains("actorUrn"));
    assertTrue(requiredParams.contains("actorName"));
    assertTrue(requiredParams.contains("timestamp"));

    // Verify optional parameters
    Set<String> optionalParams = templateType.getOptionalFields();
    assertEquals(optionalParams.size(), 3);
    assertTrue(optionalParams.contains("supportTicketId"));
    assertTrue(optionalParams.contains("sourceIP"));
    assertTrue(optionalParams.contains("userAgent"));
  }
}
