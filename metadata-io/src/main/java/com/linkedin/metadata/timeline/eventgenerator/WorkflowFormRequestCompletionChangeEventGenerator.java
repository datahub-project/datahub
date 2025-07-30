package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_COMPLETE;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.actionworkflow.ActionWorkflowFormRequest;
import com.linkedin.actionworkflow.ActionWorkflowFormRequestField;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.service.ActionWorkflowService;
import com.linkedin.metadata.service.UserService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.structured.PrimitivePropertyValue;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Specialized change event generator for workflow request ActionRequestStatus changes. Handles
 * workflow-specific logic and returns empty lists for non-workflow requests.
 */
@Slf4j
public class WorkflowFormRequestCompletionChangeEventGenerator
    extends EntityChangeEventGenerator<ActionRequestStatus> {

  private final UserService userService;
  private final OperationContext systemOperationContext;
  private final ActionWorkflowService actionWorkflowService;

  public WorkflowFormRequestCompletionChangeEventGenerator(
      @Nonnull final UserService userService,
      @Nonnull final OperationContext systemOperationContext,
      @Nonnull final ActionWorkflowService actionWorkflowService) {
    this.userService = userService;
    this.systemOperationContext = systemOperationContext;
    this.actionWorkflowService = actionWorkflowService;
  }

  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<ActionRequestStatus> from,
      @Nonnull Aspect<ActionRequestStatus> to,
      @Nonnull AuditStamp auditStamp) {

    // Check if this is a workflow request by fetching the ActionRequestInfo
    ActionRequestInfo actionRequestInfo = getActionRequestInfo(urn);

    if (actionRequestInfo != null
        && ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST.equals(actionRequestInfo.getType())) {
      return computeWorkflowRequestStatusDiffs(
          from.getValue(), to.getValue(), urn.toString(), auditStamp, actionRequestInfo);
    }

    // Return empty list for non-workflow requests so they can be handled by the delegating
    // generator
    return Collections.emptyList();
  }

  private List<ChangeEvent> computeWorkflowRequestStatusDiffs(
      @Nullable final ActionRequestStatus previousAspect,
      @Nonnull final ActionRequestStatus newAspect,
      @Nonnull final String entityUrn,
      @Nonnull final AuditStamp auditStamp,
      @Nonnull final ActionRequestInfo actionRequestInfo) {

    if (newAspect == null) {
      return Collections.emptyList();
    }

    if (previousAspect != null && previousAspect.getStatus().equals(newAspect.getStatus())) {
      return Collections.emptyList();
    }

    // Only emit events when workflow is completed
    if (!ACTION_REQUEST_STATUS_COMPLETE.equals(newAspect.getStatus())) {
      return Collections.emptyList();
    }

    return Collections.singletonList(
        ChangeEvent.builder()
            .category(ChangeCategory.LIFECYCLE)
            .operation(ChangeOperation.COMPLETED)
            .auditStamp(auditStamp)
            .entityUrn(entityUrn)
            .parameters(buildWorkflowRequestStatusParameters(newAspect, actionRequestInfo))
            .build());
  }

  @Nonnull
  private Map<String, Object> buildWorkflowRequestStatusParameters(
      @Nonnull final ActionRequestStatus actionRequestStatus,
      @Nonnull final ActionRequestInfo actionRequestInfo) {

    Map<String, Object> parameters = new HashMap<>();

    // Add the action request type
    parameters.put("actionRequestType", actionRequestInfo.getType().toString());

    // Override the operation to indicate completion
    parameters.put("operation", "COMPLETE");

    // Add result information
    if (actionRequestStatus.hasResult()) {
      parameters.put("result", actionRequestStatus.getResult());
    }

    // Extract workflow request information from ActionRequestInfo
    if (actionRequestInfo.hasParams() && actionRequestInfo.getParams().hasWorkflowFormRequest()) {
      ActionWorkflowFormRequest workflowRequest =
          actionRequestInfo.getParams().getWorkflowFormRequest();

      // Add workflow information
      if (workflowRequest.hasWorkflow()) {
        parameters.put("workflowUrn", workflowRequest.getWorkflow().toString());
        // Extract workflow ID from URN (last part after colon)
        String workflowId = workflowRequest.getWorkflow().getId();
        parameters.put("workflowId", workflowId);
      }

      if (workflowRequest.hasAccess() && workflowRequest.getAccess().hasExpiresAt()) {
        parameters.put("expiresAtMs", workflowRequest.getAccess().getExpiresAt());
      }

      // Add workflow request fields
      if (workflowRequest.hasFields()) {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode fieldsNode = objectMapper.createObjectNode();

        for (ActionWorkflowFormRequestField field : workflowRequest.getFields()) {
          if (field.hasValues() && !field.getValues().isEmpty()) {
            // Convert field values to string array
            String[] values =
                field.getValues().stream()
                    .map(this::convertPrimitiveValueToString)
                    .toArray(String[]::new);
            fieldsNode.put(field.getId(), objectMapper.valueToTree(values));
          }
        }

        parameters.put("fields", fieldsNode.toString());
      }
    }

    // Add actor information
    if (actionRequestInfo.hasCreatedBy()) {
      Urn actorUrn = actionRequestInfo.getCreatedBy();
      parameters.put("actorUrn", actorUrn.toString());

      // Resolve actor email using UserService
      try {
        String email = userService.getUserEmail(systemOperationContext, actorUrn);
        if (email != null) {
          parameters.put("actorEmail", email);
        }
      } catch (Exception e) {
        log.warn("Failed to resolve actor email for urn: {}", actorUrn, e);
      }
    }

    return parameters;
  }

  @Nullable
  private ActionRequestInfo getActionRequestInfo(@Nonnull final Urn actionRequestUrn) {
    try {
      // Use ActionWorkflowService to get the ActionRequestInfo
      return actionWorkflowService.getActionRequestInfo(systemOperationContext, actionRequestUrn);
    } catch (Exception e) {
      log.warn("Failed to fetch ActionRequestInfo for urn: {}", actionRequestUrn, e);
      return null;
    }
  }

  private String convertPrimitiveValueToString(PrimitivePropertyValue value) {
    if (value.isString()) {
      return value.getString();
    } else if (value.isDouble()) {
      return String.valueOf(value.getDouble());
    }
    return value.toString();
  }
}
