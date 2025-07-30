package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionworkflow.ActionWorkflowFormRequest;
import com.linkedin.actionworkflow.ActionWorkflowFormRequestField;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
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
 * Specialized change event generator for workflow request ActionRequestInfo changes. Handles
 * workflow-specific logic and returns empty lists for non-workflow requests.
 */
@Slf4j
public class WorkflowFormRequestStepCompletionChangeEventGenerator
    extends EntityChangeEventGenerator<ActionRequestInfo> {

  private final UserService userService;
  private final OperationContext systemOperationContext;

  public WorkflowFormRequestStepCompletionChangeEventGenerator(
      @Nonnull final UserService userService,
      @Nonnull final OperationContext systemOperationContext) {
    this.userService = userService;
    this.systemOperationContext = systemOperationContext;
  }

  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<ActionRequestInfo> from,
      @Nonnull Aspect<ActionRequestInfo> to,
      @Nonnull AuditStamp auditStamp) {

    // Get the current ActionRequestInfo
    ActionRequestInfo toActionRequestInfo = to.getValue();

    // Check if this is a workflow request
    if (toActionRequestInfo != null
        && ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST.equals(toActionRequestInfo.getType())) {
      return computeWorkflowRequestDiffs(
          from.getValue(), toActionRequestInfo, urn.toString(), auditStamp);
    }

    // Return empty list for non-workflow requests so they can be handled by the delegating
    // generator
    return Collections.emptyList();
  }

  private List<ChangeEvent> computeWorkflowRequestDiffs(
      @Nullable final ActionRequestInfo previousAspect,
      @Nonnull final ActionRequestInfo newAspect,
      @Nonnull final String entityUrn,
      @Nonnull final AuditStamp auditStamp) {

    if (previousAspect != null && previousAspect.equals(newAspect)) {
      return Collections.emptyList();
    }

    // Determine the operation type
    ChangeOperation operation = determineOperation(previousAspect, newAspect);

    return Collections.singletonList(
        ChangeEvent.builder()
            .category(ChangeCategory.LIFECYCLE)
            .operation(operation)
            .auditStamp(auditStamp)
            .entityUrn(entityUrn)
            .parameters(buildWorkflowRequestParameters(newAspect, operation, previousAspect))
            .build());
  }

  private ChangeOperation determineOperation(
      @Nullable final ActionRequestInfo previousAspect,
      @Nonnull final ActionRequestInfo newAspect) {

    if (previousAspect == null) {
      return ChangeOperation.CREATE;
    }

    // Check if this is a step completion by comparing step states
    if (isStepCompletion(previousAspect, newAspect)) {
      return ChangeOperation.MODIFY; // Use MODIFY for step completion
    }

    return ChangeOperation.MODIFY;
  }

  private boolean isStepCompletion(
      @Nonnull final ActionRequestInfo previousAspect, @Nonnull final ActionRequestInfo newAspect) {

    if (previousAspect.hasParams()
        && newAspect.hasParams()
        && previousAspect.getParams().hasWorkflowFormRequest()
        && newAspect.getParams().hasWorkflowFormRequest()) {

      ActionWorkflowFormRequest prevWorkflowRequest =
          previousAspect.getParams().getWorkflowFormRequest();
      ActionWorkflowFormRequest newWorkflowRequest = newAspect.getParams().getWorkflowFormRequest();

      if (prevWorkflowRequest.hasStepState() && newWorkflowRequest.hasStepState()) {
        String prevStepId = prevWorkflowRequest.getStepState().getStepId();
        String newStepId = newWorkflowRequest.getStepState().getStepId();

        return !prevStepId.equals(newStepId);
      }
    }

    return false;
  }

  @Nonnull
  private Map<String, Object> buildWorkflowRequestParameters(
      @Nonnull final ActionRequestInfo actionRequestInfo,
      @Nonnull final ChangeOperation operation,
      @Nullable final ActionRequestInfo previousAspect) {

    Map<String, Object> parameters = new HashMap<>();

    // Add the action request type
    parameters.put("actionRequestType", actionRequestInfo.getType().toString());

    // Extract workflow request information
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

      // Check if this is a step completion and add step-specific parameters
      if (operation == ChangeOperation.MODIFY
          && previousAspect != null
          && isStepCompletion(previousAspect, actionRequestInfo)) {
        if (workflowRequest.hasStepState()) {
          ActionWorkflowFormRequest prevWorkflowRequest =
              previousAspect.getParams().getWorkflowFormRequest();
          parameters.put("stepId", prevWorkflowRequest.getStepState().getStepId());
          // If we've gotten to this point, the step is accepted. Otherwise we'd not be in this
          // generator,
          // and request would be denied.
          parameters.put("stepResult", ACTION_REQUEST_RESULT_ACCEPTED);
        }
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

  private String convertPrimitiveValueToString(PrimitivePropertyValue value) {
    if (value.isString()) {
      return value.getString();
    } else if (value.isDouble()) {
      return String.valueOf(value.getDouble());
    }
    return value.toString();
  }
}
