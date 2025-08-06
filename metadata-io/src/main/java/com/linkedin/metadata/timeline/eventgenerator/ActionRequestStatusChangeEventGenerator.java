package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.AcrylConstants.*;

import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.ActionWorkflowService;
import com.linkedin.metadata.service.UserService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ActionRequestStatusChangeEventGenerator
    extends EntityChangeEventGenerator<ActionRequestStatus> {

  private final WorkflowFormRequestCompletionChangeEventGenerator
      workflowFormRequestCompletionChangeEventGenerator;

  // Default constructor for backward compatibility
  public ActionRequestStatusChangeEventGenerator() {
    this.workflowFormRequestCompletionChangeEventGenerator = null;
  }

  // Constructor with workflow dependencies
  public ActionRequestStatusChangeEventGenerator(
      @Nullable final UserService userService,
      @Nullable final OperationContext systemOperationContext,
      @Nullable final ActionWorkflowService actionWorkflowService,
      @Nullable final SystemEntityClient systemEntityClient) {
    this.workflowFormRequestCompletionChangeEventGenerator =
        (userService != null
                && systemOperationContext != null
                && actionWorkflowService != null
                && systemEntityClient != null)
            ? new WorkflowFormRequestCompletionChangeEventGenerator(
                userService, systemOperationContext, actionWorkflowService, systemEntityClient)
            : null;
  }

  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<ActionRequestStatus> from,
      @Nonnull Aspect<ActionRequestStatus> to,
      @Nonnull AuditStamp auditStamp) {

    // Check if this is a workflow request and delegate to workflow generator
    if (workflowFormRequestCompletionChangeEventGenerator != null) {
      // Let the workflow generator try first - it will return empty if not a workflow request
      List<ChangeEvent> workflowEvents =
          workflowFormRequestCompletionChangeEventGenerator.getChangeEvents(
              urn, entity, aspect, from, to, auditStamp);
      if (!workflowEvents.isEmpty()) {
        return workflowEvents;
      }
    }

    // Handle non-workflow requests with original logic
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }

  private List<ChangeEvent> computeDiffs(
      final ActionRequestStatus previousAspect,
      final ActionRequestStatus newAspect,
      @Nonnull final String entityUrn,
      @Nonnull final AuditStamp auditStamp) {
    if (newAspect == null) {
      return Collections.emptyList();
    }
    if (previousAspect != null && previousAspect.getStatus().equals(newAspect.getStatus())) {
      return Collections.emptyList();
    }

    final ChangeOperation changeOperation = getChangeOperation(newAspect);
    if (changeOperation == null) {
      return Collections.emptyList();
    }

    return Collections.singletonList(
        ChangeEvent.builder()
            .category(ChangeCategory.LIFECYCLE)
            .operation(ChangeOperation.valueOf(newAspect.getStatus()))
            .auditStamp(auditStamp)
            .entityUrn(entityUrn)
            .parameters(buildParameters(newAspect))
            .build());
  }

  @Nullable
  private ChangeOperation getChangeOperation(
      @Nonnull final ActionRequestStatus actionRequestStatus) {
    if (actionRequestStatus.getStatus().equals(ACTION_REQUEST_STATUS_PENDING)) {
      return ChangeOperation.CREATE;
    }
    if (actionRequestStatus.getStatus().equals(ACTION_REQUEST_STATUS_COMPLETE)) {
      return ChangeOperation.MODIFY;
    }
    return null;
  }

  @Nonnull
  private Map<String, Object> buildParameters(
      @Nonnull final ActionRequestStatus actionRequestStatus) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put(ACTION_REQUEST_STATUS_KEY, actionRequestStatus.getStatus());
    if (actionRequestStatus.hasResult()) {
      parameters.put(ACTION_REQUEST_RESULT_KEY, actionRequestStatus.getResult());
    }
    return parameters;
  }
}
