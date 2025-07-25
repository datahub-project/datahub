package com.linkedin.metadata.kafka.hook.notification.workflowrequest;

import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_RESULT_REJECTED;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_COMPLETE;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST;
import static com.linkedin.metadata.Constants.ACTION_REQUEST_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.ACTION_REQUEST_STATUS_ASPECT_NAME;
import static com.linkedin.metadata.kafka.hook.notification.NotificationUtils.generateEntityPath;

import com.datahub.notification.NotificationScenarioType;
import com.datahub.notification.NotificationTemplateType;
import com.datahub.notification.recipient.NotificationRecipientBuilders;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.actionworkflow.ActionWorkflowFormRequest;
import com.linkedin.actionworkflow.ActionWorkflowFormRequestField;
import com.linkedin.actionworkflow.ActionWorkflowInfo;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientOriginType;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.kafka.hook.notification.BaseMclNotificationGenerator;
import com.linkedin.metadata.kafka.hook.notification.NotificationRecipientsGeneratorExtraContext;
import com.linkedin.metadata.service.ActionWorkflowService;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.service.UserService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.subscription.EntityChangeType;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * An extension of {@link BaseMclNotificationGenerator} which generates notifications for workflow
 * request creation, step completion, and completion.
 */
@Slf4j
public class WorkflowRequestNotificationGenerator extends BaseMclNotificationGenerator {

  private final ActionWorkflowService _actionWorkflowService;
  private final UserService _userService;
  private final ObjectMapper _objectMapper = new ObjectMapper();

  public WorkflowRequestNotificationGenerator(
      @Nonnull OperationContext systemOpContext,
      @Nonnull final EventProducer eventProducer,
      @Nonnull final SystemEntityClient entityClient,
      @Nonnull final GraphClient graphClient,
      @Nonnull final SettingsService settingsService,
      @Nonnull final NotificationRecipientBuilders notificationRecipientBuilders,
      @Nonnull final ActionWorkflowService actionWorkflowService,
      @Nonnull final UserService userService) {
    super(
        systemOpContext,
        eventProducer,
        entityClient,
        graphClient,
        settingsService,
        notificationRecipientBuilders);
    _actionWorkflowService = actionWorkflowService;
    _userService = userService;
  }

  @Override
  public boolean isEligibleForSubscriberRecipients() {
    return false;
  }

  @Override
  public boolean isEligibleForCustomRecipients(
      @Nonnull final NotificationScenarioType scenarioType) {
    // When a new workflow request is created, we notify the assigned users, groups, and roles.
    if (NotificationScenarioType.NEW_ACTION_WORKFLOW_FORM_REQUEST.equals(scenarioType)) {
      return true;
    }
    // When a workflow request is completed, we notify the original creator.
    if (NotificationScenarioType.REQUESTER_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE.equals(
        scenarioType)) {
      return true;
    }
    return false;
  }

  @Override
  protected List<NotificationRecipient> buildCustomRecipients(
      @Nonnull OperationContext opContext,
      @Nonnull NotificationScenarioType type,
      @Nonnull final Urn entityUrn,
      @Nullable final EntityChangeType changeType,
      @Nullable final Urn actorUrn,
      @Nullable NotificationRecipientsGeneratorExtraContext extraContext) {
    if (NotificationScenarioType.NEW_ACTION_WORKFLOW_FORM_REQUEST.equals(type)) {
      // Fetch and add assignee recipients
      return buildAssigneeRecipientsForWorkflowRequest(
          systemOpContext, entityUrn, extraContext, type);
    }
    if (NotificationScenarioType.REQUESTER_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE.equals(
        type)) {
      // Fetch and add requester recipient
      return buildRequesterRecipientForCompletedWorkflowRequest(
          systemOpContext, entityUrn, extraContext);
    }
    return Collections.emptyList();
  }

  private List<NotificationRecipient> buildAssigneeRecipientsForWorkflowRequest(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nullable NotificationRecipientsGeneratorExtraContext extraContext,
      @Nonnull final NotificationScenarioType type) {

    try {
      // Step 1: Fetch assignee urns from the entity.
      final ActionRequestInfo actionRequestInfo = getActionRequestInfo(entityUrn, extraContext);
      final List<Urn> assignedUsers = actionRequestInfo.getAssignedUsers();
      final List<Urn> assignedGroups = actionRequestInfo.getAssignedGroups();
      final List<Urn> assignedRoles =
          actionRequestInfo.hasAssignedRoles()
              ? actionRequestInfo.getAssignedRoles()
              : Collections.emptyList();

      // Step 2: Use UserService to resolve groups and roles to individual users
      final Set<Urn> actorsToNotify = new HashSet<>();

      // Add directly assigned users
      if (assignedUsers != null) {
        actorsToNotify.addAll(assignedUsers);
      }

      // Resolve groups to users
      if (assignedGroups != null && !assignedGroups.isEmpty()) {
        // If enabled for the group, also notify the group itself!
        actorsToNotify.addAll(assignedGroups);
        final List<Urn> usersFromGroups = _userService.resolveGroupUsers(opContext, assignedGroups);
        actorsToNotify.addAll(usersFromGroups);
      }

      // Resolve roles to users
      if (!assignedRoles.isEmpty()) {
        final List<Urn> usersFromRoles = _userService.resolveRoleUsers(opContext, assignedRoles);
        actorsToNotify.addAll(usersFromRoles);
      }

      // Step 3: Build the recipients
      return buildActorRecipients(opContext, new ArrayList<>(actorsToNotify), type);

    } catch (Exception e) {
      log.error("Failed to resolve workflow request assignees for urn: {}", entityUrn, e);
      return Collections.emptyList();
    }
  }

  private List<NotificationRecipient> buildActorRecipients(
      @Nonnull OperationContext opContext,
      final List<Urn> actorUrns,
      final NotificationScenarioType type) {
    return _recipientBuilders.listBuilders().stream()
        .flatMap(builder -> builder.buildActorRecipients(opContext, actorUrns, type).stream())
        .filter(Objects::nonNull)
        .map(recipient -> recipient.setOrigin(NotificationRecipientOriginType.ACTOR_NOTIFICATION))
        .collect(Collectors.toList());
  }

  private List<NotificationRecipient> buildRequesterRecipientForCompletedWorkflowRequest(
      @Nonnull OperationContext opContext,
      @Nullable final Urn entityUrn,
      @Nullable NotificationRecipientsGeneratorExtraContext extraContext) {
    // Step 1: Fetch the action request info
    final ActionRequestInfo actionRequestInfo = getActionRequestInfo(entityUrn, extraContext);

    // Step 2: Resolve the creator of the workflow request
    final Urn requester = actionRequestInfo.getCreatedBy();

    // Step 3: Build the recipients
    return buildActorRecipients(
        opContext,
        Collections.singletonList(requester),
        NotificationScenarioType.REQUESTER_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE);
  }

  @Override
  public void generate(@Nonnull MetadataChangeLog event) {
    if (event.getEntityUrn() == null || event.getAspect() == null) {
      return;
    }

    if (!isEligibleForProcessingEvent(event)) {
      return;
    }

    log.debug(String.format("Found eligible Workflow Request MCL. urn: %s", event.getEntityUrn()));

    if (isNewWorkflowRequest(event)) {
      final ActionRequestInfo info =
          GenericRecordUtils.deserializeAspect(
              event.getAspect().getValue(),
              event.getAspect().getContentType(),
              ActionRequestInfo.class);

      if (!isEligibleForProcessingActionRequestInfo(info)) {
        return;
      }

      log.debug(
          String.format(
              "Found eligible new workflow request event to notify. urn: %s",
              event.getEntityUrn().toString()));

      generateNewWorkflowRequestNotifications(event.getEntityUrn(), info, event.getCreated());
    } else if (isWorkflowRequestStepCompletion(event)) {
      final ActionRequestInfo info =
          GenericRecordUtils.deserializeAspect(
              event.getAspect().getValue(),
              event.getAspect().getContentType(),
              ActionRequestInfo.class);

      if (!isEligibleForProcessingActionRequestInfo(info)) {
        return;
      }

      log.debug(
          String.format(
              "Found eligible workflow request step completion event to notify. urn: %s",
              event.getEntityUrn().toString()));

      generateWorkflowRequestStepCompletionNotifications(
          event.getEntityUrn(), info, event.getCreated());
    } else if (isWorkflowRequestStatusChange(event)) {
      final ActionRequestInfo info = getActionRequestInfo(event.getEntityUrn());

      if (info == null || !isEligibleForProcessingActionRequestInfo(info)) {
        return;
      }

      final ActionRequestStatus status =
          GenericRecordUtils.deserializeAspect(
              event.getAspect().getValue(),
              event.getAspect().getContentType(),
              ActionRequestStatus.class);

      if (!isEligibleForProcessingActionRequestStatus(status)) {
        return;
      }

      log.debug(
          String.format(
              "Found eligible workflow request status change event to notify. urn: %s",
              event.getEntityUrn().toString()));

      generateWorkflowFormRequestCompletionChangeNotifications(
          event.getEntityUrn(), info, status, event.getCreated());
    }
  }

  private boolean isEligibleForProcessingEvent(final MetadataChangeLog event) {
    return (Constants.ACTION_REQUEST_INFO_ASPECT_NAME.equals(event.getAspectName())
            || Constants.ACTION_REQUEST_STATUS_ASPECT_NAME.equals(event.getAspectName()))
        && (ChangeType.UPSERT.equals(event.getChangeType())
            || ChangeType.CREATE.equals(event.getChangeType()));
  }

  @VisibleForTesting
  boolean isEligibleForProcessingActionRequestInfo(final ActionRequestInfo info) {
    return ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST.equals(info.getType());
  }

  private boolean isEligibleForProcessingActionRequestStatus(final ActionRequestStatus status) {
    return ACTION_REQUEST_RESULT_REJECTED.equals(status.getResult())
        || ACTION_REQUEST_RESULT_ACCEPTED.equals(status.getResult());
  }

  private boolean isNewWorkflowRequest(final MetadataChangeLog event) {
    return ACTION_REQUEST_INFO_ASPECT_NAME.equals(event.getAspectName())
        && event.getPreviousAspectValue() == null;
  }

  private boolean isWorkflowRequestStepCompletion(final MetadataChangeLog event) {
    if (!ACTION_REQUEST_INFO_ASPECT_NAME.equals(event.getAspectName())
        || event.getPreviousAspectValue() == null) {
      return false;
    }

    final ActionRequestInfo currentInfo =
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(),
            event.getAspect().getContentType(),
            ActionRequestInfo.class);

    final ActionRequestInfo previousInfo =
        GenericRecordUtils.deserializeAspect(
            event.getPreviousAspectValue().getValue(),
            event.getPreviousAspectValue().getContentType(),
            ActionRequestInfo.class);

    // Check if this is a step completion by comparing step states
    return isStepStateChanged(previousInfo, currentInfo);
  }

  private boolean isStepStateChanged(
      @Nonnull final ActionRequestInfo previousInfo, @Nonnull final ActionRequestInfo currentInfo) {
    if (previousInfo.hasParams()
        && currentInfo.hasParams()
        && previousInfo.getParams().hasWorkflowFormRequest()
        && currentInfo.getParams().hasWorkflowFormRequest()) {

      ActionWorkflowFormRequest prevWorkflowRequest =
          previousInfo.getParams().getWorkflowFormRequest();
      ActionWorkflowFormRequest currentWorkflowRequest =
          currentInfo.getParams().getWorkflowFormRequest();

      if (prevWorkflowRequest.hasStepState() && currentWorkflowRequest.hasStepState()) {
        String prevStepId = prevWorkflowRequest.getStepState().getStepId();
        String currentStepId = currentWorkflowRequest.getStepState().getStepId();

        return !prevStepId.equals(currentStepId);
      }
    }

    return false;
  }

  private boolean isWorkflowRequestStatusChange(final MetadataChangeLog event) {
    if (event.getAspect() == null
        || !ACTION_REQUEST_STATUS_ASPECT_NAME.equals(event.getAspectName())) {
      return false;
    }

    final ActionRequestStatus newStatus =
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(),
            event.getAspect().getContentType(),
            ActionRequestStatus.class);

    // If new status is not complete, we ignore
    if (!ACTION_REQUEST_STATUS_COMPLETE.equals(newStatus.getStatus())) {
      return false;
    }

    // Check if status actually changed
    if (event.getPreviousAspectValue() != null) {
      final ActionRequestStatus prevStatus =
          GenericRecordUtils.deserializeAspect(
              event.getPreviousAspectValue().getValue(),
              event.getPreviousAspectValue().getContentType(),
              ActionRequestStatus.class);
      return !prevStatus.getStatus().equals(newStatus.getStatus());
    }

    return true;
  }

  public void generateNewWorkflowRequestNotifications(
      @Nonnull final Urn urn,
      @Nonnull final ActionRequestInfo info,
      @Nonnull final AuditStamp auditStamp) {
    trySendNewWorkflowFormRequestNotifications(urn, info, auditStamp);
  }

  public void generateWorkflowRequestStepCompletionNotifications(
      @Nonnull final Urn urn,
      @Nonnull final ActionRequestInfo info,
      @Nonnull final AuditStamp auditStamp) {
    // Send notification to new assigned users about the step completion
    trySendNewWorkflowFormRequestNotifications(urn, info, auditStamp);
  }

  public void generateWorkflowFormRequestCompletionChangeNotifications(
      @Nonnull final Urn urn,
      @Nonnull final ActionRequestInfo info,
      @Nonnull final ActionRequestStatus newStatus,
      @Nonnull final AuditStamp auditStamp) {
    trySendWorkflowFormRequestCompletedNotifications(urn, info, newStatus, auditStamp);
  }

  private void trySendNewWorkflowFormRequestNotifications(
      @Nonnull final Urn urn,
      @Nonnull final ActionRequestInfo info,
      @Nonnull final AuditStamp auditStamp) {
    final Urn actorUrn = auditStamp.getActor();

    // Pass down context required to build recipients
    NotificationRecipientsGeneratorExtraContext context =
        new NotificationRecipientsGeneratorExtraContext();
    context.setOriginalAspect(info);

    Set<NotificationRecipient> recipients =
        new HashSet<>(
            buildRecipients(
                systemOpContext,
                NotificationScenarioType.NEW_ACTION_WORKFLOW_FORM_REQUEST,
                urn,
                null,
                actorUrn,
                context));

    if (recipients.isEmpty()) {
      return;
    }

    // Get workflow details
    final ActionWorkflowFormRequest workflowRequest = info.getParams().getWorkflowFormRequest();
    final Urn workflowUrn = workflowRequest.getWorkflow();
    final Urn entityUrn = info.hasResource() ? UrnUtils.getUrn(info.getResource()) : null;

    // Build template parameters
    final Map<String, String> templateParams =
        buildWorkflowRequestTemplateParameters(info, workflowUrn, entityUrn, auditStamp);

    final NotificationRequest notificationRequest =
        buildNotificationRequest(
            NotificationTemplateType.BROADCAST_NEW_ACTION_WORKFLOW_FORM_REQUEST.name(),
            templateParams,
            recipients);

    log.debug(
        String.format(
            "Broadcasting new workflow request for entity %s, action request %s...",
            entityUrn, urn));

    sendNotificationRequest(notificationRequest);
  }

  private void trySendWorkflowFormRequestCompletedNotifications(
      @Nonnull final Urn urn,
      @Nonnull final ActionRequestInfo info,
      @Nonnull final ActionRequestStatus newStatus,
      @Nonnull final AuditStamp auditStamp) {

    final Urn actorUrn = auditStamp.getActor();

    // Pass down context required to build recipients
    NotificationRecipientsGeneratorExtraContext context =
        new NotificationRecipientsGeneratorExtraContext();
    context.setOriginalAspect(info);

    Set<NotificationRecipient> recipients =
        new HashSet<>(
            buildRecipients(
                systemOpContext,
                NotificationScenarioType.REQUESTER_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE,
                urn,
                null,
                actorUrn,
                context));

    if (recipients.isEmpty()) {
      return;
    }

    // Get workflow details
    final ActionWorkflowFormRequest workflowRequest = info.getParams().getWorkflowFormRequest();
    final Urn workflowUrn = workflowRequest.getWorkflow();
    final Urn entityUrn = info.hasResource() ? UrnUtils.getUrn(info.getResource()) : null;

    // Build template parameters
    final Map<String, String> templateParams =
        buildWorkflowRequestTemplateParameters(info, workflowUrn, entityUrn, auditStamp);

    // Add result-specific parameters
    final String result = getResult(newStatus);
    templateParams.put("result", result);
    if (newStatus.hasNote()) {
      templateParams.put("note", newStatus.getNote());
    }
    templateParams.put("creatorUrn", info.getCreatedBy().toString());
    templateParams.put(
        "creatorName", _entityNameProvider.getName(systemOpContext, info.getCreatedBy()));

    final NotificationRequest notificationRequest =
        buildNotificationRequest(
            NotificationTemplateType.BROADCAST_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE.name(),
            templateParams,
            recipients);

    log.debug(
        String.format(
            "Broadcasting workflow request status change for entity %s, action request %s...",
            entityUrn, urn));

    sendNotificationRequest(notificationRequest);
  }

  private Map<String, String> buildWorkflowRequestTemplateParameters(
      @Nonnull final ActionRequestInfo info,
      @Nonnull final Urn workflowUrn,
      @Nullable final Urn entityUrn,
      @Nonnull final AuditStamp auditStamp) {

    final Map<String, String> templateParams = new HashMap<>();

    // Get workflow info
    try {
      final ActionWorkflowInfo workflowInfo =
          _actionWorkflowService.getActionWorkflow(systemOpContext, workflowUrn);

      templateParams.put("workflowName", workflowInfo.getName());
      templateParams.put("workflowUrn", workflowUrn.toString());
      templateParams.put("workflowType", workflowInfo.getCategory().toString());
      if (workflowInfo.hasCustomCategory()) {
        templateParams.put("customWorkflowType", workflowInfo.getCustomCategory());
      }
    } catch (Exception e) {
      log.warn("Failed to get workflow info for urn: {}", workflowUrn, e);
      templateParams.put("workflowName", "Unknown Workflow");
      templateParams.put("workflowUrn", workflowUrn.toString());
    }

    // Actor information
    final String actorName = _entityNameProvider.getName(systemOpContext, auditStamp.getActor());
    templateParams.put("actorUrn", auditStamp.getActor().toString());
    templateParams.put("actorName", actorName);

    // Entity information if available
    if (entityUrn != null) {
      final String entityName = _entityNameProvider.getName(systemOpContext, entityUrn);
      final String entityType = _entityNameProvider.getTypeName(systemOpContext, entityUrn);
      final String entityPath = generateEntityPath(entityUrn);
      final String entityPlatform = _entityNameProvider.getPlatformName(systemOpContext, entityUrn);

      templateParams.put("entityName", entityName);
      templateParams.put("entityType", entityType);
      templateParams.put("entityPath", entityPath);
      if (entityPlatform != null) {
        templateParams.put("entityPlatform", entityPlatform);
      }
    }

    // Workflow request fields
    if (info.hasParams() && info.getParams().hasWorkflowFormRequest()) {
      final ActionWorkflowFormRequest workflowRequest = info.getParams().getWorkflowFormRequest();
      if (workflowRequest.hasFields()) {
        templateParams.put(
            "fields", serializeWorkflowFormRequestFields(workflowRequest.getFields()));
      }
    }

    return templateParams;
  }

  private String serializeWorkflowFormRequestFields(List<ActionWorkflowFormRequestField> fields) {
    try {
      Map<String, Object> fieldsMap = new HashMap<>();
      for (ActionWorkflowFormRequestField field : fields) {
        if (field.hasValues() && !field.getValues().isEmpty()) {
          List<String> values =
              field.getValues().stream()
                  .map(this::convertPrimitiveValueToString)
                  .collect(Collectors.toList());
          fieldsMap.put(field.getId(), values);
        }
      }
      return _objectMapper.writeValueAsString(fieldsMap);
    } catch (Exception e) {
      log.error("Failed to serialize workflow request fields", e);
      return "{}";
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

  private String getResult(ActionRequestStatus status) {
    switch (status.getResult()) {
      case ACTION_REQUEST_RESULT_ACCEPTED:
        return "approved";
      case ACTION_REQUEST_RESULT_REJECTED:
        return "rejected";
      default:
        return "unknown";
    }
  }

  private ActionRequestInfo getActionRequestInfo(
      @Nonnull final Urn actionRequestUrn,
      @Nonnull final NotificationRecipientsGeneratorExtraContext extraContext) {
    if (extraContext.getOriginalAspect() == null
        || !(extraContext.getOriginalAspect() instanceof ActionRequestInfo)) {
      return getActionRequestInfo(actionRequestUrn);
    }
    return (ActionRequestInfo) extraContext.getOriginalAspect();
  }

  private ActionRequestInfo getActionRequestInfo(final Urn actionRequestUrn) {
    try {
      return _actionWorkflowService.getActionRequestInfo(systemOpContext, actionRequestUrn);
    } catch (Exception e) {
      log.error("Failed to retrieve Action Request Info for urn: {}", actionRequestUrn, e);
      return null;
    }
  }
}
