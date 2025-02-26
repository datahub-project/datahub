package com.linkedin.metadata.kafka.hook.notification.proposal;

import static com.linkedin.metadata.kafka.hook.notification.NotificationUtils.*;

import com.datahub.notification.NotificationScenarioType;
import com.datahub.notification.NotificationTemplateType;
import com.datahub.notification.recipient.NotificationRecipientBuilders;
import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientOriginType;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.kafka.hook.notification.BaseMclNotificationGenerator;
import com.linkedin.metadata.kafka.hook.notification.NotificationRecipientsGeneratorExtraContext;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
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
 * An extension of {@link BaseMclNotificationGenerator} which generates notifications on incident
 * creation and status changes.
 */
@Slf4j
public class ProposalNotificationGenerator extends BaseMclNotificationGenerator {

  private static final String IS_MEMBER_OF_ROLE_RELATIONSHIP_NAME = "IsMemberOfRole";
  private static final String CORP_GROUP_URN_PREFIX = "urn:li:corpGroup";
  private static final String CORP_USER_URN_PREFIX = "urn:li:corpuser";
  private static final int ROLE_RESOLUTION_BATCH_SIZE = 1000;
  private final FeatureFlags _featureFlags;
  private final ObjectMapper _objectMapper = new ObjectMapper();

  public ProposalNotificationGenerator(
      @Nonnull OperationContext systemOpContext,
      @Nonnull final EventProducer eventProducer,
      @Nonnull final SystemEntityClient entityClient,
      @Nonnull final GraphClient graphClient,
      @Nonnull final SettingsService settingsService,
      @Nonnull final NotificationRecipientBuilders notificationRecipientBuilders,
      @Nonnull final FeatureFlags featureFlags) {
    super(
        systemOpContext,
        eventProducer,
        entityClient,
        graphClient,
        settingsService,
        notificationRecipientBuilders);
    _featureFlags = featureFlags;
  }

  @Override
  public boolean isEligibleForSubscriberRecipients() {
    return _featureFlags.isSubscriptionsEnabled();
  }

  @Override
  public boolean isEligibleForCustomRecipients(
      @Nonnull final NotificationScenarioType scenarioType) {
    // When a new proposal is raised, we also notify the assigned users, groups, and roles.
    if (NotificationScenarioType.NEW_PROPOSAL.equals(scenarioType)) {
      return true;
    }
    // When a proposal is resolved, we also notify the actor who originally raised the proposal.
    if (NotificationScenarioType.PROPOSAL_STATUS_CHANGE.equals(scenarioType)) {
      return true;
    }
    // When a proposal is resolved, we also notify the actor who originally raised the proposal.
    if (NotificationScenarioType.PROPOSER_PROPOSAL_STATUS_CHANGE.equals(scenarioType)) {
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
    System.out.println("Scenario type: " + type);
    if (NotificationScenarioType.NEW_PROPOSAL.equals(type)) {
      // Fetch and add assignee recipients, depending on settings of course.
      return buildAssigneeRecipientsForNewProposal(systemOpContext, entityUrn, extraContext);
    }
    if (NotificationScenarioType.PROPOSAL_STATUS_CHANGE.equals(type)) {
      // Fetch and add proposer recipient, depending on settings of course.
      return buildAssigneeRecipientsForNewProposal(systemOpContext, entityUrn, extraContext);
    }
    if (NotificationScenarioType.PROPOSER_PROPOSAL_STATUS_CHANGE.equals(type)) {
      // Fetch and add proposer recipient, depending on settings of course.
      return buildProposerRecipientForCompletedProposal(systemOpContext, entityUrn, extraContext);
    }
    return null;
  }

  private List<NotificationRecipient> buildAssigneeRecipientsForNewProposal(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn entityUrn,
      @Nullable NotificationRecipientsGeneratorExtraContext extraContext) {

    // Step 1: Fetch assignee urns from the entity.
    final ActionRequestInfo actionRequestInfo = getActionRequestInfo(entityUrn, extraContext);
    final List<Urn> assignedUsers = actionRequestInfo.getAssignedUsers();
    final List<Urn> assignedGroups = actionRequestInfo.getAssignedGroups();
    final List<Urn> assignedRoles =
        actionRequestInfo.hasAssignedRoles()
            ? actionRequestInfo.getAssignedRoles()
            : Collections.emptyList();

    // Step 2: Resolve the assignee urns to actors, e.g. resolve the roles.
    final List<Urn> resolvedAssignedActors = getActorsWithRoles(opContext, assignedRoles);
    final List<Urn> resolvedAssignedGroups =
        resolvedAssignedActors.stream()
            .filter(urn -> urn.toString().startsWith(CORP_GROUP_URN_PREFIX))
            .collect(Collectors.toList());
    final List<Urn> resolvedAssignedUsers =
        resolvedAssignedActors.stream()
            .filter(urn -> urn.toString().startsWith(CORP_USER_URN_PREFIX))
            .collect(Collectors.toList());

    final List<Urn> allAssignedActors = new ArrayList<>();
    allAssignedActors.addAll(resolvedAssignedUsers);
    allAssignedActors.addAll(assignedUsers);
    allAssignedActors.addAll(resolvedAssignedGroups);
    allAssignedActors.addAll(assignedGroups);

    // Step 3: Build the recipients!
    final List<NotificationRecipient> recipients = new ArrayList<>();
    recipients.addAll(
        buildActorRecipients(opContext, allAssignedActors, NotificationScenarioType.NEW_PROPOSAL));
    return recipients;
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

  private List<Urn> getActorsWithRoles(final OperationContext opContext, final List<Urn> roles) {
    final Set<Urn> allActorsWithAnyRoles = new HashSet<>();
    for (final Urn role : roles) {
      try {
        List<Urn> allRelationships = getActorsWithRole(opContext, role);
        allActorsWithAnyRoles.addAll(allRelationships);
      } catch (Exception e) {
        log.error(
            "Failed to resolve actors with role: {}. Skipping notifying these users!",
            role.toString(),
            e);
      }
    }
    return new ArrayList<>(allActorsWithAnyRoles);
  }

  private List<Urn> getActorsWithRole(final OperationContext opContext, final Urn role) {
    int start = 0;
    int total = Integer.MAX_VALUE; // Start with a large value to enter the loop.
    final List<Urn> resolvedActors = new ArrayList<>();

    while (start < total) {
      // Fetch actors with role.
      final EntityRelationships entityRelationships =
          this._graphClient.getRelatedEntities(
              role.toString(),
              Collections.singletonList(IS_MEMBER_OF_ROLE_RELATIONSHIP_NAME),
              RelationshipDirection.INCOMING,
              start,
              ROLE_RESOLUTION_BATCH_SIZE,
              opContext.getActorContext().getActorUrn().toString());

      if (entityRelationships == null || entityRelationships.getRelationships() == null) {
        break; // Exit on null to prevent NPE.
      }

      // Add fetched actors to the list.
      for (final EntityRelationship relationship : entityRelationships.getRelationships()) {
        resolvedActors.add(relationship.getEntity());
      }

      total = entityRelationships.getTotal();
      start += ROLE_RESOLUTION_BATCH_SIZE;
    }
    return resolvedActors;
  }

  private ActionRequestInfo getActionRequestInfo(
      @Nonnull final Urn actionRequestUrn,
      @Nonnull final NotificationRecipientsGeneratorExtraContext extraContext) {
    if (extraContext.getOriginalAspect() == null
        || !(extraContext.getOriginalAspect() instanceof ActionRequestInfo)) {
      // Resolve the action request info,
      return getActionRequestInfo(actionRequestUrn);
    }
    return (ActionRequestInfo) extraContext.getOriginalAspect();
  }

  private List<NotificationRecipient> buildProposerRecipientForCompletedProposal(
      @Nonnull OperationContext opContext,
      @Nullable final Urn entityUrn,
      @Nullable NotificationRecipientsGeneratorExtraContext extraContext) {
    // Step 1: Fetch assignee urns from the entity.
    final ActionRequestInfo actionRequestInfo = getActionRequestInfo(entityUrn, extraContext);

    // Step 2: Resolve the creator of the proposal
    final Urn proposer = actionRequestInfo.getCreatedBy();

    // Step 3: Build the recipients!
    final List<NotificationRecipient> recipients = new ArrayList<>();

    System.out.println("Building proposer!");
    System.out.println(proposer.toString());
    System.out.println(
        buildActorRecipients(
            opContext,
            Collections.singletonList(proposer),
            NotificationScenarioType.PROPOSER_PROPOSAL_STATUS_CHANGE));

    recipients.addAll(
        buildActorRecipients(
            opContext,
            Collections.singletonList(proposer),
            NotificationScenarioType.PROPOSER_PROPOSAL_STATUS_CHANGE));

    return recipients;
  }

  @Override
  public void generate(@Nonnull MetadataChangeLog event) {
    if (event.getEntityUrn() == null || event.getAspect() == null) {
      return;
    }

    if (!isEligibleForProcessingEvent(event)) {
      return;
    }

    log.debug(String.format("Found eligible Action Request MCL. urn: %s", event.getEntityUrn()));

    if (isNewProposal(event)) {
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
              "Found eligible new proposal event to notify. urn: %s",
              event.getEntityUrn().toString()));

      generateNewProposalNotifications(event.getEntityUrn(), info, event.getCreated());
    } else if (isProposalStatusChange(event)) {

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
              "Found eligible proposal change event to notify. urn: %s",
              event.getEntityUrn().toString()));

      generateUpdatedProposalNotifications(event.getEntityUrn(), info, status, event.getCreated());
    }
  }

  private boolean isEligibleForProcessingEvent(final MetadataChangeLog event) {
    return (Constants.ACTION_REQUEST_INFO_ASPECT_NAME.equals(event.getAspectName())
            || Constants.ACTION_REQUEST_STATUS_ASPECT_NAME.equals(event.getAspectName()))
        && (ChangeType.UPSERT.equals(event.getChangeType())
            || ChangeType.CREATE.equals(event.getChangeType()));
  }

  private boolean isEligibleForProcessingActionRequestInfo(final ActionRequestInfo info) {
    return AcrylConstants.ACTION_REQUEST_TYPE_TAG_PROPOSAL.equals(info.getType())
        || AcrylConstants.ACTION_REQUEST_TYPE_TERM_PROPOSAL.equals((info.getType()))
        || AcrylConstants.ACTION_REQUEST_TYPE_UPDATE_DESCRIPTION_PROPOSAL.equals((info.getType()))
        || AcrylConstants.ACTION_REQUEST_TYPE_DOMAIN_PROPOSAL.equals(info.getType())
        || AcrylConstants.ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL.equals(info.getType())
        || AcrylConstants.ACTION_REQUEST_TYPE_OWNER_PROPOSAL.equals(info.getType())
        || AcrylConstants.ACTION_REQUEST_TYPE_CREATE_GLOSSARY_TERM_PROPOSAL.equals((info.getType()))
        || AcrylConstants.ACTION_REQUEST_TYPE_CREATE_GLOSSARY_NODE_PROPOSAL.equals(
            (info.getType()));
  }

  private boolean isEligibleForProcessingActionRequestStatus(final ActionRequestStatus status) {
    return AcrylConstants.ACTION_REQUEST_RESULT_REJECTED.equals(status.getResult())
        || AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED.equals(status.getResult());
  }

  public void generateNewProposalNotifications(
      @Nonnull final Urn urn,
      @Nonnull final ActionRequestInfo info,
      @Nonnull final AuditStamp auditStamp) {
    // Broadcast new incident.
    trySendNewProposalNotifications(urn, info, auditStamp);
  }

  public void generateUpdatedProposalNotifications(
      @Nonnull final Urn urn,
      @Nonnull final ActionRequestInfo info,
      @Nonnull final ActionRequestStatus newStatus,
      @Nonnull final AuditStamp auditStamp) {
    // Broadcast incident status change.
    trySendProposalStatusChangeNotifications(urn, info, newStatus, auditStamp);
  }

  /** Sends a notification of template type "BROADCAST_NEW_PROPOSAL" when a proposal is created. */
  private void trySendNewProposalNotifications(
      @Nonnull final Urn urn,
      @Nonnull final ActionRequestInfo info,
      @Nonnull final AuditStamp auditStamp) {
    final Urn actorUrn = auditStamp.getActor();

    // Pass down context required to build recipients.
    NotificationRecipientsGeneratorExtraContext context =
        new NotificationRecipientsGeneratorExtraContext();
    context.setOriginalAspect(info);

    Set<NotificationRecipient> recipients =
        new HashSet<>(
            buildRecipients(
                systemOpContext,
                NotificationScenarioType.NEW_PROPOSAL,
                urn,
                null,
                actorUrn,
                context));

    final Urn entityUrn = info.hasResource() ? UrnUtils.getUrn(info.getResource()) : null;

    if (recipients.isEmpty()) {
      return;
    }

    final String subResource = info.getSubResource();
    final String subResourceType = info.getSubResourceType();
    final String operation = getOperation(info);
    final List<Urn> modifierUrns = getModifierUrns(info);
    final String modifierType = getModifierType(info);
    final List<String> modifierNames = getEntityDisplayNames(modifierUrns);
    final Map<String, String> templateParams = new HashMap<>();
    final String entityName = getEntityNameFromProposal(info);
    final String entityType = getEntityTypeNameFromProposal(info);
    final String entityPath = entityUrn != null ? generateEntityPath(entityUrn) : null;
    final String entityPlatform =
        entityUrn != null ? _entityNameProvider.getPlatformName(systemOpContext, entityUrn) : null;
    final String actorName = _entityNameProvider.getName(systemOpContext, auditStamp.getActor());
    templateParams.put("entityName", entityName);
    templateParams.put("entityType", entityType);
    templateParams.put("operation", operation);
    templateParams.put("modifierType", modifierType);
    templateParams.put("modifierNames", toSerializedJsonArray(modifierNames));
    templateParams.put("modifierPaths", toSerializedJsonArray(generateEntityPaths(modifierUrns)));
    templateParams.put("actorUrn", auditStamp.getActor().toString());
    templateParams.put("actorName", actorName);
    if (entityPath != null) {
      templateParams.put("entityPath", entityPath);
    }
    if (entityPlatform != null) {
      templateParams.put("entityPlatform", entityPlatform);
    }
    if (subResource != null && subResourceType != null) {
      templateParams.put("subResourceType", subResourceType);
      templateParams.put("subResource", subResource);
    }
    addContextToTemplateParams(templateParams, info);

    final NotificationRequest notificationRequest =
        buildNotificationRequest(
            NotificationTemplateType.BROADCAST_NEW_PROPOSAL.name(), templateParams, recipients);

    log.debug(
        String.format(
            "Broadcasting new proposal change for entity %s, action request %s...",
            entityUrn, urn));

    sendNotificationRequest(notificationRequest);
  }

  /**
   * Sends a notification of template type "BROADCAST_PROPOSAL_STATUS_CHANGE" when an proposal's
   * status is changed.
   */
  private void trySendProposalStatusChangeNotifications(
      @Nonnull final Urn urn,
      @Nonnull final ActionRequestInfo info,
      @Nonnull final ActionRequestStatus newStatus,
      @Nonnull final AuditStamp auditStamp) {

    final Urn entityUrn = info.hasResource() ? UrnUtils.getUrn(info.getResource()) : null;
    final Urn actorUrn = auditStamp.getActor();

    // Pass down context required to build recipients.
    NotificationRecipientsGeneratorExtraContext context =
        new NotificationRecipientsGeneratorExtraContext();
    context.setOriginalAspect(info);

    // Build notification recipients from 2 scenarios: As a stakeholder and as a proposer.
    // This allows us to use different configs / settings for each scenario: Reviewed and proposed.
    List<NotificationRecipient> stakeholders =
        buildRecipients(
            systemOpContext,
            NotificationScenarioType.PROPOSAL_STATUS_CHANGE,
            urn,
            null,
            actorUrn,
            context);

    List<NotificationRecipient> proposer =
        buildRecipients(
            systemOpContext,
            NotificationScenarioType.PROPOSER_PROPOSAL_STATUS_CHANGE,
            urn,
            null,
            actorUrn,
            context);

    Set<NotificationRecipient> recipients = new HashSet<>();
    recipients.addAll(stakeholders);
    recipients.addAll(proposer);

    if (recipients.isEmpty()) {
      return;
    }

    final String entityName = getEntityNameFromProposal(info);
    final String entityType = getEntityTypeNameFromProposal(info);
    final String entityPlatform =
        entityUrn != null ? _entityNameProvider.getPlatformName(systemOpContext, entityUrn) : null;
    final String entityPath = entityUrn != null ? generateEntityPath(entityUrn) : null;
    final String actorName = _entityNameProvider.getName(systemOpContext, auditStamp.getActor());
    final String subResource = info.getSubResource();
    final String subResourceType = info.getSubResourceType();
    final String operation = getOperation(info);
    final List<Urn> modifierUrns = getModifierUrns(info);
    final String modifierType = getModifierType(info);
    final List<String> modifierNames = getEntityDisplayNames(modifierUrns);
    final String action = getAction(newStatus);

    final Map<String, String> templateParams = new HashMap<>();
    templateParams.put("entityName", entityName);
    templateParams.put("entityType", entityType);
    templateParams.put("operation", operation);
    templateParams.put("modifierType", modifierType);
    templateParams.put("modifierNames", toSerializedJsonArray(modifierNames));
    templateParams.put("modifierPaths", toSerializedJsonArray(generateEntityPaths(modifierUrns)));
    templateParams.put("actorUrn", auditStamp.getActor().toString());
    templateParams.put("actorName", actorName);
    templateParams.put("creatorUrn", info.getCreatedBy().toString());
    templateParams.put("action", action);
    if (entityPath != null) {
      templateParams.put("entityPath", entityPath);
    }
    if (entityPlatform != null) {
      templateParams.put("entityPlatform", entityPlatform);
    }
    if (subResource != null && subResourceType != null) {
      templateParams.put("subResourceType", subResourceType);
      templateParams.put("subResource", subResource);
    }
    addContextToTemplateParams(templateParams, info);

    final NotificationRequest notificationRequest =
        buildNotificationRequest(
            NotificationTemplateType.BROADCAST_PROPOSAL_STATUS_CHANGE.name(),
            templateParams,
            recipients);

    log.info(
        String.format(
            "Broadcasting proposal status change for entity %s, action request %s...",
            entityUrn, urn));

    sendNotificationRequest(notificationRequest);
  }

  /** Adds proposal type specific context to the template parameters. */
  private void addContextToTemplateParams(
      @Nonnull final Map<String, String> templateParams, @Nonnull final ActionRequestInfo info) {
    if (AcrylConstants.ACTION_REQUEST_TYPE_OWNER_PROPOSAL.equals(info.getType())
        && info.getParams().hasOwnerProposal()) {
      templateParams.put("context", toSerializedJsonObject(info.getParams().getOwnerProposal()));
    } else if (AcrylConstants.ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL.equals(
            info.getType())
        && info.getParams().hasStructuredPropertyProposal()) {
      templateParams.put(
          "context", toSerializedJsonObject(info.getParams().getStructuredPropertyProposal()));
    } else if (AcrylConstants.ACTION_REQUEST_TYPE_CREATE_GLOSSARY_NODE_PROPOSAL.equals(
        info.getType())) {
      if (info.getParams().getCreateGlossaryNodeProposal().hasParentNode()) {
        templateParams.put(
            "parentTermGroupName",
            _entityNameProvider.getName(
                systemOpContext, info.getParams().getCreateGlossaryNodeProposal().getParentNode()));
      }
    } else if (AcrylConstants.ACTION_REQUEST_TYPE_CREATE_GLOSSARY_TERM_PROPOSAL.equals(
        info.getType())) {
      if (info.getParams().getCreateGlossaryTermProposal().hasParentNode()) {
        templateParams.put(
            "parentTermGroupName",
            _entityNameProvider.getName(
                systemOpContext, info.getParams().getCreateGlossaryTermProposal().getParentNode()));
      }
    }
  }

  private String getEntityNameFromProposal(@Nonnull final ActionRequestInfo info) {
    if (info.getResource() != null) {
      return _entityNameProvider.getName(systemOpContext, UrnUtils.getUrn(info.getResource()));
    } else if (AcrylConstants.ACTION_REQUEST_TYPE_CREATE_GLOSSARY_TERM_PROPOSAL.equals(
        info.getType())) {
      return info.getParams().getCreateGlossaryTermProposal().getName();
    } else if (AcrylConstants.ACTION_REQUEST_TYPE_CREATE_GLOSSARY_NODE_PROPOSAL.equals(
        info.getType())) {
      return info.getParams().getCreateGlossaryNodeProposal().getName();
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported action request type %s provided!", info.getType()));
    }
  }

  private String getEntityTypeNameFromProposal(@Nonnull final ActionRequestInfo info) {
    if (info.getResource() != null) {
      return _entityNameProvider.getTypeName(systemOpContext, UrnUtils.getUrn(info.getResource()));
    } else if (AcrylConstants.ACTION_REQUEST_TYPE_CREATE_GLOSSARY_TERM_PROPOSAL.equals(
        info.getType())) {
      return "Glossary Term";
    } else if (AcrylConstants.ACTION_REQUEST_TYPE_CREATE_GLOSSARY_NODE_PROPOSAL.equals(
        info.getType())) {
      return "Glossary Term Group";
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported action request type %s provided!", info.getType()));
    }
  }

  private ActionRequestInfo getActionRequestInfo(final Urn actionRequestUrn) {
    try {
      EntityResponse entityResponse =
          _entityClient.getV2(
              systemOpContext,
              Constants.ACTION_REQUEST_ENTITY_NAME,
              actionRequestUrn,
              ImmutableSet.of(Constants.ACTION_REQUEST_INFO_ASPECT_NAME));
      if (entityResponse != null
          && entityResponse.hasAspects()
          && entityResponse.getAspects().containsKey(Constants.ACTION_REQUEST_INFO_ASPECT_NAME)) {
        return new ActionRequestInfo(
            entityResponse
                .getAspects()
                .get(Constants.ACTION_REQUEST_INFO_ASPECT_NAME)
                .getValue()
                .data());
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to retrieve Action Request Info", e);
    }
  }

  private boolean isNewProposal(final MetadataChangeLog event) {
    return Constants.ACTION_REQUEST_INFO_ASPECT_NAME.equals(event.getAspectName())
        && event.getPreviousAspectValue() == null;
  }

  private boolean isProposalStatusChange(final MetadataChangeLog event) {
    if (event.getAspect() == null
        || !Constants.ACTION_REQUEST_STATUS_ASPECT_NAME.equals(event.getAspectName())) {
      return false;
    }
    final ActionRequestStatus newStatus =
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(),
            event.getAspect().getContentType(),
            ActionRequestStatus.class);

    // If new status is not complete, we simply ignore for now.
    if (!AcrylConstants.ACTION_REQUEST_STATUS_COMPLETE.equals(newStatus.getStatus())) {
      return false;
    }

    final ActionRequestStatus prevStatus =
        GenericRecordUtils.deserializeAspect(
            event.getPreviousAspectValue().getValue(),
            event.getPreviousAspectValue().getContentType(),
            ActionRequestStatus.class);
    return !prevStatus.getStatus().equals(newStatus.getStatus());
  }

  private String getOperation(ActionRequestInfo info) {
    switch (info.getType()) {
      case AcrylConstants.ACTION_REQUEST_TYPE_TAG_PROPOSAL:
      case AcrylConstants.ACTION_REQUEST_TYPE_TERM_PROPOSAL:
      case AcrylConstants.ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL:
      case AcrylConstants.ACTION_REQUEST_TYPE_DOMAIN_PROPOSAL:
      case AcrylConstants.ACTION_REQUEST_TYPE_OWNER_PROPOSAL:
        return "add";
      case AcrylConstants.ACTION_REQUEST_TYPE_CREATE_GLOSSARY_NODE_PROPOSAL:
      case AcrylConstants.ACTION_REQUEST_TYPE_CREATE_GLOSSARY_TERM_PROPOSAL:
        return "create";
      case AcrylConstants.ACTION_REQUEST_TYPE_UPDATE_DESCRIPTION_PROPOSAL:
        return "update";
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported action request type %s provided!", info.getType()));
    }
  }

  private String getModifierType(ActionRequestInfo info) {
    switch (info.getType()) {
      case AcrylConstants.ACTION_REQUEST_TYPE_TAG_PROPOSAL:
        return "Tag(s)";
      case AcrylConstants.ACTION_REQUEST_TYPE_TERM_PROPOSAL:
        return "Glossary Term(s)";
      case AcrylConstants.ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL:
        return "Structured Property(s)";
      case AcrylConstants.ACTION_REQUEST_TYPE_DOMAIN_PROPOSAL:
        return "Domain";
      case AcrylConstants.ACTION_REQUEST_TYPE_OWNER_PROPOSAL:
        return "Owner(s)";
      case AcrylConstants.ACTION_REQUEST_TYPE_CREATE_GLOSSARY_NODE_PROPOSAL:
        return "Glossary Term Group";
      case AcrylConstants.ACTION_REQUEST_TYPE_CREATE_GLOSSARY_TERM_PROPOSAL:
        return "Glossary Term";
      case AcrylConstants.ACTION_REQUEST_TYPE_UPDATE_DESCRIPTION_PROPOSAL:
        return "Description";
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported action request type %s provided!", info.getType()));
    }
  }

  private List<String> getEntityDisplayNames(List<Urn> entityUrns) {
    final List<String> names = new ArrayList<>();
    for (Urn urn : entityUrns) {
      names.add(_entityNameProvider.getName(systemOpContext, urn));
    }
    return names;
  }

  private List<Urn> getModifierUrns(ActionRequestInfo info) {
    switch (info.getType()) {
      case AcrylConstants.ACTION_REQUEST_TYPE_TAG_PROPOSAL:
        // Resolve and present the name of the tag.
        return info.getParams().getTagProposal().hasTags()
                && !info.getParams().getTagProposal().getTags().isEmpty()
            ? info.getParams().getTagProposal().getTags()
            : Collections.singletonList(info.getParams().getTagProposal().getTag());
      case AcrylConstants.ACTION_REQUEST_TYPE_TERM_PROPOSAL:
        // Resolve and present the name of the term.
        return info.getParams().getGlossaryTermProposal().hasGlossaryTerms()
                && !info.getParams().getGlossaryTermProposal().getGlossaryTerms().isEmpty()
            ? info.getParams().getGlossaryTermProposal().getGlossaryTerms()
            : Collections.singletonList(
                info.getParams().getGlossaryTermProposal().getGlossaryTerm());
      case AcrylConstants.ACTION_REQUEST_TYPE_OWNER_PROPOSAL:
        return info.getParams().getOwnerProposal().getOwners().stream()
            .map(owner -> owner.getOwner())
            .collect(Collectors.toList());
      case AcrylConstants.ACTION_REQUEST_TYPE_DOMAIN_PROPOSAL:
        return info.getParams().getDomainProposal().getDomains();
      case AcrylConstants.ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL:
        return info
            .getParams()
            .getStructuredPropertyProposal()
            .getStructuredPropertyValues()
            .stream()
            .map(property -> property.getPropertyUrn())
            .collect(Collectors.toList());
      case AcrylConstants.ACTION_REQUEST_TYPE_CREATE_GLOSSARY_NODE_PROPOSAL:
      case AcrylConstants.ACTION_REQUEST_TYPE_CREATE_GLOSSARY_TERM_PROPOSAL:
      case AcrylConstants.ACTION_REQUEST_TYPE_UPDATE_DESCRIPTION_PROPOSAL:
        return Collections.emptyList();
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported action request type %s provided!", info.getType()));
    }
  }

  private String getAction(ActionRequestStatus status) {
    switch (status.getResult()) {
      case AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED:
        // Resolve and present the name of the tag.
        return "accepted";
      case AcrylConstants.ACTION_REQUEST_RESULT_REJECTED:
        // Resolve and present the name of the term.
        return "rejected";
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported action request type %s provided!", status.getResult()));
    }
  }

  private String toSerializedJsonArray(@Nonnull final List<String> list) {
    // Use jackson to serialize the list to a JSON array.
    try {
      return _objectMapper.writeValueAsString(list);
    } catch (Exception e) {
      log.error("Failed to serialize list to JSON array", e);
      return "[]";
    }
  }

  private String toSerializedJsonObject(@Nonnull final RecordTemplate template) {
    // Use jackson to serialize the list to a JSON array.
    try {
      return RecordUtils.toJsonString(template);
    } catch (Exception e) {
      log.error("Failed to serialize list to JSON object", e);
      return "{}";
    }
  }
}
