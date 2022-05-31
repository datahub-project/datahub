package com.linkedin.metadata.kafka.hook.notification.proposal;

import com.datahub.authentication.Authentication;
import com.datahub.notification.NotificationTemplateType;
import com.datahub.notification.provider.EntityNameProvider;
import com.datahub.notification.provider.SettingsProvider;
import com.google.common.collect.ImmutableSet;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.kafka.hook.notification.BaseMclNotificationGenerator;
import com.linkedin.metadata.kafka.hook.notification.NotificationScenarioType;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.kafka.hook.notification.NotificationUtils.*;


/**
 * An extension of {@link BaseMclNotificationGenerator} which generates notifications
 * on incident creation and status changes.
 */
@Slf4j
public class ProposalNotificationGenerator extends BaseMclNotificationGenerator {

  private final EntityNameProvider entityNameProvider;

  public ProposalNotificationGenerator(
      @Nonnull final EventProducer eventProducer,
      @Nonnull final EntityClient entityClient,
      @Nonnull final GraphClient graphClient,
      @Nonnull final SettingsProvider settingsProvider,
      @Nonnull final Authentication systemAuthentication) {
    super(eventProducer, entityClient, graphClient, settingsProvider, systemAuthentication);
    this.entityNameProvider = new EntityNameProvider(entityClient, systemAuthentication);
  }

  @Override
  public void generate(@Nonnull MetadataChangeLog event) {
    if (!isEligibleForProcessing(event)) {
      return;
    }

    log.debug(String.format("Found eligible Action Request MCL. urn: %s", event.getEntityUrn()));

    if (isNewProposal(event)) {
      if (event.getAspect() == null) {
        return;
      }

      log.debug(String.format("Found eligible new proposal event to notify. urn: %s", event.getEntityUrn().toString()));

      generateNewProposalNotifications(
          event.getEntityUrn(),
          GenericRecordUtils.deserializeAspect(
              event.getAspect().getValue(),
              event.getAspect().getContentType(),
              ActionRequestInfo.class),
          event.getCreated() // Should always be present.
      );
    } else if (isProposalStatusChange(event)) {

      log.debug(String.format("Found eligible proposal change event to notify. urn: %s", event.getEntityUrn().toString()));

      generateUpdatedProposalNotifications(
          event.getEntityUrn(),
          GenericRecordUtils.deserializeAspect(
              event.getAspect().getValue(),
              event.getAspect().getContentType(),
              ActionRequestStatus.class),
          event.getCreated() // Should always be present.
      );
    }
  }

  private boolean isEligibleForProcessing(final MetadataChangeLog event) {
    if (event.getEntityUrn() == null) {
      return false;
    }
    return (Constants.ACTION_REQUEST_INFO_ASPECT_NAME.equals(event.getAspectName())
        || Constants.ACTION_REQUEST_STATUS_ASPECT_NAME.equals(event.getAspectName()))
        && (ChangeType.UPSERT.equals(event.getChangeType()) || ChangeType.CREATE.equals(event.getChangeType()));
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
      @Nonnull final ActionRequestStatus newStatus,
      @Nonnull final AuditStamp auditStamp) {
      // Broadcast incident status change.
      trySendProposalStatusChangeNotifications(urn, newStatus, auditStamp);
  }

  /**
   * Sends a notification of template type "BROADCAST_NEW_PROPOSAL" when an proposal is created.
   */
  private void trySendNewProposalNotifications(
      @Nonnull final Urn urn,
      @Nonnull final ActionRequestInfo info,
      @Nonnull final AuditStamp auditStamp) {

    final Urn entityUrn = UrnUtils.getUrn(info.getResource());

    Set<NotificationRecipient> recipients = new HashSet<>(buildRecipients(NotificationScenarioType.NEW_PROPOSAL, entityUrn));
    if (recipients.isEmpty()) {
      return;
    }

    final String subResource = info.getSubResource();
    final String subResourceType = info.getSubResourceType();
    final String operation = getOperation(info); // add, remove
    final Urn modifierUrn = getModifierUrn(info);
    final String modifierType = getModifierType(info);
    final String modifierName = getEntityDisplayName(modifierUrn);

    final Map<String, String> templateParams = new HashMap<>();
    templateParams.put("entityName", getEntityDisplayName(entityUrn));
    templateParams.put("entityType", getEntityType(entityUrn));
    templateParams.put("entityPath", generateEntityPath(entityUrn));
    templateParams.put("operation", operation);
    templateParams.put("modifierType", modifierType);
    templateParams.put("modifierName", modifierName);
    templateParams.put("modifierPath", generateEntityPath(modifierUrn));
    templateParams.put("actorUrn", auditStamp.getActor().toString());

    if (subResource != null && subResourceType != null) {
      templateParams.put("subResourceType", subResourceType);
      templateParams.put("subResource", subResource);
    }

    final NotificationRequest notificationRequest = buildNotificationRequest(
        NotificationTemplateType.BROADCAST_NEW_PROPOSAL.name(),
        templateParams,
        recipients
    );

    log.info(String.format("Broadcasting new proposal change for entity %s, action request %s...", entityUrn, urn));
    sendNotificationRequest(notificationRequest);
  }

  /**
   * Sends a notification of template type "BROADCAST_PROPOSAL_STATUS_CHANGE" when an proposal's status is changed.
   */
  private void trySendProposalStatusChangeNotifications(
      @Nonnull final Urn urn,
      @Nonnull final ActionRequestStatus newStatus,
      @Nonnull final AuditStamp auditStamp) {

    final ActionRequestInfo info = getActionRequestInfo(urn);

    if (info == null || !info.hasResource()) {
      log.warn(String.format("Failed to find Action Request info for action request with urn %s. Skipping broadcasting status change", urn));
      return;
    }
    final Urn entityUrn = UrnUtils.getUrn(info.getResource());

    Set<NotificationRecipient> recipients = new HashSet<>(buildRecipients(NotificationScenarioType.PROPOSAL_STATUS_CHANGE, entityUrn));
    if (recipients.isEmpty()) {
      return;
    }

    final String entityName = getEntityDisplayName(entityUrn);
    final String subResource = info.getSubResource();
    final String subResourceType = info.getSubResourceType();
    final String operation = getOperation(info); // add, remove
    final Urn modifierUrn = getModifierUrn(info);
    final String modifierType = getModifierType(info);
    final String modifierName = getEntityDisplayName(modifierUrn);
    final String action = getAction(newStatus);

    final Map<String, String> templateParams = new HashMap<>();
    templateParams.put("entityName", entityName);
    templateParams.put("entityPath", generateEntityPath(entityUrn));
    templateParams.put("entityType", getEntityType(entityUrn));
    templateParams.put("operation", operation);
    templateParams.put("modifierType", modifierType);
    templateParams.put("modifierName", modifierName);
    templateParams.put("modifierPath", generateEntityPath(modifierUrn));
    templateParams.put("actorUrn", auditStamp.getActor().toString());
    templateParams.put("action", action);

    if (subResource != null && subResourceType != null) {
      templateParams.put("subResourceType", subResourceType);
      templateParams.put("subResource", subResource);
    }

    final NotificationRequest notificationRequest = buildNotificationRequest(
        NotificationTemplateType.BROADCAST_PROPOSAL_STATUS_CHANGE.name(),
        templateParams,
        recipients
    );

    // TODO: Remove this log once we've validated.
    log.info(String.format("Broadcasting proposal status change for entity %s, action request %s...", entityUrn, urn));
    sendNotificationRequest(notificationRequest);

  }

  private ActionRequestInfo getActionRequestInfo(final Urn actionRequestUrn) {
    try {
      EntityResponse entityResponse = _entityClient.getV2(
          Constants.ACTION_REQUEST_ENTITY_NAME,
          actionRequestUrn,
          ImmutableSet.of(Constants.ACTION_REQUEST_INFO_ASPECT_NAME),
          _systemAuthentication
      );
      if (entityResponse != null && entityResponse.hasAspects() && entityResponse.getAspects().containsKey(Constants.ACTION_REQUEST_INFO_ASPECT_NAME)) {
        return new ActionRequestInfo(entityResponse.getAspects().get(Constants.ACTION_REQUEST_INFO_ASPECT_NAME).getValue().data());
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to retrieve Action Request Info", e);
    }
  }

  private boolean isNewProposal(final MetadataChangeLog event) {
    return Constants.ACTION_REQUEST_INFO_ASPECT_NAME.equals(event.getAspectName()) && event.getPreviousAspectValue() == null;
  }

  private boolean isProposalStatusChange(final MetadataChangeLog event) {
    if (event.getAspect() == null || !Constants.ACTION_REQUEST_STATUS_ASPECT_NAME.equals(event.getAspectName())) {
      return false;
    }
    final ActionRequestStatus newStatus = GenericRecordUtils.deserializeAspect(
        event.getAspect().getValue(),
        event.getAspect().getContentType(),
        ActionRequestStatus.class);

    // If new status is not complete, we simply ignore for now.
    if (!AcrylConstants.ACTION_REQUEST_STATUS_COMPLETE.equals(newStatus.getStatus())) {
      return false;
    }

    final ActionRequestStatus prevStatus = GenericRecordUtils.deserializeAspect(
        event.getPreviousAspectValue().getValue(),
        event.getPreviousAspectValue().getContentType(),
        ActionRequestStatus.class);
    return !prevStatus.getStatus().equals(newStatus.getStatus());
  }

  private String getOperation(ActionRequestInfo info) {
    switch (info.getType()) {
      case AcrylConstants.ACTION_REQUEST_TYPE_TAG_PROPOSAL:
      case AcrylConstants.ACTION_REQUEST_TYPE_TERM_PROPOSAL:
        return "add";
      default:
        throw new IllegalArgumentException(String.format("Unsupported action request type %s provided!", info.getType()));
    }
  }

  private String getModifierType(ActionRequestInfo info) {
    switch (info.getType()) {
      case AcrylConstants.ACTION_REQUEST_TYPE_TAG_PROPOSAL:
        return "Tag";
      case AcrylConstants.ACTION_REQUEST_TYPE_TERM_PROPOSAL:
        return "Glossary Term";
      default:
        throw new IllegalArgumentException(String.format("Unsupported action request type %s provided!", info.getType()));
    }
  }

  private String getEntityDisplayName(Urn entityUrn) {
    return entityNameProvider.getName(entityUrn);
  }

  private Urn getModifierUrn(ActionRequestInfo info) {
    switch (info.getType()) {
      case AcrylConstants.ACTION_REQUEST_TYPE_TAG_PROPOSAL:
        // Resolve and present the name of the tag.
        return info.getParams().getTagProposal().getTag();
      case AcrylConstants.ACTION_REQUEST_TYPE_TERM_PROPOSAL:
        // Resolve and present the name of the term.
        return info.getParams().getGlossaryTermProposal().getGlossaryTerm();
      default:
        throw new IllegalArgumentException(String.format("Unsupported action request type %s provided!", info.getType()));
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
        throw new IllegalArgumentException(String.format("Unsupported action request type %s provided!", status.getResult()));
    }
  }
}