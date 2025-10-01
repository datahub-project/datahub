package com.linkedin.datahub.graphql.resolvers.role;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.invite.InviteTokenService;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.generated.SendUserInvitationsInput;
import com.linkedin.datahub.graphql.generated.SendUserInvitationsResult;
import com.linkedin.datahub.graphql.resolvers.settings.SettingsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.event.notification.NotificationMessage;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientArray;
import com.linkedin.event.notification.NotificationRecipientType;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.event.notification.template.NotificationTemplateType;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.CorpUserInvitationStatus;
import com.linkedin.identity.InvitationStatus;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.settings.global.GlobalSettingsInfo;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class UserInvitationService {
  private final IntegrationsService integrationsService;
  private final InviteTokenService inviteTokenService;
  private final EntityService<?> entityService;
  private final EntityClient entityClient;
  private final String baseUrl;

  public UserInvitationService(
      IntegrationsService integrationsService,
      InviteTokenService inviteTokenService,
      EntityService<?> entityService,
      EntityClient entityClient,
      @Value("${baseUrl:http://localhost:9002}") String baseUrl) {
    this.integrationsService = integrationsService;
    this.inviteTokenService = inviteTokenService;
    this.entityService = entityService;
    this.entityClient = entityClient;
    this.baseUrl =
        baseUrl != null && !baseUrl.trim().isEmpty() && !"null".equals(baseUrl)
            ? baseUrl
            : "http://localhost:9002";
    log.info("UserInvitationService initialized with baseUrl: {}", this.baseUrl);
  }

  public SendUserInvitationsResult sendUserInvitations(
      OperationContext operationContext,
      SendUserInvitationsInput input,
      Authentication authentication) {

    final List<String> emails = input.getEmails();
    final String roleUrn = input.getRoleUrn();
    final String subject = input.getSubject();

    try {
      List<String> errors = new ArrayList<>();

      // Generate individual tokens for bulk invitations - one unique token per user
      List<String> individualTokens =
          inviteTokenService.generateIndividualTokens(operationContext, emails.size(), roleUrn);

      // Resolve inviter display name once (not per invitation)
      Urn inviterUrn = UrnUtils.getUrn(authentication.getActor().toUrnStr());
      String inviterDisplayName = resolveUserDisplayName(operationContext, inviterUrn);

      // Process invitations in parallel using CompletableFuture
      List<java.util.concurrent.CompletableFuture<Boolean>> futures = new ArrayList<>();

      for (int i = 0; i < emails.size(); i++) {
        final String email = emails.get(i).trim(); // Trim whitespace from email
        final String inviteToken = individualTokens.get(i); // Get unique token for this email
        final String inviteLink = buildInviteLink(operationContext, inviteToken);

        // Log for debugging
        log.debug(
            "Generated invite link with baseUrl: {}, inviteToken: {}, result: {}",
            baseUrl,
            inviteToken,
            inviteLink);

        // Create async task for each invitation
        java.util.concurrent.CompletableFuture<Boolean> future =
            java.util.concurrent.CompletableFuture.supplyAsync(
                () -> {
                  try {
                    log.info(
                        "Sending invitation to email: {} with role: {} and subject: {} from user: {}",
                        email,
                        roleUrn,
                        subject,
                        authentication.getActor().toUrnStr());

                    // Create notification parameters
                    Map<String, String> parametersMap = new HashMap<>();
                    parametersMap.put("recipientEmail", email);
                    parametersMap.put("inviterName", inviterDisplayName);
                    parametersMap.put("inviteLink", inviteLink);

                    if (subject != null) {
                      parametersMap.put("title", subject);
                    }
                    if (roleUrn != null && !roleUrn.trim().isEmpty()) {
                      parametersMap.put("roleName", roleUrn);
                    }

                    // Create notification message
                    NotificationMessage message = new NotificationMessage();
                    message.setTemplate(NotificationTemplateType.INVITATION);
                    message.setParameters(new StringMap(parametersMap));

                    // Create recipient
                    NotificationRecipient recipient = new NotificationRecipient();
                    recipient.setType(NotificationRecipientType.EMAIL);
                    recipient.setId(email);
                    recipient.setDisplayName(email);

                    // Build notification request
                    NotificationRequest notificationRequest = new NotificationRequest();
                    notificationRequest.setMessage(message);
                    notificationRequest.setRecipients(
                        new NotificationRecipientArray(List.of(recipient)));

                    // Send notification via integrations service
                    integrationsService.sendNotification(notificationRequest);

                    // Create CorpUserInvitationStatus aspect for the invited user
                    createInvitationAspect(
                        operationContext, email, roleUrn, inviteToken, authentication);

                    log.info("Successfully sent invitation to email: {}", email);
                    return true;
                  } catch (Exception e) {
                    String errorMsg =
                        String.format("Failed to send invitation to %s: %s", email, e.getMessage());
                    synchronized (errors) {
                      errors.add(errorMsg);
                    }
                    log.error(errorMsg, e);
                    return false;
                  }
                });

        futures.add(future);
      }

      // Wait for all invitations to complete
      java.util.concurrent.CompletableFuture<Void> allFutures =
          java.util.concurrent.CompletableFuture.allOf(
              futures.toArray(new java.util.concurrent.CompletableFuture[0]));

      // Block until all complete (with reasonable timeout to avoid hanging)
      try {
        allFutures.get(60, java.util.concurrent.TimeUnit.SECONDS);
      } catch (java.util.concurrent.TimeoutException e) {
        log.error("Timeout waiting for bulk invitations to complete after 60 seconds");
        errors.add("Some invitations timed out");
      }

      // Count successes
      long successCount =
          futures.stream()
              .filter(
                  f -> {
                    try {
                      return f.get();
                    } catch (Exception e) {
                      return false;
                    }
                  })
              .count();

      SendUserInvitationsResult result = new SendUserInvitationsResult();
      result.setSuccess(errors.isEmpty());
      result.setInvitationsSent((int) successCount);
      result.setErrors(errors);

      return result;
    } catch (Exception e) {
      log.error("Failed to send user invitations", e);
      throw new RuntimeException("Failed to send user invitations", e);
    }
  }

  /**
   * Resolves a user URN to get their display name. Falls back to first+last name, then URN ID if
   * display name is not available.
   */
  private String resolveUserDisplayName(OperationContext operationContext, Urn userUrn) {
    try {
      Set<String> aspectNames = new HashSet<>();
      aspectNames.add(Constants.CORP_USER_INFO_ASPECT_NAME);

      Map<Urn, EntityResponse> entityResponses =
          entityService.getEntitiesV2(operationContext, "corpuser", Set.of(userUrn), aspectNames);

      EntityResponse entityResponse = entityResponses.get(userUrn);
      if (entityResponse == null) {
        log.warn("Could not find user entity for URN: {}", userUrn);
        return userUrn.getId(); // Fallback to URN ID
      }

      EnvelopedAspect envelopedCorpUserInfo =
          entityResponse.getAspects().get(Constants.CORP_USER_INFO_ASPECT_NAME);

      if (envelopedCorpUserInfo == null) {
        log.warn("CorpUserInfo aspect not found for user: {}", userUrn);
        return userUrn.getId(); // Fallback to URN ID
      }

      CorpUserInfo corpUserInfo = new CorpUserInfo(envelopedCorpUserInfo.getValue().data());

      // Try display name first
      if (corpUserInfo.hasDisplayName() && corpUserInfo.getDisplayName() != null) {
        return corpUserInfo.getDisplayName();
      }

      // Try first + last name
      String fullName = getUserFullName(corpUserInfo.getFirstName(), corpUserInfo.getLastName());
      if (fullName != null) {
        return fullName;
      }

      // Fallback to URN ID
      return userUrn.getId();

    } catch (Exception e) {
      log.warn("Failed to resolve display name for user: {}", userUrn, e);
      return userUrn.getId(); // Fallback to URN ID
    }
  }

  /** Combines first and last name into a full name. */
  private String getUserFullName(String firstName, String lastName) {
    if (firstName != null && lastName != null) {
      return firstName + " " + lastName;
    }
    return null;
  }

  /**
   * Creates a CorpUserInvitationStatus aspect for the invited user so they appear in the Manage
   * Users & Groups table with "Invited" status.
   */
  private void createInvitationAspect(
      OperationContext operationContext,
      String email,
      String roleUrn,
      String inviteToken,
      Authentication authentication) {
    try {
      // Create URN for the user using their email as the username (trim whitespace)
      CorpuserUrn userUrn = new CorpuserUrn(email.trim());

      // Create audit stamp
      AuditStamp auditStamp = new AuditStamp();
      auditStamp.setTime(System.currentTimeMillis());
      auditStamp.setActor(UrnUtils.getUrn(authentication.getActor().toUrnStr()));

      // Create the invitation status aspect
      CorpUserInvitationStatus invitationStatus = new CorpUserInvitationStatus();
      invitationStatus.setStatus(InvitationStatus.SENT);
      invitationStatus.setInvitationToken(inviteToken);
      invitationStatus.setCreated(auditStamp);
      invitationStatus.setLastUpdated(auditStamp);

      // Set role if provided
      if (roleUrn != null && !roleUrn.trim().isEmpty()) {
        invitationStatus.setRole(UrnUtils.getUrn(roleUrn));
      }

      // Create metadata change proposal
      MetadataChangeProposal mcp = new MetadataChangeProposal();
      mcp.setEntityUrn(userUrn);
      mcp.setEntityType(Constants.CORP_USER_ENTITY_NAME);
      mcp.setAspectName(Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME);
      mcp.setAspect(GenericRecordUtils.serializeAspect(invitationStatus));
      mcp.setChangeType(com.linkedin.events.metadata.ChangeType.UPSERT);

      // Create AspectsBatch with single MCP
      AspectsBatchImpl aspectsBatch =
          AspectsBatchImpl.builder()
              .mcps(List.of(mcp), auditStamp, operationContext.getRetrieverContext())
              .build(operationContext);

      // Ingest the aspect
      entityService.ingestProposal(operationContext, aspectsBatch, false);

      log.info(
          "Successfully created invitation aspect for user: {} with token: {}", email, inviteToken);
    } catch (Exception e) {
      log.error("Failed to create invitation aspect for user: {}", email, e);
      // Don't throw exception here to avoid breaking the invitation flow
    }
  }

  /**
   * Builds the invite link with conditional redirect_on_sso parameter based on SSO configuration.
   */
  private String buildInviteLink(OperationContext operationContext, String inviteToken) {
    try {
      boolean ssoEnabled = isSsoEnabled(operationContext);
      String redirectParam = ssoEnabled ? "&redirect_on_sso=true" : "";
      return String.format("%s/signup?invite_token=%s%s", baseUrl, inviteToken, redirectParam);
    } catch (Exception e) {
      log.warn(
          "Failed to check SSO status, falling back to including redirect_on_sso parameter", e);
      // Fallback to the previous behavior if we can't determine SSO status
      return String.format("%s/signup?invite_token=%s&redirect_on_sso=true", baseUrl, inviteToken);
    }
  }

  /** Checks if SSO is enabled by retrieving the global settings. */
  private boolean isSsoEnabled(OperationContext operationContext) {
    try {
      GlobalSettingsInfo globalSettings =
          SettingsMapper.getGlobalSettings(operationContext, entityClient);
      if (globalSettings.hasSso() && globalSettings.getSso() != null) {
        var ssoSettings = globalSettings.getSso();
        if (ssoSettings != null && ssoSettings.hasOidcSettings()) {
          var oidcSettings = ssoSettings.getOidcSettings();
          if (oidcSettings != null) {
            return oidcSettings.isEnabled();
          }
        }
      }
      return false;
    } catch (Exception e) {
      log.warn("Failed to retrieve SSO settings", e);
      return false;
    }
  }
}
