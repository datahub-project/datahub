package com.datahub.authorization;

import static com.linkedin.metadata.Constants.*;

import com.datahub.authentication.Authentication;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserInvitationStatus;
import com.linkedin.identity.InvitationStatus;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * Static utility for managing user invitation status lifecycle. Used by AcceptRoleResolver (native
 * user signup) and AuthServiceController (SSO login).
 */
@Slf4j
public class InvitationStatusUtils {

  /**
   * Checks if a user has an active invitation (SENT status) with a role.
   *
   * @param entityService the entity service
   * @param operationContext the operation context
   * @param userUrn the user URN to check
   * @return InvitationDetails if active invitation found, null otherwise
   */
  public static InvitationDetails getActiveInvitation(
      EntityService<?> entityService, OperationContext operationContext, CorpuserUrn userUrn) {
    try {
      // Get the user entity to check for invitation status
      EntityResponse userEntity =
          entityService.getEntityV2(
              operationContext,
              CORP_USER_ENTITY_NAME,
              userUrn,
              Set.of(CORP_USER_INVITATION_STATUS_ASPECT_NAME));

      if (userEntity != null
          && userEntity.getAspects().containsKey(CORP_USER_INVITATION_STATUS_ASPECT_NAME)) {
        EnvelopedAspect invitationAspect =
            userEntity.getAspects().get(CORP_USER_INVITATION_STATUS_ASPECT_NAME);

        if (invitationAspect != null && invitationAspect.getValue() != null) {
          CorpUserInvitationStatus invitationStatus =
              new CorpUserInvitationStatus(invitationAspect.getValue().data());
          if (InvitationStatus.SENT.equals(invitationStatus.getStatus())
              && invitationStatus.hasRole()
              && invitationStatus.getRole() != null) {

            return new InvitationDetails(
                invitationStatus.getRole().toString(),
                invitationStatus.getInvitationToken(),
                invitationStatus);
          }
        }
      }

      return null;

    } catch (Exception e) {
      log.warn("Failed to check invitation status for user '{}': {}", userUrn, e.getMessage(), e);
      return null;
    }
  }

  /**
   * Updates a user's invitation status to ACCEPTED.
   *
   * @param entityService the entity service
   * @param operationContext the operation context
   * @param userUrn the user URN
   * @param authentication the authentication context for audit stamps
   * @return true if successfully updated, false otherwise
   */
  public static boolean markInvitationAcceptedIfExists(
      EntityService<?> entityService,
      OperationContext operationContext,
      CorpuserUrn userUrn,
      Authentication authentication) {
    try {
      // Get current invitation status
      EntityResponse userEntity =
          entityService.getEntityV2(
              operationContext,
              CORP_USER_ENTITY_NAME,
              userUrn,
              Set.of(CORP_USER_INVITATION_STATUS_ASPECT_NAME));

      if (userEntity != null
          && userEntity.getAspects().containsKey(CORP_USER_INVITATION_STATUS_ASPECT_NAME)) {
        EnvelopedAspect invitationAspect =
            userEntity.getAspects().get(CORP_USER_INVITATION_STATUS_ASPECT_NAME);

        if (invitationAspect != null && invitationAspect.getValue() != null) {
          CorpUserInvitationStatus currentStatus =
              new CorpUserInvitationStatus(invitationAspect.getValue().data());

          // Update status to accepted
          CorpUserInvitationStatus updatedStatus = currentStatus.copy();
          updatedStatus.setStatus(InvitationStatus.ACCEPTED);
          updatedStatus.setLastUpdated(createAuditStamp(authentication));

          // Create metadata change proposal to persist the updated status
          MetadataChangeProposal mcp = new MetadataChangeProposal();
          mcp.setEntityUrn(userUrn);
          mcp.setEntityType(CORP_USER_ENTITY_NAME);
          mcp.setAspectName(CORP_USER_INVITATION_STATUS_ASPECT_NAME);
          mcp.setAspect(GenericRecordUtils.serializeAspect(updatedStatus));
          mcp.setChangeType(ChangeType.UPSERT);

          // Ingest the updated invitation status
          entityService.ingestProposal(
              operationContext, mcp, createAuditStamp(authentication), false);

          log.info("Successfully updated invitation status for user '{}' to ACCEPTED", userUrn);
          return true;
        }
      }

      return false;

    } catch (Exception e) {
      log.error("Failed to update invitation status for user '{}': {}", userUrn, e.getMessage(), e);
      return false;
    }
  }

  /** Helper method to create audit stamp from authentication context. */
  private static AuditStamp createAuditStamp(Authentication authentication) {
    return new AuditStamp()
        .setActor(UrnUtils.getUrn(authentication.getActor().toUrnStr()))
        .setTime(System.currentTimeMillis());
  }

  /** Data class containing details of an active invitation. */
  public static class InvitationDetails {
    private final String roleUrn;
    private final String invitationToken;
    private final CorpUserInvitationStatus originalStatus;

    public InvitationDetails(
        String roleUrn, String invitationToken, CorpUserInvitationStatus originalStatus) {
      this.roleUrn = roleUrn;
      this.invitationToken = invitationToken;
      this.originalStatus = originalStatus;
    }

    public String getRoleUrn() {
      return roleUrn;
    }

    public String getInvitationToken() {
      return invitationToken;
    }

    public CorpUserInvitationStatus getOriginalStatus() {
      return originalStatus;
    }
  }
}
