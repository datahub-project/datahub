package com.linkedin.datahub.graphql.resolvers.role;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.CorpUserInvitationStatus;
import com.linkedin.identity.InvitationStatus;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * Shared service for user suggestion dismissal functionality. Contains common logic used by both
 * single and batch dismiss resolvers.
 */
@Slf4j
public class UserSuggestionDismissalService {

  private final EntityClient _entityClient;
  private final EntityService<?> _entityService;

  public UserSuggestionDismissalService(
      final EntityClient entityClient, final EntityService<?> entityService) {
    _entityClient = entityClient;
    _entityService = entityService;
  }

  /**
   * Validates that the user has authorization to dismiss user suggestions.
   *
   * @param context the query context
   * @throws AuthorizationException if user is not authorized
   */
  public void validateAuthorization(final QueryContext context) {
    if (!AuthorizationUtils.canManageUsersAndGroups(context)) {
      throw new AuthorizationException(
          "Unauthorized to dismiss user suggestions. Please contact your DataHub administrator.");
    }
  }

  /**
   * Creates an audit stamp for the current operation.
   *
   * @param context the query context
   * @return a new audit stamp with current timestamp and actor
   */
  public AuditStamp createAuditStamp(final QueryContext context) {
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(UrnUtils.getUrn(context.getActorUrn()));
    return auditStamp;
  }

  /**
   * Creates a dismissed invitation status, preserving existing data if available.
   *
   * @param auditStamp the audit stamp for the operation
   * @param existingStatus the existing invitation status, if any
   * @return a new dismissed invitation status
   */
  public CorpUserInvitationStatus createDismissedStatus(
      final AuditStamp auditStamp, final CorpUserInvitationStatus existingStatus) {
    CorpUserInvitationStatus dismissedStatus = new CorpUserInvitationStatus();
    dismissedStatus.setStatus(InvitationStatus.SUGGESTION_DISMISSED);
    dismissedStatus.setInvitationToken(UUID.randomUUID().toString());
    dismissedStatus.setLastUpdated(auditStamp);

    if (existingStatus != null) {
      // Keep original creation time if it exists
      dismissedStatus.setCreated(existingStatus.getCreated());

      // Preserve role if it was set
      if (existingStatus.hasRole()) {
        dismissedStatus.setRole(existingStatus.getRole());
      }
    } else {
      // Create new invitation status with dismissed status
      dismissedStatus.setCreated(auditStamp);
    }

    return dismissedStatus;
  }

  /**
   * Creates a metadata change proposal for a user dismissal.
   *
   * @param corpUserUrn the user URN
   * @param dismissedStatus the dismissed invitation status
   * @return a metadata change proposal
   */
  public MetadataChangeProposal createMetadataChangeProposal(
      final CorpuserUrn corpUserUrn, final CorpUserInvitationStatus dismissedStatus) {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(corpUserUrn);
    mcp.setEntityType(Constants.CORP_USER_ENTITY_NAME);
    mcp.setAspectName(Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME);
    mcp.setAspect(GenericRecordUtils.serializeAspect(dismissedStatus));
    mcp.setChangeType(com.linkedin.events.metadata.ChangeType.UPSERT);
    return mcp;
  }

  /**
   * Ingests dismissal proposals using batch operation.
   *
   * @param context the query context
   * @param mcps the list of metadata change proposals
   * @param auditStamp the audit stamp for the operation
   * @throws Exception if ingestion fails
   */
  public void ingestDismissals(
      final QueryContext context,
      final List<MetadataChangeProposal> mcps,
      final AuditStamp auditStamp)
      throws Exception {
    AspectsBatchImpl aspectsBatch =
        AspectsBatchImpl.builder()
            .mcps(mcps, auditStamp, context.getOperationContext().getRetrieverContext())
            .build(context.getOperationContext());

    _entityService.ingestProposal(context.getOperationContext(), aspectsBatch, false);
  }

  /**
   * Gets existing invitation status for a single user.
   *
   * @param context the query context
   * @param corpUserUrn the user URN
   * @return the existing invitation status, or null if not found
   * @throws Exception if retrieval fails
   */
  public CorpUserInvitationStatus getExistingInvitationStatus(
      final QueryContext context, final CorpuserUrn corpUserUrn) throws Exception {
    EntityResponse entityResponse =
        _entityClient.getV2(
            context.getOperationContext(),
            Constants.CORP_USER_ENTITY_NAME,
            corpUserUrn,
            Set.of(Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME));

    if (entityResponse != null
        && entityResponse
            .getAspects()
            .containsKey(Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME)) {
      EnvelopedAspect envelopedAspect =
          entityResponse.getAspects().get(Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME);
      return new CorpUserInvitationStatus(envelopedAspect.getValue().data());
    }

    return null;
  }

  /**
   * Gets existing invitation statuses for multiple users.
   *
   * @param context the query context
   * @param corpUserUrns the set of user URNs
   * @return a map of user URN to entity response
   * @throws Exception if retrieval fails
   */
  public Map<Urn, EntityResponse> getExistingInvitationStatuses(
      final QueryContext context, final Set<CorpuserUrn> corpUserUrns) throws Exception {
    return _entityClient.batchGetV2(
        context.getOperationContext(),
        Constants.CORP_USER_ENTITY_NAME,
        new java.util.HashSet<>(corpUserUrns),
        Set.of(Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME));
  }

  /**
   * Extracts invitation status from entity response.
   *
   * @param entityResponse the entity response
   * @return the invitation status, or null if not found
   */
  public CorpUserInvitationStatus extractInvitationStatus(final EntityResponse entityResponse) {
    if (entityResponse != null
        && entityResponse
            .getAspects()
            .containsKey(Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME)) {
      EnvelopedAspect envelopedAspect =
          entityResponse.getAspects().get(Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME);
      return new CorpUserInvitationStatus(envelopedAspect.getValue().data());
    }
    return null;
  }
}
