package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.datahub.authentication.invite.InviteTokenService;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
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
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/** Resolver for revoking user invitations and invalidating invitation tokens. */
@Slf4j
public class RevokeUserInvitationResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;
  private final EntityService<?> _entityService;
  private final InviteTokenService _inviteTokenService;

  public RevokeUserInvitationResolver(
      final EntityClient entityClient,
      final EntityService<?> entityService,
      final InviteTokenService inviteTokenService) {
    _entityClient = entityClient;
    _entityService = entityService;
    _inviteTokenService = inviteTokenService;
  }

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final QueryContext context = environment.getContext();
          final String userUrn = bindArgument(environment.getArgument("userUrn"), String.class);

          log.info("User {} revoking invitation for user {}", context.getActorUrn(), userUrn);

          if (!AuthorizationUtils.canManageUsersAndGroups(context)) {
            throw new AuthorizationException(
                "Unauthorized to revoke user invitations. Please contact your DataHub administrator.");
          }

          try {
            return revokeInvitation(context, userUrn);
          } catch (Exception e) {
            log.error("Failed to revoke invitation for user: {}", userUrn, e);
            throw new RuntimeException("Failed to revoke user invitation", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private Boolean revokeInvitation(final QueryContext context, final String userUrn)
      throws Exception {
    CorpuserUrn corpUserUrn = CorpuserUrn.createFromString(userUrn);

    // Get the current invitation status aspect
    EntityResponse entityResponse =
        _entityClient.getV2(
            context.getOperationContext(),
            Constants.CORP_USER_ENTITY_NAME,
            corpUserUrn,
            Set.of(Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME));

    if (entityResponse == null
        || !entityResponse
            .getAspects()
            .containsKey(Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME)) {
      log.warn("No invitation status found for user: {}", userUrn);
      return false;
    }

    EnvelopedAspect envelopedAspect =
        entityResponse.getAspects().get(Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME);
    CorpUserInvitationStatus currentStatus =
        new CorpUserInvitationStatus(envelopedAspect.getValue().data());

    // Only revoke if the invitation is currently SENT
    if (currentStatus.getStatus() != InvitationStatus.SENT) {
      log.warn(
          "Cannot revoke invitation for user {} - current status is {}",
          userUrn,
          currentStatus.getStatus());
      return false;
    }

    // Invalidate the invitation token
    String invitationToken = currentStatus.getInvitationToken();
    if (invitationToken != null) {
      try {
        _inviteTokenService.revokeInviteToken(context.getOperationContext(), invitationToken);
        log.info("Successfully invalidated invitation token for user: {}", userUrn);
      } catch (Exception e) {
        log.error("Failed to invalidate invitation token for user: {}", userUrn, e);
        // Continue with status update even if token invalidation fails
      }
    }

    // Update the invitation status to REVOKED
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(UrnUtils.getUrn(context.getActorUrn()));

    CorpUserInvitationStatus revokedStatus = new CorpUserInvitationStatus();
    revokedStatus.setStatus(InvitationStatus.REVOKED);
    revokedStatus.setInvitationToken(invitationToken); // Keep the original token for audit purposes
    revokedStatus.setCreated(currentStatus.getCreated()); // Keep original creation time
    revokedStatus.setLastUpdated(auditStamp); // Update the last modified time

    // Preserve role if it was set
    if (currentStatus.hasRole()) {
      revokedStatus.setRole(currentStatus.getRole());
    }

    // Create metadata change proposal
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(corpUserUrn);
    mcp.setEntityType(Constants.CORP_USER_ENTITY_NAME);
    mcp.setAspectName(Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME);
    mcp.setAspect(GenericRecordUtils.serializeAspect(revokedStatus));
    mcp.setChangeType(com.linkedin.events.metadata.ChangeType.UPSERT);

    // Create AspectsBatch with single MCP
    AspectsBatchImpl aspectsBatch =
        AspectsBatchImpl.builder()
            .mcps(List.of(mcp), auditStamp, context.getOperationContext().getRetrieverContext())
            .build(context.getOperationContext());

    // Ingest the aspect
    _entityService.ingestProposal(context.getOperationContext(), aspectsBatch, false);

    log.info("Successfully revoked invitation for user: {}", userUrn);
    return true;
  }
}
