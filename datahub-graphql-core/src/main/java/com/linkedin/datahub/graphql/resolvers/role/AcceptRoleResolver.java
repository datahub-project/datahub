package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.invite.InviteTokenService;
import com.datahub.authorization.role.RoleService;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.AcceptRoleInput;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserInvitationStatus;
import com.linkedin.identity.InvitationStatus;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class AcceptRoleResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final RoleService _roleService;
  private final InviteTokenService _inviteTokenService;
  private final EntityService<?> _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    final AcceptRoleInput input =
        bindArgument(environment.getArgument("input"), AcceptRoleInput.class);
    final String inviteTokenStr = input.getInviteToken();
    final Authentication authentication = context.getAuthentication();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final Urn inviteTokenUrn = _inviteTokenService.getInviteTokenUrn(inviteTokenStr);
            if (!_inviteTokenService.isInviteTokenValid(
                context.getOperationContext(), inviteTokenUrn)) {
              throw new RuntimeException(
                  String.format("Invite token %s is invalid", inviteTokenStr));
            }

            final Urn roleUrn =
                _inviteTokenService.getInviteTokenRole(
                    context.getOperationContext(), inviteTokenUrn);
            _roleService.batchAssignRoleToActors(
                context.getOperationContext(),
                Collections.singletonList(authentication.getActor().toUrnStr()),
                roleUrn);

            // Update the user's invitation status to ACCEPTED
            updateInvitationStatusToAcceptedIfExists(
                context, authentication.getActor().toUrnStr(), inviteTokenStr, roleUrn);

            // Check if this is an individual token and consume it (single-use)
            if (_inviteTokenService.isIndividualToken(
                context.getOperationContext(), inviteTokenUrn)) {
              log.info(
                  "Consuming individual invite token {} for user {}",
                  inviteTokenStr,
                  authentication.getActor().toUrnStr());
              _inviteTokenService.consumeIndividualToken(
                  context.getOperationContext(), inviteTokenUrn);
            }

            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to accept role using invite token %s", inviteTokenStr), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Updates the user's invitation status from SENT to ACCEPTED when they accept the role invite.
   * This ensures the UI shows an editable role dropdown instead of static text.
   */
  private void updateInvitationStatusToAcceptedIfExists(
      QueryContext context, String userUrnStr, String inviteToken, Urn roleUrn) {
    try {
      Urn userUrn = UrnUtils.getUrn(userUrnStr);

      // Get the existing invitation status
      CorpUserInvitationStatus existingStatus =
          (CorpUserInvitationStatus)
              _entityService.getLatestAspect(
                  context.getOperationContext(), userUrn, CORP_USER_INVITATION_STATUS_ASPECT_NAME);

      if (existingStatus != null && InvitationStatus.SENT.equals(existingStatus.getStatus())) {
        // Create updated invitation status
        CorpUserInvitationStatus updatedStatus = existingStatus.copy();
        updatedStatus.setStatus(InvitationStatus.ACCEPTED);
        updatedStatus.setLastUpdated(
            new AuditStamp()
                .setActor(UrnUtils.getUrn(context.getAuthentication().getActor().toUrnStr()))
                .setTime(System.currentTimeMillis()));

        // Create metadata change proposal
        MetadataChangeProposal mcp = new MetadataChangeProposal();
        mcp.setEntityUrn(userUrn);
        mcp.setEntityType(CORP_USER_ENTITY_NAME);
        mcp.setAspectName(CORP_USER_INVITATION_STATUS_ASPECT_NAME);
        mcp.setAspect(GenericRecordUtils.serializeAspect(updatedStatus));
        mcp.setChangeType(ChangeType.UPSERT);

        // Update the aspect
        _entityService.ingestProposal(
            context.getOperationContext(),
            mcp,
            new AuditStamp()
                .setActor(UrnUtils.getUrn(context.getAuthentication().getActor().toUrnStr()))
                .setTime(System.currentTimeMillis()),
            false);

        log.info("Updated invitation status to ACCEPTED for user: {}", userUrnStr);
      }
    } catch (Exception e) {
      log.error("Failed to update invitation status for user: {}", userUrnStr, e);
      // Don't throw - this is not critical enough to fail the role assignment
    }
  }
}
