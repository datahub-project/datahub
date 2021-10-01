package com.linkedin.datahub.graphql.resolvers.group;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.client.AspectClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.GroupMembership;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.utils.GenericAspectUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;

// Open Questions: Should we verify the user exists? Should we verify the group exists?
public class RemoveGroupMemberResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final AspectClient _aspectClient;

  public RemoveGroupMemberResolver(final AspectClient aspectClient) {
    _aspectClient = aspectClient;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    if (AuthorizationUtils.canManageUsersAndGroups(context)) {
      final String groupUrnStr = environment.getArgument("groupUrn");
      final String userUrnStr = environment.getArgument("userUrn");

      return CompletableFuture.supplyAsync(() -> {
        try {
          // First, fetch user's group membership aspect.
          final VersionedAspect gmsAspect = _aspectClient.getAspectOrNull(
              userUrnStr,
              Constants.GROUP_MEMBERSHIP_ASPECT_NAME,
              Constants.ASPECT_LATEST_VERSION,
              context.getActor());

          if (gmsAspect == null) {
            // Nothing to do, as the user is not in the group. Return false as the user was
            return false;
          }

          final GroupMembership groupMembership = gmsAspect.getAspect().getGroupMembership();
          if (groupMembership.getGroups().remove(Urn.createFromString(groupUrnStr))) {
            // Finally, create the MetadataChangeProposal.
            final MetadataChangeProposal proposal = new MetadataChangeProposal();
            proposal.setEntityUrn(Urn.createFromString(userUrnStr));
            proposal.setEntityType(Constants.CORP_USER_ENTITY_NAME);
            proposal.setAspectName(Constants.GROUP_MEMBERSHIP_ASPECT_NAME);
            proposal.setAspect(GenericAspectUtils.serializeAspect(groupMembership));
            proposal.setChangeType(ChangeType.UPSERT);
            _aspectClient.ingestProposal(proposal, context.getActor());
            return true;
          }
          return false;
        } catch (Exception e) {
          throw new RuntimeException("Failed to remove member from group", e);
        }
      });
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}