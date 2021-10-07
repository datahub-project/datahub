package com.linkedin.datahub.graphql.resolvers.group;

import com.linkedin.common.UrnArray;
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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


// Open Questions: Should we verify the user exists? Should we verify the group exists?
public class AddGroupMembersResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final AspectClient _aspectClient;

  public AddGroupMembersResolver(final AspectClient aspectClient) {
    _aspectClient = aspectClient;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    if (AuthorizationUtils.canManageUsersAndGroups(context)) {
      final String groupUrnStr = environment.getArgument("groupUrn");
      final List<String> userUrnStrs = environment.getArgument("userUrns");

      List<CompletableFuture<?>> removeGroupMemberFutures = userUrnStrs.stream().map(userUrnStr -> CompletableFuture.supplyAsync(() -> {
        try {
          // First, fetch user's group membership aspect.
          final VersionedAspect gmsAspect = _aspectClient.getAspectOrNull(
                userUrnStr,
                Constants.GROUP_MEMBERSHIP_ASPECT_NAME,
                Constants.ASPECT_LATEST_VERSION,
                context.getActor());

          GroupMembership groupMembership;
          if (gmsAspect == null) {
            // If the user doesn't have one, create one.
            groupMembership = new GroupMembership();
            groupMembership.setGroups(new UrnArray());
          } else {
            groupMembership = gmsAspect.getAspect().getGroupMembership();
          }
          // Handle the duplicate case.
          final Urn groupUrn = Urn.createFromString(groupUrnStr);
          groupMembership.getGroups().remove(groupUrn);
          groupMembership.getGroups().add(groupUrn);

          // Finally, create the MetadataChangeProposal.
          final MetadataChangeProposal proposal = new MetadataChangeProposal();
          proposal.setEntityUrn(Urn.createFromString(userUrnStr));
          proposal.setEntityType(Constants.CORP_USER_ENTITY_NAME);
          proposal.setAspectName(Constants.GROUP_MEMBERSHIP_ASPECT_NAME);
          proposal.setAspect(GenericAspectUtils.serializeAspect(groupMembership));
          proposal.setChangeType(ChangeType.UPSERT);
          _aspectClient.ingestProposal(proposal, context.getActor());
          return true;
        } catch (Exception e) {
          throw new RuntimeException("Failed to add member to group", e);
        }
      })).collect(Collectors.toList());
      return CompletableFuture.allOf(removeGroupMemberFutures.toArray(new CompletableFuture[0]))
          .thenApply((ignored) -> Boolean.TRUE);
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}