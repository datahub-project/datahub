package com.linkedin.datahub.graphql.resolvers.group;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.AddGroupMembersInput;
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

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


/**
 * Resolver that adds a set of members to a group, if the user and group both exist.
 */
public class AddGroupMembersResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final AspectClient _aspectClient;

  public AddGroupMembersResolver(final AspectClient aspectClient) {
    _aspectClient = aspectClient;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    if (AuthorizationUtils.canManageUsersAndGroups(context)) {
      final AddGroupMembersInput input = bindArgument(environment.getArgument("input"), AddGroupMembersInput.class);
      final String groupUrnStr = input.getGroupUrn();
      final List<String> userUrnStrs = input.getUserUrns();

      return CompletableFuture.runAsync(() -> {
        if (!groupExists(groupUrnStr, context)) {
          // The group doesn't exist.
          throw new DataHubGraphQLException("Failed to add member to group. Group does not exist.", DataHubGraphQLErrorCode.NOT_FOUND);
        }
      })
      .thenApply(ignored -> CompletableFuture.allOf(
          userUrnStrs.stream().map(userUrnStr -> CompletableFuture.runAsync(() -> {
              addUserToGroup(userUrnStr, groupUrnStr, context);
          })).toArray(CompletableFuture[]::new)))
      .thenApply((ignored) -> Boolean.TRUE);
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private void addUserToGroup(final String userUrnStr, final String groupUrnStr, final QueryContext context) {
    try {
      // First, fetch user's group membership aspect.
      final VersionedAspect gmsAspect =
          _aspectClient.getAspectOrNull(userUrnStr, Constants.GROUP_MEMBERSHIP_ASPECT_NAME,
              Constants.ASPECT_LATEST_VERSION, context.getActor());

      GroupMembership groupMembership;
      if (gmsAspect == null) {
        // Verify the user exists
        if (!userExists(userUrnStr, context)) {
          throw new DataHubGraphQLException("Failed to add member to group. User does not exist.", DataHubGraphQLErrorCode.NOT_FOUND);
        }
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
    } catch (Exception e) {
      throw new RuntimeException("Failed to add member to group", e);
    }
  }

  private boolean groupExists(final String groupUrnStr, final QueryContext context) {
    try {
      final VersionedAspect keyAspect = _aspectClient.getAspectOrNull(
          groupUrnStr,
          Constants.CORP_GROUP_KEY_ASPECT_NAME,
          Constants.ASPECT_LATEST_VERSION,
          context.getActor());
      return keyAspect != null;
    } catch (Exception e) {
      throw new DataHubGraphQLException("Failed to fetch group!", DataHubGraphQLErrorCode.SERVER_ERROR);
    }
  }

  private boolean userExists(final String userUrnStr, final QueryContext context) {
    try {
      final VersionedAspect keyAspect = _aspectClient.getAspectOrNull(
          userUrnStr,
          Constants.CORP_USER_KEY_ASPECT_NAME,
          Constants.ASPECT_LATEST_VERSION,
          context.getActor());
      return keyAspect != null;
    } catch (Exception e) {
      throw new DataHubGraphQLException("Failed to fetch user!", DataHubGraphQLErrorCode.SERVER_ERROR);
    }
  }
}