package com.linkedin.datahub.graphql.resolvers.group;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.group.GroupService;
import com.linkedin.common.Origin;
import com.linkedin.common.OriginType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.AddGroupMembersInput;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** Resolver that adds a set of native members to a group, if the user and group both exist. */
public class AddGroupMembersResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final GroupService _groupService;

  public AddGroupMembersResolver(final GroupService groupService) {
    _groupService = groupService;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {

    final AddGroupMembersInput input =
        bindArgument(environment.getArgument("input"), AddGroupMembersInput.class);
    final String groupUrnStr = input.getGroupUrn();
    final QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();
    Urn groupUrn = Urn.createFromString(groupUrnStr);

    if (!canEditGroupMembers(groupUrnStr, context)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    if (!_groupService.groupExists(groupUrn)) {
      // The group doesn't exist.
      throw new DataHubGraphQLException(
          String.format("Failed to add members to group %s. Group does not exist.", groupUrnStr),
          DataHubGraphQLErrorCode.NOT_FOUND);
    }
    return CompletableFuture.supplyAsync(
        () -> {
          Origin groupOrigin = _groupService.getGroupOrigin(groupUrn);
          if (groupOrigin == null || !groupOrigin.hasType()) {
            try {
              _groupService.migrateGroupMembershipToNativeGroupMembership(
                  groupUrn, context.getActorUrn(), context.getAuthentication());
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format(
                      "Failed to migrate group membership for group %s when adding group members",
                      groupUrnStr));
            }
          } else if (groupOrigin.getType() == OriginType.EXTERNAL) {
            throw new RuntimeException(
                String.format(
                    "Group %s was ingested from an external provider and cannot have members manually added to it",
                    groupUrnStr));
          }

          try {
            // Add each user to the group
            final List<Urn> userUrnList =
                input.getUserUrns().stream().map(UrnUtils::getUrn).collect(Collectors.toList());
            userUrnList.forEach(
                userUrn -> _groupService.addUserToNativeGroup(userUrn, groupUrn, authentication));
            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to add group members to group %s", groupUrnStr));
          }
        });
  }
}
