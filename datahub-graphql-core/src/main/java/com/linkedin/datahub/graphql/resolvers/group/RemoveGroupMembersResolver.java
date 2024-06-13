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
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.RemoveGroupMembersInput;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class RemoveGroupMembersResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final GroupService _groupService;

  public RemoveGroupMembersResolver(final GroupService groupService) {
    _groupService = groupService;
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {

    final RemoveGroupMembersInput input =
        bindArgument(environment.getArgument("input"), RemoveGroupMembersInput.class);
    final String groupUrnStr = input.getGroupUrn();
    final QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();

    if (!canEditGroupMembers(groupUrnStr, context)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    final Urn groupUrn = Urn.createFromString(groupUrnStr);
    final List<Urn> userUrnList =
        input.getUserUrns().stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    if (!_groupService.groupExists(context.getOperationContext(), groupUrn)) {
      // The group doesn't exist.
      throw new DataHubGraphQLException(
          String.format(
              "Failed to add remove members from group %s. Group does not exist.", groupUrnStr),
          DataHubGraphQLErrorCode.NOT_FOUND);
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          Origin groupOrigin =
              _groupService.getGroupOrigin(context.getOperationContext(), groupUrn);
          if (groupOrigin == null || !groupOrigin.hasType()) {
            try {
              _groupService.migrateGroupMembershipToNativeGroupMembership(
                  context.getOperationContext(), groupUrn, context.getActorUrn());
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format(
                      "Failed to migrate group membership when removing group members from group %s",
                      groupUrnStr));
            }
          } else if (groupOrigin.getType() == OriginType.EXTERNAL) {
            throw new RuntimeException(
                String.format(
                    "Group %s was ingested from an external provider and cannot have members manually removed from it",
                    groupUrnStr));
          }
          try {
            _groupService.removeExistingNativeGroupMembers(
                context.getOperationContext(), groupUrn, userUrnList);
            return true;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
