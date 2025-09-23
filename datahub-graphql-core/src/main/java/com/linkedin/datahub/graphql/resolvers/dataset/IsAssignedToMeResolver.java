package com.linkedin.datahub.graphql.resolvers.dataset;

import com.datahub.authentication.group.GroupService;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.Role;
import com.linkedin.datahub.graphql.generated.RoleGroup;
import com.linkedin.datahub.graphql.generated.RoleUser;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IsAssignedToMeResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final GroupService _groupService;

  public IsAssignedToMeResolver(@Nonnull final GroupService groupService) {
    _groupService = Objects.requireNonNull(groupService, "groupService must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Role role = environment.getSource();
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            // Get assigned user URNs
            final Set<String> assignedUserUrns =
                role.getActors() != null && role.getActors().getUsers() != null
                    ? role.getActors().getUsers().stream()
                        .map(RoleUser::getUser)
                        .map(CorpUser::getUrn)
                        .collect(Collectors.toSet())
                    : Collections.emptySet();

            // Get assigned group URNs
            final Set<String> assignedGroupUrns =
                role.getActors() != null && role.getActors().getGroups() != null
                    ? role.getActors().getGroups().stream()
                        .map(RoleGroup::getGroup)
                        .map(CorpGroup::getUrn)
                        .collect(Collectors.toSet())
                    : Collections.emptySet();

            final Urn userUrn = Urn.createFromString(context.getActorUrn());

            // First check whether user is directly assigned.
            if (assignedUserUrns.size() > 0) {
              boolean isUserAssigned = assignedUserUrns.contains(userUrn.toString());
              if (isUserAssigned) {
                return true;
              }
            }

            // Next check whether the user is assigned indirectly, by group.
            if (assignedGroupUrns.size() > 0) {
              final List<Urn> groupUrns =
                  _groupService.getGroupsForUser(context.getOperationContext(), userUrn);
              boolean isUserGroupAssigned =
                  groupUrns.stream()
                      .anyMatch(groupUrn -> assignedGroupUrns.contains(groupUrn.toString()));
              if (isUserGroupAssigned) {
                return true;
              }
            }

            // User is not directly or indirectly assigned.
            return false;
          } catch (Exception e) {
            log.error(
                "Failed to determine whether the role is assigned to the currently authenticated user! Returning false.",
                e);
            return false;
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
