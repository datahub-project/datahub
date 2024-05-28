package com.linkedin.datahub.graphql.resolvers.form;

import com.datahub.authentication.group.GroupService;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.FormActorAssignment;
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
public class IsFormAssignedToMeResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final GroupService _groupService;

  public IsFormAssignedToMeResolver(@Nonnull final GroupService groupService) {
    _groupService = Objects.requireNonNull(groupService, "groupService must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final FormActorAssignment parent = environment.getSource();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {

            // Assign urn and group urns
            final Set<String> assignedUserUrns =
                parent.getUsers() != null
                    ? parent.getUsers().stream().map(CorpUser::getUrn).collect(Collectors.toSet())
                    : Collections.emptySet();

            final Set<String> assignedGroupUrns =
                parent.getGroups() != null
                    ? parent.getGroups().stream().map(CorpGroup::getUrn).collect(Collectors.toSet())
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
          } catch (Exception e) {
            log.error(
                "Failed to determine whether the form is assigned to the currently authenticated user! Returning false.",
                e);
          }

          // Else the user is not directly assigned.
          return false;
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
