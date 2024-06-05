package com.linkedin.datahub.graphql.resolvers.dataset;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.Role;
import com.linkedin.datahub.graphql.generated.RoleUser;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IsAssignedToMeResolver implements DataFetcher<CompletableFuture<Boolean>> {

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Role role = environment.getSource();
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final Set<String> assignedUserUrns =
                role.getActors() != null && role.getActors().getUsers() != null
                    ? role.getActors().getUsers().stream()
                        .map(RoleUser::getUser)
                        .map(CorpUser::getUrn)
                        .collect(Collectors.toSet())
                    : Collections.emptySet();
            return assignedUserUrns.contains(context.getActorUrn());
          } catch (Exception e) {
            throw new RuntimeException(
                "Failed to determine if current user is assigned to Role", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
