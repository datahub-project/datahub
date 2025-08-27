package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AssignMultipleRolesInput;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Resolver responsible for assigning multiple roles to a single actor (user or group). */
@Slf4j
@RequiredArgsConstructor
public class AssignMultipleRolesResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final com.datahub.authorization.role.RoleService _roleService;

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    // Check authorization first
    if (!canManagePolicies(context)) {
      throw new AuthorizationException(
          "Unauthorized to assign roles. Please contact your DataHub administrator if this needs corrective action.");
    }

    final AssignMultipleRolesInput input =
        bindArgument(environment.getArgument("input"), AssignMultipleRolesInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final String actorUrn = input.getActorUrn();
            final List<String> roleUrnStrings = input.getRoleUrns();

            log.info("Assigning {} roles to actor {}", roleUrnStrings.size(), actorUrn);

            // Convert role URN strings to Urn objects
            final List<Urn> roleUrns =
                roleUrnStrings.stream()
                    .map(
                        roleUrnString -> {
                          try {
                            return Urn.createFromString(roleUrnString);
                          } catch (Exception e) {
                            log.error("Failed to parse role URN: {}", roleUrnString, e);
                            throw new RuntimeException("Invalid role URN: " + roleUrnString, e);
                          }
                        })
                    .collect(Collectors.toList());

            // Assign multiple roles to the actor
            _roleService.assignRolesToActor(context.getOperationContext(), actorUrn, roleUrns);

            log.info("Successfully assigned {} roles to actor {}", roleUrns.size(), actorUrn);
            return true;

          } catch (Exception e) {
            log.error(
                "Failed to assign multiple roles to actor {}: {}",
                input.getActorUrn(),
                e.getMessage(),
                e);
            throw new RuntimeException(
                String.format(
                    "Failed to assign roles to actor %s: %s", input.getActorUrn(), e.getMessage()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
