package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.role.RoleService;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.BatchAssignRoleInput;
import com.linkedin.entity.client.SystemEntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class BatchAssignRoleResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final RoleService _roleService;
  private final SystemEntityClient _systemEntityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    if (!canManagePolicies(context)) {
      throw new AuthorizationException(
          "Unauthorized to assign roles. Please contact your DataHub administrator if this needs corrective action.");
    }

    final BatchAssignRoleInput input =
        bindArgument(environment.getArgument("input"), BatchAssignRoleInput.class);
    final String roleUrnStr = input.getRoleUrn();
    final List<String> actors = input.getActors();
    final Authentication authentication = context.getAuthentication();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final Urn roleUrn = roleUrnStr == null ? null : Urn.createFromString(roleUrnStr);
            _roleService.batchAssignRoleToActors(context.getOperationContext(), actors, roleUrn);
            // Invalidate entity client cache for each actor's role membership
            actors.forEach(actor -> invalidateRoleMembershipCache(UrnUtils.getUrn(actor)));
            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Invalidates the entity client cache for an actor's role assignment aspect to ensure
   * authorization checks immediately reflect the role assignment without waiting for cache
   * expiration.
   */
  private void invalidateRoleMembershipCache(Urn actorUrn) {
    try {
      if (_systemEntityClient.getEntityClientCache() == null) {
        log.debug("Entity client cache is not available, skipping cache invalidation");
        return;
      }
      Set<String> roleAspects = Set.of(ROLE_MEMBERSHIP_ASPECT_NAME);

      _systemEntityClient.getEntityClientCache().invalidate(actorUrn, roleAspects);

      log.info(
          "Invalidated entity client cache for actor {} membership aspects: {}",
          actorUrn,
          roleAspects);
    } catch (Exception e) {
      log.error("Failed to invalidate entity client cache for actor: {}", actorUrn, e);
    }
  }
}
