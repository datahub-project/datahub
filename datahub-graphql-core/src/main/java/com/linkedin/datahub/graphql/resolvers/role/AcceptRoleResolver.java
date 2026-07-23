package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.invite.InviteTokenService;
import com.datahub.authorization.role.RoleService;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.AcceptRoleInput;
import com.linkedin.entity.client.SystemEntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class AcceptRoleResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final RoleService _roleService;
  private final InviteTokenService _inviteTokenService;
  private final SystemEntityClient _systemEntityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    final AcceptRoleInput input =
        bindArgument(environment.getArgument("input"), AcceptRoleInput.class);
    final String inviteTokenStr = input.getInviteToken();
    final Authentication authentication = context.getAuthentication();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final Urn inviteTokenUrn = _inviteTokenService.getInviteTokenUrn(inviteTokenStr);
            if (!_inviteTokenService.isInviteTokenValid(
                context.getOperationContext(), inviteTokenUrn)) {
              throw new RuntimeException(
                  String.format("Invite token %s is invalid", inviteTokenStr));
            }

            final Urn roleUrn =
                _inviteTokenService.getInviteTokenRole(
                    context.getOperationContext(), inviteTokenUrn);
            _roleService.batchAssignRoleToActors(
                context.getOperationContext(),
                Collections.singletonList(authentication.getActor().toUrnStr()),
                roleUrn);

            // Invalidate entity client cache for user's group/role membership
            invalidateUserMembershipCache(
                context, UrnUtils.getUrn(authentication.getActor().toUrnStr()));

            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to accept role using invite token %s", inviteTokenStr), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Invalidates the entity client cache for the user's role assignment aspect to ensure
   * authorization checks immediately reflect the role assignment without waiting for cache
   * expiration.
   */
  private void invalidateUserMembershipCache(QueryContext context, Urn userUrn) {
    try {
      if (_systemEntityClient.getEntityClientCache() == null) {
        log.debug("Entity client cache is not available, skipping cache invalidation");
        return;
      }
      java.util.Set<String> roleAspects = java.util.Set.of(ROLE_MEMBERSHIP_ASPECT_NAME);

      _systemEntityClient.getEntityClientCache().invalidate(userUrn, roleAspects);

      log.info(
          "Invalidated entity client cache for user {} membership aspects: {}",
          userUrn,
          roleAspects);
    } catch (Exception e) {
      log.error("Failed to invalidate entity client cache for user: {}", userUrn, e);
      // Don't throw - cache invalidation failure shouldn't fail the role assignment
      // The cache will eventually expire based on TTL
    }
  }
}
