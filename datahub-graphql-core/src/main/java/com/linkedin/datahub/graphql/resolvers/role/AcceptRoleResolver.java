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
import com.linkedin.identity.InviteToken;
import com.linkedin.identity.RoleMembership;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class AcceptRoleResolver implements DataFetcher<CompletableFuture<Boolean>> {

  // Priority values for built-in DataHub roles: higher number = more privileged.
  // Roles absent from this map (e.g. custom roles) are treated as lowest priority (0)
  // so that invite links for unknown roles are always honoured rather than silently dropped.
  private static final Map<String, Integer> ROLE_PRIORITY =
      Map.of("Admin", 3, "Editor", 2, "Reader", 1);

  private final RoleService _roleService;
  private final InviteTokenService _inviteTokenService;
  private final EntityService<?> _entityService;
  private final SystemEntityClient _systemEntityClient;
  private final OperationContext _systemOperationContext;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = getQueryContext(environment);

    final AcceptRoleInput input =
        bindArgument(environment.getArgument("input"), AcceptRoleInput.class);
    final String inviteTokenStr = input.getInviteToken();
    final Authentication authentication = context.getAuthentication();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final InviteToken inviteToken =
                _inviteTokenService.getVerifiedInviteToken(
                    context.getOperationContext(), inviteTokenStr);
            if (inviteToken == null) {
              throw new RuntimeException(
                  String.format("Invite token %s is invalid", inviteTokenStr));
            }

            final Urn roleUrn = inviteToken.hasRole() ? inviteToken.getRole() : null;

            final Urn userUrn = UrnUtils.getUrn(authentication.getActor().toUrnStr());
            final int currentPriority = getCurrentRolePriority(context, userUrn);
            final int invitePriority = getRolePriority(roleUrn);

            if (currentPriority > invitePriority) {
              log.info(
                  "User {} already has a higher-privilege role. Skipping role assignment from invite link to prevent downgrade.",
                  userUrn);
            } else {
              // The grant is authorized by the invite token, not by the accepting user, who holds
              // no privileges at this point. Assigning as the system actor keeps the write out of
              // scope for PrivilegeGrantAuthorizationValidator, which requires Manage Policies for
              // a
              // user to write roleMembership.
              _roleService.batchAssignRoleToActors(
                  _systemOperationContext,
                  Collections.singletonList(authentication.getActor().toUrnStr()),
                  roleUrn);

              // Invalidate entity client cache for user's group/role membership
              invalidateUserMembershipCache(context, userUrn);
            }

            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to accept role using invite token %s", inviteTokenStr), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private int getRolePriority(@Nullable final Urn roleUrn) {
    if (roleUrn == null) {
      return 0;
    }
    return ROLE_PRIORITY.getOrDefault(roleUrn.getId(), 0);
  }

  private int getCurrentRolePriority(
      @Nonnull final QueryContext context, @Nonnull final Urn userUrn) {
    try {
      final RoleMembership membership =
          (RoleMembership)
              _entityService.getLatestAspect(
                  context.getOperationContext(), userUrn, ROLE_MEMBERSHIP_ASPECT_NAME);
      if (membership == null || membership.getRoles().isEmpty()) {
        return 0;
      }
      return membership.getRoles().stream().mapToInt(this::getRolePriority).max().orElse(0);
    } catch (Exception e) {
      log.warn(
          "Failed to fetch current role for user {}, proceeding with role assignment", userUrn, e);
      return 0;
    }
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
