package io.datahubproject.metadata.context;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.ResolvedEntitySpec;
import com.datahub.authorization.SessionActorIdentity;
import com.linkedin.common.urn.Urn;
import com.linkedin.policy.DataHubPolicyInfo;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Extension of {@link com.datahub.plugins.auth.authorization.Authorizer} for implementations that
 * require the session {@link OperationContext} when authorizing a user request. Storage reads
 * during user authorization must use the session context, not the system context.
 */
public interface OperationContextAuthorizer {

  @Nonnull
  AuthorizationResult authorize(
      @Nonnull AuthorizationRequest request,
      @Nullable Map<EntitySpec, ResolvedEntitySpec> resourceSpecCache,
      @Nonnull OperationContext opContext);

  @Nonnull
  Optional<SessionActorIdentity> resolveSessionActorIdentity(
      @Nonnull Urn actorUrn, @Nonnull OperationContext opContext);

  @Nonnull
  Set<DataHubPolicyInfo> getActorPolicies(
      @Nonnull Urn actorUrn,
      @Nullable SessionActorIdentity sessionActorIdentity,
      @Nullable Collection<Urn> preloadedGroups,
      @Nullable Set<Urn> preloadedDirectRoles,
      @Nonnull OperationContext opContext);
}
