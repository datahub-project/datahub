package com.datahub.plugins.auth.authorization;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizedActors;
import com.datahub.authorization.AuthorizerContext;
import com.datahub.authorization.BatchAuthorizationRequest;
import com.datahub.authorization.BatchAuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.LazyHashMap;
import com.datahub.plugins.Plugin;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import com.linkedin.policy.DataHubPolicyInfo;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * An Authorizer is responsible for determining whether an actor should be granted a specific
 * privilege.
 */
public interface Authorizer extends Plugin {
  Authorizer EMPTY = new Authorizer() {};

  Authorizer SYSTEM =
      new Authorizer() {
        @Override
        public AuthorizationResult authorize(@Nonnull AuthorizationRequest request) {
          if (Constants.SYSTEM_ACTOR.equals(request.getActorUrn())) {
            return new AuthorizationResult(
                request,
                AuthorizationResult.Type.ALLOW,
                "Granted by system actor " + request.getActorUrn());
          }

          return new AuthorizationResult(
              request, AuthorizationResult.Type.DENY, "Only system user is allowed.");
        }
      };

  /**
   * Initialize the Authorizer. Invoked once at boot time.
   *
   * @param authorizerConfig config provided to the authenticator derived from the Metadata Service
   *     YAML config. This config comes from the "authorization.authorizers.config" configuration.
   */
  default void init(
      @Nonnull final Map<String, Object> authorizerConfig, @Nonnull final AuthorizerContext ctx) {}

  /** Authorizes an action based on the actor, the resource, and required privilege. */
  default AuthorizationResult authorize(@Nonnull final AuthorizationRequest request) {
    return new AuthorizationResult(request, AuthorizationResult.Type.DENY, "Not Implemented.");
  }

  /**
   * Authorizes an actions based on the actor, the resource, and required privileges.
   *
   * <p>Returned map with results <b>MUST</b> be thread safe for {@link Map#get} operation. For an
   * ease to use implementation take a look at the {@link LazyHashMap}
   */
  default BatchAuthorizationResult authorizeBatch(
      @Nonnull final BatchAuthorizationRequest batchRequest) {
    var results =
        new LazyHashMap<String, AuthorizationResult>(
            privilege ->
                authorize(
                    new AuthorizationRequest(
                        batchRequest.getActorUrn(),
                        privilege,
                        batchRequest.getResourceSpec(),
                        batchRequest.getSubResources())));
    return new BatchAuthorizationResult(batchRequest, results);
  }

  /**
   * Retrieves the current list of actors authorized to for a particular privilege against an
   * optional resource
   */
  default AuthorizedActors authorizedActors(
      final String privilege, final Optional<EntitySpec> resourceSpec) {
    return AuthorizedActors.builder()
        .privilege(privilege)
        .users(Collections.emptyList())
        .roles(Collections.emptyList())
        .groups(Collections.emptyList())
        .build();
  }

  /**
   * Given the actor's urn retrieve the policies.
   *
   * @param actorUrn
   * @return
   */
  default Set<DataHubPolicyInfo> getActorPolicies(@Nonnull Urn actorUrn) {
    return Collections.emptySet();
  }

  /** Given the actor's urn retrieve the actor's groups */
  default Collection<Urn> getActorGroups(@Nonnull Urn actorUrn) {
    return Collections.emptyList();
  }

  /** Given an actor's urn retrieve the actor's peers */
  default Collection<Urn> getActorPeers(@Nonnull Urn actorUrn) {
    return Collections.emptyList();
  }
}
