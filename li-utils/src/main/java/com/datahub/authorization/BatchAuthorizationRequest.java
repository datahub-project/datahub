package com.datahub.authorization;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import lombok.Value;

@Value
public class BatchAuthorizationRequest {
  /** The urn of the actor (corpuser) making the request. */
  String actorUrn;

  /** The privileges that the user is requesting */
  Set<String> privileges;

  /**
   * The resource that the user is requesting for, if applicable. If the privilege is a platform
   * privilege this optional will be empty.
   */
  Optional<EntitySpec> resourceSpec;

  /** The sub-resources that are being applied as a modification to the target resource */
  Collection<EntitySpec> subResources;

  public Stream<AuthorizationRequest> getIndividualRequests() {
    return privileges.stream()
        .map(
            privilege -> new AuthorizationRequest(actorUrn, privilege, resourceSpec, subResources));
  }
}
