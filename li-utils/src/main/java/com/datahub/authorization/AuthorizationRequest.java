package com.datahub.authorization;

import java.util.Collection;
import java.util.Optional;
import lombok.Value;

/** A request to authorize a user for a specific privilege. */
@Value
public class AuthorizationRequest {
  /** The urn of the actor (corpuser) making the request. */
  String actorUrn;

  /** The privilege that the user is requesting */
  String privilege;

  /**
   * The resource that the user is requesting for, if applicable. If the privilege is a platform
   * privilege this optional will be empty.
   */
  Optional<EntitySpec> resourceSpec;

  /** The sub-resources that are being applied as a modification to the target resource */
  Collection<EntitySpec> subResources;
}
