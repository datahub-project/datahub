package com.datahub.authorization;

import java.util.Optional;
import lombok.Value;


/**
 * A request to authorize a user for a specific privilege.
 */
@Value
public class AuthorizationRequest {
  /**
   * The urn of the actor (corpuser) making the request.
   */
  String actorUrn;
  /**
   * The privilege that the user is requesting
   */
  String privilege;
  /**
   * The resource that the user is requesting for, if applicable. If the privilege is a platform privilege
   * this optional will be empty.
   */
  Optional<ResourceSpec> resourceSpec;
}
