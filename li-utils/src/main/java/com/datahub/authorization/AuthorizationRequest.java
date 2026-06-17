package com.datahub.authorization;

import com.linkedin.data.template.RecordTemplate;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
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

  /**
   * Session-scoped actor-applicable policies indexed by privilege, or {@code null} when callers do
   * not supply session policy scope (direct authorizer invocations, test mocks).
   */
  @Nullable Map<String, List<RecordTemplate>> actorPoliciesByPrivilege;

  public AuthorizationRequest(
      String actorUrn,
      String privilege,
      Optional<EntitySpec> resourceSpec,
      Collection<EntitySpec> subResources,
      @Nullable Map<String, List<RecordTemplate>> actorPoliciesByPrivilege) {
    this.actorUrn = actorUrn;
    this.privilege = privilege;
    this.resourceSpec = resourceSpec;
    this.subResources = subResources;
    this.actorPoliciesByPrivilege = actorPoliciesByPrivilege;
  }

  public AuthorizationRequest(
      String actorUrn,
      String privilege,
      Optional<EntitySpec> resourceSpec,
      Collection<EntitySpec> subResources) {
    this(actorUrn, privilege, resourceSpec, subResources, null);
  }
}
