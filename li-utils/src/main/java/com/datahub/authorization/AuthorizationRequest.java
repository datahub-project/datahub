package com.datahub.authorization;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

  /**
   * Session-scoped group membership for the actor, or {@code null} when not supplied (direct
   * authorizer invocations, test mocks).
   */
  @Nullable Collection<Urn> actorGroupMembership;

  /** Session-scoped direct role membership for the actor, or {@code null} when not supplied. */
  @Nullable Set<Urn> actorDirectRoles;

  /**
   * Request-scoped session actor identity, or {@code null} when not supplied. When present, group
   * role inheritance is resolved at most once per request via {@link SessionActorIdentity}.
   */
  @Nullable SessionActorIdentity sessionActorIdentity;

  public AuthorizationRequest(
      String actorUrn,
      String privilege,
      Optional<EntitySpec> resourceSpec,
      Collection<EntitySpec> subResources,
      @Nullable Map<String, List<RecordTemplate>> actorPoliciesByPrivilege,
      @Nullable Collection<Urn> actorGroupMembership,
      @Nullable Set<Urn> actorDirectRoles,
      @Nullable SessionActorIdentity sessionActorIdentity) {
    this.actorUrn = actorUrn;
    this.privilege = privilege;
    this.resourceSpec = resourceSpec;
    this.subResources = subResources;
    this.actorPoliciesByPrivilege = actorPoliciesByPrivilege;
    this.actorGroupMembership = actorGroupMembership;
    this.actorDirectRoles = actorDirectRoles;
    this.sessionActorIdentity = sessionActorIdentity;
  }

  public AuthorizationRequest(
      String actorUrn,
      String privilege,
      Optional<EntitySpec> resourceSpec,
      Collection<EntitySpec> subResources,
      @Nullable Map<String, List<RecordTemplate>> actorPoliciesByPrivilege,
      @Nullable Collection<Urn> actorGroupMembership,
      @Nullable Set<Urn> actorDirectRoles) {
    this(
        actorUrn,
        privilege,
        resourceSpec,
        subResources,
        actorPoliciesByPrivilege,
        actorGroupMembership,
        actorDirectRoles,
        null);
  }

  public AuthorizationRequest(
      String actorUrn,
      String privilege,
      Optional<EntitySpec> resourceSpec,
      Collection<EntitySpec> subResources,
      @Nullable Map<String, List<RecordTemplate>> actorPoliciesByPrivilege) {
    this(
        actorUrn,
        privilege,
        resourceSpec,
        subResources,
        actorPoliciesByPrivilege,
        null,
        null,
        null);
  }

  public AuthorizationRequest(
      String actorUrn,
      String privilege,
      Optional<EntitySpec> resourceSpec,
      Collection<EntitySpec> subResources) {
    this(actorUrn, privilege, resourceSpec, subResources, null);
  }
}
