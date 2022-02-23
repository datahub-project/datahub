package com.datahub.authorization;

import com.datahub.authentication.Authentication;
import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.OwnershipClient;
import com.linkedin.identity.GroupMembership;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import com.linkedin.r2.RemoteInvocationException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;

@Slf4j
public class PolicyEngine {

  private final Authentication _systemAuthentication;
  private final EntityClient _entityClient;
  private final OwnershipClient _ownershipClient;

  public PolicyEngine(
      final Authentication systemAuthentication,
      final EntityClient entityClient,
      final OwnershipClient ownershipClient) {
    _systemAuthentication = systemAuthentication;
    _entityClient = entityClient;
    _ownershipClient = ownershipClient;
  }

  public PolicyEvaluationResult evaluatePolicy(
      final DataHubPolicyInfo policy,
      final String actorStr,
      final String privilege,
      final Optional<ResourceSpec> resource) {
    try {
      // Currently Actor must be an urn. Consider whether this contract should be pushed up.
      final Urn actor = Urn.createFromString(actorStr);
      return evaluatePolicy(policy, actor, privilege, resource);
    } catch (URISyntaxException e) {
      log.error(String.format("Failed to bind actor %s to an URN. Actors must be URNs. Denying the authorization request", actorStr));
      return PolicyEvaluationResult.DENIED;
    }
  }

  public PolicyEvaluationResult evaluatePolicy(
      final DataHubPolicyInfo policy,
      final Urn actor,
      final String privilege,
      final Optional<ResourceSpec> resource) {

    final PolicyEvaluationContext context = new PolicyEvaluationContext();

    // If policy is inactive, simply return DENY.
    if (PoliciesConfig.INACTIVE_POLICY_STATE.equals(policy.getState())) {
      return PolicyEvaluationResult.DENIED;
    }

    // If the privilege is not in scope, deny the request.
    if (!isPrivilegeMatch(privilege, policy.getPrivileges(), context)) {
      return PolicyEvaluationResult.DENIED;
    }

    // If the resource is not in scope, deny the request.
    if (!isResourceMatch(policy.getType(), policy.getResources(), resource, context)) {
      return PolicyEvaluationResult.DENIED;
    }

    // If the actor does not match, deny the request.
    if (!isActorMatch(actor, policy.getActors(), resource, context)) {
      return PolicyEvaluationResult.DENIED;
    }

    // All portions of the Policy match. Grant the request.
    return PolicyEvaluationResult.GRANTED;
  }

  public PolicyActors getMatchingActors(final DataHubPolicyInfo policy, final Optional<ResourceSpec> resource) {
    final List<Urn> users = new ArrayList<>();
    final List<Urn> groups = new ArrayList<>();
    boolean allUsers = false;
    boolean allGroups = false;
    if (policyMatchesResource(policy, resource)) {
      // Step 3: For each matching policy, find actors that are authorized.
      final DataHubActorFilter actorFilter = policy.getActors();

      // 0. Determine if we have a wildcard policy.
      if (actorFilter.isAllUsers()) {
        allUsers = true;
      }
      if (actorFilter.isAllUsers()) {
        allGroups = true;
      }

      // 1. Populate actors listed on the policy directly.
      if (actorFilter.hasUsers()) {
        users.addAll(actorFilter.getUsers());
      }
      if (actorFilter.hasGroups()) {
        groups.addAll(actorFilter.getGroups());
      }

      // 2. Fetch Actors based on resource ownership.
      if (actorFilter.isResourceOwners() && resource.isPresent()) {
        try {
          final Ownership ownership = _ownershipClient.getLatestOwnership(resource.get().getResource());
          if (ownership != null) {
            users.addAll(userOwners(ownership));
            groups.addAll(groupOwners(ownership));
          }
        } catch (RemoteInvocationException e) {
          // Throw an error, as we are not able to fully resolve the authorized policy actors.
          throw new RuntimeException("Failed to retrieve ownership when resolving authorized actors.", e);
        }
      }
    }
    return new PolicyActors(users, groups, allUsers, allGroups);
  }

  /**
   * Returns true if the policy matches the resource spec, false otherwise.
   *
   * If the policy is of type "PLATFORM", the resource will always match (since there's no resource).
   * If the policy is of type "METADATA", the resourceSpec parameter will be matched against the
   * resource filter defined on the policy.
   */
  public Boolean policyMatchesResource(final DataHubPolicyInfo policy, final Optional<ResourceSpec> resourceSpec) {
    return isResourceMatch(policy.getType(), policy.getResources(), resourceSpec, new PolicyEvaluationContext());
  }

  /**
   * Returns true if the privilege portion of a DataHub policy matches a the privilege being evaluated, false otherwise.
   */
  private boolean isPrivilegeMatch(
      final String requestPrivilege,
      final List<String> policyPrivileges,
      final PolicyEvaluationContext context) {
    return policyPrivileges.contains(requestPrivilege);
  }

  /**
   * Returns true if the resource portion of a DataHub policy matches a the resource being evaluated, false otherwise.
   */
  private boolean isResourceMatch(
      final String policyType,
      final @Nullable DataHubResourceFilter policyResourceFilter,
      final Optional<ResourceSpec> requestResource,
      final PolicyEvaluationContext context) {
    if (PoliciesConfig.PLATFORM_POLICY_TYPE.equals(policyType)) {
      // Currently, platform policies have no associated resource.
      return true;
    }
    if (policyResourceFilter == null) {
      // No resource defined on the policy.
      return true;
    }
    if (!requestResource.isPresent()) {
      // Resource filter present in policy, but no resource spec provided.
      return false;
    }
    final ResourceSpec resourceSpec = requestResource.get();
    final boolean resourceTypesMatch = policyResourceFilter.hasType() && policyResourceFilter.getType().equals(resourceSpec.getType());
    final boolean resourceIdentityMatch =
        policyResourceFilter.isAllResources()
            || (policyResourceFilter.hasResources() && Objects.requireNonNull(policyResourceFilter.getResources())
        .stream()
        .anyMatch(resource -> resource.equals(resourceSpec.getResource())));
    // If the resource's type and identity match, then the resource matches the policy.
    return resourceTypesMatch && resourceIdentityMatch;
  }

  /**
   * Returns true if the actor portion of a DataHub policy matches a the actor being evaluated, false otherwise.
   */
  private boolean isActorMatch(
      final Urn actor,
      final DataHubActorFilter actorFilter,
      final Optional<ResourceSpec> resourceSpec,
      final PolicyEvaluationContext context) {

    // 1. If the actor is a matching "User" in the actor filter, return true immediately.
     if (isUserMatch(actor, actorFilter)) {
       return true;
     }

    // 2. If the actor is in a matching "Group" in the actor filter, return true immediately.
    if (isGroupMatch(actor, actorFilter, context)) {
      return true;
    }

    // 3. If the actor is the owner, either directly or indirectly via a group, return true
    return isOwnerMatch(actor, actorFilter, resourceSpec, context);
  }

  private boolean isUserMatch(final Urn actor, final DataHubActorFilter actorFilter) {
    // If the actor is a matching "User" in the actor filter, return true immediately.
    return actorFilter.isAllUsers() || (actorFilter.hasUsers() && Objects.requireNonNull(actorFilter.getUsers())
        .stream()
        .anyMatch(user -> user.equals(actor)));
  }

  private boolean isGroupMatch(final Urn actor, final DataHubActorFilter actorFilter, final PolicyEvaluationContext context) {
    // If the actor is in a matching "Group" in the actor filter, return true immediately.
    if (actorFilter.isAllGroups() || actorFilter.hasGroups()) {
      final Set<Urn> groups = resolveGroups(actor, context);
      return actorFilter.isAllGroups() || (actorFilter.hasGroups() && Objects.requireNonNull(actorFilter.getGroups())
          .stream()
          .anyMatch(groups::contains));
    }
    // If there are no groups on the policy, return false for the group match.
    return false;
  }

  private boolean isOwnerMatch(
      final Urn actor,
      final DataHubActorFilter actorFilter,
      final Optional<ResourceSpec> requestResource,
      final PolicyEvaluationContext context) {

    // If the policy does not apply to owners, or there is no resource to own, return false immediately.
    if (!actorFilter.isResourceOwners() || !requestResource.isPresent()) {
      return false;
    }

    // Otherwise, evaluate ownership match.
    final ResourceSpec resourceSpec = requestResource.get();
    try {
      final Ownership ownership = _ownershipClient.getLatestOwnership(resourceSpec.getResource());
      if (ownership != null) {
        return isActorOwner(actor, ownership, context);
      }
    } catch (Exception e) {
      log.error(String.format("Failed to resolve Ownership of resource with URN %s. Returning DENY.", resourceSpec.getResource()), e);
    }
    return false;
  }

  private boolean isActorOwner(Urn actor, Ownership ownership, PolicyEvaluationContext context) {
    if (isUserOwner(actor, ownership)) {
      return true;
    }
    final Set<Urn> groups = resolveGroups(actor, context);
    if (isGroupOwner(groups, ownership)) {
      return true;
    }
    return false;
  }

  private boolean isUserOwner(Urn actor, Ownership ownership) {
    return ownership.getOwners().stream().anyMatch(owner -> actor.equals(owner.getOwner()));
  }

  private boolean isGroupOwner(Set<Urn> groups, Ownership ownership) {
    return ownership.getOwners().stream().anyMatch(owner -> groups.contains(owner.getOwner()));
  }

  private Set<Urn> resolveGroups(Urn actor, PolicyEvaluationContext context) {

    if (context.groups != null) {
      return context.groups;
    }

    Set<Urn> groups = new HashSet<>();
    Optional<GroupMembership> maybeGroups = resolveGroupMembership(actor);
    maybeGroups.ifPresent(groupMembership -> groups.addAll(groupMembership.getGroups()));
    context.setGroups(groups); // Cache the groups.
    return groups;
  }

  // TODO: Optimization - Cache the group membership. Refresh periodically.
  private Optional<GroupMembership> resolveGroupMembership(final Urn actor) {
    try {
      final CorpUserSnapshot corpUser = _entityClient.get(actor, _systemAuthentication).getValue().getCorpUserSnapshot();
      for (CorpUserAspect aspect : corpUser.getAspects()) {
        if (aspect.isGroupMembership()) {
          // Found group membership.
          return Optional.of(aspect.getGroupMembership());
        }
      }

    } catch (RemoteInvocationException e) {
      throw new RuntimeException(String.format("Failed to fetch corpUser for urn %s", actor), e);
    }
    return Optional.empty();
  }

  /**
   * Class used to store state across a single Policy evaluation.
   */
  static class PolicyEvaluationContext {
    private Set<Urn> groups;
    public void setGroups(Set<Urn> groups) {
      this.groups = groups;
    }
  }

  /**
   * Class used to represent the result of a Policy evaluation
   */
  static class PolicyEvaluationResult {
    public static final PolicyEvaluationResult GRANTED = new PolicyEvaluationResult(true);
    public static final PolicyEvaluationResult DENIED = new PolicyEvaluationResult(false);

    private final boolean isGranted;

    private PolicyEvaluationResult(boolean isGranted) {
      this.isGranted = isGranted;
    }

    public boolean isGranted() {
      return this.isGranted;
    }
  }

  /**
   * Class used to represent all valid users of a policy.
   */
  public static class PolicyActors {
    final List<Urn> _users;
    final List<Urn> _groups;
    final Boolean _allUsers;
    final Boolean _allGroups;

    public PolicyActors(final List<Urn> users, final List<Urn> groups, final Boolean allUsers, final Boolean allGroups) {
      _users = users;
      _groups = groups;
      _allUsers = allUsers;
      _allGroups = allGroups;
    }

    public List<Urn> getUsers() {
      return _users;
    }

    public List<Urn> getGroups() {
      return _groups;
    }

    public Boolean allUsers() {
      return _allUsers;
    }

    public Boolean allGroups() {
      return _allGroups;
    }
  }

  private List<Urn> userOwners(final Ownership ownership) {
    return ownership.getOwners()
        .stream()
        .filter(owner -> CORP_USER_ENTITY_NAME.equals(owner.getOwner().getEntityType()))
        .map(Owner::getOwner)
        .collect(Collectors.toList());
  }

  private List<Urn> groupOwners(final Ownership ownership) {
    return ownership.getOwners()
        .stream()
        .filter(owner -> CORP_GROUP_ENTITY_NAME.equals(owner.getOwner().getEntityType()))
        .map(Owner::getOwner)
        .collect(Collectors.toList());
  }
}
