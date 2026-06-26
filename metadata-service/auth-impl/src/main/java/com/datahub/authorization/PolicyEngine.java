package com.datahub.authorization;

import static com.linkedin.metadata.Constants.*;

import com.datahub.authentication.group.GroupService;
import com.linkedin.common.Owner;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import com.linkedin.policy.PolicyMatchCondition;
import com.linkedin.policy.PolicyMatchCriterion;
import com.linkedin.policy.PolicyMatchCriterionArray;
import com.linkedin.policy.PolicyMatchFilter;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ServicesRegistryContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.map.HashedMap;

@Slf4j
public class PolicyEngine {

  private final EntityClient _entityClient;
  private final GroupService _groupService;

  public PolicyEngine(
      @Nonnull final EntityClient entityClient, @Nonnull final GroupService groupService) {
    this._entityClient = Objects.requireNonNull(entityClient);
    this._groupService = Objects.requireNonNull(groupService);
  }

  public PolicyEvaluationResult evaluatePolicy(
      @Nonnull OperationContext opContext,
      final DataHubPolicyInfo policy,
      final ResolvedEntitySpec resolvedActorSpec,
      final String privilege,
      final Optional<ResolvedEntitySpec> resource,
      final List<ResolvedEntitySpec> subResources) {

    return evaluatePolicy(
        opContext,
        policy,
        resolvedActorSpec,
        privilege,
        resource,
        subResources,
        new PolicyEvaluationContext());
  }

  public PolicyEvaluationResult evaluatePolicy(
      @Nonnull OperationContext opContext,
      final DataHubPolicyInfo policy,
      final ResolvedEntitySpec resolvedActorSpec,
      final String privilege,
      final Optional<ResolvedEntitySpec> resource,
      final List<ResolvedEntitySpec> subResources,
      @Nonnull final PolicyEvaluationContext context) {
    log.debug("Evaluating policy {}", policy.getDisplayName());

    // If the privilege is not in scope, deny the request.
    if (!isPrivilegeMatch(privilege, policy.getPrivileges())) {
      return new PolicyEvaluationResult(
          policy.getDisplayName(),
          false,
          String.format(
              "Policy denied based on irrelevant privileges %s for %s",
              policy.getPrivileges(), privilege));
    }

    // If policy is not applicable, deny the request
    if (!isPolicyApplicable(opContext, policy, resolvedActorSpec, resource, context, subResources)
        .isGranted()) {
      return new PolicyEvaluationResult(
          policy.getDisplayName(),
          false,
          String.format(
              "Policy is not applicable for actor %s and resource %s",
              resolvedActorSpec.getSpec().getEntity(), resource));
    }

    return new PolicyEvaluationResult(policy.getDisplayName(), true, "Policy allowed");
  }

  /** Builds a policy evaluation context pre-seeded with session actor groups and direct roles. */
  @Nonnull
  public PolicyEvaluationContext createSeededEvaluationContext(
      @Nullable final SessionActorIdentity sessionActorIdentity,
      @Nullable final Collection<Urn> actorGroupMembership,
      @Nullable final Set<Urn> actorDirectRoles,
      @Nonnull final OperationContext opContext) {
    final PolicyEvaluationContext context = new PolicyEvaluationContext();
    if (actorGroupMembership != null) {
      context.setGroups(
          actorGroupMembership.stream().map(Urn::toString).collect(Collectors.toSet()));
    }
    if (actorDirectRoles != null) {
      context.setDirectRoles(actorDirectRoles);
    }
    if (sessionActorIdentity != null) {
      context.setSessionActorIdentity(sessionActorIdentity);
      if (opContext != null) {
        context.setOpContext(opContext);
      }
    }
    return context;
  }

  /** Builds a policy evaluation context pre-seeded with session actor groups and direct roles. */
  @Nonnull
  public PolicyEvaluationContext createSeededEvaluationContext(
      @Nullable final Collection<Urn> actorGroupMembership,
      @Nullable final Set<Urn> actorDirectRoles) {
    return createSeededEvaluationContext(null, actorGroupMembership, actorDirectRoles, null);
  }

  public PolicyActors getMatchingActors(
      final DataHubPolicyInfo policy, final Optional<ResolvedEntitySpec> resource) {
    final List<Urn> users = new ArrayList<>();
    final List<Urn> groups = new ArrayList<>();
    final List<Urn> roles = new ArrayList<>();
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
      if (actorFilter.getUsers() != null) {
        users.addAll(actorFilter.getUsers());
      }
      if (actorFilter.getGroups() != null) {
        groups.addAll(actorFilter.getGroups());
      }
      if (actorFilter.getRoles() != null) {
        roles.addAll(actorFilter.getRoles());
      }

      // 2. Fetch Actors based on resource ownership.
      if (actorFilter.isResourceOwners() && resource.isPresent()) {
        Set<Owner> owners = resource.get().getOwners();
        users.addAll(userOwners(owners));
        groups.addAll(groupOwners(owners));
      }
    }
    return new PolicyActors(users, groups, roles, allUsers, allGroups);
  }

  private PolicyEvaluationResult isPolicyApplicable(
      @Nonnull OperationContext opContext,
      final DataHubPolicyInfo policy,
      final ResolvedEntitySpec resolvedActorSpec,
      final Optional<ResolvedEntitySpec> resource,
      final PolicyEvaluationContext context,
      final List<ResolvedEntitySpec> subResources) {

    // If policy is inactive, simply return DENY.
    if (PoliciesConfig.INACTIVE_POLICY_STATE.equals(policy.getState())) {
      return new PolicyEvaluationResult(policy.getDisplayName(), false, "Inactive Policy");
    }

    final DataHubActorFilter actorFilter = policy.getActors();

    // Applicability is (actor matches) AND (resource matches), so check order never changes the
    // decision -- only the work done before short-circuiting. Resolving the resource scope can
    // trigger expensive, uncached lookups (e.g. the recursive domain/container hierarchy), whereas
    // the user/group/role actor predicates are resource-independent and memoized per request.
    // Evaluate those first so a policy that cannot apply to the actor skips resource resolution.
    final boolean actorMatchesIgnoringOwnership =
        isUserMatch(resolvedActorSpec, actorFilter)
            || isGroupMatch(resolvedActorSpec, actorFilter, context)
            || isRoleMatch(opContext, resolvedActorSpec, actorFilter, context);

    if (actorMatchesIgnoringOwnership) {
      return evaluateResourceScope(policy, resource, subResources);
    }

    if (!actorFilter.isResourceOwners()) {
      return new PolicyEvaluationResult(policy.getDisplayName(), false, "Actor did not match");
    }

    // Resource ownership is the only actor predicate that needs the resource. Resolve the resource
    // scope first so a non-matching resource short-circuits before the ownership lookup, keeping
    // ownership policies exactly as cheap as the original ordering.
    final PolicyEvaluationResult resourceResult =
        evaluateResourceScope(policy, resource, subResources);
    if (!resourceResult.isGranted()) {
      return resourceResult;
    }

    if (isOwnerMatch(opContext, resolvedActorSpec, actorFilter, resource, context)) {
      return new PolicyEvaluationResult(policy.getDisplayName(), true, "Policy is applicable");
    }
    return new PolicyEvaluationResult(policy.getDisplayName(), false, "Actor did not match");
  }

  /**
   * Returns an applicable result iff the resource and any sub-resources are in the policy's scope.
   */
  private PolicyEvaluationResult evaluateResourceScope(
      final DataHubPolicyInfo policy,
      final Optional<ResolvedEntitySpec> resource,
      final List<ResolvedEntitySpec> subResources) {
    if (!isResourceMatch(policy.getType(), policy.getResources(), resource)) {
      return new PolicyEvaluationResult(policy.getDisplayName(), false, "Resource does not match");
    }
    if (!isSubResourceAllowed(policy.getResources(), subResources)) {
      return new PolicyEvaluationResult(policy.getDisplayName(), false, "SubResource not allowed.");
    }
    return new PolicyEvaluationResult(policy.getDisplayName(), true, "Policy is applicable");
  }

  public PolicyGrantedPrivileges getGrantedPrivileges(
      @Nonnull OperationContext opContext,
      final List<DataHubPolicyInfo> policies,
      final ResolvedEntitySpec resolvedActorSpec,
      final Optional<ResolvedEntitySpec> resource,
      final List<ResolvedEntitySpec> subResources) {
    return getGrantedPrivileges(
        opContext,
        policies,
        resolvedActorSpec,
        resource,
        subResources,
        new PolicyEvaluationContext());
  }

  public PolicyGrantedPrivileges getGrantedPrivileges(
      @Nonnull OperationContext opContext,
      final List<DataHubPolicyInfo> policies,
      final ResolvedEntitySpec resolvedActorSpec,
      final Optional<ResolvedEntitySpec> resource,
      final List<ResolvedEntitySpec> subResources,
      @Nonnull final PolicyEvaluationContext context) {
    Set<String> privileges = new HashSet<>();
    Map<String, String> reasonsOfDeny = new HashedMap<>();
    for (DataHubPolicyInfo policy : policies) {
      PolicyEvaluationResult result =
          isPolicyApplicable(opContext, policy, resolvedActorSpec, resource, context, subResources);
      if (result.isGranted()) {
        privileges.addAll(policy.getPrivileges());
      } else {
        reasonsOfDeny.put(result.getPolicyName(), result.getReason());
      }
    }
    return new PolicyGrantedPrivileges(new ArrayList<>(privileges), reasonsOfDeny);
  }

  /**
   * Returns true if the policy matches the resource spec, false otherwise.
   *
   * <p>If the policy is of type "PLATFORM", the resource will always match (since there's no
   * resource). If the policy is of type "METADATA", the resourceSpec parameter will be matched
   * against the resource filter defined on the policy.
   */
  public Boolean policyMatchesResource(
      final DataHubPolicyInfo policy, final Optional<ResolvedEntitySpec> resourceSpec) {
    return isResourceMatch(policy.getType(), policy.getResources(), resourceSpec);
  }

  /**
   * Returns true if the privilege portion of a DataHub policy matches a the privilege being
   * evaluated, false otherwise.
   */
  private boolean isPrivilegeMatch(
      final String requestPrivilege, final List<String> policyPrivileges) {
    return policyPrivileges.contains(requestPrivilege);
  }

  /**
   * Returns true if the resource portion of a DataHub policy matches a the resource being
   * evaluated, false otherwise.
   */
  private boolean isResourceMatch(
      final String policyType,
      final @Nullable DataHubResourceFilter policyResourceFilter,
      final Optional<ResolvedEntitySpec> requestResource) {
    if (PoliciesConfig.PLATFORM_POLICY_TYPE.equals(policyType)) {
      // Currently, platform policies have no associated resource.
      return true;
    }
    if (policyResourceFilter == null) {
      log.debug("No resource defined on the policy.");
      return true;
    }
    if (requestResource.isEmpty()) {
      log.debug("Resource filter present in policy, but no resource spec provided.");
      return false;
    }
    final PolicyMatchFilter filter = getFilter(policyResourceFilter);
    return checkFilter(filter, requestResource.get());
  }

  private boolean isSubResourceAllowed(
      final @Nullable DataHubResourceFilter policyResourceFilter,
      final List<ResolvedEntitySpec> subResources) {
    if (policyResourceFilter == null) {
      log.debug("No resource defined on the policy.");
      return true;
    }
    if (subResources.isEmpty()) {
      log.debug("No subresources to evaluate.");
      return true;
    }
    if (policyResourceFilter.getPrivilegeConstraints() != null) {
      PolicyMatchFilter filter = policyResourceFilter.getPrivilegeConstraints();
      return subResources.stream()
          .allMatch(
              subResource ->
                  WILDCARD_URN.toString().equals(subResource.getSpec().getEntity())
                      || checkFilter(filter, subResource));
    }
    log.debug("No modification constraints specified.");
    return true;
  }

  /**
   * Get filter object from policy resource filter. Make sure it is backward compatible by
   * constructing PolicyMatchFilter object from other fields if the filter field is not set
   */
  private PolicyMatchFilter getFilter(DataHubResourceFilter policyResourceFilter) {
    if (policyResourceFilter.hasFilter()) {
      return policyResourceFilter.getFilter();
    }
    PolicyMatchCriterionArray criteria = new PolicyMatchCriterionArray();
    if (policyResourceFilter.hasType()) {
      criteria.add(
          new PolicyMatchCriterion()
              .setField(EntityFieldType.TYPE.name())
              .setValues(
                  new StringArray(Collections.singletonList(policyResourceFilter.getType()))));
    }

    if (policyResourceFilter.hasResources() && !policyResourceFilter.isAllResources()) {
      criteria.add(
          new PolicyMatchCriterion()
              .setField(EntityFieldType.URN.name())
              .setValues(policyResourceFilter.getResources()));
    }
    return new PolicyMatchFilter().setCriteria(criteria);
  }

  private boolean checkFilter(final PolicyMatchFilter filter, final ResolvedEntitySpec resource) {
    return filter.getCriteria().stream().allMatch(criterion -> checkCriterion(criterion, resource));
  }

  private boolean checkCriterion(
      final PolicyMatchCriterion criterion, final ResolvedEntitySpec resource) {
    EntityFieldType entityFieldType;
    try {
      entityFieldType = EntityFieldType.valueOf(criterion.getField().toUpperCase());
    } catch (IllegalArgumentException e) {
      log.error("Unsupported field type {}", criterion.getField());
      return false;
    }

    Set<String> fieldValues = resource.getFieldValues(entityFieldType);
    return checkCondition(fieldValues, criterion.getValues(), criterion.getCondition());
  }

  private boolean checkCondition(
      Set<String> fieldValues, List<String> filterValues, PolicyMatchCondition condition) {
    switch (condition) {
      case EQUALS:
        return filterValues.stream().anyMatch(fieldValues::contains);
      case STARTS_WITH:
        return filterValues.stream()
            .anyMatch(filterValue -> fieldValues.stream().anyMatch(v -> v.startsWith(filterValue)));
      case NOT_EQUALS:
        return filterValues.stream().noneMatch(fieldValues::contains);
      default:
        log.error("Unsupported condition {}", condition);
        return false;
    }
  }

  /**
   * Returns true if the actor portion of a DataHub policy matches the actor being evaluated, false
   * otherwise.
   */
  boolean isActorMatch(
      @Nonnull OperationContext opContext,
      final ResolvedEntitySpec resolvedActorSpec,
      final DataHubActorFilter actorFilter,
      final Optional<ResolvedEntitySpec> resourceSpec,
      final PolicyEvaluationContext context) {

    // 1. If the actor is a matching "User" in the actor filter, return true immediately.
    if (isUserMatch(resolvedActorSpec, actorFilter)) {
      return true;
    }

    // 2. If the actor is in a matching "Group" in the actor filter, return true immediately.
    if (isGroupMatch(resolvedActorSpec, actorFilter, context)) {
      return true;
    }

    // 3. If the actor is the owner, either directly or indirectly via a group, return true
    // immediately.
    if (isOwnerMatch(opContext, resolvedActorSpec, actorFilter, resourceSpec, context)) {
      return true;
    }

    // 4. If the actor is in a matching "Role" in the actor filter, return true immediately.
    return isRoleMatch(opContext, resolvedActorSpec, actorFilter, context);
  }

  private boolean isUserMatch(
      final ResolvedEntitySpec resolvedActorSpec, final DataHubActorFilter actorFilter) {
    // If the actor is a matching "User" in the actor filter, return true immediately.
    return actorFilter.isAllUsers()
        || (actorFilter.hasUsers()
            && Objects.requireNonNull(actorFilter.getUsers()).stream()
                .map(Urn::toString)
                .anyMatch(user -> user.equals(resolvedActorSpec.getSpec().getEntity())));
  }

  private boolean isGroupMatch(
      final ResolvedEntitySpec resolvedActorSpec,
      final DataHubActorFilter actorFilter,
      final PolicyEvaluationContext context) {
    // If the actor is in a matching "Group" in the actor filter, return true immediately.
    if (actorFilter.isAllGroups() || actorFilter.hasGroups()) {
      final Set<String> groups = resolveGroups(resolvedActorSpec, context);
      return (actorFilter.isAllGroups() && !groups.isEmpty())
          || (actorFilter.hasGroups()
              && Objects.requireNonNull(actorFilter.getGroups()).stream()
                  .map(Urn::toString)
                  .anyMatch(groups::contains));
    }
    // If there are no groups on the policy, return false for the group match.
    return false;
  }

  private boolean isOwnerMatch(
      @Nonnull OperationContext opContext,
      final ResolvedEntitySpec resolvedActorSpec,
      final DataHubActorFilter actorFilter,
      final Optional<ResolvedEntitySpec> requestResource,
      final PolicyEvaluationContext context) {
    // If the policy does not apply to owners, or there is no resource to own, return false
    // immediately.
    if (!actorFilter.isResourceOwners() || requestResource.isEmpty()) {
      return false;
    }
    List<Urn> ownershipTypes = actorFilter.getResourceOwnersTypes();
    return isActorOwner(
        opContext, resolvedActorSpec, requestResource.get(), ownershipTypes, context);
  }

  private Set<String> getOwnersForType(ResolvedEntitySpec spec, List<Urn> ownershipTypes) {
    if (spec.getSpec().getEntity().isEmpty()) {
      return Set.of();
    }
    Stream<Owner> ownersStream = spec.getOwners().stream();
    if (ownershipTypes != null) {
      ownersStream = ownersStream.filter(owner -> ownershipTypes.contains(owner.getTypeUrn()));
    }
    return ownersStream.map(owner -> owner.getOwner().toString()).collect(Collectors.toSet());
  }

  private boolean isActorOwner(
      @Nonnull OperationContext opContext,
      final ResolvedEntitySpec resolvedActorSpec,
      ResolvedEntitySpec resourceSpec,
      List<Urn> ownershipTypes,
      PolicyEvaluationContext context) {
    Set<String> owners = this.getOwnersForType(resourceSpec, ownershipTypes);
    if (isUserOwner(resolvedActorSpec, owners)) {
      return true;
    }
    final Set<String> groups = resolveGroups(resolvedActorSpec, context);

    return isGroupOwner(groups, owners);
  }

  private boolean isUserOwner(final ResolvedEntitySpec resolvedActorSpec, Set<String> owners) {
    return owners.contains(resolvedActorSpec.getSpec().getEntity());
  }

  private boolean isGroupOwner(Set<String> groups, Set<String> owners) {
    return groups.stream().anyMatch(owners::contains);
  }

  private boolean isRoleMatch(
      @Nonnull OperationContext opContext,
      final ResolvedEntitySpec resolvedActorSpec,
      final DataHubActorFilter actorFilter,
      final PolicyEvaluationContext context) {
    // Can immediately return false if the actor filter does not have any roles
    if (!actorFilter.hasRoles()) {
      return false;
    }
    // If the actor has a matching "Role" in the actor filter, return true immediately.
    Set<Urn> actorRoles = resolveRoles(opContext, resolvedActorSpec, context);
    return Objects.requireNonNull(actorFilter.getRoles()).stream().anyMatch(actorRoles::contains);
  }

  private Set<Urn> resolveRoles(
      @Nonnull OperationContext opContext,
      final ResolvedEntitySpec resolvedActorSpec,
      PolicyEvaluationContext context) {
    if (context.roles != null) {
      return context.roles;
    }

    if (context.sessionActorIdentity != null && context.opContext != null) {
      final Set<Urn> roles =
          context.sessionActorIdentity.resolveAllRoles(
              groups -> fetchRolesViaGroups(context.opContext, groups));
      context.setRoles(roles);
      return roles;
    }

    if (context.directRoles != null && context.groups != null) {
      final Set<Urn> roles = new HashSet<>(context.directRoles);
      if (!context.groups.isEmpty()) {
        roles.addAll(
            fetchRolesViaGroups(
                opContext,
                context.groups.stream().map(UrnUtils::getUrn).collect(Collectors.toList())));
      }
      context.setRoles(roles);
      return roles;
    }

    String actor = resolvedActorSpec.getSpec().getEntity();

    Set<Urn> roles = new HashSet<>();
    try {
      Urn actorUrn = Urn.createFromString(actor);
      final SessionActorIdentity identity = _groupService.fetchUserIdentity(opContext, actorUrn);
      roles.addAll(identity.resolveAllRoles(groups -> fetchRolesViaGroups(opContext, groups)));
    } catch (Exception e) {
      log.error(
          String.format("Failed to fetch %s for urn %s", ROLE_MEMBERSHIP_ASPECT_NAME, actor), e);
      return roles;
    }

    if (!roles.isEmpty()) {
      context.setRoles(roles);
    }

    return roles;
  }

  @Nonnull
  private Set<Urn> fetchRolesViaGroups(
      @Nonnull final OperationContext opContext, @Nonnull final Collection<Urn> groups) {
    final ServicesRegistryContext servicesRegistry = opContext.getServicesRegistryContext();
    if (servicesRegistry != null && servicesRegistry.getActorGroupMembershipService() != null) {
      return servicesRegistry.fetchRolesViaGroups(opContext, groups);
    }
    return _groupService.fetchRolesViaGroups(opContext, groups);
  }

  private Set<String> resolveGroups(
      ResolvedEntitySpec resolvedActorSpec, PolicyEvaluationContext context) {
    if (context.groups != null) {
      return context.groups;
    }

    Set<String> groups = resolvedActorSpec.getGroupMembership();

    context.setGroups(groups); // Cache the groups.
    return groups;
  }

  /** Class used to store state across a single Policy evaluation. */
  public static class PolicyEvaluationContext {
    private Set<String> groups;
    private Set<Urn> directRoles;
    private Set<Urn> roles;
    private SessionActorIdentity sessionActorIdentity;
    private OperationContext opContext;
    // Resource owners cache, keyed by resource URN. Scoped to a single authorize() call: dedupes
    // the ownership fetch across all ownership policies evaluated for the same resource.
    private final Map<String, List<Owner>> resourceOwnersByUrn = new HashMap<>();

    Map<String, List<Owner>> getResourceOwnersByUrn() {
      return resourceOwnersByUrn;
    }

    public void setGroups(Set<String> groups) {
      this.groups = groups;
    }

    public void setDirectRoles(Set<Urn> directRoles) {
      this.directRoles = directRoles;
    }

    public void setRoles(Set<Urn> roles) {
      this.roles = roles;
    }

    public void setSessionActorIdentity(SessionActorIdentity sessionActorIdentity) {
      this.sessionActorIdentity = sessionActorIdentity;
    }

    public void setOpContext(OperationContext opContext) {
      this.opContext = opContext;
    }
  }

  /** Class used to represent granted privileges and reasons why * */
  public static class PolicyGrantedPrivileges {
    private final List<String> privileges;
    private final Map<String, String> reasonOfDeny;

    private PolicyGrantedPrivileges(List<String> privileges, Map<String, String> reasonOfDeny) {
      this.privileges = privileges;
      this.reasonOfDeny = reasonOfDeny;
    }

    public List<String> getPrivileges() {
      return this.privileges;
    }

    public Map<String, String> getReasonOfDeny() {
      return this.reasonOfDeny;
    }
  }

  /** Class used to represent the result of a Policy evaluation */
  public static class PolicyEvaluationResult {
    private final String policyName;
    private final boolean isGranted;
    private final String reason;

    private PolicyEvaluationResult(String policyName, boolean isGranted, String reason) {
      this.policyName = policyName;
      this.isGranted = isGranted;
      this.reason = reason;
    }

    public boolean isGranted() {
      return this.isGranted;
    }

    public String getReason() {
      return this.reason;
    }

    public String getPolicyName() {
      return this.policyName;
    }
  }

  /** Class used to represent all valid users of a policy. */
  @Value
  @AllArgsConstructor(access = AccessLevel.PUBLIC)
  public static class PolicyActors {
    List<Urn> users;
    List<Urn> groups;
    List<Urn> roles;
    Boolean allUsers;
    Boolean allGroups;
  }

  private List<Urn> userOwners(final Set<Owner> owners) {
    return owners.stream()
        .map(Owner::getOwner)
        .filter(owner -> CORP_USER_ENTITY_NAME.equals(owner.getEntityType()))
        .collect(Collectors.toList());
  }

  private List<Urn> groupOwners(final Set<Owner> owners) {
    return owners.stream()
        .map(Owner::getOwner)
        .filter(owner -> CORP_GROUP_ENTITY_NAME.equals(owner.getEntityType()))
        .collect(Collectors.toList());
  }
}
