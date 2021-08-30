package com.datahub.metadata.authorization;

import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.AspectClient;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.GroupMembership;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.aspect.DataHubPolicyAspect;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.metadata.snapshot.DataHubPolicySnapshot;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import com.linkedin.r2.RemoteInvocationException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;


/**
 * The Authorizer is a singleton class responsible for authorizing
 * operations on the DataHub platform.
 *
 * Currently, the authorizer is implemented as a spring-instantiated Singleton
 * which manages its own thread-pool used for resolving policy predicates.
 */
// TODO: Decouple this from all Rest.li objects if possible.
// Try to avoid synchronizing on the entire cache if possible.
@Slf4j
public class AuthorizationManager implements Authorizer {

  private static final String INACTIVE_POLICY_STATE = "INACTIVE"; // TODO move to PolicyUtils.

  private static final String SYSTEM_PRINCIPAL = "urn:li:corpuser:system";

  // Maps privilege name to the associated set of policies for fast access. Not concurrent data structure because writes are always against the entire thing.
  // We can improve in the future by syncing only policy updates, instead of everything.
  private final Map<String, List<DataHubPolicyInfo>> _policyCache = new HashMap<>(); // Shared Policy Cache.

  private final EntityClient _entityClient;
  private final AspectClient _aspectClient;
  private final ScheduledExecutorService _refreshExecutorService = Executors.newScheduledThreadPool(1);
  private final PolicyRefreshRunnable _policyRefreshRunnable;
  private final AuthorizationMode _mode;

  public AuthorizationManager(
      final EntityClient entityClient,
      final AspectClient aspectClient,
      final int refreshIntervalSeconds,
      final AuthorizationMode mode) {
    _entityClient = entityClient;
    _aspectClient = aspectClient;
    _policyRefreshRunnable = new PolicyRefreshRunnable(entityClient, _policyCache);
    _refreshExecutorService.scheduleAtFixedRate(_policyRefreshRunnable, 10, refreshIntervalSeconds, TimeUnit.SECONDS);
    _mode = mode;
  }

  public AuthorizationResult authorize(final AuthorizationRequest request) {
    final List<DataHubPolicyInfo> policiesToEvaluate = _policyCache.getOrDefault(request.privilege(), new ArrayList<>());

    for (DataHubPolicyInfo policy : policiesToEvaluate) {
      if (INACTIVE_POLICY_STATE.equals(policy.getState())) {
        continue;
      }
      // TODO: Split out into a proper PolicyEngine.
      if (isAllowed(request, policy)) {
        // Short circuit. - Policy has granted privileges to this actor.
        return new AuthorizationResult(request, Optional.of(policy), AuthorizationResult.Type.ALLOW);
      }
    }
    return new AuthorizationResult(request, Optional.empty(), AuthorizationResult.Type.DENY);
  }

  public void invalidateCache() {
    // Invalidates the cache on-demand and fires off a refresh thread.
    _refreshExecutorService.execute(_policyRefreshRunnable);
  }

  @Override
  public AuthorizationMode mode() {
    return _mode;
  }

  // Notice that this is not general purpose. In the future we may need something more generalizable.
  private boolean isAllowed(final AuthorizationRequest request, final DataHubPolicyInfo policy) {

    // Evaluate policy predicates against the request.
    // For now, this is kept fairly straightforward.
    // We already know the privileges should match.

    boolean actorMatch = false;
    boolean resourceMatch = true;
    boolean groupMatch = false;
    boolean ownerMatch = false;

    // Check if user principal matches the policy.
    if (isBasicActorMatch(request.principal(), policy.getActors())) {
      actorMatch = true;
    }

    // Check if the resource matches the policy.
    if (policy.hasResources() && !isBasicResourceMatch(request.resourceSpec().get(), policy.getResources())) {
      resourceMatch = false;
    }

    // If the resource doesn't apply, short circuit.
    if (!resourceMatch) {
      return false;
    }

    // If the shallow actor match doesn't apply, try a deeper match.
    if (actorMatch) {
      // Shallow checks are done. Go for deep.
      return true;
    }

    Set<String> groups = null;

    // Check against "group member" predicates.
    if (policy.getActors().hasGroups() || policy.getActors().isAllGroups()) {
      // Policy applies to some groups.
      groups = resolveGroups(request.principal());
      groupMatch = isGroupActorMatch(groups, policy.getActors());
    }

    if (groupMatch) {
      return true;
    }

    // Finally, check against "ownership" predicates.
    if (policy.getActors().isResourceOwners() && request.resourceSpec().isPresent()) {
      if (groups == null) {
        groups = resolveGroups(request.principal());
      }
      ownerMatch = isOwnerActorMatch(request.principal(), groups, request.resourceSpec().get(), policy.getActors());
    }

    if (ownerMatch) {
      return true;
    }

    return false;
  }

  private boolean isBasicActorMatch(final String principal, final DataHubActorFilter actorFilter) {
    return actorFilter.isAllUsers() || (actorFilter.hasUsers() && Objects.requireNonNull(actorFilter.getUsers())
        .stream()
        .anyMatch(user -> user.toString().equals(principal)));
  }

  private boolean isBasicResourceMatch(
      final AuthorizationRequest.ResourceSpec resourceSpec,
      final DataHubResourceFilter resourceFilter) {
    final boolean resourceTypesMatch = resourceFilter.hasType() && resourceFilter.getType().equals(resourceSpec.getType());
    return resourceTypesMatch
        && (resourceFilter.isAllResources() || (resourceFilter.hasResources() && Objects.requireNonNull(resourceFilter.getResources())
        .stream()
        .anyMatch(resource -> resource.toString().equals(resourceSpec.getUrn()))));
  }

  private boolean isGroupActorMatch(final Set<String> groups, final DataHubActorFilter actorFilter) {
    if (groups.size() > 0) {
      return actorFilter.isAllGroups() || (actorFilter.hasGroups() && Objects.requireNonNull(actorFilter.getGroups())
          .stream()
          .anyMatch(groupUrn -> groups.contains(groupUrn.toString())));
    }
    return false;
  }

  private boolean isOwnerActorMatch(
      final String principal,
      final Set<String> groups,
      final AuthorizationRequest.ResourceSpec resource,
      final DataHubActorFilter actorFilter) {
    // Otherwise, attempt to determine whether the principal is the resource owner.
    // How can we do this without special purpose logic.
    try {
      // Fetch the latest version of "ownership" aspect.
      final VersionedAspect aspect = _aspectClient.getAspect(resource.getUrn(), "ownership", 0L, SYSTEM_PRINCIPAL);
      final Ownership ownership = aspect.getAspect().getOwnership();

      if (isUserOwner(principal, ownership)) {
        return true;
      }

      if (isGroupOwner(groups, ownership)) {
        return true;
      }

    } catch (Exception e) {
        // todo: specifically catch the 404 returned by GMS when ownership does not exist.
        log.error(String.format("Failed to resolve Ownership of resource with URN %s. Returning deny", resource.getUrn()), e);
    }
    return false;
  }

  private boolean isUserOwner(String principal, Ownership ownership) {
    return ownership.getOwners().stream().anyMatch(owner -> principal.equals(owner.getOwner().toString()));
  }

  private boolean isGroupOwner(Set<String> groups, Ownership ownership) {
    return ownership.getOwners().stream().anyMatch(owner -> groups.contains(owner.getOwner().toString()));
  }

  private Set<String> resolveGroups(String principal) {
    Set<String> groups = new HashSet<>();
    try {
      Urn corpUserUrn = Urn.createFromString(principal);
      Optional<GroupMembership> maybeGroups = resolveGroupMembership(corpUserUrn);
      maybeGroups.ifPresent(groupMembership -> groupMembership.getGroups().forEach(groupUrn -> groups.add(groupUrn.toString())));
      return groups;
    } catch (URISyntaxException e) {
      log.error(String.format("Failed to bind principal %s to an URN. Denying the authorization request", principal));
      return Collections.emptySet();
    }
  }

  // TODO: Cache the group membership locally. Refresh periodically.
  private Optional<GroupMembership> resolveGroupMembership(final Urn urn) {
    // 1. get user snapshot.
    try {
      final CorpUserSnapshot corpUser = _entityClient.get(urn, SYSTEM_PRINCIPAL).getValue().getCorpUserSnapshot();
      for (CorpUserAspect aspect : corpUser.getAspects()) {
        if (aspect.isGroupMembership()) {
          // Found group membership.
          return Optional.of(aspect.getGroupMembership());
        }
      }

    } catch (RemoteInvocationException e) {
      throw new RuntimeException(String.format("Failed to fetch corpUser for urn %s", urn), e);
    }
    return Optional.empty();
  }

  private static class PolicyRefreshRunnable implements Runnable {

    private static final String POLICY_ENTITY_NAME = "dataHubPolicy";

    private final EntityClient _entityClient; // Used for fetching policies from GMS.
    private final Map<String, List<DataHubPolicyInfo>> _policyCache;

    public PolicyRefreshRunnable(
        final EntityClient entityClient,
        final Map<String, List<DataHubPolicyInfo>> policyCache) {
      _entityClient = entityClient;
      _policyCache = policyCache;
    }

    @Override
    public void run() {
      try {
        // Populate new cache and swap.
        Map<String, List<DataHubPolicyInfo>> newCache = new HashMap<>();

        int start = 0;
        int count = 30;
        int total = 30;

        while (start < total) {
          try {
            log.debug(String.format("Batch fetching policies. start: %s, count: %s ", start, count));
            final ListUrnsResult policyUrns = _entityClient.listUrns(POLICY_ENTITY_NAME, start, count, SYSTEM_PRINCIPAL);
            final Map<Urn, Entity> policyEntities = _entityClient.batchGet(new HashSet<>(policyUrns.getEntities()), SYSTEM_PRINCIPAL);

            addPoliciesToCache(newCache, policyEntities
                .values()
                .stream()
                .map(entity -> entity.getValue().getDataHubPolicySnapshot())
                .collect(Collectors.toList()));

            total = policyUrns.getTotal();
            start = start + count;
          } catch (RemoteInvocationException e) {
            log.error(String.format(
                "Failed to retrieve policy urns! Skipping updating policy cache until next refresh. start: %s, count: %s", start, count), e);
            return;
          }
          synchronized (_policyCache) {
            _policyCache.clear();
            _policyCache.putAll(newCache);
          }
        }
        log.debug(String.format("Successfully fetched %s policies.", total));
      } catch (Exception e) {
        log.error("Caught exception while loading Policy cache. Will retry on next scheduled attempt.", e);
      }
    }

    private void addPoliciesToCache(final Map<String, List<DataHubPolicyInfo>> cache, final List<DataHubPolicySnapshot> snapshots) {
      for (final DataHubPolicySnapshot snapshot : snapshots) {
        addPolicyToCache(cache, snapshot);
      }
    }

    private void addPolicyToCache(final Map<String, List<DataHubPolicyInfo>> cache, final DataHubPolicySnapshot snapshot) {
      for (DataHubPolicyAspect aspect : snapshot.getAspects()) {
        if (aspect.isDataHubPolicyInfo()) {
          addPolicyToCache(cache, aspect.getDataHubPolicyInfo());
          return;
        }
      }
      throw new IllegalArgumentException(
          String.format("Failed to find DataHubPolicyInfo aspect in DataHubPolicySnapshot data %s. Invalid state.", snapshot.data()));
    }

    private void addPolicyToCache(final Map<String, List<DataHubPolicyInfo>> cache, final DataHubPolicyInfo policy) {
      final List<String> privileges = policy.getPrivileges();
      for (String privilege : privileges) {
        List<DataHubPolicyInfo> existingPolicies = cache.getOrDefault(privilege, new ArrayList<>());
        existingPolicies.add(policy);
        cache.put(privilege, existingPolicies);
      }
    }
  }
}
