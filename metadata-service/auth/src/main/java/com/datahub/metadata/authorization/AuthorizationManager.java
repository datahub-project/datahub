package com.datahub.metadata.authorization;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.aspect.DataHubPolicyAspect;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.snapshot.DataHubPolicySnapshot;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import com.linkedin.policy.DataHubResourceFilter;
import com.linkedin.r2.RemoteInvocationException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
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
@Slf4j
public class AuthorizationManager {

  // Maps privilege name to the associated set of policies for fast access. Not concurrent because writes are always against the entire thing.
  // We can improve in the future by syncing only policy updates, instead of everything.
  private final Map<String, List<DataHubPolicyInfo>> _policyCache = new HashMap<>(); // Shared Policy Cache.

  private final ScheduledExecutorService _refreshExecutorService = Executors.newScheduledThreadPool(1);
  private final PolicyRefreshRunnable _policyRefreshRunnable;

  public AuthorizationManager(
      final EntityClient entityClient,
      final int refreshIntervalSeconds) {
    _policyRefreshRunnable = new PolicyRefreshRunnable(entityClient, _policyCache);
    _refreshExecutorService.scheduleAtFixedRate(_policyRefreshRunnable, 10, refreshIntervalSeconds, TimeUnit.SECONDS);
  }

  public CompletableFuture<AuthorizationResult> authorize(final AuthorizationRequest request) {
    return CompletableFuture.supplyAsync(() -> {
      final List<DataHubPolicyInfo> policiesToEvaluate = _policyCache.get(request.privilege());
      for (DataHubPolicyInfo policy : policiesToEvaluate) {
        // TODO: Split out into a proper PolicyEngine.
        if (isAllowed(request, policy)) {
          // Short circuit. - Policy has granted privileges to this actor.
          return new AuthorizationResult(request, Optional.of(policy), AuthorizationResult.Type.ALLOW);
        }
      }
      return new AuthorizationResult(request, Optional.empty(), AuthorizationResult.Type.DENY);
    });
  }

  public void invalidateCache() {
    // Invalidates the cache on-demand and fires off a refresh thread.
    _refreshExecutorService.execute(_policyRefreshRunnable);
  }

  // Notice that this is not very general purpose. In the future we may need something more generalizable.
  private boolean isAllowed(final AuthorizationRequest request, final DataHubPolicyInfo policy) {

    // Evaluate policy predicates against the request.
    // For now, this is kept fairly straightforward.
    // We already know the privileges should match.

    boolean actorMatch = false;
    boolean resourceMatch = true;

    // Cheap Match Predicates
    if (isBasicActorMatch(request.principal(), policy.getActors())) {
      actorMatch = true;
    }

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

    return false;

    // Deeper match predicates (where additional facts need to be collected).

    // Should be a more generalizable edge predicate or actor attribute predicate.

    // if (isGroupActorMatch()) {
      // 1. Resolve Groups for user. (Use a cache)
      // 2. Check if any of the groups match the policy.
   // }

    // Deep match predicates.
    // if (isOwnerActorMatch()) {
    //
    //}
  }

  private boolean isBasicActorMatch(final String principal, final DataHubActorFilter actorFilter) {
    return actorFilter.isAllUsers() || (actorFilter.hasUsers() && Objects.requireNonNull(actorFilter.getUsers())
        .stream()
        .anyMatch(user -> user.toString().equals(principal)));
  }

  private boolean isBasicResourceMatch(
      final AuthorizationRequest.ResourceSpec resourceSpec,
      final DataHubResourceFilter resourceFilter) {
    return resourceFilter.isAllResources() || (resourceFilter.hasResources() && Objects.requireNonNull(resourceFilter.getResources())
        .stream()
        .anyMatch(resource -> resource.toString().equals(resourceSpec.getUrn())));
  }

  private static class PolicyRefreshRunnable implements Runnable {

    private static final String POLICY_ENTITY_NAME = "dataHubPolicy";
    private static final String SYSTEM_PRINCIPAL = "urn:li:corpuser:system";

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
      // Here's where we populate the cache.
      synchronized (_policyCache) {

        int start = 0;
        int count = 30;
        int total = 30;

        while (start < total) {
          try {
            log.debug(String.format("Batch fetching policies. start: %s, count: %s ", start, count));
            final ListUrnsResult policyUrns = _entityClient.listUrns(POLICY_ENTITY_NAME, start, count, SYSTEM_PRINCIPAL);
            final Map<Urn, Entity> policyEntities = _entityClient.batchGet(new HashSet<>(policyUrns.getEntities()), SYSTEM_PRINCIPAL);

            addPoliciesToCache(policyEntities.values().stream().map(entity -> entity.getValue().getDataHubPolicySnapshot()).collect(
                Collectors.toList()));

            total = policyUrns.getTotal();
            start = start + count;
          } catch (RemoteInvocationException e) {
            log.error(String.format(
                "Failed to retrieve policy urns! Skipping updating policy cache until next refresh. start: %s, count: %s", start, count), e);
            return;
          }
        }
        log.debug(String.format("Successfully fetched %s policies.", total));
      }
    }

    private void addPoliciesToCache(final List<DataHubPolicySnapshot> snapshots) {
      for (final DataHubPolicySnapshot snapshot : snapshots) {
        addPoliciesToCache(snapshot);
      }
    }

    private void addPoliciesToCache(final DataHubPolicySnapshot snapshot) {
      for (DataHubPolicyAspect aspect : snapshot.getAspects()) {
        if (aspect.isDataHubPolicyInfo()) {
          addPolicyToCache(aspect.getDataHubPolicyInfo());
        }
      }
      throw new IllegalArgumentException(
          String.format("Failed to find DataHubPolicyInfo aspect in DataHubPolicySnapshot data %s. Invalid state.", snapshot.data()));
    }

    private void addPolicyToCache(final DataHubPolicyInfo policy) {
      final List<String> privileges = policy.getPrivileges();
      for (String privilege : privileges) {
        List<DataHubPolicyInfo> existingPolicies = _policyCache.getOrDefault(privilege, new ArrayList<>());
        existingPolicies.add(policy);
        _policyCache.put(privilege, existingPolicies);
      }
    }
  }
}
