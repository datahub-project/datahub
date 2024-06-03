package com.datahub.authorization;

import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.policy.DataHubPolicyInfo;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * The Authorizer is a singleton class responsible for authorizing operations on the DataHub
 * platform via DataHub Policies.
 *
 * <p>Currently, the authorizer is implemented as a spring-instantiated Singleton which manages its
 * own thread-pool used for resolving policy predicates.
 */
// TODO: Decouple this from all Rest.li objects if possible.
@Slf4j
public class DataHubAuthorizer implements Authorizer {

  public enum AuthorizationMode {
    /** Default mode simply means that authorization is enforced, with a DENY result returned */
    DEFAULT,
    /**
     * Allow all means that the DataHubAuthorizer will allow all actions. This is used as an
     * override to disable the policies feature.
     */
    ALLOW_ALL
  }

  // Maps privilege name to the associated set of policies for fast access.
  // Not concurrent data structure because writes are always against the entire thing.
  protected final Map<String, List<DataHubPolicyInfo>> policyCache =
      new HashMap<>(); // Shared Policy Cache.
  protected final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  protected final Lock readLock = readWriteLock.readLock();

  private final ScheduledExecutorService refreshExecutorService =
      Executors.newScheduledThreadPool(1);
  private final PolicyRefreshRunnable policyRefreshRunnable;
  private final PolicyEngine policyEngine;
  private EntitySpecResolver entitySpecResolver;
  private AuthorizationMode mode;
  @Getter private final OperationContext systemOpContext;

  public static final String ALL = "ALL";

  public DataHubAuthorizer(
      @Nonnull final OperationContext systemOpContext,
      final EntityClient entityClient,
      final int delayIntervalSeconds,
      final int refreshIntervalSeconds,
      final AuthorizationMode mode,
      final int policyFetchSize) {
    this.systemOpContext = systemOpContext;
    this.mode = Objects.requireNonNull(mode);
    policyEngine = new PolicyEngine(Objects.requireNonNull(entityClient));
    if (refreshIntervalSeconds > 0) {
      policyRefreshRunnable =
          new PolicyRefreshRunnable(
              systemOpContext,
              new PolicyFetcher(entityClient),
              policyCache,
              readWriteLock.writeLock(),
              policyFetchSize);
      refreshExecutorService.scheduleAtFixedRate(
          policyRefreshRunnable, delayIntervalSeconds, refreshIntervalSeconds, TimeUnit.SECONDS);
    } else {
      policyRefreshRunnable = null;
    }
  }

  @Override
  public void init(@Nonnull Map<String, Object> authorizerConfig, @Nonnull AuthorizerContext ctx) {
    // Pass. No static config.
    entitySpecResolver = Objects.requireNonNull(ctx.getEntitySpecResolver());
  }

  public AuthorizationResult authorize(@Nonnull final AuthorizationRequest request) {

    // 0. Short circuit: If the action is being performed by the system (root), always allow it.
    if (isSystemRequest(request, systemOpContext.getAuthentication())) {
      return new AuthorizationResult(request, AuthorizationResult.Type.ALLOW, null);
    }

    Optional<ResolvedEntitySpec> resolvedResourceSpec =
        request.getResourceSpec().map(entitySpecResolver::resolve);

    // 1. Fetch the policies relevant to the requested privilege.
    final List<DataHubPolicyInfo> policiesToEvaluate =
        new LinkedList<>(getOrDefault(request.getPrivilege(), new ArrayList<>()));
    policiesToEvaluate.addAll(
        PoliciesConfig.getDefaultPolicies(UrnUtils.getUrn(request.getActorUrn())));

    // 2. Evaluate each policy.
    for (DataHubPolicyInfo policy : policiesToEvaluate) {
      if (isRequestGranted(policy, request, resolvedResourceSpec)) {
        // Short circuit if policy has granted privileges to this actor.
        return new AuthorizationResult(
            request,
            AuthorizationResult.Type.ALLOW,
            String.format("Granted by policy with type: %s", policy.getType()));
      }
    }
    return new AuthorizationResult(request, AuthorizationResult.Type.DENY, null);
  }

  public List<String> getGrantedPrivileges(
      final String actor, final Optional<EntitySpec> resourceSpec) {

    Urn actorUrn = UrnUtils.getUrn(actor);

    // 1. Fetch all policies
    final List<DataHubPolicyInfo> policiesToEvaluate =
        new LinkedList<>(getOrDefault(ALL, new ArrayList<>()));
    policiesToEvaluate.addAll(PoliciesConfig.getDefaultPolicies(actorUrn));

    final ResolvedEntitySpec resolvedActorSpec =
        entitySpecResolver.resolve(new EntitySpec(actorUrn.getEntityType(), actor));

    Optional<ResolvedEntitySpec> resolvedResourceSpec =
        resourceSpec.map(entitySpecResolver::resolve);

    return policyEngine.getGrantedPrivileges(
        systemOpContext, policiesToEvaluate, resolvedActorSpec, resolvedResourceSpec);
  }

  @Override
  public Set<DataHubPolicyInfo> getActorPolicies(@Nonnull Urn actorUrn) {
    // 1. Fetch all policies
    final List<DataHubPolicyInfo> policiesToEvaluate =
        new LinkedList<>(getOrDefault(ALL, new ArrayList<>()));
    policiesToEvaluate.addAll(PoliciesConfig.getDefaultPolicies(actorUrn));

    // 2. Actor identity
    final ResolvedEntitySpec resolvedActorSpec =
        entitySpecResolver.resolve(new EntitySpec(actorUrn.getEntityType(), actorUrn.toString()));

    return policiesToEvaluate.stream()
        .filter(policy -> PoliciesConfig.ACTIVE_POLICY_STATE.equals(policy.getState()))
        .filter(
            policy ->
                (policy.getActors() != null && policy.getActors().isResourceOwners())
                    || policyEngine.isActorMatch(
                        systemOpContext,
                        resolvedActorSpec,
                        policy.getActors(),
                        Optional.empty(),
                        new PolicyEngine.PolicyEvaluationContext()))
        .collect(Collectors.toSet());
  }

  @Override
  public Collection<Urn> getActorGroups(@Nonnull Urn actorUrn) {
    // 1. Actor identity
    final ResolvedEntitySpec resolvedActorSpec =
        entitySpecResolver.resolve(new EntitySpec(actorUrn.getEntityType(), actorUrn.toString()));

    return resolvedActorSpec.getGroupMembership().stream()
        .map(UrnUtils::getUrn)
        .collect(Collectors.toList());
  }

  @Override
  public Collection<Urn> getActorPeers(@Nonnull Urn actorUrn) {
    // TODO: Fetch users from groups the actor is a member of
    return List.of(actorUrn);
  }

  /**
   * Retrieves the current list of actors authorized to for a particular privilege against an
   * optional resource
   */
  public AuthorizedActors authorizedActors(
      final String privilege, final Optional<EntitySpec> resourceSpec) {

    final List<Urn> authorizedUsers = new ArrayList<>();
    final List<Urn> authorizedGroups = new ArrayList<>();
    final List<Urn> authorizedRoles = new ArrayList<>();
    boolean allUsers = false;
    boolean allGroups = false;

    // Step 1: Find policies granting the privilege.
    final List<DataHubPolicyInfo> policiesToEvaluate = getOrDefault(privilege, new ArrayList<>());

    Optional<ResolvedEntitySpec> resolvedResourceSpec =
        resourceSpec.map(entitySpecResolver::resolve);

    // Step 2: For each policy, determine whether the resource is a match.
    for (DataHubPolicyInfo policy : policiesToEvaluate) {
      if (!PoliciesConfig.ACTIVE_POLICY_STATE.equals(policy.getState())) {
        // Policy is not active, skip.
        continue;
      }

      final PolicyEngine.PolicyActors matchingActors =
          policyEngine.getMatchingActors(policy, resolvedResourceSpec);

      // Step 3: For each matching policy, add actors that are authorized.
      authorizedUsers.addAll(matchingActors.getUsers());
      authorizedGroups.addAll(matchingActors.getGroups());
      authorizedRoles.addAll(matchingActors.getRoles());
      if (matchingActors.getAllUsers()) {
        allUsers = true;
      }
      if (matchingActors.getAllGroups()) {
        allGroups = true;
      }
    }

    // Step 4: Return all authorized users and groups.
    return new AuthorizedActors(
        privilege, authorizedUsers, authorizedGroups, authorizedRoles, allUsers, allGroups);
  }

  /**
   * Invalidates the policy cache and fires off a refresh thread. Should be invoked when a policy is
   * created, modified, or deleted.
   */
  public void invalidateCache() {
    if (policyRefreshRunnable != null) {
      refreshExecutorService.execute(policyRefreshRunnable);
    }
  }

  public AuthorizationMode mode() {
    return mode;
  }

  public void setMode(final AuthorizationMode mode) {
    this.mode = mode;
  }

  /**
   * Returns true if the request's is coming from the system itself, in which cases the action is
   * always authorized.
   */
  private boolean isSystemRequest(
      final AuthorizationRequest request, final Authentication systemAuthentication) {
    return systemAuthentication.getActor().toUrnStr().equals(request.getActorUrn());
  }

  /** Returns true if a policy grants the requested privilege for a given actor and resource. */
  private boolean isRequestGranted(
      final DataHubPolicyInfo policy,
      final AuthorizationRequest request,
      final Optional<ResolvedEntitySpec> resourceSpec) {
    if (AuthorizationMode.ALLOW_ALL.equals(mode())) {
      return true;
    }

    Optional<Urn> actorUrn = getUrnFromRequestActor(request.getActorUrn());
    if (actorUrn.isEmpty()) {
      return false;
    }

    try {
      final ResolvedEntitySpec resolvedActorSpec =
          entitySpecResolver.resolve(
              new EntitySpec(actorUrn.get().getEntityType(), request.getActorUrn()));

      final PolicyEngine.PolicyEvaluationResult result =
          policyEngine.evaluatePolicy(
              systemOpContext, policy, resolvedActorSpec, request.getPrivilege(), resourceSpec);
      return result.isGranted();
    } catch (RuntimeException e) {
      log.error("Error evaluating policy {} for request {}", policy.getDisplayName(), request);
      throw e;
    }
  }

  private Optional<Urn> getUrnFromRequestActor(String actor) {
    try {
      return Optional.of(Urn.createFromString(actor));
    } catch (URISyntaxException e) {
      log.error(
          String.format(
              "Failed to bind actor %s to an URN. Actors must be URNs. Denying the authorization request",
              actor));
      return Optional.empty();
    }
  }

  private List<DataHubPolicyInfo> getOrDefault(String key, List<DataHubPolicyInfo> defaultValue) {
    readLock.lock();
    try {
      return policyCache.getOrDefault(key, defaultValue);
    } finally {
      // To unlock the acquired read thread
      readLock.unlock();
    }
  }

  /**
   * A {@link Runnable} used to periodically fetch a new instance of the policies Cache.
   *
   * <p>Currently, the refresh logic is not very smart. When the cache is invalidated, we simply
   * re-fetch the entire cache using Policies stored in the backend.
   */
  @VisibleForTesting
  @RequiredArgsConstructor
  static class PolicyRefreshRunnable implements Runnable {

    private final OperationContext systemOpContext;
    private final PolicyFetcher policyFetcher;
    private final Map<String, List<DataHubPolicyInfo>> policyCache;
    private final Lock writeLock;
    private final int count;

    @Override
    public void run() {
      try {
        // Populate new cache and swap.
        Map<String, List<DataHubPolicyInfo>> newCache = new HashMap<>();
        Integer total = null;
        String scrollId = null;

        while (total == null || scrollId != null) {
          try {
            final PolicyFetcher.PolicyFetchResult policyFetchResult =
                policyFetcher.fetchPolicies(systemOpContext, count, scrollId, null);

            addPoliciesToCache(newCache, policyFetchResult.getPolicies());

            total = policyFetchResult.getTotal();
            scrollId = policyFetchResult.getScrollId();
          } catch (Exception e) {
            log.error(
                "Failed to retrieve policy urns! Skipping updating policy cache until next refresh. count: {}, scrollId: {}",
                count,
                scrollId,
                e);
            return;
          }
        }

        writeLock.lock();
        try {
          policyCache.clear();
          policyCache.putAll(newCache);
        } finally {
          // To unlock the acquired write thread
          writeLock.unlock();
        }

        log.debug(String.format("Successfully fetched %s policies.", total));
      } catch (Exception e) {
        log.error(
            "Caught exception while loading Policy cache. Will retry on next scheduled attempt.",
            e);
      }
    }

    private void addPoliciesToCache(
        final Map<String, List<DataHubPolicyInfo>> cache,
        final List<PolicyFetcher.Policy> policies) {
      policies.forEach(policy -> addPolicyToCache(cache, policy.getPolicyInfo()));
    }

    private void addPolicyToCache(
        final Map<String, List<DataHubPolicyInfo>> cache, final DataHubPolicyInfo policy) {
      final List<String> privileges = policy.getPrivileges();
      for (String privilege : privileges) {
        List<DataHubPolicyInfo> existingPolicies =
            cache.containsKey(privilege)
                ? new ArrayList<>(cache.get(privilege))
                : new ArrayList<>();
        existingPolicies.add(policy);
        cache.put(privilege, existingPolicies);
      }
      List<DataHubPolicyInfo> existingPolicies =
          cache.containsKey(ALL) ? new ArrayList<>(cache.get(ALL)) : new ArrayList<>();
      existingPolicies.add(policy);
      cache.put(ALL, existingPolicies);
    }
  }
}
