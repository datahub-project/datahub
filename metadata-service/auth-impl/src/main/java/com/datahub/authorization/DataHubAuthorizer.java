package com.datahub.authorization;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.group.GroupService;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.datahub.plugins.auth.authorization.ResourceSpecCachingAuthorizer;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.policy.DataHubPolicyInfo;
import io.datahubproject.metadata.context.ActorContext;
import io.datahubproject.metadata.context.AuthorizationContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextAuthorizer;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
public class DataHubAuthorizer
    implements Authorizer, ResourceSpecCachingAuthorizer, OperationContextAuthorizer {

  public enum AuthorizationMode {
    /** Default mode simply means that authorization is enforced, with a DENY result returned */
    DEFAULT,
    /**
     * Allow all means that the DataHubAuthorizer will allow all actions. This is used as an
     * override to disable the policies feature.
     */
    ALLOW_ALL
  }

  // Policies indexed by privilege name. Each refresh replaces the whole snapshot, so readers need
  // no lock and never see a partly built index.
  private volatile PolicySnapshot snapshot = PolicySnapshot.EMPTY;

  private final ScheduledExecutorService refreshExecutorService =
      Executors.newScheduledThreadPool(1);
  private final PolicyRefreshRunnable policyRefreshRunnable;

  // Set when policies load from SQL (POLICY_CACHE_SQL_SOURCE_ENABLED); null keeps the search-based
  // refresh. See requestLoad() for how loads are ordered and joined.
  @Nullable private final PolicyLoader policyLoader;
  private final Object loadQueueLock = new Object();
  @Nullable private CompletableFuture<Void> queuedLoad;
  private final PolicyEngine policyEngine;
  private final GroupService groupService;
  private EntitySpecResolver entitySpecResolver;
  private AuthorizationMode mode;
  @Getter private final OperationContext systemOpContext;

  public static final String ALL = "ALL";
  // Total time initialLoad waits, for all attempts together.
  private static final long INITIAL_LOAD_TIMEOUT_SECONDS = 60;
  // Until the first load succeeds, a failed load is retried after this delay, not after the
  // refresh interval: the cache is empty until then.
  private long firstLoadRetryMillis = TimeUnit.SECONDS.toMillis(10);
  private volatile boolean loadedOnce;

  public DataHubAuthorizer(
      @Nonnull final OperationContext systemOpContext,
      final EntityClient entityClient,
      @Nonnull final GroupService groupService,
      final int delayIntervalSeconds,
      final int refreshIntervalSeconds,
      final AuthorizationMode mode,
      final int policyFetchSize) {
    this(
        systemOpContext,
        entityClient,
        groupService,
        delayIntervalSeconds,
        refreshIntervalSeconds,
        mode,
        policyFetchSize,
        null);
  }

  /**
   * @param policyLoader when not null, policies load from the primary store with this loader
   *     instead of the search index. The caller should then call {@link #initialLoad} before the
   *     authorizer serves requests.
   */
  public DataHubAuthorizer(
      @Nonnull final OperationContext systemOpContext,
      final EntityClient entityClient,
      @Nonnull final GroupService groupService,
      final int delayIntervalSeconds,
      final int refreshIntervalSeconds,
      final AuthorizationMode mode,
      final int policyFetchSize,
      @Nullable final PolicyLoader policyLoader) {
    this.systemOpContext = systemOpContext;
    this.mode = Objects.requireNonNull(mode);
    this.groupService = Objects.requireNonNull(groupService);
    this.policyLoader = policyLoader;
    policyEngine = new PolicyEngine(entityClient, groupService);
    if (policyLoader != null) {
      policyRefreshRunnable = null;
      // The first load is synchronous (initialLoad), so the timer starts one interval later.
      // An interval <= 0 only turns off the timer; requested loads still run.
      if (refreshIntervalSeconds > 0) {
        refreshExecutorService.scheduleAtFixedRate(
            this::requestLoad, refreshIntervalSeconds, refreshIntervalSeconds, TimeUnit.SECONDS);
      }
    } else if (refreshIntervalSeconds > 0) {
      policyRefreshRunnable =
          new PolicyRefreshRunnable(
              systemOpContext, new PolicyFetcher(entityClient), this::publish, policyFetchSize);
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
    return authorize(request, null);
  }

  @Override
  public AuthorizationResult authorize(
      @Nonnull final AuthorizationRequest request,
      @Nullable final Map<EntitySpec, ResolvedEntitySpec> resourceSpecCache) {
    return authorize(request, resourceSpecCache, systemOpContext);
  }

  @Override
  public AuthorizationResult authorize(
      @Nonnull final AuthorizationRequest request,
      @Nullable final Map<EntitySpec, ResolvedEntitySpec> resourceSpecCache,
      @Nonnull final OperationContext opContext) {

    // 0. Short circuit: If the action is being performed by the system (root), always allow it.
    if (isSystemRequest(request, systemOpContext.getAuthentication())) {
      return new AuthorizationResult(request, AuthorizationResult.Type.ALLOW, null);
    }

    final Map<EntitySpec, Map<EntityFieldType, FieldResolver>> fieldOverrides =
        requestFieldOverrides(resourceSpecCache, opContext);

    Optional<ResolvedEntitySpec> resolvedResourceSpec =
        request
            .getResourceSpec()
            .map(spec -> resolveWithCache(spec, resourceSpecCache, fieldOverrides));

    List<ResolvedEntitySpec> resolvedSubResources =
        request.getSubResources().stream()
            .map(spec -> resolveWithCache(spec, resourceSpecCache, fieldOverrides))
            .collect(Collectors.toList());

    // 1. Fetch the policies relevant to the requested privilege.
    final List<DataHubPolicyInfo> policiesToEvaluate = new LinkedList<>();
    if (request.getActorPoliciesByPrivilege() != null) {
      for (final RecordTemplate policy :
          request
              .getActorPoliciesByPrivilege()
              .getOrDefault(request.getPrivilege(), Collections.emptyList())) {
        policiesToEvaluate.add((DataHubPolicyInfo) policy);
      }
    } else {
      policiesToEvaluate.addAll(getOrDefault(request.getPrivilege(), new ArrayList<>()));
    }
    for (final DataHubPolicyInfo defaultPolicy :
        PoliciesConfig.getDefaultPolicies(UrnUtils.getUrn(request.getActorUrn()))) {
      if (policiesToEvaluate.stream().noneMatch(existing -> existing == defaultPolicy)) {
        policiesToEvaluate.add(defaultPolicy);
      }
    }

    Optional<Urn> actorUrn = getUrnFromRequestActor(request.getActorUrn());
    if (actorUrn.isEmpty()) {
      return new AuthorizationResult(request, AuthorizationResult.Type.DENY, null);
    }

    final ResolvedEntitySpec resolvedActorSpec =
        resolveActorEntitySpec(
            new EntitySpec(actorUrn.get().getEntityType(), request.getActorUrn()), opContext);

    final PolicyEngine.PolicyEvaluationContext sharedEvaluationContext =
        policyEngine.createSeededEvaluationContext(
            request.getSessionActorIdentity(),
            request.getActorGroupMembership(),
            request.getActorDirectRoles(),
            opContext);

    // 2. Evaluate each policy.
    for (DataHubPolicyInfo policy : policiesToEvaluate) {
      if (isRequestGranted(
          policy,
          request,
          resolvedActorSpec,
          resolvedResourceSpec,
          resolvedSubResources,
          sharedEvaluationContext,
          opContext)) {
        // Short circuit if policy has granted privileges to this actor.
        return new AuthorizationResult(
            request,
            AuthorizationResult.Type.ALLOW,
            String.format("Granted by policy with type: %s", policy.getType()));
      }
    }
    return new AuthorizationResult(request, AuthorizationResult.Type.DENY, null);
  }

  /**
   * Resolve a spec, sharing the result through the request-scoped cache when present so the same
   * resource is resolved (and its domain/container/owner hierarchy walked) at most once per
   * request. Request field overrides are laid over the resolved spec on every read; the cache keeps
   * the plain resolution, so it does not matter who populated it first.
   */
  private ResolvedEntitySpec resolveWithCache(
      final EntitySpec spec,
      @Nullable final Map<EntitySpec, ResolvedEntitySpec> cache,
      @Nonnull final Map<EntitySpec, Map<EntityFieldType, FieldResolver>> fieldOverrides) {
    final ResolvedEntitySpec resolved =
        cache == null
            ? entitySpecResolver.resolve(spec)
            : cache.computeIfAbsent(spec, entitySpecResolver::resolve);
    final Map<EntityFieldType, FieldResolver> overrides = fieldOverrides.get(spec);
    if (overrides == null) {
      return resolved;
    }
    // Only the overridden fields change; every other field keeps its lazy stored-value resolver.
    final Map<EntityFieldType, FieldResolver> merged = new EnumMap<>(EntityFieldType.class);
    merged.putAll(resolved.getFieldResolvers());
    merged.putAll(overrides);
    return new ResolvedEntitySpec(spec, merged);
  }

  /**
   * Overrides belong to the {@link AuthorizationContext} that owns the request cache. Apply them
   * only when the caller passed that same cache, so overrides recorded on one context never leak
   * into a cache from another (e.g. the 2-arg path, which runs with the system context).
   */
  @Nonnull
  private static Map<EntitySpec, Map<EntityFieldType, FieldResolver>> requestFieldOverrides(
      @Nullable final Map<EntitySpec, ResolvedEntitySpec> resourceSpecCache,
      @Nonnull final OperationContext opContext) {
    final AuthorizationContext authorizationContext = opContext.getAuthorizationContext();
    if (resourceSpecCache == null
        || resourceSpecCache != authorizationContext.getSessionResourceSpecCache()) {
      return Map.of();
    }
    return authorizationContext.getSessionResourceFieldOverrides();
  }

  public PolicyEngine.PolicyGrantedPrivileges getGrantedPrivileges(
      final String actor, final Optional<EntitySpec> resourceSpec) {
    return getGrantedPrivileges(actor, resourceSpec, systemOpContext);
  }

  public PolicyEngine.PolicyGrantedPrivileges getGrantedPrivileges(
      final String actor,
      final Optional<EntitySpec> resourceSpec,
      @Nonnull final OperationContext opContext) {

    Urn actorUrn = UrnUtils.getUrn(actor);

    // 1. Fetch all policies
    final List<DataHubPolicyInfo> policiesToEvaluate =
        new LinkedList<>(getOrDefault(ALL, new ArrayList<>()));
    policiesToEvaluate.addAll(PoliciesConfig.getDefaultPolicies(actorUrn));

    final ResolvedEntitySpec resolvedActorSpec =
        resolveActorEntitySpec(new EntitySpec(actorUrn.getEntityType(), actor), opContext);

    Optional<ResolvedEntitySpec> resolvedResourceSpec =
        resourceSpec.map(entitySpecResolver::resolve);

    final PolicyEngine.PolicyEvaluationContext evaluationContext =
        buildActorEvaluationContext(opContext, actorUrn, null, null, null);

    return policyEngine.getGrantedPrivileges(
        opContext,
        policiesToEvaluate,
        resolvedActorSpec,
        resolvedResourceSpec,
        Collections.emptyList(),
        evaluationContext);
  }

  @Override
  public Set<DataHubPolicyInfo> getActorPolicies(@Nonnull Urn actorUrn) {
    return getActorPolicies(actorUrn, null);
  }

  @Override
  public Set<DataHubPolicyInfo> getActorPolicies(
      @Nonnull Urn actorUrn, @Nullable Collection<Urn> preloadedGroups) {
    return getActorPolicies(actorUrn, preloadedGroups, null);
  }

  @Override
  public Set<DataHubPolicyInfo> getActorPolicies(
      @Nonnull Urn actorUrn,
      @Nullable Collection<Urn> preloadedGroups,
      @Nullable Set<Urn> preloadedDirectRoles) {
    return getActorPolicies(actorUrn, preloadedGroups, preloadedDirectRoles, systemOpContext);
  }

  public Set<DataHubPolicyInfo> getActorPolicies(
      @Nonnull Urn actorUrn,
      @Nullable Collection<Urn> preloadedGroups,
      @Nullable Set<Urn> preloadedDirectRoles,
      @Nonnull OperationContext opContext) {
    return getActorPolicies(actorUrn, null, preloadedGroups, preloadedDirectRoles, opContext);
  }

  @Override
  public Set<DataHubPolicyInfo> getActorPolicies(
      @Nonnull Urn actorUrn,
      @Nullable SessionActorIdentity sessionActorIdentity,
      @Nullable Collection<Urn> preloadedGroups,
      @Nullable Set<Urn> preloadedDirectRoles,
      @Nonnull OperationContext opContext) {
    // 1. Fetch all policies
    final List<DataHubPolicyInfo> policiesToEvaluate =
        new LinkedList<>(getOrDefault(ALL, new ArrayList<>()));
    policiesToEvaluate.addAll(PoliciesConfig.getDefaultPolicies(actorUrn));

    // 2. Actor identity
    final ResolvedEntitySpec resolvedActorSpec =
        resolveActorEntitySpec(
            new EntitySpec(actorUrn.getEntityType(), actorUrn.toString()), opContext);

    final PolicyEngine.PolicyEvaluationContext sharedContext =
        buildActorEvaluationContext(
            opContext, actorUrn, sessionActorIdentity, preloadedGroups, preloadedDirectRoles);

    return policiesToEvaluate.stream()
        .filter(policy -> PoliciesConfig.ACTIVE_POLICY_STATE.equals(policy.getState()))
        .filter(
            policy ->
                (policy.getActors() != null && policy.getActors().isResourceOwners())
                    || policyEngine.isActorMatch(
                        opContext,
                        resolvedActorSpec,
                        policy.getActors(),
                        Optional.empty(),
                        sharedContext))
        .collect(Collectors.toSet());
  }

  @Override
  public Optional<SessionActorIdentity> resolveSessionActorIdentity(@Nonnull Urn actorUrn) {
    return resolveSessionActorIdentity(actorUrn, systemOpContext);
  }

  @Override
  public Optional<SessionActorIdentity> resolveSessionActorIdentity(
      @Nonnull Urn actorUrn, @Nonnull OperationContext opContext) {
    return Optional.of(groupService.fetchUserIdentity(opContext, actorUrn));
  }

  @Override
  public Collection<Urn> getActorGroups(@Nonnull Urn actorUrn) {
    return getActorGroups(actorUrn, systemOpContext);
  }

  public Collection<Urn> getActorGroups(
      @Nonnull Urn actorUrn, @Nonnull OperationContext opContext) {
    return groupService.getGroupsForUser(opContext, actorUrn);
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
    if (policyLoader != null) {
      requestLoad();
    } else if (policyRefreshRunnable != null) {
      refreshExecutorService.execute(policyRefreshRunnable);
    }
  }

  public boolean isSqlSourceEnabled() {
    return policyLoader != null;
  }

  /**
   * Requests a full load of all policies from the primary store, and returns a future that
   * completes when a load that started after this call has replaced the cached policies.
   *
   * <p>All loads run on the single refresh thread, so they finish in the order they start, and a
   * load that starts later reads newer committed data. That ordering is what keeps the cache
   * correct: no per-policy versions or merges are needed. A request joins a load that is queued but
   * not yet started, because that load will still see everything committed before the request. A
   * load that is already running may have started too early, so a request then queues a new one.
   */
  public CompletableFuture<Void> requestLoad() {
    if (policyLoader == null) {
      return CompletableFuture.completedFuture(null);
    }
    increment("policyCache.load.requested");
    synchronized (loadQueueLock) {
      if (queuedLoad == null) {
        final CompletableFuture<Void> load = new CompletableFuture<>();
        queuedLoad = load;
        try {
          refreshExecutorService.execute(() -> runLoad(load));
        } catch (RejectedExecutionException e) {
          queuedLoad = null;
          load.completeExceptionally(e);
          return load.copy();
        }
      }
      // A copy, so callers cannot complete the shared future.
      return queuedLoad.copy();
    }
  }

  private void runLoad(@Nonnull CompletableFuture<Void> load) {
    synchronized (loadQueueLock) {
      if (queuedLoad == load) {
        // From now on, new requests must queue another load.
        queuedLoad = null;
      }
    }
    final long start = System.nanoTime();
    try {
      final PolicySnapshot next = PolicySnapshot.of(policyLoader.loadAll());
      publish(next);
      loadedOnce = true;
      increment("policyCache.load.run");
      systemOpContext
          .getMetricUtils()
          .ifPresent(
              m -> {
                m.time(
                    DataHubAuthorizer.class.getName() + ".policyCache.load.duration",
                    System.nanoTime() - start);
                m.setGaugeValue(DataHubAuthorizer.class, "policyCache.policies", next.size());
              });
      load.complete(null);
    } catch (Throwable t) {
      increment("policyCache.load.failed");
      log.error("Failed to load policies. Keeping the current policies.", t);
      load.completeExceptionally(t);
      if (!loadedOnce) {
        scheduleFirstLoadRetry();
      }
    }
  }

  private void scheduleFirstLoadRetry() {
    try {
      refreshExecutorService.schedule(
          () -> {
            if (!loadedOnce) {
              requestLoad();
            }
          },
          firstLoadRetryMillis,
          TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException e) {
      // Shutting down.
    }
  }

  /**
   * Loads the policies synchronously, trying up to {@code attempts} times within 60 s in total.
   * Call this once at startup so the authorizer never serves requests with an empty cache.
   *
   * @return true if a load succeeded; false if every attempt failed, in which case a load is
   *     retried every 10 s until one succeeds.
   */
  public boolean initialLoad(int attempts, long backoffMillis) {
    if (policyLoader == null) {
      return false;
    }
    log.info("policy cache source=sql, loading policies");
    final long start = System.nanoTime();
    final long deadline = start + TimeUnit.SECONDS.toNanos(INITIAL_LOAD_TIMEOUT_SECONDS);
    for (int attempt = 1; attempt <= attempts; attempt++) {
      final long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        break;
      }
      try {
        // A timed-out load keeps the single load thread busy, so later attempts would queue
        // behind it: all attempts share one deadline.
        requestLoad().get(remaining, TimeUnit.NANOSECONDS);
        log.info(
            "policy cache loaded, policies={}, load={} ms",
            snapshot.size(),
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        return true;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        log.warn("Initial policy load attempt {} of {} failed.", attempt, attempts, e);
        if (attempt < attempts && backoffMillis > 0) {
          try {
            Thread.sleep(backoffMillis);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    }
    log.error(
        "Could not load policies at startup. Continuing with no policies until a later load"
            + " succeeds.");
    return false;
  }

  @VisibleForTesting
  void setFirstLoadRetryMillis(long millis) {
    this.firstLoadRetryMillis = millis;
  }

  @VisibleForTesting
  PolicySnapshot currentSnapshot() {
    return snapshot;
  }

  private void increment(@Nonnull String metricName) {
    systemOpContext
        .getMetricUtils()
        .ifPresent(m -> m.increment(DataHubAuthorizer.class, metricName, 1));
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
      final ResolvedEntitySpec resolvedActorSpec,
      final Optional<ResolvedEntitySpec> resourceSpec,
      final List<ResolvedEntitySpec> subResources,
      final PolicyEngine.PolicyEvaluationContext sharedEvaluationContext,
      @Nonnull final OperationContext opContext) {
    if (AuthorizationMode.ALLOW_ALL.equals(mode())) {
      return true;
    }

    try {
      final PolicyEngine.PolicyEvaluationResult result =
          policyEngine.evaluatePolicy(
              opContext,
              policy,
              resolvedActorSpec,
              request.getPrivilege(),
              resourceSpec,
              subResources,
              sharedEvaluationContext);
      return result.isGranted();
    } catch (RuntimeException e) {
      // Log the error but don't fail the entire authorization chain.
      // Continue to evaluate the next policy instead of blocking authorization.
      log.warn(
          "Error evaluating policy {} for request {}, skipping this policy and continuing to next",
          policy.getDisplayName(),
          request,
          e);
      return false;
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

  /**
   * Resolves actor-scoped entity fields (group membership, roles) using the session context.
   * Resource specs continue to use the system context via {@link EntitySpecResolver#resolve}.
   */
  @Nonnull
  private ResolvedEntitySpec resolveActorEntitySpec(
      @Nonnull final EntitySpec entitySpec, @Nonnull final OperationContext opContext) {
    if (entitySpecResolver instanceof ContextualEntitySpecResolver contextualResolver) {
      return contextualResolver.resolve(entitySpec, opContext);
    }
    return entitySpecResolver.resolve(entitySpec);
  }

  /**
   * Builds a policy evaluation context seeded with the actor's identity. Uses request-scoped
   * session data when the actor is the session user; otherwise resolves identity via the session
   * context.
   */
  @Nonnull
  private PolicyEngine.PolicyEvaluationContext buildActorEvaluationContext(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn actorUrn,
      @Nullable final SessionActorIdentity sessionActorIdentity,
      @Nullable final Collection<Urn> preloadedGroups,
      @Nullable final Set<Urn> preloadedDirectRoles) {
    if (sessionActorIdentity != null || preloadedGroups != null || preloadedDirectRoles != null) {
      return policyEngine.createSeededEvaluationContext(
          sessionActorIdentity, preloadedGroups, preloadedDirectRoles, opContext);
    }

    final ActorContext sessionActor = opContext.getSessionActorContext();
    if (actorUrn.equals(sessionActor.getActorUrn())) {
      return policyEngine.createSeededEvaluationContext(
          opContext.getAuthorizationContext().getSessionActorIdentity(actorUrn),
          sessionActor.getGroupMembership(),
          sessionActor.getDirectRoleMembership(),
          opContext);
    }

    return resolveSessionActorIdentity(actorUrn, opContext)
        .map(
            identity ->
                policyEngine.createSeededEvaluationContext(
                    identity, identity.getGroups(), identity.getDirectRoles(), opContext))
        .orElse(new PolicyEngine.PolicyEvaluationContext());
  }

  private List<DataHubPolicyInfo> getOrDefault(String key, List<DataHubPolicyInfo> defaultValue) {
    return snapshot.getOrDefault(key, defaultValue);
  }

  private void publish(@Nonnull PolicySnapshot next) {
    snapshot = next;
  }

  /** Replaces all cached policies at once. For tests that need a fixed set of policies. */
  @VisibleForTesting
  protected void replacePolicies(@Nonnull Collection<DataHubPolicyInfo> policies) {
    publish(PolicySnapshot.ofPolicyInfos(policies));
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
    private final Consumer<PolicySnapshot> publisher;
    private final int count;

    @Override
    public void run() {
      try {
        // Fetch every page first, then publish one complete snapshot.
        final List<PolicyFetcher.Policy> policies = new ArrayList<>();
        Integer total = null;
        String scrollId = null;

        while (total == null || scrollId != null) {
          try {
            final PolicyFetcher.PolicyFetchResult policyFetchResult =
                policyFetcher.fetchPolicies(systemOpContext, count, scrollId, null);

            policies.addAll(policyFetchResult.getPolicies());

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

        publisher.accept(PolicySnapshot.of(policies));

        log.debug("Successfully fetched {} policies.", total);
      } catch (Exception e) {
        log.error(
            "Caught exception while loading Policy cache. Will retry on next scheduled attempt.",
            e);
      }
    }
  }
}
