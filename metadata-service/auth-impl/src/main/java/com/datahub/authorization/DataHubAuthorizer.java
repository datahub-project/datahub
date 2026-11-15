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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
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

  // The refresh timer. With a policy loader, it only queues loads, so a slow load cannot stop it.
  private final ScheduledExecutorService refreshExecutorService =
      Executors.newScheduledThreadPool(1);
  private final PolicyRefreshRunnable policyRefreshRunnable;

  // Set with POLICY_CACHE_IMMEDIATE_UPDATE_ENABLED; null keeps the search-based refresh. See
  // enqueue() for how loads are ordered and joined.
  @Nullable private final PolicyLoader policyLoader;
  // Runs every load and update, one at a time, in the order they were queued.
  @Nullable private final ExecutorService loadExecutor;
  private final Object loadQueueLock = new Object();
  @Nullable private LoadJob queuedJob;
  // When the running job started (System.nanoTime), or 0 when no job runs.
  private volatile long jobStartedNanos;
  private final int refreshIntervalSeconds;

  // Only read and written on the load thread.
  private Map<Urn, PolicyFetcher.Policy> policiesByUrn = new HashMap<>();
  // Policies changed recently, with the System.nanoTime at which each entry expires. A full load
  // reads these by key too, because the search index may not list a new policy yet.
  private final Map<Urn, Long> recentChanges = new HashMap<>();
  private volatile long recentChangeWindowNanos = TimeUnit.MINUTES.toNanos(10);
  private volatile long policyChangeWaitMillis = DEFAULT_POLICY_CHANGE_WAIT_MILLIS;
  // True when this process's entity service calls onPolicyChange after each committed policy write.
  private volatile boolean policyWritesObserved;
  private final PolicyEngine policyEngine;
  private final GroupService groupService;
  private EntitySpecResolver entitySpecResolver;
  private AuthorizationMode mode;
  @Getter private final OperationContext systemOpContext;

  public static final String ALL = "ALL";
  // Total time initialLoad waits, for all attempts together.
  private static final long INITIAL_LOAD_TIMEOUT_SECONDS = 60;
  private static final long DEFAULT_POLICY_CHANGE_WAIT_MILLIS = 5000;
  // Until the first load succeeds, a failed load is retried after this delay, not after the
  // refresh interval: the cache is empty until then.
  private long firstLoadRetryMillis = TimeUnit.SECONDS.toMillis(10);
  private volatile boolean loadedOnce;
  // At most one retry is scheduled at a time, so failed loads do not start parallel retry chains.
  private final AtomicBoolean firstLoadRetryScheduled = new AtomicBoolean();
  // Consecutive failed loads: only the first one logs a stack trace.
  private int consecutiveLoadFailures;

  /**
   * @param delayIntervalSeconds delay before the first search-based refresh; not used with a {@code
   *     policyLoader}, which loads first in {@link #initialLoad}
   * @param refreshIntervalSeconds interval of the refresh timer; 0 or less turns the timer off
   * @param policyFetchSize page size of the search-based refresh; not used with a {@code
   *     policyLoader}, which has its own page size
   * @param policyLoader when not null, the cache loads with this loader and also updates single
   *     policies by URN ({@link #requestUpdate}). The caller should then call {@link #initialLoad}
   *     before the authorizer serves requests.
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
    this.refreshIntervalSeconds = refreshIntervalSeconds;
    policyEngine = new PolicyEngine(entityClient, groupService);
    if (policyLoader != null) {
      policyRefreshRunnable = null;
      loadExecutor =
          Executors.newSingleThreadExecutor(
              r -> {
                final Thread t = new Thread(r, "policy-cache-load");
                t.setDaemon(true);
                return t;
              });
      // The first load is synchronous (initialLoad), so the timer starts one interval later.
      // An interval <= 0 only turns off the timer; requested loads still run.
      if (refreshIntervalSeconds > 0) {
        refreshExecutorService.scheduleAtFixedRate(
            this::onRefreshTimer, refreshIntervalSeconds, refreshIntervalSeconds, TimeUnit.SECONDS);
      }
    } else if (refreshIntervalSeconds > 0) {
      loadExecutor = null;
      policyRefreshRunnable =
          new PolicyRefreshRunnable(
              systemOpContext, new PolicyFetcher(entityClient), this::publish, policyFetchSize);
      refreshExecutorService.scheduleAtFixedRate(
          policyRefreshRunnable, delayIntervalSeconds, refreshIntervalSeconds, TimeUnit.SECONDS);
    } else {
      policyRefreshRunnable = null;
      loadExecutor = null;
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
      // With the observer, the write already requested a load after its commit (and waited for
      // it, unless the wait is 0). Another load here would only read the same policies again.
      if (!policyWritesObserved) {
        requestLoad();
      }
    } else if (policyRefreshRunnable != null) {
      refreshExecutorService.execute(policyRefreshRunnable);
    }
  }

  public boolean isImmediateUpdateEnabled() {
    return policyLoader != null;
  }

  /**
   * How long a changed policy stays on the recent-changes list. Must be longer than the normal
   * search index delay: until then, a full load reads the policy by key even if the index does not
   * list it yet.
   */
  public void setRecentChangeWindow(@Nonnull Duration window) {
    this.recentChangeWindowNanos = window.toNanos();
  }

  /**
   * Requests a full load of all policies, and returns a future that completes when a load that
   * started after this call has replaced the cached policies.
   */
  public CompletableFuture<Void> requestLoad() {
    return enqueue(true, Set.of());
  }

  /**
   * Requests a read of the given policies by primary key, and returns a future that completes when
   * the cache holds their state from a read that started after this call. A policy that no longer
   * exists is removed from the cache.
   */
  public CompletableFuture<Void> requestUpdate(@Nonnull Collection<Urn> policyUrns) {
    if (policyUrns.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return enqueue(false, policyUrns);
  }

  /**
   * All jobs run on the single load thread, so they finish in the order they start, and a job that
   * starts later reads newer committed data. That ordering is what keeps the cache correct. A
   * request joins a job that is queued but not yet started, because that job will still read
   * everything committed before the request: their URN sets merge, and a full load also reads the
   * waiting URNs. A job that is already running may have started too early, so a request then
   * queues a new one.
   */
  private CompletableFuture<Void> enqueue(boolean full, @Nonnull Collection<Urn> urns) {
    if (policyLoader == null || loadExecutor == null) {
      return CompletableFuture.completedFuture(null);
    }
    increment(full ? "policyCache.load.requested" : "policyCache.update.requested");
    synchronized (loadQueueLock) {
      if (queuedJob == null) {
        final LoadJob job = new LoadJob();
        queuedJob = job;
        try {
          loadExecutor.execute(() -> runJob(job));
        } catch (RejectedExecutionException e) {
          queuedJob = null;
          job.done.completeExceptionally(e);
          return job.done.copy();
        }
      }
      queuedJob.full |= full;
      queuedJob.urns.addAll(urns);
      // A copy, so callers cannot complete the shared future.
      return queuedJob.done.copy();
    }
  }

  private static final class LoadJob {
    boolean full;
    final Set<Urn> urns = new HashSet<>();
    final CompletableFuture<Void> done = new CompletableFuture<>();
  }

  private void runJob(@Nonnull LoadJob job) {
    synchronized (loadQueueLock) {
      if (queuedJob == job) {
        // From now on, new requests must queue another job.
        queuedJob = null;
      }
    }
    // Until the first full load succeeds, the cache holds no policies, so a key read alone would
    // publish a partial cache: load everything instead.
    final boolean full = job.full || !loadedOnce;
    final String kind = full ? "load" : "update";
    final long start = System.nanoTime();
    jobStartedNanos = start;
    try {
      // Recorded before the read, so a failed read still makes the next full load read them.
      final long expires = start + recentChangeWindowNanos;
      for (Urn urn : job.urns) {
        recentChanges.put(urn, expires);
      }
      recentChanges.values().removeIf(expiry -> expiry - start <= 0);

      final Map<Urn, PolicyFetcher.Policy> next =
          full ? loadAllWithRecentChanges() : updateByUrn(job.urns);
      final PolicySnapshot snapshot = PolicySnapshot.of(next.values());
      publish(snapshot);
      policiesByUrn = next;
      if (full) {
        loadedOnce = true;
      }
      consecutiveLoadFailures = 0;
      increment("policyCache." + kind + ".run");
      systemOpContext
          .getMetricUtils()
          .ifPresent(
              m -> {
                m.time(
                    DataHubAuthorizer.class.getName() + ".policyCache." + kind + ".duration",
                    System.nanoTime() - start);
                m.setGaugeValue(DataHubAuthorizer.class, "policyCache.policies", snapshot.size());
              });
      job.done.complete(null);
    } catch (Throwable t) {
      increment("policyCache." + kind + ".failed");
      // Jobs run on one thread, so this counter needs no lock. A failure that repeats every few
      // seconds logs its stack trace only once.
      if (consecutiveLoadFailures++ == 0) {
        log.error("Failed to {} policies. Keeping the current policies.", kind, t);
      } else {
        log.error(
            "Failed to {} policies ({} failures in a row). Keeping the current policies: {}",
            kind,
            consecutiveLoadFailures,
            t.toString());
      }
      job.done.completeExceptionally(t);
      if (!loadedOnce) {
        scheduleFirstLoadRetry();
      }
    } finally {
      jobStartedNanos = 0;
    }
  }

  /** Reads every policy, plus the recently changed ones that the search index may not list yet. */
  private Map<Urn, PolicyFetcher.Policy> loadAllWithRecentChanges() {
    final Map<Urn, PolicyFetcher.Policy> next = new HashMap<>();
    for (PolicyFetcher.Policy policy : policyLoader.loadAll()) {
      if (policy.getUrn() != null) {
        next.put(policy.getUrn(), policy);
      }
    }
    // Policies the index lists are read from the primary store already. A deleted policy that the
    // index still lists has no stored value, so the loader leaves it out.
    final Set<Urn> missing = new HashSet<>(recentChanges.keySet());
    missing.removeAll(next.keySet());
    if (!missing.isEmpty()) {
      next.putAll(policyLoader.loadByUrns(missing));
    }
    return next;
  }

  private Map<Urn, PolicyFetcher.Policy> updateByUrn(@Nonnull Set<Urn> urns) {
    final Map<Urn, PolicyFetcher.Policy> read = policyLoader.loadByUrns(urns);
    final Map<Urn, PolicyFetcher.Policy> next = new HashMap<>(policiesByUrn);
    for (Urn urn : urns) {
      final PolicyFetcher.Policy policy = read.get(urn);
      if (policy != null) {
        next.put(urn, policy);
      } else {
        next.remove(urn);
      }
    }
    return next;
  }

  private void onRefreshTimer() {
    // The timer must never throw: a scheduled task that throws is not run again.
    try {
      final long started = jobStartedNanos;
      if (started != 0
          && System.nanoTime() - started > TimeUnit.SECONDS.toNanos(refreshIntervalSeconds)) {
        // A blocking read cannot be safely stopped, so report it. Later requests queue behind it.
        increment("policyCache.load.stuck");
        log.warn(
            "A policy load has run for {} s; later policy loads wait for it.",
            TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - started));
      }
      requestLoad();
    } catch (Throwable t) {
      log.error("Policy refresh timer failed; it runs again on the next interval.", t);
    }
  }

  private void scheduleFirstLoadRetry() {
    if (!firstLoadRetryScheduled.compareAndSet(false, true)) {
      return;
    }
    try {
      refreshExecutorService.schedule(
          () -> {
            firstLoadRetryScheduled.set(false);
            if (!loadedOnce) {
              requestLoad();
            }
          },
          firstLoadRetryMillis,
          TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException e) {
      // Shutting down.
      firstLoadRetryScheduled.set(false);
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
    log.info("policy cache immediate updates on, loading policies");
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
        // The load thread already logged the stack trace.
        log.warn(
            "Initial policy load attempt {} of {} failed: {}",
            attempt,
            attempts,
            String.valueOf(e.getCause() != null ? e.getCause() : e));
        if (attempt < attempts && backoffMillis > 0) {
          try {
            // Never sleep past the shared deadline.
            Thread.sleep(
                Math.min(
                    backoffMillis,
                    Math.max(0, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()))));
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

  /**
   * Called after a policy write or delete has been committed in this process. Requests a load and
   * waits for it, up to {@link #setPolicyChangeWaitMillis}, so the write's response goes out only
   * after this process enforces the change. If the limit is reached, the load still completes
   * later. A limit of 0 requests the load without waiting; if that load fails, it is not retried
   * here, and the next requested load or the refresh timer applies the change. Never throws: the
   * write it follows is already committed.
   */
  public void onPolicyChange() {
    if (policyLoader == null) {
      return;
    }
    if (policyChangeWaitMillis == 0) {
      // Waiting is turned off: the load runs in the background, and that is not a timeout. A zero
      // wait sample keeps the observer.wait count equal to the number of policy changes.
      requestLoad();
      recordObserverWait(0);
      return;
    }
    final long start = System.nanoTime();
    try {
      requestLoad().get(policyChangeWaitMillis, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      increment("policyCache.observer.timeout");
      log.warn(
          "Policy load did not finish within {} ms after a policy change; it continues in the"
              + " background.",
          policyChangeWaitMillis);
    } catch (InterruptedException e) {
      // Stop waiting, but do not keep the interrupt: the write is already committed, and the
      // interrupt would make the change log send that follows on this thread fail. The load
      // continues in the background.
      increment("policyCache.observer.interrupted");
      log.warn("Interrupted while waiting for the policy load after a policy change.");
    } catch (ExecutionException e) {
      increment("policyCache.observer.failed");
      log.warn("Policy load after a policy change failed; requesting one more.", e.getCause());
      // One retry, without waiting. Otherwise this process keeps the old policies until the timer.
      requestLoad();
    } finally {
      recordObserverWait(System.nanoTime() - start);
    }
  }

  private void recordObserverWait(long nanos) {
    systemOpContext
        .getMetricUtils()
        .ifPresent(
            m -> m.time(DataHubAuthorizer.class.getName() + ".policyCache.observer.wait", nanos));
  }

  public void setPolicyChangeWaitMillis(long waitMillis) {
    this.policyChangeWaitMillis = Math.max(0, waitMillis);
  }

  /**
   * Call after this process's entity service is set to call {@link #onPolicyChange} for each
   * committed policy write. {@link #invalidateCache} then has nothing left to do.
   */
  public void setPolicyWritesObserved(boolean observed) {
    this.policyWritesObserved = observed;
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
    increment(metricName, 1);
  }

  private void increment(@Nonnull String metricName, double count) {
    systemOpContext
        .getMetricUtils()
        .ifPresent(m -> m.increment(DataHubAuthorizer.class, metricName, count));
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
