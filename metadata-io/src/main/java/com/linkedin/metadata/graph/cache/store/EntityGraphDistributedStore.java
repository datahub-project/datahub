package com.linkedin.metadata.graph.cache.store;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.FullWalkEdge;
import com.linkedin.metadata.graph.cache.FullWalkPublishResult;
import com.linkedin.metadata.graph.cache.FullWalkWriteBack;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphRegistry;
import com.linkedin.metadata.graph.cache.service.freshness.SnapshotFreshnessEvaluator;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphEndpoints;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot.DirectedEdge;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotEditor;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotEditor.FullWalkEdit;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphView;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage.DirectionCoverage;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EntityGraphDistributedStore {

  /** Topology fingerprint for failure tombstones (empty edge set). */
  static final String TOMBSTONE_FINGERPRINT = "tombstone";

  private final EntityGraphRegistry registry;
  @Nullable private final IMap<String, EntityGraphSnapshot> fullSnapshots;
  private final Map<String, IMap<String, EntityGraphSnapshot>> partialSnapshotsByGraphId;
  private final IMap<String, EntityGraphOperationalStatus> status;
  private final IMap<String, Long> invalidationGenerations;
  private final PartialSeedIndex partialSeedIndex = new PartialSeedIndex();

  public EntityGraphDistributedStore(
      @Nonnull HazelcastInstance hazelcastInstance,
      @Nonnull EntityGraphRegistry registry,
      @Nonnull Consumer<String> onSnapshotUpdated) {
    this.registry = registry;
    if (registry.hasFullScopeGraphs()) {
      this.fullSnapshots = hazelcastInstance.getMap(EntityGraphCacheProperties.FULL_SNAPSHOTS_MAP);
      registerSnapshotListeners(fullSnapshots, onSnapshotUpdated);
    } else {
      this.fullSnapshots = null;
    }
    this.partialSnapshotsByGraphId = new HashMap<>();
    for (String graphId : registry.getGraphsById().keySet()) {
      EntityGraphDefinition definition = registry.getDefinition(graphId);
      if (definition != null && definition.getScope().getMode() == ScopeMode.PARTIAL) {
        IMap<String, EntityGraphSnapshot> partialMap =
            hazelcastInstance.getMap(EntityGraphCacheProperties.partialSnapshotsMapName(graphId));
        partialSnapshotsByGraphId.put(graphId, partialMap);
        registerSnapshotListeners(partialMap, onSnapshotUpdated);
      }
    }
    this.status = hazelcastInstance.getMap(EntityGraphCacheProperties.STATUS_MAP);
    this.invalidationGenerations =
        hazelcastInstance.getMap(EntityGraphCacheProperties.INVALIDATION_GENERATION_MAP);
  }

  private static void registerSnapshotListeners(
      @Nonnull IMap<String, EntityGraphSnapshot> snapshots,
      @Nonnull Consumer<String> onSnapshotUpdated) {
    SnapshotChangeListener listener = new SnapshotChangeListener(onSnapshotUpdated);
    snapshots.addLocalEntryListener(listener);
  }

  private static final class SnapshotChangeListener
      implements EntryAddedListener<String, EntityGraphSnapshot>,
          EntryUpdatedListener<String, EntityGraphSnapshot>,
          EntryRemovedListener<String, EntityGraphSnapshot> {

    private final Consumer<String> onSnapshotUpdated;

    private SnapshotChangeListener(@Nonnull Consumer<String> onSnapshotUpdated) {
      this.onSnapshotUpdated = onSnapshotUpdated;
    }

    @Override
    public void entryAdded(@Nonnull EntryEvent<String, EntityGraphSnapshot> event) {
      onSnapshotUpdated.accept(event.getKey());
    }

    @Override
    public void entryUpdated(@Nonnull EntryEvent<String, EntityGraphSnapshot> event) {
      onSnapshotUpdated.accept(event.getKey());
    }

    @Override
    public void entryRemoved(@Nonnull EntryEvent<String, EntityGraphSnapshot> event) {
      onSnapshotUpdated.accept(event.getKey());
    }
  }

  public void refreshPartialSeedIndex(@Nonnull String cacheKey) {
    EntityGraphSnapshot snapshot = getSnapshot(cacheKey);
    if (snapshot == null
        || snapshot.getEdges() == null
        || snapshot.getEdges().isEmpty()
        || CacheStatus.INVALID.name().equals(snapshot.getCacheStatus())) {
      partialSeedIndex.removeCacheKey(cacheKey);
      return;
    }
    EntityGraphDefinition definition = registry.getDefinition(snapshot.getGraphId());
    if (definition != null && definition.getScope().getMode() == ScopeMode.PARTIAL) {
      partialSeedIndex.indexSnapshot(snapshot);
    }
  }

  @Nullable
  public EntityGraphSnapshot getSnapshot(@Nonnull String cacheKey) {
    return snapshotsForKey(cacheKey).get(cacheKey);
  }

  /** Generation is embedded in the published snapshot (single source of truth). */
  public long getGeneration(@Nonnull String cacheKey) {
    EntityGraphSnapshot snapshot = getSnapshot(cacheKey);
    return snapshot == null ? 0L : snapshot.getGeneration();
  }

  /** Per-graph invalidation generation; incremented on drop or surgical delete. */
  public long getInvalidationGeneration(@Nonnull String graphId) {
    Long generation = invalidationGenerations.get(graphId);
    return generation == null ? 0L : generation;
  }

  private void recordInvalidationGeneration(@Nonnull String graphId) {
    invalidationGenerations.compute(graphId, (key, current) -> current == null ? 1L : current + 1L);
  }

  /**
   * Resolves cache status for a key: {@code entityGraphStatus} when present, else embedded {@code
   * cacheStatus} on the snapshot, else {@link CacheStatus#ABSENT} (cold miss — not {@link
   * CacheStatus#BUILDING}).
   */
  @Nonnull
  public CacheStatus getStatus(@Nonnull String cacheKey) {
    EntityGraphOperationalStatus operational = status.get(cacheKey);
    if (operational != null) {
      try {
        return operational.cacheStatus();
      } catch (IllegalArgumentException e) {
        log.warn(
            "Unknown entity graph cache status '{}' for key {}", operational.getStatus(), cacheKey);
      }
    }
    EntityGraphSnapshot snapshot = getSnapshot(cacheKey);
    if (snapshot != null && snapshot.getCacheStatus() != null) {
      try {
        return CacheStatus.valueOf(snapshot.getCacheStatus());
      } catch (IllegalArgumentException e) {
        log.warn(
            "Unknown entity graph cache status '{}' embedded in snapshot for key {}",
            snapshot.getCacheStatus(),
            cacheKey);
      }
    }
    return CacheStatus.ABSENT;
  }

  /**
   * Claims a distributed rebuild lease via atomic {@code status.compute}. Returns false when
   * another pod holds a non-stale {@link CacheStatus#BUILDING} lease.
   */
  public boolean tryClaimRebuild(@Nonnull String cacheKey, long staleBuildingMillis) {
    boolean[] claimed = {false};
    long now = System.currentTimeMillis();
    status.compute(
        cacheKey,
        (key, current) -> {
          if (current == null) {
            claimed[0] = true;
            return EntityGraphOperationalStatus.of(CacheStatus.BUILDING, now);
          }
          if (current.cacheStatus() == CacheStatus.BUILDING) {
            if (isStaleClaim(current, staleBuildingMillis)) {
              claimed[0] = true;
              return EntityGraphOperationalStatus.of(CacheStatus.BUILDING, now);
            }
            return current;
          }
          claimed[0] = true;
          return EntityGraphOperationalStatus.of(CacheStatus.BUILDING, now);
        });
    return claimed[0];
  }

  /** Drops a {@link CacheStatus#BUILDING} lease when a claimed rebuild does not publish. */
  public void releaseRebuildClaim(@Nonnull String cacheKey) {
    status.compute(
        cacheKey,
        (key, current) ->
            current != null && current.cacheStatus() == CacheStatus.BUILDING ? null : current);
  }

  /**
   * Publishes {@link CacheStatus#ACTIVE} in a single snapshot-map write ({@code cacheStatus}
   * embedded on the snapshot). Clears any {@code BUILDING} lease from {@code entityGraphStatus}.
   */
  public void publish(@Nonnull EntityGraphSnapshot snapshot, @Nonnull CacheStatus cacheStatus) {
    if (cacheStatus != CacheStatus.ACTIVE) {
      throw new IllegalArgumentException(
          "publish only supports ACTIVE snapshots; use markCooldown/markOverLimit/markInvalid for "
              + cacheStatus);
    }
    String key = snapshot.getCacheKey();
    String statusName = cacheStatus.name();
    List<EntityGraphSnapshot.DirectedEdge> edges =
        snapshot.getEdges() != null ? snapshot.getEdges() : List.of();
    snapshotsForKey(key)
        .compute(
            key,
            (ignored, existing) ->
                EntityGraphSnapshot.builder()
                    .graphId(snapshot.getGraphId())
                    .cacheKey(key)
                    .generation((existing == null ? 0L : existing.getGeneration()) + 1L)
                    .buildSource(snapshot.getBuildSource())
                    .builtAtMillis(snapshot.getBuiltAtMillis())
                    .edges(edges)
                    .vertexCount(snapshot.getVertexCount())
                    .edgeCount(snapshot.getEdgeCount())
                    .topologyFingerprint(snapshot.getTopologyFingerprint())
                    .traversalCoverage(snapshot.getTraversalCoverage())
                    .cacheStatus(statusName)
                    .build());
    status.remove(key);
    refreshPartialSeedIndex(key);
  }

  public void markCooldown(@Nonnull String cacheKey) {
    status.put(
        cacheKey,
        EntityGraphOperationalStatus.of(CacheStatus.COOLDOWN, System.currentTimeMillis()));
    writeFailureTombstoneIfSnapshotKey(cacheKey, CacheStatus.COOLDOWN);
  }

  public void markOverLimit(@Nonnull String cacheKey) {
    status.put(cacheKey, EntityGraphOperationalStatus.of(CacheStatus.OVER_LIMIT));
    writeFailureTombstoneIfSnapshotKey(cacheKey, CacheStatus.OVER_LIMIT);
  }

  public void markInvalid(@Nonnull String cacheKey) {
    status.put(cacheKey, EntityGraphOperationalStatus.of(CacheStatus.INVALID));
    writeFailureTombstoneIfSnapshotKey(cacheKey, CacheStatus.INVALID);
  }

  public boolean shouldSkipPublish(
      @Nonnull String cacheKey, @Nonnull EntityGraphSnapshot candidate) {
    EntityGraphSnapshot existing = getSnapshot(cacheKey);
    if (existing == null) {
      return false;
    }
    CacheStatus cacheStatus = getStatus(cacheKey);
    // A rebuild holds BUILDING on the same key it is about to publish. That lease must not let a
    // depth-capped snapshot replace a fresh trusted walk.
    if (cacheStatus != CacheStatus.ACTIVE && cacheStatus != CacheStatus.BUILDING) {
      return false;
    }
    TraversalCoverage candidateCoverage = candidate.getTraversalCoverage();
    if (freshTrustedWalkWouldBeLost(existing, candidateCoverage)) {
      return true;
    }
    if (cacheStatus != CacheStatus.ACTIVE) {
      return false;
    }
    if (candidateCoverage == null) {
      return false;
    }
    EntityGraphDefinition definition = registry.getDefinition(existing.getGraphId());
    if (definition != null
        && SnapshotFreshnessEvaluator.isStale(existing, definition)
        && replacesTrustedDirection(existing.getTraversalCoverage(), candidateCoverage)) {
      return false;
    }
    if (!candidate.getTopologyFingerprint().equals(existing.getTopologyFingerprint())) {
      return false;
    }
    return !candidateCoverage.isStrictImprovementOver(existing.getTraversalCoverage());
  }

  /**
   * A fresh trusted walk is the more complete snapshot. Skip a publish that would drop it,
   * including when the candidate fingerprint differs, until the snapshot is stale.
   */
  private boolean freshTrustedWalkWouldBeLost(
      @Nonnull EntityGraphSnapshot existing, @Nullable TraversalCoverage candidateCoverage) {
    TraversalCoverage existingCoverage = existing.getTraversalCoverage();
    if (existingCoverage == null || !existingCoverage.hasTrustedFullWalk()) {
      return false;
    }
    EntityGraphDefinition definition = registry.getDefinition(existing.getGraphId());
    if (definition == null || SnapshotFreshnessEvaluator.isStale(existing, definition)) {
      return false;
    }
    if (candidateCoverage == null) {
      return true;
    }
    for (DirectionCoverage direction : existingCoverage.getDirections()) {
      if (!direction.isTrustedFullWalk()) {
        continue;
      }
      DirectionCoverage replacement = candidateCoverage.getDirection(direction.getDirection());
      if (replacement == null || !replacement.isTrustedFullWalk()) {
        return true;
      }
    }
    return false;
  }

  /**
   * True when {@code candidate} carries a direction that {@code existing} stamped as a trusted full
   * walk and the candidate no longer marks that direction trusted. A stale snapshot must publish so
   * the stamp does not stick forever.
   */
  private static boolean replacesTrustedDirection(
      @Nullable TraversalCoverage existing, @Nonnull TraversalCoverage candidate) {
    if (existing == null) {
      return false;
    }
    for (DirectionCoverage direction : existing.getDirections()) {
      if (!direction.isTrustedFullWalk()) {
        continue;
      }
      DirectionCoverage replacement = candidate.getDirection(direction.getDirection());
      if (replacement != null && !replacement.isTrustedFullWalk()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Publishes a walk the caller already finished. The invalidation generation is checked inside the
   * same {@link IMap#compute} as the edge replace. Does not bump the invalidation generation and
   * does not write an {@link CacheStatus#OVER_LIMIT} tombstone.
   */
  @Nonnull
  public FullWalkPublishResult publishFullWalk(
      @Nullable EntityGraphDefinition definition, @Nonnull FullWalkWriteBack writeBack) {
    if (writeBack.getSeeds() == null || writeBack.getSeeds().isEmpty()) {
      return FullWalkPublishResult.REJECTED_SIGNAL;
    }
    if (writeBack.isTruncated()) {
      return FullWalkPublishResult.REJECTED_TRUNCATED;
    }
    if (definition == null || definition.getScope().getMode() != ScopeMode.PARTIAL) {
      return FullWalkPublishResult.REJECTED_NOT_PARTIAL;
    }
    if (definition.getBuildSource() != writeBack.getSource()) {
      return FullWalkPublishResult.REJECTED_SOURCE;
    }
    List<DirectedEdge> walkedEdges = toDirectedEdges(writeBack.getEdges());
    Set<String> resolvedKeys = new LinkedHashSet<>();
    for (String vertex : closureVertices(writeBack.getSeeds(), walkedEdges)) {
      findCacheKeyForSeeds(writeBack.getGraphId(), writeBack.getSource(), Set.of(vertex))
          .ifPresent(resolvedKeys::add);
    }
    if (resolvedKeys.size() > 1) {
      return FullWalkPublishResult.REJECTED_SPLIT_COMPONENTS;
    }
    boolean create = resolvedKeys.isEmpty();
    if (create && walkedEdges.isEmpty()) {
      return FullWalkPublishResult.REJECTED_EMPTY_CREATE;
    }
    String cacheKey;
    if (create) {
      List<DirectedEdge> createdEdges =
          new EntityGraphView(List.of())
              .replacingClosure(writeBack.getDirection(), writeBack.getSeeds(), walkedEdges)
              .getEdges();
      if (!new EntityGraphView(createdEdges).isSingleWeakComponent()) {
        return FullWalkPublishResult.REJECTED_SPLIT_COMPONENTS;
      }
      cacheKey =
          EntityGraphCacheKeys.componentCacheKey(
              writeBack.getGraphId(),
              writeBack.getSource(),
              EntityGraphSnapshotBuilder.topologyFingerprint(createdEdges));
    } else {
      cacheKey = resolvedKeys.iterator().next();
    }
    int maxVertices = definition.getBounds().getMaxVertices();
    int maxEdges = definition.getBounds().getMaxEdges().orElse(Integer.MAX_VALUE);
    int configuredMaxDepth = definition.getScope().getMaxDepth();
    FullWalkPublishResult[] outcome = {FullWalkPublishResult.PUBLISHED};
    snapshotsForKey(cacheKey)
        .compute(
            cacheKey,
            (key, existing) -> {
              long currentGeneration = getInvalidationGeneration(writeBack.getGraphId());
              if (currentGeneration != writeBack.getInvalidationGenerationAtStart()) {
                outcome[0] = FullWalkPublishResult.REJECTED_GENERATION;
                return existing;
              }
              long builtAtMillis = System.currentTimeMillis();
              long generation = existing == null ? 1L : existing.getGeneration() + 1L;
              String buildSource =
                  existing != null && existing.getBuildSource() != null
                      ? existing.getBuildSource()
                      : writeBack.getSource().name().toLowerCase(java.util.Locale.ROOT);
              FullWalkEdit edit =
                  EntityGraphSnapshotEditor.applyFullWalk(
                      existing,
                      writeBack.getGraphId(),
                      key,
                      buildSource,
                      builtAtMillis,
                      generation,
                      writeBack.getDirection(),
                      writeBack.getSeeds(),
                      walkedEdges,
                      configuredMaxDepth);
              if (!edit.isContainsAllSeeds() || edit.getSnapshot() == null) {
                outcome[0] = FullWalkPublishResult.REJECTED_EMPTY_CREATE;
                return existing;
              }
              EntityGraphSnapshot updated = edit.getSnapshot();
              if (updated.getVertexCount() > maxVertices || updated.getEdgeCount() > maxEdges) {
                outcome[0] = FullWalkPublishResult.REJECTED_BOUNDS;
                return existing;
              }
              if (!new EntityGraphView(updated.getEdges()).isSingleWeakComponent()) {
                outcome[0] = FullWalkPublishResult.REJECTED_SPLIT_COMPONENTS;
                return existing;
              }
              outcome[0] = FullWalkPublishResult.PUBLISHED;
              return updated;
            });
    if (outcome[0] == FullWalkPublishResult.PUBLISHED) {
      status.remove(cacheKey);
      refreshPartialSeedIndex(cacheKey);
    }
    return outcome[0];
  }

  @Nonnull
  private static Set<String> closureVertices(
      @Nonnull Set<String> seeds, @Nonnull List<DirectedEdge> walkedEdges) {
    Set<String> vertices = new LinkedHashSet<>();
    for (String seed : seeds) {
      addCanonical(vertices, seed);
    }
    for (DirectedEdge edge : walkedEdges) {
      addCanonical(vertices, edge.getSourceUrn());
      addCanonical(vertices, edge.getDestinationUrn());
    }
    return vertices;
  }

  private static void addCanonical(@Nonnull Set<String> vertices, @Nullable String raw) {
    String canonical = EntityGraphEndpoints.parse(raw);
    if (canonical != null) {
      vertices.add(canonical);
    }
  }

  @Nonnull
  private static List<DirectedEdge> toDirectedEdges(@Nullable List<FullWalkEdge> edges) {
    if (edges == null || edges.isEmpty()) {
      return List.of();
    }
    List<DirectedEdge> directed = new ArrayList<>(edges.size());
    for (FullWalkEdge edge : edges) {
      directed.add(
          DirectedEdge.builder()
              .sourceUrn(edge.getSourceUrn())
              .destinationUrn(edge.getDestinationUrn())
              .relationshipType(edge.getRelationshipType())
              .build());
    }
    return directed;
  }

  /** When a transient failure cooldown was recorded, or empty if never in cooldown. */
  @Nonnull
  public Optional<Long> getCooldownRecordedAt(@Nonnull String cacheKey) {
    EntityGraphOperationalStatus operational = status.get(cacheKey);
    if (operational == null || operational.getRecordedAtMillis() == null) {
      return Optional.empty();
    }
    return Optional.of(operational.getRecordedAtMillis());
  }

  /**
   * Returns true when a {@link CacheStatus#BUILDING} lease is recorded in {@code
   * entityGraphStatus}.
   */
  public boolean isRebuildLeaseHeld(@Nonnull String cacheKey) {
    EntityGraphOperationalStatus operational = status.get(cacheKey);
    return operational != null && operational.cacheStatus() == CacheStatus.BUILDING;
  }

  @Nullable
  private static EntityGraphView resolveViewForVertexLookup(
      @Nonnull EntityGraphSnapshot snapshot,
      @Nullable Function<EntityGraphSnapshot, Optional<EntityGraphView>> localViewLookup) {
    if (localViewLookup != null) {
      Optional<EntityGraphView> cached = localViewLookup.apply(snapshot);
      if (cached.isPresent()) {
        return cached.get();
      }
    }
    if (snapshot.getEdges() == null || snapshot.getEdges().isEmpty()) {
      return null;
    }
    return new EntityGraphView(snapshot.getEdges());
  }

  @Nonnull
  public Optional<String> findCacheKeyForSeeds(
      @Nonnull String graphId, @Nonnull GraphSnapshotSource source, @Nonnull Set<String> seeds) {
    return findCacheKeyForSeeds(graphId, source, seeds, null);
  }

  /**
   * When {@code localViewLookup} returns a view for a snapshot, reuses the in-memory graph view
   * instead of deserializing edges into a new {@link EntityGraphView}.
   */
  @Nonnull
  public Optional<String> findCacheKeyForSeeds(
      @Nonnull String graphId,
      @Nonnull GraphSnapshotSource source,
      @Nonnull Set<String> seeds,
      @Nullable Function<EntityGraphSnapshot, Optional<EntityGraphView>> localViewLookup) {
    Set<String> candidateKeys = partialSeedIndex.candidateKeysForSeeds(graphId, source, seeds);
    if (!candidateKeys.isEmpty()) {
      Optional<String> indexed = findCacheKeyAmongCandidates(candidateKeys, seeds, localViewLookup);
      if (indexed.isPresent()) {
        return indexed;
      }
    }
    for (Map.Entry<String, EntityGraphSnapshot> entry : snapshotEntriesForGraph(graphId)) {
      if (!EntityGraphCacheKeys.cacheKeyMatchesSource(entry.getKey(), graphId, source)) {
        continue;
      }
      EntityGraphSnapshot snapshot = entry.getValue();
      if (snapshot == null) {
        continue;
      }
      EntityGraphView view = resolveViewForVertexLookup(snapshot, localViewLookup);
      if (view != null && view.seedsInSameWeakComponent(seeds)) {
        partialSeedIndex.indexSnapshot(snapshot);
        return Optional.of(entry.getKey());
      }
    }
    return Optional.empty();
  }

  @Nonnull
  private Optional<String> findCacheKeyAmongCandidates(
      @Nonnull Set<String> candidateKeys,
      @Nonnull Set<String> seeds,
      @Nullable Function<EntityGraphSnapshot, Optional<EntityGraphView>> localViewLookup) {
    for (String cacheKey : candidateKeys) {
      EntityGraphSnapshot snapshot = getSnapshot(cacheKey);
      if (snapshot == null) {
        continue;
      }
      EntityGraphView view = resolveViewForVertexLookup(snapshot, localViewLookup);
      if (view != null && view.seedsInSameWeakComponent(seeds)) {
        return Optional.of(cacheKey);
      }
    }
    return Optional.empty();
  }

  public void dropCacheKey(@Nonnull String cacheKey) {
    partialSeedIndex.removeCacheKey(cacheKey);
    snapshotsForKey(cacheKey).remove(cacheKey);
    status.remove(cacheKey);
  }

  /**
   * Returns the first non-{@link CacheStatus#ABSENT} status among snapshot keys for {@code
   * graphId}.
   */
  @Nonnull
  public CacheStatus anySnapshotStatusForGraph(@Nonnull String graphId) {
    for (Map.Entry<String, EntityGraphSnapshot> entry : snapshotEntriesForGraph(graphId)) {
      CacheStatus status = getStatus(entry.getKey());
      if (status != CacheStatus.ABSENT) {
        return status;
      }
    }
    return CacheStatus.ABSENT;
  }

  /** Drops all snapshot keys for a FULL-scope graph (invalidation path). */
  public void dropGraph(@Nonnull String graphId) {
    EntityGraphDefinition definition = registry.getDefinition(graphId);
    if (definition == null || definition.getScope().getMode() != ScopeMode.FULL) {
      return;
    }
    if (fullSnapshots != null) {
      removeKeysForGraphId(fullSnapshots, graphId);
    }
    removeKeysForGraphId(status, graphId);
    recordInvalidationGeneration(graphId);
  }

  /** Drops all snapshot keys for a PARTIAL-scope graph (invalidation path). */
  public void dropPartialGraph(@Nonnull String graphId) {
    EntityGraphDefinition definition = registry.getDefinition(graphId);
    if (definition == null || definition.getScope().getMode() != ScopeMode.PARTIAL) {
      return;
    }
    partialSeedIndex.removeGraph(graphId);
    IMap<String, EntityGraphSnapshot> partialSnapshots = partialSnapshotsByGraphId.get(graphId);
    if (partialSnapshots != null) {
      partialSnapshots.clear();
    }
    removeKeysForGraphId(status, graphId);
    recordInvalidationGeneration(graphId);
  }

  /**
   * Removes {@code entityUrn} from every snapshot for {@code graphId}. When the edited snapshot was
   * {@link CacheStatus#OVER_LIMIT} and the remaining vertex count is below {@code maxVertices},
   * clears the failure status so rebuild can proceed. Returns true when any snapshot changed.
   */
  public boolean removeVertexFromSnapshot(
      @Nonnull String graphId, @Nonnull String entityUrn, int maxVertices) {
    boolean changed = false;
    List<String> cacheKeys = new ArrayList<>();
    for (Map.Entry<String, EntityGraphSnapshot> entry : snapshotEntriesForGraph(graphId)) {
      cacheKeys.add(entry.getKey());
    }
    for (String cacheKey : cacheKeys) {
      CacheStatus statusBefore = getStatus(cacheKey);
      if (statusBefore == CacheStatus.INVALID) {
        continue;
      }
      AtomicVertexRemovalResult removalResult = applyVertexRemovalInCompute(cacheKey, entityUrn);
      if (!removalResult.changed) {
        continue;
      }
      changed = true;
      if (removalResult.droppedKey) {
        partialSeedIndex.removeCacheKey(cacheKey);
      } else if (removalResult.snapshotAfter != null) {
        partialSeedIndex.indexSnapshot(removalResult.snapshotAfter);
      }
      status.remove(cacheKey);
      if (!removalResult.droppedKey
          && statusBefore == CacheStatus.OVER_LIMIT
          && removalResult.snapshotAfter != null
          && removalResult.snapshotAfter.getVertexCount() < maxVertices) {
        status.remove(cacheKey);
      }
    }
    if (changed) {
      recordInvalidationGeneration(graphId);
    }
    return changed;
  }

  /**
   * Applies a vertex removal in a single {@link IMap#compute} so concurrent surgical edits on the
   * same cache key cannot resurrect removed edges.
   */
  @Nonnull
  AtomicVertexRemovalResult applyVertexRemovalInCompute(
      @Nonnull String cacheKey, @Nonnull String entityUrn) {
    AtomicVertexRemovalResult result = new AtomicVertexRemovalResult();
    snapshotsForKey(cacheKey)
        .compute(
            cacheKey,
            (key, existing) -> {
              if (existing == null) {
                return null;
              }
              EntityGraphSnapshotEditor.VertexRemovalResult removal =
                  EntityGraphSnapshotEditor.removeVertex(existing, entityUrn);
              if (!removal.isChanged()) {
                return existing;
              }
              result.changed = true;
              if (removal.isDropKey()) {
                result.droppedKey = true;
                return null;
              }
              EntityGraphSnapshot updated = removal.getSnapshot();
              if (updated == null) {
                return existing;
              }
              result.snapshotAfter =
                  EntityGraphSnapshot.builder()
                      .graphId(updated.getGraphId())
                      .cacheKey(key)
                      .generation(existing.getGeneration() + 1L)
                      .buildSource(updated.getBuildSource())
                      .builtAtMillis(updated.getBuiltAtMillis())
                      .edges(updated.getEdges() != null ? updated.getEdges() : List.of())
                      .vertexCount(updated.getVertexCount())
                      .edgeCount(updated.getEdgeCount())
                      .topologyFingerprint(updated.getTopologyFingerprint())
                      .traversalCoverage(updated.getTraversalCoverage())
                      .cacheStatus(CacheStatus.ACTIVE.name())
                      .build();
              return result.snapshotAfter;
            });
    return result;
  }

  static final class AtomicVertexRemovalResult {
    boolean changed;
    boolean droppedKey;
    @Nullable EntityGraphSnapshot snapshotAfter;
  }

  /** Clears failure marker keys for the given roots after a successful partial publish. */
  public void dropFailureMarkerKeysForRoots(
      @Nonnull String graphId,
      @Nonnull GraphSnapshotSource source,
      @Nonnull Collection<String> roots) {
    for (String root : roots) {
      String markerKey = EntityGraphCacheKeys.partialFailureMarkerKey(graphId, source, root);
      status.remove(markerKey);
    }
  }

  /**
   * Replaces a snapshot-map entry with an empty tombstone so stale ACTIVE blobs are not left after
   * failure. Failure-marker keys ({@code :marker:}) are status-only and skip tombstone writes.
   */
  private void writeFailureTombstoneIfSnapshotKey(
      @Nonnull String cacheKey, @Nonnull CacheStatus failureStatus) {
    if (EntityGraphCacheKeys.isFailureMarkerKey(cacheKey)) {
      return;
    }
    try {
      EntityGraphSnapshot existing = getSnapshot(cacheKey);
      String graphId = EntityGraphCacheKeys.graphIdFromCacheKey(cacheKey);
      GraphSnapshotSource source = EntityGraphCacheKeys.sourceFromCacheKey(cacheKey);
      String buildSource =
          existing != null && existing.getBuildSource() != null
              ? existing.getBuildSource()
              : source.name().toLowerCase(java.util.Locale.ROOT);
      long generation = existing != null ? existing.getGeneration() : 0L;
      EntityGraphSnapshot tombstone =
          EntityGraphSnapshot.builder()
              .graphId(graphId)
              .cacheKey(cacheKey)
              .generation(generation)
              .buildSource(buildSource)
              .builtAtMillis(System.currentTimeMillis())
              .edges(List.of())
              .vertexCount(0)
              .edgeCount(0)
              .topologyFingerprint(TOMBSTONE_FINGERPRINT)
              .traversalCoverage(null)
              .cacheStatus(failureStatus.name())
              .build();
      snapshotsForKey(cacheKey).put(cacheKey, tombstone);
    } catch (RuntimeException e) {
      log.warn("Failed to write failure tombstone for cache key {}", cacheKey, e);
    }
  }

  private boolean isStaleClaim(
      @Nonnull EntityGraphOperationalStatus current, long staleBuildingMillis) {
    Long recordedAt = current.getRecordedAtMillis();
    if (recordedAt == null) {
      // Incomplete BUILDING entry — reclaimable (legacy split-map entries may lack a timestamp).
      return true;
    }
    return System.currentTimeMillis() - recordedAt > staleBuildingMillis;
  }

  @Nonnull
  private Iterable<Map.Entry<String, EntityGraphSnapshot>> snapshotEntriesForGraph(
      @Nonnull String graphId) {
    EntityGraphDefinition definition = registry.getDefinition(graphId);
    if (definition == null) {
      return Set.of();
    }
    if (definition.getScope().getMode() == ScopeMode.FULL) {
      if (fullSnapshots == null) {
        return Set.of();
      }
      return fullSnapshots.entrySet().stream()
              .filter(
                  entry -> graphId.equals(EntityGraphCacheKeys.graphIdFromCacheKey(entry.getKey())))
          ::iterator;
    }
    IMap<String, EntityGraphSnapshot> partialSnapshots = partialSnapshotsByGraphId.get(graphId);
    if (partialSnapshots == null || partialSnapshots.isEmpty()) {
      return Set.of();
    }
    return partialSnapshots.entrySet();
  }

  private static void removeKeysForGraphId(@Nonnull IMap<String, ?> map, @Nonnull String graphId) {
    List<String> keysToRemove = new ArrayList<>();
    for (String key : map.keySet()) {
      if (graphId.equals(EntityGraphCacheKeys.graphIdFromCacheKey(key))) {
        keysToRemove.add(key);
      }
    }
    for (String key : keysToRemove) {
      map.remove(key);
    }
  }

  @Nonnull
  private IMap<String, EntityGraphSnapshot> snapshotsForKey(@Nonnull String cacheKey) {
    String graphId = EntityGraphCacheKeys.graphIdFromCacheKey(cacheKey);
    EntityGraphDefinition definition = registry.getDefinition(graphId);
    if (definition == null) {
      throw new IllegalStateException("No graph definition registered for graphId " + graphId);
    }
    if (definition.getScope().getMode() == ScopeMode.FULL) {
      if (fullSnapshots == null) {
        throw new IllegalStateException(
            "No FULL snapshot map registered — graph " + graphId + " is FULL scope");
      }
      return fullSnapshots;
    }
    IMap<String, EntityGraphSnapshot> partialSnapshots = partialSnapshotsByGraphId.get(graphId);
    if (partialSnapshots == null) {
      throw new IllegalStateException("No partial snapshot map registered for graphId " + graphId);
    }
    return partialSnapshots;
  }
}
