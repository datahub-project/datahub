package com.linkedin.metadata.graph.cache.store;

import com.linkedin.metadata.graph.cache.config.EntityGraphModel.LocalEvictionLimits;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphView;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

public class EntityGraphLocalViewCache {

  private static final class LocalViewEntry {
    private final String graphId;
    private final EntityGraphView view;
    private final long generation;
    private final long estimatedBytes;
    private volatile long lastAccessTime;

    LocalViewEntry(String graphId, EntityGraphView view, long generation) {
      this.graphId = graphId;
      this.view = view;
      this.generation = generation;
      this.estimatedBytes = estimateBytes(view);
      this.lastAccessTime = System.currentTimeMillis();
    }

    void touch() {
      lastAccessTime = System.currentTimeMillis();
    }

    private static long estimateBytes(EntityGraphView view) {
      return (long) view.vertexCount() * 128L + (long) view.edgeCount() * 64L;
    }
  }

  private final Map<String, LocalViewEntry> views = new ConcurrentHashMap<>();

  @Nonnull
  public Optional<EntityGraphView> get(@Nonnull String cacheKey, long generation) {
    LocalViewEntry entry = views.get(cacheKey);
    if (entry == null || entry.generation != generation) {
      return Optional.empty();
    }
    entry.touch();
    return Optional.of(entry.view);
  }

  public void put(
      @Nonnull String graphId,
      @Nonnull String cacheKey,
      @Nonnull EntityGraphView view,
      long generation,
      @Nonnull LocalEvictionLimits limits) {
    if (!limits.isEnabled()) {
      return;
    }
    views.put(cacheKey, new LocalViewEntry(graphId, view, generation));
    enforceLimits(graphId, limits);
  }

  public void evict(@Nonnull String cacheKey) {
    views.remove(cacheKey);
  }

  public void evictAll() {
    views.clear();
  }

  public void evictGraph(@Nonnull String graphId) {
    views.entrySet().removeIf(entry -> graphId.equals(entry.getValue().graphId));
  }

  private void enforceLimits(@Nonnull String graphId, @Nonnull LocalEvictionLimits limits) {
    while (countViewsForGraph(graphId) > limits.getMaxViews()) {
      evictLruForGraph(graphId, 1);
    }
    while (limits.getMaxEstimatedBytes() > 0
        && estimatedBytesForGraph(graphId) > limits.getMaxEstimatedBytes()) {
      long viewCount = countViewsForGraph(graphId);
      if (viewCount == 0) {
        break;
      }
      evictLruForGraph(graphId, Math.max(1, (int) (viewCount / 4)));
    }
  }

  private long countViewsForGraph(@Nonnull String graphId) {
    return views.values().stream().filter(entry -> graphId.equals(entry.graphId)).count();
  }

  private long estimatedBytesForGraph(@Nonnull String graphId) {
    return views.values().stream()
        .filter(entry -> graphId.equals(entry.graphId))
        .mapToLong(entry -> entry.estimatedBytes)
        .sum();
  }

  public void evictLruForGraph(@Nonnull String graphId, int count) {
    views.entrySet().stream()
        .filter(entry -> graphId.equals(entry.getValue().graphId))
        .sorted((a, b) -> Long.compare(a.getValue().lastAccessTime, b.getValue().lastAccessTime))
        .limit(count)
        .map(Map.Entry::getKey)
        .forEach(this::evict);
  }
}
