package datahub.client.v2.entity;

import com.linkedin.data.template.RecordTemplate;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Simple TTL-based cache for entity aspects fetched from the server.
 *
 * <p>This cache stores aspects retrieved from DataHub with automatic expiration based on
 * time-to-live. It provides:
 *
 * <ul>
 *   <li>TTL-based automatic expiration
 *   <li>Thread-safe operations for concurrent access
 *   <li>Simple get/put semantics
 * </ul>
 *
 * <p>This cache is used to avoid redundant server fetches for recently accessed aspects. Aspects
 * that exceed their TTL are automatically removed on the next access attempt.
 */
public class AspectCache {
  private final Map<String, CachedAspect> cache = new ConcurrentHashMap<>();
  private final long ttlMillis;

  /**
   * Creates a new aspect cache with the specified TTL.
   *
   * @param ttlMillis time-to-live in milliseconds for cached aspects
   */
  public AspectCache(long ttlMillis) {
    this.ttlMillis = ttlMillis;
  }

  /**
   * Retrieves an aspect from the cache.
   *
   * <p>This method returns null if:
   *
   * <ul>
   *   <li>The aspect is not in the cache
   *   <li>The aspect has expired (TTL exceeded)
   * </ul>
   *
   * Expired aspects are automatically removed from the cache.
   *
   * @param aspectName the name of the aspect to retrieve
   * @param aspectClass the expected class of the aspect
   * @param <T> the aspect type
   * @return the cached aspect, or null if not found or expired
   */
  @Nullable
  public <T extends RecordTemplate> T get(
      @Nonnull String aspectName, @Nonnull Class<T> aspectClass) {
    CachedAspect cached = cache.get(aspectName);
    if (cached == null) {
      return null;
    }

    // Check if aspect has expired
    long age = System.currentTimeMillis() - cached.timestamp;
    if (age > ttlMillis) {
      cache.remove(aspectName);
      return null;
    }

    return aspectClass.cast(cached.aspect);
  }

  /**
   * Stores an aspect in the cache with current timestamp.
   *
   * <p>This method replaces any existing cached aspect with the same name.
   *
   * @param aspectName the name of the aspect
   * @param aspect the aspect data to cache
   */
  public void put(@Nonnull String aspectName, @Nonnull RecordTemplate aspect) {
    cache.put(aspectName, new CachedAspect(aspect, System.currentTimeMillis()));
  }

  /**
   * Removes an aspect from the cache.
   *
   * @param aspectName the name of the aspect to remove
   * @return the removed aspect data, or null if it wasn't present
   */
  @Nullable
  public RecordTemplate remove(@Nonnull String aspectName) {
    CachedAspect cached = cache.remove(aspectName);
    return cached != null ? cached.aspect : null;
  }

  /**
   * Returns all aspects currently in the cache (including expired ones).
   *
   * @return map of aspect names to their aspect data for all cached aspects
   */
  @Nonnull
  public Map<String, RecordTemplate> getAllAspects() {
    Map<String, RecordTemplate> all = new HashMap<>();
    for (Map.Entry<String, CachedAspect> entry : cache.entrySet()) {
      all.put(entry.getKey(), entry.getValue().aspect);
    }
    return all;
  }

  /**
   * Checks if the cache is empty.
   *
   * @return true if the cache contains no aspects
   */
  public boolean isEmpty() {
    return cache.isEmpty();
  }

  /**
   * Returns the number of aspects in the cache.
   *
   * @return the number of cached aspects
   */
  public int size() {
    return cache.size();
  }

  /** Simple holder for a cached aspect with its timestamp. */
  private static class CachedAspect {
    final RecordTemplate aspect;
    final long timestamp;

    CachedAspect(RecordTemplate aspect, long timestamp) {
      this.aspect = aspect;
      this.timestamp = timestamp;
    }
  }
}
