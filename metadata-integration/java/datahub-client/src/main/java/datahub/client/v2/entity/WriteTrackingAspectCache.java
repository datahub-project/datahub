package datahub.client.v2.entity;

import com.linkedin.data.template.RecordTemplate;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Thread-safe cache for entity aspects with TTL and dirty tracking.
 *
 * <p><strong>NOTE: This class is not currently used.</strong> It is preserved for future
 * implementation of write tracking and read-your-own-writes semantics. See {@link AspectCache} for
 * the current simple TTL-based cache.
 *
 * <p>This cache maintains aspects fetched from the server and locally modified aspects, providing:
 *
 * <ul>
 *   <li>TTL-based expiration for SERVER-sourced aspects
 *   <li>Dirty tracking for aspects with pending writes
 *   <li>Read-your-own-writes semantics via {@link ReadMode}
 *   <li>Thread-safe operations for concurrent access
 * </ul>
 *
 * <p>Aspects can come from two sources:
 *
 * <ul>
 *   <li>{@link AspectSource#SERVER} - fetched from DataHub, subject to TTL expiration
 *   <li>{@link AspectSource#LOCAL} - created/modified locally, never expire, always dirty
 * </ul>
 *
 * <p>Read modes control what gets returned:
 *
 * <ul>
 *   <li>{@link ReadMode#ALLOW_DIRTY} - returns dirty aspects (read-your-own-writes)
 *   <li>{@link ReadMode#SERVER_ONLY} - skips dirty aspects, returns only server state
 * </ul>
 */
public class WriteTrackingAspectCache {
  private final Map<String, CachedAspect> cache = new ConcurrentHashMap<>();
  private final long ttlMillis;

  /**
   * Creates a new aspect cache with the specified TTL.
   *
   * @param ttlMillis time-to-live in milliseconds for SERVER-sourced aspects. LOCAL-sourced aspects
   *     never expire regardless of this value.
   */
  public WriteTrackingAspectCache(long ttlMillis) {
    this.ttlMillis = ttlMillis;
  }

  /**
   * Retrieves an aspect from the cache.
   *
   * <p>This method:
   *
   * <ul>
   *   <li>Returns null if the aspect is not in the cache
   *   <li>Enforces TTL for SERVER-sourced aspects (removes if expired)
   *   <li>Respects {@link ReadMode} to control dirty aspect visibility
   *   <li>Never expires LOCAL-sourced aspects
   * </ul>
   *
   * @param aspectName the name of the aspect to retrieve
   * @param aspectClass the expected class of the aspect
   * @param readMode controls whether dirty aspects should be returned
   * @param <T> the aspect type
   * @return the cached aspect, or null if not found/expired/filtered by read mode
   */
  @Nullable
  public <T extends RecordTemplate> T get(
      @Nonnull String aspectName, @Nonnull Class<T> aspectClass, @Nonnull ReadMode readMode) {
    CachedAspect cached = cache.get(aspectName);
    if (cached == null) {
      return null;
    }

    // Check if SERVER-sourced aspect has expired
    if (cached.getSource() == AspectSource.SERVER) {
      long age = System.currentTimeMillis() - cached.getTimestamp();
      if (age > ttlMillis) {
        cache.remove(aspectName);
        return null;
      }
    }

    // Filter based on read mode
    if (readMode == ReadMode.SERVER_ONLY && cached.isDirty()) {
      return null;
    }

    return aspectClass.cast(cached.getAspect());
  }

  /**
   * Stores an aspect in the cache.
   *
   * <p>This method replaces any existing cached aspect with the same name.
   *
   * @param aspectName the name of the aspect
   * @param aspect the aspect data to cache
   * @param source where this aspect came from
   * @param dirty whether this aspect needs to be written to the server
   */
  public void put(
      @Nonnull String aspectName,
      @Nonnull RecordTemplate aspect,
      @Nonnull AspectSource source,
      boolean dirty) {
    cache.put(aspectName, new CachedAspect(aspect, source, dirty));
  }

  /**
   * Marks an aspect as dirty (having unsaved local modifications).
   *
   * <p>This is used when an aspect is modified locally and needs to be written to the server.
   *
   * @param aspectName the name of the aspect to mark dirty
   */
  public void markDirty(@Nonnull String aspectName) {
    CachedAspect cached = cache.get(aspectName);
    if (cached != null) {
      cached.markDirty();
    }
  }

  /**
   * Returns all aspects that have unsaved local modifications.
   *
   * <p>This includes both:
   *
   * <ul>
   *   <li>LOCAL-sourced aspects (always dirty by definition)
   *   <li>SERVER-sourced aspects that have been modified locally
   * </ul>
   *
   * @return map of aspect names to their aspect data for all dirty aspects
   */
  @Nonnull
  public Map<String, RecordTemplate> getDirtyAspects() {
    Map<String, RecordTemplate> dirty = new HashMap<>();
    for (Map.Entry<String, CachedAspect> entry : cache.entrySet()) {
      if (entry.getValue().isDirty()) {
        dirty.put(entry.getKey(), entry.getValue().getAspect());
      }
    }
    return dirty;
  }

  /**
   * Checks if an aspect has pending writes.
   *
   * @param aspectName the name of the aspect to check
   * @return true if the aspect exists in cache and is dirty, false otherwise
   */
  public boolean hasPendingWrites(@Nonnull String aspectName) {
    CachedAspect cached = cache.get(aspectName);
    return cached != null && cached.isDirty();
  }

  /**
   * Marks an aspect as clean (in sync with server).
   *
   * <p>This is typically called after successfully writing an aspect to the server.
   *
   * @param aspectName the name of the aspect to mark clean
   */
  public void markClean(@Nonnull String aspectName) {
    CachedAspect cached = cache.get(aspectName);
    if (cached != null) {
      cached.markClean();
    }
  }

  /**
   * Clears all dirty flags from the cache.
   *
   * <p>This is typically called after successfully writing all dirty aspects to the server. This
   * method does not remove aspects from the cache, it only marks them as clean.
   */
  public void clearDirty() {
    for (CachedAspect cached : cache.values()) {
      cached.markClean();
    }
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
    return cached != null ? cached.getAspect() : null;
  }

  /**
   * Returns all aspects in the cache (both clean and dirty).
   *
   * @return map of aspect names to their aspect data for all cached aspects
   */
  @Nonnull
  public Map<String, RecordTemplate> getAllAspects() {
    Map<String, RecordTemplate> all = new HashMap<>();
    for (Map.Entry<String, CachedAspect> entry : cache.entrySet()) {
      all.put(entry.getKey(), entry.getValue().getAspect());
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
}
