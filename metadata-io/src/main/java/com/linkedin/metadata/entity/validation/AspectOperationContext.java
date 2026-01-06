package com.linkedin.metadata.entity.validation;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * ThreadLocal context for passing operation-specific flags through the aspect loading stack.
 *
 * <p>Used to communicate context from high-level operations (e.g., deleteAspect) down to validation
 * layers without threading parameters through many method signatures.
 *
 * <p><b>Usage Pattern:</b>
 *
 * <pre>
 * try {
 *   AspectOperationContext.set("isRemediationDeletion", true);
 *   // ... operation that loads aspects ...
 * } finally {
 *   AspectOperationContext.clear(); // Always cleanup
 * }
 * </pre>
 */
public class AspectOperationContext {

  private static final ThreadLocal<Map<String, Object>> CONTEXT = new ThreadLocal<>();

  private AspectOperationContext() {
    // Utility class
  }

  /**
   * Set a context value for the current thread.
   *
   * @param key context key
   * @param value context value
   */
  public static void set(@Nonnull String key, @Nonnull Object value) {
    Map<String, Object> context = CONTEXT.get();
    if (context == null) {
      context = new HashMap<>();
      CONTEXT.set(context);
    }
    context.put(key, value);
  }

  /**
   * Get the entire context map for the current thread.
   *
   * @return unmodifiable context map, or empty map if no context set
   */
  @Nonnull
  public static Map<String, Object> get() {
    Map<String, Object> context = CONTEXT.get();
    return context != null ? Collections.unmodifiableMap(context) : Collections.emptyMap();
  }

  /**
   * Get a specific context value for the current thread.
   *
   * @param key context key
   * @return context value, or null if not set
   */
  public static Object get(@Nonnull String key) {
    Map<String, Object> context = CONTEXT.get();
    return context != null ? context.get(key) : null;
  }

  /**
   * Clear all context for the current thread. Should always be called in a finally block to prevent
   * memory leaks when threads are returned to pools.
   */
  public static void clear() {
    CONTEXT.remove();
  }
}
