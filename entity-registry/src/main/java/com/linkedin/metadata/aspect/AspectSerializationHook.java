package com.linkedin.metadata.aspect;

import javax.annotation.Nonnull;

/**
 * Hook interface called after aspect serialization but before database persistence. Allows
 * intercepting the serialized EntityAspect for validation, metrics, or other processing without
 * duplicate serialization overhead.
 *
 * <p>The hook is called with both the original SystemAspect (for context like URN, aspect name) and
 * the serialized EntityAspect (containing the JSON metadata string).
 */
public interface AspectSerializationHook {
  /**
   * Called after an aspect has been serialized to EntityAspect but before database write.
   *
   * @param systemAspect the original aspect with context (URN, aspect spec, etc.)
   * @param serializedAspect the serialized EntityAspect containing JSON metadata
   * @throws RuntimeException if validation or processing fails (will prevent DB write)
   */
  void afterSerialization(
      @Nonnull SystemAspect systemAspect, @Nonnull EntityAspect serializedAspect);
}
