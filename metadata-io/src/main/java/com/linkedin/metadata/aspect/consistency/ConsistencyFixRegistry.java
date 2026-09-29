package com.linkedin.metadata.aspect.consistency;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.aspect.consistency.fix.BatchItemsFix;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFix;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

/**
 * Registry for consistency fixes.
 *
 * <p>Maps {@link ConsistencyFixType} to {@link ConsistencyFix} implementations for applying fixes.
 *
 * <p><b>Thread Safety:</b> This class is thread-safe. All internal collections are immutable after
 * construction.
 */
@Slf4j
@ThreadSafe
public class ConsistencyFixRegistry {

  private final ImmutableMap<ConsistencyFixType, ConsistencyFix> fixes;

  /**
   * Create a new registry with the given fixes.
   *
   * <p>BatchItemsFix is registered for all its supported types (CREATE, UPSERT, PATCH, SOFT_DELETE,
   * DELETE_ASPECT). Other fixes are registered for their single type.
   *
   * @param fixList list of consistency fixes to register
   */
  public ConsistencyFixRegistry(@Nonnull Collection<ConsistencyFix> fixList) {
    Map<ConsistencyFixType, ConsistencyFix> fixMap = new HashMap<>();

    for (ConsistencyFix fix : fixList) {
      if (fix instanceof BatchItemsFix) {
        // BatchItemsFix handles multiple types
        BatchItemsFix batchFix = (BatchItemsFix) fix;
        for (ConsistencyFixType type : BatchItemsFix.SUPPORTED_TYPES) {
          fixMap.put(type, batchFix);
        }
      } else {
        // Other fixes handle a single type
        fixMap.put(fix.getType(), fix);
      }
    }

    this.fixes = ImmutableMap.copyOf(fixMap);
    log.info("ConsistencyFixRegistry initialized with {} fixes: {}", fixes.size(), fixes.keySet());
  }

  /**
   * Get the fix for a specific type.
   *
   * @param type fix type
   * @return optional containing the fix if found
   */
  @Nonnull
  public Optional<ConsistencyFix> getFix(@Nonnull ConsistencyFixType type) {
    return Optional.ofNullable(fixes.get(type));
  }

  /**
   * Get the fix for a specific type, throwing if not found.
   *
   * @param type fix type
   * @return the fix
   * @throws IllegalArgumentException if no fix is registered for the type
   */
  @Nonnull
  public ConsistencyFix getFixOrThrow(@Nonnull ConsistencyFixType type) {
    ConsistencyFix fix = fixes.get(type);
    if (fix == null) {
      throw new IllegalArgumentException("No fix registered for type: " + type);
    }
    return fix;
  }

  /**
   * Check if a fix is registered for a type.
   *
   * @param type fix type
   * @return true if a fix is registered
   */
  public boolean hasFix(@Nonnull ConsistencyFixType type) {
    return fixes.containsKey(type);
  }

  /**
   * Get the number of registered fixes.
   *
   * @return count of fixes
   */
  public int size() {
    return fixes.size();
  }
}
