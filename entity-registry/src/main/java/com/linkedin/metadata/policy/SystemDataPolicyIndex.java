package com.linkedin.metadata.policy;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Precomputed system-data policy decisions for an {@link EntityRegistry}. Built once per registry
 * instance and cached for the lifetime of that registry.
 */
@Immutable
public final class SystemDataPolicyIndex {

  private static final Map<EntityRegistry, SystemDataPolicyIndex> CACHE =
      Collections.synchronizedMap(new WeakHashMap<>());

  private final Set<String> knownEntityTypesLower;
  private final Set<String> systemEntityTypesLower;
  private final Map<String, String> keyAspectByEntityTypeLower;
  private final Map<String, Boolean> entityAllowRead;
  private final Map<String, Boolean> entityAllowExists;
  private final Map<String, Boolean> aspectAllowRead;
  private final Map<String, Boolean> aspectAllowExists;

  private SystemDataPolicyIndex(@Nonnull EntityRegistry entityRegistry) {
    Set<String> known = new HashSet<>();
    Set<String> systemEntities = new HashSet<>();
    Map<String, String> keyAspects = new HashMap<>();
    Map<String, Boolean> entityRead = new HashMap<>();
    Map<String, Boolean> entityExists = new HashMap<>();
    Map<String, Boolean> aspectRead = new HashMap<>();
    Map<String, Boolean> aspectExists = new HashMap<>();

    for (Map.Entry<String, EntitySpec> entry : entityRegistry.getEntitySpecs().entrySet()) {
      String entityTypeLower = entry.getKey().toLowerCase();
      EntitySpec entitySpec = entry.getValue();
      known.add(entityTypeLower);
      String keyAspectName = entitySpec.getKeyAspectName();
      keyAspects.put(entityTypeLower, keyAspectName != null ? keyAspectName : "");

      if (!entitySpec.isSystemEntity()) {
        continue;
      }

      systemEntities.add(entityTypeLower);
      entityRead.put(entityTypeLower, entitySpec.isSystemEntityAllowRead());
      entityExists.put(entityTypeLower, entitySpec.isSystemEntityAllowExists());

      if (entitySpec.getAspectSpecs() == null) {
        continue;
      }
      for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
        String aspectKey = aspectKey(entityTypeLower, aspectSpec.getName());
        aspectRead.put(aspectKey, SystemDataPolicy.effectiveAllowRead(entitySpec, aspectSpec));
        aspectExists.put(aspectKey, SystemDataPolicy.effectiveAllowExists(entitySpec, aspectSpec));
      }
    }

    this.knownEntityTypesLower = Set.copyOf(known);
    this.systemEntityTypesLower = Set.copyOf(systemEntities);
    this.keyAspectByEntityTypeLower = Map.copyOf(keyAspects);
    this.entityAllowRead = Map.copyOf(entityRead);
    this.entityAllowExists = Map.copyOf(entityExists);
    this.aspectAllowRead = Map.copyOf(aspectRead);
    this.aspectAllowExists = Map.copyOf(aspectExists);
  }

  @Nonnull
  public static SystemDataPolicyIndex forRegistry(@Nonnull EntityRegistry entityRegistry) {
    return CACHE.computeIfAbsent(entityRegistry, SystemDataPolicyIndex::new);
  }

  public boolean isKnownEntity(@Nonnull String entityType) {
    return knownEntityTypesLower.contains(entityType.toLowerCase());
  }

  public boolean requiresSystemActorWrite(@Nonnull String entityType) {
    return systemEntityTypesLower.contains(entityType.toLowerCase());
  }

  public boolean isEntityReadEligible(@Nonnull String entityType) {
    String entityTypeLower = entityType.toLowerCase();
    if (!systemEntityTypesLower.contains(entityTypeLower)) {
      return true;
    }
    return entityAllowRead.getOrDefault(entityTypeLower, false);
  }

  public boolean isEntityExistsEligible(@Nonnull String entityType) {
    String entityTypeLower = entityType.toLowerCase();
    if (!systemEntityTypesLower.contains(entityTypeLower)) {
      return true;
    }
    // Read implies exist: callers that can read an entity may check that it exists.
    return entityAllowExists.getOrDefault(entityTypeLower, false)
        || entityAllowRead.getOrDefault(entityTypeLower, false);
  }

  public boolean isReadEligible(@Nonnull String entityType, @Nonnull String aspectName) {
    String entityTypeLower = entityType.toLowerCase();
    if (!systemEntityTypesLower.contains(entityTypeLower)) {
      return true;
    }
    return aspectAllowRead.getOrDefault(aspectKey(entityTypeLower, aspectName), false);
  }

  public boolean isExistsEligible(@Nonnull String entityType, @Nonnull String aspectName) {
    String entityTypeLower = entityType.toLowerCase();
    if (!systemEntityTypesLower.contains(entityTypeLower)) {
      return true;
    }
    String key = aspectKey(entityTypeLower, aspectName);
    // Read implies exist at the aspect level as well (effective flags precomputed at index build).
    return aspectAllowExists.getOrDefault(key, false) || aspectAllowRead.getOrDefault(key, false);
  }

  @Nonnull
  public String getKeyAspectName(@Nonnull String entityType) {
    return keyAspectByEntityTypeLower.getOrDefault(entityType.toLowerCase(), "");
  }

  @VisibleForTesting
  public static void clearCache() {
    CACHE.clear();
  }

  @Nonnull
  private static String aspectKey(@Nonnull String entityTypeLower, @Nonnull String aspectName) {
    return entityTypeLower + '\0' + aspectName;
  }
}
