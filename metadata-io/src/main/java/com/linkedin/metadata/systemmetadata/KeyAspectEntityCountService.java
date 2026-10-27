package com.linkedin.metadata.systemmetadata;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.systemmetadata.cache.KeyAspectEntityCountCache;
import com.linkedin.metadata.systemmetadata.cache.KeyAspectEntityCountCacheKey;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class KeyAspectEntityCountService {

  private final EntityRegistry entityRegistry;
  private final SystemMetadataService systemMetadataService;
  private final KeyAspectEntityCountCache countCache;
  private final int maxEntityTypes;

  @Nonnull
  public KeyAspectEntityCountResult getCounts(
      @Nonnull OperationContext opContext, @Nullable List<String> entityTypes, boolean skipCache) {
    List<String> resolvedTypes = resolveEntityTypes(entityTypes);
    KeyAspectEntityCountCacheKey cacheKey =
        KeyAspectEntityCountCacheKey.of(opContext.getSearchContextId(), resolvedTypes);
    return countCache.get(
        opContext, cacheKey, skipCache, () -> loadCounts(opContext, resolvedTypes));
  }

  @Nonnull
  public KeyAspectEntityCountResult getCountForEntityType(
      @Nonnull OperationContext opContext, @Nonnull String entityType, boolean skipCache) {
    return getCounts(opContext, List.of(entityType), skipCache);
  }

  @Nonnull
  private KeyAspectEntityCountResult loadCounts(
      @Nonnull OperationContext opContext, @Nonnull List<String> entityTypes) {
    Map<String, String> entityToKeyAspect = buildEntityToKeyAspectMap(entityTypes);
    List<String> keyAspects = new ArrayList<>(entityToKeyAspect.values());

    Map<String, KeyAspectCount> countsByAspect;
    if (keyAspects.size() == 1) {
      String keyAspect = keyAspects.get(0);
      countsByAspect =
          Map.of(keyAspect, systemMetadataService.countByKeyAspect(opContext, keyAspect));
    } else {
      countsByAspect = systemMetadataService.countByKeyAspects(opContext, keyAspects);
    }

    List<KeyAspectEntityCountEntry> entries =
        entityTypes.stream()
            .map(
                entityType -> {
                  String keyAspect = entityToKeyAspect.get(entityType);
                  KeyAspectCount count =
                      countsByAspect.getOrDefault(keyAspect, KeyAspectCount.empty());
                  return KeyAspectEntityCountEntry.of(entityType, keyAspect, count);
                })
            .collect(Collectors.toList());

    return KeyAspectEntityCountResult.builder()
        .counts(entries)
        .requestedTypes(entityTypes)
        .computedAt(Instant.now())
        .cacheHit(false)
        .build();
  }

  @Nonnull
  private List<String> resolveEntityTypes(@Nullable List<String> entityTypes) {
    if (entityTypes == null || entityTypes.isEmpty()) {
      return entityRegistry.getEntitySpecs().keySet().stream()
          .sorted(Comparator.naturalOrder())
          .collect(Collectors.toList());
    }

    List<String> normalized =
        entityTypes.stream()
            .map(String::toLowerCase)
            .distinct()
            .sorted()
            .collect(Collectors.toList());

    if (normalized.size() > maxEntityTypes) {
      throw new IllegalArgumentException(
          "Requested entity type count "
              + normalized.size()
              + " exceeds maximum "
              + maxEntityTypes);
    }

    for (String entityType : normalized) {
      if (!entityRegistry.getEntitySpecs().containsKey(entityType)) {
        throw new IllegalArgumentException("Unknown entity type: " + entityType);
      }
    }
    return normalized;
  }

  @Nonnull
  private Map<String, String> buildEntityToKeyAspectMap(@Nonnull List<String> entityTypes) {
    Map<String, String> entityToKeyAspect = new LinkedHashMap<>();
    for (String entityType : entityTypes) {
      EntitySpec entitySpec = entityRegistry.getEntitySpec(entityType);
      entityToKeyAspect.put(entityType, entitySpec.getKeyAspectName());
    }
    return entityToKeyAspect;
  }
}
