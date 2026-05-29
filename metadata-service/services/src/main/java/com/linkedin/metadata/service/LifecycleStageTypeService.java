package com.linkedin.metadata.service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.lifecycle.LifecycleStageSettings;
import com.linkedin.lifecycle.LifecycleStageTypeInfo;
import com.linkedin.metadata.search.SearchEntity;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for loading and caching lifecycle stage type definitions.
 *
 * <p>Provides the search layer with the set of lifecycle stage URNs that should be hidden from
 * default search results, broken down by entity type. Caches results with periodic refresh.
 */
@Slf4j
public class LifecycleStageTypeService {

  public static final String LIFECYCLE_STAGE_TYPE_ENTITY_NAME = "lifecycleStageType";
  public static final String LIFECYCLE_STAGE_TYPE_INFO_ASPECT_NAME = "lifecycleStageTypeInfo";

  /** URN constant for the PROPOSED lifecycle stage (bootstrapped default). */
  public static final String PROPOSED_LIFECYCLE_STAGE_URN = "urn:li:lifecycleStageType:PROPOSED";

  private final SystemEntityClient entityClient;

  /**
   * Cache key is a sentinel (we cache one global snapshot). Value is a map from entity type name to
   * the set of lifecycle stage URN strings that should be hidden for that entity type.
   *
   * <p>Stages with {@code entityTypes == null} (applies to all) are stored under the special key
   * {@link #ALL_ENTITY_TYPES_KEY}.
   */
  private static final String ALL_ENTITY_TYPES_KEY = "*";

  private final LoadingCache<String, Map<String, Set<String>>> cache;

  @Nullable private volatile OperationContext systemOpContext;

  public LifecycleStageTypeService(@Nonnull SystemEntityClient entityClient) {
    this.entityClient = entityClient;
    this.cache =
        CacheBuilder.newBuilder()
            .maximumSize(1)
            .refreshAfterWrite(5, TimeUnit.MINUTES)
            .build(
                new CacheLoader<>() {
                  @Nonnull
                  @Override
                  public Map<String, Set<String>> load(@Nonnull String key) {
                    return loadHiddenStages();
                  }
                });
  }

  /** Must be called once after Spring wiring with the system operation context. */
  public void setSystemOperationContext(@Nonnull OperationContext opContext) {
    this.systemOpContext = opContext;
  }

  /**
   * Returns the set of lifecycle stage URN strings that should be excluded from default search for
   * the given entity types. Includes stages that apply to all entity types ({@code entityTypes ==
   * null} on the definition) and stages that explicitly list any of the requested types.
   */
  @Nonnull
  public Set<String> getHiddenStageUrns(@Nonnull Set<String> entityTypes) {
    Map<String, Set<String>> hiddenByType;
    try {
      hiddenByType = cache.get("singleton");
    } catch (Exception e) {
      log.warn("Failed to load lifecycle stage cache, returning empty set", e);
      return Collections.emptySet();
    }

    Set<String> result = new HashSet<>();

    // Stages that apply to ALL entity types
    Set<String> globalStages = hiddenByType.getOrDefault(ALL_ENTITY_TYPES_KEY, Set.of());
    result.addAll(globalStages);

    // Stages specific to the requested entity types
    for (String entityType : entityTypes) {
      result.addAll(hiddenByType.getOrDefault(entityType, Set.of()));
    }

    return result;
  }

  /**
   * Returns the info for a specific lifecycle stage type, or null if not found. Uses the stored
   * system operation context.
   */
  @Nullable
  public LifecycleStageTypeInfo getStageInfo(@Nonnull Urn stageUrn) {
    if (systemOpContext == null) {
      log.debug("System operation context not yet available, cannot load stage info");
      return null;
    }
    return getStageInfo(systemOpContext, stageUrn);
  }

  /** Returns the info for a specific lifecycle stage type, or null if not found. */
  @Nullable
  public LifecycleStageTypeInfo getStageInfo(
      @Nonnull OperationContext opContext, @Nonnull Urn stageUrn) {
    try {
      EntityResponse response =
          entityClient.getV2(
              opContext,
              LIFECYCLE_STAGE_TYPE_ENTITY_NAME,
              stageUrn,
              ImmutableSet.of(LIFECYCLE_STAGE_TYPE_INFO_ASPECT_NAME));
      if (response == null
          || !response.getAspects().containsKey(LIFECYCLE_STAGE_TYPE_INFO_ASPECT_NAME)) {
        return null;
      }
      return new LifecycleStageTypeInfo(
          response.getAspects().get(LIFECYCLE_STAGE_TYPE_INFO_ASPECT_NAME).getValue().data());
    } catch (Exception e) {
      log.warn("Failed to load lifecycle stage info for {}", stageUrn, e);
      return null;
    }
  }

  /**
   * Loads all lifecycle stage types and builds the hidden-stage map.
   *
   * <p>Returns a map: entity type name → set of lifecycle stage URN strings with hideInSearch=true.
   * The special key "*" holds stages that apply to all entity types.
   */
  @Nonnull
  private Map<String, Set<String>> loadHiddenStages() {
    if (systemOpContext == null) {
      log.debug("System operation context not yet available, returning empty hidden stages");
      return Collections.emptyMap();
    }

    try {
      // Search for all lifecycle stage type entities
      var searchResult =
          entityClient.search(
              systemOpContext, LIFECYCLE_STAGE_TYPE_ENTITY_NAME, "", null, null, 0, 1000);

      if (searchResult == null || searchResult.getEntities().isEmpty()) {
        return Collections.emptyMap();
      }

      Set<Urn> stageUrns =
          searchResult.getEntities().stream()
              .map(SearchEntity::getEntity)
              .collect(Collectors.toSet());

      Map<Urn, EntityResponse> responses =
          entityClient.batchGetV2(
              systemOpContext,
              LIFECYCLE_STAGE_TYPE_ENTITY_NAME,
              stageUrns,
              ImmutableSet.of(LIFECYCLE_STAGE_TYPE_INFO_ASPECT_NAME));

      Map<String, Set<String>> result = new HashMap<>();

      for (Map.Entry<Urn, EntityResponse> entry : responses.entrySet()) {
        EntityResponse response = entry.getValue();
        if (!response.getAspects().containsKey(LIFECYCLE_STAGE_TYPE_INFO_ASPECT_NAME)) {
          continue;
        }

        LifecycleStageTypeInfo info =
            new LifecycleStageTypeInfo(
                response.getAspects().get(LIFECYCLE_STAGE_TYPE_INFO_ASPECT_NAME).getValue().data());

        LifecycleStageSettings settings = info.getSettings();
        if (settings == null || !settings.isHideInSearch()) {
          continue;
        }

        String stageUrnStr = entry.getKey().toString();

        if (!info.hasEntityTypes()) {
          // null entityTypes → applies to all entity types
          result.computeIfAbsent(ALL_ENTITY_TYPES_KEY, k -> new HashSet<>()).add(stageUrnStr);
        } else if (info.getEntityTypes().isEmpty()) {
          // empty list → applies to no entity types (skip)
          continue;
        } else {
          for (String entityType : info.getEntityTypes()) {
            result.computeIfAbsent(entityType, k -> new HashSet<>()).add(stageUrnStr);
          }
        }
      }

      log.debug("Loaded hidden lifecycle stages: {}", result);
      return result;
    } catch (Exception e) {
      log.warn("Failed to load lifecycle stage types", e);
      return Collections.emptyMap();
    }
  }
}
