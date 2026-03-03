package com.linkedin.metadata.aspect.consistency.check;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Builder;
import lombok.Data;

/**
 * Shared context for consistency checks during a batch operation.
 *
 * <p>This context is created for each batch and provides:
 *
 * <ul>
 *   <li>References to services needed for checking (EntityService, GraphClient)
 *   <li>Caches for entity data to avoid redundant fetches
 *   <li>Check-specific configuration
 * </ul>
 *
 * <p><b>Thread Safety:</b> This class is NOT thread-safe. It is designed to be created and used
 * within a single batch operation. Do not share instances between threads.
 */
@Data
@Builder
@NotThreadSafe
public class CheckContext {

  /** Operation context for the current request */
  @Nonnull private final OperationContext operationContext;

  /** Entity service for entity operations */
  @Nonnull private final EntityService<?> entityService;

  /** Graph client for relationship queries (may be null) */
  @Nullable private final GraphClient graphClient;

  /** Cache of entity responses (URN -> EntityResponse) */
  @Nonnull @Builder.Default
  private final Map<Urn, EntityResponse> entityResponseCache = new HashMap<>();

  /** Cache of aspect data (URN -> aspect name -> RecordTemplate) */
  @Nonnull @Builder.Default
  private final Map<Urn, Map<String, RecordTemplate>> aspectCache = new HashMap<>();

  /**
   * URNs found in indices but not in SQL (orphans).
   *
   * <p>These are URNs that were returned from the system metadata index query but had no
   * corresponding EntityResponse from EntityService.getEntitiesV2(). This indicates orphaned index
   * documents that need to be cleaned up.
   *
   * <p>Populated by ConsistencyService.checkBatch() and used by OrphanIndexDocumentCheck.
   */
  @Nonnull @Builder.Default
  private final Map<String, java.util.Set<Urn>> orphanUrnsByType = new HashMap<>();

  /**
   * Check-specific configuration (check ID -> config key -> value).
   *
   * <p>This is populated from the consistencyChecks.checks section of application.yaml and allows
   * checks to access their specific configuration at runtime. Values are strings; callers should
   * coerce types as needed.
   */
  @Nonnull @Builder.Default
  private final Map<String, Map<String, String>> checkConfigs = new HashMap<>();

  /**
   * Cache an entity response for later retrieval.
   *
   * @param urn URN of the entity
   * @param response entity response to cache
   */
  public void cacheEntityResponse(Urn urn, EntityResponse response) {
    entityResponseCache.put(urn, response);
  }

  /**
   * Get a cached entity response.
   *
   * @param urn URN of the entity
   * @return cached entity response, or null if not cached
   */
  @Nullable
  public EntityResponse getCachedEntityResponse(Urn urn) {
    return entityResponseCache.get(urn);
  }

  /**
   * Cache an aspect for quick access.
   *
   * @param urn URN of the entity
   * @param aspectName name of the aspect
   * @param aspect the aspect to cache
   */
  public void cacheAspect(Urn urn, String aspectName, RecordTemplate aspect) {
    aspectCache.computeIfAbsent(urn, k -> new HashMap<>()).put(aspectName, aspect);
  }

  /**
   * Get a cached aspect.
   *
   * @param urn URN of the entity
   * @param aspectName name of the aspect
   * @return cached aspect, or null if not cached
   */
  @Nullable
  @SuppressWarnings("unchecked")
  public <T extends RecordTemplate> T getCachedAspect(Urn urn, String aspectName) {
    Map<String, RecordTemplate> aspects = aspectCache.get(urn);
    if (aspects == null) {
      return null;
    }
    return (T) aspects.get(aspectName);
  }

  /** Clear all caches. Call this between batches if needed to prevent memory buildup. */
  public void clearCaches() {
    entityResponseCache.clear();
    aspectCache.clear();
    orphanUrnsByType.clear();
  }

  /**
   * Add orphan URNs for a specific entity type.
   *
   * @param entityType the entity type
   * @param orphanUrns set of orphan URNs to add
   */
  public void addOrphanUrns(String entityType, java.util.Set<Urn> orphanUrns) {
    orphanUrnsByType.computeIfAbsent(entityType, k -> new java.util.HashSet<>()).addAll(orphanUrns);
  }

  /**
   * Get orphan URNs for a specific entity type.
   *
   * @param entityType the entity type
   * @return set of orphan URNs, or empty set if none
   */
  @Nonnull
  public java.util.Set<Urn> getOrphanUrns(String entityType) {
    return orphanUrnsByType.getOrDefault(entityType, java.util.Set.of());
  }

  /**
   * Check if there are any orphan URNs for a specific entity type.
   *
   * @param entityType the entity type
   * @return true if there are orphan URNs
   */
  public boolean hasOrphanUrns(String entityType) {
    java.util.Set<Urn> orphans = orphanUrnsByType.get(entityType);
    return orphans != null && !orphans.isEmpty();
  }

  // ============================================================================
  // Check Configuration Access
  // ============================================================================

  /**
   * Get configuration for a specific check.
   *
   * @param checkId the check ID
   * @return configuration map for the check, or empty map if not configured
   */
  @Nonnull
  public Map<String, String> getCheckConfig(String checkId) {
    return checkConfigs.getOrDefault(checkId, Map.of());
  }
}
