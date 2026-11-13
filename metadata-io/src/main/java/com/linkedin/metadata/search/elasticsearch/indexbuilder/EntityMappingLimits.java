package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolved {@code elasticsearch.index.entityMappingLimits}: configured limit keys translated to ES
 * setting paths (e.g. {@code totalFields} -> {@code mapping.total_fields.limit}), keyed by
 * lower-cased entity name.
 *
 * <p>Limits stay keyed by entity rather than by index name so they are resolved against the actual
 * index at build time. That covers the V2 index and its semantic twin, and any per-operation index
 * prefix, without enumerating index names up front. {@link #defaults} applies to entity indices
 * without an explicit entry, and never to non-entity indices (graph, system metadata, timeseries)
 * or V3 indices, which manage their own field limit.
 */
@Slf4j
public record EntityMappingLimits(
    @Nonnull Map<String, Map<String, String>> byEntity, @Nonnull Map<String, String> defaults) {

  /**
   * Configured limit key -> ES index setting path. Kept small and code-defined so the surface is
   * locked to known-safe dynamic mapping ceilings; unknown keys are dropped with a warning.
   */
  public static final Map<String, String> LIMIT_SETTING_KEYS =
      Map.of("totalfields", "mapping.total_fields.limit");

  /** Reserved entity key that applies to entity indices without an explicit entry. */
  public static final String DEFAULT_KEY = "default";

  public static final EntityMappingLimits EMPTY = new EntityMappingLimits(Map.of(), Map.of());

  /**
   * Translate {@code elasticsearch.index.entityMappingLimits} (entity name -> limit name -> value).
   * Entity and limit keys are matched case-insensitively: Spring's map binder lower-cases keys that
   * come from env vars ({@code ..._DATAJOB_TOTALFIELDS}) but preserves YAML keys ({@code
   * dataJob.totalFields}).
   */
  @Nonnull
  public static EntityMappingLimits fromConfig(@Nullable Map<String, Map<String, Integer>> config) {
    if (config == null || config.isEmpty()) {
      return EMPTY;
    }
    Map<String, String> defaults = Map.of();
    Map<String, Map<String, String>> byEntity = new HashMap<>();
    for (Map.Entry<String, Map<String, Integer>> entityEntry : config.entrySet()) {
      String entity = entityEntry.getKey().toLowerCase(Locale.ROOT);
      Map<String, String> esSettings = translateLimitKeys(entity, entityEntry.getValue());
      if (esSettings.isEmpty()) {
        continue;
      }
      if (DEFAULT_KEY.equals(entity)) {
        defaults = esSettings;
      } else {
        byEntity.put(entity, esSettings);
      }
    }
    return new EntityMappingLimits(Map.copyOf(byEntity), defaults);
  }

  @Nonnull
  private static Map<String, String> translateLimitKeys(
      @Nonnull String entity, @Nullable Map<String, Integer> limits) {
    if (limits == null) {
      return Map.of();
    }
    Map<String, String> out = new HashMap<>();
    for (Map.Entry<String, Integer> e : limits.entrySet()) {
      String esKey = LIMIT_SETTING_KEYS.get(e.getKey().toLowerCase(Locale.ROOT));
      if (esKey == null || e.getValue() == null) {
        log.warn(
            "Ignoring entityMappingLimits.{}.{} = {} (unsupported limit key; supported: {})",
            entity,
            e.getKey(),
            e.getValue(),
            LIMIT_SETTING_KEYS.keySet());
        continue;
      }
      out.put(esKey, String.valueOf(e.getValue()));
    }
    return Map.copyOf(out);
  }

  public boolean isEmpty() {
    return byEntity.isEmpty() && defaults.isEmpty();
  }

  /**
   * Limits for {@code indexName}: the entity's explicit entry, else {@link #defaults} for any other
   * V2 or semantic entity index, else empty.
   */
  @Nonnull
  public Map<String, String> forIndex(
      @Nonnull IndexConvention indexConvention,
      @Nonnull OperationFingerprint operation,
      @Nonnull String indexName) {
    if (isEmpty()) {
      return Map.of();
    }
    Optional<String> entity;
    if (indexConvention.isSemanticEntityIndexType(indexName)) {
      entity = indexConvention.getEntityNameSemantic(operation, indexName);
    } else if (indexConvention.isV2EntityIndexType(indexName)) {
      entity = indexConvention.getEntityName(operation, indexName);
    } else {
      return Map.of();
    }
    return entity
        .map(name -> byEntity.getOrDefault(name.toLowerCase(Locale.ROOT), defaults))
        .orElse(Map.of());
  }
}
