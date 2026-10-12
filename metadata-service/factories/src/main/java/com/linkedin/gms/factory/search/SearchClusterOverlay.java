package com.linkedin.gms.factory.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Applies a sparse per-cluster overlay onto a shared operational config block.
 *
 * <p>Overlays are field-level so a deployment can change one key — a heap threshold, a refresh
 * policy — without restating the whole block. The merge is always {@code shared defaults + this
 * cluster's overlay}; one cluster's overlay is never visible to another.
 */
@Slf4j
public final class SearchClusterOverlay {

  private SearchClusterOverlay() {}

  /**
   * Returns {@code defaults} with {@code overlay} applied on top, leaving {@code defaults}
   * untouched. A null or empty overlay returns the defaults unchanged.
   */
  @SuppressWarnings("unchecked")
  public static <T> T apply(
      @Nonnull ObjectMapper objectMapper,
      @Nonnull T defaults,
      @Nullable Map<String, Object> overlay) {
    if (overlay == null || overlay.isEmpty()) {
      return defaults;
    }
    Map<String, Object> merged =
        objectMapper.convertValue(
            defaults,
            objectMapper
                .getTypeFactory()
                .constructMapType(java.util.LinkedHashMap.class, String.class, Object.class));
    Map<String, Object> knownOverlay = new java.util.LinkedHashMap<>();
    for (Map.Entry<String, Object> entry : overlay.entrySet()) {
      if (!merged.containsKey(entry.getKey())) {
        log.warn(
            "Ignoring unknown search cluster overlay key '{}' for {}; it is not a property of that config bean",
            entry.getKey(),
            defaults.getClass().getSimpleName());
        continue;
      }
      knownOverlay.put(entry.getKey(), entry.getValue());
    }
    merged.putAll(knownOverlay);
    return (T) objectMapper.convertValue(merged, defaults.getClass());
  }
}
