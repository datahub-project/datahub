package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.metadata.config.search.SearchTemplateRolloutConfiguration;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CRC32;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Decides whether a request renders a template, serves it, or stays on the Java V2.5 builder. The
 * bucket is CRC32 of the actor URN and surface, so the same actor stays on one side of a canary.
 */
@Slf4j
public final class SearchTemplateRollout {
  public static final int BUCKETS = 100;

  // Every request reads these settings, so a malformed semantic search mode or percent is logged
  // once per value
  private static final Set<String> MALFORMED_SEMANTIC_SEARCH_SETTINGS =
      ConcurrentHashMap.newKeySet();

  public enum Effect {
    /** Java V2.5 builder. No template render and no template execution. */
    OFF,
    /** Render the template for telemetry. Do not execute it. */
    SHADOW,
    /** Serve the rendered template. */
    ACTIVE
  }

  private SearchTemplateRollout() {}

  @Nonnull
  public static Effect effect(
      boolean templatesEnabled,
      @Nullable SearchTemplateRolloutConfiguration rollout,
      @Nullable String surface,
      @Nonnull String bucketKey) {
    if (!templatesEnabled) {
      return Effect.OFF;
    }
    Mode mode = modeFor(rollout, surface);
    if (mode == Mode.OFF) {
      return Effect.OFF;
    }
    if (mode == Mode.SHADOW) {
      return Effect.SHADOW;
    }
    int percent = percentFor(rollout, surface);
    if (percent >= BUCKETS) {
      return Effect.ACTIVE;
    }
    if (percent <= 0) {
      return Effect.OFF;
    }
    return bucket(bucketKey) < percent ? Effect.ACTIVE : Effect.OFF;
  }

  /** Stable 0–99 bucket. {@link String#hashCode()} is not used; it is not a rollout contract. */
  public static int bucket(@Nonnull String key) {
    CRC32 crc = new CRC32();
    byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
    crc.update(bytes, 0, bytes.length);
    return (int) (crc.getValue() % BUCKETS);
  }

  @Nonnull
  public static String bucketKey(@Nullable String actorUrn, @Nullable String surface) {
    String actor = actorUrn == null || actorUrn.isBlank() ? "anonymous" : actorUrn;
    String where = surface == null || surface.isBlank() ? "default" : surface;
    return actor + "\n" + where;
  }

  public static boolean scrollSurface(@Nullable String surface) {
    return SearchTemplateSurfaces.SCROLL_ENTITIES.equals(surface);
  }

  @Nonnull
  private static Mode modeFor(
      @Nullable SearchTemplateRolloutConfiguration rollout, @Nullable String surface) {
    String override = rollout == null ? null : surfaceMode(rollout, surface);
    if (override != null && !override.isBlank()) {
      return SearchTemplateSurfaces.SEMANTIC_SEARCH.equals(surface)
          ? semanticSearchMode(override)
          : parseMode(override);
    }
    // Semantic search stays off until its own setting turns it on. It never inherits mode, so a
    // configuration without the setting, or none at all, keeps it off.
    if (SearchTemplateSurfaces.SEMANTIC_SEARCH.equals(surface)) {
      return Mode.OFF;
    }
    if (rollout == null) {
      return Mode.ACTIVE;
    }
    return parseMode(blankTo(rollout.getMode(), "active"));
  }

  private static int percentFor(
      @Nullable SearchTemplateRolloutConfiguration rollout, @Nullable String surface) {
    if (rollout == null) {
      return BUCKETS;
    }
    int override = surfacePercent(rollout, surface);
    // Semantic search does not inherit percent either: once set, it serves every actor by default.
    // Only -1 leaves its percent unset, so a lower one reaches the malformed check below
    int percent =
        override >= 0
            ? override
            : SearchTemplateSurfaces.SEMANTIC_SEARCH.equals(surface)
                ? (override == -1 ? BUCKETS : override)
                : rollout.getPercent();
    if (percent < 0 || percent > BUCKETS) {
      // As with a malformed mode, a bad semantic search percent keeps the surface off instead of
      // failing every semantic search
      if (SearchTemplateSurfaces.SEMANTIC_SEARCH.equals(surface)) {
        if (MALFORMED_SEMANTIC_SEARCH_SETTINGS.add("percent " + percent)) {
          log.warn(
              "The semantic search surface's template rollout percent is {}, not from 0 to 100, so"
                  + " semantic search stays on its own kNN search",
              percent);
        }
        return 0;
      }
      throw new IllegalArgumentException(
          "Search template rollout percent must be from 0 to 100: " + percent);
    }
    return percent;
  }

  @Nullable
  private static String surfaceMode(
      @Nonnull SearchTemplateRolloutConfiguration rollout, @Nullable String surface) {
    if (surface == null) {
      return null;
    }
    return switch (surface) {
      case SearchTemplateSurfaces.SEARCH_ENTITIES -> rollout.getSearchEntities();
      case SearchTemplateSurfaces.SCROLL_ENTITIES -> rollout.getScrollEntities();
      case SearchTemplateSurfaces.AGGREGATE_ENTITIES -> rollout.getAggregateEntities();
      case SearchTemplateSurfaces.SEMANTIC_SEARCH -> rollout.getSemanticSearch();
      default -> null;
    };
  }

  private static int surfacePercent(
      @Nonnull SearchTemplateRolloutConfiguration rollout, @Nullable String surface) {
    if (surface == null) {
      return -1;
    }
    return switch (surface) {
      case SearchTemplateSurfaces.SEARCH_ENTITIES -> rollout.getSearchEntitiesPercent();
      case SearchTemplateSurfaces.SCROLL_ENTITIES -> rollout.getScrollEntitiesPercent();
      case SearchTemplateSurfaces.AGGREGATE_ENTITIES -> rollout.getAggregateEntitiesPercent();
      case SearchTemplateSurfaces.SEMANTIC_SEARCH -> rollout.getSemanticSearchPercent();
      default -> -1;
    };
  }

  @Nonnull
  private static Mode parseMode(@Nonnull String raw) {
    String mode = raw.trim().toLowerCase(Locale.ROOT).replace('_', '-');
    return switch (mode) {
      case "off" -> Mode.OFF;
      case "shadow", "render-only" -> Mode.SHADOW;
      case "active", "canary" -> Mode.ACTIVE;
      default ->
          throw new IllegalArgumentException(
              "Search template rollout mode must be off, shadow, or active: " + raw);
    };
  }

  /**
   * Semantic search's own setting. A malformed value, such as {@code on}, keeps semantic search on
   * its own kNN search, as a blank one does, instead of failing every semantic search. Each such
   * value is logged once.
   */
  @Nonnull
  private static Mode semanticSearchMode(@Nonnull String raw) {
    try {
      return parseMode(raw);
    } catch (IllegalArgumentException e) {
      if (MALFORMED_SEMANTIC_SEARCH_SETTINGS.add("mode " + raw)) {
        log.warn(
            "The semantic search surface's template rollout mode is '{}', not off, shadow, or"
                + " active, so semantic search stays on its own kNN search",
            raw);
      }
      return Mode.OFF;
    }
  }

  @Nonnull
  private static String blankTo(@Nullable String value, @Nonnull String fallback) {
    return value == null || value.isBlank() ? fallback : value;
  }

  private enum Mode {
    OFF,
    SHADOW,
    ACTIVE
  }
}
