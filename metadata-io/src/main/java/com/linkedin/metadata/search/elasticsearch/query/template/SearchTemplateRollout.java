package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.metadata.config.search.SearchTemplateRolloutConfiguration;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.zip.CRC32;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Decides whether a request renders a template, serves it, or stays on the Java V2.5 builder. The
 * bucket is CRC32 of the actor URN and surface, so the same actor stays on one side of a canary.
 */
public final class SearchTemplateRollout {
  public static final int BUCKETS = 100;

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
    return SearchTemplateSurfaces.SCROLL_ENTITIES.equals(surface)
        || SearchTemplateSurfaces.SCROLL_ENTITIES_LINEAGE.equals(surface);
  }

  @Nonnull
  private static Mode modeFor(
      @Nullable SearchTemplateRolloutConfiguration rollout, @Nullable String surface) {
    if (rollout == null) {
      return Mode.ACTIVE;
    }
    String override = surfaceMode(rollout, surface);
    if (override != null && !override.isBlank()) {
      return parseMode(override);
    }
    return parseMode(blankTo(rollout.getMode(), "active"));
  }

  private static int percentFor(
      @Nullable SearchTemplateRolloutConfiguration rollout, @Nullable String surface) {
    if (rollout == null) {
      return BUCKETS;
    }
    int override = surfacePercent(rollout, surface);
    int percent = override >= 0 ? override : rollout.getPercent();
    if (percent < 0 || percent > BUCKETS) {
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
      case SearchTemplateSurfaces.BROWSE_ENTITIES -> rollout.getBrowseEntities();
      case SearchTemplateSurfaces.SEARCH_ENTITIES_LINEAGE -> rollout.getSearchEntitiesLineage();
      case SearchTemplateSurfaces.SCROLL_ENTITIES_LINEAGE -> rollout.getScrollEntitiesLineage();
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
      case SearchTemplateSurfaces.BROWSE_ENTITIES -> rollout.getBrowseEntitiesPercent();
      case SearchTemplateSurfaces.SEARCH_ENTITIES_LINEAGE ->
          rollout.getSearchEntitiesLineagePercent();
      case SearchTemplateSurfaces.SCROLL_ENTITIES_LINEAGE ->
          rollout.getScrollEntitiesLineagePercent();
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
