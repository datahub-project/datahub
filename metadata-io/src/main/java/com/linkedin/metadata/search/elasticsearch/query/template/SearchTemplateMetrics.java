package com.linkedin.metadata.search.elasticsearch.query.template;

import io.datahubproject.metadata.context.OperationContext;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Search-template counters and timers. Tags stay inside a fixed set: shipped template ids collapse
 * to those ids, and every other template is {@code custom}.
 */
public final class SearchTemplateMetrics {
  public static final String SELECTED = "search.template.selected";
  public static final String SERVED = "search.template.served";
  public static final String RENDER_FAILURE = "search.template.render.failure";
  public static final String ROUTE_DEPTH = "search.template.route.depth";
  public static final String LADDER_DEPTH = "search.template.ladder.depth";
  public static final String FALLBACK = "search.template.fallback";
  public static final String RUNG = "search.template.rung";
  public static final String CLAUSE_BUDGET = "search.template.clause.budget.reduction";
  public static final String CACHE_AGE = "search.template.cache.age";
  public static final String CACHE = "search.template.cache";
  public static final String SHADOW = "search.template.shadow";

  public static final String SLOT_CALLER = "caller";

  private static final Set<String> SURFACES =
      Set.of(
          SearchTemplateSurfaces.SEARCH_ENTITIES,
          SearchTemplateSurfaces.SCROLL_ENTITIES,
          SearchTemplateSurfaces.AGGREGATE_ENTITIES);

  private static final Set<String> SLOTS =
      Set.of(
          "default",
          "elasticsearch8",
          "elasticsearch9",
          "opensearch2",
          "opensearch3",
          "postgres",
          SLOT_CALLER);

  private SearchTemplateMetrics() {}

  public static void selected(
      @Nonnull OperationContext opContext, @Nonnull String urn, @Nullable String slot) {
    counter(opContext, SELECTED, "template", templateLabel(urn), "slot", slotLabel(slot));
  }

  /** A shadow render. The template was not executed. */
  public static void shadow(
      @Nonnull OperationContext opContext, @Nonnull String urn, @Nullable String surface) {
    counter(opContext, SHADOW, "template", templateLabel(urn), "surface", surfaceLabel(surface));
  }

  public static void served(@Nonnull OperationContext opContext, @Nonnull String urn) {
    counter(opContext, SERVED, "template", templateLabel(urn));
  }

  public static void renderFailure(@Nonnull OperationContext opContext, @Nonnull String stage) {
    counter(opContext, RENDER_FAILURE, "stage", stage);
  }

  public static void routeDepth(@Nonnull OperationContext opContext, int depth) {
    distribution(opContext, ROUTE_DEPTH, depth);
  }

  public static void ladderDepth(@Nonnull OperationContext opContext, int depth) {
    distribution(opContext, LADDER_DEPTH, depth);
  }

  public static void fallback(@Nonnull OperationContext opContext, @Nonnull String kind) {
    counter(opContext, FALLBACK, "kind", kind);
  }

  public static void rung(
      @Nonnull OperationContext opContext, int depth, boolean parallel, long durationNanos) {
    opContext
        .getMetricUtils()
        .ifPresent(
            metrics ->
                metrics.recordTimer(RUNG, durationNanos, "position", position(depth, parallel)));
  }

  public static void clauseBudget(@Nonnull OperationContext opContext, @Nonnull String kind) {
    counter(opContext, CLAUSE_BUDGET, "kind", kind);
  }

  /** {@code ageMillis} is how long the served merge has been cached. */
  public static void cache(@Nonnull OperationContext opContext, long ageMillis) {
    counter(opContext, CACHE, "outcome", "hit");
    distribution(opContext, CACHE_AGE, Math.max(0, ageMillis));
  }

  @Nonnull
  static String templateLabel(@Nullable String urn) {
    if (urn == null || urn.isBlank()) {
      return "custom";
    }
    try {
      if (!SearchTemplateBootstrap.urns().contains(urn)) {
        return "custom";
      }
    } catch (RuntimeException e) {
      return "custom";
    }
    int colon = urn.lastIndexOf(':');
    String id = colon < 0 ? urn : urn.substring(colon + 1);
    return id.isBlank() ? "custom" : id;
  }

  @Nonnull
  static String surfaceLabel(@Nullable String surface) {
    if (surface != null && SURFACES.contains(surface)) {
      return surface;
    }
    return "other";
  }

  @Nonnull
  static String slotLabel(@Nullable String slot) {
    if (slot != null && SLOTS.contains(slot)) {
      return slot;
    }
    return "other";
  }

  @Nonnull
  private static String position(int depth, boolean parallel) {
    if (parallel) {
      return "parallel";
    }
    if (depth <= 0) {
      return "first";
    }
    if (depth == 1) {
      return "next";
    }
    return "deeper";
  }

  private static void counter(
      @Nonnull OperationContext opContext, @Nonnull String name, @Nonnull String... tags) {
    opContext.getMetricUtils().ifPresent(metrics -> metrics.incrementMicrometer(name, 1, tags));
  }

  private static void distribution(
      @Nonnull OperationContext opContext, @Nonnull String name, long value) {
    opContext.getMetricUtils().ifPresent(metrics -> metrics.recordDistribution(name, value));
  }
}
