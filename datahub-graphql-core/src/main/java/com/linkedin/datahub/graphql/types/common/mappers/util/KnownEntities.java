package com.linkedin.datahub.graphql.types.common.mappers.util;

import static com.linkedin.metadata.utils.UnknownDataHandlings.handling;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.UnknownDataGuard;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps references to GraphQL entities, leaving out the ones GraphQL can't represent.
 *
 * <p>After a zero-downtime upgrade rollback, data can reference entities of types only a newer
 * version knows. {@link UrnToEntityMapper} maps those to null, and a null inside a non-null field
 * ({@code Entity!}, {@code [Entity!]!}) fails the whole parent object or list. Use these helpers
 * wherever references become entities in a list or a required field, so one unknown reference drops
 * only itself.
 *
 * <p>When unknown data isn't tolerated, nothing is left out or looked up: every urn is mapped as
 * before, including to null.
 */
public final class KnownEntities {

  private static final UnknownDataGuard UNREPRESENTABLE =
      UnknownDataGuard.forSite(KnownEntities.class, "reference");

  private KnownEntities() {}

  /** The entities for these urns, in order, without the ones GraphQL can't represent. */
  @Nonnull
  public static List<Entity> mapAll(
      @Nullable final QueryContext context, @Nonnull final Collection<Urn> urns) {
    final List<Entity> entities = new ArrayList<>(urns.size());
    if (!handling().isTolerant()) {
      for (Urn urn : urns) {
        entities.add(UrnToEntityMapper.map(context, urn));
      }
      return entities;
    }
    for (Urn urn : urns) {
      final boolean known = isKnown(context, urn);
      final Entity entity = known ? UrnToEntityMapper.map(context, urn) : null;
      if (entity == null) {
        UNREPRESENTABLE.skippedBecause(
            skipMetrics(context, known),
            urn.getEntityType(),
            "GraphQL can't represent its entity type",
            urn);
      } else {
        entities.add(entity);
      }
    }
    return entities;
  }

  /** The first of these urns GraphQL can represent, or null if none. */
  @Nullable
  public static Entity mapFirst(
      @Nullable final QueryContext context, @Nonnull final Collection<Urn> urns) {
    if (!handling().isTolerant()) {
      // Throws for no urns, as the callers' get(0) did.
      return UrnToEntityMapper.map(context, new ArrayList<>(urns).get(0));
    }
    for (Urn urn : urns) {
      final boolean known = isKnown(context, urn);
      final Entity entity = known ? UrnToEntityMapper.map(context, urn) : null;
      if (entity != null) {
        return entity;
      }
      UNREPRESENTABLE.skippedBecause(
          skipMetrics(context, known),
          urn.getEntityType(),
          "GraphQL can't represent its entity type",
          urn);
    }
    return null;
  }

  /**
   * Metrics for a reference GraphQL left out. Only references the registry doesn't know count
   * toward {@code unknown_to_registry_skipped}; types it knows but GraphQL doesn't model (e.g.
   * plugin entities) are only logged, so the metric keeps measuring data from a newer version.
   */
  @Nonnull
  public static Optional<MetricUtils> skipMetrics(
      @Nullable final QueryContext context, final boolean knownToRegistry) {
    return knownToRegistry ? Optional.empty() : metrics(context);
  }

  /** Metrics of the request, for reporting skips. */
  @Nonnull
  public static Optional<MetricUtils> metrics(@Nullable final QueryContext context) {
    return context == null || context.getOperationContext() == null
        ? Optional.empty()
        : context.getOperationContext().getMetricUtils();
  }

  /**
   * True unless the urn's entity type, or that of a urn in its key (e.g. the monitored entity in a
   * monitor urn), is not in the registry. Without a registry in the context everything counts as
   * known.
   */
  public static boolean isKnown(@Nullable final QueryContext context, @Nonnull final Urn urn) {
    if (!handling().isTolerant()) {
      return true;
    }
    final EntityRegistry registry =
        context == null || context.getOperationContext() == null
            ? null
            : context.getOperationContext().getEntityRegistry();
    return registry == null || !handling().referencesUnknownEntityType(registry, urn);
  }
}
