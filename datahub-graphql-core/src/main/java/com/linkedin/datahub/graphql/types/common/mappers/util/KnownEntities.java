package com.linkedin.datahub.graphql.types.common.mappers.util;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.RegistryKnowledge;
import com.linkedin.metadata.utils.UnknownDataGuard;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
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
    for (Urn urn : urns) {
      final Entity entity = isKnown(context, urn) ? UrnToEntityMapper.map(context, urn) : null;
      if (entity == null) {
        UNREPRESENTABLE.skippedBecause(
            metrics(context), urn.getEntityType(), "GraphQL can't represent its entity type", urn);
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
    return urns.stream()
        .map(urn -> UrnToEntityMapper.map(context, urn))
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(null);
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
    final EntityRegistry registry =
        context == null || context.getOperationContext() == null
            ? null
            : context.getOperationContext().getEntityRegistry();
    return registry == null || !RegistryKnowledge.referencesUnknownEntityType(registry, urn);
  }
}
