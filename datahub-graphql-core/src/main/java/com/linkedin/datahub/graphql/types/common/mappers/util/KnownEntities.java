package com.linkedin.datahub.graphql.types.common.mappers.util;

import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;
import static com.linkedin.metadata.utils.UnknownDataHandlings.handling;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.UnknownDataGuard;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.net.URISyntaxException;
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
    if (!handling().toleratesUnknownEntityAspect()) {
      for (Urn urn : urns) {
        entities.add(UrnToEntityMapper.map(context, urn));
      }
      return entities;
    }
    for (Urn urn : urns) {
      final boolean known = isKnown(context, urn);
      final Entity entity = known ? mapReturnable(context, urn) : null;
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
    if (!handling().toleratesUnknownEntityAspect()) {
      // Throws for no urns, as the callers' get(0) did.
      return UrnToEntityMapper.map(context, new ArrayList<>(urns).get(0));
    }
    for (Urn urn : urns) {
      final boolean known = isKnown(context, urn);
      final Entity entity = known ? mapReturnable(context, urn) : null;
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
    if (!handling().toleratesUnknownEntityAspect()) {
      return true;
    }
    final EntityRegistry registry =
        context == null || context.getOperationContext() == null
            ? null
            : context.getOperationContext().getEntityRegistry();
    return registry == null || !handling().referencesUnknownEntityType(registry, urn);
  }

  /**
   * Under tolerant handling, whether GraphQL can map the urn, as {@link #mapFirst} decides, without
   * logging a skip.
   */
  public static boolean isRepresentable(
      @Nullable final QueryContext context, @Nonnull final Urn urn) {
    return isKnown(context, urn) && mapReturnable(context, urn) != null;
  }

  /**
   * Under tolerant handling, false for a schema field whose parent, the first entity in its urn,
   * GraphQL can't represent, logging the skip: SchemaFieldEntity.parent is non-null, so such a
   * field can't be returned. True for any other urn, and for every urn when unknown data isn't
   * tolerated.
   */
  public static boolean hasRepresentableParent(
      @Nullable final QueryContext context, @Nonnull final Urn urn) {
    if (!handling().toleratesUnknownEntityAspect()
        || !SCHEMA_FIELD_ENTITY_NAME.equals(urn.getEntityType())) {
      return true;
    }
    final Urn parent = schemaFieldParent(urn);
    if (parent == null || isRepresentable(context, parent)) {
      return true;
    }
    UNREPRESENTABLE.skippedBecause(
        skipMetrics(context, isKnown(context, parent)),
        SCHEMA_FIELD_ENTITY_NAME,
        "GraphQL can't represent its parent",
        urn);
    return false;
  }

  /**
   * Under tolerant handling, false for a urn GraphQL can't return as an entity, logging the skip:
   * an entity type it can't map (one only a newer version registered, or one it doesn't model), or
   * a schema field whose parent it can't represent. Use it before a non-null entity slot, such as a
   * search result's. True for every urn when unknown data isn't tolerated.
   */
  public static boolean isReturnable(@Nullable final QueryContext context, @Nonnull final Urn urn) {
    if (!handling().toleratesUnknownEntityAspect()) {
      return true;
    }
    if (!hasRepresentableParent(context, urn)) {
      return false;
    }
    final boolean known = isKnown(context, urn);
    if (known && UrnToEntityMapper.map(context, urn) != null) {
      return true;
    }
    UNREPRESENTABLE.skippedBecause(
        skipMetrics(context, known),
        urn.getEntityType(),
        "GraphQL can't represent its entity type",
        urn);
    return false;
  }

  /**
   * The urn as an entity GraphQL can return, or null. A schema field also needs a parent GraphQL
   * can represent, as the loader returns one without it as missing.
   */
  @Nullable
  private static Entity mapReturnable(
      @Nullable final QueryContext context, @Nonnull final Urn urn) {
    final Entity entity = UrnToEntityMapper.map(context, urn);
    if (entity == null || !SCHEMA_FIELD_ENTITY_NAME.equals(urn.getEntityType())) {
      return entity;
    }
    final Urn parent = schemaFieldParent(urn);
    return parent == null || isRepresentable(context, parent) ? entity : null;
  }

  /** A schema field's parent, the first entity in its urn, or null if the urn can't be read. */
  @Nullable
  private static Urn schemaFieldParent(@Nonnull final Urn schemaFieldUrn) {
    try {
      return Urn.createFromString(schemaFieldUrn.getEntityKey().get(0));
    } catch (URISyntaxException | IndexOutOfBoundsException e) {
      // Not a schema field urn this can read: it maps as it did before.
      return null;
    }
  }
}
