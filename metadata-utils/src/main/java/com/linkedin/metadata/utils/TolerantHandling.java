package com.linkedin.metadata.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.RegistryKnowledge;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.SystemMetadata;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Tolerates data the registry doesn't know: skips it, strips references to it, and attributes
 * validation failures to a newer version where it can.
 *
 * <p>Data whose own entity type is unknown is tolerated only when that type is a newer version's
 * (see {@link NextVersionEntityTypes}); any other unknown type, such as a typo, is handled as
 * {@link StrictHandling} handles it. Unknown aspects of known types and references to unknown types
 * are always tolerated.
 */
public final class TolerantHandling implements UnknownDataHandling {

  /** Tolerates every unknown entity type. */
  public static final TolerantHandling INSTANCE = new TolerantHandling(entityType -> true);

  private final Predicate<String> nextVersionEntityType;

  private TolerantHandling(@Nonnull final Predicate<String> nextVersionEntityType) {
    this.nextVersionEntityType = nextVersionEntityType;
  }

  /** Tolerates unknown entity types only when {@code nextVersionEntityType} accepts them. */
  @Nonnull
  public static TolerantHandling forNextVersionEntityTypes(
      @Nonnull final Predicate<String> nextVersionEntityType) {
    return new TolerantHandling(nextVersionEntityType);
  }

  @Override
  public boolean admit(
      @Nonnull final UnknownDataGuard site,
      @Nonnull final EntityRegistry registry,
      @Nonnull final Optional<MetricUtils> metricUtils,
      @Nullable final String entityType,
      @Nullable final String aspectName,
      @Nullable final Object subject) {
    if (isUnregistered(registry, entityType)) {
      return StrictHandling.INSTANCE.admit(
          site, registry, metricUtils, entityType, aspectName, subject);
    }
    return site.admit(registry, metricUtils, entityType, aspectName, subject);
  }

  @Override
  public boolean admitUrn(
      @Nonnull final UnknownDataGuard site,
      @Nonnull final EntityRegistry registry,
      @Nonnull final Optional<MetricUtils> metricUtils,
      @Nullable final String urn,
      @Nullable final String aspectName) {
    if (isUnregistered(registry, entityTypeOf(urn))) {
      return StrictHandling.INSTANCE.admitUrn(site, registry, metricUtils, urn, aspectName);
    }
    return site.admitUrn(registry, metricUtils, urn, aspectName);
  }

  @Override
  public boolean admitEvent(
      @Nonnull final UnknownDataGuard site,
      @Nonnull final EntityRegistry registry,
      @Nonnull final Optional<MetricUtils> metricUtils,
      @Nullable final Urn urn,
      @Nullable final String entityType,
      @Nullable final String aspectName) {
    if (isUnregistered(registry, entityType)
        || (urn != null && isUnregistered(registry, urn.getEntityType()))) {
      return StrictHandling.INSTANCE.admitEvent(
          site, registry, metricUtils, urn, entityType, aspectName);
    }
    return site.admitEvent(registry, metricUtils, urn, entityType, aspectName);
  }

  @Override
  public boolean isUnknown(
      @Nonnull final EntityRegistry registry,
      @Nullable final String entityType,
      @Nullable final String aspectName) {
    if (isUnregistered(registry, entityType)) {
      return StrictHandling.INSTANCE.isUnknown(registry, entityType, aspectName);
    }
    return RegistryKnowledge.isUnknown(registry, entityType, aspectName);
  }

  @Override
  public boolean isUnknownUrn(
      @Nonnull final EntityRegistry registry,
      @Nullable final String urn,
      @Nullable final String aspectName) {
    if (isUnregistered(registry, entityTypeOf(urn))) {
      return StrictHandling.INSTANCE.isUnknownUrn(registry, urn, aspectName);
    }
    if (RegistryKnowledge.classifyUrn(registry, urn, aspectName).isUnknown()) {
      return true;
    }
    // Same rule as admitUrn, which also skips a row whose urn key nests an unknown entity type.
    try {
      return RegistryKnowledge.referencesUnknownEntityType(registry, Urn.createFromString(urn));
    } catch (URISyntaxException | IllegalArgumentException e) {
      return false;
    }
  }

  @Override
  public boolean referencesUnknownEntityType(
      @Nonnull final EntityRegistry registry, @Nonnull final Urn urn) {
    if (isUnregistered(registry, urn.getEntityType())) {
      return StrictHandling.INSTANCE.referencesUnknownEntityType(registry, urn);
    }
    return RegistryKnowledge.referencesUnknownEntityType(registry, urn);
  }

  @Override
  public boolean isWrittenByNewerSchema(
      @Nullable final SystemMetadata systemMetadata, @Nullable final AspectSpec aspectSpec) {
    return RegistryKnowledge.isWrittenByNewerSchema(systemMetadata, aspectSpec);
  }

  @Nonnull
  @Override
  public Optional<EntitySpec> entitySpecForData(
      @Nonnull final EntityRegistry registry, @Nonnull final String entityType) {
    if (isUnregistered(registry, entityType)) {
      return StrictHandling.INSTANCE.entitySpecForData(registry, entityType);
    }
    return registry.findEntitySpec(entityType);
  }

  @Nonnull
  @Override
  public Optional<AspectSpec> aspectSpecForData(
      @Nonnull final EntityRegistry registry,
      @Nonnull final String entityType,
      @Nonnull final String aspectName) {
    if (isUnregistered(registry, entityType)) {
      return StrictHandling.INSTANCE.aspectSpecForData(registry, entityType, aspectName);
    }
    return registry.findAspectSpec(entityType, aspectName);
  }

  @Override
  public int stripUnknownReferences(
      @Nonnull final RecordTemplate aspect, @Nonnull final EntityRegistry registry) {
    return UnknownEntityUrnStripper.strip(aspect, registry);
  }

  @Nonnull
  @Override
  public UnknownReferenceMerger.MergeResult mergeStoredReferences(
      @Nonnull final RecordTemplate request,
      @Nullable final RecordTemplate stored,
      @Nonnull final EntityRegistry registry) {
    return UnknownReferenceMerger.merge(request, stored, registry);
  }

  @Nonnull
  @Override
  public UnknownEntityUrnStripper.StripResult stripReporting(
      @Nonnull final RecordTemplate aspect, @Nonnull final EntityRegistry registry) {
    return UnknownEntityUrnStripper.stripReporting(aspect, registry);
  }

  @Override
  public boolean aspectReferencesUnknownEntityType(
      @Nonnull final RecordTemplate aspect, @Nonnull final EntityRegistry registry) {
    return UnknownEntityUrnStripper.referencesUnknownEntityType(aspect, registry);
  }

  @Override
  public boolean isFromNewerVersion(
      @Nullable final RecordTemplate aspect,
      @Nullable final SystemMetadata systemMetadata,
      @Nullable final AspectSpec aspectSpec,
      @Nonnull final EntityRegistry registry) {
    return UnknownEntityUrnStripper.isFromNewerVersion(
        aspect, systemMetadata, aspectSpec, registry);
  }

  @Override
  public boolean isTolerant() {
    return true;
  }

  /** An entity type the registry doesn't know and no newer version registered. */
  private boolean isUnregistered(
      @Nonnull final EntityRegistry registry, @Nullable final String entityType) {
    return entityType != null
        && registry.findEntitySpec(entityType).isEmpty()
        && !nextVersionEntityType.test(entityType);
  }

  @Nullable
  private static String entityTypeOf(@Nullable final String urn) {
    if (urn == null) {
      return null;
    }
    try {
      return Urn.createFromString(urn).getEntityType();
    } catch (URISyntaxException | IllegalArgumentException e) {
      return null;
    }
  }
}
