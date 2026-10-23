package com.linkedin.metadata.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.validation.ValidationResult;
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
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Tolerates data a newer version wrote, of the kinds its flags enable; every other kind is handled
 * as {@link StrictHandling} handles it.
 *
 * <ul>
 *   <li>Unknown entity types, aspects and references: skipped, stripped and kept on write. Data
 *       whose own entity type is unknown is tolerated only when that type is a newer version's (see
 *       {@link NextVersionEntityTypes}); any other unknown type, such as a typo, fails as before.
 *       Unknown aspects of known types and references to unknown types are always tolerated.
 *   <li>Unknown enum symbols and union members: a validation failure on nothing else is attributed
 *       to a newer version, and GraphQL falls back instead of failing.
 * </ul>
 *
 * <p>One rule applies under either kind: a stored row stamped with a newer schema version ({@link
 * #isWrittenByNewerSchema}) is the newer version's, since either kind of change can be why it
 * bumped the version, and is left as stored rather than failing.
 */
public final class TolerantHandling implements UnknownDataHandling {

  /** Tolerates both kinds, and every unknown entity type. */
  public static final TolerantHandling INSTANCE =
      new TolerantHandling(true, true, entityType -> true);

  // featureFlags.unknownEntityAspectToleranceEnabled: entity types, aspects and references.
  private final boolean unknownEntityAspectTolerance;
  // featureFlags.unknownEnumToleranceEnabled: enum symbols and union members.
  private final boolean unknownEnumTolerance;
  private final Predicate<String> nextVersionEntityType;

  private TolerantHandling(
      final boolean unknownEntityAspectTolerance,
      final boolean unknownEnumTolerance,
      @Nonnull final Predicate<String> nextVersionEntityType) {
    this.unknownEntityAspectTolerance = unknownEntityAspectTolerance;
    this.unknownEnumTolerance = unknownEnumTolerance;
    this.nextVersionEntityType = nextVersionEntityType;
  }

  /**
   * Tolerates the kinds enabled, and unknown entity types only when {@code nextVersionEntityType}
   * accepts them. At least one kind must be enabled; with neither, use {@link StrictHandling},
   * which looks nothing up and doesn't attribute rows to a newer version.
   */
  @Nonnull
  public static TolerantHandling of(
      final boolean unknownEntityAspectTolerance,
      final boolean unknownEnumTolerance,
      @Nonnull final Predicate<String> nextVersionEntityType) {
    if (!unknownEntityAspectTolerance && !unknownEnumTolerance) {
      throw new IllegalArgumentException(
          "Neither kind of unknown data is tolerated: use StrictHandling");
    }
    return new TolerantHandling(
        unknownEntityAspectTolerance, unknownEnumTolerance, nextVersionEntityType);
  }

  @Override
  public boolean admit(
      @Nonnull final UnknownDataGuard site,
      @Nonnull final EntityRegistry registry,
      @Nonnull final Optional<MetricUtils> metricUtils,
      @Nullable final String entityType,
      @Nullable final String aspectName,
      @Nullable final Object subject) {
    if (!unknownEntityAspectTolerance || isUnregistered(registry, entityType)) {
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
    if (!unknownEntityAspectTolerance || isUnregistered(registry, entityTypeOf(urn))) {
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
    if (!unknownEntityAspectTolerance
        || isUnregistered(registry, entityType)
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
    if (!unknownEntityAspectTolerance || isUnregistered(registry, entityType)) {
      return StrictHandling.INSTANCE.isUnknown(registry, entityType, aspectName);
    }
    return RegistryKnowledge.isUnknown(registry, entityType, aspectName);
  }

  @Override
  public boolean isUnknownUrn(
      @Nonnull final EntityRegistry registry,
      @Nullable final String urn,
      @Nullable final String aspectName) {
    if (!unknownEntityAspectTolerance || isUnregistered(registry, entityTypeOf(urn))) {
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
    if (!unknownEntityAspectTolerance || isUnregistered(registry, urn.getEntityType())) {
      return StrictHandling.INSTANCE.referencesUnknownEntityType(registry, urn);
    }
    return RegistryKnowledge.referencesUnknownEntityType(registry, urn);
  }

  @Override
  public boolean isWrittenByNewerSchema(
      @Nullable final SystemMetadata systemMetadata, @Nullable final AspectSpec aspectSpec) {
    // Either kind can be why a newer version bumped the schema.
    return RegistryKnowledge.isWrittenByNewerSchema(systemMetadata, aspectSpec);
  }

  @Nonnull
  @Override
  public Optional<EntitySpec> entitySpecForData(
      @Nonnull final EntityRegistry registry, @Nonnull final String entityType) {
    if (!unknownEntityAspectTolerance || isUnregistered(registry, entityType)) {
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
    if (!unknownEntityAspectTolerance || isUnregistered(registry, entityType)) {
      return StrictHandling.INSTANCE.aspectSpecForData(registry, entityType, aspectName);
    }
    return registry.findAspectSpec(entityType, aspectName);
  }

  @Override
  public int stripUnknownReferences(
      @Nonnull final RecordTemplate aspect, @Nonnull final EntityRegistry registry) {
    return unknownEntityAspectTolerance ? UnknownEntityUrnStripper.strip(aspect, registry) : 0;
  }

  @Nonnull
  @Override
  public UnknownReferenceMerger.MergeResult mergeStoredReferences(
      @Nonnull final RecordTemplate request,
      @Nonnull final Supplier<RecordTemplate> stored,
      @Nonnull final EntityRegistry registry) {
    return unknownEntityAspectTolerance
        ? UnknownReferenceMerger.merge(request, stored.get(), registry)
        : UnknownReferenceMerger.MergeResult.NONE;
  }

  @Nonnull
  @Override
  public UnknownEntityUrnStripper.StripResult stripReporting(
      @Nonnull final RecordTemplate aspect, @Nonnull final EntityRegistry registry) {
    return unknownEntityAspectTolerance
        ? UnknownEntityUrnStripper.stripReporting(aspect, registry)
        : UnknownEntityUrnStripper.StripResult.NONE;
  }

  @Override
  public boolean aspectReferencesUnknownEntityType(
      @Nonnull final RecordTemplate aspect, @Nonnull final EntityRegistry registry) {
    return unknownEntityAspectTolerance
        && UnknownEntityUrnStripper.referencesUnknownEntityType(aspect, registry);
  }

  @Override
  public boolean isFromNewerVersion(
      @Nullable final RecordTemplate aspect,
      @Nullable final SystemMetadata systemMetadata,
      @Nullable final AspectSpec aspectSpec,
      @Nonnull final EntityRegistry registry) {
    return isWrittenByNewerSchema(systemMetadata, aspectSpec)
        || (aspect != null && aspectReferencesUnknownEntityType(aspect, registry))
        || (unknownEnumTolerance
            && aspect != null
            && UnknownSchemaValues.failsOnlyOnUnknownValues(aspect));
  }

  @Override
  public boolean failsOnlyOnValuesFromNewerVersion(
      @Nullable final ValidationResult validationFailure) {
    return unknownEnumTolerance && UnknownSchemaValues.failsOnlyOnUnknownValues(validationFailure);
  }

  @Override
  public boolean toleratesUnknownEntityAspect() {
    return unknownEntityAspectTolerance;
  }

  @Override
  public boolean toleratesUnknownEnum() {
    return unknownEnumTolerance;
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
