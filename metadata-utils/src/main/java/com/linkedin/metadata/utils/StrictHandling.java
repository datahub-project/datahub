package com.linkedin.metadata.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.validation.ValidationResult;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.SystemMetadata;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The behavior from before unknown data was tolerated: nothing is skipped or stripped, nothing is
 * looked up to decide, and data the registry doesn't know fails where it is resolved. Used when
 * both {@code featureFlags.unknownEntityAspectToleranceEnabled} and {@code
 * featureFlags.unknownEnumToleranceEnabled} are off.
 */
public final class StrictHandling implements UnknownDataHandling {

  public static final StrictHandling INSTANCE = new StrictHandling();

  private StrictHandling() {}

  @Override
  public boolean admit(
      @Nonnull final UnknownDataGuard site,
      @Nonnull final EntityRegistry registry,
      @Nonnull final Optional<MetricUtils> metricUtils,
      @Nullable final String entityType,
      @Nullable final String aspectName,
      @Nullable final Object subject) {
    return true;
  }

  @Override
  public boolean admitUrn(
      @Nonnull final UnknownDataGuard site,
      @Nonnull final EntityRegistry registry,
      @Nonnull final Optional<MetricUtils> metricUtils,
      @Nullable final String urn,
      @Nullable final String aspectName) {
    return true;
  }

  @Override
  public boolean admitEvent(
      @Nonnull final UnknownDataGuard site,
      @Nonnull final EntityRegistry registry,
      @Nonnull final Optional<MetricUtils> metricUtils,
      @Nullable final Urn urn,
      @Nullable final String entityType,
      @Nullable final String aspectName) {
    return true;
  }

  @Override
  public boolean isUnknown(
      @Nonnull final EntityRegistry registry,
      @Nullable final String entityType,
      @Nullable final String aspectName) {
    return false;
  }

  @Override
  public boolean isUnknownUrn(
      @Nonnull final EntityRegistry registry,
      @Nullable final String urn,
      @Nullable final String aspectName) {
    return false;
  }

  @Override
  public boolean referencesUnknownEntityType(
      @Nonnull final EntityRegistry registry, @Nonnull final Urn urn) {
    return false;
  }

  @Override
  public boolean isWrittenByNewerSchema(
      @Nullable final SystemMetadata systemMetadata, @Nullable final AspectSpec aspectSpec) {
    return false;
  }

  /** Throws for an entity type the registry doesn't know, as {@code getEntitySpec} always did. */
  @Nonnull
  @Override
  public Optional<EntitySpec> entitySpecForData(
      @Nonnull final EntityRegistry registry, @Nonnull final String entityType) {
    return Optional.ofNullable(registry.getEntitySpec(entityType));
  }

  /**
   * Throws for an entity type or aspect the registry doesn't know. Before tolerance an unknown
   * aspect usually surfaced later as a null spec; failing here has the same outcome without keeping
   * that null in the callers.
   */
  @Nonnull
  @Override
  public Optional<AspectSpec> aspectSpecForData(
      @Nonnull final EntityRegistry registry,
      @Nonnull final String entityType,
      @Nonnull final String aspectName) {
    final EntitySpec entitySpec = registry.getEntitySpec(entityType);
    final AspectSpec aspectSpec = entitySpec == null ? null : entitySpec.getAspectSpec(aspectName);
    if (aspectSpec == null) {
      throw new IllegalArgumentException(
          String.format("Unknown aspect %s for entity %s", aspectName, entityType));
    }
    return Optional.of(aspectSpec);
  }

  @Override
  public int stripUnknownReferences(
      @Nonnull final RecordTemplate aspect, @Nonnull final EntityRegistry registry) {
    return 0;
  }

  @Nonnull
  @Override
  public UnknownReferenceMerger.MergeResult mergeStoredReferences(
      @Nonnull final RecordTemplate request,
      @Nonnull final Supplier<RecordTemplate> stored,
      @Nonnull final EntityRegistry registry) {
    return UnknownReferenceMerger.MergeResult.NONE;
  }

  @Nonnull
  @Override
  public UnknownEntityUrnStripper.StripResult stripReporting(
      @Nonnull final RecordTemplate aspect, @Nonnull final EntityRegistry registry) {
    return UnknownEntityUrnStripper.StripResult.NONE;
  }

  @Override
  public boolean aspectReferencesUnknownEntityType(
      @Nonnull final RecordTemplate aspect, @Nonnull final EntityRegistry registry) {
    return false;
  }

  @Override
  public boolean failsOnlyOnValuesFromNewerVersion(
      @Nullable final ValidationResult validationFailure) {
    return false;
  }

  @Override
  public boolean isFromNewerVersion(
      @Nullable final RecordTemplate aspect,
      @Nullable final SystemMetadata systemMetadata,
      @Nullable final AspectSpec aspectSpec,
      @Nonnull final EntityRegistry registry) {
    return false;
  }

  @Override
  public boolean toleratesUnknownEntityAspect() {
    return false;
  }

  @Override
  public boolean toleratesUnknownEnum() {
    return false;
  }
}
