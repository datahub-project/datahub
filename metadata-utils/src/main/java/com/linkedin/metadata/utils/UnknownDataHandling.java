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
 * How this process treats data a newer version wrote, read after a zero-downtime upgrade rollback:
 * entity types, aspects and references its entity registry doesn't know, and enum symbols or union
 * members its schemas don't define.
 *
 * <p>One implementation is chosen per process ({@link UnknownDataHandlings}). Shared behavior (what
 * to admit, strip or merge) goes through the methods below; call sites whose tolerant path differs
 * in a way no method covers branch on {@link #toleratesUnknownEntityAspect()}, {@link
 * #toleratesUnknownEnum()} or {@link #toleratesUnknownData()}. The two kinds are tolerated
 * separately ({@code featureFlags.unknownEntityAspectToleranceEnabled} and {@code
 * featureFlags.unknownEnumToleranceEnabled}, both defaulting to env {@code
 * UNKNOWN_DATA_TOLERANCE_ENABLED}, which turns on both for a rollback): {@link TolerantHandling}
 * skips and strips what its flags tolerate; {@link StrictHandling}, with both off, keeps the
 * behavior from before this tolerance existed.
 */
public interface UnknownDataHandling {

  /** True when data naming this entity type and (optional) aspect can be processed. */
  boolean admit(
      @Nonnull UnknownDataGuard site,
      @Nonnull EntityRegistry registry,
      @Nonnull Optional<MetricUtils> metricUtils,
      @Nullable String entityType,
      @Nullable String aspectName,
      @Nullable Object subject);

  /** As {@link #admit}, for a urn string that may be null or unparseable. */
  boolean admitUrn(
      @Nonnull UnknownDataGuard site,
      @Nonnull EntityRegistry registry,
      @Nonnull Optional<MetricUtils> metricUtils,
      @Nullable String urn,
      @Nullable String aspectName);

  /** As {@link #admit}, also checking entity types referenced in the urn's key. */
  boolean admitEvent(
      @Nonnull UnknownDataGuard site,
      @Nonnull EntityRegistry registry,
      @Nonnull Optional<MetricUtils> metricUtils,
      @Nullable Urn urn,
      @Nullable String entityType,
      @Nullable String aspectName);

  /** True when the entity type and aspect are unknown to the registry (not malformed). */
  boolean isUnknown(
      @Nonnull EntityRegistry registry, @Nullable String entityType, @Nullable String aspectName);

  /**
   * As {@link #isUnknown}, for a urn string that may be null or unparseable (which is not unknown,
   * so validation still reports it). A urn whose key nests an unknown entity type is unknown too,
   * matching what {@link #admitUrn} skips.
   */
  boolean isUnknownUrn(
      @Nonnull EntityRegistry registry, @Nullable String urn, @Nullable String aspectName);

  /** True when the urn's entity type, or one referenced in its key, is unknown to the registry. */
  boolean referencesUnknownEntityType(@Nonnull EntityRegistry registry, @Nonnull Urn urn);

  /** True when a stored aspect was written under a newer schema version than this registry's. */
  boolean isWrittenByNewerSchema(
      @Nullable SystemMetadata systemMetadata, @Nullable AspectSpec aspectSpec);

  /** The spec of an entity type named by data, for code that skips data it can't resolve. */
  @Nonnull
  Optional<EntitySpec> entitySpecForData(
      @Nonnull EntityRegistry registry, @Nonnull String entityType);

  /** As {@link #entitySpecForData}, for an aspect of an entity type. */
  @Nonnull
  Optional<AspectSpec> aspectSpecForData(
      @Nonnull EntityRegistry registry, @Nonnull String entityType, @Nonnull String aspectName);

  /**
   * Removes references to entity types the registry doesn't know from the aspect, in place.
   *
   * @return the number of elements removed
   */
  int stripUnknownReferences(@Nonnull RecordTemplate aspect, @Nonnull EntityRegistry registry);

  /**
   * As {@link #stripUnknownReferences}, also reporting whether the aspect referenced an unknown
   * entity type before stripping.
   */
  @Nonnull
  UnknownEntityUrnStripper.StripResult stripReporting(
      @Nonnull RecordTemplate aspect, @Nonnull EntityRegistry registry);

  /**
   * Keeps, in a written aspect, the stored references to entity types the registry doesn't know
   * (see {@link UnknownReferenceMerger}), and reports the ones the write adds, which the caller
   * rejects.
   *
   * @param stored supplies the stored aspect as written, before any read dropped references, or
   *     null if none. Reading it parses the stored row, so it is called only when unknown entity
   *     types, aspects and references are tolerated.
   */
  @Nonnull
  UnknownReferenceMerger.MergeResult mergeStoredReferences(
      @Nonnull RecordTemplate request,
      @Nonnull Supplier<RecordTemplate> stored,
      @Nonnull EntityRegistry registry);

  /** True if the aspect references an entity type the registry doesn't know, anywhere in it. */
  boolean aspectReferencesUnknownEntityType(
      @Nonnull RecordTemplate aspect, @Nonnull EntityRegistry registry);

  /**
   * True when a validation failure is only on enum symbols or union members this version doesn't
   * define, which a newer version added (see {@link UnknownSchemaValues}).
   */
  boolean failsOnlyOnValuesFromNewerVersion(@Nullable ValidationResult validationFailure);

  /** True when an aspect that fails validation can be attributed to a newer version. */
  boolean isFromNewerVersion(
      @Nullable RecordTemplate aspect,
      @Nullable SystemMetadata systemMetadata,
      @Nullable AspectSpec aspectSpec,
      @Nonnull EntityRegistry registry);

  /**
   * For one-off branches that don't fit a method above: true when entity types, aspects and
   * references the registry doesn't know are tolerated ({@code
   * featureFlags.unknownEntityAspectToleranceEnabled}).
   */
  boolean toleratesUnknownEntityAspect();

  /**
   * For one-off branches that don't fit a method above: true when enum symbols and union members a
   * newer version added are tolerated ({@code featureFlags.unknownEnumToleranceEnabled}).
   */
  boolean toleratesUnknownEnum();

  /**
   * For one-off branches about a stored row a newer version wrote, whatever it holds: true when
   * either kind of unknown data is tolerated (both default to env {@code
   * UNKNOWN_DATA_TOLERANCE_ENABLED}).
   */
  default boolean toleratesUnknownData() {
    return toleratesUnknownEntityAspect() || toleratesUnknownEnum();
  }
}
