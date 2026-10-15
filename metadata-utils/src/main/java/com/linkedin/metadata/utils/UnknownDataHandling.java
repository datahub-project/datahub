package com.linkedin.metadata.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.SystemMetadata;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * How this process treats data its entity registry doesn't know: entity types, aspects and
 * references written by a newer version and read after a zero-downtime upgrade rollback.
 *
 * <p>Every behavior that differs between tolerating such data and failing on it goes through this
 * interface, so one implementation is chosen per process ({@link UnknownDataHandlings}) instead of
 * each call site checking a flag. {@link TolerantHandling} skips and strips; a strict
 * implementation keeps the behavior from before this tolerance existed.
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
   * so validation still reports it).
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

  /** True if the aspect references an entity type the registry doesn't know, anywhere in it. */
  boolean aspectReferencesUnknownEntityType(
      @Nonnull RecordTemplate aspect, @Nonnull EntityRegistry registry);

  /** True when an aspect that fails validation can be attributed to a newer version. */
  boolean isFromNewerVersion(
      @Nullable RecordTemplate aspect,
      @Nullable SystemMetadata systemMetadata,
      @Nullable AspectSpec aspectSpec,
      @Nonnull EntityRegistry registry);

  /** For one-off branches that don't fit a method above: true when unknown data is tolerated. */
  boolean isTolerant();
}
