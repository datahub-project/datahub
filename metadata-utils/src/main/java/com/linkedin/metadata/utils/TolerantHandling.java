package com.linkedin.metadata.utils;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.RegistryKnowledge;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.SystemMetadata;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Tolerates data the registry doesn't know: skips it, strips references to it, and attributes
 * validation failures to a newer version where it can.
 */
public final class TolerantHandling implements UnknownDataHandling {

  public static final TolerantHandling INSTANCE = new TolerantHandling();

  private TolerantHandling() {}

  @Override
  public boolean admit(
      @Nonnull final UnknownDataGuard site,
      @Nonnull final EntityRegistry registry,
      @Nonnull final Optional<MetricUtils> metricUtils,
      @Nullable final String entityType,
      @Nullable final String aspectName,
      @Nullable final Object subject) {
    return site.admit(registry, metricUtils, entityType, aspectName, subject);
  }

  @Override
  public boolean admitUrn(
      @Nonnull final UnknownDataGuard site,
      @Nonnull final EntityRegistry registry,
      @Nonnull final Optional<MetricUtils> metricUtils,
      @Nullable final String urn,
      @Nullable final String aspectName) {
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
    return site.admitEvent(registry, metricUtils, urn, entityType, aspectName);
  }

  @Override
  public boolean isUnknown(
      @Nonnull final EntityRegistry registry,
      @Nullable final String entityType,
      @Nullable final String aspectName) {
    return RegistryKnowledge.isUnknown(registry, entityType, aspectName);
  }

  @Override
  public boolean isUnknownUrn(
      @Nonnull final EntityRegistry registry,
      @Nullable final String urn,
      @Nullable final String aspectName) {
    return RegistryKnowledge.classifyUrn(registry, urn, aspectName).isUnknown();
  }

  @Override
  public boolean referencesUnknownEntityType(
      @Nonnull final EntityRegistry registry, @Nonnull final Urn urn) {
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
    return registry.findEntitySpec(entityType);
  }

  @Nonnull
  @Override
  public Optional<AspectSpec> aspectSpecForData(
      @Nonnull final EntityRegistry registry,
      @Nonnull final String entityType,
      @Nonnull final String aspectName) {
    return registry.findAspectSpec(entityType, aspectName);
  }

  @Override
  public int stripUnknownReferences(
      @Nonnull final RecordTemplate aspect, @Nonnull final EntityRegistry registry) {
    return UnknownEntityUrnStripper.strip(aspect, registry);
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
}
