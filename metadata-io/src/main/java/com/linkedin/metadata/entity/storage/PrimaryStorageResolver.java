package com.linkedin.metadata.entity.storage;

import com.datastax.oss.driver.api.core.CqlSession;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.PrimaryStorageContext;
import io.datahubproject.metadata.context.ReadPreference;
import io.datahubproject.metadata.context.StorageTarget;
import io.ebean.Database;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PrimaryStorageResolver {

  public static final String METRIC_TARGET_USED = "primary_storage_target_used";
  public static final String METRIC_READ_FALLBACK = "primary_storage_read_fallback_to_primary";

  private final PrimaryStorageRegistry registry;
  private final ReadPreference deployDefaultReadPreference;
  @Nullable private final MetricUtils metricUtils;
  @Nullable private final String storeTag;

  public PrimaryStorageResolver(
      @Nonnull PrimaryStorageRegistry registry,
      @Nonnull ReadPreference deployDefaultReadPreference) {
    this(registry, deployDefaultReadPreference, null, null);
  }

  public PrimaryStorageResolver(
      @Nonnull PrimaryStorageRegistry registry,
      @Nonnull ReadPreference deployDefaultReadPreference,
      @Nullable MetricUtils metricUtils,
      @Nullable String storeTag) {
    this.registry = registry;
    this.deployDefaultReadPreference = deployDefaultReadPreference;
    this.metricUtils = metricUtils;
    this.storeTag = storeTag;
  }

  @Nonnull
  public PrimaryStorageRegistry getRegistry() {
    return registry;
  }

  @Nonnull
  public StorageTarget resolveTarget(@Nullable OperationContext opContext, boolean forUpdate) {
    StorageTarget target = resolveTargetInternal(opContext, forUpdate);
    recordMetrics(opContext, forUpdate, target);
    return target;
  }

  @Nonnull
  private StorageTarget resolveTargetInternal(
      @Nullable OperationContext opContext, boolean forUpdate) {
    if (forUpdate) {
      return StorageTarget.PRIMARY;
    }
    if (opContext == null) {
      return StorageTarget.PRIMARY;
    }
    PrimaryStorageContext storageContext = opContext.getPrimaryStorageContext();
    if (storageContext == null) {
      return StorageTarget.PRIMARY;
    }
    if (storageContext.getStorageTargetOverride().isPresent()) {
      StorageTarget override = storageContext.getStorageTargetOverride().get();
      if (registry.has(override)) {
        return override;
      }
      log.debug("Storage target override {} not registered, falling back to PRIMARY", override);
      return StorageTarget.PRIMARY;
    }
    ReadPreference preference = storageContext.getReadPreference();
    if (preference == ReadPreference.READ && registry.has(StorageTarget.READ)) {
      return StorageTarget.READ;
    }
    if (preference == ReadPreference.READ && !registry.has(StorageTarget.READ)) {
      log.trace("READ preference set but READ pool not registered, using PRIMARY");
    }
    return StorageTarget.PRIMARY;
  }

  private void recordMetrics(
      @Nullable OperationContext opContext, boolean forUpdate, StorageTarget target) {
    if (metricUtils == null || storeTag == null) {
      return;
    }
    metricUtils.incrementMicrometer(
        METRIC_TARGET_USED,
        1,
        "target",
        target.name(),
        "store",
        storeTag,
        "forUpdate",
        String.valueOf(forUpdate));
    if (!forUpdate
        && opContext != null
        && opContext.getPrimaryStorageContext() != null
        && opContext.getPrimaryStorageContext().getReadPreference() == ReadPreference.READ
        && target == StorageTarget.PRIMARY
        && !registry.has(StorageTarget.READ)) {
      metricUtils.incrementMicrometer(METRIC_READ_FALLBACK, 1, "store", storeTag);
    }
  }

  @Nonnull
  public Database resolveEbean(@Nullable OperationContext opContext, boolean forUpdate) {
    StorageTarget target = resolveTarget(opContext, forUpdate);
    return registry.get(target, Database.class);
  }

  @Nonnull
  public CqlSession resolveCassandra(@Nullable OperationContext opContext, boolean forUpdate) {
    StorageTarget target = resolveTarget(opContext, forUpdate);
    return registry.get(target, CqlSession.class);
  }

  @Nonnull
  public Database resolveEbeanPrimary() {
    return registry.get(StorageTarget.PRIMARY, Database.class);
  }

  @Nonnull
  public CqlSession resolveCassandraPrimary() {
    return registry.get(StorageTarget.PRIMARY, CqlSession.class);
  }

  @Nonnull
  public static PrimaryStorageResolver forSingleEbeanDatabase(@Nonnull Database primaryDatabase) {
    PrimaryStorageRegistry registry = new PrimaryStorageRegistry(false);
    registry.register(StorageTarget.PRIMARY, primaryDatabase);
    return new PrimaryStorageResolver(registry, ReadPreference.PRIMARY);
  }

  @Nonnull
  public static PrimaryStorageResolver forSingleCassandraSession(
      @Nonnull CqlSession primarySession) {
    PrimaryStorageRegistry registry = new PrimaryStorageRegistry(false);
    registry.register(StorageTarget.PRIMARY, primarySession);
    return new PrimaryStorageResolver(registry, ReadPreference.PRIMARY);
  }

  @Nonnull
  public static PrimaryStorageContext buildDefaultPrimaryStorageContext(
      @Nonnull PrimaryStorageRegistry registry) {
    ReadPreference preference =
        registry.has(StorageTarget.READ) ? ReadPreference.READ : ReadPreference.PRIMARY;
    return PrimaryStorageContext.builder()
        .readPreference(preference)
        .includeReadPreferenceInEntityCacheKey(registry.isDistinctReadEndpoint())
        .build();
  }
}
