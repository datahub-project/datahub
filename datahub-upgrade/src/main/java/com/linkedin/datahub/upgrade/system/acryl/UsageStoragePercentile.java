package com.linkedin.datahub.upgrade.system.acryl;

import static com.linkedin.metadata.Constants.STORAGE_FEATURES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.USAGE_FEATURES_ASPECT_NAME;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.AbstractMCLStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

/**
 * A job that reindexes all usageFeatures, storageFeatures aspects. This is required to support
 * search ranking with percentiles.
 */
@Slf4j
public class UsageStoragePercentile implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public UsageStoragePercentile(
      OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      boolean enabled,
      Integer batchSize,
      Integer batchDelayMs,
      Integer limit) {
    if (enabled) {
      _steps =
          ImmutableList.of(
              new UsagePercentileStep(
                  opContext, entityService, aspectDao, batchSize, batchDelayMs, limit),
              new StoragePercentileStep(
                  opContext, entityService, aspectDao, batchSize, batchDelayMs, limit));
    } else {
      _steps = ImmutableList.of();
    }
  }

  @Override
  public String id() {
    return this.getClass().getName();
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }

  @Slf4j
  public static class UsagePercentileStep extends AbstractMCLStep {

    public UsagePercentileStep(
        OperationContext opContext,
        EntityService<?> entityService,
        AspectDao aspectDao,
        Integer batchSize,
        Integer batchDelayMs,
        Integer limit) {
      super(opContext, entityService, aspectDao, batchSize, batchDelayMs, limit);
    }

    @Override
    public String id() {
      return "usage-percentile-v1";
    }

    @Nonnull
    @Override
    protected String getAspectName() {
      return USAGE_FEATURES_ASPECT_NAME;
    }

    @Nullable
    @Override
    protected String getUrnLike() {
      return null;
    }
  }

  @Slf4j
  public static class StoragePercentileStep extends AbstractMCLStep {

    public StoragePercentileStep(
        OperationContext opContext,
        EntityService<?> entityService,
        AspectDao aspectDao,
        Integer batchSize,
        Integer batchDelayMs,
        Integer limit) {
      super(opContext, entityService, aspectDao, batchSize, batchDelayMs, limit);
    }

    @Override
    public String id() {
      return "storage-percentile-v1";
    }

    @Nonnull
    @Override
    protected String getAspectName() {
      return STORAGE_FEATURES_ASPECT_NAME;
    }

    @Nullable
    @Override
    protected String getUrnLike() {
      return null;
    }
  }
}
