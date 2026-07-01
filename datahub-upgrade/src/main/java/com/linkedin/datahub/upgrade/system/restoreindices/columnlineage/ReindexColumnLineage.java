package com.linkedin.datahub.upgrade.system.restoreindices.columnlineage;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Reindexes the aspects that back column-level lineage: dataset upstreamLineage plus chart and
 * dashboard inputFields.
 */
@Slf4j
public class ReindexColumnLineage implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public ReindexColumnLineage(
      @Nonnull OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      boolean enabled,
      Integer batchSize,
      Integer batchDelayMs,
      Integer limit) {
    if (enabled) {
      _steps =
          ImmutableList.of(
              new ReindexDatasetUpstreamLineageStep(
                  opContext, entityService, aspectDao, batchSize, batchDelayMs, limit),
              new ReindexChartInputFieldsStep(
                  opContext, entityService, aspectDao, batchSize, batchDelayMs, limit),
              new ReindexDashboardInputFieldsStep(
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
}
