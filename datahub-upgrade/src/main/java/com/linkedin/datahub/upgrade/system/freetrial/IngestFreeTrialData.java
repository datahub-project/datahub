package com.linkedin.datahub.upgrade.system.freetrial;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.gms.factory.statistics.OrderDetailsStatisticsGenerator;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * Non-blocking system upgrade that ingests sample data for free trial instances and generates
 * statistics.
 *
 * <p>This upgrade runs after GMS starts, allowing the instance to come online quickly while sample
 * data is ingested and statistics are generated in the background. Statistics include both
 * historical and future-dated data to provide a rich demo experience.
 */
@Slf4j
public class IngestFreeTrialData implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> steps;

  public IngestFreeTrialData(
      OperationContext opContext,
      EntityService<?> entityService,
      OrderDetailsStatisticsGenerator statisticsGenerator,
      boolean enabled,
      boolean reprocessEnabled,
      int batchSize,
      int historicalDays,
      int futureDays) {
    if (enabled) {
      ImmutableList.Builder<UpgradeStep> stepsBuilder = ImmutableList.builder();

      // Always add the data ingestion step
      stepsBuilder.add(
          new IngestFreeTrialDataStep(
              opContext, entityService, enabled, reprocessEnabled, batchSize));

      // Only add statistics generation if the generator bean is available
      if (statisticsGenerator != null) {
        stepsBuilder.add(
            new GenerateOrderDetailsStatisticsStep(
                opContext,
                entityService,
                statisticsGenerator,
                reprocessEnabled,
                historicalDays,
                futureDays));
      } else {
        log.warn(
            "OrderDetailsStatisticsGenerator bean not available. "
                + "Statistics generation will be skipped. "
                + "This is expected when datahub.freeTrialInstance=false.");
      }

      steps = stepsBuilder.build();
    } else {
      steps = ImmutableList.of();
    }
  }

  @Override
  public String id() {
    return getClass().getSimpleName();
  }

  @Override
  public List<UpgradeStep> steps() {
    return steps;
  }
}
