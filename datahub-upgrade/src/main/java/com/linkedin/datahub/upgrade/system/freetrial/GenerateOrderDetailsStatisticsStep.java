package com.linkedin.datahub.upgrade.system.freetrial;

import static com.linkedin.gms.factory.statistics.OrderDetailsStatisticsGenerator.ORDER_DETAILS_DATASET_URN;
import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.gms.factory.statistics.OrderDetailsStatisticsGenerator;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Upgrade step to generate statistics for the order_details dataset.
 *
 * <p>This step generates both datasetProfile and datasetUsageStatistics for the Snowflake
 * order_details dataset from the free trial sample data. It generates historical and future-dated
 * statistics in one execution, providing a rich demo experience as time passes.
 */
@Slf4j
public class GenerateOrderDetailsStatisticsStep implements UpgradeStep {

  private static final String UPGRADE_ID = "generate-order-details-statistics-v1";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);

  private final OperationContext systemOpContext;
  private final EntityService<?> entityService;
  private final OrderDetailsStatisticsGenerator statisticsGenerator;
  private final boolean reprocessEnabled;
  private final int historicalDays;
  private final int futureDays;

  public GenerateOrderDetailsStatisticsStep(
      OperationContext systemOpContext,
      EntityService<?> entityService,
      OrderDetailsStatisticsGenerator statisticsGenerator,
      boolean reprocessEnabled,
      int historicalDays,
      int futureDays) {
    this.systemOpContext = systemOpContext;
    this.entityService = entityService;
    this.statisticsGenerator = statisticsGenerator;
    this.reprocessEnabled = reprocessEnabled;
    this.historicalDays = historicalDays > 0 ? historicalDays : 30;
    this.futureDays = futureDays > 0 ? futureDays : 30;
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        log.info(
            "Starting generation of order_details statistics: {} days past + {} days future",
            historicalDays,
            futureDays);

        final DateTime now = DateTime.now(DateTimeZone.UTC);
        final DateTime startOfToday = now.withTimeAtStartOfDay();

        // Generate past + today + future (historicalDays + 1 + futureDays total days)
        final DateTime startDate = startOfToday.minusDays(historicalDays);
        final int totalDays = historicalDays + 1 + futureDays;

        statisticsGenerator.generateHistoricalStatistics(
            context.opContext(), startDate.getMillis(), totalDays);

        // Mark upgrade as complete
        BootstrapStep.setUpgradeResult(context.opContext(), UPGRADE_ID_URN, entityService);

        log.info("Successfully generated {} days of order_details statistics", totalDays);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);

      } catch (Exception e) {
        log.error("Failed to generate order_details statistics: {}", e.getMessage(), e);
        // Return FAILED - isOptional()=true ensures this won't block startup
        // The step will retry on next startup since upgrade result is not persisted
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  @Override
  public boolean isOptional() {
    // Stats generation is optional - should not block system startup
    return true;
  }

  @Override
  public boolean skip(UpgradeContext context) {
    // Check if dataset exists (more robust than checking upgrade record)
    try {
      Urn datasetUrn = Urn.createFromString(ORDER_DETAILS_DATASET_URN);
      if (!entityService.exists(systemOpContext, datasetUrn, true)) {
        log.info(
            "order_details dataset not found ({}). Skipping stats generation.",
            ORDER_DETAILS_DATASET_URN);
        return true;
      }
    } catch (Exception e) {
      log.warn("Failed to check if order_details dataset exists: {}", e.getMessage());
      return true;
    }

    if (reprocessEnabled) {
      log.info("GenerateOrderDetailsStatistics reprocess enabled. Running.");
      return false;
    }

    // Check if upgrade already ran
    boolean previouslyRun =
        entityService.exists(
            systemOpContext, UPGRADE_ID_URN, DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);

    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
    }
    return previouslyRun;
  }
}
