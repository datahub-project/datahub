package com.linkedin.datahub.upgrade.applyretention;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.nocode.NoCodeUpgrade;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import io.ebean.EbeanServer;
import io.ebean.ExpressionList;
import io.ebean.PagedList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;


@RequiredArgsConstructor
public class ApplyRetentionStep implements UpgradeStep {

  private static final int DEFAULT_BATCH_SIZE = 1000;
  private static final long DEFAULT_BATCH_DELAY_MS = 250;

  private final EbeanServer _server;
  private final RetentionService _retentionService;

  @Override
  public String id() {
    return "ApplyRetentionStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {

      context.report().addLine("Sending MAE from local DB...");
      final int rowCount = getQuery().findCount();
      context.report().addLine(String.format("Found %s urn, aspect pair with more than 1 version", rowCount));

      int totalRowsMigrated = 0;
      int start = 0;
      int count = getBatchSize(context.parsedArgs());
      while (start < rowCount) {

        context.report().addLine(String.format("Reading rows %s through %s.", start, start + count));
        PagedList<EbeanAspectV2> rows = getPagedAspects(start, count);

        for (EbeanAspectV2 row : rows.getList()) {
          // 1. Extract an Entity type from the entity Urn
          Urn urn;
          try {
            urn = Urn.createFromString(row.getUrn());
          } catch (Exception e) {
            context.report()
                .addLine(
                    String.format("Failed to bind Urn with value %s into Urn object: %s", row.getUrn(), e));
            return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
          }
          final String aspectName = row.getAspect();
          _retentionService.applyRetention(urn, aspectName, Optional.empty());

          totalRowsMigrated++;
        }
        context.report().addLine(String.format("Successfully applied retention to %s rows", totalRowsMigrated));
        start = start + count;
        try {
          TimeUnit.MILLISECONDS.sleep(getBatchDelayMs(context.parsedArgs()));
        } catch (InterruptedException e) {
          throw new RuntimeException("Thread interrupted while sleeping after successful batch migration.");
        }
      }
      if (totalRowsMigrated != rowCount) {
        context.report()
            .addLine(String.format("Number of times we applied retention %s does not equal to row count %s...",
                totalRowsMigrated, rowCount));
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }

  private ExpressionList<EbeanAspectV2> getQuery() {
    return _server.find(EbeanAspectV2.class)
        .setDistinct(true)
        .select(EbeanAspectV2.URN_COLUMN + ", " + EbeanAspectV2.ASPECT_COLUMN)
        .where()
        .gt(EbeanAspectV2.VERSION_COLUMN, 0);
  }

  private PagedList<EbeanAspectV2> getPagedAspects(final int start, final int pageSize) {
    return getQuery().orderBy().asc(EbeanAspectV2.URN_COLUMN).setFirstRow(start).setMaxRows(pageSize).findPagedList();
  }

  private int getBatchSize(final Map<String, Optional<String>> parsedArgs) {
    int resolvedBatchSize = DEFAULT_BATCH_SIZE;
    if (parsedArgs.containsKey(ApplyRetention.BATCH_SIZE_ARG_NAME) && parsedArgs.get(NoCodeUpgrade.BATCH_SIZE_ARG_NAME)
        .isPresent()) {
      resolvedBatchSize = Integer.parseInt(parsedArgs.get(ApplyRetention.BATCH_SIZE_ARG_NAME).get());
    }
    return resolvedBatchSize;
  }

  private long getBatchDelayMs(final Map<String, Optional<String>> parsedArgs) {
    long resolvedBatchDelayMs = DEFAULT_BATCH_DELAY_MS;
    if (parsedArgs.containsKey(ApplyRetention.BATCH_DELAY_MS_ARG_NAME) && parsedArgs.get(
        NoCodeUpgrade.BATCH_DELAY_MS_ARG_NAME).isPresent()) {
      resolvedBatchDelayMs = Long.parseLong(parsedArgs.get(ApplyRetention.BATCH_DELAY_MS_ARG_NAME).get());
    }
    return resolvedBatchDelayMs;
  }
}
