package com.linkedin.datahub.upgrade.loadindices;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.BackfillHeaders;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.annotation.TxIsolation;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LoadIndicesStep implements UpgradeStep {

  private final UpdateIndicesService updateIndicesService;
  private final LoadIndicesIndexManager indexManager;
  private final AspectIndexLoader loader;

  public LoadIndicesStep(
      final AspectIndexLoader loader,
      final UpdateIndicesService updateIndicesService,
      final LoadIndicesIndexManager indexManager) {
    this.loader = loader;
    this.updateIndicesService = updateIndicesService;
    this.indexManager = indexManager;
  }

  @Override
  public String id() {
    return "LoadIndicesStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      LoadIndicesArgs args = getArgs(context);

      try {
        context
            .report()
            .addLine(
                "Loading indices directly from local DB ordered by URN/aspect for optimal document batching");

        indexManager.optimizeForBulkOperations(context.opContext());
        context.report().addLine("Optimized settings for bulk operations on DataHub indices");

        log.info("Starting loadIndices");

        long startTime = System.currentTimeMillis();

        LoadIndicesResult result =
            loader.load(
                context.opContext(),
                args,
                (msg) -> {
                  context.report().addLine(msg);
                  return null;
                },
                null,
                updateIndicesService::handleChangeEvents);

        long totalTime = System.currentTimeMillis() - startTime;
        context.report().addLine(String.format("Processing completed: %s", result));
        context
            .report()
            .addLine(
                String.format(
                    "Total processing time: %.2f minutes", (float) totalTime / 1000 / 60));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);

      } catch (Exception e) {
        log.error("Error during LoadIndices execution", e);
        context.report().addLine(String.format("Error during execution: %s", e.getMessage()));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      } finally {
        if (indexManager.isSettingsOptimized()) {
          try {
            indexManager.restoreFromConfiguration(context.opContext());
            context
                .report()
                .addLine("Restored settings to configured values for all DataHub indices");
          } catch (IOException e) {
            log.error("Failed to restore settings", e);
            context
                .report()
                .addLine(String.format("Warning: Failed to restore settings: %s", e.getMessage()));
          }
        }
      }
    };
  }

  private LoadIndicesArgs getArgs(UpgradeContext context) {
    LoadIndicesArgs result = new LoadIndicesArgs();
    result.batchSize = getBatchSize(context.parsedArgs());
    result.limit = getLimit(context.parsedArgs());
    context.report().addLine(String.format("batchSize is %d", result.batchSize));
    if (result.limit == Integer.MAX_VALUE) {
      context.report().addLine("limit is not applied (processing all matching records)");
    } else {
      context.report().addLine(String.format("limit is %d", result.limit));
    }

    if (containsKey(context.parsedArgs(), LoadIndices.URN_LIKE_ARG_NAME)) {
      result.urnLike = context.parsedArgs().get(LoadIndices.URN_LIKE_ARG_NAME).get();
      context.report().addLine(String.format("urnLike is %s", result.urnLike));
    } else {
      context.report().addLine("No urnLike arg present");
    }
    if (containsKey(context.parsedArgs(), LoadIndices.LE_PIT_EPOCH_MS_ARG_NAME)) {
      result.lePitEpochMs =
          Long.parseLong(context.parsedArgs().get(LoadIndices.LE_PIT_EPOCH_MS_ARG_NAME).get());
      context.report().addLine(String.format("lePitEpochMs is %s", result.lePitEpochMs));
    }
    if (containsKey(context.parsedArgs(), LoadIndices.GE_PIT_EPOCH_MS_ARG_NAME)) {
      result.gePitEpochMs =
          Long.parseLong(context.parsedArgs().get(LoadIndices.GE_PIT_EPOCH_MS_ARG_NAME).get());
      context.report().addLine(String.format("gePitEpochMs is %s", result.gePitEpochMs));
    }
    if (containsKey(context.parsedArgs(), LoadIndices.ASPECT_NAMES_ARG_NAME)) {
      result.aspectNames =
          Arrays.asList(
              context.parsedArgs().get(LoadIndices.ASPECT_NAMES_ARG_NAME).get().split(","));
      context.report().addLine(String.format("aspectNames is %s", result.aspectNames));
    } else {
      // Set default aspect names based on entity registry when not provided
      result.aspectNames = loader.getDefaultAspectNames(context.opContext());
      context
          .report()
          .addLine(
              String.format("aspectNames not provided, using defaults: %s", result.aspectNames));
    }
    if (containsKey(context.parsedArgs(), LoadIndices.LAST_URN_ARG_NAME)) {
      result.lastUrn = context.parsedArgs().get(LoadIndices.LAST_URN_ARG_NAME).get();
      context.report().addLine(String.format("lastUrn is %s", result.lastUrn));
    } else {
      context.report().addLine("No lastUrn arg present - will process from beginning");
    }
    result.v3BackfillMode = getV3BackfillMode(context.parsedArgs());
    result.txIsolation = TxIsolation.READ_UNCOMMITTED;
    context.report().addLine(String.format("v3BackfillMode is %s", result.v3BackfillMode));
    result.skipConversionErrors =
        containsKey(context.parsedArgs(), LoadIndices.SKIP_CONVERSION_ERRORS_ARG_NAME)
            && Boolean.parseBoolean(
                context.parsedArgs().get(LoadIndices.SKIP_CONVERSION_ERRORS_ARG_NAME).get());
    context
        .report()
        .addLine(String.format("skipConversionErrors is %s", result.skipConversionErrors));
    return result;
  }

  private int getBatchSize(final Map<String, Optional<String>> parsedArgs) {
    return getInt(
        parsedArgs, AspectIndexLoader.DEFAULT_BATCH_SIZE, LoadIndices.BATCH_SIZE_ARG_NAME);
  }

  private int getLimit(final Map<String, Optional<String>> parsedArgs) {
    return getInt(parsedArgs, Integer.MAX_VALUE, LoadIndices.LIMIT_ARG_NAME);
  }

  private String getV3BackfillMode(final Map<String, Optional<String>> parsedArgs) {
    String mode = BackfillHeaders.MODE_BASELINE;
    if (containsKey(parsedArgs, LoadIndices.V3_BACKFILL_MODE_ARG_NAME)) {
      mode = parsedArgs.get(LoadIndices.V3_BACKFILL_MODE_ARG_NAME).get();
    }
    return AspectIndexLoader.normalizeV3BackfillMode(mode);
  }

  public boolean containsKey(Map<String, Optional<String>> parsedArgs, String key) {
    return parsedArgs.containsKey(key)
        && parsedArgs.get(key) != null
        && parsedArgs.get(key).isPresent();
  }

  private int getInt(
      final Map<String, Optional<String>> parsedArgs, int defaultVal, String argKey) {
    int result = defaultVal;
    if (containsKey(parsedArgs, argKey)) {
      result = Integer.parseInt(parsedArgs.get(argKey).get());
    }
    return result;
  }
}
