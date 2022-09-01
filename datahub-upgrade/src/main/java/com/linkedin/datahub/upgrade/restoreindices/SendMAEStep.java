package com.linkedin.datahub.upgrade.restoreindices;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesResult;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.ebean.EbeanServer;
import io.ebean.ExpressionList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;


public class SendMAEStep implements UpgradeStep {

  private static final int DEFAULT_BATCH_SIZE = 1000;
  private static final long DEFAULT_BATCH_DELAY_MS = 250;
  private static final int DEFAULT_THREADS = 1;

  private final EbeanServer _server;
  private final EntityService _entityService;

  public class KafkaJob implements Callable<RestoreIndicesResult> {
      UpgradeContext context;
      RestoreIndicesArgs args;
      public KafkaJob(UpgradeContext context, RestoreIndicesArgs args) {
        this.context = context;
        this.args = args;
      }
      @Override
      public RestoreIndicesResult call() {
        return _entityService.restoreIndices(args, context.report()::addLine);
      }
  }

  public SendMAEStep(final EbeanServer server, final EntityService entityService, final EntityRegistry entityRegistry) {
    _server = server;
    _entityService = entityService;
  }

  @Override
  public String id() {
    return "SendMAEStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  private List<RestoreIndicesResult> iterateFutures(List<Future<RestoreIndicesResult>> futures) {
    List<RestoreIndicesResult> result = new ArrayList<>();
    for (Future<RestoreIndicesResult> future: new ArrayList<>(futures)) {
      if (future.isDone()) {
        try {
          result.add(future.get());
          futures.remove(future);
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
        }
      }
    }
    return result;
  }

  private RestoreIndicesArgs getArgs(UpgradeContext context) {
    RestoreIndicesArgs result = new RestoreIndicesArgs();
    result.batchSize = getBatchSize(context.parsedArgs());
    result.numThreads = getThreadCount(context.parsedArgs());
    result.batchDelayMs = getBatchDelayMs(context.parsedArgs());
    if (containsKey(context.parsedArgs(), RestoreIndices.ASPECT_NAME_ARG_NAME)) {
      result.aspectName = context.parsedArgs().get(RestoreIndices.ASPECT_NAME_ARG_NAME).get();
    }
    if (containsKey(context.parsedArgs(), RestoreIndices.URN_ARG_NAME)) {
      result.urn = context.parsedArgs().get(RestoreIndices.URN_ARG_NAME).get();
    }
    if (containsKey(context.parsedArgs(), RestoreIndices.URN_LIKE_ARG_NAME)) {
      result.urnLike = context.parsedArgs().get(RestoreIndices.URN_LIKE_ARG_NAME).get();
    }
    return result;
  }

  private int getRowCount(RestoreIndicesArgs args) {
    ExpressionList<EbeanAspectV2> countExp =
            _server.find(EbeanAspectV2.class)
                    .where()
                    .eq(EbeanAspectV2.VERSION_COLUMN, ASPECT_LATEST_VERSION);
    if (args.aspectName != null) {
      countExp = countExp.eq(EbeanAspectV2.ASPECT_COLUMN, args.aspectName);
    }
    if (args.urn != null) {
      countExp = countExp.eq(EbeanAspectV2.URN_COLUMN, args.urn);
    }
    if (args.urnLike != null) {
      countExp = countExp.like(EbeanAspectV2.URN_COLUMN, args.urnLike);
    }
    return countExp.findCount();
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      RestoreIndicesResult finalJobResult = new RestoreIndicesResult();
      RestoreIndicesArgs args = getArgs(context);
      ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(args.numThreads);

      context.report().addLine("Sending MAE from local DB");
      long startTime = System.currentTimeMillis();
      final int rowCount = getRowCount(args);
      context.report().addLine(String.format("Found %s latest aspects in aspects table in %.2f minutes.",
              rowCount, (float) (System.currentTimeMillis() - startTime) / 1000 / 60));
      int start = 0;

      List<Future<RestoreIndicesResult>> futures = new ArrayList<>();
      startTime = System.currentTimeMillis();
      while (start < rowCount) {
        args = args.clone();
        args.start = start;
        futures.add(executor.submit(new KafkaJob(context, args)));
        start = start + args.batchSize;
      }
      while (futures.size() > 0) {
        List<RestoreIndicesResult> tmpResults = iterateFutures(futures);
        for (RestoreIndicesResult tmpResult: tmpResults) {
          reportStats(context, finalJobResult, tmpResult, rowCount, startTime);
        }
      }
      executor.shutdown();
      if (finalJobResult.rowsMigrated != rowCount) {
        float percentFailed = 0.0f;
        if (rowCount > 0) {
          percentFailed = (float) (rowCount - finalJobResult.rowsMigrated) * 100 / rowCount;
        }
        context.report().addLine(String.format(
                "Failed to send MAEs for %d rows (%.2f%% of total).",
                rowCount - finalJobResult.rowsMigrated, percentFailed));
      }
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }

  private static void reportStats(UpgradeContext context, RestoreIndicesResult finalResult, RestoreIndicesResult tmpResult,
                                  int rowCount, long startTime) {
    finalResult.ignored += tmpResult.ignored;
    finalResult.rowsMigrated += tmpResult.rowsMigrated;
    finalResult.timeSqlQueryMs += tmpResult.timeSqlQueryMs;
    finalResult.timeUrnMs += tmpResult.timeUrnMs;
    finalResult.timeEntityRegistryCheckMs += tmpResult.timeEntityRegistryCheckMs;
    finalResult.aspectCheckMs += tmpResult.aspectCheckMs;
    finalResult.createRecordMs += tmpResult.createRecordMs;
    finalResult.sendMessageMs += tmpResult.sendMessageMs;
    context.report().addLine(String.format("metrics so far %s", finalResult));

    long currentTime = System.currentTimeMillis();
    float timeSoFarMinutes = (float) (currentTime - startTime) / 1000 / 60;
    float percentSent = (float) finalResult.rowsMigrated * 100 / rowCount;
    float percentIgnored = (float) finalResult.ignored * 100 / rowCount;
    float estimatedTimeMinutesComplete = -1;
    if (percentSent > 0) {
      estimatedTimeMinutesComplete = timeSoFarMinutes * (100 - percentSent) / percentSent;
    }
    float totalTimeComplete = timeSoFarMinutes + estimatedTimeMinutesComplete;
    context.report().addLine(String.format(
            "Successfully sent MAEs for %s/%s rows (%.2f%% of total). %s rows ignored (%.2f%% of total)",
            finalResult.rowsMigrated, rowCount, percentSent, finalResult.ignored, percentIgnored));
    context.report().addLine(String.format("%.2f mins taken. %.2f est. mins to completion. Total mins est. = %.2f.",
            timeSoFarMinutes, estimatedTimeMinutesComplete, totalTimeComplete));
  }

  private int getBatchSize(final Map<String, Optional<String>> parsedArgs) {
    return getInt(parsedArgs, DEFAULT_BATCH_SIZE, RestoreIndices.BATCH_SIZE_ARG_NAME);
  }

  private long getBatchDelayMs(final Map<String, Optional<String>> parsedArgs) {
    long resolvedBatchDelayMs = DEFAULT_BATCH_DELAY_MS;
    if (containsKey(parsedArgs, RestoreIndices.BATCH_DELAY_MS_ARG_NAME)) {
      resolvedBatchDelayMs = Long.parseLong(parsedArgs.get(RestoreIndices.BATCH_DELAY_MS_ARG_NAME).get());
    }
    return resolvedBatchDelayMs;
  }

  private int getThreadCount(final Map<String, Optional<String>> parsedArgs) {
    return getInt(parsedArgs, DEFAULT_THREADS, RestoreIndices.NUM_THREADS_ARG_NAME);
  }

  private int getInt(final Map<String, Optional<String>> parsedArgs, int defaultVal, String argKey) {
    int result = defaultVal;
    if (containsKey(parsedArgs, argKey)) {
      result = Integer.parseInt(parsedArgs.get(argKey).get());
    }
    return result;
  }

  public static boolean containsKey(final Map<String, Optional<String>> parsedArgs, String key) {
    return parsedArgs.containsKey(key) && parsedArgs.get(key).isPresent();
  }
}
