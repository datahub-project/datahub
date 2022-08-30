package com.linkedin.datahub.upgrade.restoreindices;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.SystemMetadata;
import io.ebean.EbeanServer;
import io.ebean.ExpressionList;
import io.ebean.PagedList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;


public class SendMAEStep implements UpgradeStep {

  private static final int DEFAULT_BATCH_SIZE = 1000;
  private static final long DEFAULT_BATCH_DELAY_MS = 250;
  private static final int DEFAULT_THREADS = 1;

  private final EbeanServer _server;
  private final EntityService _entityService;
  private final EntityRegistry _entityRegistry;

  public static class KafkaJobResult {
    public int ignored;
    public int rowsMigrated;
    public KafkaJobResult(int ignored, int rowsMigrated) {
      this.ignored = ignored;
      this.rowsMigrated = rowsMigrated;
    }
  }

  public class KafkaJob implements Callable<KafkaJobResult> {
      UpgradeContext context;
      int start;
      JobArgs args;
      public KafkaJob(UpgradeContext context, int start, JobArgs args) {
        this.context = context;
        this.start = start;
        this.args = args;
      }
      @Override
      public KafkaJobResult call() {
        int ignored = 0;
        int rowsMigrated = 0;
        context.report().addLine(String.format(
                "Reading rows %s through %s from the aspects table started.", start, start + args.batchSize));
        PagedList<EbeanAspectV2> rows = getPagedAspects(start, args);
        context.report().addLine(String.format(
                "Reading rows %s through %s from the aspects table completed.", start, start + args.batchSize));

        for (EbeanAspectV2 aspect : rows.getList()) {
          // 1. Extract an Entity type from the entity Urn
          Urn urn;
          try {
            urn = Urn.createFromString(aspect.getKey().getUrn());
          } catch (Exception e) {
            context.report()
                    .addLine(String.format("Failed to bind Urn with value %s into Urn object: %s. Ignoring row.",
                            aspect.getKey().getUrn(), e));
            ignored = ignored + 1;
            continue;
          }

          // 2. Verify that the entity associated with the aspect is found in the registry.
          final String entityName = urn.getEntityType();
          final EntitySpec entitySpec;
          try {
            entitySpec = _entityRegistry.getEntitySpec(entityName);
          } catch (Exception e) {
            context.report()
                    .addLine(String.format("Failed to find entity with name %s in Entity Registry: %s. Ignoring row.",
                            entityName, e));
            ignored = ignored + 1;
            continue;
          }
          final String aspectName = aspect.getKey().getAspect();

          // 3. Verify that the aspect is a valid aspect associated with the entity
          AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);
          if (aspectSpec == null) {
            context.report()
                    .addLine(String.format("Failed to find aspect with name %s associated with entity named %s", aspectName,
                            entityName));
            ignored = ignored + 1;
            continue;
          }

          // 4. Create record from json aspect
          final RecordTemplate aspectRecord;
          try {
            aspectRecord = EntityUtils.toAspectRecord(entityName, aspectName, aspect.getMetadata(), _entityRegistry);
          } catch (Exception e) {
            context.report()
                    .addLine(String.format("Failed to deserialize row %s for entity %s, aspect %s: %s. Ignoring row.",
                            aspect.getMetadata(), entityName, aspectName, e));
            ignored = ignored + 1;
            continue;
          }

          SystemMetadata latestSystemMetadata = EntityUtils.parseSystemMetadata(aspect.getSystemMetadata());

          // 5. Produce MAE events for the aspect record
          _entityService.produceMetadataChangeLog(urn, entityName, aspectName, aspectSpec, null, aspectRecord, null,
                  latestSystemMetadata,
                  new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(System.currentTimeMillis()),
                  ChangeType.RESTATE);

          rowsMigrated++;
        }

        try {
          TimeUnit.MILLISECONDS.sleep(args.batchDelayMs);
        } catch (InterruptedException e) {
          throw new RuntimeException("Thread interrupted while sleeping after successful batch migration.");
        }
        return new KafkaJobResult(ignored, rowsMigrated);
      }
  }

  public SendMAEStep(final EbeanServer server, final EntityService entityService, final EntityRegistry entityRegistry) {
    _server = server;
    _entityService = entityService;
    _entityRegistry = entityRegistry;
  }

  @Override
  public String id() {
    return "SendMAEStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  private List<KafkaJobResult> iterateFutures(List<Future<KafkaJobResult>> futures) {
    int beforeSize = futures.size();
    int afterSize = futures.size();
    List<KafkaJobResult> result = new ArrayList<>();
    while (afterSize > 0 && beforeSize == afterSize) {
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        // suppress
      }
      for (Future<KafkaJobResult> future: new ArrayList<>(futures)) {
        if (future.isDone()) {
          try {
            result.add(future.get());
            futures.remove(future);
          } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
          }
        }
      }
      afterSize = futures.size();
    }
    return result;
  }

  private static class JobArgs {
    int batchSize;
    int numThreads;
    long batchDelayMs;
    String aspectName;
    String urn;
    String urnLike;
  }

  private JobArgs getArgs(UpgradeContext context) {
    JobArgs result = new JobArgs();
    result.batchSize = getBatchSize(context.parsedArgs());
    context.report().addLine(String.format("batchSize is %d", result.batchSize));
    result.numThreads = getThreadCount(context.parsedArgs());
    context.report().addLine(String.format("numThreads is %d", result.numThreads));
    result.batchDelayMs = getBatchDelayMs(context.parsedArgs());
    context.report().addLine(String.format("batchDelayMs is %d", result.batchDelayMs));
    if (containsKey(context.parsedArgs(), RestoreIndices.ASPECT_NAME_ARG_NAME)) {
      result.aspectName = context.parsedArgs().get(RestoreIndices.ASPECT_NAME_ARG_NAME).get();
      context.report().addLine(String.format("aspect is %s", result.aspectName));
      context.report().addLine(String.format("Found aspectName arg as %s", result.aspectName));
    } else {
      context.report().addLine("No aspectName arg present");
    }
    if (containsKey(context.parsedArgs(), RestoreIndices.URN_ARG_NAME)) {
      result.urn = context.parsedArgs().get(RestoreIndices.URN_ARG_NAME).get();
      context.report().addLine(String.format("urn is %s", result.urn));
      context.report().addLine(String.format("Found urn arg as %s", result.urn));
    } else {
      context.report().addLine("No urn arg present");
    }
    if (containsKey(context.parsedArgs(), RestoreIndices.URN_LIKE_ARG_NAME)) {
      result.urnLike = context.parsedArgs().get(RestoreIndices.URN_LIKE_ARG_NAME).get();
      context.report().addLine(String.format("urnLike is %s", result.urnLike));
      context.report().addLine(String.format("Found urn like arg as %s", result.urnLike));
    } else {
      context.report().addLine("No urnLike arg present");
    }
    return result;
  }

  private int getRowCount(JobArgs args) {
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
      JobArgs args = getArgs(context);
      ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(args.numThreads);

      context.report().addLine("Sending MAE from local DB");
      long startTime = System.currentTimeMillis();
      final int rowCount = getRowCount(args);
      context.report().addLine(String.format("Found %s latest aspects in aspects table in %.2f minutes.",
              rowCount, (float) (System.currentTimeMillis() - startTime) / 1000 / 60));

      int totalRowsMigrated = 0;
      int start = 0;
      int ignored = 0;

      List<Future<KafkaJobResult>> futures = new ArrayList<>();
      startTime = System.currentTimeMillis();
      while (start < rowCount) {
        while (futures.size() < args.numThreads && start < rowCount) {
          futures.add(executor.submit(new KafkaJob(context, start, args)));
          start = start + args.batchSize;
        }
        List<KafkaJobResult> tmpResults = iterateFutures(futures);
        for (KafkaJobResult tmpResult: tmpResults) {
          totalRowsMigrated += tmpResult.rowsMigrated;
          ignored += tmpResult.ignored;
          reportStats(context, totalRowsMigrated, ignored, rowCount, startTime);
        }
      }
      while (futures.size() > 0) {
        List<KafkaJobResult> tmpResults = iterateFutures(futures);
        for (KafkaJobResult tmpResult: tmpResults) {
          totalRowsMigrated += tmpResult.rowsMigrated;
          ignored += tmpResult.ignored;
          reportStats(context, totalRowsMigrated, ignored, rowCount, startTime);
        }
      }
      executor.shutdown();
      if (totalRowsMigrated != rowCount) {
        float percentFailed = 0.0f;
        if (rowCount > 0) {
          percentFailed = (float) (rowCount - totalRowsMigrated) * 100 / rowCount;
        }
        context.report().addLine(String.format(
                "Failed to send MAEs for %d rows (%.2f%% of total).", rowCount - totalRowsMigrated, percentFailed));
      }
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }

  private static void reportStats(UpgradeContext context, int totalRowsMigrated, int ignored, int rowCount,
                                  long startTime) {
    long currentTime = System.currentTimeMillis();
    float timeSoFarMinutes = (float) (currentTime - startTime) / 1000 / 60;
    float percentSent = (float) totalRowsMigrated * 100 / rowCount;
    float percentIgnored = (float) ignored * 100 / rowCount;
    float estimatedTimeMinutesComplete = -1;
    if (percentSent > 0) {
      estimatedTimeMinutesComplete = timeSoFarMinutes * (100 - percentSent) / percentSent;
    }
    float totalTimeComplete = timeSoFarMinutes + estimatedTimeMinutesComplete;
    context.report().addLine(String.format(
            "Successfully sent MAEs for %s/%s rows (%.2f%% of total). %s rows ignored (%.2f%% of total)",
            totalRowsMigrated, rowCount, percentSent, ignored, percentIgnored));
    context.report().addLine(String.format("%.2f mins taken. %.2f est. mins to completion. Total mins est. = %.2f.",
            timeSoFarMinutes, estimatedTimeMinutesComplete, totalTimeComplete));
  }

  private PagedList<EbeanAspectV2> getPagedAspects(final int start, final JobArgs args) {
    ExpressionList<EbeanAspectV2> exp = _server.find(EbeanAspectV2.class)
            .select(EbeanAspectV2.ALL_COLUMNS)
            .where()
            .eq(EbeanAspectV2.VERSION_COLUMN, ASPECT_LATEST_VERSION);
    if (args.aspectName != null) {
      exp = exp.eq(EbeanAspectV2.ASPECT_COLUMN, args.aspectName);
    }
    if (args.urn != null) {
      exp = exp.eq(EbeanAspectV2.URN_COLUMN, args.urn);
    }
    if (args.urnLike != null) {
      exp = exp.like(EbeanAspectV2.URN_COLUMN, args.urnLike);
    }
    return  exp.orderBy()
        .asc(EbeanAspectV2.URN_COLUMN)
        .orderBy()
        .asc(EbeanAspectV2.ASPECT_COLUMN)
        .setFirstRow(start)
        .setMaxRows(args.batchSize)
        .findPagedList();
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
