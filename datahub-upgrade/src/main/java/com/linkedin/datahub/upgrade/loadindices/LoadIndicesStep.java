package com.linkedin.datahub.upgrade.loadindices;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.ebean.Database;
import io.ebean.ExpressionList;
import io.ebean.annotation.TxIsolation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LoadIndicesStep implements UpgradeStep {

  private final Database server;
  private final UpdateIndicesService updateIndicesService;
  private final LoadIndicesIndexManager indexManager;

  public LoadIndicesStep(
      final Database server,
      final UpdateIndicesService updateIndicesService,
      final LoadIndicesIndexManager indexManager) {
    this.server = server;
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

        indexManager.optimizeForBulkOperations();
        context.report().addLine("Optimized settings for bulk operations on DataHub indices");

        log.info("Starting loadIndices");

        long startTime = System.currentTimeMillis();

        LoadIndicesResult result =
            processAllDataDirectly(
                context.opContext(),
                args,
                (msg) -> {
                  context.report().addLine(msg);
                  return null;
                });

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
            indexManager.restoreFromConfiguration();
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
      result.aspectNames = getDefaultAspectNames(context.opContext());
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
    return result;
  }

  private LoadIndicesResult processAllDataDirectly(
      OperationContext opContext, LoadIndicesArgs args, Function<String, Void> reportFunction) {
    LoadIndicesResult result = new LoadIndicesResult();
    long totalStartTime = System.currentTimeMillis();

    try {
      // Create EbeanAspectDao for streaming
      EbeanAspectDao aspectDao =
          new EbeanAspectDao(
              server,
              EbeanConfiguration.testDefault,
              null,
              java.util.Collections.emptyList(),
              null);
      aspectDao.setConnectionValidated(true);

      // Process data using streaming approach (no cursor pagination needed!)
      int batchSize = args.batchSize;
      final int[] totalProcessed = {0}; // Use array to make it effectively final
      final int limit = args.limit;

      // Determine totalRecords based on whether limit is specified
      final long totalRecords;
      if (limit == Integer.MAX_VALUE) {
        reportFunction.apply("No limit specified - counting total aspects for ETA calculation...");
        long countStartTime = System.currentTimeMillis();

        ExpressionList<EbeanAspectV2> countQuery =
            server
                .find(EbeanAspectV2.class)
                .where()
                .eq(EbeanAspectV2.VERSION_COLUMN, ASPECT_LATEST_VERSION);

        // Apply same filters as main query
        if (args.urnLike != null) {
          countQuery = countQuery.like(EbeanAspectV2.URN_COLUMN, args.urnLike);
        }
        if (args.aspectNames != null && !args.aspectNames.isEmpty()) {
          countQuery = countQuery.in(EbeanAspectV2.ASPECT_COLUMN, args.aspectNames);
        }
        if (args.lePitEpochMs != null) {
          countQuery = countQuery.le(EbeanAspectV2.CREATED_ON_COLUMN, args.lePitEpochMs);
        }
        if (args.gePitEpochMs != null) {
          countQuery = countQuery.ge(EbeanAspectV2.CREATED_ON_COLUMN, args.gePitEpochMs);
        }
        if (args.lastUrn != null) {
          countQuery = countQuery.ge(EbeanAspectV2.URN_COLUMN, args.lastUrn);
        }

        totalRecords = countQuery.findCount();
        long countTime = System.currentTimeMillis() - countStartTime;

        reportFunction.apply(
            String.format(
                "Found %d total aspects to process (count took %.2f seconds)",
                totalRecords, countTime / 1000.0));
      } else {
        totalRecords = limit;
        reportFunction.apply(
            String.format("Limit specified (%d) - will use this for progress calculation", limit));
      }

      reportFunction.apply(
          "Starting main data query - this may take a moment for SQL to return the first batch...");

      // Use streaming approach ordered by URN/aspect for optimal ES document batching
      RestoreIndicesArgs restoreArgs = convertToRestoreIndicesArgs(args, limit);
      try (PartitionedStream<EbeanAspectV2> stream =
          aspectDao.streamAspectBatches(restoreArgs, TxIsolation.READ_UNCOMMITTED)) {

        // Simple forEach approach since SQL handles the limiting
        stream
            .partition(batchSize)
            .forEach(
                batch -> {
                  long batchStartTime = System.currentTimeMillis();

                  List<EbeanAspectV2> aspects = batch.collect(java.util.stream.Collectors.toList());

                  if (aspects.isEmpty()) {
                    return;
                  }

                  result.timeSqlQueryMs += System.currentTimeMillis() - batchStartTime;

                  // Pre-allocate list to avoid multiple resizing
                  List<MetadataChangeLog> mclBatch = new ArrayList<>(aspects.size());
                  int conversionErrors = 0;
                  for (EbeanAspectV2 aspect : aspects) {
                    try {
                      MetadataChangeLog mcl = convertToMetadataChangeLog(opContext, aspect);
                      mclBatch.add(mcl);
                    } catch (Exception e) {
                      log.debug("Error converting aspect: {}", aspect.getKey(), e);
                      conversionErrors++;
                      result.ignored++;
                    }
                  }

                  if (!mclBatch.isEmpty()) {
                    writeBatchWithRetry(opContext, mclBatch, result, reportFunction);
                    int aspectsProcessed = aspects.size() - conversionErrors;
                    totalProcessed[0] += aspectsProcessed;

                    // Log the last URN of every batch for resume capability
                    String lastUrn = aspects.get(aspects.size() - 1).getKey().getUrn();

                    if (totalProcessed[0] % batchSize == 0 || conversionErrors > 0) {
                      long currentTime = System.currentTimeMillis();
                      long elapsedTime = currentTime - totalStartTime;
                      double aspectsPerSecond = (double) totalProcessed[0] / (elapsedTime / 1000.0);

                      String progressMessage;
                      if (conversionErrors > 0) {
                        progressMessage =
                            String.format(
                                "Processed %d aspects (total: %d, %d conversion errors) - %.1f aspects/sec",
                                aspectsProcessed,
                                totalProcessed[0],
                                conversionErrors,
                                aspectsPerSecond);
                      } else {
                        progressMessage =
                            String.format(
                                "Processed %d aspects - %.1f aspects/sec",
                                totalProcessed[0], aspectsPerSecond);
                      }

                      if (totalRecords > 0 && aspectsPerSecond > 0 && totalProcessed[0] > 50000) {
                        long remainingAspects;
                        if (limit != Integer.MAX_VALUE) {
                          remainingAspects = Math.min(limit, totalRecords) - totalProcessed[0];
                        } else {
                          remainingAspects = totalRecords - totalProcessed[0];
                        }

                        if (remainingAspects > 0) {
                          long estimatedRemainingMs =
                              (long) (remainingAspects / aspectsPerSecond * 1000);
                          long estimatedRemainingMinutes = estimatedRemainingMs / 60000;
                          long estimatedRemainingSeconds = (estimatedRemainingMs % 60000) / 1000;

                          int progressPercent = (int) ((totalProcessed[0] * 100L) / totalRecords);
                          progressMessage +=
                              String.format(
                                  " - Progress: %d%% - ETA: %dm %ds",
                                  progressPercent,
                                  estimatedRemainingMinutes,
                                  estimatedRemainingSeconds);
                        }
                      }

                      reportFunction.apply(progressMessage);
                      reportFunction.apply("Last URN processed: " + lastUrn);
                    }
                  }
                });
      }

      result.timeElasticsearchWriteMs =
          System.currentTimeMillis() - totalStartTime - result.timeSqlQueryMs;

      try {
        updateIndicesService.flush();
        log.info("Final flush completed - all data written to Elasticsearch");
      } catch (Exception e) {
        log.error("Failed to perform final flush: {}", e.getMessage());
      }

      result.rowsProcessed = totalProcessed[0];
      long totalTimeMs = System.currentTimeMillis() - totalStartTime;
      double finalThroughput = (double) result.rowsProcessed / (totalTimeMs / 1000.0);
      reportFunction.apply(
          String.format(
              "Processing completed: %d aspects processed, %d ignored - Final throughput: %.1f aspects/sec",
              result.rowsProcessed, result.ignored, finalThroughput));

    } catch (Exception e) {
      log.error("Error in processAllDataDirectly", e);
      throw new RuntimeException(e);
    }

    return result;
  }

  /** Writes a batch to UpdateIndices with retry logic that splits the batch in half if it fails. */
  private void writeBatchWithRetry(
      OperationContext opContext,
      List<MetadataChangeLog> batch,
      LoadIndicesResult result,
      Function<String, Void> reportFunction) {

    List<MetadataChangeLog> currentBatch = new ArrayList<>(batch);
    int retryCount = 0;
    final int maxRetries = 3;

    while (!currentBatch.isEmpty() && retryCount <= maxRetries) {
      try {
        updateIndicesService.handleChangeEvents(opContext, currentBatch);

        if (retryCount > 0) {
          log.info(
              "Successfully wrote batch of {} MCL events after {} retries",
              currentBatch.size(),
              retryCount);
        }
        break;

      } catch (Exception e) {
        retryCount++;
        log.warn(
            "Failed to write batch of {} MCL events (attempt {}): {}",
            currentBatch.size(),
            retryCount,
            e.getMessage());

        if (retryCount > maxRetries) {
          log.error(
              "Max retries ({}) exceeded for batch of {} MCL events. Giving up.",
              maxRetries,
              currentBatch.size());
          result.ignored += currentBatch.size();
          reportFunction.apply(
              String.format(
                  "Failed to write batch of %d MCL events after %d retries - ignoring",
                  currentBatch.size(), maxRetries));
          break;
        }

        int splitPoint = currentBatch.size() / 2;
        if (splitPoint == 0) {
          log.error(
              "Cannot split batch further (size: {}), marking as ignored", currentBatch.size());
          result.ignored += currentBatch.size();
          reportFunction.apply(
              String.format(
                  "Failed to write single MCL event after %d retries - ignoring", retryCount));
          break;
        }

        List<MetadataChangeLog> firstHalf = currentBatch.subList(0, splitPoint);
        List<MetadataChangeLog> secondHalf = currentBatch.subList(splitPoint, currentBatch.size());

        log.info(
            "Splitting failed batch of {} MCL events into two batches of {} and {} for retry",
            currentBatch.size(),
            firstHalf.size(),
            secondHalf.size());

        try {
          updateIndicesService.handleChangeEvents(opContext, firstHalf);
          log.debug(
              "Successfully wrote first half of split batch ({} MCL events)", firstHalf.size());
        } catch (Exception e2) {
          log.error("Failed to write first half of split batch: {}", e2.getMessage());
          result.ignored += firstHalf.size();
          reportFunction.apply(
              String.format(
                  "Failed to write first half of split batch (%d MCL events) - ignoring",
                  firstHalf.size()));
        }

        currentBatch = secondHalf;
      }
    }
  }

  private MetadataChangeLog convertToMetadataChangeLog(
      OperationContext opContext, EbeanAspectV2 ebeanAspect) throws Exception {
    Urn urn = UrnUtils.getUrn(ebeanAspect.getKey().getUrn());
    String aspectName = ebeanAspect.getKey().getAspect();
    String entityType = urn.getEntityType();

    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(
                UrnUtils.getUrn(
                    opContext.getActorContext().getAuthentication().getActor().toUrnStr()))
            .setTime(ebeanAspect.getCreatedOn().getTime());

    SystemMetadata systemMetadata = null;
    if (ebeanAspect.getSystemMetadata() != null) {
      systemMetadata =
          RecordUtils.toRecordTemplate(SystemMetadata.class, ebeanAspect.getSystemMetadata());
    }

    return PegasusUtils.constructMCL(
        null,
        entityType,
        urn,
        ChangeType.RESTATE,
        aspectName,
        auditStamp,
        RecordUtils.toRecordTemplate(
            opContext
                .getEntityRegistry()
                .getEntitySpec(entityType)
                .getAspectSpec(aspectName)
                .getDataTemplateClass(),
            ebeanAspect.getMetadata()),
        systemMetadata,
        null,
        null);
  }

  private int getBatchSize(final Map<String, Optional<String>> parsedArgs) {
    return getInt(parsedArgs, 10000, LoadIndices.BATCH_SIZE_ARG_NAME);
  }

  private int getLimit(final Map<String, Optional<String>> parsedArgs) {
    return getInt(parsedArgs, Integer.MAX_VALUE, LoadIndices.LIMIT_ARG_NAME);
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

  /**
   * Get default aspect names based on entity registry. Includes all aspects with searchable
   * annotations and key aspects for entities that have at least one searchable aspect.
   */
  private Set<String> getDefaultAspectNames(OperationContext opContext) {
    Set<String> aspectNames = new HashSet<>();

    for (String entityName : opContext.getEntityRegistry().getEntitySpecs().keySet()) {
      try {
        com.linkedin.metadata.models.EntitySpec entitySpec =
            opContext.getEntityRegistry().getEntitySpec(entityName);

        boolean entityHasSearchableAspects = false;

        for (com.linkedin.metadata.models.AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
          if (!aspectSpec.getSearchableFieldSpecs().isEmpty()) {
            entityHasSearchableAspects = true;
            aspectNames.add(aspectSpec.getName());
          }
        }

        if (entityHasSearchableAspects) {
          String keyAspectName = entitySpec.getKeyAspectName();
          if (!aspectNames.contains(keyAspectName)) {
            aspectNames.add(keyAspectName);
          }
        }
      } catch (Exception e) {
        log.warn(
            "Error processing entity {} for default aspect names: {}", entityName, e.getMessage());
      }
    }

    return aspectNames;
  }

  /** Convert LoadIndicesArgs to RestoreIndicesArgs for compatibility with streamAspectBatches. */
  private RestoreIndicesArgs convertToRestoreIndicesArgs(LoadIndicesArgs args, int limit) {
    RestoreIndicesArgs restoreArgs = new RestoreIndicesArgs();

    if (args.aspectNames != null && !args.aspectNames.isEmpty()) {
      restoreArgs.aspectNames = new ArrayList<>(args.aspectNames);
    }
    restoreArgs.urnLike = args.urnLike;
    restoreArgs.gePitEpochMs = args.gePitEpochMs != null ? args.gePitEpochMs : 0L;
    restoreArgs.lePitEpochMs =
        args.lePitEpochMs != null ? args.lePitEpochMs : System.currentTimeMillis();
    restoreArgs.limit = limit;

    // Enable URN-based pagination if lastUrn is provided
    if (args.lastUrn != null) {
      restoreArgs.urnBasedPagination = true;
      restoreArgs.lastUrn = args.lastUrn;
    }

    return restoreArgs;
  }
}
