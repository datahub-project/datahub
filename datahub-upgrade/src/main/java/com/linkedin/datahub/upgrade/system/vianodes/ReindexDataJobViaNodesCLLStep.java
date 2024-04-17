package com.linkedin.datahub.upgrade.system.vianodes;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.util.Pair;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReindexDataJobViaNodesCLLStep implements UpgradeStep {

  public static final String UPGRADE_ID = "via-node-cll-reindex-datajob-v3";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);

  private final EntityService<?> entityService;
  private final AspectDao aspectDao;
  private final int batchSize;
  private final int batchDelayMs;
  private final int limit;

  public ReindexDataJobViaNodesCLLStep(
      EntityService<?> entityService,
      AspectDao aspectDao,
      Integer batchSize,
      Integer batchDelayMs,
      Integer limit) {
    this.entityService = entityService;
    this.aspectDao = aspectDao;
    this.batchSize = batchSize != null ? batchSize : 200;
    this.batchDelayMs = batchDelayMs;
    this.limit = limit;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {

      // re-using for configuring the sql scan
      RestoreIndicesArgs args =
          new RestoreIndicesArgs()
              .aspectName(DATA_JOB_INPUT_OUTPUT_ASPECT_NAME)
              .urnLike("urn:li:" + DATA_JOB_ENTITY_NAME + ":%")
              .batchSize(batchSize)
              .limit(limit);

      final AspectSpec aspectSpec =
          context
              .opContext()
              .getEntityRegistry()
              .getAspectSpecs()
              .get(DATA_JOB_INPUT_OUTPUT_ASPECT_NAME);

      aspectDao
          .streamAspectBatches(args)
          .forEach(
              batch -> {
                log.info("Processing batch of size {}.", batchSize);

                List<Pair<Future<?>, Boolean>> futures =
                    EntityUtils.toSystemAspectFromEbeanAspects(
                            context.opContext().getRetrieverContext().get(),
                            batch.collect(Collectors.toList()))
                        .stream()
                        .map(
                            systemAspect ->
                                entityService.alwaysProduceMCLAsync(
                                    context.opContext(),
                                    systemAspect.getUrn(),
                                    systemAspect.getUrn().getEntityType(),
                                    DATA_JOB_INPUT_OUTPUT_ASPECT_NAME,
                                    aspectSpec,
                                    null,
                                    systemAspect.getRecordTemplate(),
                                    null,
                                    systemAspect
                                        .getSystemMetadata()
                                        .setRunId(UPGRADE_ID)
                                        .setLastObserved(System.currentTimeMillis()),
                                    AuditStampUtils.createDefaultAuditStamp(),
                                    ChangeType.UPSERT))
                        .collect(Collectors.toList());

                futures.forEach(
                    f -> {
                      try {
                        f.getFirst().get();
                      } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                      }
                    });

                if (batchDelayMs > 0) {
                  log.info("Sleeping for {} ms", batchDelayMs);
                  try {
                    Thread.sleep(batchDelayMs);
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                }
              });

      entityService
          .streamRestoreIndices(
              context.opContext(), args, x -> context.report().addLine((String) x))
          .forEach(
              result -> {
                context.report().addLine("Rows migrated: " + result.rowsMigrated);
                context.report().addLine("Rows ignored: " + result.ignored);
              });

      BootstrapStep.setUpgradeResult(context.opContext(), UPGRADE_ID_URN, entityService);
      context.report().addLine("State updated: " + UPGRADE_ID_URN);

      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }

  /**
   * Returns whether the upgrade should proceed if the step fails after exceeding the maximum
   * retries.
   */
  @Override
  public boolean isOptional() {
    return false;
  }

  @Override
  /**
   * Returns whether the upgrade should be skipped. Uses previous run history or the environment
   * variable SKIP_REINDEX_DATA_JOB_INPUT_OUTPUT to determine whether to skip.
   */
  public boolean skip(UpgradeContext context) {
    boolean previouslyRun =
        entityService.exists(
            context.opContext(), UPGRADE_ID_URN, DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);
    boolean envFlagRecommendsSkip =
        Boolean.parseBoolean(System.getenv("SKIP_REINDEX_DATA_JOB_INPUT_OUTPUT"));
    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
    }
    if (envFlagRecommendsSkip) {
      log.info("Environment variable SKIP_REINDEX_DATA_JOB_INPUT_OUTPUT is set to true. Skipping.");
    }
    return (previouslyRun || envFlagRecommendsSkip);
  }
}
