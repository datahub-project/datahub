package com.linkedin.datahub.upgrade.system;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;

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
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Generic upgrade step class for generating MCLs for a given aspect in order to update ES documents
 */
@Slf4j
public abstract class AbstractMCLStep implements UpgradeStep {
  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final AspectDao aspectDao;

  private final int batchSize;
  private final int batchDelayMs;
  private final int limit;

  public AbstractMCLStep(
      OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      Integer batchSize,
      Integer batchDelayMs,
      Integer limit) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.aspectDao = aspectDao;
    this.batchSize = batchSize;
    this.batchDelayMs = batchDelayMs;
    this.limit = limit;
  }

  @Nonnull
  protected abstract String getAspectName();

  protected Urn getUpgradeIdUrn() {
    return BootstrapStep.getUpgradeUrn(id());
  }

  /** Optionally apply an urn-like sql filter, otherwise all urns */
  @Nullable
  protected abstract String getUrnLike();

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {

      // re-using for configuring the sql scan
      RestoreIndicesArgs args =
          new RestoreIndicesArgs().aspectName(getAspectName()).batchSize(batchSize).limit(limit);

      if (getUrnLike() != null) {
        args = args.urnLike(getUrnLike());
      }

      try (PartitionedStream<EbeanAspectV2> stream = aspectDao.streamAspectBatches(args)) {
        stream
            .partition(args.batchSize)
            .forEach(
                batch -> {
                  log.info("Processing batch({}) of size {}.", getAspectName(), batchSize);

                  List<Pair<Future<?>, Boolean>> futures;

                  futures =
                      EntityUtils.toSystemAspectFromEbeanAspects(
                              opContext.getRetrieverContext().get(),
                              batch.collect(Collectors.toList()))
                          .stream()
                          .map(
                              systemAspect ->
                                  entityService.alwaysProduceMCLAsync(
                                      opContext,
                                      systemAspect.getUrn(),
                                      systemAspect.getUrn().getEntityType(),
                                      getAspectName(),
                                      systemAspect.getAspectSpec(),
                                      null,
                                      systemAspect.getRecordTemplate(),
                                      null,
                                      systemAspect
                                          .getSystemMetadata()
                                          .setRunId(id())
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
      }

      BootstrapStep.setUpgradeResult(opContext, getUpgradeIdUrn(), entityService);
      context.report().addLine("State updated: " + getUpgradeIdUrn());

      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }

  @Override
  /** Returns whether the upgrade should be skipped. */
  public boolean skip(UpgradeContext context) {
    boolean previouslyRun =
        entityService.exists(
            opContext, getUpgradeIdUrn(), DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);
    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
    }
    return previouslyRun;
  }
}
