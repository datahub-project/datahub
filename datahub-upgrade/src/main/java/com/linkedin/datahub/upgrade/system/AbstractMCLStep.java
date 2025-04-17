package com.linkedin.datahub.upgrade.system;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  public static final String LAST_URN_KEY = "lastUrn";

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
      // Resume state
      Optional<DataHubUpgradeResult> prevResult =
          context.upgrade().getUpgradeResult(opContext, getUpgradeIdUrn(), entityService);
      String resumeUrn =
          prevResult
              .filter(
                  result ->
                      DataHubUpgradeState.IN_PROGRESS.equals(result.getState())
                          && result.getResult() != null
                          && result.getResult().containsKey(LAST_URN_KEY))
              .map(result -> result.getResult().get(LAST_URN_KEY))
              .orElse(null);
      if (resumeUrn != null) {
        log.info("{}: Resuming from URN: {}", getUpgradeIdUrn(), resumeUrn);
      }

      // re-using for configuring the sql scan
      RestoreIndicesArgs args =
          new RestoreIndicesArgs()
              .aspectName(getAspectName())
              .batchSize(batchSize)
              .lastUrn(resumeUrn)
              .urnBasedPagination(resumeUrn != null)
              .limit(limit);

      if (getUrnLike() != null) {
        args = args.urnLike(getUrnLike());
      }

      try (PartitionedStream<EbeanAspectV2> stream = aspectDao.streamAspectBatches(args)) {
        stream
            .partition(args.batchSize)
            .forEach(
                batch -> {
                  log.info("Processing batch({}) of size {}.", getAspectName(), batchSize);

                  List<Pair<Future<?>, SystemAspect>> futures;
                  futures =
                      EntityUtils.toSystemAspectFromEbeanAspects(
                              opContext.getRetrieverContext(), batch.collect(Collectors.toList()))
                          .stream()
                          .map(
                              systemAspect -> {
                                Pair<Future<?>, Boolean> future =
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
                                        ChangeType.UPSERT);
                                return Pair.<Future<?>, SystemAspect>of(
                                    future.getFirst(), systemAspect);
                              })
                          .toList();

                  SystemAspect lastAspect =
                      futures.stream()
                          .map(
                              f -> {
                                try {
                                  f.getFirst().get();
                                  return f.getSecond();
                                } catch (InterruptedException | ExecutionException e) {
                                  throw new RuntimeException(e);
                                }
                              })
                          .reduce((a, b) -> b)
                          .orElse(null);

                  // record progress
                  if (lastAspect != null) {
                    log.info(
                        "{}: Saving state. Last urn:{}", getUpgradeIdUrn(), lastAspect.getUrn());
                    context
                        .upgrade()
                        .setUpgradeResult(
                            opContext,
                            getUpgradeIdUrn(),
                            entityService,
                            DataHubUpgradeState.IN_PROGRESS,
                            Map.of(LAST_URN_KEY, lastAspect.getUrn().toString()));
                  }

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

      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  @Override
  /** Returns whether the upgrade should be skipped. */
  public boolean skip(UpgradeContext context) {
    Optional<DataHubUpgradeResult> prevResult =
        context.upgrade().getUpgradeResult(opContext, getUpgradeIdUrn(), entityService);

    boolean previousRunFinal =
        prevResult
            .filter(
                result ->
                    DataHubUpgradeState.SUCCEEDED.equals(result.getState())
                        || DataHubUpgradeState.ABORTED.equals(result.getState()))
            .isPresent();

    if (previousRunFinal) {
      log.info(
          "{} was already run. State: {} Skipping.",
          id(),
          prevResult.map(DataHubUpgradeResult::getState));
    }
    return previousRunFinal;
  }
}
