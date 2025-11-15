package com.linkedin.datahub.upgrade.system;

import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

/**
 * Generic upgrade step class for generating MCPs for a given aspect in order to update SQL+ES
 * documents
 */
@Slf4j
public abstract class AbstractMCPStep implements UpgradeStep {
  public static final String LAST_URN_KEY = "lastUrn";

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final AspectDao aspectDao;

  private final int batchSize;
  private final int batchDelayMs;
  private final int limit;

  public AbstractMCPStep(
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
    log.info("{} initialized", id());
  }

  @Nonnull
  protected abstract List<String> getAspectNames();

  protected Urn getUpgradeIdUrn() {
    return BootstrapStep.getUpgradeUrn(id());
  }

  /** Optionally apply an urn-like sql filter, otherwise all urns */
  @VisibleForTesting
  @Nullable
  public abstract String getUrnLike();

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    log.info("Starting {}", id());
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
              .aspectNames(getAspectNames())
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
                  log.info(
                      "Processing batch({}) of size {}.",
                      String.join(",", getAspectNames()),
                      batchSize);

                  AspectsBatch aspectsBatch =
                      AspectsBatchImpl.builder()
                          .retrieverContext(opContext.getRetrieverContext())
                          .items(
                              batch
                                  .<SystemAspect>flatMap(
                                      ebeanAspectV2 ->
                                          EntityUtils.toSystemAspectFromEbeanAspects(
                                              opContext.getRetrieverContext(),
                                              Set.of(ebeanAspectV2))
                                              .stream())
                                  .map(
                                      systemAspect ->
                                          ChangeItemImpl.builder()
                                              .changeType(ChangeType.UPSERT)
                                              .urn(systemAspect.getUrn())
                                              .entitySpec(systemAspect.getEntitySpec())
                                              .aspectName(systemAspect.getAspectName())
                                              .aspectSpec(systemAspect.getAspectSpec())
                                              .recordTemplate(systemAspect.getRecordTemplate())
                                              .auditStamp(systemAspect.getAuditStamp())
                                              .systemMetadata(
                                                  withAppSource(systemAspect.getSystemMetadata()))
                                              .build(opContext.getAspectRetriever()))
                                  .collect(Collectors.toList()))
                          .build(opContext);

                  // re-ingest the aspects to trigger side effects
                  entityService.ingestProposal(opContext, aspectsBatch, true);

                  // record progress
                  Urn lastUrn =
                      aspectsBatch.getItems().stream()
                          .reduce((a, b) -> b)
                          .map(ReadItem::getUrn)
                          .orElse(null);
                  if (lastUrn != null) {
                    log.info("{}: Saving state. Last urn:{}", getUpgradeIdUrn(), lastUrn);
                    context
                        .upgrade()
                        .setUpgradeResult(
                            opContext,
                            getUpgradeIdUrn(),
                            entityService,
                            DataHubUpgradeState.IN_PROGRESS,
                            Map.of(LAST_URN_KEY, lastUrn.toString()));
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

  private static SystemMetadata withAppSource(@Nullable SystemMetadata systemMetadata) {
    SystemMetadata withAppSourceSystemMetadata = null;
    try {
      withAppSourceSystemMetadata =
          systemMetadata != null
              ? new SystemMetadata(systemMetadata.copy().data())
              : new SystemMetadata();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    StringMap properties = withAppSourceSystemMetadata.getProperties();
    StringMap map = properties != null ? new StringMap(properties.data()) : new StringMap();
    map.put(APP_SOURCE, SYSTEM_UPDATE_SOURCE);

    withAppSourceSystemMetadata.setProperties(map);
    return withAppSourceSystemMetadata;
  }
}
