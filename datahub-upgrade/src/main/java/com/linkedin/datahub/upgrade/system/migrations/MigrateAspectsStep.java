package com.linkedin.datahub.upgrade.system.migrations;

import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.data.template.StringMap;
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
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
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
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * An {@link UpgradeStep} that migrates all aspects registered in {@link #aspectTargetVersions}.
 *
 * <p>Rows are streamed ordered by {@code createdon ASC} across all aspect types. On {@code
 * version=0} rows {@code createdon} is the <b>last-write timestamp</b> (updated on every upsert),
 * not the original entity creation time — the true creation time is preserved in {@code
 * systemMetadata.aspectCreated.time}. The cursor ({@code lastCreatedOnMs}) persisted after each
 * batch allows the sweep to resume from where it left off after a restart.
 *
 * <p>The DB query pre-filters to rows whose stored {@code schemaVersion} has not yet reached the
 * target version for each aspect, so batches contain only rows that actually need work.
 *
 * <p>The three protected methods — {@link #openStream}, {@link #cursorState}, and {@link
 * #loadResumeState} — are structured to mirror a future base class abstraction. When extracted,
 * {@link #openStream} becomes abstract (subclass controls stream source + filter), {@link
 * #cursorState} becomes abstract (subclass controls cursor key), and the rest of {@link
 * #executable} moves to the base.
 */
@Slf4j
public class MigrateAspectsStep implements UpgradeStep {

  static final String LAST_CREATED_ON_MS_KEY = "lastCreatedOnMs";

  static final String STEP_ID_PREFIX = "migrate-aspects-";

  public static String stepId(@Nonnull String upgradeVersion) {
    return STEP_ID_PREFIX + upgradeVersion;
  }

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final AspectDao aspectDao;

  /** Map of aspect name → target schema version. Aspects at DEFAULT_SCHEMA_VERSION are excluded. */
  private final Map<String, Long> aspectTargetVersions;

  private final String stepId;
  private final int batchSize;
  private final int batchDelayMs;
  private final int limit;

  public MigrateAspectsStep(
      @Nonnull OperationContext opContext,
      @Nonnull EntityService<?> entityService,
      @Nonnull AspectDao aspectDao,
      @Nonnull Map<String, Long> aspectTargetVersions,
      @Nonnull String upgradeVersion,
      int batchSize,
      int batchDelayMs,
      int limit) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.aspectDao = aspectDao;
    this.aspectTargetVersions = aspectTargetVersions;
    this.stepId = stepId(upgradeVersion);
    this.batchSize = batchSize;
    this.batchDelayMs = batchDelayMs;
    this.limit = limit;
  }

  @Override
  public String id() {
    return stepId;
  }

  @VisibleForTesting
  protected com.linkedin.common.urn.Urn getUpgradeIdUrn() {
    return BootstrapStep.getUpgradeUrn(id());
  }

  // ---------------------------------------------------------------------------
  // Methods structured for future base-class extraction
  // ---------------------------------------------------------------------------

  /**
   * Opens the stream of rows to process for this sweep.
   *
   * <p><b>Future base contract:</b> abstract — subclass controls stream source, cursor
   * interpretation, and row filter. The base class default will use URN-ordered {@code
   * streamAspectBatchesForMigration} with no filter.
   */
  protected PartitionedStream<EbeanAspectV2> openStream(@Nonnull Map<String, String> resumeState) {
    long afterMs = Long.parseLong(resumeState.getOrDefault(LAST_CREATED_ON_MS_KEY, "0"));
    if (afterMs > 0) {
      log.info("{}: Resuming from createdOn >= {}ms.", id(), afterMs);
    }
    return aspectDao.streamAspectBatchesForMigration(
        aspectTargetVersions, afterMs, batchSize, limit);
  }

  /**
   * Extracts the cursor state to persist after a batch completes.
   *
   * <p><b>Future base contract:</b> abstract — subclass decides which field(s) to save. The base
   * class default will save {@code lastUrn}.
   */
  protected Map<String, String> cursorState(@Nonnull List<EbeanAspectV2> batch) {
    if (batch.isEmpty()) {
      return Map.of();
    }
    EbeanAspectV2 last = batch.get(batch.size() - 1);
    return Map.of(
        LAST_CREATED_ON_MS_KEY, String.valueOf(last.getCreatedOn().toInstant().toEpochMilli()));
  }

  /**
   * Loads resume state from a prior {@code IN_PROGRESS} upgrade result.
   *
   * <p><b>Future base contract:</b> moves to base class unchanged.
   */
  protected Map<String, String> loadResumeState(
      @Nonnull Optional<DataHubUpgradeResult> prevResult) {
    return prevResult
        .filter(r -> DataHubUpgradeState.IN_PROGRESS.equals(r.getState()) && r.getResult() != null)
        .<Map<String, String>>map(DataHubUpgradeResult::getResult)
        .orElse(Map.of());
  }

  // ---------------------------------------------------------------------------

  @Override
  public boolean skip(UpgradeContext context) {
    Optional<DataHubUpgradeResult> prevResult =
        context.upgrade().getUpgradeResult(opContext, getUpgradeIdUrn(), entityService);

    if (prevResult
        .filter(
            r ->
                DataHubUpgradeState.SUCCEEDED.equals(r.getState())
                    || DataHubUpgradeState.ABORTED.equals(r.getState()))
        .isPresent()) {
      log.info("{}: Previous result was {}; skipping.", id(), prevResult.get().getState());
      return true;
    }

    return false;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    log.info(
        "{}: Starting migration sweep across {} aspect(s).", id(), aspectTargetVersions.size());
    return context -> {
      Optional<DataHubUpgradeResult> prevResult =
          context.upgrade().getUpgradeResult(opContext, getUpgradeIdUrn(), entityService);

      Map<String, String> resumeState = loadResumeState(prevResult);

      java.util.concurrent.atomic.AtomicLong totalMigrated =
          new java.util.concurrent.atomic.AtomicLong(0);

      try (PartitionedStream<EbeanAspectV2> stream = openStream(resumeState)) {
        stream
            .partition(batchSize)
            .forEach(
                rawBatch -> {
                  List<EbeanAspectV2> batchList = rawBatch.collect(Collectors.toList());

                  List<ChangeItemImpl> items =
                      batchList.stream()
                          .flatMap(
                              ea ->
                                  EntityUtils.toSystemAspectFromEbeanAspects(
                                      opContext.getRetrieverContext(), Set.of(ea))
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
                          .collect(Collectors.toList());

                  if (!items.isEmpty()) {
                    entityService.ingestProposal(
                        opContext,
                        AspectsBatchImpl.builder()
                            .retrieverContext(opContext.getRetrieverContext())
                            .items(items)
                            .build(opContext),
                        true);
                    totalMigrated.addAndGet(items.size());
                    log.info("{}: Migrated {} aspect(s) in batch.", id(), items.size());
                  }

                  Map<String, String> cursor = cursorState(batchList);
                  if (!cursor.isEmpty()) {
                    log.info("{}: Saving state. Cursor: {}", id(), cursor);
                    context
                        .upgrade()
                        .setUpgradeResult(
                            opContext,
                            getUpgradeIdUrn(),
                            entityService,
                            DataHubUpgradeState.IN_PROGRESS,
                            cursor);
                  }

                  if (batchDelayMs > 0) {
                    try {
                      Thread.sleep(batchDelayMs);
                    } catch (InterruptedException e) {
                      throw new RuntimeException(e);
                    }
                  }
                });
      }

      log.info("{}: Sweep complete. Total migrated: {}.", id(), totalMigrated.get());
      BootstrapStep.setUpgradeResult(opContext, getUpgradeIdUrn(), entityService);
      context.report().addLine("State updated: " + getUpgradeIdUrn());
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  private static SystemMetadata withAppSource(@Nullable SystemMetadata systemMetadata) {
    SystemMetadata result;
    try {
      result =
          systemMetadata != null
              ? new SystemMetadata(systemMetadata.copy().data())
              : new SystemMetadata();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    StringMap properties = result.getProperties();
    StringMap map = properties != null ? new StringMap(properties.data()) : new StringMap();
    map.put(APP_SOURCE, SYSTEM_UPDATE_SOURCE);
    result.setProperties(map);
    return result;
  }
}
