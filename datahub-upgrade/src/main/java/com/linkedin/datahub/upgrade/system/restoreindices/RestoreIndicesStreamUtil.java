package com.linkedin.datahub.upgrade.system.restoreindices;

import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Shared streaming reindex logic. Streams every persisted aspect matching {@code aspectName}
 * (optionally filtered by {@code urnLike}) in keyset-paginated batches and produces one MCL per
 * aspect via {@link EntityService#alwaysProduceMCLAsync} to rebuild the search and graph documents.
 *
 * <p>Both {@code AbstractMCLStep} and the legacy restore-indices steps call this so the scan lives
 * in one place. The overload without {@code runId}/{@code resumeUrn}/{@code onBatchLastUrn} is for
 * callers that own their own idempotency and don't need resume/progress bookkeeping.
 */
@Slf4j
public final class RestoreIndicesStreamUtil {

  private RestoreIndicesStreamUtil() {}

  public static void reindexAspect(
      @Nonnull final OperationContext opContext,
      @Nonnull final EntityService<?> entityService,
      @Nonnull final AspectDao aspectDao,
      @Nonnull final String aspectName,
      @Nullable final String urnLike,
      final int batchSize,
      final int batchDelayMs,
      final int limit,
      @Nonnull final ChangeType changeType) {
    reindexAspect(
        opContext,
        entityService,
        aspectDao,
        aspectName,
        urnLike,
        batchSize,
        batchDelayMs,
        limit,
        changeType,
        null,
        null,
        null);
  }

  /**
   * @param runId when non-null, stamped onto each produced aspect's system metadata (with a fresh
   *     lastObserved) — used by {@code AbstractMCLStep} to tag the run.
   * @param resumeUrn when non-null, resumes urn-based pagination from this urn.
   * @param onBatchLastUrn when non-null, invoked after each batch with the last processed urn —
   *     used by {@code AbstractMCLStep} to persist resume state.
   */
  public static void reindexAspect(
      @Nonnull final OperationContext opContext,
      @Nonnull final EntityService<?> entityService,
      @Nonnull final AspectDao aspectDao,
      @Nonnull final String aspectName,
      @Nullable final String urnLike,
      final int batchSize,
      final int batchDelayMs,
      final int limit,
      @Nonnull final ChangeType changeType,
      @Nullable final String runId,
      @Nullable final String resumeUrn,
      @Nullable final Consumer<Urn> onBatchLastUrn) {

    RestoreIndicesArgs args =
        new RestoreIndicesArgs()
            .aspectName(aspectName)
            .batchSize(batchSize)
            .lastUrn(resumeUrn)
            .urnBasedPagination(resumeUrn != null)
            .limit(limit);
    if (urnLike != null) {
      args = args.urnLike(urnLike);
    }

    try (PartitionedStream<EbeanAspectV2> stream = aspectDao.streamAspectBatches(opContext, args)) {
      stream
          .partition(args.batchSize)
          .forEach(
              batch -> {
                log.info("Reindexing batch({}) of size {}.", aspectName, batchSize);

                List<Pair<Future<?>, SystemAspect>> futures =
                    EntityUtils.toSystemAspectFromEbeanAspects(
                            opContext,
                            opContext.getRetrieverContext(),
                            batch.collect(Collectors.toList()))
                        .stream()
                        .map(
                            (SystemAspect systemAspect) -> {
                              SystemMetadata systemMetadata = systemAspect.getSystemMetadata();
                              if (runId != null) {
                                systemMetadata =
                                    systemMetadata
                                        .setRunId(runId)
                                        .setLastObserved(System.currentTimeMillis());
                              }
                              Pair<Future<?>, Boolean> future =
                                  entityService.alwaysProduceMCLAsync(
                                      opContext,
                                      systemAspect.getUrn(),
                                      systemAspect.getUrn().getEntityType(),
                                      aspectName,
                                      systemAspect.getAspectSpec(),
                                      null,
                                      systemAspect.getRecordTemplate(),
                                      null,
                                      systemMetadata,
                                      AuditStampUtils.createDefaultAuditStamp(),
                                      changeType);
                              return Pair.<Future<?>, SystemAspect>of(
                                  future.getFirst(), systemAspect);
                            })
                        .collect(Collectors.toList());

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

                if (onBatchLastUrn != null && lastAspect != null) {
                  onBatchLastUrn.accept(lastAspect.getUrn());
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
  }
}
