package com.linkedin.datahub.upgrade.loadindices;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.BackfillHeaders;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Pages the latest aspects out of primary storage in key order, converts them to RESTATE change
 * logs stamped for a V3 baseline or catch-up backfill, and writes each page through the
 * update-indices strategies. Shared by the operator-driven {@link LoadIndicesStep} and the
 * upgrade-driven entity index fill, which differ only in how they scope the run and where they send
 * the batches. Reads go through the deployment's {@link AspectDao}, so primary-storage routing is
 * whatever the process was wired with; nothing here opens a connection of its own.
 *
 * <p>Each page is its own bounded query seeking from the previous page's last (urn, aspect), so
 * memory holds one page rather than the whole result set and no statement stays open across the
 * write. Rows for one entity are contiguous in key order, so a page that fills its limit mid-entity
 * fetches the rest of that entity; every entity a page emits is whole. When the stream carries no
 * aspect or time filter, those events are stamped {@link
 * BackfillHeaders#COMPLETE_ENTITY_HEADER_NAME} so a writer that replaces whole documents can take
 * the batch as the entity's stored state.
 */
@Slf4j
public class AspectIndexLoader {

  public static final int DEFAULT_BATCH_SIZE = 10000;

  /** Rows per partition when materialising the tail of one entity; far above any aspect count. */
  private static final int REST_OF_ENTITY_PARTITION = 1000;

  /** Receives each written batch: its last URN and the cumulative counts for this run. */
  @FunctionalInterface
  public interface BatchListener {
    void onBatch(@Nonnull String lastUrn, long rowsProcessed, long urnsProcessed);
  }

  /** Where a batch of change logs is written. */
  @FunctionalInterface
  public interface BatchWriter {
    void write(@Nonnull OperationContext opContext, @Nonnull List<MetadataChangeLog> batch);
  }

  /**
   * Thrown after the batch that crosses {@link LoadIndicesArgs#deadlineEpochMs} has been written
   * and reported to the listener, so the caller's checkpoint is consistent with what landed.
   */
  public static final class DeadlineExceeded extends RuntimeException {
    private final String lastUrn;
    private final long rowsProcessed;

    public DeadlineExceeded(@Nonnull final String lastUrn, final long rowsProcessed) {
      super(
          String.format(
              "Deadline reached after %d aspects; last urn processed: %s", rowsProcessed, lastUrn));
      this.lastUrn = lastUrn;
      this.rowsProcessed = rowsProcessed;
    }

    @Nonnull
    public String lastUrn() {
      return lastUrn;
    }

    public long rowsProcessed() {
      return rowsProcessed;
    }
  }

  private final AspectDao aspectDao;
  private final UpdateIndicesService updateIndicesService;

  public AspectIndexLoader(
      @Nonnull final AspectDao aspectDao,
      @Nonnull final UpdateIndicesService updateIndicesService) {
    this.aspectDao = aspectDao;
    this.updateIndicesService = updateIndicesService;
  }

  /**
   * Streams and writes every aspect selected by {@code args}. Batches go to {@code writer}; after
   * each batch {@code listener} (if any) sees the last URN and the cumulative counts. The bulk
   * processor is drained before returning or throwing, so a batch reported to the listener has
   * landed.
   */
  public LoadIndicesResult load(
      OperationContext opContext,
      LoadIndicesArgs args,
      Function<String, Void> reportFunction,
      @Nullable BatchListener listener,
      @Nonnull BatchWriter writer) {
    args.v3BackfillMode = normalizeV3BackfillMode(args.v3BackfillMode);
    LoadIndicesResult result = new LoadIndicesResult();
    long totalStartTime = System.currentTimeMillis();
    RuntimeException processingFailure = null;
    int totalProcessed = 0;
    long urnsProcessed = 0;
    String previousUrn = null;

    try {

      int batchSize = args.batchSize > 0 ? args.batchSize : DEFAULT_BATCH_SIZE;
      final int limit = args.limit;

      final long totalRecords;
      if (limit == Integer.MAX_VALUE) {
        reportFunction.apply("No limit specified - counting total aspects for ETA calculation...");
        long countStartTime = System.currentTimeMillis();

        RestoreIndicesArgs countArgs = convertToRestoreIndicesArgs(args, 0);
        if (args.lastUrn != null) {
          countArgs.urnBasedPagination = true;
          countArgs.lastUrn = args.lastUrn;
        }
        totalRecords = aspectDao.countAspect(opContext, countArgs);
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

      final boolean wholeEntities = streamsWholeEntities(args);
      // Both "no limit" spellings the callers use; the DAO reads 0 as unbounded too.
      final boolean unbounded = limit <= 0 || limit == Integer.MAX_VALUE;
      String cursorUrn = args.lastUrn;
      // Empty: the first page after a checkpoint re-reads the checkpointed entity from its first
      // row, so a resume never trusts a partially written entity.
      String cursorAspect = "";
      boolean more = true;
      while (more) {
        final int remaining = unbounded ? Integer.MAX_VALUE : limit - totalProcessed;
        if (remaining <= 0) {
          break;
        }
        final int pageLimit = Math.min(batchSize, remaining);
        final long pageStartTime = System.currentTimeMillis();
        final List<EbeanAspectV2> page =
            fetchPage(opContext, args, pageLimit, cursorUrn, cursorAspect);
        if (page.isEmpty()) {
          result.timeSqlQueryMs += System.currentTimeMillis() - pageStartTime;
          break;
        }
        final boolean pageFull = page.size() >= pageLimit;
        // The run's row cap, not the page size, cut this page: honour the cap and leave its last
        // entity to hydration rather than fetching the rest of it.
        final boolean cappedByLimit =
            pageFull && !unbounded && totalProcessed + page.size() >= limit;
        if (pageFull && !cappedByLimit) {
          // LIMIT may have cut the last entity mid-way; its remaining aspects are contiguous in
          // key order, so one point-range query completes it. A page is therefore at most
          // batchSize plus one entity's aspects, and every entity it holds is whole.
          page.addAll(fetchRestOfEntity(opContext, args, page.get(page.size() - 1)));
        }
        result.timeSqlQueryMs += System.currentTimeMillis() - pageStartTime;
        // A short page is the end of the data.
        more = pageFull;
        final EbeanAspectV2 lastRow = page.get(page.size() - 1);
        final String lastUrn = lastRow.getKey().getUrn();
        final boolean lastEntityComplete = !cappedByLimit;

        List<MetadataChangeLog> mclBatch = new ArrayList<>(page.size());
        for (EbeanAspectV2 aspect : page) {
          final boolean completeEntity =
              wholeEntities && (lastEntityComplete || !lastUrn.equals(aspect.getKey().getUrn()));
          try {
            mclBatch.add(convertToMetadataChangeLog(opContext, aspect, args, completeEntity));
          } catch (Exception e) {
            if (args.skipConversionErrors) {
              // Escape hatch (default off): keep the run going past a single corrupt aspect.
              // Counted and reported so the operator sees the skip; the aspect is absent from the
              // rebuilt index until it is re-ingested or repaired.
              result.conversionErrorsSkipped++;
              reportFunction.apply(
                  String.format(
                      "Skipped aspect conversion error urn=%s, aspect=%s: %s",
                      aspect.getKey().getUrn(), aspect.getKey().getAspect(), e.getMessage()));
              continue;
            }
            throw new RuntimeException(
                String.format(
                    "Failed to convert aspect urn=%s, aspect=%s",
                    aspect.getKey().getUrn(), aspect.getKey().getAspect()),
                e);
          }
        }

        if (!mclBatch.isEmpty()) {
          writeBatchWithRetry(opContext, mclBatch, reportFunction, writer, 0);
          // The listener may checkpoint this page. Drain first, so that cursor names only
          // documents the bulk processor has acknowledged.
          updateIndicesService.flushAndWait();
          totalProcessed += page.size();
          // Rows arrive in key order, so a URN change is a new document.
          for (EbeanAspectV2 aspect : page) {
            String urn = aspect.getKey().getUrn();
            if (!urn.equals(previousUrn)) {
              urnsProcessed++;
              previousUrn = urn;
            }
          }

          long elapsedTime = System.currentTimeMillis() - totalStartTime;
          double aspectsPerSecond = (double) totalProcessed / (elapsedTime / 1000.0);
          String progressMessage =
              String.format(
                  "Processed %d aspects - %.1f aspects/sec", totalProcessed, aspectsPerSecond);
          if (totalRecords > 0 && aspectsPerSecond > 0 && totalProcessed > 50000) {
            long remainingAspects;
            if (limit != Integer.MAX_VALUE) {
              remainingAspects = Math.min(limit, totalRecords) - totalProcessed;
            } else {
              remainingAspects = totalRecords - totalProcessed;
            }
            if (remainingAspects > 0) {
              long estimatedRemainingMs = (long) (remainingAspects / aspectsPerSecond * 1000);
              long estimatedRemainingMinutes = estimatedRemainingMs / 60000;
              long estimatedRemainingSeconds = (estimatedRemainingMs % 60000) / 1000;
              int progressPercent = (int) ((totalProcessed * 100L) / totalRecords);
              progressMessage +=
                  String.format(
                      " - Progress: %d%% - ETA: %dm %ds",
                      progressPercent, estimatedRemainingMinutes, estimatedRemainingSeconds);
            }
          }
          reportFunction.apply(progressMessage);
          reportFunction.apply("Last URN processed: " + lastUrn);

          if (listener != null) {
            listener.onBatch(lastUrn, totalProcessed, urnsProcessed);
          }
          if (args.deadlineEpochMs != null && System.currentTimeMillis() > args.deadlineEpochMs) {
            throw new DeadlineExceeded(lastUrn, totalProcessed);
          }
        }
        cursorUrn = lastUrn;
        cursorAspect = lastRow.getKey().getAspect();
      }

      result.timeElasticsearchWriteMs =
          System.currentTimeMillis() - totalStartTime - result.timeSqlQueryMs;
    } catch (Exception e) {
      processingFailure =
          e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
    }

    // Always drain the bulk processors, including after a processing failure, so partially
    // buffered writes surface here rather than in the next run.
    try {
      updateIndicesService.flushAndWait();
    } catch (RuntimeException drainFailure) {
      if (processingFailure != null) {
        processingFailure.addSuppressed(drainFailure);
      } else {
        processingFailure = drainFailure;
      }
    }

    if (processingFailure != null) {
      throw processingFailure;
    }

    result.rowsProcessed = totalProcessed;
    result.urnsProcessed = urnsProcessed;
    long totalTimeMs = System.currentTimeMillis() - totalStartTime;
    double finalThroughput = (double) result.rowsProcessed / (totalTimeMs / 1000.0);
    reportFunction.apply(
        String.format(
            "Processing completed: %d aspects processed - Final throughput: %.1f aspects/sec",
            result.rowsProcessed, finalThroughput));
    if (result.conversionErrorsSkipped > 0) {
      reportFunction.apply(
          String.format(
              "WARNING: skipConversionErrors skipped %d aspect(s) that failed MCL conversion; those"
                  + " documents are missing from the rebuilt index until re-ingested or repaired.",
              result.conversionErrorsSkipped));
    }
    return result;
  }

  private void writeBatchWithRetry(
      OperationContext opContext,
      List<MetadataChangeLog> batch,
      Function<String, Void> reportFunction,
      BatchWriter writer,
      int retryCount) {
    final int maxRetries = 3;

    try {
      writer.write(opContext, batch);
      if (retryCount > 0) {
        log.info(
            "Successfully wrote batch of {} MCL events after {} retries", batch.size(), retryCount);
      }
      return;
    } catch (Exception e) {
      log.warn(
          "Failed to write batch of {} MCL events (attempt {}): {}",
          batch.size(),
          retryCount + 1,
          e.getMessage());

      if (batch.size() == 1) {
        if (retryCount >= maxRetries) {
          MetadataChangeLog event = batch.get(0);
          String failureMessage =
              String.format(
                  "Failed to write MCL event urn=%s, aspect=%s after %d attempts",
                  event.getEntityUrn(), event.getAspectName(), retryCount + 1);
          reportFunction.apply(failureMessage);
          throw new RuntimeException(failureMessage, e);
        }
        writeBatchWithRetry(opContext, batch, reportFunction, writer, retryCount + 1);
        return;
      }

      int splitPoint = batch.size() / 2;
      List<MetadataChangeLog> firstHalf = new ArrayList<>(batch.subList(0, splitPoint));
      List<MetadataChangeLog> secondHalf = new ArrayList<>(batch.subList(splitPoint, batch.size()));
      log.info(
          "Splitting failed batch of {} MCL events into two batches of {} and {} for retry",
          batch.size(),
          firstHalf.size(),
          secondHalf.size());

      RuntimeException firstFailure = null;
      try {
        writeBatchWithRetry(opContext, firstHalf, reportFunction, writer, 0);
      } catch (RuntimeException splitFailure) {
        firstFailure = splitFailure;
      }

      RuntimeException secondFailure = null;
      try {
        writeBatchWithRetry(opContext, secondHalf, reportFunction, writer, 0);
      } catch (RuntimeException splitFailure) {
        secondFailure = splitFailure;
      }

      if (firstFailure != null) {
        if (secondFailure != null) {
          firstFailure.addSuppressed(secondFailure);
        }
        throw firstFailure;
      }
      if (secondFailure != null) {
        throw secondFailure;
      }
    }
  }

  private MetadataChangeLog convertToMetadataChangeLog(
      OperationContext opContext,
      EbeanAspectV2 ebeanAspect,
      LoadIndicesArgs args,
      final boolean completeEntity)
      throws Exception {
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

    MetadataChangeLog metadataChangeLog =
        PegasusUtils.constructMCL(
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
    StringMap headers =
        metadataChangeLog.hasHeaders() && metadataChangeLog.getHeaders() != null
            ? new StringMap(metadataChangeLog.getHeaders().data())
            : new StringMap();
    String backfillMode = normalizeV3BackfillMode(args.v3BackfillMode);
    headers.put(BackfillHeaders.MODE_HEADER_NAME, backfillMode);
    if (BackfillHeaders.MODE_BASELINE.equals(backfillMode)) {
      if (args.v3TargetIndex != null && !args.v3TargetIndex.isEmpty()) {
        headers.put(BackfillHeaders.TARGET_INDEX_HEADER_NAME, args.v3TargetIndex);
      }
      if (completeEntity) {
        headers.put(BackfillHeaders.COMPLETE_ENTITY_HEADER_NAME, Boolean.TRUE.toString());
      }
    }
    metadataChangeLog.setHeaders(headers);
    return metadataChangeLog;
  }

  static String normalizeV3BackfillMode(@Nullable final String mode) {
    String normalizedMode =
        mode == null ? BackfillHeaders.MODE_BASELINE : mode.trim().toLowerCase(Locale.ROOT);
    if (!BackfillHeaders.MODE_BASELINE.equals(normalizedMode)
        && !BackfillHeaders.MODE_CATCH_UP.equals(normalizedMode)) {
      throw new IllegalArgumentException(
          String.format(
              "%s must be '%s' or '%s'",
              LoadIndices.V3_BACKFILL_MODE_ARG_NAME,
              BackfillHeaders.MODE_BASELINE,
              BackfillHeaders.MODE_CATCH_UP));
    }
    return normalizedMode;
  }

  /**
   * Default aspect names from the entity registry: every aspect with searchable fields plus the key
   * aspect of each entity that has at least one searchable aspect.
   */
  public Set<String> getDefaultAspectNames(OperationContext opContext) {
    Set<String> aspectNames = new HashSet<>();

    for (String entityName : opContext.getEntityRegistry().getEntitySpecs().keySet()) {
      try {
        EntitySpec entitySpec = opContext.getEntityRegistry().getEntitySpec(entityName);

        boolean entityHasSearchableAspects = false;

        for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
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
  /**
   * One page: at most {@code pageLimit} rows strictly after the cursor in (urn, aspect) order. An
   * empty cursor aspect means "from the cursor URN's first row", which is how a checkpointed URN is
   * re-read whole on resume.
   */
  private List<EbeanAspectV2> fetchPage(
      @Nonnull final OperationContext opContext,
      @Nonnull final LoadIndicesArgs args,
      final int pageLimit,
      @Nullable final String cursorUrn,
      @Nonnull final String cursorAspect) {
    RestoreIndicesArgs pageArgs = convertToRestoreIndicesArgs(args, pageLimit);
    if (cursorUrn != null) {
      pageArgs.urnBasedPagination = true;
      pageArgs.lastUrn = cursorUrn;
      pageArgs.lastAspect = cursorAspect;
    }
    return fetchRows(opContext, args, pageArgs, pageLimit);
  }

  /** The aspects of {@code lastRow}'s entity that sort after it: a point range on the key. */
  private List<EbeanAspectV2> fetchRestOfEntity(
      @Nonnull final OperationContext opContext,
      @Nonnull final LoadIndicesArgs args,
      @Nonnull final EbeanAspectV2 lastRow) {
    RestoreIndicesArgs restArgs = convertToRestoreIndicesArgs(args, 0);
    restArgs.urn = lastRow.getKey().getUrn();
    restArgs.urnBasedPagination = true;
    restArgs.lastUrn = lastRow.getKey().getUrn();
    restArgs.lastAspect = lastRow.getKey().getAspect();
    return fetchRows(opContext, args, restArgs, REST_OF_ENTITY_PARTITION);
  }

  /**
   * Runs one bounded query and materialises its rows. The consumer only collects, so the DAO's rule
   * for isolation-scoped streams (no nested queries inside the consumer) holds, and processing
   * happens after the statement and its transaction have closed.
   */
  private List<EbeanAspectV2> fetchRows(
      @Nonnull final OperationContext opContext,
      @Nonnull final LoadIndicesArgs args,
      @Nonnull final RestoreIndicesArgs query,
      final int partitionSize) {
    return aspectDao.streamAspectBatches(
        opContext,
        query,
        args.txIsolation,
        stream ->
            stream
                .partition(partitionSize)
                .flatMap(Function.identity())
                .collect(Collectors.toList()));
  }

  /**
   * Whether every page carries each entity's stored aspects in full: a baseline stream with no
   * aspect or time filter. Only then may events be stamped as a complete entity.
   */
  private static boolean streamsWholeEntities(@Nonnull final LoadIndicesArgs args) {
    return BackfillHeaders.MODE_BASELINE.equals(args.v3BackfillMode)
        && (args.aspectNames == null || args.aspectNames.isEmpty())
        && (args.gePitEpochMs == null || args.gePitEpochMs <= 0)
        && (args.lePitEpochMs == null || args.lePitEpochMs <= 0);
  }

  private static RestoreIndicesArgs convertToRestoreIndicesArgs(LoadIndicesArgs args, int limit) {
    RestoreIndicesArgs restoreArgs = new RestoreIndicesArgs();

    Collection<String> aspectNames = args.aspectNames;
    if (aspectNames != null && !aspectNames.isEmpty()) {
      restoreArgs.aspectNames = new ArrayList<>(aspectNames);
    }
    restoreArgs.urnLike = args.urnLike;
    restoreArgs.gePitEpochMs = args.gePitEpochMs != null ? args.gePitEpochMs : 0L;
    restoreArgs.lePitEpochMs = args.lePitEpochMs != null ? args.lePitEpochMs : 0L;
    restoreArgs.limit = limit;
    return restoreArgs;
  }
}
