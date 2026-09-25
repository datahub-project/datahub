package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.entity.validation.ValidationApiUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.metadata.utils.arch.OperationContextExempt;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An interface specifying create, update, and read operations against metadata entity aspects. This
 * interface is meant to abstract away the storage concerns of these pieces of metadata, permitting
 * any underlying storage system to be used.
 *
 * <p>Requirements for any implementation: 1. Being able to map its internal storage representation
 * to {@link EntityAspect}; 2. Honor the internal versioning semantics. The latest version of any
 * aspect is set to 0 for efficient retrieval. In most cases only the latest state of an aspect will
 * be fetched. See {@link EntityServiceImpl} for more details.
 *
 * <p>TODO: This interface exposes {@link #runInTransactionWithRetry(OperationContext, Function,
 * int)} (TransactionContext)} because {@link EntityServiceImpl} concerns itself with batching
 * multiple commands into a single transaction. It exposes storage concerns somewhat and it'd be
 * worth looking into ways to move this responsibility inside {@link AspectDao} implementations.
 */
public interface AspectDao {
  String ASPECT_WRITE_COUNT_METRIC_NAME = "aspectWriteCount";
  String ASPECT_WRITE_BYTES_METRIC_NAME = "aspectWriteBytes";

  @Nullable
  EntityAspect getAspect(
      @Nonnull OperationContext opContext,
      @Nonnull final String urn,
      @Nonnull final String aspectName,
      final long version);

  @Nullable
  EntityAspect getAspect(
      @Nonnull OperationContext opContext, @Nonnull final EntityAspectIdentifier key);

  @Nonnull
  Map<EntityAspectIdentifier, EntityAspect> batchGet(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<EntityAspectIdentifier> keys,
      boolean forUpdate);

  @Nonnull
  List<EntityAspect> getAspectsInRange(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      Set<String> aspectNames,
      long startTimeMillis,
      long endTimeMillis);

  /**
   * @param urn urn to fetch
   * @param aspectName aspect to fetch
   * @param forUpdate set to true if the result is used for versioning <a
   *     href="https://ebean.io/docs/query/option#forUpdate">link</a>
   * @return
   */
  @Nullable
  default SystemAspect getLatestAspect(
      @Nonnull OperationContext opContext,
      @Nonnull final String urn,
      @Nonnull final String aspectName,
      boolean forUpdate) {
    return getLatestAspects(opContext, Map.of(urn, Set.of(aspectName)), forUpdate)
        .getOrDefault(urn, Map.of())
        .getOrDefault(aspectName, null);
  }

  /**
   * @param urnAspects urn/aspects to fetch
   * @param forUpdate set to true if the result is used for versioning <a
   *     href="https://ebean.io/docs/query/option#forUpdate">link</a>
   * @return the data
   */
  @Nonnull
  Map<String, Map<String, SystemAspect>> getLatestAspects(
      @Nonnull OperationContext opContext, Map<String, Set<String>> urnAspects, boolean forUpdate);

  /**
   * Updates the system aspect
   *
   * @param operationContext
   * @param txContext transaction context
   * @param aspect the orm model to update
   */
  @Nonnull
  Optional<EntityAspect> updateAspect(
      OperationContext operationContext,
      @Nullable TransactionContext txContext,
      @Nonnull final SystemAspect aspect);

  @OperationContextExempt(reason = "Returns static DAO mode flag, no request context needed")
  default boolean isOptimisticLockingEnabled() {
    return false;
  }

  /**
   * Whether the OL persist loop batches version-0 CAS UPDATEs into one JDBC {@code executeBatch}.
   * Off by default.
   *
   * <p>Contract for implementations: this must return {@code true} ONLY when BOTH {@link
   * #isOptimisticLockingEnabled()} and {@link #isScopedRetryEnabled()} are true. Batching's
   * per-item conflict handling only works on the scoped-retry compute path, so returning {@code
   * true} without scoped retry would let a caller batch on a path that cannot honor the results.
   */
  @OperationContextExempt(reason = "Returns static DAO mode flag, no request context needed")
  default boolean isOptimisticWriteBatchEnabled() {
    return false;
  }

  /** Minimum number of eligible CAS updates to batch; below this the loop stays sequential. */
  @OperationContextExempt(reason = "Returns static DAO config value, no request context needed")
  default int getOptimisticWriteBatchMinSize() {
    return 0;
  }

  /**
   * Whether this DAO, when optimistic locking is enabled, retries only the conflicted URN's branch
   * within the transaction (scoped retry) instead of re-running the whole batch. Default {@code
   * false} keeps the full-batch retry behavior of the optimistic-locking base.
   */
  @OperationContextExempt(reason = "Returns static DAO mode flag, no request context needed")
  default boolean isScopedRetryEnabled() {
    return false;
  }

  @Nonnull
  default Optional<EntityAspect> updateAspectConditional(
      @Nonnull OperationContext operationContext,
      @Nullable TransactionContext txContext,
      @Nonnull final SystemAspect aspect,
      @Nullable String expectedSystemMetadataVersion) {
    throw new UnsupportedOperationException(
        "Optimistic locking conditional update is not supported by this AspectDao");
  }

  /**
   * Batch CAS updates. Result ordering matches input ordering exactly. Implementation wraps in
   * {@code txnFactory.runInScope(...)} for routing parity with single-row {@link
   * #updateAspectConditional}.
   *
   * @param opContext operation context
   * @param txContext transaction context
   * @param updates list of CAS updates
   * @return list of per-item outcomes (UPDATED / CONFLICT), same length as input, ordering
   *     preserved
   * @throws jakarta.persistence.PersistenceException on any non-1/0 JDBC batch result (a thrown
   *     BatchUpdateException, -3 EXECUTE_FAILED, or -2 SUCCESS_NO_INFO). In all of these the
   *     per-row outcome is unknown and rows may already have applied, so an in-txn sequential
   *     re-CAS is unsafe (it would false-conflict an already-applied row). The caller must NOT
   *     catch-and- continue: the outer transaction rolls back and retries on the sequential
   *     (non-batched) path.
   */
  @Nonnull
  default List<ConditionalUpdateResult> updateAspectsConditionalBatch(
      @Nonnull OperationContext opContext,
      @Nullable TransactionContext txContext,
      @Nonnull List<ConditionalAspectUpdate> updates) {
    throw new UnsupportedOperationException(
        "Optimistic locking batch conditional update is not supported by this AspectDao");
  }

  /**
   * Insert system aspect, returning the inserted aspect which may be different from the input
   * aspect, having been replaced with an ORM variation.
   *
   * @param operationContext
   * @param txContext transaction context
   * @param aspect the aspect to insert
   */
  @Nonnull
  Optional<EntityAspect> insertAspect(
      OperationContext operationContext,
      @Nullable TransactionContext txContext,
      @Nonnull final SystemAspect aspect,
      long version);

  /**
   * Update the latest aspect version 0 and insert the previous version. Returns the updated version
   * 0 aspect which may be different from the input aspect, having been replaced with an ORM
   * variation.
   *
   * @param txContext transaction context
   * @param latestAspect the aspect currently at version 0
   * @param newAspect the new aspect to be inserted or updated at version 0
   * @param maxVersionsToKeep when <= 1, do not insert a new row for the previous version (only
   *     update the existing version 0 row); when > 1, insert previous version as a history row then
   *     update version 0
   */
  default Pair<Optional<EntityAspect>, Optional<EntityAspect>> saveLatestAspect(
      @Nonnull OperationContext opContext,
      @Nullable TransactionContext txContext,
      @Nullable SystemAspect latestAspect,
      @Nonnull SystemAspect newAspect,
      int maxVersionsToKeep) {

    if (newAspect.getSystemMetadataVersion().isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Expected a version in systemMetadata.%s", newAspect.getSystemMetadata()));
    }

    if (latestAspect != null && latestAspect.getDatabaseAspect().isPresent()) {

      SystemAspect currentVersion0 = latestAspect.getDatabaseAspect().get();

      // update version 0 with optional write to version N
      long targetVersion =
          nextVersionResolution(currentVersion0.getSystemMetadata(), newAspect.getSystemMetadata());

      // write version N (from previous database state if the version is modified)
      Optional<EntityAspect> inserted = Optional.empty();
      if (maxVersionsToKeep > 1
          && !newAspect
              .getSystemMetadataVersion()
              .equals(currentVersion0.getSystemMetadataVersion())) {

        inserted =
            insertAspect(
                opContext, txContext, latestAspect.getDatabaseAspect().get(), targetVersion);
      }

      // update version 0
      Optional<EntityAspect> updated = Optional.empty();
      final boolean isNoOp =
          ValidationApiUtils.normalizedEqual(
              currentVersion0.getRecordTemplate(), newAspect.getRecordTemplate());

      // update trace
      newAspect.setSystemMetadata(opContext.withTraceId(newAspect.getSystemMetadata(), true));

      if (!Objects.equals(currentVersion0.getSystemMetadata(), newAspect.getSystemMetadata())
          || !isNoOp) {
        // update no-op used for tracing
        SystemMetadataUtils.setNoOp(newAspect.getSystemMetadata(), isNoOp);
        updated = updateAspect(opContext, txContext, newAspect);
      }

      return Pair.of(inserted, updated);
    } else {
      // initial insert
      newAspect.setSystemMetadata(opContext.withTraceId(newAspect.getSystemMetadata(), false));
      Optional<EntityAspect> inserted =
          insertAspect(opContext, txContext, newAspect, ASPECT_LATEST_VERSION);
      return Pair.of(Optional.empty(), inserted);
    }
  }

  default ConditionalSaveResult saveLatestAspectConditional(
      @Nonnull OperationContext opContext,
      @Nullable TransactionContext txContext,
      @Nullable SystemAspect latestAspect,
      @Nonnull SystemAspect newAspect,
      int maxVersionsToKeep) {

    ConditionalWritePlan plan =
        planConditionalWrite(opContext, latestAspect, newAspect, maxVersionsToKeep);
    return executePlannedWrite(
        opContext, txContext, latestAspect, newAspect, maxVersionsToKeep, plan);
  }

  /**
   * Execute a {@link ConditionalWritePlan} produced by {@link #planConditionalWrite}. Split out so
   * the OL batch path can run the version-0 CAS of many {@link
   * ConditionalWritePlan.Kind#ELIGIBLE_CAS} plans as one JDBC {@code executeBatch}, while executing
   * NOOP / INSERT_NEW / LEGACY_UNCONDITIONAL plans inline through this same code — the sequential
   * and batched flows share one execute path, so they cannot drift.
   */
  default ConditionalSaveResult executePlannedWrite(
      @Nonnull OperationContext opContext,
      @Nullable TransactionContext txContext,
      @Nullable SystemAspect latestAspect,
      @Nonnull SystemAspect newAspect,
      int maxVersionsToKeep,
      @Nonnull ConditionalWritePlan plan) {
    switch (plan.getKind()) {
      case NOOP:
        return new ConditionalSaveResult(
            ConditionalWriteOutcome.SKIPPED_NOOP, Optional.empty(), Optional.empty());
      case LEGACY_UNCONDITIONAL:
        {
          Pair<Optional<EntityAspect>, Optional<EntityAspect>> legacy =
              saveLatestAspect(opContext, txContext, latestAspect, newAspect, maxVersionsToKeep);
          return new ConditionalSaveResult(
              ConditionalWriteOutcome.UPDATED, legacy.getFirst(), legacy.getSecond());
        }
      case INSERT_NEW:
        {
          Optional<EntityAspect> inserted =
              insertAspect(opContext, txContext, newAspect, ASPECT_LATEST_VERSION);
          return new ConditionalSaveResult(
              ConditionalWriteOutcome.UPDATED, Optional.empty(), inserted);
        }
      case ELIGIBLE_CAS:
      default:
        {
          Optional<EntityAspect> updated =
              updateAspectConditional(opContext, txContext, newAspect, plan.getExpectedVersion());
          if (updated.isEmpty()) {
            return new ConditionalSaveResult(
                ConditionalWriteOutcome.CONFLICT, Optional.empty(), Optional.empty());
          }
          Optional<EntityAspect> inserted = applyConditionalHistory(opContext, txContext, plan);
          return new ConditionalSaveResult(ConditionalWriteOutcome.UPDATED, inserted, updated);
        }
    }
  }

  /**
   * Decide how a version-0 write should proceed WITHOUT executing it, so the OL persist loop can
   * batch the CAS UPDATEs of many aspects into one JDBC {@code executeBatch} while keeping inserts
   * / legacy / no-op writes sequential. This is the single source of truth shared by the sequential
   * {@link #saveLatestAspectConditional} and the batched persist path — they cannot drift.
   *
   * <p>MUTATES {@code newAspect}'s systemMetadata (traceId + no-op flag) exactly as the write path
   * requires, so an {@link ConditionalWritePlan.Kind#ELIGIBLE_CAS} plan's CAS and an {@link
   * ConditionalWritePlan.Kind#INSERT_NEW} plan's insert can run {@code newAspect} as-is.
   */
  default ConditionalWritePlan planConditionalWrite(
      @Nonnull OperationContext opContext,
      @Nullable SystemAspect latestAspect,
      @Nonnull SystemAspect newAspect,
      int maxVersionsToKeep) {

    if (newAspect.getSystemMetadataVersion().isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Expected a version in systemMetadata.%s", newAspect.getSystemMetadata()));
    }

    if (latestAspect != null && latestAspect.getDatabaseAspect().isPresent()) {
      SystemAspect currentVersion0 = latestAspect.getDatabaseAspect().get();
      String expectedVersion =
          Optional.ofNullable(currentVersion0.getSystemMetadata())
              .map(SystemMetadata::getVersion)
              .orElse(null);

      if (expectedVersion == null) {
        // Legacy row written before optimistic locking stamped a version. There is nothing to CAS
        // against, so this is an UNCONDITIONAL last-writer-wins update — CAS does NOT guard these
        // rows, and concurrent writers to the same legacy URN can clobber each other until a write
        // stamps a version. The write gate (when enabled) still serializes them; with no gate,
        // legacy rows behave exactly as they did before OL. One-time, self-healing: the next write
        // stamps a version and subsequent writes take the CAS path below.
        return ConditionalWritePlan.of(ConditionalWritePlan.Kind.LEGACY_UNCONDITIONAL);
      }

      long targetVersion =
          nextVersionResolution(currentVersion0.getSystemMetadata(), newAspect.getSystemMetadata());
      boolean isNoOp =
          ValidationApiUtils.normalizedEqual(
              currentVersion0.getRecordTemplate(), newAspect.getRecordTemplate());

      newAspect.setSystemMetadata(opContext.withTraceId(newAspect.getSystemMetadata(), true));
      if (Objects.equals(currentVersion0.getSystemMetadata(), newAspect.getSystemMetadata())
          && isNoOp) {
        incrementOptimisticLockMetric("optimistic_lock_skipped_noop");
        return ConditionalWritePlan.of(ConditionalWritePlan.Kind.NOOP);
      }

      SystemMetadataUtils.setNoOp(newAspect.getSystemMetadata(), isNoOp);
      boolean needsHistory =
          maxVersionsToKeep > 1
              && !newAspect
                  .getSystemMetadataVersion()
                  .equals(currentVersion0.getSystemMetadataVersion());
      return ConditionalWritePlan.eligibleCas(
          expectedVersion, currentVersion0, targetVersion, needsHistory);
    }

    newAspect.setSystemMetadata(opContext.withTraceId(newAspect.getSystemMetadata(), false));
    return ConditionalWritePlan.of(ConditionalWritePlan.Kind.INSERT_NEW);
  }

  /**
   * Apply the deferred version-N history row after a WINNING conditional update. A no-op unless the
   * plan is an {@link ConditionalWritePlan.Kind#ELIGIBLE_CAS} that needs history. Uses the pre-CAS
   * {@code oldDbAspect} captured on the plan, so the archived content is correct even though the
   * CAS has already overwritten version 0. Shared by the sequential and batched flows.
   */
  default Optional<EntityAspect> applyConditionalHistory(
      @Nonnull OperationContext opContext,
      @Nullable TransactionContext txContext,
      @Nonnull ConditionalWritePlan plan) {
    if (plan.getKind() == ConditionalWritePlan.Kind.ELIGIBLE_CAS
        && plan.isNeedsHistory()
        && plan.getOldDbAspect() != null) {
      return insertAspect(opContext, txContext, plan.getOldDbAspect(), plan.getTargetVersion());
    }
    return Optional.empty();
  }

  private long nextVersionResolution(
      @Nullable SystemMetadata currentSystemMetadata, @Nullable SystemMetadata newSystemMetadata) {
    // existing row 0 should have at least version 1
    long currentVersion =
        Optional.ofNullable(currentSystemMetadata)
            .map(SystemMetadata::getVersion)
            .map(Long::valueOf)
            .orElse(1L);
    // new row should have at least version 2 with the expected current version one behind
    long expectedCurrentVersion =
        Optional.ofNullable(newSystemMetadata)
                .map(SystemMetadata::getVersion)
                .map(Long::valueOf)
                .orElse(2L)
            - 1;

    // reconcile the target version based on the max of the two values in case of previous
    // corruption
    long targetVersion = Math.max(1, Math.max(currentVersion, expectedCurrentVersion));

    // if mismatch, fix row 0 metadata
    if (currentVersion != targetVersion) {
      currentSystemMetadata.setVersion(String.valueOf(targetVersion), SetMode.DISALLOW_NULL);
    }

    return targetVersion;
  }

  void deleteAspect(
      OperationContext operationContext,
      @Nonnull final Urn urn,
      @Nonnull final String aspect,
      @Nonnull final Long version);

  @Nonnull
  ListResult<String> listUrns(
      OperationContext operationContext,
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final int start,
      final int pageSize);

  @Nonnull
  Integer countAspect(
      OperationContext operationContext,
      @Nonnull final String aspectName,
      @Nullable String urnLike);

  @Nonnull
  Integer countAspect(OperationContext operationContext, final RestoreIndicesArgs args);

  /**
   * Stream latest-version aspect rows matching {@code args}, ordered by URN/aspect, and hand the
   * lazily-fetched {@link PartitionedStream} to {@code consumer} to process.
   *
   * <p>The stream is <b>consume-in-scope</b>: {@code consumer} runs while a {@link
   * ScopedTransactionFactory} routing scope is open, and the stream is closed when it returns. This
   * is load-bearing when an extension routes queries to different backend databases — {@code
   * findStream()} pulls rows lazily as the stream is consumed, so the cursor's connection has to
   * stay routed for the whole consumption. Returning a live stream to the caller (as the previous
   * signature did) let the scope close before the rows were fetched, so later batches could run on
   * an unrouted connection already returned to the pool. Do not stash the {@link PartitionedStream}
   * for use after {@code consumer} returns.
   *
   * @param consumer processes the partitioned stream and returns a result; must fully consume it
   *     before returning
   * @return whatever {@code consumer} returns
   */
  @Nullable
  <R> R streamAspectBatches(
      @Nonnull OperationContext opContext,
      @Nonnull final RestoreIndicesArgs args,
      @Nonnull final Function<PartitionedStream<EbeanAspectV2>, R> consumer);

  /**
   * Stream latest-version (v0) rows for the given aspects ordered by creation time ascending,
   * filtering to only rows whose stored {@code schemaVersion} has not yet reached the per-aspect
   * target version. Intended for cross-aspect migration sweeps where oldest data is migrated first.
   *
   * <p>Only aspects whose target version is strictly greater than {@link
   * com.linkedin.metadata.Constants#DEFAULT_SCHEMA_VERSION} are included; aspects mapped to the
   * default version are ignored.
   *
   * <p>A row is included when its stored {@code schemaVersion} is absent/null or differs from the
   * target version for that aspect.
   *
   * <p>Cursor-based: pass {@code afterCreatedOnMs = 0} to start from the beginning, or the epoch-ms
   * value of the last processed batch's {@code createdon} to resume. Rows already at the target
   * version are excluded by the DB filter, so re-visiting rows at the resumed timestamp is safe.
   *
   * @param aspectTargetVersions map of aspect name to its target schema version; aspects with
   *     target == DEFAULT_SCHEMA_VERSION are skipped
   * @param afterCreatedOnMs epoch-ms cursor; 0 means no lower bound
   * @param batchSize hint for fetch size
   * @param limit overall row cap; 0 means unlimited
   * @return partitioned stream of matching rows
   */
  @Nonnull
  @OperationContextExempt(
      reason =
          "TODO: Needs a bigger refactor, will be handled later. Streams need to follow a consumer pattern")
  PartitionedStream<EbeanAspectV2> streamAspectBatchesForMigration(
      @Nonnull Map<String, Long> aspectTargetVersions,
      long afterCreatedOnMs,
      int batchSize,
      int limit);

  /**
   * Stream aspects for the given entity / aspect name.
   *
   * @param entityName the entity name (urn-type prefix)
   * @param aspectName the aspect to stream
   * @return stream of aspects; caller is responsible for closing
   */
  @Nonnull
  @OperationContextExempt(
      reason =
          "TODO: Needs a bigger refactor, will be handled later. Streams need to follow a consumer pattern")
  Stream<EntityAspect> streamAspects(@Nonnull String entityName, @Nonnull String aspectName);

  /**
   * Hard-delete all aspects for a urn. Must be called within an enclosing transaction: the ordered
   * {@code FOR UPDATE} lock (and any advisory lock) rely on the thread's active transaction, so
   * invoking this outside one would run them in short-lived auto-commit transactions and defeat the
   * lock ordering. All current call sites route through {@code runInTransactionWithRetry}.
   */
  int deleteUrn(
      @Nonnull OperationContext opContext,
      @Nullable TransactionContext txContext,
      @Nonnull final String urn);

  /**
   * Optionally serialize concurrent writers on the given {@code (urn, aspect)} pairs before any row
   * locks are acquired. Default is a no-op; the Ebean/Postgres implementation may take a
   * transaction-scoped advisory lock per {@code (urn, aspect)} when enabled.
   *
   * <p>Keying on the {@code (urn, aspect)} conflict unit (not the whole entity) matches what CAS
   * and {@code FOR UPDATE} actually contend on: two writers on the same URN but different aspects
   * share no row and must not share a mutex. Whole-entity ops (e.g. {@code deleteUrn}) pass the
   * entity's full aspect key-set so delete↔upsert safety is key-set overlap, not a permanent
   * URN-wide lock on every ingest. Used to prevent lock-order deadlocks between multi-row writes
   * (e.g. logical-model linking) and concurrent hard-deletes touching the same rows.
   */
  default void lockAspectsForWrite(
      @Nonnull OperationContext opContext, @Nonnull Map<String, Set<String>> urnAspects) {
    // no-op by default
  }

  @Nonnull
  ListResult<String> listLatestAspectMetadata(
      OperationContext operationContext,
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final int start,
      final int pageSize);

  @Nonnull
  ListResult<String> listAspectMetadata(
      OperationContext operationContext,
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final long version,
      final int start,
      final int pageSize);

  Map<String, Map<String, Long>> getNextVersions(
      @Nonnull OperationContext opContext,
      @Nonnull Map<String, Set<String>> urnAspectMap,
      boolean lockLatestForWrite);

  default Map<String, Map<String, Long>> getNextVersions(
      @Nonnull OperationContext operationContext,
      @Nullable TransactionContext txContext,
      @Nonnull Map<String, Set<String>> urnAspectMap,
      boolean lockLatestForWrite) {
    return getNextVersions(operationContext, urnAspectMap, lockLatestForWrite);
  }

  default long getNextVersion(
      @Nonnull OperationContext opContext,
      @Nonnull final String urn,
      @Nonnull final String aspectName) {
    return getNextVersions(opContext, Map.of(urn, Set.of(aspectName)), true)
        .get(urn)
        .get(aspectName);
  }

  default Map<String, Long> getNextVersions(
      @Nonnull OperationContext opContext,
      @Nonnull final String urn,
      @Nonnull final Set<String> aspectNames) {
    return getNextVersions(opContext, Map.of(urn, aspectNames), true).get(urn);
  }

  long getMaxVersion(
      OperationContext operationContext,
      @Nonnull final String urn,
      @Nonnull final String aspectName);

  /**
   * Return the min/max version for the given URN & aspect
   *
   * @param operationContext
   * @param urn the urn
   * @param aspectName the aspect
   * @return the range of versions, if they do not exist -1 is returned
   */
  @Nonnull
  Pair<Long, Long> getVersionRange(
      OperationContext operationContext,
      @Nonnull final String urn,
      @Nonnull final String aspectName);

  @OperationContextExempt(reason = "Lifecycle/admin toggle, no actor context needed")
  void setWritable(boolean canWrite);

  @Nonnull
  <T> Optional<T> runInTransactionWithRetry(
      OperationContext operationContext,
      @Nonnull final Function<TransactionContext, TransactionResult<T>> block,
      final int maxTransactionRetry);

  @Nonnull
  default <T> Optional<T> runInTransactionWithRetry(
      OperationContext operationContext,
      @Nonnull final Function<TransactionContext, TransactionResult<T>> block,
      AspectsBatch batch,
      final int maxTransactionRetry) {
    return runInTransactionWithRetry(operationContext, block, maxTransactionRetry);
  }

  default void incrementWriteMetrics(
      OperationContext opContext, String aspectName, long count, long bytes) {
    opContext
        .getMetricUtils()
        .ifPresent(
            metricUtils ->
                metricUtils.increment(
                    this.getClass(),
                    String.join(
                        MetricUtils.DELIMITER, List.of(ASPECT_WRITE_COUNT_METRIC_NAME, aspectName)),
                    count));
    opContext
        .getMetricUtils()
        .ifPresent(
            metricUtils ->
                metricUtils.increment(
                    this.getClass(),
                    String.join(
                        MetricUtils.DELIMITER, List.of(ASPECT_WRITE_BYTES_METRIC_NAME, aspectName)),
                    bytes));
  }

  @OperationContextExempt(reason = "Metrics counter helper, no request context needed")
  default void incrementOptimisticLockMetric(@Nonnull String name) {}

  @Nonnull
  @OperationContextExempt(reason = "Returns static config, no request context needed")
  List<com.linkedin.metadata.aspect.SystemAspectValidator> getSystemAspectValidators();

  @Nullable
  @OperationContextExempt(reason = "Returns static config, no request context needed")
  com.linkedin.metadata.config.AspectSizeValidationConfiguration getValidationConfig();
}
