package com.linkedin.metadata.entity.ebean;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.DEFAULT_SCHEMA_VERSION;
import static com.linkedin.metadata.Constants.READ_ONLY_LOG;

import com.codahale.metrics.MetricRegistry;
import com.datahub.util.exception.DatabaseTransactionConflictException;
import com.datahub.util.exception.ModelConversionException;
import com.datahub.util.exception.RetryLimitReached;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.SystemAspectValidator;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.config.AspectSizeValidationConfiguration;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.TransactionRetryConfiguration;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.AspectMigrationsDao;
import com.linkedin.metadata.entity.AspectWriteDisabledException;
import com.linkedin.metadata.entity.ConditionalAspectUpdate;
import com.linkedin.metadata.entity.ConditionalUpdateResult;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.entity.OptimisticLockConflictException;
import com.linkedin.metadata.entity.TransactionContext;
import com.linkedin.metadata.entity.TransactionResult;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.entity.storage.PrimaryStorageResolver;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.ExtraInfoArray;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.ebean.Database;
import io.ebean.DuplicateKeyException;
import io.ebean.ExpressionList;
import io.ebean.Junction;
import io.ebean.PagedList;
import io.ebean.Query;
import io.ebean.RawSql;
import io.ebean.RawSqlBuilder;
import io.ebean.SqlQuery;
import io.ebean.SqlRow;
import io.ebean.SqlUpdate;
import io.ebean.Transaction;
import io.ebean.TxScope;
import io.ebean.annotation.Platform;
import io.ebean.annotation.TxIsolation;
import jakarta.persistence.PersistenceException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EbeanAspectDao implements AspectDao, AspectMigrationsDao {

  /** Primary pool for writes, transactions, and {@code FOR UPDATE} reads. */
  @Getter private final Database server;

  private final PrimaryStorageResolver primaryStorageResolver;

  /**
   * Resolves the aspect table name spliced into the hand-built raw SQL below. Ebean ORM queries are
   * already table-agnostic and do not use this.
   */
  @Nonnull private final AspectTableResolver tableResolver;

  /**
   * Seam for beginning transactions and scoping non-transactional work against an {@link
   * OperationContext}, so an extension module can route each operation to the correct underlying
   * database without this class knowing how.
   */
  @Nonnull private final ScopedTransactionFactory txnFactory;

  @Setter private boolean connectionValidated = false;

  // Flag used to make sure the dao isn't writing aspects
  // while its storage is being migrated
  private boolean canWrite = true;

  // Why 375? From tuning, this seems to be about the largest size we can get without having ebean
  // batch issues.
  // This may be able to be moved up, 375 is a bit conservative. However, we should be careful to
  // tweak this without
  // more testing.
  // Can be configured via ebean.queryKeysCountForBatch in application.yaml
  private int queryKeysCount = EbeanConfiguration.DEFAULT_QUERY_KEYS_COUNT;

  private final String batchGetMethod;
  @Nullable private final MetricUtils metricUtils;
  @Getter @Nonnull private final List<SystemAspectValidator> systemAspectValidators;
  @Getter @Nullable private final AspectSizeValidationConfiguration validationConfig;
  @Nonnull private final TransactionRetryPolicy transactionRetryPolicy;
  private final boolean optimisticLocking;
  // Opt-in scoped-retry: derived from config (not a constructor arg) so existing call sites are
  // unchanged; only takes effect when optimistic locking is on.
  private final boolean scopedRetryEnabled;
  // Opt-in CAS batching: derived from config; only takes effect when optimistic locking is on.
  private final boolean optimisticWriteBatchEnabled;
  private final int optimisticWriteBatchMinSize;
  // Latched true the first time a batch returns SUCCESS_NO_INFO (the rewriteBatchedStatements
  // signature): per-row counts are then unavailable for the life of the connection pool, so
  // batching
  // is disabled process-wide and writes fall back to the sequential path. Never reset — a restart
  // is
  // required to re-enable after fixing the datasource URL.
  private volatile boolean casBatchRuntimeDisabled = false;

  public enum Dialect {
    MYSQL,
    POSTGRES,
    H2_OR_OTHER
  }

  @Getter @Nonnull private final Dialect dialect;

  // Whether the primary store is PostgreSQL. The deterministic-lock-order work below (ORDER BY on
  // FOR UPDATE reads, the up-front lock in deleteUrn) targets PostgreSQL, whose default plan can
  // acquire row locks in physical scan order; and pg_advisory_xact_lock is Postgres-only SQL. On
  // MySQL/InnoDB the FOR UPDATE reads and bulk deletes already lock the primary-key IN-list in
  // index
  // order, so none of this is needed and nothing changes there.
  private final boolean isPostgres;
  // Opt-in per-entity write serialization via the Postgres advisory lock (pg_advisory_xact_lock).
  private final boolean entityWriteAdvisoryLockEnabled;
  // Arbitrary fixed namespace for entity-write advisory locks, so they can't collide with other
  // pg_advisory lock users in the same database; hashtext(urn) supplies the per-entity key.
  private static final int ADVISORY_LOCK_NAMESPACE = 0x44480001;
  // Composite-key separator for the per-(urn, aspect) advisory lock. The separator must not
  // appear in a urn or an aspect name so "<urn><sep><aspect>" is unambiguous, AND it must be
  // valid UTF-8 (NUL cannot be used: the Postgres JDBC driver rejects 0x00 in bound string
  // parameters). URNs are "urn:..." (no '|') and aspect names are alphanumeric PDL identifiers
  // (no '|'), so '|' is unambiguous and JDBC-safe.
  private static final String ADVISORY_LOCK_KEY_SEP = "|";

  public EbeanAspectDao(
      @Nonnull final PrimaryStorageResolver primaryStorageResolver,
      EbeanConfiguration ebeanConfiguration,
      MetricUtils metricUtils,
      @Nonnull List<SystemAspectValidator> systemAspectValidators,
      @Nullable AspectSizeValidationConfiguration validationConfig,
      boolean optimisticLocking) {
    this(
        primaryStorageResolver,
        ebeanConfiguration,
        metricUtils,
        systemAspectValidators,
        validationConfig,
        new PlainAspectTableResolver(),
        new PassThroughScopedTransactionFactory(primaryStorageResolver.resolveEbeanPrimary()),
        optimisticLocking);
  }

  public EbeanAspectDao(
      @Nonnull final PrimaryStorageResolver primaryStorageResolver,
      EbeanConfiguration ebeanConfiguration,
      MetricUtils metricUtils,
      @Nonnull List<SystemAspectValidator> systemAspectValidators,
      @Nullable AspectSizeValidationConfiguration validationConfig,
      @Nonnull AspectTableResolver tableResolver,
      @Nonnull ScopedTransactionFactory txnFactory) {
    this(
        primaryStorageResolver,
        ebeanConfiguration,
        metricUtils,
        systemAspectValidators,
        validationConfig,
        tableResolver,
        txnFactory,
        false);
  }

  public EbeanAspectDao(
      @Nonnull final PrimaryStorageResolver primaryStorageResolver,
      EbeanConfiguration ebeanConfiguration,
      MetricUtils metricUtils,
      @Nonnull List<SystemAspectValidator> systemAspectValidators,
      @Nullable AspectSizeValidationConfiguration validationConfig,
      @Nonnull AspectTableResolver tableResolver,
      @Nonnull ScopedTransactionFactory txnFactory,
      boolean optimisticLocking) {
    this.primaryStorageResolver = primaryStorageResolver;
    this.tableResolver = tableResolver;
    this.txnFactory = txnFactory;
    this.server = primaryStorageResolver.resolveEbeanPrimary();
    // Resolve the engine from Ebean's own detection (live connection metadata), not the JDBC
    // url/driver string — correct even for Aurora Postgres or the AWS JDBC wrapper. Advisory locks
    // and the ORDER BY lock-ordering below are Postgres-specific.
    Platform platform = resolvePlatform(server);
    this.isPostgres = platform == Platform.POSTGRES;
    this.optimisticLocking = optimisticLocking;
    this.dialect = resolveDialect(platform, optimisticLocking);
    String resolvedBatchGetMethod =
        ebeanConfiguration.getBatchGetMethod() != null
            ? ebeanConfiguration.getBatchGetMethod()
            : "IN";
    if (isPostgres && "UNION".equalsIgnoreCase(resolvedBatchGetMethod)) {
      // PostgreSQL rejects "... UNION ALL ... FOR UPDATE" ("FOR UPDATE is not allowed with
      // UNION/INTERSECT/EXCEPT"), so the UNION batch-get cannot take row locks there. Fall back to
      // the IN form, which supports FOR UPDATE (and ORDER BY) on PostgreSQL.
      log.warn(
          "EBEAN_BATCH_GET_METHOD=UNION is not supported with FOR UPDATE on PostgreSQL; using IN.");
      resolvedBatchGetMethod = "IN";
    }
    this.batchGetMethod = resolvedBatchGetMethod;
    Integer configuredKeysCount = ebeanConfiguration.getQueryKeysCountForBatch();
    if (configuredKeysCount != null) {
      this.queryKeysCount = configuredKeysCount;
    }
    this.metricUtils = metricUtils;
    this.systemAspectValidators = systemAspectValidators;
    this.validationConfig = validationConfig;
    TransactionRetryConfiguration retryConfig = ebeanConfiguration.getTransactionRetry();
    this.transactionRetryPolicy =
        new TransactionRetryPolicy(
            retryConfig != null ? retryConfig : new TransactionRetryConfiguration());

    // Postgres deadlock-ordering advisory lock — independent of the Hazelcast write-gate backend.
    this.entityWriteAdvisoryLockEnabled = ebeanConfiguration.isEntityWriteAdvisoryLockEnabled();
    // Scoped retry is an OL-only flow; enforce the prerequisite at the source so it can never read
    // "on" while optimistic locking is off.
    this.scopedRetryEnabled = optimisticLocking && ebeanConfiguration.isScopedRetryEnabled();
    // CAS batching requires optimistic locking AND scoped retry: the batched flush only runs on the
    // scoped-retry compute path (computeAndPersistWithinTransaction). Gate at the source so
    // isOptimisticWriteBatchEnabled() can never read "on" without its prerequisites — even for a
    // future direct caller. (scopedRetryEnabled already implies optimisticLocking.)
    this.optimisticWriteBatchEnabled =
        scopedRetryEnabled && ebeanConfiguration.isOptimisticWriteBatchEnabled();
    this.optimisticWriteBatchMinSize = ebeanConfiguration.getOptimisticWriteBatchMinSize();
    if (optimisticLocking) {
      log.info(
          "EbeanAspectDao optimistic locking enabled (dialect={}, scopedRetry={}, casBatch={})",
          dialect,
          scopedRetryEnabled,
          optimisticWriteBatchEnabled);
    }
  }

  @Override
  public boolean isOptimisticLockingEnabled() {
    return optimisticLocking;
  }

  @Override
  public boolean isOptimisticWriteBatchEnabled() {
    return optimisticWriteBatchEnabled && !casBatchRuntimeDisabled;
  }

  @Override
  public int getOptimisticWriteBatchMinSize() {
    return optimisticWriteBatchMinSize;
  }

  @Override
  public boolean isScopedRetryEnabled() {
    return scopedRetryEnabled;
  }

  @Nonnull
  private static Dialect resolveDialect(
      @Nonnull Platform platform, boolean optimisticLockingEnabled) {
    return switch (platform) {
      case POSTGRES, POSTGRES9, COCKROACH -> Dialect.POSTGRES;
      case MYSQL, MYSQL55, MARIADB -> Dialect.MYSQL;
      case H2, HSQLDB -> Dialect.H2_OR_OTHER;
      default -> {
        if (optimisticLockingEnabled) {
          throw new IllegalStateException(
              "Optimistic locking requires MySQL or PostgreSQL; unsupported platform=" + platform);
        }
        yield Dialect.H2_OR_OTHER;
      }
    };
  }

  /**
   * Resolve the primary store's database platform via Ebean's own detection (from live connection
   * metadata, so it is correct even for Aurora Postgres or the AWS JDBC wrapper, where the JDBC
   * url/driver string may not literally contain "postgres"). {@link Platform#base()} normalizes
   * variants (e.g. {@code POSTGRES9}) to their base. On any failure this logs and defaults to
   * {@link Platform#MYSQL}, which safely disables the Postgres-only paths below.
   */
  private static Platform resolvePlatform(@Nonnull final Database server) {
    try {
      final Platform platform = server.platform();
      if (platform != null) {
        return platform.base();
      }
      log.error("Ebean returned no database platform; defaulting to MYSQL for advisory locking");
    } catch (RuntimeException e) {
      log.error(
          "Could not determine DB platform for entity-write advisory locking; defaulting to MYSQL",
          e);
    }
    return Platform.MYSQL;
  }

  /**
   * Postgres-only, opt-in per-{@code (urn, aspect)} write serialization. When enabled, both the
   * ingest write path and {@link #deleteUrn} take a transaction-scoped advisory lock per {@code
   * (urn, aspect)} <em>before</em> acquiring any row locks, so a multi-row {@code FOR UPDATE}
   * writer (e.g. logical-model linking) and a concurrent hard-delete cannot interleave their
   * row-lock acquisition into a cycle. The advisory lock ({@code pg_advisory_xact_lock}) is
   * released automatically on commit/rollback, so no explicit release is needed. No-op unless the
   * store is Postgres and the feature is enabled.
   *
   * <p>Keyed by {@code pg_advisory_xact_lock(<namespace>, hashtext(urn || '|' || aspect))}. The '|'
   * separator cannot appear in a urn or an aspect name, so the composite is unambiguous, and it is
   * valid UTF-8 (a NUL separator would be rejected by the Postgres JDBC driver as a bound string
   * parameter). {@code hashtext} is a 32-bit hash, so distinct {@code (urn, aspect)} pairs can
   * collide on the same lock key and serialize against each other. That is a false serialization:
   * it costs throughput but never affects correctness (CAS remains the guard). It is acceptable
   * here because the feature is opt-in and collisions are rare relative to the set of pairs being
   * written concurrently.
   *
   * <p>Collision bound for operators: {@code hashtext} yields {@code int4} (2^32 ≈ 4.29e9 keys). By
   * the birthday paradox the 50% collision point is at √2^32 ≈ 65,536 distinct pairs. A deployment
   * with 10M entities × ~10 aspects = 100M pairs will almost certainly have hash collisions, but a
   * collision only matters when two writers on colliding pairs run concurrently — the cost is that
   * they serialize unnecessarily for the lock hold time, not a lost or corrupted write. For
   * workloads where that false serialization is unacceptable, keep the feature off (the default) or
   * use the Hazelcast write gate ({@code ENTITY_WRITE_LOCK_BACKEND=hazelcast}), which keys on the
   * full composite string with no hash collision.
   *
   * <p>Keying on the {@code (urn, aspect)} conflict unit (not the whole entity) matches what CAS
   * and {@code FOR UPDATE} actually contend on: two writers on the same URN but different aspects
   * share no row and must not share a mutex, so cross-aspect writers on the same URN do not
   * serialize. Whole-entity ops (e.g. {@link #deleteUrn}) pass the entity's full aspect key-set so
   * delete↔upsert safety is key-set overlap, not a permanent URN-wide lock on every ingest.
   */
  @Override
  public void lockAspectsForWrite(
      @Nonnull OperationContext opContext, @Nonnull Map<String, Set<String>> urnAspects) {
    if (!canWrite || !entityWriteAdvisoryLockEnabled || !isPostgres || urnAspects.isEmpty()) {
      return;
    }
    // Flatten to composite keys urn|aspect, de-duplicated and sorted. Sorting the composite
    // strings is equivalent to sorting by (urn, aspect) because '|' is the separator and
    // cannot appear in either field, so every transaction presents the keys in the same order
    // (advisory locks can self-deadlock across transactions otherwise). Note: the SQL below
    // re-sorts by hashtext(key) (the actual lock id) under an OFFSET 0 fence, which is what
    // guarantees acquisition order; this Java sort is a best-effort pre-sort.
    final List<String> sortedKeys =
        urnAspects.entrySet().stream()
            .flatMap(e -> e.getValue().stream().map(a -> e.getKey() + ADVISORY_LOCK_KEY_SEP + a))
            .distinct()
            .sorted()
            .collect(Collectors.toList());
    if (sortedKeys.isEmpty()) {
      return;
    }
    txnFactory.runInScope(
        opContext,
        () -> {
          // Transaction-scoped: without an active transaction the advisory lock would auto-commit
          // and release immediately. All real callsites run inside runInTransactionWithRetry; if
          // that ever isn't the case, skip the lock with a warning rather than abort the caller's
          // write.
          if (!hasActiveTransaction("lockAspectsForWrite")) {
            return null;
          }
          // Acquire all advisory locks in ONE round trip with guaranteed acquisition
          // order. The inner subquery computes hashtext(key) (the actual int4 lock id)
          // and sorts by THAT, and OFFSET 0 is an optimizer fence preventing the planner
          // from flattening the subquery, so the Sort materializes BEFORE the outer SELECT
          // evaluates pg_advisory_xact_lock. Ordering by the hash -- not the composite
          // string -- is what makes hash collisions throughput-only: two batches whose
          // distinct composites collide on the same int4 still acquire those ids in the
          // same ascending order, so no ABBA deadlock. Ordering by the composite string
          // would leave colliding hashes free to be acquired in opposite orders across
          // batches. The outer SELECT locks on the pre-computed int4 (hashtext is not
          // re-evaluated). pg_advisory_xact_lock is transaction-scoped and reentrant per
          // session, so re-acquiring the same key within this transaction is a no-op.
          final StringBuilder inner =
              new StringBuilder("select hashtext(v.key) as h from (values ");
          for (int i = 0; i < sortedKeys.size(); i++) {
            if (i > 0) {
              inner.append(", ");
            }
            inner.append("(:k").append(i).append(")");
          }
          inner.append(") as v(key) order by h offset 0");
          final String sql =
              "select pg_advisory_xact_lock(:ns, ordered.h) from (" + inner + ") as ordered(h)";
          final SqlQuery lockQuery =
              server.sqlQuery(sql.toString()).setParameter("ns", ADVISORY_LOCK_NAMESPACE);
          for (int i = 0; i < sortedKeys.size(); i++) {
            lockQuery.setParameter("k" + i, sortedKeys.get(i));
          }
          lockQuery.findList();
          return null;
        });
  }

  /**
   * Transaction-scoped locks (advisory locks and {@code FOR UPDATE} reads) only hold if the thread
   * has an active Ebean transaction; without one they would run under auto-commit and release
   * immediately, so they must be skipped rather than issued. All real callsites run inside {@code
   * runInTransactionWithRetry}, so this normally returns true. If it is ever false we log and skip
   * the lock — the write still succeeds, only the (opt-in) serialization / lock ordering is
   * forgone. Never aborts the caller.
   */
  private boolean hasActiveTransaction(String operation) {
    if (server.currentTransaction() == null) {
      log.warn(
          "{} invoked without an active transaction; skipping the transaction-scoped lock. The "
              + "write proceeds, but lock ordering / serialization is not applied.",
          operation);
      return false;
    }
    return true;
  }

  @Override
  public void setWritable(boolean canWrite) {
    this.canWrite = canWrite;
  }

  private void ensureWritableForOptimisticWrite() {
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      throw new AspectWriteDisabledException(READ_ONLY_LOG);
    }
  }

  private boolean validateConnection() {
    if (connectionValidated) {
      return true;
    }
    if (!AspectStorageValidationUtil.checkV2TableExists(server)) {
      log.error("Table metadata_aspect_v2 does not exist.");
      canWrite = false;
      return false;
    } else {
      connectionValidated = true;
      return true;
    }
  }

  @Nonnull
  @Override
  public Optional<EntityAspect> updateAspect(
      @Nonnull OperationContext opContext,
      @Nullable TransactionContext txContext,
      @Nonnull SystemAspect aspect) {
    validateConnection();
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return Optional.empty();
    }

    return txnFactory.runInScope(
        opContext,
        () -> {
          EbeanAspectV2 ebeanAspectV2 = EbeanAspectV2.fromEntityAspect(aspect.asLatest());

          saveEbeanAspect(txContext, ebeanAspectV2, false);
          return Optional.of(ebeanAspectV2.toEntityAspect());
        });
  }

  @Override
  @Nonnull
  public Optional<EntityAspect> updateAspectConditional(
      @Nonnull OperationContext opContext,
      @Nullable TransactionContext txContext,
      @Nonnull SystemAspect newAspect,
      @Nullable String expectedSystemMetadataVersion) {
    validateConnection();
    ensureWritableForOptimisticWrite();

    return txnFactory.runInScope(
        opContext,
        () -> {
          EntityAspect entityAspect = newAspect.asLatest();
          SqlUpdate update =
              server
                  .sqlUpdate(
                      buildConditionalUpdateSql(
                          dialect,
                          tableResolver.aspectTable(opContext, EbeanAspectV2.TABLE_NAME),
                          false))
                  .setParameter("metadata", entityAspect.getMetadata())
                  .setParameter("systemMetadata", entityAspect.getSystemMetadata())
                  .setParameter("createdOn", entityAspect.getCreatedOn())
                  .setParameter("createdBy", entityAspect.getCreatedBy())
                  .setParameter("createdFor", entityAspect.getCreatedFor())
                  .setParameter("urn", entityAspect.getUrn())
                  .setParameter("aspect", entityAspect.getAspect())
                  .setParameter("expectedVersion", expectedSystemMetadataVersion);

          Transaction tx = txContext != null ? txContext.tx() : null;
          boolean restoreBatchMode = false;
          boolean priorBatchMode = false;
          if (tx != null) {
            priorBatchMode = tx.isBatchMode();
            if (priorBatchMode) {
              tx.flush();
              tx.setBatchMode(false);
              restoreBatchMode = true;
            }
          }

          int modified;
          try {
            modified = tx != null ? server.execute(update, tx) : update.execute();
          } finally {
            if (restoreBatchMode) {
              tx.setBatchMode(priorBatchMode);
            }
          }

          incrementOptimisticMetric("optimistic_lock_update_attempt");
          if (modified == 0) {
            incrementOptimisticMetric("optimistic_lock_update_conflict");
            incrementConflictByEntityType(entityAspect.getUrn());
            return Optional.empty();
          }
          return Optional.of(entityAspect);
        });
  }

  @Override
  @Nonnull
  public List<ConditionalUpdateResult> updateAspectsConditionalBatch(
      @Nonnull OperationContext opContext,
      @Nullable TransactionContext txContext,
      @Nonnull List<ConditionalAspectUpdate> updates) {
    validateConnection();
    ensureWritableForOptimisticWrite();

    return txnFactory.runInScope(
        opContext,
        () -> {
          if (updates.isEmpty()) {
            return List.of();
          }

          Transaction tx = txContext != null ? txContext.tx() : null;
          if (tx == null) {
            throw new IllegalStateException(
                "updateAspectsConditionalBatch requires an active transaction");
          }

          String sql =
              buildConditionalUpdateSql(
                  dialect, tableResolver.aspectTable(opContext, EbeanAspectV2.TABLE_NAME), true);

          // Ebean has no per-row batch API, so drop to raw JDBC on the transaction's own
          // connection. Flush any Ebean-buffered writes first so our statements run against a
          // consistent connection state and Ebean does not reorder its buffer around ours.
          boolean priorBatchMode = tx.isBatchMode();
          if (priorBatchMode) {
            tx.flush();
            tx.setBatchMode(false);
          }

          int[] results;
          try {
            java.sql.Connection conn = tx.connection();
            try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
              for (ConditionalAspectUpdate update : updates) {
                EntityAspect entityAspect = update.getNewAspect().asLatest();
                ps.setString(1, entityAspect.getMetadata());
                ps.setString(2, entityAspect.getSystemMetadata());
                ps.setObject(3, entityAspect.getCreatedOn());
                ps.setString(4, entityAspect.getCreatedBy());
                ps.setString(5, entityAspect.getCreatedFor());
                ps.setString(6, entityAspect.getUrn());
                ps.setString(7, entityAspect.getAspect());
                ps.setString(8, update.getExpectedSystemMetadataVersion());
                ps.addBatch();
              }
              results = ps.executeBatch();
            }
          } catch (java.sql.SQLException e) {
            // A thrown batch error (BatchUpdateException is a SQLException) means the outcome is
            // unknown and rows may have partially applied. Never continue in this transaction —
            // wrap so the outer runInTransactionWithRetry rolls back and retries on the sequential
            // path. (On Postgres the txn is already aborted; on MySQL earlier rows applied.)
            incrementOptimisticMetric("optimistic_lock_batch_ambiguous_result");
            throw new jakarta.persistence.PersistenceException(
                "Conditional CAS batch failed; rolling back for sequential retry", e);
          } finally {
            if (priorBatchMode) {
              tx.setBatchMode(true);
            }
          }

          // Map per-item results to outcomes. Reuse helper to emit per-item metrics.
          List<ConditionalUpdateResult> outcomes = new ArrayList<>();

          for (int i = 0; i < results.length; i++) {
            int count = results[i];
            ConditionalAspectUpdate update = updates.get(i);
            EntityAspect entityAspect = update.getNewAspect().asLatest();

            if (count == 1) {
              // CAS match: row updated
              incrementOptimisticMetric("optimistic_lock_update_attempt");
              outcomes.add(ConditionalUpdateResult.UPDATED);
            } else if (count == 0) {
              // CAS miss: legitimate conflict, transaction still healthy
              incrementOptimisticMetric("optimistic_lock_update_attempt");
              incrementOptimisticMetric("optimistic_lock_update_conflict");
              incrementConflictByEntityType(entityAspect.getUrn());
              outcomes.add(ConditionalUpdateResult.CONFLICT);
            } else {
              // A non-throwing executeBatch returns only 1, 0, or -2 (SUCCESS_NO_INFO) here. -3
              // (EXECUTE_FAILED) does NOT reach this loop — it appears only in a thrown
              // BatchUpdateException.getUpdateCounts(), which the catch above already handles. So
              // in
              // practice this is the SUCCESS_NO_INFO case (MySQL rewriteBatchedStatements): the
              // driver executed the statements but cannot report per-row counts, so the per-row
              // outcome is UNKNOWN and rows may already have applied. An in-txn sequential re-CAS
              // is
              // UNSAFE — an already-applied row now holds the NEW version, so a re-CAS on the old
              // expectedVersion matches 0 rows and reports a FALSE CONFLICT for a write that
              // actually
              // succeeded. The only safe recovery is to abandon the transaction: throw so the outer
              // runInTransactionWithRetry rolls back and re-runs.
              //
              // rewriteBatchedStatements is connection-level, so every subsequent batch hits the
              // same
              // -2. Latch batching OFF process-wide on the FIRST -2 so the retry (and all later
              // writes) take the sequential path and make progress. ONLY -2 latches: a thrown
              // BatchUpdateException (handled by the catch above) may be transient (deadlock,
              // serialization) and must NOT permanently disable batching.
              if (count == java.sql.Statement.SUCCESS_NO_INFO && !casBatchRuntimeDisabled) {
                casBatchRuntimeDisabled = true;
                log.warn(
                    "JDBC batch returned SUCCESS_NO_INFO — rewriteBatchedStatements is likely enabled "
                        + "on the datasource. CAS batching cannot report per-row counts and is now "
                        + "disabled for this process; writes fall back to the sequential path.");
              }
              incrementOptimisticMetric("optimistic_lock_batch_ambiguous_result");
              throw new jakarta.persistence.PersistenceException(
                  String.format(
                      "Ambiguous JDBC batch result code %d for item %d (urn=%s aspect=%s); rolling "
                          + "back for sequential retry",
                      count, i, entityAspect.getUrn(), entityAspect.getAspect()));
            }
          }

          // Emit batch-level metrics: batch_size is the ROW COUNT of this executeBatch (so
          // batch_size/executions = avg rows per batch); executions counts the calls.
          incrementOptimisticMetric("optimistic_lock_batch_size", results.length);
          incrementOptimisticMetric("optimistic_lock_batch_executions");

          return outcomes;
        });
  }

  @VisibleForTesting
  @Nonnull
  public String buildConditionalUpdateSql(@Nonnull Dialect sqlDialect) {
    return buildConditionalUpdateSql(sqlDialect, " metadata_aspect_v2 ", false);
  }

  /**
   * The version-0 CAS UPDATE. {@code positional=false} emits Ebean named params ({@code :metadata}
   * …) for the single-row {@link #updateAspectConditional}; {@code positional=true} emits JDBC
   * {@code ?} for the batched raw {@code PreparedStatement} in {@link
   * #updateAspectsConditionalBatch}. Single source of the dialect version-predicate and column
   * list, so the sequential and batch SQL cannot diverge.
   */
  @Nonnull
  private static String buildConditionalUpdateSql(
      @Nonnull Dialect sqlDialect, @Nonnull String aspectTable, boolean positional) {
    String v = positional ? "?" : ":expectedVersion";
    String versionPredicate =
        switch (sqlDialect) {
          case POSTGRES -> "(systemmetadata::jsonb ->> 'version') = " + v;
          case MYSQL -> "systemmetadata->>'$.version' = " + v;
          // H2 has no JSON path operator comparable to MySQL/Postgres. This INSTR substring match
          // is
          // a TEST-ONLY approximation and can false-positive/negative vs real JSON path equality —
          // do
          // not treat H2 CAS results as production dialect coverage.
          case H2_OR_OTHER ->
              "INSTR(CAST(systemmetadata AS VARCHAR), CONCAT('\"version\":\"', "
                  + v
                  + ", '\"')) > 0";
        };
    String columns =
        positional
            ? "SET metadata = ?, systemmetadata = ?, createdon = ?, createdby = ?, createdfor = ? "
                + "WHERE urn = ? AND aspect = ? AND version = 0 AND "
            : "SET metadata = :metadata, systemmetadata = :systemMetadata, "
                + "createdon = :createdOn, createdby = :createdBy, createdfor = :createdFor "
                + "WHERE urn = :urn AND aspect = :aspect AND version = 0 AND ";
    return "UPDATE" + aspectTable + columns + versionPredicate;
  }

  @Override
  @Nonnull
  public Optional<EntityAspect> insertAspect(
      @Nonnull OperationContext opContext,
      @Nullable TransactionContext txContext,
      @Nonnull SystemAspect aspect,
      final long version) {
    validateConnection();
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      if (optimisticLocking) {
        throw new AspectWriteDisabledException(READ_ONLY_LOG);
      }
      return Optional.empty();
    }

    return txnFactory.runInScope(
        opContext,
        () -> {
          EbeanAspectV2 ebeanAspectV2 = EbeanAspectV2.fromEntityAspect(aspect.withVersion(version));

          try {
            saveEbeanAspect(txContext, ebeanAspectV2, true);
            return Optional.of(ebeanAspectV2.toEntityAspect());
          } catch (DuplicateKeyException e) {
            if (optimisticLocking && version == ASPECT_LATEST_VERSION) {
              throwOnDuplicateKeyInsertConflict(aspect, e);
            }
            throw e;
          } catch (PersistenceException e) {
            if (optimisticLocking && version == ASPECT_LATEST_VERSION && isDuplicateKeyCause(e)) {
              throwOnDuplicateKeyInsertConflict(aspect, e);
            }
            throw e;
          }
        });
  }

  /**
   * Concurrent version-0 inserts race on the unique key. PostgreSQL aborts the open transaction on
   * DuplicateKey (SQLState {@code 25P02} in-failed-sql-transaction), so in-transaction CAS recovery
   * is not viable — convert to {@link OptimisticLockConflictException} and let the outer retry loop
   * re-read and re-apply in a fresh transaction.
   */
  private void throwOnDuplicateKeyInsertConflict(
      @Nonnull SystemAspect aspect, @Nonnull PersistenceException original) {
    incrementOptimisticMetric("optimistic_lock_insert_fallback");
    // Also tag by entity type so creation-race conflicts show up on the same per-entity dashboard
    // as CAS-update conflicts (updateAspectConditional), not just in the aggregate counter.
    incrementConflictByEntityType(aspect.getUrn().toString());
    throw new OptimisticLockConflictException(
        String.format(
            "Optimistic lock conflict on concurrent v0 insert urn=%s aspect=%s",
            aspect.getUrn(), aspect.getAspectName()),
        original);
  }

  private static final int MAX_DUPLICATE_KEY_CAUSE_DEPTH = 10;
  private static final String SQLSTATE_UNIQUE_VIOLATION = "23505";
  private static final int MYSQL_ER_DUP_ENTRY = 1062;

  @VisibleForTesting
  static boolean isDuplicateKeyCause(@Nonnull PersistenceException exception) {
    Throwable cause = exception;
    int depth = 0;
    while (cause != null && depth < MAX_DUPLICATE_KEY_CAUSE_DEPTH) {
      if (cause instanceof DuplicateKeyException) {
        return true;
      }
      if (cause instanceof SQLException sqlException
          && isUniqueConstraintSqlException(sqlException)) {
        return true;
      }
      String name = cause.getClass().getName();
      if (name.contains("DuplicateKey") || name.contains("UniqueConstraint")) {
        return true;
      }
      cause = cause.getCause();
      depth++;
    }
    return false;
  }

  private static boolean isUniqueConstraintSqlException(@Nonnull SQLException sqlException) {
    for (SQLException current = sqlException;
        current != null;
        current = current.getNextException()) {
      if (SQLSTATE_UNIQUE_VIOLATION.equals(current.getSQLState())
          || current.getErrorCode() == MYSQL_ER_DUP_ENTRY) {
        return true;
      }
    }
    return false;
  }

  private void incrementOptimisticMetric(@Nonnull String name) {
    incrementOptimisticMetric(name, 1);
  }

  private void incrementOptimisticMetric(@Nonnull String name, long count) {
    if (metricUtils != null) {
      metricUtils.increment(MetricRegistry.name(this.getClass(), name), count);
    }
  }

  /**
   * Attributes optimistic-lock conflicts to the entity type so operators can see WHICH entity is
   * contended (a specific consumer's hot key), not just an aggregate rate. Entity type is
   * low-cardinality; the raw URN is deliberately NOT tagged (unbounded → metric explosion).
   */
  private void incrementConflictByEntityType(@Nullable String urn) {
    if (metricUtils == null || urn == null) {
      return;
    }
    String entityType;
    try {
      entityType = UrnUtils.getUrn(urn).getEntityType();
    } catch (RuntimeException e) {
      entityType = "unknown";
    }
    metricUtils.incrementMicrometer(
        MetricRegistry.name(this.getClass(), "optimistic_lock_conflict"),
        1.0,
        "entityType",
        entityType);
  }

  @Override
  public void incrementOptimisticLockMetric(@Nonnull String name) {
    incrementOptimisticMetric(name);
  }

  private void saveEbeanAspect(
      @Nullable TransactionContext txContext,
      @Nonnull final EbeanAspectV2 ebeanAspect,
      final boolean insert) {
    validateConnection();
    if (txContext != null && txContext.tx() != null) {
      if (insert) {
        server.insert(ebeanAspect, txContext.tx());
      } else {
        server.update(ebeanAspect, txContext.tx());
      }
    } else {
      if (insert) {
        server.insert(ebeanAspect);
      } else {
        server.update(ebeanAspect);
      }
    }
  }

  @Nonnull
  @Override
  public Map<String, Map<String, SystemAspect>> getLatestAspects(
      @Nonnull OperationContext opContext,
      @Nonnull Map<String, Set<String>> urnAspects,
      boolean forUpdate) {
    validateConnection();

    return txnFactory.runInScope(
        opContext,
        () -> {
          Set<EbeanAspectV2.PrimaryKey> keys =
              urnAspects.entrySet().stream()
                  .flatMap(
                      entry ->
                          entry.getValue().stream()
                              .map(
                                  aspect ->
                                      new EbeanAspectV2.PrimaryKey(
                                          entry.getKey(), aspect, ASPECT_LATEST_VERSION)))
                  .collect(Collectors.toSet());

          // Use batchGet to chunk large IN clauses and avoid optimizer memory exhaustion
          // (range_optimizer_max_mem_size)
          final List<EbeanAspectV2> results =
              batchGet(opContext, keys, queryKeysCount, forUpdate && canWrite);
          return toUrnAspectMap(opContext.getEntityRegistry(), results, opContext);
        });
  }

  @Override
  public long countEntities(@Nonnull OperationContext opContext) {
    validateConnection();
    return txnFactory.runInScope(
        opContext,
        () ->
            server
                .find(EbeanAspectV2.class)
                .setDistinct(true)
                .select(EbeanAspectV2.URN_COLUMN)
                .findCount());
  }

  @Override
  public boolean checkIfAspectExists(
      @Nonnull OperationContext opContext, @Nonnull String aspectName) {
    validateConnection();
    return txnFactory.runInScope(
        opContext,
        () ->
            server
                .find(EbeanAspectV2.class)
                .where()
                .eq(EbeanAspectV2.ASPECT_COLUMN, aspectName)
                .exists());
  }

  @Override
  @Nullable
  public EntityAspect getAspect(
      @Nonnull OperationContext opContext,
      @Nonnull final String urn,
      @Nonnull final String aspectName,
      final long version) {
    return getAspect(opContext, new EntityAspectIdentifier(urn, aspectName, version));
  }

  @Override
  @Nullable
  public EntityAspect getAspect(
      @Nonnull OperationContext opContext, @Nonnull final EntityAspectIdentifier key) {
    validateConnection();
    return txnFactory.runInScope(
        opContext,
        () -> {
          EbeanAspectV2.PrimaryKey primaryKey =
              new EbeanAspectV2.PrimaryKey(key.getUrn(), key.getAspect(), key.getVersion());
          EbeanAspectV2 ebeanAspect =
              primaryStorageResolver
                  .resolveEbean(opContext, false)
                  .find(EbeanAspectV2.class, primaryKey);
          return ebeanAspect == null ? null : ebeanAspect.toEntityAspect();
        });
  }

  @Override
  public void deleteAspect(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final String aspect,
      @Nonnull final Long version) {
    validateConnection();
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return;
    }
    txnFactory.runInScope(
        opContext,
        () ->
            server
                .createQuery(EbeanAspectV2.class)
                .where()
                .eq(EbeanAspectV2.URN_COLUMN, urn.toString())
                .eq(EbeanAspectV2.ASPECT_COLUMN, aspect)
                .eq(EbeanAspectV2.VERSION_COLUMN, version)
                .delete());
  }

  @Override
  public int deleteUrn(
      @Nonnull OperationContext opContext,
      @Nullable TransactionContext txContext,
      @Nonnull final String urn) {
    validateConnection();
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return 0;
    }

    return txnFactory.runInScope(
        opContext,
        () -> {
          Urn urnObj = UrnUtils.getUrn(urn);
          String keyAspectName = opContext.getKeyAspectName(urnObj);

          // Opt-in Postgres per-(urn, aspect) write serialization (advisory lock), taken before
          // any row locks. No-op unless enabled on a Postgres store; when on, it serializes this
          // delete against a concurrent multi-row write (e.g. logical-model linking) on the same
          // entity. A hard-delete wipes the whole entity, so it locks the entity's full aspect
          // key-set (wide) — delete↔upsert safety is key-set overlap, not a permanent URN-wide
          // lock on every ingest. Postgres transaction-scoped pg_advisory_xact_lock, auto-released
          // on commit/rollback.
          //
          // Defensive: the registry lookup should return a non-null aspect set, but a misconfigured
          // entity registry (entity with no registered aspects) could return null and NPE inside
          // Map.of, which would abort the delete. Fall back to the single-URN composite key
          // (urn + keyAspect) so the delete still takes a lock on the key aspect and proceeds —
          // the FOR UPDATE ordering below still guards the deadlock; only the advisory
          // serialization is narrowed.
          final Set<String> entityAspects = opContext.getEntityAspectNames(urnObj);
          if (entityAspects == null || entityAspects.isEmpty()) {
            log.warn(
                "Entity registry returned no aspects for urn={}; falling back to key-aspect-only"
                    + " advisory lock for deleteUrn. Check the entity registry for this entity"
                    + " type.",
                urn);
            lockAspectsForWrite(opContext, Map.of(urn, Set.of(keyAspectName)));
          } else {
            lockAspectsForWrite(opContext, Map.of(urn, entityAspects));
          }

          // On PostgreSQL, acquire this urn's rows up front in canonical (urn, aspect, version)
          // order — the same order the upsert write path uses for its FOR UPDATE reads. The bulk
          // DELETEs below otherwise lock rows in the engine's scan order (physical/CTID order on
          // PostgreSQL), unrelated to key order, so a concurrent multi-row FOR UPDATE write and
          // this
          // hard-delete could acquire overlapping rows in opposite orders and deadlock. The
          // explicit
          // ORDER BY (not the query's natural order) is what makes PostgreSQL place a Sort below
          // its
          // LockRows node so locks are actually taken in key order. On MySQL/InnoDB the bulk
          // DELETEs
          // already lock the primary-key rows in index order, so no up-front lock is needed and
          // this
          // block is skipped. The lock query hydrates only this one urn's rows (bounded by its
          // aspect/version count) purely to take the locks. (canWrite is guaranteed true by the
          // early return above.)
          if (isPostgres && hasActiveTransaction("deleteUrn ordered lock")) {
            // Select only the key columns (urn, aspect, version): FOR UPDATE still locks the
            // matched
            // rows, but this avoids hydrating the metadata/systemMetadata LOBs purely to take the
            // locks.
            server
                .find(EbeanAspectV2.class)
                .select(EbeanAspectV2.KEY_ORDER_BY_SQL)
                .where()
                .eq(EbeanAspectV2.URN_COLUMN, urn)
                .orderBy(EbeanAspectV2.KEY_ORDER_BY_PROPERTY_PATH)
                .forUpdate()
                .findList();
          }

          // First, delete all non-key aspects
          int nonKeyCount =
              server
                  .createQuery(EbeanAspectV2.class)
                  .where()
                  .eq(EbeanAspectV2.URN_COLUMN, urn)
                  .ne(EbeanAspectV2.ASPECT_COLUMN, keyAspectName)
                  .delete();

          // Then, delete the key aspect
          int keyCount =
              server
                  .createQuery(EbeanAspectV2.class)
                  .where()
                  .eq(EbeanAspectV2.URN_COLUMN, urn)
                  .eq(EbeanAspectV2.ASPECT_COLUMN, keyAspectName)
                  .delete();

          return nonKeyCount + keyCount;
        });
  }

  @Override
  @Nonnull
  public Map<EntityAspectIdentifier, EntityAspect> batchGet(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<EntityAspectIdentifier> keys,
      boolean forUpdate) {
    validateConnection();
    if (keys.isEmpty()) {
      return Collections.emptyMap();
    }

    return txnFactory.runInScope(
        opContext,
        () -> {
          final Set<EbeanAspectV2.PrimaryKey> ebeanKeys =
              keys.stream()
                  .map(EbeanAspectV2.PrimaryKey::fromAspectIdentifier)
                  .collect(Collectors.toSet());
          final List<EbeanAspectV2> records;
          if (queryKeysCount == 0) {
            records = batchGet(opContext, ebeanKeys, ebeanKeys.size(), forUpdate);
          } else {
            records = batchGet(opContext, ebeanKeys, queryKeysCount, forUpdate);
          }
          return records.stream()
              .collect(
                  Collectors.toMap(
                      record -> record.getKey().toAspectIdentifier(),
                      EbeanAspectV2::toEntityAspect));
        });
  }

  /**
   * BatchGet that allows pagination on keys to avoid large queries. TODO: can further improve by
   * running the sub queries in parallel
   *
   * @param keys a set of keys with urn, aspect and version
   * @param keysCount the max number of keys for each sub query
   * @param forUpdate whether the operation is intending to write to this row in a tx
   */
  @Nonnull
  private List<EbeanAspectV2> batchGet(
      @Nonnull OperationContext opContext,
      @Nonnull final Set<EbeanAspectV2.PrimaryKey> keys,
      final int keysCount,
      boolean forUpdate) {
    if (keys.isEmpty()) {
      return Collections.emptyList();
    }
    validateConnection();

    int position = 0;

    List<EbeanAspectV2.PrimaryKey> keyList = new ArrayList<>(keys);
    boolean lockRows = forUpdate && canWrite && !optimisticLocking;
    // Only when we actually take row locks: sort by primary key so all transactions acquire locks
    // in the same (urn, aspect, version) order. Unordered keys under FOR UPDATE cause lock-order
    // deadlocks between concurrent writers ("Deadlock found when trying to get lock"). Non-locking
    // reads take no row locks, so sorting them would be wasted work.
    if (lockRows) {
      keyList.sort(
          Comparator.comparing(EbeanAspectV2.PrimaryKey::getUrn)
              .thenComparing(EbeanAspectV2.PrimaryKey::getAspect)
              .thenComparing(EbeanAspectV2.PrimaryKey::getVersion));
    }
    final int totalPageCount = QueryUtils.getTotalPageCount(keys.size(), keysCount);
    final List<EbeanAspectV2> finalResult =
        batchGetSelectString(opContext, keyList, keysCount, position, forUpdate);

    while (QueryUtils.hasMore(position, keysCount, totalPageCount)) {
      position += keysCount;
      final List<EbeanAspectV2> oneStatementResult =
          batchGetSelectString(opContext, keyList, keysCount, position, forUpdate);
      finalResult.addAll(oneStatementResult);
    }

    syncKeyScalarsFromEmbeddedId(finalResult);
    return finalResult;
  }

  /**
   * Parsed {@link RawSql} keeps one bean property per <em>result column name</em>: a second {@code
   * columnMapping} for the same column <strong>replaces</strong> the first, so we cannot map both
   * {@code key.urn} and top-level {@code urn} for {@code urn}. Map only the embedded id; {@link
   * #syncKeyScalarsFromEmbeddedId} fills the duplicate scalar fields.
   */
  @Nonnull
  private static RawSql parseBatchGetRawSql(String sql) {
    return RawSqlBuilder.parse(sql)
        .columnMapping(EbeanAspectV2.URN_COLUMN, "key.urn")
        .columnMapping(EbeanAspectV2.ASPECT_COLUMN, "key.aspect")
        .columnMapping(EbeanAspectV2.VERSION_COLUMN, "key.version")
        .create();
  }

  /**
   * RawSql maps result columns only to {@code key.*}; copy into the denormalized scalar fields so
   * {@link EbeanAspectV2#getUrn()} and related accessors match the embedded id.
   */
  private static void syncKeyScalarsFromEmbeddedId(Iterable<EbeanAspectV2> aspects) {
    for (EbeanAspectV2 aspect : aspects) {
      aspect.setUrn(aspect.getKey().getUrn());
      aspect.setAspect(aspect.getKey().getAspect());
      aspect.setVersion(aspect.getKey().getVersion());
    }
  }

  @VisibleForTesting
  @Nonnull
  protected List<EbeanAspectV2> batchGetSelectString(
      @Nonnull OperationContext opContext,
      @Nonnull final List<EbeanAspectV2.PrimaryKey> keys,
      final int keysCount,
      final int position,
      boolean forUpdate) {
    boolean writeIntent = forUpdate;
    boolean lockRows = writeIntent && canWrite && !optimisticLocking;
    return batchGetSelectString(opContext, keys, keysCount, position, lockRows, writeIntent);
  }

  @Nonnull
  protected List<EbeanAspectV2> batchGetSelectString(
      @Nonnull OperationContext opContext,
      @Nonnull final List<EbeanAspectV2.PrimaryKey> keys,
      final int keysCount,
      final int position,
      boolean lockRows,
      boolean writeIntent) {

    if (batchGetMethod.equals("IN")) {
      return batchGetIn(opContext, keys, keysCount, position, lockRows, writeIntent);
    }

    return batchGetUnion(opContext, keys, keysCount, position, lockRows, writeIntent);
  }

  /**
   * Builds a single SELECT statement for batch get, which selects one entity, and then can be
   * UNION'd with other SELECT statements.
   */
  private String batchGetSelectString(
      @Nonnull final OperationContext opContext,
      final int selectId,
      @Nonnull final String urn,
      @Nonnull final String aspect,
      final long version,
      @Nonnull final Map<String, Object> outputParamsToValues) {
    validateConnection();

    final String urnArg = "urn" + selectId;
    final String aspectArg = "aspect" + selectId;
    final String versionArg = "version" + selectId;

    outputParamsToValues.put(urnArg, urn);
    outputParamsToValues.put(aspectArg, aspect);
    outputParamsToValues.put(versionArg, version);

    return String.format(
        "SELECT urn, aspect, version, metadata, systemMetadata, createdOn, createdBy, createdFor "
            + "FROM%sWHERE urn = :%s AND aspect = :%s AND version = :%s",
        tableResolver.aspectTable(opContext, EbeanAspectV2.TABLE_NAME),
        urnArg,
        aspectArg,
        versionArg);
  }

  @Nonnull
  private List<EbeanAspectV2> batchGetUnion(
      @Nonnull OperationContext opContext,
      @Nonnull final List<EbeanAspectV2.PrimaryKey> keys,
      final int keysCount,
      final int position,
      boolean lockRows,
      boolean writeIntent) {
    validateConnection();

    // Build one SELECT per key and then UNION ALL the results. This can be much more performant
    // than OR'ing the
    // conditions together. Our query will look like:
    //   SELECT * FROM metadata_aspect WHERE urn = 'urn0' AND aspect = 'aspect0' AND version = 0
    //   UNION ALL
    //   SELECT * FROM metadata_aspect WHERE urn = 'urn0' AND aspect = 'aspect1' AND version = 0
    //   ...
    // Note: UNION ALL should be safe and more performant than UNION. We're selecting the entire
    // entity key (as well
    // as data), so each result should be unique. No need to deduplicate.
    // Another note: ebean doesn't support UNION ALL, so we need to manually build the SQL statement
    // ourselves.
    final StringBuilder sb = new StringBuilder();
    final int end = Math.min(keys.size(), position + keysCount);
    final Map<String, Object> params = new HashMap<>();
    for (int index = position; index < end; index++) {
      sb.append(
          batchGetSelectString(
              opContext,
              index - position,
              keys.get(index).getUrn(),
              keys.get(index).getAspect(),
              keys.get(index).getVersion(),
              params));

      if (index != end - 1) {
        sb.append(" UNION ALL ");
      }
    }

    // Add FOR UPDATE clause only once at the end of the entire statement
    if (lockRows) {
      // Defense-in-depth: PostgreSQL rejects FOR UPDATE with UNION, and the constructor already
      // coerces PostgreSQL to the IN batch-get. This guards against a future regression routing a
      // locking read through the UNION path on PostgreSQL.
      if (isPostgres) {
        throw new IllegalStateException(
            "UNION batch-get cannot take FOR UPDATE on PostgreSQL; use the IN batch-get method.");
      }
      sb.append(" FOR UPDATE");
    }

    final RawSql rawSql = parseBatchGetRawSql(sb.toString());

    final Query<EbeanAspectV2> query =
        resolveBatchGetDatabase(opContext, writeIntent).find(EbeanAspectV2.class).setRawSql(rawSql);

    for (Map.Entry<String, Object> param : params.entrySet()) {
      query.setParameter(param.getKey(), param.getValue());
    }

    return txnFactory.runInScope(opContext, query::findList);
  }

  @Nonnull
  private List<EbeanAspectV2> batchGetIn(
      @Nonnull OperationContext opContext,
      @Nonnull final List<EbeanAspectV2.PrimaryKey> keys,
      final int keysCount,
      final int position,
      boolean lockRows,
      boolean writeIntent) {
    validateConnection();

    // Build a single SELECT with IN clause using composite key comparison
    // Query will look like:
    // SELECT * FROM metadata_aspect WHERE (urn, aspect, version) IN
    // (('urn0', 'aspect0', 0), ('urn1', 'aspect1', 1))
    final StringBuilder sb = new StringBuilder();
    sb.append(
        "SELECT urn, aspect, version, metadata, systemMetadata, createdOn, createdBy, createdFor ");
    sb.append("FROM")
        .append(tableResolver.aspectTable(opContext, EbeanAspectV2.TABLE_NAME))
        .append("WHERE (urn, aspect, version) IN (");

    final int end = Math.min(keys.size(), position + keysCount);
    final Map<String, Object> params = new HashMap<>();

    for (int index = position; index < end; index++) {
      int paramIndex = index - position;
      String urnParam = "urn" + paramIndex;
      String aspectParam = "aspect" + paramIndex;
      String versionParam = "version" + paramIndex;

      params.put(urnParam, keys.get(index).getUrn());
      params.put(aspectParam, keys.get(index).getAspect());
      params.put(versionParam, keys.get(index).getVersion());

      sb.append("(:" + urnParam + ", :" + aspectParam + ", :" + versionParam + ")");

      if (index != end - 1) {
        sb.append(",");
      }
    }

    sb.append(")");

    if (lockRows) {
      // On PostgreSQL, ORDER BY forces a Sort/ordered-scan below the LockRows executor node, so row
      // locks are acquired in (urn, aspect, version) order instead of physical/CTID scan order,
      // preventing lock-order deadlocks between concurrent writers. MySQL/InnoDB already locks the
      // primary-key IN-list in index order, so the clause is only added for Postgres. Keys are
      // globally pre-sorted in batchGet before chunking, so per-chunk ORDER BY plus sequential
      // (same-transaction) chunk execution preserves one global lock order across chunk boundaries.
      if (isPostgres) {
        sb.append(" ORDER BY ").append(EbeanAspectV2.KEY_ORDER_BY_SQL);
      }
      sb.append(" FOR UPDATE");
    }

    final RawSql rawSql = parseBatchGetRawSql(sb.toString());

    final Query<EbeanAspectV2> query =
        resolveBatchGetDatabase(opContext, writeIntent).find(EbeanAspectV2.class).setRawSql(rawSql);

    for (Map.Entry<String, Object> param : params.entrySet()) {
      query.setParameter(param.getKey(), param.getValue());
    }

    return txnFactory.runInScope(opContext, query::findList);
  }

  @VisibleForTesting
  @Nonnull
  Database resolveBatchGetDatabase(@Nonnull OperationContext opContext, boolean writeIntent) {
    return primaryStorageResolver.resolveEbean(opContext, writeIntent);
  }

  @Override
  @Nonnull
  public ListResult<String> listUrns(
      @Nonnull OperationContext opContext,
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final int start,
      final int pageSize) {

    validateConnection();

    return txnFactory.runInScope(
        opContext,
        () -> {
          final String urnPrefixMatcher = "urn:li:" + entityName + ":%";
          final PagedList<EbeanAspectV2> pagedList =
              server
                  .find(EbeanAspectV2.class)
                  .select(EbeanAspectV2.KEY_ID)
                  .where()
                  .like(EbeanAspectV2.URN_COLUMN, urnPrefixMatcher)
                  .eq(EbeanAspectV2.ASPECT_COLUMN, aspectName)
                  .eq(EbeanAspectV2.VERSION_COLUMN, ASPECT_LATEST_VERSION)
                  .setFirstRow(start)
                  .setMaxRows(pageSize)
                  .orderBy()
                  .asc(EbeanAspectV2.URN_COLUMN)
                  .findPagedList();

          final List<String> urns =
              pagedList.getList().stream()
                  .map(entry -> entry.getKey().getUrn())
                  .collect(Collectors.toList());

          return toListResult(urns, null, pagedList, start);
        });
  }

  @Nonnull
  @Override
  public Integer countAspect(
      @Nonnull OperationContext opContext, @Nonnull String aspectName, @Nullable String urnLike) {
    return txnFactory.runInScope(
        opContext,
        () -> {
          ExpressionList<EbeanAspectV2> exp =
              server
                  .find(EbeanAspectV2.class)
                  .select(EbeanAspectV2.KEY_ID)
                  .where()
                  .eq(EbeanAspectV2.VERSION_COLUMN, ASPECT_LATEST_VERSION)
                  .eq(EbeanAspectV2.ASPECT_COLUMN, aspectName);

          if (urnLike != null) {
            exp = exp.like(EbeanAspectV2.URN_COLUMN, urnLike);
          }
          return exp.findCount();
        });
  }

  @Nonnull
  @Override
  public Integer countAspect(@Nonnull OperationContext opContext, final RestoreIndicesArgs args) {
    return txnFactory.runInScope(opContext, () -> buildExpressionList(args, true).findCount());
  }

  @Nullable
  @Override
  public <R> R streamAspectBatches(
      @Nonnull final OperationContext opContext,
      @Nonnull final RestoreIndicesArgs args,
      @Nonnull final Function<PartitionedStream<EbeanAspectV2>, R> consumer) {
    // Use the database default isolation for existing RestoreIndices operations.
    return streamAspectBatches(opContext, args, null, consumer);
  }

  /**
   * Consume-in-scope streaming of aspects ordered by URN/aspect for optimal Elasticsearch document
   * batching, with an optional transaction isolation override (e.g. LoadIndices scans with {@link
   * TxIsolation#READ_UNCOMMITTED}).
   *
   * <p>{@code findStream()} is lazy — its JDBC cursor pulls rows as {@code consumer} iterates the
   * stream, so the cursor's connection has to stay routed for the whole consumption when an
   * extension routes queries to different backend databases. We get that from Ebean itself rather
   * than by holding an explicit transaction open across the consumer:
   *
   * <ul>
   *   <li><b>Default path ({@code isolationLevel == null}) — {@link
   *       ScopedTransactionFactory#scope}, no explicit transaction.</b> Ebean opens its <i>own</i>
   *       implicit read-only transaction for the {@code findStream()} query, routed from the
   *       ambient scope. Crucially that implicit transaction is <b>not thread-current</b>, so any
   *       nested query the {@code consumer} runs (e.g. {@code getLatestAspects}, {@code
   *       ingestProposal}) opens a <i>separate</i> implicit transaction on its own connection —
   *       instead of colliding with the still-open cursor ("another command is already in
   *       progress") or forcing every per-batch side effect into one long-lived transaction. The
   *       scope only has to stay open across consumption so those nested lookups still route
   *       correctly.
   *   <li><b>Isolation path ({@code isolationLevel != null}) — explicit {@link
   *       ScopedTransactionFactory#begin}.</b> An isolation override can only ride a real
   *       transaction, which becomes thread-current — so callers on this path (only LoadIndices)
   *       must not run nested aspect queries inside the {@code consumer}. LoadIndices does not: its
   *       consumer only converts rows and writes to Elasticsearch.
   * </ul>
   *
   * <p>The implicit read-only query transaction is created by ebean-core (pinned {@code 15.5.2},
   * identical at the public {@code 15.1.0} tag) in <a
   * href="https://github.com/ebean-orm/ebean/blob/15.1.0/ebean-core/src/main/java/io/ebeaninternal/server/core/OrmQueryRequest.java#L204-L214">{@code
   * OrmQueryRequest#initTransIfRequired}</a>.
   *
   * @param args Stream arguments and filters
   * @param isolationLevel Optional isolation level override (null = database default)
   * @param consumer processes the partitioned stream inside the scope and returns a result; it must
   *     fully consume the stream before returning
   * @return whatever {@code consumer} returns
   */
  public <R> R streamAspectBatches(
      @Nonnull final OperationContext opContext,
      @Nonnull final RestoreIndicesArgs args,
      @Nullable final TxIsolation isolationLevel,
      @Nonnull final Function<PartitionedStream<EbeanAspectV2>, R> consumer) {
    if (isolationLevel != null) {
      // An isolation override can only ride a real transaction. It becomes thread-current, so the
      // cursor and any nested query would share one connection — safe only because this path's sole
      // caller (LoadIndices) runs no nested aspect queries in its consumer.
      try (Transaction tx =
              txnFactory.begin(opContext, TxScope.requiresNew().setIsolation(isolationLevel));
          PartitionedStream<EbeanAspectV2> partitioned =
              PartitionedStream.<EbeanAspectV2>builder()
                  .delegateStream(buildStreamQuery(args))
                  .build()) {
        final R result = consumer.apply(partitioned);
        tx.commit();
        return result;
      }
    }
    // Default path: keep a routing scope open (not a transaction). Ebean's own implicit read-only
    // transaction for findStream() is not thread-current, so nested queries the consumer runs get
    // their own connection (see javadoc).
    try (ScopedTransactionFactory.Scope scope = txnFactory.scope(opContext);
        PartitionedStream<EbeanAspectV2> partitioned =
            PartitionedStream.<EbeanAspectV2>builder()
                .delegateStream(buildStreamQuery(args))
                .build()) {
      return consumer.apply(partitioned);
    }
  }

  @Nonnull
  private Stream<EbeanAspectV2> buildStreamQuery(@Nonnull final RestoreIndicesArgs args) {
    ExpressionList<EbeanAspectV2> exp = buildExpressionList(args, false);
    if (args.limit > 0) {
      exp = exp.setMaxRows(args.limit);
    }
    final int start = args.urnBasedPagination ? 0 : args.start;
    return exp.orderBy()
        .asc(EbeanAspectV2.URN_COLUMN)
        .orderBy()
        .asc(EbeanAspectV2.ASPECT_COLUMN)
        .setFirstRow(start)
        .findStream();
  }

  private ExpressionList<EbeanAspectV2> buildExpressionList(
      RestoreIndicesArgs args, boolean forCount) {
    ExpressionList<EbeanAspectV2> exp =
        server
            .find(EbeanAspectV2.class)
            .select(forCount ? EbeanAspectV2.KEY_ID : EbeanAspectV2.ALL_COLUMNS)
            .where()
            .eq(EbeanAspectV2.VERSION_COLUMN, ASPECT_LATEST_VERSION);
    if (args.aspectName != null) {
      exp = exp.eq(EbeanAspectV2.ASPECT_COLUMN, args.aspectName);
    }
    if (args.aspectNames != null && !args.aspectNames.isEmpty()) {
      exp = exp.in(EbeanAspectV2.ASPECT_COLUMN, args.aspectNames);
    }
    if (args.urn != null) {
      exp = exp.eq(EbeanAspectV2.URN_COLUMN, args.urn);
    }
    if (args.urnLike != null) {
      exp = exp.like(EbeanAspectV2.URN_COLUMN, args.urnLike);
    }
    // Apply the time-range bounds independently: an unset bound is left at 0, and applying
    // `createdon <= epoch(0)` when only gePitEpochMs was provided would silently match zero rows.
    if (args.gePitEpochMs > 0) {
      exp =
          exp.ge(
              EbeanAspectV2.CREATED_ON_COLUMN,
              Timestamp.from(Instant.ofEpochMilli(args.gePitEpochMs)));
    }
    if (args.lePitEpochMs > 0) {
      exp =
          exp.le(
              EbeanAspectV2.CREATED_ON_COLUMN,
              Timestamp.from(Instant.ofEpochMilli(args.lePitEpochMs)));
    }

    if (args.urnBasedPagination) {
      if (args.lastUrn != null && !args.lastUrn.isEmpty()) {
        exp = exp.where().ge(EbeanAspectV2.URN_COLUMN, args.lastUrn);

        // To prevent processing the same aspect multiple times in a restore, it compares against
        // the last aspect if the urn matches the last urn
        if (args.lastAspect != null && !args.lastAspect.isEmpty()) {
          exp =
              exp.where()
                  .and()
                  .or()
                  .ne(EbeanAspectV2.URN_COLUMN, args.lastUrn)
                  .gt(EbeanAspectV2.ASPECT_COLUMN, args.lastAspect);
        }
      }
    }
    return exp;
  }

  /**
   * TODO(op-propagation): Not routed through {@link ScopedTransactionFactory}: the {@code
   * AspectDao} contract marks this method {@code @OperationContextExempt}, so there is no {@link
   * OperationContext} to scope against. It runs against the single configured primary regardless of
   * which routing context is in effect. A downstream module routing to different backend databases
   * must either block this path while its routing mode is active, or wait for it to be wired —
   * thread an {@link OperationContext} through this method (and {@code
   * AspectDao#streamAspectBatchesForMigration}) to close the gap.
   */
  @Override
  @Nonnull
  public PartitionedStream<EbeanAspectV2> streamAspectBatchesForMigration(
      @Nonnull Map<String, Long> aspectTargetVersions,
      long afterCreatedOnMs,
      int batchSize,
      int limit) {
    validateConnection();

    // Only include aspects whose target version is above the default — nothing to migrate
    // otherwise.
    Map<String, Long> versionedAspects =
        aspectTargetVersions.entrySet().stream()
            .filter(e -> e.getValue() > DEFAULT_SCHEMA_VERSION)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    if (versionedAspects.isEmpty()) {
      return PartitionedStream.<EbeanAspectV2>builder().delegateStream(Stream.empty()).build();
    }

    // Build: OR over aspects of (aspect = X AND schemaVersion != targetVersion(X))
    // "not at target" means: schemaVersion key is absent, OR its value != target.
    ExpressionList<EbeanAspectV2> base =
        server
            .find(EbeanAspectV2.class)
            .select(EbeanAspectV2.ALL_COLUMNS)
            .where()
            .eq(EbeanAspectV2.VERSION_COLUMN, ASPECT_LATEST_VERSION);

    if (afterCreatedOnMs > 0) {
      base =
          base.ge(
              EbeanAspectV2.CREATED_ON_COLUMN,
              Timestamp.from(Instant.ofEpochMilli(afterCreatedOnMs)));
    }

    // Use LIKE-based raw() rather than Ebean's jsonEqualTo() / jsonNotEqualTo(): jsonEqualTo() on
    // Postgres generates "(col ->> 'key')::bigint = ?" where the bind parameter is untyped, causing
    // "operator does not exist: bigint = unknown" at runtime. NOT LIKE on the serialised JSON
    // is DB-agnostic; schemaVersion values are small integers that cannot collide with other
    // JSON key or value substrings (the key name "schemaVersion" is controlled by us).
    final io.ebean.Junction<EbeanAspectV2> aspectOr = base.or();
    for (Map.Entry<String, Long> entry : versionedAspects.entrySet()) {
      long target = entry.getValue();
      // Per-aspect: (aspect = X) AND (schemaVersion absent OR schemaVersion != target)
      aspectOr
          .and()
          .eq(EbeanAspectV2.ASPECT_COLUMN, entry.getKey())
          .or()
          .raw(
              "("
                  + EbeanAspectV2.SYSTEM_METADATA_COLUMN
                  + " IS NULL OR "
                  + EbeanAspectV2.SYSTEM_METADATA_COLUMN
                  + " NOT LIKE '%\"schemaVersion\"%')")
          .raw(
              EbeanAspectV2.SYSTEM_METADATA_COLUMN + " NOT LIKE ?",
              "%" + "\"schemaVersion\":" + target + "%")
          .endOr()
          .endAnd();
    }

    ExpressionList<EbeanAspectV2> exp = aspectOr.endOr();

    if (limit > 0) {
      exp = exp.setMaxRows(limit);
    }

    Stream<EbeanAspectV2> stream = exp.orderBy().asc(EbeanAspectV2.CREATED_ON_COLUMN).findStream();

    return PartitionedStream.<EbeanAspectV2>builder().delegateStream(stream).build();
  }

  /**
   * Warning the stream must be closed
   *
   * <p>TODO(op-propagation): Not routed through {@link ScopedTransactionFactory}: the {@code
   * AspectDao} contract marks this method {@code @OperationContextExempt}, so there is no {@link
   * OperationContext} to scope against. It runs against the single configured primary regardless of
   * which routing context is in effect. A downstream module routing to different backend databases
   * must either block this path while its routing mode is active, or wait for it to be wired —
   * thread an {@link OperationContext} through this method (and {@code
   * AspectDao#streamAspectBatchesForMigration}) to close the gap.
   *
   * @param entityName
   * @param aspectName
   * @return
   */
  @Override
  @Nonnull
  public Stream<EntityAspect> streamAspects(
      @Nonnull String entityName, @Nonnull String aspectName) {
    ExpressionList<EbeanAspectV2> exp =
        server
            .find(EbeanAspectV2.class)
            .select(EbeanAspectV2.ALL_COLUMNS)
            .where()
            .eq(EbeanAspectV2.VERSION_COLUMN, ASPECT_LATEST_VERSION)
            .eq(EbeanAspectV2.ASPECT_COLUMN, aspectName)
            .like(EbeanAspectV2.URN_COLUMN, "urn:li:" + entityName + ":%");
    return exp.query().findStream().map(EbeanAspectV2::toEntityAspect);
  }

  @Override
  @Nonnull
  public Iterable<String> listAllUrns(
      @Nonnull OperationContext opContext, int start, int pageSize) {
    validateConnection();
    return txnFactory.runInScope(
        opContext,
        () -> {
          PagedList<EbeanAspectV2> ebeanAspects =
              server
                  .find(EbeanAspectV2.class)
                  .setDistinct(true)
                  .select(EbeanAspectV2.URN_COLUMN)
                  .orderBy()
                  .asc(EbeanAspectV2.URN_COLUMN)
                  .setFirstRow(start)
                  .setMaxRows(pageSize)
                  .findPagedList();
          return ebeanAspects.getList().stream()
              .map(EbeanAspectV2::getUrn)
              .collect(Collectors.toList());
        });
  }

  @Override
  @Nonnull
  public ListResult<String> listAspectMetadata(
      @Nonnull OperationContext opContext,
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final long version,
      final int start,
      final int pageSize) {

    validateConnection();

    return txnFactory.runInScope(
        opContext,
        () -> {
          final String urnPrefixMatcher = "urn:li:" + entityName + ":%";
          final PagedList<EbeanAspectV2> pagedList =
              server
                  .find(EbeanAspectV2.class)
                  .select(EbeanAspectV2.ALL_COLUMNS)
                  .where()
                  .like(EbeanAspectV2.URN_COLUMN, urnPrefixMatcher)
                  .eq(EbeanAspectV2.ASPECT_COLUMN, aspectName)
                  .eq(EbeanAspectV2.VERSION_COLUMN, version)
                  .setFirstRow(start)
                  .setMaxRows(pageSize)
                  .orderBy()
                  .asc(EbeanAspectV2.URN_COLUMN)
                  .findPagedList();

          final List<String> aspects =
              pagedList.getList().stream()
                  .map(EbeanAspectV2::getMetadata)
                  .collect(Collectors.toList());
          final ListResultMetadata listResultMetadata =
              toListResultMetadata(
                  pagedList.getList().stream()
                      .map(EbeanAspectDao::toExtraInfo)
                      .collect(Collectors.toList()));
          return toListResult(aspects, listResultMetadata, pagedList, start);
        });
  }

  @Override
  @Nonnull
  public ListResult<String> listLatestAspectMetadata(
      @Nonnull OperationContext opContext,
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final int start,
      final int pageSize) {

    return listAspectMetadata(
        opContext, entityName, aspectName, ASPECT_LATEST_VERSION, start, pageSize);
  }

  @Override
  @Nonnull
  public <T> Optional<T> runInTransactionWithRetry(
      @Nonnull OperationContext opContext,
      @Nonnull final Function<TransactionContext, TransactionResult<T>> block,
      final int maxTransactionRetry) {
    return runInTransactionWithRetryUnlocked(opContext, block, null, maxTransactionRetry)
        .getResults();
  }

  @Override
  @Nonnull
  public <T> Optional<T> runInTransactionWithRetry(
      @Nonnull OperationContext opContext,
      @Nonnull final Function<TransactionContext, TransactionResult<T>> block,
      @Nullable AspectsBatch batch,
      final int maxTransactionRetry) {

    return runInTransactionWithRetryUnlocked(opContext, block, batch, maxTransactionRetry)
        .getResults();
  }

  @Nonnull
  public <T> TransactionResult<T> runInTransactionWithRetryUnlocked(
      @Nonnull OperationContext opContext,
      @Nonnull final Function<TransactionContext, TransactionResult<T>> block,
      @Nullable AspectsBatch batch,
      final int maxTransactionRetry) {

    validateConnection();
    TransactionContext transactionContext = TransactionContext.empty(maxTransactionRetry);

    // Default state is rollback
    TransactionResult<T> result = TransactionResult.rollback();
    do {
      // Use TxScope.requiresNew() without setIsolation(): explicit isolation forces JDBC
      // Connection.setTransactionIsolation on every begin, which PostgreSQL rejects if the pooled
      // connection already has an active transaction ("Cannot change transaction isolation level in
      // the middle of a transaction"). READ COMMITTED remains the effective level via pool defaults
      // on the metadata DataSource where configured.
      try (Transaction transaction = txnFactory.begin(opContext, TxScope.requiresNew())) {
        transaction.setBatchMode(true);
        result = block.apply(transactionContext.tx(transaction));
        if (result.isCommitOrRollback()) {
          transaction.commit();
        } else {
          transaction.rollback();
        }
        transactionContext.success();
        break;
      } catch (PersistenceException exception) {
        boolean optimisticConflict = exception instanceof OptimisticLockConflictException;
        if (optimisticConflict) {
          incrementOptimisticMetric("optimistic_lock_retry");
        }
        if (exception instanceof DuplicateKeyException) {
          if (!optimisticLocking
              && batch != null
              && batch.getItems().stream()
                  .allMatch(
                      a ->
                          a.getAspectName()
                              .equals(a.getEntitySpec().getKeyAspectSpec().getName()))) {
            log.warn(
                "Skipping DuplicateKeyException retry since aspect is the key aspect. {}",
                batch.getUrnAspectsMap().keySet());
            break;
          }
        }

        if (metricUtils != null)
          metricUtils.increment(MetricRegistry.name(this.getClass(), "txFailed"), 1);

        boolean backoff = optimisticConflict || transactionRetryPolicy.shouldBackoff(exception);
        SQLException matchedSql = transactionRetryPolicy.findMatchingSqlError(exception);
        transactionContext.addException(exception);
        // Sleep only when another attempt will run — skip delay before exhaustion throw.
        // try-with-resources closes/rolls back the Transaction before this catch runs, so the
        // DB connection is returned to the pool during backoff sleep.
        if (backoff && transactionContext.shouldAttemptRetry()) {
          if (metricUtils != null) {
            metricUtils.incrementMicrometer(
                "ebean.tx.transient_backoff", 1.0, transientMetricTags(batch));
          }
          log.warn(
              "Retryable PersistenceException with backoff: sqlState={}, vendorCode={}, message={}",
              matchedSql != null ? matchedSql.getSQLState() : null,
              matchedSql != null ? matchedSql.getErrorCode() : null,
              exception.getMessage());
          // attempt index: exceptions.size()-1 → 0 on first retry after the initial failure
          sleepBeforeRetry(
              transactionRetryPolicy.backoffMillis(transactionContext.exceptions().size() - 1));
        } else if (backoff) {
          if (metricUtils != null) {
            metricUtils.incrementMicrometer(
                "ebean.tx.transient_exhausted", 1.0, transientMetricTags(batch));
          }
          log.warn(
              "Retryable PersistenceException with backoff (retries exhausted): sqlState={}, vendorCode={}, message={}",
              matchedSql != null ? matchedSql.getSQLState() : null,
              matchedSql != null ? matchedSql.getErrorCode() : null,
              exception.getMessage());
        } else {
          log.warn("Retryable PersistenceException: {}", exception.getMessage());
        }
      }
    } while (transactionContext.shouldAttemptRetry());

    if (transactionContext.lastException() != null) {
      if (metricUtils != null)
        metricUtils.increment(MetricRegistry.name(this.getClass(), "txFailedAfterRetries"), 1);
      RuntimeException last = transactionContext.lastException();
      boolean optimisticConflict = last instanceof OptimisticLockConflictException;
      if (optimisticConflict) {
        incrementOptimisticMetric("optimistic_lock_retry_exhausted");
      }
      if (optimisticConflict || transactionRetryPolicy.shouldBackoff(last)) {
        SQLException matchedSql = transactionRetryPolicy.findMatchingSqlError(last);
        String sqlState = matchedSql != null ? matchedSql.getSQLState() : null;
        throw new DatabaseTransactionConflictException(
            "Failed to add after " + maxTransactionRetry + " retries due to transaction conflict",
            sqlState,
            last,
            transactionRetryPolicy.getRetryAfterSeconds());
      }
      throw new RetryLimitReached("Failed to add after " + maxTransactionRetry + " retries", last);
    }

    return result;
  }

  /**
   * Metric path for the two EntityServiceImpl call sites: ingest (batch present) and delete (no
   * batch). Other writers (link/restore/migrate) go through ingestAspectsToLocalDB → {@code
   * ingest}.
   */
  @VisibleForTesting
  static String[] transientMetricTags(@Nullable AspectsBatch batch) {
    return new String[] {"path", batch != null ? "ingest" : "delete"};
  }

  @VisibleForTesting
  protected void sleepBeforeRetry(long backoffMs) {
    if (backoffMs <= 0) {
      return;
    }
    try {
      Thread.sleep(backoffMs);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RetryLimitReached("Interrupted during retry backoff", ie);
    }
  }

  @Override
  @Nonnull
  public Pair<Long, Long> getVersionRange(
      @Nonnull OperationContext opContext,
      @Nonnull final String urn,
      @Nonnull final String aspectName) {
    validateConnection();

    return txnFactory.runInScope(
        opContext,
        () -> {
          // Use SQL aggregation to get both min and max in a single query
          SqlQuery query =
              server.sqlQuery(
                  "SELECT MIN(version) as min_version, MAX(version) as max_version "
                      + "FROM"
                      + tableResolver.aspectTable(opContext, EbeanAspectV2.TABLE_NAME)
                      + "WHERE urn = :urn AND aspect = :aspect");

          query.setParameter("urn", urn);
          query.setParameter("aspect", aspectName);

          SqlRow result = query.findOne();

          if (result == null) {
            return Pair.of(-1L, -1L);
          }

          Long minVersion = result.getLong("min_version");
          Long maxVersion = result.getLong("max_version");
          if (minVersion == null || maxVersion == null) {
            // MySQL returns a row with NULL MIN/MAX when no versions exist for urn+aspect.
            return Pair.of(-1L, -1L);
          }

          return Pair.of(minVersion, maxVersion);
        });
  }

  @Override
  public long getMaxVersion(
      @Nonnull OperationContext opContext,
      @Nonnull final String urn,
      @Nonnull final String aspectName) {
    return getVersionRange(opContext, urn, aspectName).getSecond();
  }

  @VisibleForTesting
  @Nonnull
  Database resolveGetNextVersionsDatabase(
      @Nonnull OperationContext opContext, boolean lockLatestForWrite) {
    return lockLatestForWrite
        ? primaryStorageResolver.resolveEbeanPrimary()
        : primaryStorageResolver.resolveEbean(opContext, false);
  }

  /**
   * This method is only used as a fallback. It does incur an extra read-lock that is naturally a
   * result of getLatestAspects(, forUpdate=true)
   *
   * @param urnAspects urn and aspect names to fetch
   * @return map of the aspect's next version
   */
  public Map<String, Map<String, Long>> getNextVersions(
      @Nonnull OperationContext opContext,
      @Nonnull Map<String, Set<String>> urnAspects,
      boolean lockLatestForWrite) {
    return getNextVersions(opContext, null, urnAspects, lockLatestForWrite);
  }

  @Override
  public Map<String, Map<String, Long>> getNextVersions(
      @Nonnull OperationContext opContext,
      @Nullable TransactionContext txContext,
      @Nonnull Map<String, Set<String>> urnAspects,
      boolean lockLatestForWrite) {
    validateConnection();

    return txnFactory.runInScope(
        opContext,
        () -> {
          List<EbeanAspectV2.PrimaryKey> forUpdateKeys = new ArrayList<>();

          // initialize with default next version of 0
          Map<String, Map<String, Long>> result =
              new HashMap<>(
                  urnAspects.entrySet().stream()
                      .map(
                          entry -> {
                            Map<String, Long> defaultNextVersion = new HashMap<>();
                            entry
                                .getValue()
                                .forEach(
                                    aspectName -> {
                                      defaultNextVersion.put(aspectName, ASPECT_LATEST_VERSION);
                                      forUpdateKeys.add(
                                          new EbeanAspectV2.PrimaryKey(
                                              entry.getKey(), aspectName, ASPECT_LATEST_VERSION));
                                    });
                            return Map.entry(entry.getKey(), defaultNextVersion);
                          })
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

          // forUpdate is required to avoid duplicate key violations (it is used as an indication
          // that the max(version) was invalidated
          if (canWrite && lockLatestForWrite && !optimisticLocking) {
            // Acquire the version-0 row locks in canonical key order so concurrent writers never
            // lock overlapping rows in opposite orders. The Java-side sort covers MySQL/InnoDB
            // (which locks the primary-key IN-list in index order); on PostgreSQL a bare
            // idIn(...).forUpdate() would lock in physical scan order, so the explicit ORDER BY is
            // added to place a Sort below the LockRows node and force key-order lock acquisition.
            Collections.sort(forUpdateKeys);
            final Query<EbeanAspectV2> lockQuery =
                server.find(EbeanAspectV2.class).where().idIn(forUpdateKeys).query();
            if (isPostgres) {
              lockQuery.orderBy(EbeanAspectV2.KEY_ORDER_BY_PROPERTY_PATH);
            }
            lockQuery.forUpdate().findList();
          }

          // Write path must read max(version) from primary to avoid stale replica counts after
          // forUpdate.
          Database versionDatabase = resolveGetNextVersionsDatabase(opContext, lockLatestForWrite);
          Junction<EbeanAspectV2> queryJunction =
              versionDatabase
                  .find(EbeanAspectV2.class)
                  .select("urn, aspect, max(version)")
                  .where()
                  .in("urn", urnAspects.keySet())
                  .or();

          ExpressionList<EbeanAspectV2> exp = null;
          for (Map.Entry<String, Set<String>> entry : urnAspects.entrySet()) {
            if (exp == null) {
              exp =
                  queryJunction
                      .and()
                      .eq("urn", entry.getKey())
                      .in("aspect", entry.getValue())
                      .endAnd();
            } else {
              exp = exp.and().eq("urn", entry.getKey()).in("aspect", entry.getValue()).endAnd();
            }
          }

          if (exp == null) {
            return result;
          }

          List<EbeanAspectV2.PrimaryKey> dbResults = exp.endOr().findIds();

          mergeNextVersionsFromDb(result, dbResults);

          return result;
        });
  }

  /**
   * Merges DB max-version results into the request-keyed result map. Package-private for testing.
   */
  static void mergeNextVersionsFromDb(
      Map<String, Map<String, Long>> result, List<EbeanAspectV2.PrimaryKey> dbResults) {
    for (EbeanAspectV2.PrimaryKey key : dbResults) {
      try {
        if (result.get(key.getUrn()).get(key.getAspect()) <= key.getVersion()) {
          result.get(key.getUrn()).put(key.getAspect(), key.getVersion() + 1L);
        }
      } catch (NullPointerException e) {
        throw new IllegalStateException(
            "URN or aspect from database did not match request keys (urn=\""
                + key.getUrn()
                + "\", aspect=\""
                + key.getAspect()
                + "\"). "
                + "Possible cause: MySQL database/table created with different charset or collation "
                + "than mysql-setup (ensure CHARACTER SET utf8mb4 COLLATE utf8mb4_bin). "
                + "Create schema via docker/mysql-setup or datahub-upgrade SqlSetup with aligned DDL.",
            e);
      }
    }
  }

  @Nonnull
  private <T> ListResult<T> toListResult(
      @Nonnull final List<T> values,
      @Nullable final ListResultMetadata listResultMetadata,
      @Nonnull final PagedList<?> pagedList,
      @Nullable final Integer start) {
    final int nextStart =
        (start != null && pagedList.hasNext())
            ? start + pagedList.getList().size()
            : ListResult.INVALID_NEXT_START;
    return ListResult.<T>builder()
        // Format
        .values(values)
        .metadata(listResultMetadata)
        .nextStart(nextStart)
        .hasNext(pagedList.hasNext())
        .totalCount(pagedList.getTotalCount())
        .totalPageCount(pagedList.getTotalPageCount())
        .pageSize(pagedList.getPageSize())
        .build();
  }

  @Nonnull
  private static ExtraInfo toExtraInfo(@Nonnull final EbeanAspectV2 aspect) {
    final ExtraInfo extraInfo = new ExtraInfo();
    extraInfo.setVersion(aspect.getKey().getVersion());
    extraInfo.setAudit(toAuditStamp(aspect));
    try {
      extraInfo.setUrn(Urn.createFromString(aspect.getKey().getUrn()));
    } catch (URISyntaxException e) {
      throw new ModelConversionException(e.getMessage());
    }

    return extraInfo;
  }

  @Nonnull
  private static AuditStamp toAuditStamp(@Nonnull final EbeanAspectV2 aspect) {
    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(aspect.getCreatedOn().getTime());

    try {
      auditStamp.setActor(new Urn(aspect.getCreatedBy()));
      if (aspect.getCreatedFor() != null) {
        auditStamp.setImpersonator(new Urn(aspect.getCreatedFor()));
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return auditStamp;
  }

  @Nonnull
  private ListResultMetadata toListResultMetadata(@Nonnull final List<ExtraInfo> extraInfos) {
    final ListResultMetadata listResultMetadata = new ListResultMetadata();
    listResultMetadata.setExtraInfos(new ExtraInfoArray(extraInfos));
    return listResultMetadata;
  }

  @Override
  @Nonnull
  public List<EntityAspect> getAspectsInRange(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      Set<String> aspectNames,
      long startTimeMillis,
      long endTimeMillis) {
    validateConnection();
    return txnFactory.runInScope(
        opContext,
        () -> {
          List<EbeanAspectV2> ebeanAspects =
              primaryStorageResolver
                  .resolveEbean(opContext, false)
                  .find(EbeanAspectV2.class)
                  .select(EbeanAspectV2.ALL_COLUMNS)
                  .where()
                  .eq(EbeanAspectV2.URN_COLUMN, urn.toString())
                  .in(EbeanAspectV2.ASPECT_COLUMN, aspectNames)
                  .inRange(
                      EbeanAspectV2.CREATED_ON_COLUMN,
                      new Timestamp(startTimeMillis),
                      new Timestamp(endTimeMillis))
                  .findList();
          return ebeanAspects.stream()
              .map(EbeanAspectV2::toEntityAspect)
              .collect(Collectors.toList());
        });
  }

  private Map<String, SystemAspect> toAspectMap(
      @Nonnull EntityRegistry entityRegistry,
      Set<EbeanAspectV2> beans,
      @Nullable io.datahubproject.metadata.context.OperationContext opContext) {
    return beans.stream()
        .map(bean -> Map.entry(bean.getAspect(), bean))
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e ->
                    EbeanSystemAspect.builder()
                        .systemAspectValidators(systemAspectValidators)
                        .validationConfig(validationConfig)
                        .operationContext(opContext)
                        .forUpdate(e.getValue(), entityRegistry)));
  }

  private Map<String, Map<String, SystemAspect>> toUrnAspectMap(
      @Nonnull EntityRegistry entityRegistry,
      Collection<EbeanAspectV2> beans,
      @Nullable io.datahubproject.metadata.context.OperationContext opContext) {
    return beans.stream()
        .collect(Collectors.groupingBy(EbeanAspectV2::getUrn, Collectors.toSet()))
        .entrySet()
        .stream()
        .map(e -> Map.entry(e.getKey(), toAspectMap(entityRegistry, e.getValue(), opContext)))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
