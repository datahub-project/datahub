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
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.entity.ListResult;
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
import io.ebean.Transaction;
import io.ebean.TxScope;
import io.ebean.annotation.Platform;
import io.ebean.annotation.TxIsolation;
import jakarta.persistence.PersistenceException;
import jakarta.persistence.Table;
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

  // Whether the primary store is PostgreSQL. The deterministic-lock-order work below (ORDER BY on
  // FOR UPDATE reads, the up-front lock in deleteUrn) targets PostgreSQL, whose default plan can
  // acquire row locks in physical scan order; and pg_advisory_xact_lock is Postgres-only SQL. On
  // MySQL/InnoDB the FOR UPDATE reads and bulk deletes already lock the primary-key IN-list in
  // index
  // order, so none of this is needed and nothing changes there.
  private final boolean isPostgres;
  // Opt-in per-entity write serialization via pg_advisory_xact_lock (Postgres only).
  private final boolean entityWriteAdvisoryLockEnabled;
  // Arbitrary fixed namespace for entity-write advisory locks, so they can't collide with other
  // pg_advisory lock users in the same database; hashtext(urn) supplies the per-entity key.
  private static final int ADVISORY_LOCK_NAMESPACE = 0x44480001;

  public EbeanAspectDao(
      @Nonnull final PrimaryStorageResolver primaryStorageResolver,
      EbeanConfiguration ebeanConfiguration,
      MetricUtils metricUtils,
      @Nonnull List<SystemAspectValidator> systemAspectValidators,
      @Nullable AspectSizeValidationConfiguration validationConfig) {
    this.primaryStorageResolver = primaryStorageResolver;
    this.server = primaryStorageResolver.resolveEbeanPrimary();
    // Resolve the engine from Ebean's own detection (live connection metadata), not the JDBC
    // url/driver string — correct even for Aurora Postgres or the AWS JDBC wrapper. Advisory locks
    // and the ORDER BY lock-ordering below are Postgres-specific.
    this.isPostgres = resolvePlatform(server) == Platform.POSTGRES;
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

    this.entityWriteAdvisoryLockEnabled = ebeanConfiguration.isEntityWriteAdvisoryLockEnabled();
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
   * Postgres-only, opt-in per-entity write serialization. When enabled, both the ingest write path
   * and {@link #deleteUrn} take a transaction-scoped advisory lock per urn <em>before</em>
   * acquiring any row locks, so a multi-row {@code FOR UPDATE} writer (e.g. logical-model linking)
   * and a concurrent hard-delete cannot interleave their row-lock acquisition into a cycle. The
   * advisory lock is released automatically on commit/rollback. No-op unless the store is Postgres
   * and the feature is enabled.
   *
   * <p>Keyed by {@code pg_advisory_xact_lock(<namespace>, hashtext(urn))}. {@code hashtext} is a
   * 32-bit hash, so distinct urns can collide on the same lock key and serialize against each
   * other. That is a false serialization: it costs throughput but never affects correctness. It is
   * acceptable here because the feature is opt-in and collisions are rare relative to the set of
   * entities being written concurrently.
   */
  @Override
  public void lockUrnsForWrite(
      @Nonnull OperationContext opContext, @Nonnull Collection<String> urns) {
    if (!canWrite || !entityWriteAdvisoryLockEnabled || !isPostgres || urns.isEmpty()) {
      return;
    }
    // Transaction-scoped: without an active transaction the advisory lock would auto-commit and
    // release immediately. All real callsites run inside runInTransactionWithRetry; if that ever
    // isn't the case, skip the lock with a warning rather than abort the caller's write.
    if (!hasActiveTransaction("lockUrnsForWrite")) {
      return;
    }
    // Acquire all the advisory locks in ONE round trip. Sort first so every transaction presents
    // the keys in the same order (advisory locks can self-deadlock across transactions otherwise),
    // then lock them in a single statement over a VALUES list — a logical-model link can carry
    // hundreds/thousands of urns, and a round trip per urn would swamp the DB. The VALUES scan
    // evaluates the (void-returning) function once per row, in list order. Result discarded.
    final List<String> sortedUrns = urns.stream().distinct().sorted().collect(Collectors.toList());
    final StringBuilder sql =
        new StringBuilder("select pg_advisory_xact_lock(:ns, hashtext(v.urn)) from (values ");
    for (int i = 0; i < sortedUrns.size(); i++) {
      if (i > 0) {
        sql.append(", ");
      }
      sql.append("(:u").append(i).append(")");
    }
    sql.append(") as v(urn)");
    final SqlQuery lockQuery =
        server.sqlQuery(sql.toString()).setParameter("ns", ADVISORY_LOCK_NAMESPACE);
    for (int i = 0; i < sortedUrns.size(); i++) {
      lockQuery.setParameter("u" + i, sortedUrns.get(i));
    }
    lockQuery.findList();
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

    EbeanAspectV2 ebeanAspectV2 = EbeanAspectV2.fromEntityAspect(aspect.asLatest());

    saveEbeanAspect(txContext, ebeanAspectV2, false);
    return Optional.of(ebeanAspectV2.toEntityAspect());
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
      return Optional.empty();
    }

    EbeanAspectV2 ebeanAspectV2 = EbeanAspectV2.fromEntityAspect(aspect.withVersion(version));

    saveEbeanAspect(txContext, ebeanAspectV2, true);
    return Optional.of(ebeanAspectV2.toEntityAspect());
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
  }

  @Override
  public void lockLatestRows(
      @Nonnull OperationContext opContext, @Nonnull Map<String, Set<String>> urnAspects) {
    validateConnection();

    // Read-only mode takes no row locks (mirrors the forUpdate && canWrite guard used elsewhere).
    if (!canWrite) {
      return;
    }

    // Build the LATEST-version (version 0) primary keys for the entire closure.
    final List<EbeanAspectV2.PrimaryKey> keys =
        urnAspects.entrySet().stream()
            .flatMap(
                entry ->
                    entry.getValue().stream()
                        .map(
                            aspect ->
                                new EbeanAspectV2.PrimaryKey(
                                    entry.getKey(), aspect, ASPECT_LATEST_VERSION)))
            .collect(Collectors.toCollection(ArrayList::new));

    if (keys.isEmpty()) {
      return;
    }

    // Sort the FULL keyset ONCE by natural PrimaryKey order (PrimaryKey.compareTo) so every
    // transaction acquires row locks in the same global order. Using PrimaryKey.compareTo -- the
    // same
    // ordering getNextVersions applies via Collections.sort -- keeps the lock order consistent
    // across
    // the statements/transactions that lock these rows (it strips trailing whitespace exactly as
    // the
    // key's equals/hashCode do). This is the deadlock-safety invariant: two different orderings of
    // the
    // same rows under FOR UPDATE cause lock-order deadlocks between concurrent writers.
    keys.sort(null);

    // Chunk the pre-sorted keyset (same queryKeysCount batchGet uses) to bound IN-clause size, and
    // FOR UPDATE each chunk in sorted order. KEY_ID projects only the @EmbeddedId (urn, aspect,
    // version) -- the indexed composite key -- so the metadata/systemMetadata @Lob blobs are never
    // fetched; the lock only needs the row locks taken via the key/index.
    final int chunkSize = queryKeysCount > 0 ? queryKeysCount : keys.size();
    for (int position = 0; position < keys.size(); position += chunkSize) {
      final List<EbeanAspectV2.PrimaryKey> chunk =
          keys.subList(position, Math.min(position + chunkSize, keys.size()));
      final Query<EbeanAspectV2> chunkQuery =
          primaryStorageResolver
              .resolveEbean(opContext, true)
              .find(EbeanAspectV2.class)
              .select(EbeanAspectV2.KEY_ID)
              .where()
              .idIn(chunk)
              .query();
      // PostgreSQL locks the IN-list in physical scan order -- force key-order lock acquisition
      // with
      // an explicit ORDER BY (MySQL/InnoDB already locks the PK IN-list in index order).
      if (isPostgres) {
        chunkQuery.orderBy(EbeanAspectV2.KEY_ORDER_BY_PROPERTY_PATH);
      }
      chunkQuery.forUpdate().findList();
    }
  }

  @Override
  public long countEntities(@Nonnull OperationContext opContext) {
    validateConnection();
    return server
        .find(EbeanAspectV2.class)
        .setDistinct(true)
        .select(EbeanAspectV2.URN_COLUMN)
        .findCount();
  }

  @Override
  public boolean checkIfAspectExists(
      @Nonnull OperationContext opContext, @Nonnull String aspectName) {
    validateConnection();
    return server
        .find(EbeanAspectV2.class)
        .where()
        .eq(EbeanAspectV2.ASPECT_COLUMN, aspectName)
        .exists();
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
    EbeanAspectV2.PrimaryKey primaryKey =
        new EbeanAspectV2.PrimaryKey(key.getUrn(), key.getAspect(), key.getVersion());
    EbeanAspectV2 ebeanAspect =
        primaryStorageResolver.resolveEbean(opContext, false).find(EbeanAspectV2.class, primaryKey);
    return ebeanAspect == null ? null : ebeanAspect.toEntityAspect();
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
    server
        .createQuery(EbeanAspectV2.class)
        .where()
        .eq(EbeanAspectV2.URN_COLUMN, urn.toString())
        .eq(EbeanAspectV2.ASPECT_COLUMN, aspect)
        .eq(EbeanAspectV2.VERSION_COLUMN, version)
        .delete();
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

    Urn urnObj = UrnUtils.getUrn(urn);
    String keyAspectName = opContext.getKeyAspectName(urnObj);

    // Opt-in Postgres entity-write serialization (advisory lock), taken before any row locks. No-op
    // unless enabled on a Postgres store; when on, it serializes this delete against a concurrent
    // multi-row write (e.g. logical-model linking) on the same entity.
    lockUrnsForWrite(opContext, List.of(urn));

    // On PostgreSQL, acquire this urn's rows up front in canonical (urn, aspect, version) order —
    // the same order the upsert write path uses for its FOR UPDATE reads. The bulk DELETEs below
    // otherwise lock rows in the engine's scan order (physical/CTID order on PostgreSQL), unrelated
    // to key order, so a concurrent multi-row FOR UPDATE write and this hard-delete could acquire
    // overlapping rows in opposite orders and deadlock. The explicit ORDER BY (not the query's
    // natural order) is what makes PostgreSQL place a Sort below its LockRows node so locks are
    // actually taken in key order. On MySQL/InnoDB the bulk DELETEs already lock the primary-key
    // rows in index order, so no up-front lock is needed and this block is skipped. The lock query
    // hydrates only this one urn's rows (bounded by its aspect/version count) purely to take the
    // locks. (canWrite is guaranteed true by the early return above.)
    if (isPostgres && hasActiveTransaction("deleteUrn ordered lock")) {
      // Select only the key columns (urn, aspect, version): FOR UPDATE still locks the matched
      // rows, but this avoids hydrating the metadata/systemMetadata LOBs purely to take the locks.
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
                record -> record.getKey().toAspectIdentifier(), EbeanAspectV2::toEntityAspect));
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
    // Only when we actually take row locks: sort by primary key so all transactions acquire locks
    // in the same (urn, aspect, version) order. Unordered keys under FOR UPDATE cause lock-order
    // deadlocks between concurrent writers ("Deadlock found when trying to get lock"). Non-locking
    // reads take no row locks, so sorting them would be wasted work.
    if (forUpdate && canWrite) {
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

    if (batchGetMethod.equals("IN")) {
      return batchGetIn(opContext, keys, keysCount, position, forUpdate);
    }

    return batchGetUnion(opContext, keys, keysCount, position, forUpdate);
  }

  /**
   * Builds a single SELECT statement for batch get, which selects one entity, and then can be
   * UNION'd with other SELECT statements.
   */
  private String batchGetSelectString(
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
            + "FROM %s WHERE urn = :%s AND aspect = :%s AND version = :%s",
        EbeanAspectV2.class.getAnnotation(Table.class).name(), urnArg, aspectArg, versionArg);
  }

  @Nonnull
  private List<EbeanAspectV2> batchGetUnion(
      @Nonnull OperationContext opContext,
      @Nonnull final List<EbeanAspectV2.PrimaryKey> keys,
      final int keysCount,
      final int position,
      boolean forUpdate) {
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
    if (forUpdate && canWrite) {
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
        primaryStorageResolver
            .resolveEbean(opContext, forUpdate)
            .find(EbeanAspectV2.class)
            .setRawSql(rawSql);

    for (Map.Entry<String, Object> param : params.entrySet()) {
      query.setParameter(param.getKey(), param.getValue());
    }

    return query.findList();
  }

  @Nonnull
  private List<EbeanAspectV2> batchGetIn(
      @Nonnull OperationContext opContext,
      @Nonnull final List<EbeanAspectV2.PrimaryKey> keys,
      final int keysCount,
      final int position,
      boolean forUpdate) {
    validateConnection();

    // Build a single SELECT with IN clause using composite key comparison
    // Query will look like:
    // SELECT * FROM metadata_aspect WHERE (urn, aspect, version) IN
    // (('urn0', 'aspect0', 0), ('urn1', 'aspect1', 1))
    final StringBuilder sb = new StringBuilder();
    sb.append(
        "SELECT urn, aspect, version, metadata, systemMetadata, createdOn, createdBy, createdFor ");
    sb.append("FROM metadata_aspect_v2 WHERE (urn, aspect, version) IN (");

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

    if (forUpdate && canWrite) {
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
        primaryStorageResolver
            .resolveEbean(opContext, forUpdate)
            .find(EbeanAspectV2.class)
            .setRawSql(rawSql);

    for (Map.Entry<String, Object> param : params.entrySet()) {
      query.setParameter(param.getKey(), param.getValue());
    }

    return query.findList();
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
  }

  @Nonnull
  @Override
  public Integer countAspect(
      @Nonnull OperationContext opContext, @Nonnull String aspectName, @Nullable String urnLike) {
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
  }

  @Nonnull
  @Override
  public Integer countAspect(@Nonnull OperationContext opContext, final RestoreIndicesArgs args) {
    return buildExpressionList(args, true).findCount();
  }

  /**
   * Warning this inner Streams must be closed
   *
   * @param args
   * @return
   */
  @Nonnull
  @Override
  public PartitionedStream<EbeanAspectV2> streamAspectBatches(
      @Nonnull OperationContext opContext, final RestoreIndicesArgs args) {
    // Use default for existing RestoreIndices operations
    return streamAspectBatches(opContext, args, null);
  }

  /**
   * Stream aspects ordered by URN/aspect for optimal Elasticsearch document batching. Supports
   * configurable transaction isolation level to optimize for different use cases: - LoadIndices can
   * use READ_UNCOMMITTED for faster scanning
   *
   * @param args Stream arguments and filters
   * @param isolationLevel Optional isolation level override (null = database default)
   * @return PartitionedStream of aspects ordered by URN/aspect
   */
  public PartitionedStream<EbeanAspectV2> streamAspectBatches(
      @Nonnull OperationContext opContext,
      final RestoreIndicesArgs args,
      final TxIsolation isolationLevel) {
    ExpressionList<EbeanAspectV2> exp = buildExpressionList(args, false);
    if (args.limit > 0) {
      exp = exp.setMaxRows(args.limit);
    }

    int start = args.urnBasedPagination ? 0 : args.start;

    // Execute with specific transaction isolation level
    Stream<EbeanAspectV2> stream;
    if (isolationLevel == TxIsolation.READ_UNCOMMITTED) {
      // Use explicit transaction scope for READ_UNCOMMITTED to override default
      try (Transaction transaction =
          server.beginTransaction(TxScope.requiresNew().setIsolation(isolationLevel))) {
        stream =
            exp.orderBy()
                .asc(EbeanAspectV2.URN_COLUMN)
                .orderBy()
                .asc(EbeanAspectV2.ASPECT_COLUMN)
                .setFirstRow(start)
                .findStream(); // Transaction auto-closes when stream completes
      }
    } else {
      // For READ_COMMITTED and other levels, use standard approach
      stream =
          exp.orderBy()
              .asc(EbeanAspectV2.URN_COLUMN)
              .orderBy()
              .asc(EbeanAspectV2.ASPECT_COLUMN)
              .setFirstRow(start)
              .findStream();
    }

    return PartitionedStream.<EbeanAspectV2>builder().delegateStream(stream).build();
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
    if (args.gePitEpochMs > 0) {
      exp =
          exp.ge(
                  EbeanAspectV2.CREATED_ON_COLUMN,
                  Timestamp.from(Instant.ofEpochMilli(args.gePitEpochMs)))
              .le(
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
    return ebeanAspects.getList().stream().map(EbeanAspectV2::getUrn).collect(Collectors.toList());
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
        pagedList.getList().stream().map(EbeanAspectV2::getMetadata).collect(Collectors.toList());
    final ListResultMetadata listResultMetadata =
        toListResultMetadata(
            pagedList.getList().stream()
                .map(EbeanAspectDao::toExtraInfo)
                .collect(Collectors.toList()));
    return toListResult(aspects, listResultMetadata, pagedList, start);
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
      try (Transaction transaction = server.beginTransaction(TxScope.requiresNew())) {
        transaction.setBatchMode(true);
        result = block.apply(transactionContext.tx(transaction));
        if (result.isCommitOrRollback()) {
          transaction.commit();
        } else {
          transaction.rollback();
        }
        break;
      } catch (PersistenceException exception) {
        if (exception instanceof DuplicateKeyException) {
          if (batch != null
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

        boolean backoff = transactionRetryPolicy.shouldBackoff(exception);
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
      if (transactionRetryPolicy.shouldBackoff(last)) {
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

    // Use SQL aggregation to get both min and max in a single query
    SqlQuery query =
        server.sqlQuery(
            "SELECT MIN(version) as min_version, MAX(version) as max_version "
                + "FROM metadata_aspect_v2 "
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
  }

  @Override
  public long getMaxVersion(
      @Nonnull OperationContext opContext,
      @Nonnull final String urn,
      @Nonnull final String aspectName) {
    return getVersionRange(opContext, urn, aspectName).getSecond();
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
    validateConnection();

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

    // forUpdate is required to avoid duplicate key violations (it is used as an indication that the
    // max(version) was invalidated
    if (canWrite && lockLatestForWrite) {
      // Acquire the version-0 row locks in canonical key order so concurrent writers never lock
      // overlapping rows in opposite orders. The Java-side sort covers MySQL/InnoDB (which locks
      // the
      // primary-key IN-list in index order); on PostgreSQL a bare idIn(...).forUpdate() would lock
      // in
      // physical scan order, so the explicit ORDER BY is added to place a Sort below the LockRows
      // node and force key-order lock acquisition.
      Collections.sort(forUpdateKeys);
      // Chunk the pre-sorted keyset (same queryKeysCount batchGet uses) to bound IN-clause size so
      // a
      // large keyset cannot exhaust the DB optimizer (e.g. MySQL range_optimizer_max_mem_size).
      // Each
      // chunk is locked in the same global sorted order, so this stays deadlock-safe -- it only
      // splits one statement into bounded ones. KEY_ID projects only the @EmbeddedId (urn, aspect,
      // version) -- the indexed composite key -- so the metadata/systemMetadata @Lob blobs are
      // never
      // fetched; the lock result is discarded (max(version) below is a separate query).
      final int chunkSize = queryKeysCount > 0 ? queryKeysCount : forUpdateKeys.size();
      for (int position = 0; position < forUpdateKeys.size(); position += chunkSize) {
        final List<EbeanAspectV2.PrimaryKey> chunk =
            forUpdateKeys.subList(position, Math.min(position + chunkSize, forUpdateKeys.size()));
        // PostgreSQL locks the IN-list in physical scan order, so add an explicit ORDER BY to force
        // key-order lock acquisition; MySQL/InnoDB already locks the PK IN-list in index order.
        final Query<EbeanAspectV2> chunkQuery =
            server
                .find(EbeanAspectV2.class)
                .select(EbeanAspectV2.KEY_ID)
                .where()
                .idIn(chunk)
                .query();
        if (isPostgres) {
          chunkQuery.orderBy(EbeanAspectV2.KEY_ORDER_BY_PROPERTY_PATH);
        }
        chunkQuery.forUpdate().findList();
      }
    }

    // Write path must read max(version) from primary to avoid stale replica counts after forUpdate.
    Database versionDatabase = primaryStorageResolver.resolveEbean(opContext, lockLatestForWrite);
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
        exp = queryJunction.and().eq("urn", entry.getKey()).in("aspect", entry.getValue()).endAnd();
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
    return ebeanAspects.stream().map(EbeanAspectV2::toEntityAspect).collect(Collectors.toList());
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
