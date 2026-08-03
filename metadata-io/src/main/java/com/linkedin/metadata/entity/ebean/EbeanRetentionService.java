package com.linkedin.metadata.entity.ebean;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionArgs;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionResult;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.retention.DataHubRetentionConfig;
import com.linkedin.retention.Retention;
import com.linkedin.retention.TimeBasedRetention;
import com.linkedin.retention.VersionBasedRetention;
import io.datahubproject.metadata.context.OperationContext;
import io.ebean.Database;
import io.ebean.Expression;
import io.ebean.ExpressionList;
import io.ebean.SqlQuery;
import io.ebean.SqlRow;
import io.ebean.Transaction;
import io.ebean.TxScope;
import io.ebeaninternal.server.expression.Op;
import io.ebeaninternal.server.expression.SimpleExpression;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class EbeanRetentionService<U extends ChangeMCP> extends RetentionService<U> {
  private static final String EMPTY_KEYSET = "";

  private final EntityService<U> _entityService;
  private final Database _server;
  private final int _batchSize;

  private final Clock _clock = Clock.systemUTC();

  @Override
  public EntityService<U> getEntityService() {
    return _entityService;
  }

  @Override
  protected AspectsBatch buildAspectsBatch(
      @Nonnull OperationContext opContext,
      List<MetadataChangeProposal> mcps,
      @Nonnull AuditStamp auditStamp) {
    return AspectsBatchImpl.builder()
        .mcps(mcps, auditStamp, opContext.getRetrieverContext())
        .build(opContext);
  }

  @Override
  @WithSpan
  protected void applyRetention(List<RetentionContext> retentionContexts) {

    List<RetentionContext> nonEmptyContexts =
        retentionContexts.stream()
            .filter(
                context -> {
                  if (!context.getRetentionPolicy().isPresent()) {
                    return false;
                  }
                  Retention policy = context.getRetentionPolicy().get();

                  // Skip if policy is empty (no version or time retention configured)
                  return !policy.data().isEmpty();
                })
            .collect(Collectors.toList());

    // Separate DELETEs per retention context (better index use than one DELETE with many ORs).
    // Tx scope depends on caller: legacy in-tx path shares the upsert transaction; when invoked
    // from the post-commit path (postCommitRetentionEnabled), deletes run outside that upsert tx.
    if (!nonEmptyContexts.isEmpty()) {
      int deletedCount = 0;
      for (RetentionContext context : nonEmptyContexts) {
        int rowsDeleted = executeRetentionDeleteForContext(context);
        deletedCount += rowsDeleted;
        if (rowsDeleted > 0) {
          log.debug(
              "Deleted {} rows for urn={} aspect={}",
              rowsDeleted,
              context.getUrn(),
              context.getAspectName());
        }
      }

      if (deletedCount > 0) {
        log.debug(
            "Retention applied: deleted {} total rows across {} (urn, aspect) pairs",
            deletedCount,
            nonEmptyContexts.size());
      }
    }
  }

  /**
   * Build and execute the single-pair DELETE for one {@link RetentionContext}. Returns rows deleted
   * (0 when no version/time condition applies — a no-op, not a failure). Caller owns the
   * transaction; this method issues one {@code DELETE} against the ambient/current tx.
   */
  private int executeRetentionDeleteForContext(@Nonnull RetentionContext context) {
    Retention retentionPolicy = context.getRetentionPolicy().orElseThrow();

    ExpressionList<EbeanAspectV2> deleteQuery =
        _server
            .find(EbeanAspectV2.class)
            .where()
            .eq(EbeanAspectV2.URN_COLUMN, context.getUrn().toString())
            .eq(EbeanAspectV2.ASPECT_COLUMN, context.getAspectName())
            .ne(EbeanAspectV2.VERSION_COLUMN, Constants.ASPECT_LATEST_VERSION);

    boolean hasVersionCondition = false;
    if (retentionPolicy.hasVersion()) {
      Optional<Expression> versionExpr =
          getVersionBasedRetentionQuery(
              context.getUrn(),
              context.getAspectName(),
              retentionPolicy.getVersion(),
              context.getMaxVersion());
      if (versionExpr.isPresent()) {
        deleteQuery.add(versionExpr.get());
        hasVersionCondition = true;
      }
    }

    boolean hasTimeCondition = false;
    if (retentionPolicy.hasTime()) {
      deleteQuery.add(getTimeBasedRetentionQuery(retentionPolicy.getTime()));
      hasTimeCondition = true;
    }

    if (hasVersionCondition || hasTimeCondition) {
      return deleteQuery.delete();
    }
    return 0;
  }

  /**
   * Batch variant of {@link #applyRetentionWithPolicyDefaults} that wraps all per-pair DELETEs in a
   * single database transaction (one commit / fsync per drain tick). Per-pair JDBC savepoints
   * isolate failures: a poison (urn, aspect) pair rolls back only its own DELETE, so one bad pair
   * no longer wedges the whole batch. Returns the contexts that were durably committed — the caller
   * ({@link com.linkedin.metadata.entity.retention.buffer.RetentionDrainer}) should clear only
   * those keys from the buffer via {@code removeIfSame}.
   *
   * <p>Empty-policy contexts are returned as successes (no-op DELETEs) so their buffer keys are
   * cleared rather than retried forever.
   *
   * <p>If the final commit fails, returns an empty list — nothing is durable and all keys stay for
   * retry.
   */
  @Override
  @WithSpan
  @Nonnull
  public List<RetentionContext> applyRetentionBatchWithPolicyDefaults(
      @Nonnull OperationContext opContext, @Nonnull List<RetentionContext> retentionContexts) {
    List<RetentionContext> withDefaults =
        retentionContexts.stream()
            .map(
                context -> {
                  if (context.getRetentionPolicy().isEmpty()) {
                    Retention retentionPolicy =
                        getRetention(
                            opContext, context.getUrn().getEntityType(), context.getAspectName());
                    return context.toBuilder()
                        .retentionPolicy(Optional.of(retentionPolicy))
                        .build();
                  }
                  return context;
                })
            .collect(Collectors.toList());

    List<RetentionContext> successes = new ArrayList<>(withDefaults.size());
    if (withDefaults.isEmpty()) {
      return successes;
    }

    Transaction tx = _server.beginTransaction(TxScope.required());
    tx.setBatchMode(true);
    tx.setBatchSize(_batchSize);
    try {
      Connection connection = tx.connection();
      for (RetentionContext context : withDefaults) {
        Savepoint savepoint = connection.setSavepoint();
        try {
          int rowsDeleted = executeRetentionDeleteForContext(context);
          connection.releaseSavepoint(savepoint);
          successes.add(context);
          if (rowsDeleted > 0) {
            log.debug(
                "Deleted {} rows for urn={} aspect={}",
                rowsDeleted,
                context.getUrn(),
                context.getAspectName());
          }
        } catch (Exception e) {
          try {
            connection.rollback(savepoint);
          } catch (SQLException sqle) {
            log.warn(
                "Failed to rollback to savepoint for urn={} aspect={}",
                context.getUrn(),
                context.getAspectName(),
                sqle);
          }
          log.warn(
              "Retention delete failed for urn={} aspect={}; leaving key for retry",
              context.getUrn(),
              context.getAspectName(),
              e);
        }
      }
      tx.commit();
      return successes;
    } catch (Exception e) {
      log.warn(
          "Retention batch tx failed; {} of {} pairs applied before failure",
          successes.size(),
          withDefaults.size(),
          e);
      try {
        tx.rollback();
      } catch (Exception ignored) {
        // Already logging the primary failure above.
      }
      return List.of();
    } finally {
      tx.end();
    }
  }

  private long getMaxVersion(@Nonnull final String urn, @Nonnull final String aspectName) {
    List<EbeanAspectV2> result =
        _server
            .find(EbeanAspectV2.class)
            .where()
            .eq("urn", urn)
            .eq("aspect", aspectName)
            .orderBy()
            .desc("version")
            .findList();
    if (result.size() == 0) {
      return -1;
    }
    return result.get(0).getKey().getVersion();
  }

  private Optional<Expression> getVersionBasedRetentionQuery(
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      @Nonnull final VersionBasedRetention retention,
      @Nonnull final Optional<Long> maxVersionFromUpdate) {
    long largestVersion =
        maxVersionFromUpdate.orElseGet(() -> getMaxVersion(urn.toString(), aspectName));

    if (largestVersion < retention.getMaxVersions()) {
      return Optional.empty();
    }
    return Optional.of(
        new SimpleExpression(
            EbeanAspectV2.VERSION_COLUMN, Op.LT, largestVersion - retention.getMaxVersions() + 1));
  }

  private Expression getTimeBasedRetentionQuery(@Nonnull final TimeBasedRetention retention) {
    return new SimpleExpression(
        EbeanAspectV2.CREATED_ON_COLUMN,
        Op.LT,
        new Timestamp(_clock.millis() - retention.getMaxAgeInSeconds() * 1000L));
  }

  private void applyRetention(
      List<EbeanAspectV2> rows,
      Map<String, DataHubRetentionConfig> retentionPolicyMap,
      BulkApplyRetentionResult applyRetentionResult) {
    try (Transaction transaction = _server.beginTransaction(TxScope.required())) {
      transaction.setBatchMode(true);
      transaction.setBatchSize(_batchSize);

      List<RetentionContext> retentionContexts =
          rows.stream()
              .filter(row -> row.getVersion() != 0)
              .map(
                  row -> {
                    Urn urn;
                    try {
                      urn = Urn.createFromString(row.getUrn());
                    } catch (Exception e) {
                      log.error("Failed to serialize urn {}", row.getUrn(), e);
                      return null;
                    }

                    final String aspectNameFromRecord = row.getAspect();
                    log.debug("Handling urn {} aspect {}", row.getUrn(), row.getAspect());
                    Optional<Retention> retentionPolicy =
                        getRetentionKeys(urn.getEntityType(), aspectNameFromRecord).stream()
                            .map(key -> retentionPolicyMap.get(key.toString()))
                            .filter(Objects::nonNull)
                            .findFirst()
                            .map(DataHubRetentionConfig::getRetention);

                    return RetentionService.RetentionContext.builder()
                        .urn(urn)
                        .aspectName(aspectNameFromRecord)
                        .retentionPolicy(retentionPolicy)
                        .maxVersion(Optional.of(row.getVersion()))
                        .build();
                  })
              .filter(Objects::nonNull)
              .collect(Collectors.toList());

      applyRetention(retentionContexts);
      if (applyRetentionResult != null) {
        applyRetentionResult.rowsHandled += retentionContexts.size();
      }

      transaction.commit();
    }
  }

  @Override
  @WithSpan
  public void batchApplyRetention(@Nullable String entityName, @Nullable String aspectName) {
    log.debug("Applying retention to all records");
    Map<String, DataHubRetentionConfig> retentionPolicyMap = getAllRetentionPolicies();

    String lastUrn = EMPTY_KEYSET;
    String lastAspect = EMPTY_KEYSET;
    long pairsHandled = 0;
    List<EbeanAspectV2> rows;
    do {
      rows =
          getPagedAspectsByKeyset(
              null, entityName, aspectName, lastUrn, lastAspect, _batchSize, null);
      if (!rows.isEmpty()) {
        log.info(
            "Applying retention to {} (urn, aspect) pairs with version > 0 after key ({}, {})",
            rows.size(),
            lastUrn.isEmpty() ? "<start>" : lastUrn,
            lastAspect.isEmpty() ? "<start>" : lastAspect);
        applyRetention(rows, retentionPolicyMap, null);
        pairsHandled += rows.size();
        EbeanAspectV2 last = rows.get(rows.size() - 1);
        lastUrn = last.getUrn();
        lastAspect = last.getAspect();
      }
    } while (rows.size() == _batchSize);

    log.info(
        "Finished applying retention to {} (urn, aspect) pairs with version > 0", pairsHandled);
  }

  @Override
  public BulkApplyRetentionResult batchApplyRetentionEntities(
      @Nonnull BulkApplyRetentionArgs args) {
    long startTime = System.currentTimeMillis();

    BulkApplyRetentionResult result = new BulkApplyRetentionResult();
    result.argStart = args.start;
    result.argCount = args.count;
    result.argAttemptWithVersion = args.attemptWithVersion;
    result.argAspectName = args.aspectName;
    result.argUrn = args.urn;

    Map<String, DataHubRetentionConfig> retentionPolicyMap = getAllRetentionPolicies();
    result.timeRetentionPolicyMapMs = System.currentTimeMillis() - startTime;
    startTime = System.currentTimeMillis();

    // only supports version based retention for batch apply
    // find urn, aspect pairs where version count > attemptWithVersion
    int start = args.start != null ? args.start : 0;
    int count = args.count != null ? args.count : 100;
    String lastUrn = EMPTY_KEYSET;
    String lastAspect = EMPTY_KEYSET;

    // Honor legacy start without SQL OFFSET: advance keyset until start rows are skipped.
    int skipped = 0;
    while (skipped < start) {
      int pageSize = Math.min(_batchSize, start - skipped);
      List<EbeanAspectV2> skipPage =
          getPagedAspectsByKeyset(
              args.urn,
              null,
              args.aspectName,
              lastUrn,
              lastAspect,
              pageSize,
              args.attemptWithVersion);
      if (skipPage.isEmpty()) {
        result.timeRowMs = System.currentTimeMillis() - startTime;
        return result;
      }
      skipped += skipPage.size();
      EbeanAspectV2 last = skipPage.get(skipPage.size() - 1);
      lastUrn = last.getUrn();
      lastAspect = last.getAspect();
    }

    List<EbeanAspectV2> rows =
        getPagedAspectsByKeyset(
            args.urn, null, args.aspectName, lastUrn, lastAspect, count, args.attemptWithVersion);
    result.timeRowMs = System.currentTimeMillis() - startTime;

    for (EbeanAspectV2 row : rows) {
      long applyStart = System.currentTimeMillis();
      log.debug("For {},{} max version is {}", row.getUrn(), row.getAspect(), row.getVersion());
      try {
        Urn.createFromString(row.getUrn());
      } catch (Exception e) {
        log.error("Failed to serialize urn {}", row.getUrn(), e);
        continue;
      }
      applyRetention(List.of(row), retentionPolicyMap, result);
      result.timeApplyRetentionMs += System.currentTimeMillis() - applyStart;
    }

    return result;
  }

  private Map<String, DataHubRetentionConfig> getAllRetentionPolicies() {
    return _server
        .find(EbeanAspectV2.class)
        .select(
            String.format(
                "%s, %s, %s",
                EbeanAspectV2.URN_COLUMN,
                EbeanAspectV2.ASPECT_COLUMN,
                EbeanAspectV2.METADATA_COLUMN))
        .where()
        .eq(EbeanAspectV2.ASPECT_COLUMN, Constants.DATAHUB_RETENTION_ASPECT)
        .eq(EbeanAspectV2.VERSION_COLUMN, Constants.ASPECT_LATEST_VERSION)
        .findList()
        .stream()
        .collect(
            Collectors.toMap(
                EbeanAspectV2::getUrn,
                row ->
                    RecordUtils.toRecordTemplate(DataHubRetentionConfig.class, row.getMetadata())));
  }

  /**
   * Keyset-paginated candidate (urn, aspect) pairs.
   *
   * <p>Avoids {@code OFFSET} over {@code GROUP BY}, which becomes pathological on large
   * metadata_aspect_v2 tables. When {@code minVersionCount} is null, candidates are pairs with
   * {@code version > 0}. When set, candidates must also satisfy {@code COUNT(version) >
   * minVersionCount}.
   */
  @Nonnull
  List<EbeanAspectV2> getPagedAspectsByKeyset(
      @Nullable String urn,
      @Nullable String entityName,
      @Nullable String aspectName,
      @Nonnull String lastUrn,
      @Nonnull String lastAspect,
      final int pageSize,
      @Nullable Integer minVersionCount) {
    if (pageSize <= 0) {
      return List.of();
    }

    StringBuilder sql =
        new StringBuilder(
            "SELECT urn, aspect, MAX(version) AS version FROM metadata_aspect_v2 WHERE 1=1");
    if (minVersionCount == null) {
      sql.append(" AND version > 0");
    }
    if (urn != null) {
      sql.append(" AND urn = :urn");
    }
    if (entityName != null) {
      sql.append(" AND urn LIKE :entityLike");
    }
    if (aspectName != null) {
      sql.append(" AND aspect = :aspectName");
    }
    sql.append(" AND (urn > :lastUrn OR (urn = :lastUrn AND aspect > :lastAspect))");
    sql.append(" GROUP BY urn, aspect");
    if (minVersionCount != null) {
      sql.append(" HAVING COUNT(version) > :minVersionCount");
    }
    sql.append(" ORDER BY urn, aspect LIMIT :pageSize");

    SqlQuery query =
        _server
            .sqlQuery(sql.toString())
            .setParameter("lastUrn", lastUrn)
            .setParameter("lastAspect", lastAspect)
            .setParameter("pageSize", pageSize);
    if (urn != null) {
      query.setParameter("urn", urn);
    }
    if (entityName != null) {
      query.setParameter("entityLike", String.format("urn:li:%s%%", entityName));
    }
    if (aspectName != null) {
      query.setParameter("aspectName", aspectName);
    }
    if (minVersionCount != null) {
      query.setParameter("minVersionCount", minVersionCount);
    }

    List<SqlRow> sqlRows = query.findList();
    List<EbeanAspectV2> results = new ArrayList<>(sqlRows.size());
    for (SqlRow sqlRow : sqlRows) {
      String rowUrn = sqlRow.getString("urn");
      String rowAspect = sqlRow.getString("aspect");
      long version = sqlRow.getLong("version");
      EbeanAspectV2 row = new EbeanAspectV2();
      row.setKey(new EbeanAspectV2.PrimaryKey(rowUrn, rowAspect, version));
      row.setUrn(rowUrn);
      row.setAspect(rowAspect);
      row.setVersion(version);
      results.add(row);
    }
    return results;
  }
}
