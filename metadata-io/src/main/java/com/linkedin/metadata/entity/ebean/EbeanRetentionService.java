package com.linkedin.metadata.entity.ebean;

import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.entity.ebean.transactions.AspectsBatchImpl;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionArgs;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionResult;
import com.linkedin.metadata.entity.transactions.AspectsBatch;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.retention.DataHubRetentionConfig;
import com.linkedin.retention.Retention;
import com.linkedin.retention.TimeBasedRetention;
import com.linkedin.retention.VersionBasedRetention;
import io.ebean.Database;
import io.ebean.Expression;
import io.ebean.ExpressionList;
import io.ebean.PagedList;
import io.ebean.Query;
import io.ebean.Transaction;
import io.ebean.TxScope;
import io.ebeaninternal.server.expression.Op;
import io.ebeaninternal.server.expression.SimpleExpression;
import io.opentelemetry.extension.annotations.WithSpan;
import java.sql.Timestamp;
import java.time.Clock;
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
public class EbeanRetentionService extends RetentionService {
  private final EntityService _entityService;
  private final Database _server;
  private final int _batchSize;

  private final Clock _clock = Clock.systemUTC();

  @Override
  public EntityService getEntityService() {
    return _entityService;
  }

  @Override
  protected AspectsBatch buildAspectsBatch(List<MetadataChangeProposal> mcps) {
    return AspectsBatchImpl.builder().mcps(mcps, _entityService.getEntityRegistry()).build();
  }

  @Override
  @WithSpan
  protected void applyRetention(List<RetentionContext> retentionContexts) {

    List<RetentionContext> nonEmptyContexts =
        retentionContexts.stream()
            .filter(
                context ->
                    context.getRetentionPolicy().isPresent()
                        && !context.getRetentionPolicy().get().data().isEmpty())
            .collect(Collectors.toList());

    // Only run delete if at least one of the retention policies are applicable
    if (!nonEmptyContexts.isEmpty()) {
      ExpressionList<EbeanAspectV2> deleteQuery =
          _server
              .find(EbeanAspectV2.class)
              .where()
              .ne(EbeanAspectV2.VERSION_COLUMN, Constants.ASPECT_LATEST_VERSION)
              .or();

      boolean applied = false;
      for (RetentionContext context : nonEmptyContexts) {
        Retention retentionPolicy = context.getRetentionPolicy().get();

        if (retentionPolicy.hasVersion()) {
          boolean appliedVersion =
              getVersionBasedRetentionQuery(
                      context.getUrn(),
                      context.getAspectName(),
                      retentionPolicy.getVersion(),
                      context.getMaxVersion())
                  .map(
                      expr ->
                          deleteQuery
                              .and()
                              .eq(EbeanAspectV2.URN_COLUMN, context.getUrn().toString())
                              .eq(EbeanAspectV2.ASPECT_COLUMN, context.getAspectName())
                              .add(expr)
                              .endAnd())
                  .isPresent();

          applied = appliedVersion || applied;
        }

        if (retentionPolicy.hasTime()) {
          deleteQuery
              .and()
              .eq(EbeanAspectV2.URN_COLUMN, context.getUrn().toString())
              .eq(EbeanAspectV2.ASPECT_COLUMN, context.getAspectName())
              .add(getTimeBasedRetentionQuery(retentionPolicy.getTime()))
              .endAnd();
          applied = true;
        }
      }

      if (applied) {
        deleteQuery.endOr().delete();
      }
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
        new Timestamp(_clock.millis() - retention.getMaxAgeInSeconds() * 1000));
  }

  private void applyRetention(
      PagedList<EbeanAspectV2> rows,
      Map<String, DataHubRetentionConfig> retentionPolicyMap,
      BulkApplyRetentionResult applyRetentionResult) {
    try (Transaction transaction = _server.beginTransaction(TxScope.required())) {
      transaction.setBatchMode(true);
      transaction.setBatchSize(_batchSize);

      List<RetentionContext> retentionContexts =
          rows.getList().stream()
              .filter(row -> row.getVersion() != 0)
              .map(
                  row -> {
                    // 1. Extract an Entity type from the entity Urn
                    Urn urn;
                    try {
                      urn = Urn.createFromString(row.getUrn());
                    } catch (Exception e) {
                      log.error("Failed to serialize urn {}", row.getUrn(), e);
                      return null;
                    }

                    final String aspectNameFromRecord = row.getAspect();
                    log.debug("Handling urn {} aspect {}", row.getUrn(), row.getAspect());
                    // Get the retention policies to apply from the local retention policy map
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
    int numCandidates = queryCandidates(null, entityName, aspectName).findCount();
    log.info("Found {} urn, aspect pair with more than 1 version", numCandidates);
    Map<String, DataHubRetentionConfig> retentionPolicyMap = getAllRetentionPolicies();

    int start = 0;
    while (start < numCandidates) {
      log.info("Applying retention to pairs {} through {}", start, start + _batchSize);
      PagedList<EbeanAspectV2> rows = getPagedAspects(entityName, aspectName, start, _batchSize);
      applyRetention(rows, retentionPolicyMap, null);
      start += _batchSize;
    }

    log.info("Finished applying retention to all records");
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
    // find urn, aspect pair where distinct versions > 20 to apply retention policy
    Query<EbeanAspectV2> query =
        _server
            .find(EbeanAspectV2.class)
            .setDistinct(true)
            .select(
                String.format(
                    "%s, %s, count(%s)",
                    EbeanAspectV2.URN_COLUMN,
                    EbeanAspectV2.ASPECT_COLUMN,
                    EbeanAspectV2.VERSION_COLUMN));
    ExpressionList<EbeanAspectV2> exp = null;
    if (args.urn != null || args.aspectName != null) {
      exp = query.where();
      if (args.aspectName != null) {
        exp = exp.eq(EbeanAspectV2.ASPECT_COLUMN, args.aspectName);
      }
      if (args.urn != null) {
        exp = exp.eq(EbeanAspectV2.URN_COLUMN, args.urn);
      }
    }
    if (exp == null) {
      exp = query.having();
    } else {
      exp = exp.having();
    }

    PagedList<EbeanAspectV2> rows =
        exp.gt(String.format("count(%s)", EbeanAspectV2.VERSION_COLUMN), args.attemptWithVersion)
            .setFirstRow(args.start)
            .setMaxRows(args.count)
            .findPagedList();
    result.timeRowMs = System.currentTimeMillis() - startTime;

    for (EbeanAspectV2 row : rows.getList()) {
      startTime = System.currentTimeMillis();
      log.debug("For {},{} version count is {}", row.getUrn(), row.getAspect(), row.getVersion());
      try {
        Urn.createFromString(row.getUrn());
      } catch (Exception e) {
        log.error("Failed to serialize urn {}", row.getUrn(), e);
        continue;
      }
      PagedList<EbeanAspectV2> rowsToChange =
          queryCandidates(row.getUrn(), null, row.getAspect())
              .setFirstRow(args.start)
              .setMaxRows(args.count)
              .findPagedList();

      applyRetention(rowsToChange, retentionPolicyMap, result);
      result.timeApplyRetentionMs += System.currentTimeMillis() - startTime;
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

  private ExpressionList<EbeanAspectV2> queryCandidates(
      @Nullable String urn, @Nullable String entityName, @Nullable String aspectName) {
    ExpressionList<EbeanAspectV2> query =
        _server
            .find(EbeanAspectV2.class)
            .setDistinct(true)
            .select(
                String.format(
                    "%s, %s, max(%s)",
                    EbeanAspectV2.URN_COLUMN,
                    EbeanAspectV2.ASPECT_COLUMN,
                    EbeanAspectV2.VERSION_COLUMN))
            .where();
    if (urn != null) {
      query.eq(EbeanAspectV2.URN_COLUMN, urn);
    }
    if (entityName != null) {
      query.like(EbeanAspectV2.URN_COLUMN, String.format("urn:li:%s%%", entityName));
    }
    if (aspectName != null) {
      query.eq(EbeanAspectV2.ASPECT_COLUMN, aspectName);
    }
    return query;
  }

  private PagedList<EbeanAspectV2> getPagedAspects(
      @Nullable String entityName,
      @Nullable String aspectName,
      final int start,
      final int pageSize) {
    return queryCandidates(null, entityName, aspectName)
        .orderBy(EbeanAspectV2.URN_COLUMN + ", " + EbeanAspectV2.ASPECT_COLUMN)
        .setFirstRow(start)
        .setMaxRows(pageSize)
        .findPagedList();
  }
}
