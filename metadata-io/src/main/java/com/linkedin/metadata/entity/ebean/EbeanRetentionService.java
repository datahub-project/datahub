package com.linkedin.metadata.entity.ebean;

import com.linkedin.common.urn.Urn;
import com.datahub.util.RecordUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.retention.DataHubRetentionConfig;
import com.linkedin.retention.Retention;
import com.linkedin.retention.TimeBasedRetention;
import com.linkedin.retention.VersionBasedRetention;
import io.ebean.EbeanServer;
import io.ebean.Expression;
import io.ebean.ExpressionList;
import io.ebean.PagedList;
import io.ebean.Transaction;
import io.ebeaninternal.server.expression.Op;
import io.ebeaninternal.server.expression.SimpleExpression;
import io.opentelemetry.extension.annotations.WithSpan;
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

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;


@Slf4j
@RequiredArgsConstructor
public class EbeanRetentionService extends RetentionService {
  private final EntityService _entityService;
  private final EbeanServer _server;
  private final int _batchSize;

  private final Clock _clock = Clock.systemUTC();

  @Override
  public EntityService getEntityService() {
    return _entityService;
  }

  @Override
  @WithSpan
  public void applyRetention(@Nonnull Urn urn, @Nonnull String aspectName, Retention retentionPolicy,
      Optional<RetentionContext> retentionContext) {
    log.debug("Applying retention to urn {}, aspectName {}", urn, aspectName);
    // If no policies are set or has indefinite policy set, do not apply any retention
    if (retentionPolicy.data().isEmpty()) {
      return;
    }
    ExpressionList<EbeanAspectV2> deleteQuery = _server.find(EbeanAspectV2.class)
        .where()
        .eq(EbeanAspectV2.URN_COLUMN, urn.toString())
        .eq(EbeanAspectV2.ASPECT_COLUMN, aspectName)
        .ne(EbeanAspectV2.VERSION_COLUMN, ASPECT_LATEST_VERSION)
        .or();

    List<Expression> filterList = new ArrayList<>();
    if (retentionPolicy.hasVersion()) {
      getVersionBasedRetentionQuery(urn, aspectName, retentionPolicy.getVersion(),
          retentionContext.flatMap(RetentionService.RetentionContext::getMaxVersion)).ifPresent(filterList::add);
    }
    if (retentionPolicy.hasTime()) {
      filterList.add(getTimeBasedRetentionQuery(retentionPolicy.getTime()));
    }

    // Only run delete if at least one of the retention policies are applicable
    if (!filterList.isEmpty()) {
      filterList.forEach(deleteQuery::add);
      deleteQuery.endOr().delete();
    }
  }

  private long getMaxVersion(@Nonnull final String urn, @Nonnull final String aspectName) {
    List<EbeanAspectV2> result = _server.find(EbeanAspectV2.class)
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

  private Optional<Expression> getVersionBasedRetentionQuery(@Nonnull Urn urn, @Nonnull String aspectName,
      @Nonnull final VersionBasedRetention retention, final Optional<Long> maxVersionFromUpdate) {
    long largestVersion = maxVersionFromUpdate.orElseGet(() -> getMaxVersion(urn.toString(), aspectName));

    if (largestVersion < retention.getMaxVersions()) {
      return Optional.empty();
    }
    return Optional.of(
        new SimpleExpression(EbeanAspectV2.VERSION_COLUMN, Op.LT, largestVersion - retention.getMaxVersions() + 1));
  }

  private Expression getTimeBasedRetentionQuery(@Nonnull final TimeBasedRetention retention) {
    return new SimpleExpression(EbeanAspectV2.CREATED_ON_COLUMN, Op.LT,
        new Timestamp(_clock.millis() - retention.getMaxAgeInSeconds() * 1000));
  }

  @Override
  @WithSpan
  public void batchApplyRetention(@Nullable String entityName, @Nullable String aspectName) {
    log.debug("Applying retention to all records");
    int numCandidates = queryCandidates(entityName, aspectName).findCount();
    log.info("Found {} urn, aspect pair with more than 1 version", numCandidates);
    Map<String, DataHubRetentionConfig> retentionPolicyMap = getAllRetentionPolicies();

    int start = 0;
    while (start < numCandidates) {
      log.info("Applying retention to pairs {} through {}", start, start + _batchSize);
      PagedList<EbeanAspectV2> rows = getPagedAspects(entityName, aspectName, start, _batchSize);

      try (Transaction transaction = _server.beginTransaction()) {
        transaction.setBatchMode(true);
        transaction.setBatchSize(_batchSize);
        for (EbeanAspectV2 row : rows.getList()) {
          // Only run for cases where there's multiple versions of the aspect
          if (row.getVersion() == 0) {
            continue;
          }
          // 1. Extract an Entity type from the entity Urn
          Urn urn;
          try {
            urn = Urn.createFromString(row.getUrn());
          } catch (Exception e) {
            log.error("Failed to serialize urn {}", row.getUrn(), e);
            continue;
          }
          final String aspectNameFromRecord = row.getAspect();
          // Get the retention policies to apply from the local retention policy map
          Optional<Retention> retentionPolicy = getRetentionKeys(urn.getEntityType(), aspectNameFromRecord).stream()
              .map(key -> retentionPolicyMap.get(key.toString()))
              .filter(Objects::nonNull)
              .findFirst()
              .map(DataHubRetentionConfig::getRetention);
          retentionPolicy.ifPresent(retention -> applyRetention(urn, aspectNameFromRecord, retention,
              Optional.of(new RetentionContext(Optional.of(row.getVersion())))));
        }
        transaction.commit();
      }

      start += _batchSize;
    }

    log.info("Finished applying retention to all records");
  }

  private Map<String, DataHubRetentionConfig> getAllRetentionPolicies() {
    return _server.find(EbeanAspectV2.class)
        .select(String.format("%s, %s, %s", EbeanAspectV2.URN_COLUMN, EbeanAspectV2.ASPECT_COLUMN,
            EbeanAspectV2.METADATA_COLUMN))
        .where()
        .eq(EbeanAspectV2.ASPECT_COLUMN, DATAHUB_RETENTION_ASPECT)
        .eq(EbeanAspectV2.VERSION_COLUMN, ASPECT_LATEST_VERSION)
        .findList()
        .stream()
        .collect(Collectors.toMap(EbeanAspectV2::getUrn,
            row -> RecordUtils.toRecordTemplate(DataHubRetentionConfig.class, row.getMetadata())));
  }

  private ExpressionList<EbeanAspectV2> queryCandidates(@Nullable String entityName, @Nullable String aspectName) {
    ExpressionList<EbeanAspectV2> query = _server.find(EbeanAspectV2.class)
        .setDistinct(true)
        .select(String.format("%s, %s, max(%s)", EbeanAspectV2.URN_COLUMN, EbeanAspectV2.ASPECT_COLUMN,
            EbeanAspectV2.VERSION_COLUMN))
        .where();
    if (entityName != null) {
      query.like(EbeanAspectV2.URN_COLUMN, String.format("urn:li:%s%%", entityName));
    }
    if (aspectName != null) {
      query.eq(EbeanAspectV2.ASPECT_COLUMN, aspectName);
    }
    return query;
  }

  private PagedList<EbeanAspectV2> getPagedAspects(@Nullable String entityName, @Nullable String aspectName,
      final int start, final int pageSize) {
    return queryCandidates(entityName, aspectName).orderBy(
        EbeanAspectV2.URN_COLUMN + ", " + EbeanAspectV2.ASPECT_COLUMN)
        .setFirstRow(start)
        .setMaxRows(pageSize)
        .findPagedList();
  }
}
