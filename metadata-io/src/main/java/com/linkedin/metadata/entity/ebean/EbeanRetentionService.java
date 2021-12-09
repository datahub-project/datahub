package com.linkedin.metadata.entity.ebean;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.retention.DataHubRetentionInfo;
import com.linkedin.retention.Retention;
import com.linkedin.retention.TimeBasedRetention;
import com.linkedin.retention.VersionBasedRetention;
import io.ebean.EbeanServer;
import io.ebean.ExpressionList;
import io.ebean.PagedList;
import io.ebean.Transaction;
import io.opentelemetry.extension.annotations.WithSpan;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.Collections;
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

  public EntityService getEntityService() {
    return _entityService;
  }

  @Override
  @WithSpan
  public void applyRetention(@Nonnull Urn urn, @Nonnull String aspectName, List<Retention> retentionPolicies,
      Optional<RetentionService.RetentionContext> retentionContext) {
    log.debug("Applying retention to urn {}, aspectName {}", urn, aspectName);
    // If no policies are set or has indefinite policy set, do not apply any retention
    if (retentionPolicies.isEmpty() || retentionPolicies.get(0).hasIndefinite()) {
      return;
    }
    ExpressionList<EbeanAspectV2> deleteQuery = _server.find(EbeanAspectV2.class)
        .where()
        .eq(EbeanAspectV2.URN_COLUMN, urn.toString())
        .eq(EbeanAspectV2.ASPECT_COLUMN, aspectName)
        .ne(EbeanAspectV2.VERSION_COLUMN, ASPECT_LATEST_VERSION)
        .or();
    boolean retentionApplied = false;
    for (Retention retention : retentionPolicies) {
      if (retention.hasVersion() && applyVersionBasedRetention(urn, aspectName, deleteQuery, retention.getVersion(),
          retentionContext.flatMap(RetentionService.RetentionContext::getMaxVersion))) {
        retentionApplied = true;
      } else if (retention.hasTime() && applyTimeBasedRetention(deleteQuery, retention.getTime())) {
        retentionApplied = true;
      }
    }

    // Only run delete if at least one of the retention policies are applicable
    if (retentionApplied) {
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

  private boolean applyVersionBasedRetention(@Nonnull Urn urn, @Nonnull String aspectName,
      @Nonnull final ExpressionList<EbeanAspectV2> querySoFar, @Nonnull final VersionBasedRetention retention,
      final Optional<Long> maxVersionFromUpdate) {
    long largestVersion = maxVersionFromUpdate.orElseGet(() -> getMaxVersion(urn.toString(), aspectName));

    if (largestVersion < retention.getMaxVersions()) {
      return false;
    }
    querySoFar.le(EbeanAspectV2.VERSION_COLUMN, largestVersion - retention.getMaxVersions() + 1);
    return true;
  }

  private boolean applyTimeBasedRetention(@Nonnull final ExpressionList<EbeanAspectV2> querySoFar,
      @Nonnull final TimeBasedRetention retention) {
    querySoFar.lt(EbeanAspectV2.CREATED_ON_COLUMN,
        new Timestamp(_clock.millis() - retention.getMaxAgeInSeconds() * 1000));
    return true;
  }

  @Override
  @WithSpan
  public void batchApplyRetention(@Nullable String entityName, @Nullable String aspectName) {
    log.debug("Applying retention to all records");
    int numCandidates = queryCandidates(entityName, aspectName).findCount();
    log.info("Found {} urn, aspect pair with more than 1 version", numCandidates);
    Map<String, DataHubRetentionInfo> retentionPolicyMap = getRetentionPolicies();

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
          List<Retention> retentionPolicies = getRetentionKeys(urn.getEntityType(), aspectNameFromRecord).stream()
              .map(key -> retentionPolicyMap.get(key.toString()))
              .filter(Objects::nonNull)
              .findFirst().<List<Retention>>map(DataHubRetentionInfo::getRetentionPolicies).orElse(
                  Collections.emptyList());
          applyRetention(urn, aspectNameFromRecord, retentionPolicies,
              Optional.of(new RetentionContext(Optional.of(row.getVersion()))));
        }
        transaction.commit();
      }

      start += _batchSize;
    }

    log.info("Finished applying retention to all records");
  }

  private Map<String, DataHubRetentionInfo> getRetentionPolicies() {
    return _server.find(EbeanAspectV2.class)
        .select(String.format("%s, %s, %s", EbeanAspectV2.URN_COLUMN, EbeanAspectV2.ASPECT_COLUMN,
            EbeanAspectV2.METADATA_COLUMN))
        .where()
        .eq(EbeanAspectV2.ASPECT_COLUMN, DATAHUB_RETENTION_ASPECT)
        .eq(EbeanAspectV2.VERSION_COLUMN, ASPECT_LATEST_VERSION)
        .findList()
        .stream()
        .collect(Collectors.toMap(EbeanAspectV2::getUrn,
            row -> RecordUtils.toRecordTemplate(DataHubRetentionInfo.class, row.getMetadata())));
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
