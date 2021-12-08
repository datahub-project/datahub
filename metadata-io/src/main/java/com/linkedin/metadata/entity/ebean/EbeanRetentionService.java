package com.linkedin.metadata.entity.ebean;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.retention.Retention;
import com.linkedin.retention.TimeBasedRetention;
import com.linkedin.retention.VersionBasedRetention;
import io.ebean.EbeanServer;
import io.ebean.ExpressionList;
import io.ebean.PagedList;
import io.ebean.Transaction;
import io.ebean.annotation.TxIsolation;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
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
  public void applyRetention(@Nonnull Urn urn, @Nonnull String aspectName,
      Optional<RetentionService.RetentionContext> retentionContext) {
    log.debug("Applying retention to urn {}, aspectName {}", urn, aspectName);
    List<Retention> retentionPolicies = getRetention(urn.getEntityType(), aspectName);
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
    long largestVersion = maxVersionFromUpdate.orElse(getMaxVersion(urn.toString(), aspectName));

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
  public void applyRetentionToAll() {
    log.debug("Applying retention to all records");
    int numCandidates = queryAllCandidates().findCount();
    log.info("Found {} urn, aspect pair with more than 1 version", numCandidates);

    int start = 0;
    while (start < numCandidates) {
      log.info("Applying retention to pairs {} through {}", start, start + _batchSize);
      PagedList<EbeanAspectV2> rows = getPagedAspects(start, _batchSize);

      try (Transaction transaction = _server.beginTransaction(TxIsolation.REPEATABLE_READ)) {
        transaction.setBatchMode(true);
        for (EbeanAspectV2 row : rows.getList()) {
          // 1. Extract an Entity type from the entity Urn
          Urn urn;
          try {
            urn = Urn.createFromString(row.getUrn());
          } catch (Exception e) {
            log.error("Failed to serialize urn {}", row.getUrn(), e);
            continue;
          }
          final String aspectName = row.getAspect();
          applyRetention(urn, aspectName, Optional.empty());
        }
        transaction.commit();
      }

      start += _batchSize;
    }

    log.info("Finished applying retention to all records");
  }

  private ExpressionList<EbeanAspectV2> queryAllCandidates() {
    return _server.find(EbeanAspectV2.class)
        .setDistinct(true)
        .select(EbeanAspectV2.URN_COLUMN + ", " + EbeanAspectV2.ASPECT_COLUMN)
        .where()
        .gt(EbeanAspectV2.VERSION_COLUMN, 0);
  }

  private PagedList<EbeanAspectV2> getPagedAspects(final int start, final int pageSize) {
    return queryAllCandidates().orderBy()
        .asc(EbeanAspectV2.URN_COLUMN)
        .setFirstRow(start)
        .setMaxRows(pageSize)
        .findPagedList();
  }
}
