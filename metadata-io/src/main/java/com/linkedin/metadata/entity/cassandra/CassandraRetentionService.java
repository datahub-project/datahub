package com.linkedin.metadata.entity.cassandra;

import com.datahub.util.RecordUtils;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.entity.EntityAspect;
import com.linkedin.retention.DataHubRetentionConfig;
import com.linkedin.retention.Retention;
import com.linkedin.retention.TimeBasedRetention;
import com.linkedin.retention.VersionBasedRetention;
import io.opentelemetry.extension.annotations.WithSpan;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;

@Slf4j
@RequiredArgsConstructor
public class CassandraRetentionService extends RetentionService {
  private final EntityService _entityService;
  private final CqlSession _cqlSession;
  private final int _batchSize;

  private final Clock _clock = Clock.systemUTC();

  @Override
  public EntityService getEntityService() {
    return _entityService;
  }

  @Override
  @WithSpan
  public void applyRetention(@Nonnull Urn urn, @Nonnull String aspectName, Retention retentionPolicy, Optional<RetentionContext> retentionContext) {
    log.debug("Applying retention to urn {}, aspectName {}", urn, aspectName);
    // If no policies are set or has indefinite policy set, do not apply any retention
    if (retentionPolicy.data().isEmpty()) {
      return;
    }

    if (retentionPolicy.hasVersion()) {
      applyVersionBasedRetention(urn, aspectName, retentionPolicy.getVersion(), retentionContext.flatMap(RetentionService.RetentionContext::getMaxVersion));
    }

    if (retentionPolicy.hasTime()) {
      applyTimeBasedRetention(urn, aspectName, retentionPolicy.getTime());
    }
  }

  @Override
  @WithSpan
  public void batchApplyRetention(@Nullable String entityName, @Nullable String aspectName) {
    // TODO: This method is not actually batching anything. Cassandra makes it complicated.
    log.debug("Applying retention to all records");
    List<EntityAspectIdentifier> candidates = queryCandidates(entityName, aspectName);
    int numCandidates = candidates.size();
    log.info("Found {} urn, aspect pairs with more than 1 version", numCandidates);
    Map<String, DataHubRetentionConfig> retentionPolicyMap = getAllRetentionPolicies();

    long i = 0;
    for (EntityAspectIdentifier id : candidates) {
      // Only run for cases where there's multiple versions of the aspect
      if (id.getVersion() == 0) {
        continue;
      }
      // 1. Extract an Entity type from the entity Urn
      Urn urn;
      try {
        urn = Urn.createFromString(id.getUrn());
      } catch (Exception e) {
        log.error("Failed to serialize urn {}", id.getUrn(), e);
        continue;
      }
      final String aspectNameFromRecord = id.getAspect();
      // Get the retention policies to apply from the local retention policy map
      Optional<Retention> retentionPolicy = getRetentionKeys(urn.getEntityType(), aspectNameFromRecord).stream()
          .map(key -> retentionPolicyMap.get(key.toString()))
          .filter(Objects::nonNull)
          .findFirst()
          .map(DataHubRetentionConfig::getRetention);
      retentionPolicy.ifPresent(retention ->
          applyRetention(urn, aspectNameFromRecord, retention, Optional.of(new RetentionContext(Optional.of(id.getVersion())))));

      i += 1;
      if (i % _batchSize == 0) {
        log.info(String.format("Retention applied to {} aspect so far.", i));
      }
    }

    log.info("Finished applying retention to all records");
  }

  private void applyVersionBasedRetention(
      @Nonnull final Urn urn,
      @Nonnull final String aspectName,
      @Nonnull final VersionBasedRetention retention,
      @Nonnull Optional<Long> maxVersionFromUpdate) {

    long largestVersion = maxVersionFromUpdate.orElseGet(() -> getMaxVersion(urn, aspectName));

    SimpleStatement ss = deleteFrom(CassandraAspect.TABLE_NAME)
        .whereColumn(CassandraAspect.URN_COLUMN).isEqualTo(literal(urn.toString()))
        .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
        .whereColumn(CassandraAspect.VERSION_COLUMN).isGreaterThan(literal(ASPECT_LATEST_VERSION))
        .whereColumn(CassandraAspect.VERSION_COLUMN).isLessThanOrEqualTo(literal(largestVersion - retention.getMaxVersions() + 1L))
        .build();

    _cqlSession.execute(ss);
  }

  private long getMaxVersion(@Nonnull final Urn urn, @Nonnull final String aspectName) {
    SimpleStatement ss = selectFrom(CassandraAspect.TABLE_NAME)
        .function("max", Selector.column(CassandraAspect.VERSION_COLUMN))
        .whereColumn(CassandraAspect.URN_COLUMN).isEqualTo(literal(urn.toString()))
        .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
        .orderBy(CassandraAspect.VERSION_COLUMN, ClusteringOrder.DESC)
        .build();
    ResultSet rs = _cqlSession.execute(ss);
    Row row = rs.one();
    return row.getLong(CassandraAspect.VERSION_COLUMN);
  }

  private void applyTimeBasedRetention(
      @Nonnull final Urn urn,
      @Nonnull final String aspectName,
      @Nonnull final TimeBasedRetention retention) {
    Timestamp threshold = new Timestamp(_clock.millis() - retention.getMaxAgeInSeconds() * 1000);
    SimpleStatement ss = deleteFrom(CassandraAspect.TABLE_NAME)
        .whereColumn(CassandraAspect.URN_COLUMN).isEqualTo(literal(urn.toString()))
        .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
        .whereColumn(CassandraAspect.CREATED_ON_COLUMN).isLessThanOrEqualTo(literal(threshold))
        .build();

    _cqlSession.execute(ss);
  }

  private List<EntityAspectIdentifier> queryCandidates(@Nullable String entityName, @Nullable String aspectName) {
    Select select = selectFrom(CassandraAspect.TABLE_NAME)
        .selectors(
            Selector.column(CassandraAspect.URN_COLUMN),
            Selector.column(CassandraAspect.ASPECT_COLUMN),
            Selector.function("max", Selector.column(CassandraAspect.VERSION_COLUMN)).as(CassandraAspect.VERSION_COLUMN))
        .allowFiltering();
    if (aspectName != null) {
      select = select.whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName));
    }
    select = select.whereColumn(CassandraAspect.VERSION_COLUMN).isGreaterThan(literal(ASPECT_LATEST_VERSION));
    if (entityName != null) {
      select = select.whereColumn(CassandraAspect.ENTITY_COLUMN).isEqualTo(literal(entityName));
    }
    select = select.groupBy(ImmutableList.of(Selector.column(CassandraAspect.URN_COLUMN), Selector.column(CassandraAspect.ASPECT_COLUMN)));
    SimpleStatement ss = select.build();
    ResultSet rs = _cqlSession.execute(ss);
    return rs.all().stream().map(CassandraAspect::rowToAspectIdentifier).collect(Collectors.toList());
  }

  private Map<String, DataHubRetentionConfig> getAllRetentionPolicies() {
    SimpleStatement ss = selectFrom(CassandraAspect.TABLE_NAME)
        .all()
        .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(DATAHUB_RETENTION_ASPECT))
        .whereColumn(CassandraAspect.VERSION_COLUMN).isEqualTo(literal(ASPECT_LATEST_VERSION))
        .allowFiltering()
        .build();
    ResultSet rs = _cqlSession.execute(ss);
    return rs.all().stream()
        .map(CassandraAspect::rowToEntityAspect)
        .collect(Collectors.toMap(
            EntityAspect::getUrn,
            aspect -> RecordUtils.toRecordTemplate(DataHubRetentionConfig.class, aspect.getMetadata())));
  }
}
