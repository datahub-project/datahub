package com.linkedin.metadata.entity.cassandra;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
import static com.linkedin.metadata.Constants.*;

import com.datahub.util.exception.ModelConversionException;
import com.datahub.util.exception.RetryLimitReached;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.paging.OffsetPager;
import com.datastax.oss.driver.api.core.paging.OffsetPager.Page;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.AspectMigrationsDao;
import com.linkedin.metadata.entity.EntityAspect;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.entity.TransactionContext;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.ExtraInfoArray;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.util.Pair;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CassandraAspectDao implements AspectDao, AspectMigrationsDao {

  private final CqlSession _cqlSession;
  private boolean _canWrite = true;
  private boolean _connectionValidated = false;

  public CassandraAspectDao(@Nonnull final CqlSession cqlSession) {
    _cqlSession = cqlSession;
  }

  public void setConnectionValidated(boolean validated) {
    _connectionValidated = validated;
    _canWrite = validated;
  }

  private boolean validateConnection() {
    if (_connectionValidated) {
      return true;
    }
    if (!AspectStorageValidationUtil.checkTableExists(_cqlSession)) {
      log.error("GMS can't find entity aspects table in Cassandra storage layer.");
      _canWrite = false;
      return false;
    }
    _connectionValidated = true;
    return true;
  }

  @Override
  public EntityAspect getLatestAspect(
      @Nonnull String urn, @Nonnull String aspectName, boolean forUpdate) {
    validateConnection();
    return getAspect(urn, aspectName, ASPECT_LATEST_VERSION);
  }

  @Override
  public Map<String, Map<String, EntityAspect>> getLatestAspects(
      Map<String, Set<String>> urnAspects, boolean forUpdate) {
    return urnAspects.entrySet().stream()
        .map(
            entry ->
                Map.entry(
                    entry.getKey(),
                    entry.getValue().stream()
                        .map(
                            aspectName -> {
                              EntityAspect aspect =
                                  getLatestAspect(entry.getKey(), aspectName, forUpdate);
                              return aspect != null ? Map.entry(aspectName, aspect) : null;
                            })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public long getMaxVersion(@Nonnull final String urn, @Nonnull final String aspectName) {
    validateConnection();
    Map<String, Pair<Long, Long>> result = getVersionRanges(urn, ImmutableSet.of(aspectName));
    return result.get(aspectName).getSecond();
  }

  @Override
  @Nonnull
  public Pair<Long, Long> getVersionRange(@Nonnull String urn, @Nonnull String aspectName) {
    Map<String, Pair<Long, Long>> result = getVersionRanges(urn, ImmutableSet.of(aspectName));
    return result.get(aspectName);
  }

  @Override
  public long countEntities() {
    validateConnection();
    SimpleStatement ss =
        selectFrom(CassandraAspect.TABLE_NAME)
            .distinct()
            .column(CassandraAspect.URN_COLUMN)
            .build();

    ResultSet rs = _cqlSession.execute(ss);
    // TODO: make sure it doesn't blow up on a large database
    //  Getting a count of distinct values in a Cassandra query doesn't seem to be feasible, but
    // counting them in the app is dangerous
    //  The saving grace here is that the only place where this method is used should only run once,
    // what the database is still young
    return rs.all().size();
  }

  @Override
  public boolean checkIfAspectExists(@Nonnull String aspectName) {
    validateConnection();
    SimpleStatement ss =
        selectFrom(CassandraAspect.TABLE_NAME)
            .column(CassandraAspect.URN_COLUMN)
            .whereColumn(CassandraAspect.ASPECT_COLUMN)
            .isEqualTo(literal(aspectName))
            .limit(1)
            .allowFiltering()
            .build();

    ResultSet rs = _cqlSession.execute(ss);
    return rs.one() != null;
  }

  private Map<String, Pair<Long, Long>> getVersionRanges(
      @Nonnull final String urn, @Nonnull final Set<String> aspectNames) {
    SimpleStatement ss =
        selectFrom(CassandraAspect.TABLE_NAME)
            .selectors(
                Selector.column(CassandraAspect.URN_COLUMN),
                Selector.column(CassandraAspect.ASPECT_COLUMN),
                Selector.function("min", Selector.column(CassandraAspect.VERSION_COLUMN))
                    .as("min_version"),
                Selector.function("max", Selector.column(CassandraAspect.VERSION_COLUMN))
                    .as("max_version"))
            .whereColumn(CassandraAspect.URN_COLUMN)
            .isEqualTo(literal(urn))
            .whereColumn(CassandraAspect.ASPECT_COLUMN)
            .in(aspectNamesToLiterals(aspectNames))
            .groupBy(
                ImmutableList.of(
                    Selector.column(CassandraAspect.URN_COLUMN),
                    Selector.column(CassandraAspect.ASPECT_COLUMN)))
            .build();

    ResultSet rs = _cqlSession.execute(ss);
    Map<String, Pair<Long, Long>> aspectVersionRanges =
        rs.all().stream()
            .collect(
                Collectors.toMap(
                    row -> row.getString(CassandraAspect.ASPECT_COLUMN),
                    row -> Pair.of(row.getLong("min_version"), row.getLong("max_version"))));

    // For each requested aspect that didn't come back from DB, add a version range of (-1, -1)
    for (String aspect : aspectNames) {
      if (!aspectVersionRanges.containsKey(aspect)) {
        aspectVersionRanges.put(aspect, Pair.of(-1L, -1L));
      }
    }

    return aspectVersionRanges;
  }

  @Override
  public void saveAspect(
      @Nullable TransactionContext txContext, @Nonnull EntityAspect aspect, final boolean insert) {
    validateConnection();
    SimpleStatement statement = generateSaveStatement(aspect, insert);
    _cqlSession.execute(statement);
  }

  // TODO: can further improve by running the sub queries in parallel
  // TODO: look into supporting pagination
  @Override
  @Nonnull
  public Map<EntityAspectIdentifier, EntityAspect> batchGet(
      @Nonnull final Set<EntityAspectIdentifier> keys, boolean forUpdate) {
    validateConnection();
    return keys.stream()
        .map(this::getAspect)
        .filter(Objects::nonNull)
        .collect(Collectors.toMap(EntityAspectIdentifier::fromEntityAspect, aspect -> aspect));
  }

  @Override
  @Nullable
  public EntityAspect getAspect(@Nonnull EntityAspectIdentifier key) {
    validateConnection();
    return getAspect(key.getUrn(), key.getAspect(), key.getVersion());
  }

  @Override
  @Nonnull
  public ListResult<String> listLatestAspectMetadata(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final int start,
      final int pageSize) {
    validateConnection();
    return listAspectMetadata(entityName, aspectName, ASPECT_LATEST_VERSION, start, pageSize);
  }

  @Override
  @Nonnull
  public ListResult<String> listAspectMetadata(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final long version,
      final int start,
      final int pageSize) {

    validateConnection();
    SimpleStatement ss =
        selectFrom(CassandraAspect.TABLE_NAME)
            .all()
            .whereColumn(CassandraAspect.ASPECT_COLUMN)
            .isEqualTo(literal(aspectName))
            .whereColumn(CassandraAspect.VERSION_COLUMN)
            .isEqualTo(literal(version))
            .whereColumn(CassandraAspect.ENTITY_COLUMN)
            .isEqualTo(literal(entityName))
            .allowFiltering()
            .build();

    ResultSet rs = _cqlSession.execute(ss);

    int pageNumber = start / pageSize + 1;
    OffsetPager offsetPager = new OffsetPager(pageSize);
    Page<Row> page = offsetPager.getPage(rs, pageNumber);

    final List<EntityAspect> aspects =
        page.getElements().stream()
            .map(CassandraAspect::rowToEntityAspect)
            .collect(Collectors.toList());

    // TODO: address performance issue for getting total count
    //  https://www.datastax.com/blog/running-count-expensive-cassandra
    SimpleStatement ssCount =
        selectFrom(CassandraAspect.TABLE_NAME)
            .countAll()
            .whereColumn(CassandraAspect.ASPECT_COLUMN)
            .isEqualTo(literal(aspectName))
            .whereColumn(CassandraAspect.VERSION_COLUMN)
            .isEqualTo(literal(version))
            .whereColumn(CassandraAspect.ENTITY_COLUMN)
            .isEqualTo(literal(entityName))
            .allowFiltering()
            .build();

    long totalCount = _cqlSession.execute(ssCount).one().getLong(0);

    final List<String> aspectMetadatas =
        aspects.stream().map(EntityAspect::getMetadata).collect(Collectors.toList());

    final ListResultMetadata listResultMetadata =
        toListResultMetadata(
            aspects.stream().map(CassandraAspectDao::toExtraInfo).collect(Collectors.toList()));

    return toListResult(
        aspectMetadatas, listResultMetadata, start, pageNumber, pageSize, totalCount);
  }

  @Override
  @Nonnull
  public <T> T runInTransactionWithRetry(
      @Nonnull final Function<TransactionContext, T> block, final int maxTransactionRetry) {
    validateConnection();
    TransactionContext txContext = TransactionContext.empty(maxTransactionRetry);
    do {
      try {
        // TODO: Try to bend this code to make use of Cassandra batches. This method is called from
        // single-urn operations, so perf should not suffer much
        return block.apply(txContext);
      } catch (DriverException exception) {
        txContext.addException(exception);
      }
    } while (txContext.shouldAttemptRetry());

    throw new RetryLimitReached(
        "Failed to add after " + maxTransactionRetry + " retries", txContext.lastException());
  }

  private <T> ListResult<T> toListResult(
      @Nonnull final List<T> values,
      final ListResultMetadata listResultMetadata,
      @Nonnull final Integer start,
      @Nonnull final Integer pageNumber,
      @Nonnull final Integer pageSize,
      final long totalCount) {
    final int numPages = (int) (totalCount / pageSize + (totalCount % pageSize == 0 ? 0 : 1));
    final boolean hasNext = pageNumber < numPages;

    final int nextStart =
        (start != null && hasNext) ? (pageNumber * pageSize) : ListResult.INVALID_NEXT_START;

    return ListResult.<T>builder()
        .values(values)
        .metadata(listResultMetadata)
        .nextStart(nextStart)
        .hasNext(hasNext)
        .totalCount((int) totalCount)
        .totalPageCount(numPages)
        .pageSize(pageSize)
        .build();
  }

  @Nonnull
  private ListResultMetadata toListResultMetadata(@Nonnull final List<ExtraInfo> extraInfos) {
    final ListResultMetadata listResultMetadata = new ListResultMetadata();
    listResultMetadata.setExtraInfos(new ExtraInfoArray(extraInfos));
    return listResultMetadata;
  }

  @Nonnull
  private static ExtraInfo toExtraInfo(@Nonnull final EntityAspect aspect) {
    final ExtraInfo extraInfo = new ExtraInfo();
    extraInfo.setVersion(aspect.getVersion());
    extraInfo.setAudit(toAuditStamp(aspect));
    try {
      extraInfo.setUrn(Urn.createFromString(aspect.getUrn()));
    } catch (URISyntaxException e) {
      throw new ModelConversionException(e.getMessage());
    }

    return extraInfo;
  }

  @Nonnull
  private static AuditStamp toAuditStamp(@Nonnull final EntityAspect aspect) {
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

  @Override
  public void deleteAspect(
      @Nullable TransactionContext txContext, @Nonnull final EntityAspect aspect) {
    validateConnection();
    SimpleStatement ss =
        deleteFrom(CassandraAspect.TABLE_NAME)
            .whereColumn(CassandraAspect.URN_COLUMN)
            .isEqualTo(literal(aspect.getUrn()))
            .whereColumn(CassandraAspect.ASPECT_COLUMN)
            .isEqualTo(literal(aspect.getAspect()))
            .whereColumn(CassandraAspect.VERSION_COLUMN)
            .isEqualTo(literal(aspect.getVersion()))
            .ifExists()
            .build();

    _cqlSession.execute(ss);
  }

  @Override
  public int deleteUrn(@Nullable TransactionContext txContext, @Nonnull final String urn) {
    validateConnection();
    SimpleStatement ss =
        deleteFrom(CassandraAspect.TABLE_NAME)
            .whereColumn(CassandraAspect.URN_COLUMN)
            .isEqualTo(literal(urn))
            .build();
    ResultSet rs = _cqlSession.execute(ss);
    // TODO: look into how to get around this for counts in Cassandra
    // https://stackoverflow.com/questions/28611459/how-to-know-affected-rows-in-cassandracql
    return rs.getExecutionInfo().getErrors().size() == 0 ? -1 : 0;
  }

  public List<EntityAspect> getAllAspects(String urn, String aspectName) {
    SimpleStatement ss =
        selectFrom(CassandraAspect.TABLE_NAME)
            .all()
            .whereColumn(CassandraAspect.URN_COLUMN)
            .isEqualTo(literal(urn))
            .whereColumn(CassandraAspect.ASPECT_COLUMN)
            .isEqualTo(literal(aspectName))
            .build();

    ResultSet rs = _cqlSession.execute(ss);
    return rs.all().stream().map(CassandraAspect::rowToEntityAspect).collect(Collectors.toList());
  }

  @Override
  @Nullable
  public EntityAspect getAspect(@Nonnull String urn, @Nonnull String aspectName, long version) {
    validateConnection();
    SimpleStatement ss =
        selectFrom(CassandraAspect.TABLE_NAME)
            .all()
            .whereColumn(CassandraAspect.URN_COLUMN)
            .isEqualTo(literal(urn))
            .whereColumn(CassandraAspect.ASPECT_COLUMN)
            .isEqualTo(literal(aspectName))
            .whereColumn(CassandraAspect.VERSION_COLUMN)
            .isEqualTo(literal(version))
            .limit(1)
            .build();

    ResultSet rs = _cqlSession.execute(ss);
    Row row = rs.one();
    return row == null ? null : CassandraAspect.rowToEntityAspect(row);
  }

  @Override
  @Nonnull
  public ListResult<String> listUrns(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final int start,
      final int pageSize) {

    validateConnection();
    SimpleStatement ss =
        selectFrom(CassandraAspect.TABLE_NAME)
            .columns(
                CassandraAspect.URN_COLUMN,
                CassandraAspect.ASPECT_COLUMN,
                CassandraAspect.VERSION_COLUMN)
            .whereColumn(CassandraAspect.ASPECT_COLUMN)
            .isEqualTo(literal(aspectName))
            .whereColumn(CassandraAspect.VERSION_COLUMN)
            .isEqualTo(literal(ASPECT_LATEST_VERSION))
            .whereColumn(CassandraAspect.ENTITY_COLUMN)
            .isEqualTo(literal(entityName))
            .allowFiltering()
            .build();

    ResultSet rs = _cqlSession.execute(ss);

    OffsetPager offsetPager = new OffsetPager(pageSize);
    int pageNumber = start / pageSize + 1;

    Page<Row> page = offsetPager.getPage(rs, pageNumber);

    final List<String> urns =
        page.getElements().stream()
            .map(row -> CassandraAspect.rowToAspectIdentifier(row).getUrn())
            .collect(Collectors.toList());

    // TODO: address performance issue for getting total count
    // https://www.datastax.com/blog/running-count-expensive-cassandra
    SimpleStatement ssCount =
        selectFrom(CassandraAspect.TABLE_NAME)
            .countAll()
            .whereColumn(CassandraAspect.ASPECT_COLUMN)
            .isEqualTo(literal(aspectName))
            .whereColumn(CassandraAspect.VERSION_COLUMN)
            .isEqualTo(literal(ASPECT_LATEST_VERSION))
            .allowFiltering()
            .build();

    long totalCount = _cqlSession.execute(ssCount).one().getLong(0);

    return toListResult(urns, null, start, pageNumber, pageSize, totalCount);
  }

  @Nonnull
  @Override
  public Integer countAspect(@Nonnull String aspectName, @Nullable String urnLike) {
    // Not implemented
    return -1;
  }

  @Nonnull
  public PartitionedStream<EbeanAspectV2> streamAspectBatches(final RestoreIndicesArgs args) {
    // Not implemented
    return null;
  }

  @Nonnull
  @Override
  public Stream<EntityAspect> streamAspects(String entityName, String aspectName) {
    SimpleStatement ss =
        selectFrom(CassandraAspect.TABLE_NAME)
            .all()
            // assumes alpha characters after the entityType prefix
            .whereColumn(CassandraAspect.URN_COLUMN)
            .isGreaterThan(literal(String.join(":", List.of("urn", "li", entityName, ""))))
            .whereColumn(CassandraAspect.URN_COLUMN)
            .isLessThan(
                literal(
                    String.join(
                        ":",
                        List.of(
                            "urn",
                            "li",
                            entityName,
                            "|")))) // this is used for slicing prefixes with alpha characters
            .whereColumn(CassandraAspect.ASPECT_COLUMN)
            .isEqualTo(literal(aspectName))
            .allowFiltering() // performance impact, however # of properties expected to be
            // relatively small
            .build();

    ResultSet rs = _cqlSession.execute(ss);
    return rs.all().stream().map(CassandraAspect::rowToEntityAspect);
  }

  @Override
  @Nonnull
  public Iterable<String> listAllUrns(int start, int pageSize) {
    validateConnection();
    SimpleStatement ss =
        selectFrom(CassandraAspect.TABLE_NAME).column(CassandraAspect.URN_COLUMN).build();

    ResultSet rs = _cqlSession.execute(ss);

    int pageNumber = start / pageSize + 1;
    OffsetPager offsetPager = new OffsetPager(pageSize);
    Page<Row> page = offsetPager.getPage(rs, pageNumber);

    return page.getElements().stream()
        .map(row -> row.getString(CassandraAspect.URN_COLUMN))
        .collect(Collectors.toList());
  }

  @Override
  public Map<String, Map<String, Long>> getNextVersions(Map<String, Set<String>> urnAspectMap) {
    validateConnection();
    Map<String, Map<String, Long>> result = new HashMap<>();

    for (Map.Entry<String, Set<String>> aspectNames : urnAspectMap.entrySet()) {
      Map<String, Pair<Long, Long>> maxVersions =
          getVersionRanges(aspectNames.getKey(), aspectNames.getValue());
      Map<String, Long> nextVersions = new HashMap<>();

      for (String aspectName : aspectNames.getValue()) {
        long latestVersion = maxVersions.get(aspectName).getSecond();
        long nextVal = latestVersion < 0 ? ASPECT_LATEST_VERSION : latestVersion + 1L;
        nextVersions.put(aspectName, nextVal);
      }

      result.put(aspectNames.getKey(), nextVersions);
    }

    return result;
  }

  @Override
  public long saveLatestAspect(
      @Nullable TransactionContext txContext,
      @Nonnull final String urn,
      @Nonnull final String aspectName,
      @Nullable final String oldAspectMetadata,
      @Nullable final String oldActor,
      @Nullable final String oldImpersonator,
      @Nullable final Timestamp oldTime,
      @Nullable final String oldSystemMetadata,
      @Nonnull final String newAspectMetadata,
      @Nonnull final String newActor,
      @Nullable final String newImpersonator,
      @Nonnull final Timestamp newTime,
      @Nullable final String newSystemMetadata,
      final Long nextVersion) {

    validateConnection();
    if (!_canWrite) {
      return 0;
    }
    // Save oldValue as the largest version + 1
    long largestVersion = ASPECT_LATEST_VERSION;
    BatchStatement batch = BatchStatement.newInstance(BatchType.UNLOGGED);
    if (!ASPECT_LATEST_VERSION.equals(nextVersion) && oldTime != null) {
      largestVersion = nextVersion;
      final EntityAspect aspect =
          new EntityAspect(
              urn,
              aspectName,
              largestVersion,
              oldAspectMetadata,
              oldSystemMetadata,
              oldTime,
              oldActor,
              oldImpersonator);
      batch = batch.add(generateSaveStatement(aspect, true));
    }

    // Save newValue as the latest version (v0)
    final EntityAspect aspect =
        new EntityAspect(
            urn,
            aspectName,
            ASPECT_LATEST_VERSION,
            newAspectMetadata,
            newSystemMetadata,
            newTime,
            newActor,
            newImpersonator);
    batch = batch.add(generateSaveStatement(aspect, ASPECT_LATEST_VERSION.equals(nextVersion)));
    _cqlSession.execute(batch);
    return largestVersion;
  }

  private SimpleStatement generateSaveStatement(EntityAspect aspect, boolean insert) {
    String entity;
    try {
      entity = (new Urn(aspect.getUrn())).getEntityType();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    if (insert) {
      Insert ri =
          insertInto(CassandraAspect.TABLE_NAME)
              .value(CassandraAspect.URN_COLUMN, literal(aspect.getUrn()))
              .value(CassandraAspect.ASPECT_COLUMN, literal(aspect.getAspect()))
              .value(CassandraAspect.VERSION_COLUMN, literal(aspect.getVersion()))
              .value(CassandraAspect.SYSTEM_METADATA_COLUMN, literal(aspect.getSystemMetadata()))
              .value(CassandraAspect.METADATA_COLUMN, literal(aspect.getMetadata()))
              .value(CassandraAspect.CREATED_ON_COLUMN, literal(aspect.getCreatedOn().getTime()))
              .value(CassandraAspect.CREATED_FOR_COLUMN, literal(aspect.getCreatedFor()))
              .value(CassandraAspect.ENTITY_COLUMN, literal(entity))
              .value(CassandraAspect.CREATED_BY_COLUMN, literal(aspect.getCreatedBy()))
              .ifNotExists();
      return ri.build();
    } else {

      UpdateWithAssignments uwa =
          update(CassandraAspect.TABLE_NAME)
              .setColumn(CassandraAspect.METADATA_COLUMN, literal(aspect.getMetadata()))
              .setColumn(
                  CassandraAspect.SYSTEM_METADATA_COLUMN, literal(aspect.getSystemMetadata()))
              .setColumn(
                  CassandraAspect.CREATED_ON_COLUMN, literal(aspect.getCreatedOn().getTime()))
              .setColumn(CassandraAspect.CREATED_BY_COLUMN, literal(aspect.getCreatedBy()))
              .setColumn(CassandraAspect.CREATED_FOR_COLUMN, literal(aspect.getCreatedFor()));

      Update u =
          uwa.whereColumn(CassandraAspect.URN_COLUMN)
              .isEqualTo(literal(aspect.getUrn()))
              .whereColumn(CassandraAspect.ASPECT_COLUMN)
              .isEqualTo(literal(aspect.getAspect()))
              .whereColumn(CassandraAspect.VERSION_COLUMN)
              .isEqualTo(literal(aspect.getVersion()))
              .ifExists();

      return u.build();
    }
  }

  @Override
  public void setWritable(boolean canWrite) {
    _canWrite = canWrite;
  }

  @Override
  public void saveAspect(
      @Nullable TransactionContext txContext,
      @Nonnull final String urn,
      @Nonnull final String aspectName,
      @Nonnull final String aspectMetadata,
      @Nonnull final String actor,
      @Nullable final String impersonator,
      @Nonnull final Timestamp timestamp,
      @Nonnull final String systemMetadata,
      final long version,
      final boolean insert) {

    validateConnection();
    final EntityAspect aspect =
        new EntityAspect(
            urn,
            aspectName,
            version,
            aspectMetadata,
            systemMetadata,
            timestamp,
            actor,
            impersonator);

    saveAspect(txContext, aspect, insert);

    // metrics
    incrementWriteMetrics(aspectName, 1, aspectMetadata.getBytes(StandardCharsets.UTF_8).length);
  }

  @Override
  @Nonnull
  public List<EntityAspect> getAspectsInRange(
      @Nonnull Urn urn, Set<String> aspectNames, long startTimeMillis, long endTimeMillis) {
    validateConnection();
    SimpleStatement ss =
        selectFrom(CassandraAspect.TABLE_NAME)
            .all()
            .whereColumn(CassandraAspect.URN_COLUMN)
            .isEqualTo(literal(urn.toString()))
            .whereColumn(CassandraAspect.ASPECT_COLUMN)
            .in(aspectNamesToLiterals(aspectNames))
            .whereColumn(CassandraAspect.CREATED_ON_COLUMN)
            .isLessThanOrEqualTo(literal(startTimeMillis))
            .whereColumn(CassandraAspect.CREATED_ON_COLUMN)
            .isGreaterThan(literal(endTimeMillis))
            .allowFiltering()
            .build();

    ResultSet rs = _cqlSession.execute(ss);

    return rs.all().stream().map(CassandraAspect::rowToEntityAspect).collect(Collectors.toList());
  }

  private Iterable<Term> aspectNamesToLiterals(Set<String> aspectNames) {
    return aspectNames.stream().map(QueryBuilder::literal).collect(Collectors.toSet());
  }
}
