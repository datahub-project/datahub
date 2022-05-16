package com.linkedin.metadata.entity.cassandra;

import com.datahub.util.exception.ModelConversionException;
import com.datahub.util.exception.RetryLimitReached;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
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
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.ExtraInfoArray;
import com.linkedin.metadata.query.ListResultMetadata;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.update;
import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;

@Slf4j
public class CassandraAspectDao implements AspectDao {
  protected final CqlSession _cqlSession;
  private boolean _canWrite = true;

    public CassandraAspectDao(@Nonnull final CqlSession cqlSession) {
    _cqlSession = cqlSession;
  }

  public CassandraAspect getLatestAspect(String urn, String aspectName) {
    return getAspect(urn, aspectName, ASPECT_LATEST_VERSION);
  }

  public long getMaxVersion(@Nonnull final String urn, @Nonnull final String aspectName) {
    Map<String, Long> result = getMaxVersions(urn, ImmutableSet.of(aspectName));
    return result.get(aspectName);
  }

  public Map<String, Long> getMaxVersions(@Nonnull final String urn, @Nonnull final Set<String> aspectNames) {
    Set<Term> aspectNamesArg = aspectNames.stream().map(QueryBuilder::literal).collect(Collectors.toSet());
    SimpleStatement ss = selectFrom(CassandraAspect.TABLE_NAME)
        .selectors(
            Selector.column(CassandraAspect.URN_COLUMN),
            Selector.column(CassandraAspect.ASPECT_COLUMN),
            Selector.function("max", Selector.column(CassandraAspect.VERSION_COLUMN)).as(CassandraAspect.VERSION_COLUMN))
        .whereColumn(CassandraAspect.URN_COLUMN).isEqualTo(literal(urn))
        .whereColumn(CassandraAspect.ASPECT_COLUMN).in(aspectNamesArg)
        .groupBy(ImmutableList.of(Selector.column(CassandraAspect.URN_COLUMN), Selector.column(CassandraAspect.ASPECT_COLUMN)))
        .build();

    ResultSet rs = _cqlSession.execute(ss);
    Map<String, Long> aspectVersions = rs.all().stream()
        .collect(Collectors.toMap(
            row -> row.getString(CassandraAspect.ASPECT_COLUMN),
            row -> row.getLong(CassandraAspect.VERSION_COLUMN)));

    // For each requested aspect that didn't come back from DB, add a version -1
    for (String aspect : aspectNames) {
      if (!aspectVersions.containsKey(aspect)) {
        aspectVersions.put(aspect, -1L);
      }
    }

    return aspectVersions;
  }

  private Insert createCondInsertStatement(CassandraAspect cassandraAspect) {

    String entity;

    try {
      entity = (new Urn(cassandraAspect.getUrn())).getEntityType();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    return insertInto(CassandraAspect.TABLE_NAME)
            .value(CassandraAspect.URN_COLUMN, literal(cassandraAspect.getUrn()))
            .value(CassandraAspect.ASPECT_COLUMN, literal(cassandraAspect.getAspect()))
            .value(CassandraAspect.VERSION_COLUMN, literal(cassandraAspect.getVersion()))
            .value(CassandraAspect.SYSTEM_METADATA_COLUMN, literal(cassandraAspect.getSystemMetadata()))
            .value(CassandraAspect.METADATA_COLUMN, literal(cassandraAspect.getMetadata()))
            .value(CassandraAspect.CREATED_ON_COLUMN, literal(cassandraAspect.getCreatedOn().getTime()))
            .value(CassandraAspect.CREATED_FOR_COLUMN, literal(cassandraAspect.getCreatedFor()))
            .value(CassandraAspect.ENTITY_COLUMN, literal(entity))
            .value(CassandraAspect.CREATED_BY_COLUMN, literal(cassandraAspect.getCreatedBy()))
            .ifNotExists();
  }

  private Update createUpdateStatement(CassandraAspect newCassandraAspect, CassandraAspect oldCassandraAspect) {
    return update(CassandraAspect.TABLE_NAME)
            .setColumn(CassandraAspect.METADATA_COLUMN, literal(newCassandraAspect.getMetadata()))
            .setColumn(CassandraAspect.SYSTEM_METADATA_COLUMN, literal(newCassandraAspect.getSystemMetadata()))
            .setColumn(CassandraAspect.CREATED_ON_COLUMN, literal(newCassandraAspect.getCreatedOn().getTime()))
            .setColumn(CassandraAspect.CREATED_BY_COLUMN, literal(newCassandraAspect.getCreatedBy()))
            .setColumn(CassandraAspect.CREATED_FOR_COLUMN, literal(newCassandraAspect.getCreatedFor()))
            .whereColumn(CassandraAspect.URN_COLUMN).isEqualTo(literal(newCassandraAspect.getUrn()))
            .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(newCassandraAspect.getAspect()))
            .whereColumn(CassandraAspect.VERSION_COLUMN).isEqualTo(literal(newCassandraAspect.getVersion()));
  }

  public void saveAspect(CassandraAspect cassandraAspect, final boolean insert) {

    String entity;

    try {
      entity = (new Urn(cassandraAspect.getUrn())).getEntityType();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    if (insert) {
      Insert ri = insertInto(CassandraAspect.TABLE_NAME)
        .value(CassandraAspect.URN_COLUMN, literal(cassandraAspect.getUrn()))
        .value(CassandraAspect.ASPECT_COLUMN, literal(cassandraAspect.getAspect()))
        .value(CassandraAspect.VERSION_COLUMN, literal(cassandraAspect.getVersion()))
        .value(CassandraAspect.SYSTEM_METADATA_COLUMN, literal(cassandraAspect.getSystemMetadata()))
        .value(CassandraAspect.METADATA_COLUMN, literal(cassandraAspect.getMetadata()))
        .value(CassandraAspect.CREATED_ON_COLUMN, literal(cassandraAspect.getCreatedOn().getTime()))
        .value(CassandraAspect.CREATED_FOR_COLUMN, literal(cassandraAspect.getCreatedFor()))
        .value(CassandraAspect.ENTITY_COLUMN, literal(entity))
        .value(CassandraAspect.CREATED_BY_COLUMN, literal(cassandraAspect.getCreatedBy()));
      _cqlSession.execute(ri.build());
    } else {

      UpdateWithAssignments uwa = update(CassandraAspect.TABLE_NAME)
        .setColumn(CassandraAspect.METADATA_COLUMN, literal(cassandraAspect.getMetadata()))
        .setColumn(CassandraAspect.SYSTEM_METADATA_COLUMN, literal(cassandraAspect.getSystemMetadata()))
        .setColumn(CassandraAspect.CREATED_ON_COLUMN, literal(cassandraAspect.getCreatedOn().getTime()))
        .setColumn(CassandraAspect.CREATED_BY_COLUMN, literal(cassandraAspect.getCreatedBy()))
        .setColumn(CassandraAspect.CREATED_FOR_COLUMN, literal(cassandraAspect.getCreatedFor()));

      Update u = uwa.whereColumn(CassandraAspect.URN_COLUMN).isEqualTo(literal(cassandraAspect.getUrn()))
        .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(cassandraAspect.getAspect()))
        .whereColumn(CassandraAspect.VERSION_COLUMN).isEqualTo(literal(cassandraAspect.getVersion()));

      _cqlSession.execute(u.build());
    }
  }

  // TODO: can further improve by running the sub queries in parallel
  // TODO: look into supporting pagination
  @Nonnull
  public Map<CassandraAspect.PrimaryKey, CassandraAspect> batchGet(@Nonnull final Set<CassandraAspect.PrimaryKey> keys) {
    return keys.stream()
            .map(k -> getAspect(k))
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(CassandraAspect::toPrimaryKey, record -> record));
  }

  public CassandraAspect getAspect(CassandraAspect.PrimaryKey pk) {
    return getAspect(pk.getUrn(), pk.getAspect(), pk.getVersion());
  }

  @Nonnull
  public ListResult<String> listLatestAspectMetadata(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final int start,
      final int pageSize) {
    return listAspectMetadata(entityName, aspectName, ASPECT_LATEST_VERSION, start, pageSize);
  }

  @Nonnull
  public ListResult<String> listAspectMetadata(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final long version,
      final int start,
      final int pageSize) {

    OffsetPager offsetPager = new OffsetPager(pageSize);

    SimpleStatement ss = selectFrom(CassandraAspect.TABLE_NAME)
      .all()
      .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
      .whereColumn(CassandraAspect.VERSION_COLUMN).isEqualTo(literal(version))
      .whereColumn(CassandraAspect.ENTITY_COLUMN).isEqualTo(literal(entityName))
      .allowFiltering()
      .build();

    ResultSet rs = _cqlSession.execute(ss);

    int pageNumber = start / pageSize + 1;

    Page<Row> page = offsetPager.getPage(rs, pageNumber);

    final List<CassandraAspect> aspects = page
      .getElements()
      .stream().map(CassandraAspect::fromRow)
      .collect(Collectors.toList());

    // TODO: address performance issue for getting total count
    // https://www.datastax.com/blog/running-count-expensive-cassandra
    SimpleStatement ssCount = selectFrom(CassandraAspect.TABLE_NAME)
      .countAll()
      .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
      .whereColumn(CassandraAspect.VERSION_COLUMN).isEqualTo(literal(version))
      .whereColumn(CassandraAspect.ENTITY_COLUMN).isEqualTo(literal(entityName))
      .allowFiltering()
      .build();

    long totalCount = _cqlSession.execute(ssCount).one().getLong(0);

    final List<String> aspectMetadatas = aspects
            .stream()
            .map(CassandraAspect::getMetadata)
            .collect(Collectors.toList());

    final ListResultMetadata listResultMetadata = toListResultMetadata(aspects
            .stream()
            .map(CassandraAspectDao::toExtraInfo)
            .collect(Collectors.toList()));

    return toListResult(aspectMetadatas, listResultMetadata, start, pageNumber, pageSize, totalCount);
  }

  @Nonnull
  public <T> T runInTransactionWithRetry(@Nonnull final Supplier<T> block, final int maxTransactionRetry) {
    int retryCount = 0;
    Exception lastException;

    do {
      try {
        // TODO: Try to bend this code to make use of Cassandra batches. This method is called from single-urn operations, so perf should not suffer much
        return block.get();
      } catch (DriverException exception) {
        lastException = exception;
      }
    } while (++retryCount <= maxTransactionRetry);

    throw new RetryLimitReached("Failed to add after " + maxTransactionRetry + " retries", lastException);
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

    final int nextStart = (start != null && hasNext) ? (pageNumber * pageSize) : ListResult.INVALID_NEXT_START;

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
  private static ExtraInfo toExtraInfo(@Nonnull final CassandraAspect aspect) {
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
  private static AuditStamp toAuditStamp(@Nonnull final CassandraAspect aspect) {
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

  public boolean deleteAspect(@Nonnull final CassandraAspect aspect) {
      SimpleStatement ss = deleteFrom(CassandraAspect.TABLE_NAME)
      .whereColumn(CassandraAspect.URN_COLUMN).isEqualTo(literal(aspect.getUrn()))
      .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(aspect.getAspect()))
      .whereColumn(CassandraAspect.VERSION_COLUMN).isEqualTo(literal(aspect.getVersion()))
        .build();
    ResultSet rs = _cqlSession.execute(ss);

    return rs.getExecutionInfo().getErrors().size() == 0;
  }

  public int deleteUrn(@Nonnull final String urn) {
      SimpleStatement ss = deleteFrom(CassandraAspect.TABLE_NAME)
      .whereColumn(CassandraAspect.URN_COLUMN).isEqualTo(literal(urn))
        .build();
    ResultSet rs = _cqlSession.execute(ss);
    // TODO: look into how to get around this for counts in Cassandra
    // https://stackoverflow.com/questions/28611459/how-to-know-affected-rows-in-cassandracql
    return rs.getExecutionInfo().getErrors().size() == 0 ? -1 : 0;
  }

  public List<CassandraAspect> getAllAspects(String urn, String aspectName) {
    SimpleStatement ss = selectFrom(CassandraAspect.TABLE_NAME)
            .all()
            .whereColumn(CassandraAspect.URN_COLUMN).isEqualTo(literal(urn))
            .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
            .build();

    ResultSet rs = _cqlSession.execute(ss);

    return rs.all().stream().map(CassandraAspect::fromRow).collect(Collectors.toList());
  }

  public CassandraAspect getAspect(String urn, String aspectName, long version) {
    SimpleStatement ss = selectFrom(CassandraAspect.TABLE_NAME)
      .all()
      .whereColumn(CassandraAspect.URN_COLUMN).isEqualTo(literal(urn))
      .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
      .whereColumn(CassandraAspect.VERSION_COLUMN).isEqualTo(literal(version))
      .limit(1)
      .build();


    ResultSet rs = _cqlSession.execute(ss);
    Row r = rs.one();

    if (r == null) {
      return null;
    }

    return CassandraAspect.fromRow(r);
  }

  @Nonnull
  public ListResult<String> listUrns(
          @Nonnull final String aspectName,
          final int start,
          final int pageSize) {

    OffsetPager offsetPager = new OffsetPager(pageSize);

    SimpleStatement ss = selectFrom(CassandraAspect.TABLE_NAME)
            .all()
            .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
            .whereColumn(CassandraAspect.VERSION_COLUMN).isEqualTo(literal(ASPECT_LATEST_VERSION))
            .allowFiltering()
            .build();

    ResultSet rs = _cqlSession.execute(ss);

    int pageNumber = start / pageSize + 1;

    Page<Row> page = offsetPager.getPage(rs, pageNumber);

    final List<String> urns = page
            .getElements()
            .stream().map(r -> CassandraAspect.fromRow(r).getUrn())
            .collect(Collectors.toList());

    // TODO: address performance issue for getting total count
    // https://www.datastax.com/blog/running-count-expensive-cassandra
    SimpleStatement ssCount = selectFrom(CassandraAspect.TABLE_NAME)
            .countAll()
            .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
            .whereColumn(CassandraAspect.VERSION_COLUMN).isEqualTo(literal(ASPECT_LATEST_VERSION))
            .allowFiltering()
            .build();

    long totalCount = _cqlSession.execute(ssCount).one().getLong(0);

    return toListResult(urns, null, start, pageNumber, pageSize, totalCount);
  }

  public long getNextVersion(@Nonnull final String urn, @Nonnull final String aspectName) {
    Map<String, Long> versions = getNextVersions(urn, ImmutableSet.of(aspectName));
    return versions.get(aspectName);
  }

  public Map<String, Long> getNextVersions(@Nonnull final String urn, @Nonnull final Set<String> aspectNames) {
    Map<String, Long> maxVersions = getMaxVersions(urn, aspectNames);
    Map<String, Long> nextVersions = new HashMap<>();

    for (String aspectName: aspectNames) {
      long latestVersion = maxVersions.get(aspectName);
      long nextVal = latestVersion < 0 ? ASPECT_LATEST_VERSION : latestVersion + 1L;
      nextVersions.put(aspectName, nextVal);
    }

    return nextVersions;
  }

  public long saveLatestAspect(
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
      final Long nextVersion
  ) {
    if (!_canWrite) {
      return 0;
    }
    // Save oldValue as the largest version + 1
    long largestVersion = ASPECT_LATEST_VERSION;
    if (oldAspectMetadata != null && oldTime != null) {
      largestVersion = nextVersion;
      saveAspect(urn, aspectName, oldAspectMetadata, oldActor, oldImpersonator, oldTime, oldSystemMetadata, largestVersion, true);
    }

    // Save newValue as the latest version (v0)
    saveAspect(urn, aspectName, newAspectMetadata, newActor, newImpersonator, newTime, newSystemMetadata, ASPECT_LATEST_VERSION, oldAspectMetadata == null);

    return largestVersion;
  }

  public void setWritable(boolean canWrite) {
    _canWrite = canWrite;
  }

  protected void saveAspect(
      @Nonnull final String urn,
      @Nonnull final String aspectName,
      @Nonnull final String aspectMetadata,
      @Nonnull final String actor,
      @Nullable final String impersonator,
      @Nonnull final Timestamp timestamp,
      @Nonnull final String systemMetadata,
      final long version,
      final boolean insert) {

    final CassandraAspect aspect = new CassandraAspect();
    aspect.setUrn(urn);
    aspect.setAspect(aspectName);
    aspect.setVersion(version);
    aspect.setMetadata(aspectMetadata);
    aspect.setSystemMetadata(systemMetadata);
    aspect.setCreatedOn(timestamp);
    aspect.setCreatedBy(actor);
    if (impersonator != null) {
      aspect.setCreatedFor(impersonator);
    }

    saveAspect(aspect, insert);
  }
}