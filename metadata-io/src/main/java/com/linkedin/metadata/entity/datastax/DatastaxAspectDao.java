package com.linkedin.metadata.entity.datastax;

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
public class DatastaxAspectDao implements AspectDao {
  protected final CqlSession _cqlSession;
  private boolean _canWrite = true;

    public DatastaxAspectDao(@Nonnull final CqlSession cqlSession) {
    _cqlSession = cqlSession;
  }

  public DatastaxAspect getLatestAspect(String urn, String aspectName) {
    return getAspect(urn, aspectName, ASPECT_LATEST_VERSION);
  }

  public long getMaxVersion(@Nonnull final String urn, @Nonnull final String aspectName) {
    Map<String, Long> result = getMaxVersions(urn, ImmutableSet.of(aspectName));
    return result.get(aspectName);
  }

  public Map<String, Long> getMaxVersions(@Nonnull final String urn, @Nonnull final Set<String> aspectNames) {
    Set<Term> aspectNamesArg = aspectNames.stream().map(QueryBuilder::literal).collect(Collectors.toSet());
    SimpleStatement ss = selectFrom(DatastaxAspect.TABLE_NAME)
        .selectors(
            Selector.column(DatastaxAspect.URN_COLUMN),
            Selector.column(DatastaxAspect.ASPECT_COLUMN),
            Selector.function("max", Selector.column(DatastaxAspect.VERSION_COLUMN)).as(DatastaxAspect.VERSION_COLUMN))
        .whereColumn(DatastaxAspect.URN_COLUMN).isEqualTo(literal(urn))
        .whereColumn(DatastaxAspect.ASPECT_COLUMN).in(aspectNamesArg)
        .groupBy(ImmutableList.of(Selector.column(DatastaxAspect.URN_COLUMN), Selector.column(DatastaxAspect.ASPECT_COLUMN)))
        .build();

    ResultSet rs = _cqlSession.execute(ss);
    Map<String, Long> aspectVersions = rs.all().stream()
        .collect(Collectors.toMap(
            row -> row.getString(DatastaxAspect.ASPECT_COLUMN),
            row -> row.getLong(DatastaxAspect.VERSION_COLUMN)));

    // For each requested aspect that didn't come back from DB, add a version -1
    for (String aspect : aspectNames) {
      if (!aspectVersions.containsKey(aspect)) {
        aspectVersions.put(aspect, -1L);
      }
    }

    return aspectVersions;
  }

  private Insert createCondInsertStatement(DatastaxAspect datastaxAspect) {

    String entity;

    try {
      entity = (new Urn(datastaxAspect.getUrn())).getEntityType();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    return insertInto(DatastaxAspect.TABLE_NAME)
            .value(DatastaxAspect.URN_COLUMN, literal(datastaxAspect.getUrn()))
            .value(DatastaxAspect.ASPECT_COLUMN, literal(datastaxAspect.getAspect()))
            .value(DatastaxAspect.VERSION_COLUMN, literal(datastaxAspect.getVersion()))
            .value(DatastaxAspect.SYSTEM_METADATA_COLUMN, literal(datastaxAspect.getSystemMetadata()))
            .value(DatastaxAspect.METADATA_COLUMN, literal(datastaxAspect.getMetadata()))
            .value(DatastaxAspect.CREATED_ON_COLUMN, literal(datastaxAspect.getCreatedOn().getTime()))
            .value(DatastaxAspect.CREATED_FOR_COLUMN, literal(datastaxAspect.getCreatedFor()))
            .value(DatastaxAspect.ENTITY_COLUMN, literal(entity))
            .value(DatastaxAspect.CREATED_BY_COLUMN, literal(datastaxAspect.getCreatedBy()))
            .ifNotExists();
  }

  private Update createUpdateStatement(DatastaxAspect newDatastaxAspect, DatastaxAspect oldDatastaxAspect) {
    return update(DatastaxAspect.TABLE_NAME)
            .setColumn(DatastaxAspect.METADATA_COLUMN, literal(newDatastaxAspect.getMetadata()))
            .setColumn(DatastaxAspect.SYSTEM_METADATA_COLUMN, literal(newDatastaxAspect.getSystemMetadata()))
            .setColumn(DatastaxAspect.CREATED_ON_COLUMN, literal(newDatastaxAspect.getCreatedOn().getTime()))
            .setColumn(DatastaxAspect.CREATED_BY_COLUMN, literal(newDatastaxAspect.getCreatedBy()))
            .setColumn(DatastaxAspect.CREATED_FOR_COLUMN, literal(newDatastaxAspect.getCreatedFor()))
            .whereColumn(DatastaxAspect.URN_COLUMN).isEqualTo(literal(newDatastaxAspect.getUrn()))
            .whereColumn(DatastaxAspect.ASPECT_COLUMN).isEqualTo(literal(newDatastaxAspect.getAspect()))
            .whereColumn(DatastaxAspect.VERSION_COLUMN).isEqualTo(literal(newDatastaxAspect.getVersion()));
  }

  public void saveAspect(DatastaxAspect datastaxAspect, final boolean insert) {

    String entity;

    try {
      entity = (new Urn(datastaxAspect.getUrn())).getEntityType();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    if (insert) {
      Insert ri = insertInto(DatastaxAspect.TABLE_NAME)
        .value(DatastaxAspect.URN_COLUMN, literal(datastaxAspect.getUrn()))
        .value(DatastaxAspect.ASPECT_COLUMN, literal(datastaxAspect.getAspect()))
        .value(DatastaxAspect.VERSION_COLUMN, literal(datastaxAspect.getVersion()))
        .value(DatastaxAspect.SYSTEM_METADATA_COLUMN, literal(datastaxAspect.getSystemMetadata()))
        .value(DatastaxAspect.METADATA_COLUMN, literal(datastaxAspect.getMetadata()))
        .value(DatastaxAspect.CREATED_ON_COLUMN, literal(datastaxAspect.getCreatedOn().getTime()))
        .value(DatastaxAspect.CREATED_FOR_COLUMN, literal(datastaxAspect.getCreatedFor()))
        .value(DatastaxAspect.ENTITY_COLUMN, literal(entity))
        .value(DatastaxAspect.CREATED_BY_COLUMN, literal(datastaxAspect.getCreatedBy()));
      _cqlSession.execute(ri.build());
    } else {

      UpdateWithAssignments uwa = update(DatastaxAspect.TABLE_NAME)
        .setColumn(DatastaxAspect.METADATA_COLUMN, literal(datastaxAspect.getMetadata()))
        .setColumn(DatastaxAspect.SYSTEM_METADATA_COLUMN, literal(datastaxAspect.getSystemMetadata()))
        .setColumn(DatastaxAspect.CREATED_ON_COLUMN, literal(datastaxAspect.getCreatedOn().getTime()))
        .setColumn(DatastaxAspect.CREATED_BY_COLUMN, literal(datastaxAspect.getCreatedBy()))
        .setColumn(DatastaxAspect.CREATED_FOR_COLUMN, literal(datastaxAspect.getCreatedFor()));

      Update u = uwa.whereColumn(DatastaxAspect.URN_COLUMN).isEqualTo(literal(datastaxAspect.getUrn()))
        .whereColumn(DatastaxAspect.ASPECT_COLUMN).isEqualTo(literal(datastaxAspect.getAspect()))
        .whereColumn(DatastaxAspect.VERSION_COLUMN).isEqualTo(literal(datastaxAspect.getVersion()));

      _cqlSession.execute(u.build());
    }
  }

  // TODO: can further improve by running the sub queries in parallel
  // TODO: look into supporting pagination
  @Nonnull
  public Map<DatastaxAspect.PrimaryKey, DatastaxAspect> batchGet(@Nonnull final Set<DatastaxAspect.PrimaryKey> keys) {
    return keys.stream()
            .map(k -> getAspect(k))
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(DatastaxAspect::toPrimaryKey, record -> record));
  }

  public DatastaxAspect getAspect(DatastaxAspect.PrimaryKey pk) {
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

    SimpleStatement ss = selectFrom(DatastaxAspect.TABLE_NAME)
      .all()
      .whereColumn(DatastaxAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
      .whereColumn(DatastaxAspect.VERSION_COLUMN).isEqualTo(literal(version))
      .whereColumn(DatastaxAspect.ENTITY_COLUMN).isEqualTo(literal(entityName))
      .allowFiltering()
      .build();

    ResultSet rs = _cqlSession.execute(ss);

    int pageNumber = start / pageSize + 1;

    Page<Row> page = offsetPager.getPage(rs, pageNumber);

    final List<DatastaxAspect> aspects = page
      .getElements()
      .stream().map(DatastaxAspect::fromRow)
      .collect(Collectors.toList());

    // TODO: address performance issue for getting total count
    // https://www.datastax.com/blog/running-count-expensive-cassandra
    SimpleStatement ssCount = selectFrom(DatastaxAspect.TABLE_NAME)
      .countAll()
      .whereColumn(DatastaxAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
      .whereColumn(DatastaxAspect.VERSION_COLUMN).isEqualTo(literal(version))
      .whereColumn(DatastaxAspect.ENTITY_COLUMN).isEqualTo(literal(entityName))
      .allowFiltering()
      .build();

    long totalCount = _cqlSession.execute(ssCount).one().getLong(0);

    final List<String> aspectMetadatas = aspects
            .stream()
            .map(DatastaxAspect::getMetadata)
            .collect(Collectors.toList());

    final ListResultMetadata listResultMetadata = toListResultMetadata(aspects
            .stream()
            .map(DatastaxAspectDao::toExtraInfo)
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
  private static ExtraInfo toExtraInfo(@Nonnull final DatastaxAspect aspect) {
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
  private static AuditStamp toAuditStamp(@Nonnull final DatastaxAspect aspect) {
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

  public boolean deleteAspect(@Nonnull final DatastaxAspect aspect) {
      SimpleStatement ss = deleteFrom(DatastaxAspect.TABLE_NAME)
      .whereColumn(DatastaxAspect.URN_COLUMN).isEqualTo(literal(aspect.getUrn()))
      .whereColumn(DatastaxAspect.ASPECT_COLUMN).isEqualTo(literal(aspect.getAspect()))
      .whereColumn(DatastaxAspect.VERSION_COLUMN).isEqualTo(literal(aspect.getVersion()))
        .build();
    ResultSet rs = _cqlSession.execute(ss);

    return rs.getExecutionInfo().getErrors().size() == 0;
  }

  public int deleteUrn(@Nonnull final String urn) {
      SimpleStatement ss = deleteFrom(DatastaxAspect.TABLE_NAME)
      .whereColumn(DatastaxAspect.URN_COLUMN).isEqualTo(literal(urn))
        .build();
    ResultSet rs = _cqlSession.execute(ss);
    // TODO: look into how to get around this for counts in Cassandra
    // https://stackoverflow.com/questions/28611459/how-to-know-affected-rows-in-cassandracql
    return rs.getExecutionInfo().getErrors().size() == 0 ? -1 : 0;
  }

  public List<DatastaxAspect> getAllAspects(String urn, String aspectName) {
    SimpleStatement ss = selectFrom(DatastaxAspect.TABLE_NAME)
            .all()
            .whereColumn(DatastaxAspect.URN_COLUMN).isEqualTo(literal(urn))
            .whereColumn(DatastaxAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
            .build();

    ResultSet rs = _cqlSession.execute(ss);

    return rs.all().stream().map(DatastaxAspect::fromRow).collect(Collectors.toList());
  }

  public DatastaxAspect getAspect(String urn, String aspectName, long version) {
    SimpleStatement ss = selectFrom(DatastaxAspect.TABLE_NAME)
      .all()
      .whereColumn(DatastaxAspect.URN_COLUMN).isEqualTo(literal(urn))
      .whereColumn(DatastaxAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
      .whereColumn(DatastaxAspect.VERSION_COLUMN).isEqualTo(literal(version))
      .limit(1)
      .build();


    ResultSet rs = _cqlSession.execute(ss);
    Row r = rs.one();

    if (r == null) {
      return null;
    }

    return DatastaxAspect.fromRow(r);
  }

  @Nonnull
  public ListResult<String> listUrns(
          @Nonnull final String aspectName,
          final int start,
          final int pageSize) {

    OffsetPager offsetPager = new OffsetPager(pageSize);

    SimpleStatement ss = selectFrom(DatastaxAspect.TABLE_NAME)
            .all()
            .whereColumn(DatastaxAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
            .whereColumn(DatastaxAspect.VERSION_COLUMN).isEqualTo(literal(ASPECT_LATEST_VERSION))
            .allowFiltering()
            .build();

    ResultSet rs = _cqlSession.execute(ss);

    int pageNumber = start / pageSize + 1;

    Page<Row> page = offsetPager.getPage(rs, pageNumber);

    final List<String> urns = page
            .getElements()
            .stream().map(r -> DatastaxAspect.fromRow(r).getUrn())
            .collect(Collectors.toList());

    // TODO: address performance issue for getting total count
    // https://www.datastax.com/blog/running-count-expensive-cassandra
    SimpleStatement ssCount = selectFrom(DatastaxAspect.TABLE_NAME)
            .countAll()
            .whereColumn(DatastaxAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
            .whereColumn(DatastaxAspect.VERSION_COLUMN).isEqualTo(literal(ASPECT_LATEST_VERSION))
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

    final DatastaxAspect aspect = new DatastaxAspect();
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