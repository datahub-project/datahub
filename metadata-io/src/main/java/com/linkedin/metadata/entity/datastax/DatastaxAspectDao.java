package com.linkedin.metadata.entity.datastax;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.paging.OffsetPager;
import com.datastax.oss.driver.api.core.paging.OffsetPager.Page;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.exception.ModelConversionException;
import com.linkedin.metadata.dao.exception.RetryLimitReached;
import com.linkedin.metadata.dao.retention.IndefiniteRetention;
import com.linkedin.metadata.dao.retention.Retention;
import com.linkedin.metadata.dao.retention.TimeBasedRetention;
import com.linkedin.metadata.dao.retention.VersionBasedRetention;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.ExtraInfoArray;
import com.linkedin.metadata.query.ListResultMetadata;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.time.Clock;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;

import lombok.extern.slf4j.Slf4j;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.update;
import static com.linkedin.metadata.Constants.*;

@Slf4j
public class DatastaxAspectDao implements AspectDao {
  protected final CqlSession _cqlSession;
  private static final IndefiniteRetention INDEFINITE_RETENTION = new IndefiniteRetention();
  private final Map<String, Retention> _aspectRetentionMap = new HashMap<>();
  private final Clock _clock = Clock.systemUTC();

  public DatastaxAspectDao(@Nonnull final Map<String, String> sessionConfig) {

    int port = Integer.parseInt(sessionConfig.get("port"));
    List<InetSocketAddress> addresses = Arrays.stream(sessionConfig.get("hosts").split(","))
      .map(host -> new InetSocketAddress(host, port))
      .collect(Collectors.toList());

    String dc = sessionConfig.get("datacenter");
    String ks = sessionConfig.get("keyspace");
    String username = sessionConfig.get("username");
    String password = sessionConfig.get("password");

    CqlSessionBuilder csb = CqlSession.builder()
      .addContactPoints(addresses)
      .withLocalDatacenter(dc)
      .withKeyspace(ks)
      .withAuthCredentials(username, password);

    if (sessionConfig.containsKey("useSsl") && sessionConfig.get("useSsl").equals("true")) {
      try {
        csb = csb.withSslContext(SSLContext.getDefault());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    _cqlSession = csb.build();
  }

  public DatastaxAspect getLatestAspect(String urn, String aspectName) {
    return getAspect(urn, aspectName, 0L);
  }

  public long getMaxVersion(@Nonnull final String urn, @Nonnull final String aspectName) {
    SimpleStatement ss = selectFrom(DatastaxAspect.TABLE_NAME)
      .all()
      .whereColumn(DatastaxAspect.URN_COLUMN)
      .isEqualTo(literal(urn))
      .whereColumn(DatastaxAspect.ASPECT_COLUMN)
      .isEqualTo(literal(aspectName))
      .build();

    ResultSet rs = _cqlSession.execute(ss);
    List<Row> rows = rs.all();

    return getMaxVersion(rows);
  }

  private long getMaxVersion(List<Row> rows) {
    long maxVersion = -1;
    for (Row r : rows) {
      if (r.getLong(DatastaxAspect.VERSION_COLUMN) > maxVersion) {
        maxVersion = r.getLong(DatastaxAspect.VERSION_COLUMN);
      }
    }
    return maxVersion;
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

  public long batchSaveLatestAspect(
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
          final int nextVersion
  ) {
    BatchStatementBuilder batch = BatchStatement.builder(BatchType.LOGGED);
    DatastaxAspect oldDatastaxAspect = new DatastaxAspect(
            urn, aspectName, nextVersion, oldAspectMetadata, oldSystemMetadata, oldTime, oldActor, oldImpersonator);

    if (oldAspectMetadata != null && oldTime != null) {
      // Save oldValue as nextVersion
      batch = batch.addStatement(createCondInsertStatement(oldDatastaxAspect).build());
    }

    // Save newValue as the latest version (v0)
    DatastaxAspect newDatastaxAspect = new DatastaxAspect(urn, aspectName, 0, newAspectMetadata, newSystemMetadata, newTime, newActor, newImpersonator);

    if (nextVersion == 0)  {
      batch = batch.addStatement(createCondInsertStatement(newDatastaxAspect).build());
    } else {
      // We don't need to add a condition here as the conditional insert will fail if another thread has updated the
      // aspect in the meantime
      batch = batch.addStatement(createUpdateStatement(newDatastaxAspect, oldDatastaxAspect).build());
    }

    ResultSet rs = _cqlSession.execute(batch.build());
    if (!rs.wasApplied()) {
      throw new ConditionalWriteFailedException("Conditional entity ingestion failed");
    }

    // Apply retention policy
    applyRetention(urn, aspectName, getRetention(aspectName), nextVersion);

    return nextVersion;
  }

  private void applyRetention(
      @Nonnull final String urn,
      @Nonnull final String aspectName,
      @Nonnull final Retention retention,
      long largestVersion) {
    if (retention instanceof IndefiniteRetention) {
      return;
    }

    if (retention instanceof VersionBasedRetention) {
      applyVersionBasedRetention(urn, aspectName, (VersionBasedRetention) retention, largestVersion);
      return;
    }

    if (retention instanceof TimeBasedRetention) {
      applyTimeBasedRetention(urn, aspectName, (TimeBasedRetention) retention, _clock.millis());
    }
  }

  protected void applyVersionBasedRetention(
      @Nonnull final String urn,
      @Nonnull final String aspectName,
      @Nonnull final VersionBasedRetention retention,
      long largestVersion) {

    SimpleStatement ss = deleteFrom(DatastaxAspect.TABLE_NAME)
      .whereColumn(DatastaxAspect.URN_COLUMN).isEqualTo(literal(urn))
      .whereColumn(DatastaxAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
      .whereColumn(DatastaxAspect.VERSION_COLUMN).isNotEqualTo(literal(0))
      .whereColumn(DatastaxAspect.VERSION_COLUMN).isLessThanOrEqualTo(literal(largestVersion - retention.getMaxVersionsToRetain() + 1))
      .build();

    _cqlSession.execute(ss);
  }

  protected void applyTimeBasedRetention(
      @Nonnull final String urn,
      @Nonnull final String aspectName,
      @Nonnull final TimeBasedRetention retention,
      long currentTime) {
    SimpleStatement ss = deleteFrom(DatastaxAspect.TABLE_NAME)
      .whereColumn(DatastaxAspect.URN_COLUMN).isEqualTo(literal(urn))
      .whereColumn(DatastaxAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
      .whereColumn(DatastaxAspect.CREATED_ON_COLUMN).isLessThanOrEqualTo(literal(new Timestamp(currentTime - retention.getMaxAgeToRetain())))
      .build();

    _cqlSession.execute(ss);
  }


  @Nonnull
  public Retention getRetention(@Nonnull final String aspectName) {
    return _aspectRetentionMap.getOrDefault(aspectName, INDEFINITE_RETENTION);
  }

  @Nonnull
  public <T> T runInConditionalWithRetry(@Nonnull final Supplier<T> block, final int maxConditionalRetry) {
    int retryCount = 0;
    Exception lastException;

    T result = null;
    do {
      try {
        result = block.get();
        lastException = null;
        break;
      } catch (ConditionalWriteFailedException exception) {
        lastException = exception;
      }
    } while (++retryCount <= maxConditionalRetry);

    if (lastException != null) {
      throw new RetryLimitReached("Failed to add after " + maxConditionalRetry + " retries", lastException);
    }

    return result;
  }

  public ResultSet updateSystemMetadata(@Nonnull DatastaxAspect datastaxAspect) {

      Update u = update(DatastaxAspect.TABLE_NAME)
              .setColumn(DatastaxAspect.SYSTEM_METADATA_COLUMN, literal(datastaxAspect.getSystemMetadata()))
              .whereColumn(DatastaxAspect.URN_COLUMN).isEqualTo(literal(datastaxAspect.getUrn()))
              .whereColumn(DatastaxAspect.ASPECT_COLUMN).isEqualTo(literal(datastaxAspect.getAspect()))
              .whereColumn(DatastaxAspect.VERSION_COLUMN).isEqualTo(literal(datastaxAspect.getVersion()));

      return _cqlSession.execute(u.build());
    }

  public ResultSet condUpsertAspect(DatastaxAspect datastaxAspect, DatastaxAspect oldDatastaxAspect) {

    String entity;

    try {
      entity = (new Urn(datastaxAspect.getUrn())).getEntityType();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    if (oldDatastaxAspect == null) {
      return _cqlSession.execute(createCondInsertStatement(datastaxAspect).build());
    } else {
      Update u = update(DatastaxAspect.TABLE_NAME)
              .setColumn(DatastaxAspect.METADATA_COLUMN, literal(datastaxAspect.getMetadata()))
              .setColumn(DatastaxAspect.SYSTEM_METADATA_COLUMN, literal(datastaxAspect.getSystemMetadata()))
              .setColumn(DatastaxAspect.CREATED_ON_COLUMN, literal(datastaxAspect.getCreatedOn() == null ? null : datastaxAspect.getCreatedOn().getTime()))
              .setColumn(DatastaxAspect.CREATED_BY_COLUMN, literal(datastaxAspect.getCreatedBy()))
              .setColumn(DatastaxAspect.ENTITY_COLUMN, literal(entity))
              .setColumn(DatastaxAspect.CREATED_FOR_COLUMN, literal(datastaxAspect.getCreatedFor()))
              .whereColumn(DatastaxAspect.URN_COLUMN).isEqualTo(literal(datastaxAspect.getUrn()))
              .whereColumn(DatastaxAspect.ASPECT_COLUMN).isEqualTo(literal(datastaxAspect.getAspect()))
              .whereColumn(DatastaxAspect.VERSION_COLUMN).isEqualTo(literal(datastaxAspect.getVersion()));

      return _cqlSession.execute(u.build());
    }
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
      .stream().map(this::toDatastaxAspect)
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

  @Nullable
  public Optional<DatastaxAspect> getEarliestAspect(@Nonnull final String urn) {
    SimpleStatement ss = selectFrom(DatastaxAspect.TABLE_NAME)
      .all()
      .whereColumn(DatastaxAspect.URN_COLUMN).isEqualTo(literal(urn))
      .build();

    ResultSet rs = _cqlSession.execute(ss);
    List<DatastaxAspect> das = rs.all().stream().map(this::toDatastaxAspect).collect(Collectors.toList());

    if (das.size() == 0) {
      return Optional.empty();
    }

    return das.stream().reduce((d1, d2) -> d1.getCreatedOn().before(d2.getCreatedOn()) ? d1 : d2);
  }

  public List<DatastaxAspect> getAllAspects(String urn, String aspectName) {
    SimpleStatement ss = selectFrom(DatastaxAspect.TABLE_NAME)
            .all()
            .whereColumn(DatastaxAspect.URN_COLUMN).isEqualTo(literal(urn))
            .whereColumn(DatastaxAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
            .build();

    ResultSet rs = _cqlSession.execute(ss);

    return rs.all().stream().map(this::toDatastaxAspect).collect(Collectors.toList());
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

    return toDatastaxAspect(r);
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
            .stream().map(r -> toDatastaxAspect(r).getUrn())
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

  private DatastaxAspect toDatastaxAspect(Row r) {
    return new DatastaxAspect(
            r.getString(DatastaxAspect.URN_COLUMN),
            r.getString(DatastaxAspect.ASPECT_COLUMN),
            r.getLong(DatastaxAspect.VERSION_COLUMN),
            r.getString(DatastaxAspect.METADATA_COLUMN),
            r.getString(DatastaxAspect.SYSTEM_METADATA_COLUMN),
            r.getInstant(DatastaxAspect.CREATED_ON_COLUMN) == null ? null : Timestamp.from(r.getInstant(DatastaxAspect.CREATED_ON_COLUMN)),
            r.getString(DatastaxAspect.CREATED_BY_COLUMN),
            r.getString(DatastaxAspect.CREATED_FOR_COLUMN));
  }
}