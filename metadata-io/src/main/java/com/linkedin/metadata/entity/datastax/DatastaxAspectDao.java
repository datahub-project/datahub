package com.linkedin.metadata.entity.datastax;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
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
import com.linkedin.metadata.dao.retention.IndefiniteRetention;
import com.linkedin.metadata.dao.retention.Retention;
import com.linkedin.metadata.dao.retention.TimeBasedRetention;
import com.linkedin.metadata.dao.retention.VersionBasedRetention;
import com.linkedin.metadata.dao.utils.QueryUtils;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.ExtraInfoArray;
import com.linkedin.metadata.query.ListResultMetadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.time.Clock;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;

import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;

@Slf4j
public class DatastaxAspectDao {
  protected final CqlSession _cqlSession;
  private static final IndefiniteRetention INDEFINITE_RETENTION = new IndefiniteRetention();
  private final Map<String, Retention> _aspectRetentionMap = new HashMap<>();
  private final Clock _clock = Clock.systemUTC();
  private int _queryKeysCount = 0; // 0 means no pagination on keys

  public DatastaxAspectDao(@Nonnull final Map<String, String> sessionConfig) {

    Integer port = Integer.parseInt(sessionConfig.get("port"));
    List<InetSocketAddress> addresses = Arrays.asList(sessionConfig.get("hosts").split(","))
      .stream()
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

  @Nullable
  public long getMaxVersion(@Nonnull final String urn, @Nonnull final String aspectName) {
    SimpleStatement ss = selectFrom("metadata_aspect_v2")
      .all()
      .whereColumn("urn")
      .isEqualTo(literal(urn))
      .whereColumn("aspect")
      .isEqualTo(literal(aspectName))
      .build();

    ResultSet rs = _cqlSession.execute(ss);
    List<Row> rows = rs.all();

    return getMaxVersion(rows);
  }

  private long getMaxVersion(List<Row> rows) {
    long maxVersion = -1;
    for (Row r : rows) {
      if (r.getLong("version") > maxVersion) {
        maxVersion = r.getLong("version");
      }
    }
    return maxVersion;
  }

  private long getNextVersion(@Nonnull final String urn, @Nonnull final String aspectName) {
    SimpleStatement ss = selectFrom("metadata_aspect_v2")
      .all()
      .whereColumn("urn")
      .isEqualTo(literal(urn))
      .whereColumn("aspect")
      .isEqualTo(literal(aspectName))
      .build();

    ResultSet rs = _cqlSession.execute(ss);
    List<Row> rows = rs.all();

    return getMaxVersion(rows) + 1;
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
      @Nullable final String newSystemMetadata
) {
    // Save oldValue as the largest version + 1
    long largestVersion = 0;
    if (oldAspectMetadata != null && oldTime != null) {
      largestVersion = getNextVersion(urn, aspectName);
      insertAspect(new DatastaxAspect(urn, aspectName, largestVersion, oldAspectMetadata, oldSystemMetadata, oldTime, oldActor, oldImpersonator));
    }

    // Save newValue as the latest version (v0)
    ResultSet rs = updateAspect(new DatastaxAspect(urn, aspectName, 0, newAspectMetadata, newSystemMetadata, newTime, newActor, newImpersonator),
            new DatastaxAspect(urn, aspectName, 0, oldAspectMetadata, oldSystemMetadata, oldTime, oldActor, oldImpersonator));

    // Apply retention policy
    applyRetention(urn, aspectName, getRetention(aspectName), largestVersion);

    return largestVersion;
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
      return;
    }
  }

  protected void applyVersionBasedRetention(
      @Nonnull final String urn,
      @Nonnull final String aspectName,
      @Nonnull final VersionBasedRetention retention,
      long largestVersion) {

    SimpleStatement ss = deleteFrom("metadata_aspect_v2")
      .whereColumn("urn").isEqualTo(literal(urn))
      .whereColumn("aspect").isEqualTo(literal(aspectName))
      .whereColumn("version").isNotEqualTo(literal(0))
      .whereColumn("version").isLessThanOrEqualTo(literal(largestVersion - retention.getMaxVersionsToRetain() + 1))
      .build();

    _cqlSession.execute(ss);
  }

  protected void applyTimeBasedRetention(
      @Nonnull final String urn,
      @Nonnull final String aspectName,
      @Nonnull final TimeBasedRetention retention,
      long currentTime) {
    SimpleStatement ss = deleteFrom("metadata_aspect_v2")
      .whereColumn("urn").isEqualTo(literal(urn))
      .whereColumn("aspect").isEqualTo(literal(aspectName))
      .whereColumn("created_on").isLessThanOrEqualTo(literal(new Timestamp(currentTime - retention.getMaxAgeToRetain())))
      .build();

    _cqlSession.execute(ss);
  }


  @Nonnull
  public Retention getRetention(@Nonnull final String aspectName) {
    return _aspectRetentionMap.getOrDefault(aspectName, INDEFINITE_RETENTION);
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

    saveAspect(new DatastaxAspect(urn, aspectName, version, aspectMetadata, systemMetadata, timestamp, actor, impersonator != null ? impersonator : null
), insert);
  }

  private ResultSet insertAspect(DatastaxAspect datastaxAspect) {

    String entity;

    try {
      entity = (new Urn(datastaxAspect.getUrn())).getEntityType();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

      Insert ri = insertInto("metadata_aspect_v2")
        .value("urn", literal(datastaxAspect.getUrn()))
        .value("aspect", literal(datastaxAspect.getAspect()))
        .value("version", literal(datastaxAspect.getVersion()))
        .value("systemmetadata", literal(datastaxAspect.getSystemMetadata()))
        .value("metadata", literal(datastaxAspect.getMetadata()))
        .value("createdon", literal(datastaxAspect.getCreatedOn().getTime()))
        .value("createdfor", literal(datastaxAspect.getCreatedfor()))
        .value("entity", literal(entity))
        .value("createdby", literal(datastaxAspect.getCreatedBy()))
        .ifNotExists();
      return _cqlSession.execute(ri.build());
  }

  public ResultSet updateAspect(DatastaxAspect datastaxAspect, DatastaxAspect oldDatastaxAspect) {

    String entity;

    try {
      entity = (new Urn(datastaxAspect.getUrn())).getEntityType();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    Update u = update("metadata_aspect_v2")
      .setColumn("metadata", literal(datastaxAspect.getMetadata()))
      .setColumn("systemmetadata", literal(datastaxAspect.getSystemMetadata()))
      .setColumn("createdon", literal(datastaxAspect.getCreatedOn() == null ? null : datastaxAspect.getCreatedOn().getTime()))
      .setColumn("createdby", literal(datastaxAspect.getCreatedBy()))
      .setColumn("entity", literal(entity))
      .setColumn("createdfor", literal(datastaxAspect.getCreatedfor()))
      .whereColumn("urn").isEqualTo(literal(datastaxAspect.getUrn()))
      .whereColumn("aspect").isEqualTo(literal(datastaxAspect.getAspect()))
      .whereColumn("version").isEqualTo(literal(datastaxAspect.getVersion()));

    return _cqlSession.execute(u.build());
  }

  public void saveAspect(DatastaxAspect datastaxAspect, final boolean insert) {

    String entity;

    try {
      entity = (new Urn(datastaxAspect.getUrn())).getEntityType();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    if (insert) {
      Insert ri = insertInto("metadata_aspect_v2")
        .value("urn", literal(datastaxAspect.getUrn()))
        .value("aspect", literal(datastaxAspect.getAspect()))
        .value("version", literal(datastaxAspect.getVersion()))
        .value("systemmetadata", literal(datastaxAspect.getSystemMetadata()))
        .value("metadata", literal(datastaxAspect.getMetadata()))
        .value("createdon", literal(datastaxAspect.getCreatedOn().getTime()))
        .value("createdfor", literal(datastaxAspect.getCreatedfor()))
        .value("entity", literal(entity))
        .value("createdby", literal(datastaxAspect.getCreatedBy()));
      _cqlSession.execute(ri.build());
    } else {

      UpdateWithAssignments uwa = update("metadata_aspect_v2")
        .setColumn("metadata", literal(datastaxAspect.getMetadata()))
        .setColumn("systemmetadata", literal(datastaxAspect.getSystemMetadata()))
        .setColumn("createdon", literal(datastaxAspect.getCreatedOn().getTime()))
        .setColumn("createdby", literal(datastaxAspect.getCreatedBy()))
        .setColumn("createdfor", literal(datastaxAspect.getCreatedfor()));

      Update u = uwa.whereColumn("urn").isEqualTo(literal(datastaxAspect.getUrn()))
        .whereColumn("aspect").isEqualTo(literal(datastaxAspect.getAspect()))
        .whereColumn("version").isEqualTo(literal(datastaxAspect.getVersion()));

      _cqlSession.execute(u.build());
    }
  }

  @Nonnull
  public Map<DatastaxAspect.PrimaryKey, DatastaxAspect> batchGet(@Nonnull final Set<DatastaxAspect.PrimaryKey> keys) {
    if (keys.isEmpty()) {
      return Collections.emptyMap();
    }

    final List<DatastaxAspect> records;
    if (_queryKeysCount == 0) {
      records = batchGet(keys, keys.size());
    } else {
      records = batchGet(keys, _queryKeysCount);
    }
    return records.stream().collect(Collectors.toMap(DatastaxAspect::toPrimaryKey, record -> record));
  }

  @Nonnull
  private List<DatastaxAspect> batchGet(@Nonnull final Set<DatastaxAspect.PrimaryKey> keys, final int keysCount) {

    int position = 0;

    final int totalPageCount = QueryUtils.getTotalPageCount(keys.size(), keysCount);
    final List<DatastaxAspect> finalResult = batchGetOr(new ArrayList<>(keys), keysCount, position);

    while (QueryUtils.hasMore(position, keysCount, totalPageCount)) {
      position += keysCount;
      final List<DatastaxAspect> oneStatementResult = batchGetOr(new ArrayList<>(keys), keysCount, position);
      finalResult.addAll(oneStatementResult);
    }

    return finalResult;
  }

    @Nonnull
    private List<DatastaxAspect> batchGetOr(@Nonnull final ArrayList<DatastaxAspect.PrimaryKey> keys, int keysCount, int position) {

      final int end = Math.min(keys.size(), position + keysCount);
      final int start = position;

      final ArrayList<DatastaxAspect> aspects = new ArrayList<DatastaxAspect>();

      for (int index = start; index < end; index++) {
        final DatastaxAspect.PrimaryKey key = keys.get(index);

        final DatastaxAspect datastaxAspect = getAspect(key.getUrn(), key.getAspect(), key.getVersion());

        if (datastaxAspect != null) {
          aspects.add(datastaxAspect);
        }

      }

      return aspects;

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

    SimpleStatement ss = selectFrom("metadata_aspect_v2")
      .all()
      .whereColumn("aspect").isEqualTo(literal(aspectName))
      .whereColumn("version").isEqualTo(literal(version))
      .whereColumn("entity").isEqualTo(literal(entityName))
      .allowFiltering()
      .build();

    ResultSet rs = _cqlSession.execute(ss);

    int pageNumber = start / pageSize + 1;

    Page<Row> page = offsetPager.getPage(rs, pageNumber);

    final List<DatastaxAspect> aspects = page
      .getElements()
      .stream().map(r -> toDatastaxAspect(r))
      .collect(Collectors.toList());

    SimpleStatement ssCount = selectFrom("metadata_aspect_v2")
      .countAll()
      .whereColumn("aspect").isEqualTo(literal(aspectName))
      .whereColumn("version").isEqualTo(literal(version))
      .whereColumn("entity").isEqualTo(literal(entityName))
      .allowFiltering()
      .build();

    long totalCount = _cqlSession.execute(ssCount).one().getLong(0);

    return toListResult(aspects, start, pageNumber, pageSize, totalCount);
  }

  private ListResult<String> toListResult(
      @Nonnull final List<DatastaxAspect> dsas,
      @Nonnull final Integer start,
      @Nonnull final Integer pageNumber,
      @Nonnull final Integer pageSize,
      @Nonnull final long totalCount) {
    final int numPages = (int) (totalCount / pageSize + (totalCount % pageSize == 0 ? 0 : 1));
    final boolean hasNext = pageNumber < numPages;

    final int nextStart =
      (start != null && hasNext) ? (pageNumber * pageSize) : ListResult.INVALID_NEXT_START;

    final List<String> aspects = dsas
      .stream()
      .map(r -> r.getMetadata())
      .collect(Collectors.toList());

    final ListResultMetadata listResultMetadata = toListResultMetadata(dsas
                                                                       .stream()
                                                                       .map(r -> toExtraInfo(r))
                                                                       .collect(Collectors.toList()));

    return ListResult.<String>builder()
      .values(aspects)
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
      if (aspect.getCreatedfor() != null) {
        auditStamp.setImpersonator(new Urn(aspect.getCreatedfor()));
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return auditStamp;
  }

  @Nullable
  public boolean deleteAspect(@Nonnull final DatastaxAspect aspect) {
      SimpleStatement ss = deleteFrom("metadata_aspect_v2")
      .whereColumn("urn").isEqualTo(literal(aspect.getUrn()))
      .whereColumn("aspect").isEqualTo(literal(aspect.getAspect()))
      .whereColumn("version").isEqualTo(literal(aspect.getVersion()))
        .build();
    ResultSet rs = _cqlSession.execute(ss);

    return rs.getExecutionInfo().getErrors().size() == 0;
  }

  @Nullable
  public int deleteUrn(@Nonnull final String urn) {
      SimpleStatement ss = deleteFrom("metadata_aspect_v2")
      .whereColumn("urn").isEqualTo(literal(urn))
        .build();
    ResultSet rs = _cqlSession.execute(ss);
    // https://stackoverflow.com/questions/28611459/how-to-know-affected-rows-in-cassandracql
    return rs.getExecutionInfo().getErrors().size() == 0 ? -1 : 0;
  }

  @Nullable
  public Optional<DatastaxAspect> getEarliestAspect(@Nonnull final String urn) {
    SimpleStatement ss = selectFrom("metadata_aspect_v2")
      .all()
      .whereColumn("urn").isEqualTo(literal(urn))
      .build();

    ResultSet rs = _cqlSession.execute(ss);
    List<DatastaxAspect> das = rs.all().stream().map(r -> toDatastaxAspect(r)).collect(Collectors.toList());

    if (das.size() == 0) {
      return null;
    }

    return das.stream().reduce((d1, d2) -> d1.getCreatedOn().before(d2.getCreatedOn()) ? d1 : d2);
  }
  
  public DatastaxAspect getAspect(String urn, String aspectName, long version) {
    SimpleStatement ss = selectFrom("metadata_aspect_v2")
      .all()
      .whereColumn("urn").isEqualTo(literal(urn))
      .whereColumn("aspect").isEqualTo(literal(aspectName))
      .whereColumn("version").isEqualTo(literal(version))
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

    return null;
  }

  private DatastaxAspect toDatastaxAspect(Row r) {
    return new DatastaxAspect(r.getString("urn"), r.getString("aspect"), r.getLong("version"), r.getString("metadata"),
        r.getString("systemmetadata"), Timestamp.from(r.getInstant("createdon")), r.getString("createdby"),
        r.getString("createdfor"));
  }
}