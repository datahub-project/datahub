package com.linkedin.metadata.timeseries.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.TimeseriesAspectServiceConfig;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.timeseries.BatchWriteOperationsOptions;
import com.linkedin.metadata.timeseries.GenericTimeseriesDocument;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.TimeseriesScrollResult;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder;
import com.linkedin.metadata.timeseries.write.AbstractTimeseriesAspectWriteSink;
import com.linkedin.metadata.timeseries.write.AbstractTimeseriesAspectWriteSink.TimeseriesAspectRowPayload;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.DeleteAspectValuesResult;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.TimeseriesIndexSizeResult;
import io.datahubproject.metadata.context.OperationContext;
import io.ebean.Database;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * PostgreSQL-backed {@link TimeseriesAspectService} for {@code {prefix}_aspect_row} (see SqlSetup
 * pgTimeseries).
 */
@Slf4j
public class PostgresTimeseriesAspectService implements TimeseriesAspectService {

  @Nonnull private final Database database;
  @Nonnull private final PostgresSqlSetupProperties postgresSqlSetupProperties;
  @Nonnull private final TimeseriesAspectServiceConfig timeseriesAspectServiceConfig;
  @Nonnull private final QueryFilterRewriteChain queryFilterRewriteChain;
  @Nonnull private final EntityRegistry entityRegistry;
  @Nonnull private final PostgresTimeseriesAspectDao pgTimeseriesAspectDao;
  private final ExecutorService deleteExecutor =
      Executors.newCachedThreadPool(r -> new Thread(r, "pg-timeseries-delete"));

  public PostgresTimeseriesAspectService(
      @Nonnull Database database,
      @Nonnull PostgresSqlSetupProperties postgresSqlSetupProperties,
      @Nonnull TimeseriesAspectServiceConfig timeseriesAspectServiceConfig,
      @Nonnull QueryFilterRewriteChain queryFilterRewriteChain,
      @Nonnull EntityRegistry entityRegistry) {
    this.database = database;
    this.postgresSqlSetupProperties = postgresSqlSetupProperties;
    this.timeseriesAspectServiceConfig = timeseriesAspectServiceConfig;
    this.queryFilterRewriteChain = queryFilterRewriteChain;
    this.entityRegistry = entityRegistry;
    this.pgTimeseriesAspectDao =
        new PostgresTimeseriesAspectDao(database, postgresSqlSetupProperties);
  }

  @Nonnull
  private String qualifiedTable() {
    return pgTimeseriesAspectDao.qualifiedTable();
  }

  @Override
  public long countByFilter(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nullable Filter filter) {
    TimeseriesFilterSqlBuilder.BuiltSql built =
        TimeseriesFilterSqlBuilder.buildDocumentFilter(
            filter,
            true,
            opContext.getEntityRegistry().getEntitySpec(entityName).getSearchableFieldTypes(),
            opContext,
            queryFilterRewriteChain);
    String sql =
        "SELECT COUNT(*) FROM "
            + qualifiedTable()
            + " WHERE entity_name = ? AND aspect_name = ? AND ("
            + built.getExpression()
            + ")";
    List<Object> params = new ArrayList<>();
    params.add(entityName);
    params.add(aspectName);
    params.addAll(built.getParams());
    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      bind(ps, params);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return rs.getLong(1);
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException("PostgreSQL timeseries count failed", e);
    }
    return 0;
  }

  @Nonnull
  @Override
  public List<EnvelopedAspect> getAspectValues(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis,
      @Nullable Integer limit,
      @Nullable Filter filter,
      @Nullable SortCriterion sort) {

    TimeseriesFilterSqlBuilder.BuiltSql built =
        TimeseriesFilterSqlBuilder.buildDocumentFilter(
            filter,
            true,
            opContext.getEntityRegistry().getEntitySpec(entityName).getSearchableFieldTypes(),
            opContext,
            queryFilterRewriteChain);

    List<Object> params = new ArrayList<>();
    StringBuilder where = new StringBuilder();
    where.append("entity_name = ? AND aspect_name = ? AND urn = ?");
    params.add(entityName);
    params.add(aspectName);
    params.add(urn.toString());
    where.append(" AND (").append(built.getExpression()).append(")");
    params.addAll(built.getParams());

    if (startTimeMillis != null) {
      where.append(" AND (document->>'timestampMillis')::bigint >= ?");
      params.add(startTimeMillis);
    }
    if (endTimeMillis != null) {
      where.append(" AND (document->>'timestampMillis')::bigint <= ?");
      params.add(endTimeMillis);
    }

    String orderBy = sort != null ? orderByClause(sort) : "event_time DESC, message_id DESC";

    int lim = ConfigUtils.applyLimit(timeseriesAspectServiceConfig, limit);
    String sql =
        "SELECT event, system_metadata, document FROM "
            + qualifiedTable()
            + " WHERE "
            + where
            + " ORDER BY "
            + orderBy
            + " LIMIT ?";
    params.add(lim);

    List<EnvelopedAspect> out = new ArrayList<>();
    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      bind(ps, params);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          out.add(TimeseriesPgDocumentMapper.envelopedAspectFromRow(opContext, rs, true));
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException("PostgreSQL getAspectValues failed", e);
    }
    return out;
  }

  private static String orderByClause(SortCriterion sort) {
    String f = TimeseriesPgJsonPaths.stripKeywordSuffix(sort.getField());
    if (MappingsBuilder.TIMESTAMP_MILLIS_FIELD.equals(f) || "@timestamp".equals(f)) {
      return "event_time " + (sort.getOrder() == SortOrder.ASCENDING ? "ASC" : "DESC");
    }
    String path = PostgresTimeseriesAggregatedStatsDao.documentTextPathSql(f);
    return path + " " + (sort.getOrder() == SortOrder.ASCENDING ? "ASC" : "DESC");
  }

  @Nonnull
  @Override
  public Map<Urn, Map<String, EnvelopedAspect>> getLatestTimeseriesAspectValues(
      @Nonnull OperationContext opContext,
      @Nonnull Set<Urn> urns,
      @Nonnull Set<String> aspectNames,
      @Nullable Map<String, Long> endTimeMillis) {

    Map<Urn, Map<String, EnvelopedAspect>> result = new HashMap<>();
    for (Urn urn : urns) {
      String entityName = urn.getEntityType();
      Map<String, EnvelopedAspect> perAspect = new HashMap<>();
      for (String aspectName : aspectNames) {
        AspectSpec asp = entityRegistry.getEntitySpec(entityName).getAspectSpec(aspectName);
        if (asp == null || !asp.isTimeseries()) {
          continue;
        }
        Long end = endTimeMillis == null ? null : endTimeMillis.get(aspectName);
        List<EnvelopedAspect> one =
            getAspectValues(opContext, urn, entityName, aspectName, null, end, 1, null, null);
        if (!one.isEmpty()) {
          perAspect.put(aspectName, one.get(0));
        }
      }
      if (!perAspect.isEmpty()) {
        result.put(urn, perAspect);
      }
    }
    return result;
  }

  @Nonnull
  @Override
  public GenericTable getAggregatedStats(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull AggregationSpec[] aggregationSpecs,
      @Nullable Filter filter,
      @Nullable GroupingBucket[] groupingBuckets) {

    TimeseriesFilterSqlBuilder.BuiltSql built =
        TimeseriesFilterSqlBuilder.buildDocumentFilter(
            filter,
            true,
            opContext.getEntityRegistry().getEntitySpec(entityName).getSearchableFieldTypes(),
            opContext,
            queryFilterRewriteChain);
    AspectSpec aspectSpec =
        opContext.getEntityRegistry().getEntitySpec(entityName).getAspectSpec(aspectName);
    try (Connection c = database.dataSource().getConnection()) {
      return PostgresTimeseriesAggregatedStatsDao.getAggregatedStats(
          c,
          qualifiedTable(),
          entityName,
          aspectName,
          aspectSpec,
          aggregationSpecs,
          groupingBuckets,
          built);
    } catch (SQLException e) {
      throw new IllegalStateException("PostgreSQL getAggregatedStats failed", e);
    }
  }

  @Nonnull
  @Override
  public DeleteAspectValuesResult deleteAspectValues(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull Filter filter) {

    TimeseriesFilterSqlBuilder.BuiltSql built =
        TimeseriesFilterSqlBuilder.buildDocumentFilter(
            filter,
            true,
            opContext.getEntityRegistry().getEntitySpec(entityName).getSearchableFieldTypes(),
            opContext,
            queryFilterRewriteChain);
    String sql =
        "DELETE FROM "
            + qualifiedTable()
            + " WHERE entity_name = ? AND aspect_name = ? AND ("
            + built.getExpression()
            + ")";
    List<Object> params = new ArrayList<>();
    params.add(entityName);
    params.add(aspectName);
    params.addAll(built.getParams());
    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      bind(ps, params);
      int deleted = ps.executeUpdate();
      return new DeleteAspectValuesResult().setNumDocsDeleted(deleted);
    } catch (SQLException e) {
      throw new IllegalStateException("PostgreSQL deleteAspectValues failed", e);
    }
  }

  @Nonnull
  @Override
  public String deleteAspectValuesAsync(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull Filter filter,
      @Nonnull BatchWriteOperationsOptions options) {
    String taskId = UUID.randomUUID().toString();
    deleteExecutor.submit(
        () -> {
          try {
            deleteAspectValues(opContext, entityName, aspectName, filter);
          } catch (Exception e) {
            log.error("Async PG timeseries delete failed: {}", e.toString(), e);
          }
        });
    return taskId;
  }

  @Override
  public String reindexAsync(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull Filter filter,
      @Nonnull BatchWriteOperationsOptions options) {
    throw new UnsupportedOperationException(
        "PostgreSQL timeseries service does not support reindex; use Elasticsearch operations.");
  }

  @Nonnull
  @Override
  public DeleteAspectValuesResult rollbackTimeseriesAspects(
      @Nonnull OperationContext opContext, @Nonnull String runId) {
    int total = 0;
    try (Connection c = database.dataSource().getConnection()) {
      for (var ent : opContext.getEntityRegistry().getEntitySpecs().entrySet()) {
        for (AspectSpec asp : ent.getValue().getAspectSpecs()) {
          if (!asp.isTimeseries()) {
            continue;
          }
          String sql =
              "DELETE FROM "
                  + qualifiedTable()
                  + " WHERE entity_name = ? AND aspect_name = ? AND run_id = ?";
          try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, ent.getKey());
            ps.setString(2, asp.getName());
            ps.setString(3, runId);
            total += ps.executeUpdate();
          }
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException("PostgreSQL rollbackTimeseriesAspects failed", e);
    }
    return new DeleteAspectValuesResult().setNumDocsDeleted(total);
  }

  @Override
  public void upsertDocument(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull String docId,
      @Nonnull JsonNode document) {
    TimeseriesAspectRowPayload row =
        AbstractTimeseriesAspectWriteSink.parsePayload(entityName, aspectName, docId, document);
    try {
      pgTimeseriesAspectDao.upsert(row);
    } catch (SQLException e) {
      throw new IllegalStateException("PostgreSQL timeseries upsert failed", e);
    }
  }

  @Override
  public void deleteDocument(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull String docId,
      @SuppressWarnings("unused") boolean isExploded) {
    String messageId = AbstractTimeseriesAspectWriteSink.resolveMessageId(docId, null);
    try {
      pgTimeseriesAspectDao.deleteByMessageId(entityName, aspectName, messageId);
    } catch (SQLException e) {
      throw new IllegalStateException("PostgreSQL timeseries deleteDocument failed", e);
    }
  }

  @Override
  public List<TimeseriesIndexSizeResult> getIndexSizes(@Nonnull OperationContext opContext) {
    List<TimeseriesIndexSizeResult> out = new ArrayList<>();
    String table = qualifiedTable();
    String sql = "SELECT pg_total_relation_size(?::regclass)";
    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, table);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          TimeseriesIndexSizeResult r = new TimeseriesIndexSizeResult();
          r.setIndexName(table);
          r.setEntityName("*");
          r.setAspectName("*");
          r.setSizeInMb(rs.getLong(1) / 1_000_000.0);
          out.add(r);
        }
      }
    } catch (SQLException e) {
      log.warn("Could not read pg_total_relation_size for {}: {}", table, e.toString());
    }
    return out;
  }

  @Nonnull
  @Override
  public TimeseriesScrollResult scrollAspects(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nullable Filter filter,
      @Nonnull List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable Integer count,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis) {

    TimeseriesFilterSqlBuilder.BuiltSql built =
        TimeseriesFilterSqlBuilder.buildDocumentFilter(
            filter,
            true,
            opContext.getEntityRegistry().getEntitySpec(entityName).getSearchableFieldTypes(),
            opContext,
            queryFilterRewriteChain);

    ScrollCursor cursor = ScrollCursor.decode(scrollId);
    int lim = ConfigUtils.applyLimit(timeseriesAspectServiceConfig, count);

    StringBuilder where = new StringBuilder();
    List<Object> params = new ArrayList<>();
    where.append("entity_name = ? AND aspect_name = ?");
    params.add(entityName);
    params.add(aspectName);
    where.append(" AND (").append(built.getExpression()).append(")");
    params.addAll(built.getParams());
    if (startTimeMillis != null) {
      where.append(" AND (document->>'timestampMillis')::bigint >= ?");
      params.add(startTimeMillis);
    }
    if (endTimeMillis != null) {
      where.append(" AND (document->>'timestampMillis')::bigint <= ?");
      params.add(endTimeMillis);
    }
    if (cursor != null) {
      where.append(" AND (event_time, message_id) < (?, ?)");
      params.add(cursor.eventTime());
      params.add(cursor.messageId());
    }

    String orderBy;
    if (sortCriteria.isEmpty()) {
      orderBy = "event_time DESC, message_id DESC";
    } else {
      List<String> sorts = new ArrayList<>();
      for (SortCriterion sc : sortCriteria) {
        sorts.add(sortCriterionToSql(sc));
      }
      orderBy = String.join(", ", sorts);
    }

    String sql =
        "SELECT event, system_metadata, document, event_time, message_id FROM "
            + qualifiedTable()
            + " WHERE "
            + where
            + " ORDER BY "
            + orderBy
            + " LIMIT ?";
    params.add(lim + 1);

    List<EnvelopedAspect> events = new ArrayList<>();
    List<GenericTimeseriesDocument> docs = new ArrayList<>();
    java.sql.Timestamp lastEventTime = null;
    String lastMessageId = null;
    boolean hasMore = false;
    try (Connection c = database.dataSource().getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      bind(ps, params);
      try (ResultSet rs = ps.executeQuery()) {
        int n = 0;
        while (rs.next()) {
          n++;
          if (n > lim) {
            hasMore = true;
            break;
          }
          lastEventTime = rs.getTimestamp("event_time");
          lastMessageId = rs.getString("message_id");
          events.add(TimeseriesPgDocumentMapper.envelopedAspectFromRow(opContext, rs, true));
          docs.add(parseGenericDoc(opContext.getObjectMapper(), rs.getString("document")));
        }
      }
    } catch (SQLException | JsonProcessingException e) {
      throw new IllegalStateException("scrollAspects failed", e);
    }

    String nextScroll = null;
    if (hasMore && lastEventTime != null && lastMessageId != null) {
      nextScroll = ScrollCursor.encode(lastEventTime, lastMessageId);
    }
    return TimeseriesScrollResult.builder()
        .numResults(events.size() + (hasMore ? 1 : 0))
        .pageSize(events.size())
        .scrollId(nextScroll)
        .events(events)
        .documents(docs)
        .build();
  }

  private static GenericTimeseriesDocument parseGenericDoc(ObjectMapper mapper, String docJson)
      throws JsonProcessingException {
    if (docJson == null) {
      return GenericTimeseriesDocument.builder()
          .urn("")
          .timestampMillis(0L)
          .timestamp(0L)
          .event(Map.of())
          .build();
    }
    Map<String, Object> m = mapper.readValue(docJson, new TypeReference<Map<String, Object>>() {});
    return GenericTimeseriesDocument.builder()
        .urn(Objects.toString(m.get(MappingsBuilder.URN_FIELD), ""))
        .timestampMillis(toLong(m.get(MappingsBuilder.TIMESTAMP_MILLIS_FIELD)))
        .timestamp(toLong(m.get(MappingsBuilder.TIMESTAMP_FIELD)))
        .event(m.get(MappingsBuilder.EVENT_FIELD))
        .messageId(Objects.toString(m.get(MappingsBuilder.MESSAGE_ID_FIELD), null))
        .systemMetadata(m.get(MappingsBuilder.SYSTEM_METADATA_FIELD))
        .build();
  }

  private static long toLong(Object o) {
    if (o instanceof Number) {
      return ((Number) o).longValue();
    }
    return 0L;
  }

  private static String sortCriterionToSql(SortCriterion sc) {
    String f = TimeseriesPgJsonPaths.stripKeywordSuffix(sc.getField());
    if (MappingsBuilder.TIMESTAMP_MILLIS_FIELD.equals(f) || "@timestamp".equals(f)) {
      return "event_time " + (sc.getOrder() == SortOrder.ASCENDING ? "ASC" : "DESC");
    }
    return PostgresTimeseriesAggregatedStatsDao.documentTextPathSql(f)
        + " "
        + (sc.getOrder() == SortOrder.ASCENDING ? "ASC" : "DESC");
  }

  /** Keyset cursor for scroll (PostgreSQL tuple comparison). */
  private static final class ScrollCursor {
    private final java.sql.Timestamp eventTime;
    private final String messageId;

    private ScrollCursor(java.sql.Timestamp eventTime, String messageId) {
      this.eventTime = eventTime;
      this.messageId = messageId;
    }

    java.sql.Timestamp eventTime() {
      return eventTime;
    }

    String messageId() {
      return messageId;
    }

    static ScrollCursor decode(String scrollId) {
      if (scrollId == null || scrollId.isBlank()) {
        return null;
      }
      try {
        String json = new String(Base64.getUrlDecoder().decode(scrollId), StandardCharsets.UTF_8);
        ObjectMapper om = new ObjectMapper();
        JsonNode n = om.readTree(json);
        return new ScrollCursor(
            java.sql.Timestamp.from(java.time.Instant.ofEpochMilli(n.get("t").asLong())),
            n.get("m").asText());
      } catch (Exception e) {
        return null;
      }
    }

    static String encode(java.sql.Timestamp eventTime, String messageId) {
      try {
        ObjectMapper om = new ObjectMapper();
        ObjectNode o = om.createObjectNode();
        o.put("t", eventTime.getTime());
        o.put("m", messageId);
        return Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(o.toString().getBytes(StandardCharsets.UTF_8));
      } catch (Exception e) {
        return null;
      }
    }
  }

  @Override
  public Map<Urn, Map<String, Map<String, Object>>> raw(
      OperationContext opContext, Map<String, Set<String>> urnAspects) {
    Map<Urn, Map<String, Map<String, Object>>> result = new HashMap<>();
    ObjectMapper mapper = opContext.getObjectMapper();
    for (Map.Entry<String, Set<String>> e : urnAspects.entrySet()) {
      try {
        Urn urn = UrnUtils.getUrn(e.getKey());
        String entityName = urn.getEntityType();
        Map<String, Map<String, Object>> aspects = new HashMap<>();
        for (String aspectName : e.getValue()) {
          AspectSpec asp =
              opContext.getEntityRegistry().getEntitySpec(entityName).getAspectSpec(aspectName);
          if (asp == null || !asp.isTimeseries()) {
            continue;
          }
          String sql =
              "SELECT document FROM "
                  + qualifiedTable()
                  + " WHERE entity_name = ? AND aspect_name = ? AND urn = ? "
                  + " ORDER BY event_time DESC LIMIT 1";
          try (Connection c = database.dataSource().getConnection();
              PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, entityName);
            ps.setString(2, aspectName);
            ps.setString(3, urn.toString());
            try (ResultSet rs = ps.executeQuery()) {
              if (rs.next()) {
                Map<String, Object> doc =
                    TimeseriesPgDocumentMapper.rawDocumentMap(mapper, rs.getString(1));
                if (doc != null) {
                  aspects.put(aspectName, doc);
                }
              }
            }
          }
        }
        if (!aspects.isEmpty()) {
          result.put(urn, aspects);
        }
      } catch (Exception ex) {
        log.warn("raw() failed for urn {}: {}", e.getKey(), ex.toString());
      }
    }
    return result;
  }

  private static void bind(PreparedStatement ps, List<Object> params) throws SQLException {
    PostgresPreparedBinder.bind(ps, params);
  }
}
