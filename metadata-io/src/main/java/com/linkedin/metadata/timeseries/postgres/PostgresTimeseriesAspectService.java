package com.linkedin.metadata.timeseries.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * PostgreSQL-backed {@link TimeseriesAspectService} for {@code {prefix}_aspect} (see SqlSetup
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
      where.append(" AND event_time >= ?");
      params.add(java.sql.Timestamp.from(java.time.Instant.ofEpochMilli(startTimeMillis)));
    }
    if (endTimeMillis != null) {
      where.append(" AND event_time <= ?");
      params.add(java.sql.Timestamp.from(java.time.Instant.ofEpochMilli(endTimeMillis)));
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
    return deleteAspectValuesBatched(opContext, entityName, aspectName, filter, 0, 0L);
  }

  /**
   * Deletes matching rows in batches of {@code batchSize} (when &gt; 0). When {@code batchSize} is
   * ≤ 0, deletes in one statement. When {@code timeoutSeconds} &gt; 0, aborts if wall time exceeds
   * the limit between batches.
   */
  @Nonnull
  DeleteAspectValuesResult deleteAspectValuesBatched(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull Filter filter,
      int batchSize,
      long timeoutSeconds) {

    TimeseriesFilterSqlBuilder.BuiltSql built =
        TimeseriesFilterSqlBuilder.buildDocumentFilter(
            filter,
            true,
            opContext.getEntityRegistry().getEntitySpec(entityName).getSearchableFieldTypes(),
            opContext,
            queryFilterRewriteChain);
    List<Object> baseParams = new ArrayList<>();
    baseParams.add(entityName);
    baseParams.add(aspectName);
    baseParams.addAll(built.getParams());

    String table = qualifiedTable();
    long deadlineNanos =
        timeoutSeconds > 0
            ? System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds)
            : Long.MAX_VALUE;
    int totalDeleted = 0;
    try (Connection c = database.dataSource().getConnection()) {
      c.setAutoCommit(true);
      if (batchSize <= 0) {
        String sql =
            "DELETE FROM "
                + table
                + " WHERE entity_name = ? AND aspect_name = ? AND ("
                + built.getExpression()
                + ")";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
          bind(ps, baseParams);
          totalDeleted = ps.executeUpdate();
        }
      } else {
        // PostgreSQL DELETE has no LIMIT — delete via PK subquery batches.
        String sql =
            "DELETE FROM "
                + table
                + " WHERE (entity_name, aspect_name, message_id, event_time) IN ("
                + "SELECT entity_name, aspect_name, message_id, event_time FROM "
                + table
                + " WHERE entity_name = ? AND aspect_name = ? AND ("
                + built.getExpression()
                + ") LIMIT ?)";
        while (true) {
          if (System.nanoTime() > deadlineNanos) {
            throw new IllegalStateException(
                "PostgreSQL deleteAspectValues timed out after deleting "
                    + totalDeleted
                    + " rows (timeoutSeconds="
                    + timeoutSeconds
                    + ")");
          }
          List<Object> params = new ArrayList<>(baseParams);
          params.add(batchSize);
          int deleted;
          try (PreparedStatement ps = c.prepareStatement(sql)) {
            bind(ps, params);
            deleted = ps.executeUpdate();
          }
          totalDeleted += deleted;
          if (deleted < batchSize) {
            break;
          }
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException("PostgreSQL deleteAspectValues failed", e);
    }
    return new DeleteAspectValuesResult().setNumDocsDeleted(totalDeleted);
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
    int batchSize = options.getBatchSize();
    long timeoutSeconds = options.getTimeoutSeconds();
    Future<DeleteAspectValuesResult> future =
        deleteExecutor.submit(
            () ->
                deleteAspectValuesBatched(
                    opContext, entityName, aspectName, filter, batchSize, timeoutSeconds));
    try {
      if (timeoutSeconds > 0) {
        future.get(timeoutSeconds, TimeUnit.SECONDS);
      } else {
        future.get();
      }
    } catch (TimeoutException e) {
      future.cancel(true);
      throw new IllegalStateException(
          "PostgreSQL async timeseries delete timed out (taskId=" + taskId + ")", e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new IllegalStateException(
          "PostgreSQL async timeseries delete failed (taskId=" + taskId + ")", cause);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      future.cancel(true);
      throw new IllegalStateException(
          "PostgreSQL async timeseries delete interrupted (taskId=" + taskId + ")", e);
    }
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

  @Override
  public boolean supportsReindexForTruncate() {
    return false;
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
  public boolean applyDocumentDeleteOnMclDelete() {
    return true;
  }

  @Override
  public void deleteDocument(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull String docId,
      @Nullable JsonNode document,
      @SuppressWarnings("unused") boolean isExploded) {
    String messageId = AbstractTimeseriesAspectWriteSink.resolveMessageId(docId, document);
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

    List<SortKey> sortKeys = resolveSortKeys(sortCriteria);
    List<Object> cursorValues = ScrollCursor.decode(scrollId, sortKeys);
    int lim = ConfigUtils.applyLimit(timeseriesAspectServiceConfig, count);

    StringBuilder where = new StringBuilder();
    List<Object> params = new ArrayList<>();
    where.append("entity_name = ? AND aspect_name = ?");
    params.add(entityName);
    params.add(aspectName);
    where.append(" AND (").append(built.getExpression()).append(")");
    params.addAll(built.getParams());
    if (startTimeMillis != null) {
      where.append(" AND event_time >= ?");
      params.add(java.sql.Timestamp.from(java.time.Instant.ofEpochMilli(startTimeMillis)));
    }
    if (endTimeMillis != null) {
      where.append(" AND event_time <= ?");
      params.add(java.sql.Timestamp.from(java.time.Instant.ofEpochMilli(endTimeMillis)));
    }
    if (cursorValues != null) {
      appendKeysetPredicate(where, params, sortKeys, cursorValues);
    }

    String orderBy =
        sortKeys.stream()
            .map(k -> k.sqlExpr() + (k.ascending() ? " ASC" : " DESC"))
            .reduce((a, b) -> a + ", " + b)
            .orElse("event_time DESC, message_id DESC");

    StringBuilder selectKeys = new StringBuilder();
    for (int i = 0; i < sortKeys.size(); i++) {
      selectKeys.append(", ").append(sortKeys.get(i).sqlExpr()).append(" AS _sk").append(i);
    }

    String sql =
        "SELECT event, system_metadata, document, event_time, message_id"
            + selectKeys
            + " FROM "
            + qualifiedTable()
            + " WHERE "
            + where
            + " ORDER BY "
            + orderBy
            + " LIMIT ?";
    params.add(lim + 1);

    List<EnvelopedAspect> events = new ArrayList<>();
    List<GenericTimeseriesDocument> docs = new ArrayList<>();
    List<Object> lastSortValues = null;
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
          lastSortValues = readSortValues(rs, sortKeys);
          events.add(TimeseriesPgDocumentMapper.envelopedAspectFromRow(opContext, rs, true));
          docs.add(parseGenericDoc(opContext.getObjectMapper(), rs.getString("document")));
        }
      }
    } catch (SQLException | JsonProcessingException e) {
      throw new IllegalStateException("scrollAspects failed", e);
    }

    String nextScroll = null;
    if (hasMore && lastSortValues != null) {
      nextScroll = ScrollCursor.encode(lastSortValues);
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

  /** Resolves scroll sort keys; always ends with {@code message_id} for stable paging. */
  @Nonnull
  static List<SortKey> resolveSortKeys(@Nonnull List<SortCriterion> sortCriteria) {
    List<SortKey> keys = new ArrayList<>();
    if (sortCriteria.isEmpty()) {
      keys.add(new SortKey("event_time", false, SortValueKind.EVENT_TIME));
      keys.add(new SortKey("message_id", false, SortValueKind.MESSAGE_ID));
      return keys;
    }
    for (SortCriterion sc : sortCriteria) {
      keys.add(SortKey.fromCriterion(sc));
    }
    boolean hasMessageId = keys.stream().anyMatch(k -> k.valueKind() == SortValueKind.MESSAGE_ID);
    if (!hasMessageId) {
      boolean ascending = keys.get(keys.size() - 1).ascending();
      keys.add(new SortKey("message_id", ascending, SortValueKind.MESSAGE_ID));
    }
    return keys;
  }

  /**
   * Appends a direction-aware keyset predicate matching ES {@code search_after} semantics:
   *
   * <pre>
   * (k0 op0 ?) OR (k0 IS NOT DISTINCT FROM ? AND k1 op1 ?) OR ...
   * </pre>
   *
   * where {@code op_i} is {@code <} for DESC and {@code >} for ASC.
   */
  static void appendKeysetPredicate(
      @Nonnull StringBuilder where,
      @Nonnull List<Object> params,
      @Nonnull List<SortKey> sortKeys,
      @Nonnull List<Object> cursorValues) {
    where.append(" AND (");
    for (int i = 0; i < sortKeys.size(); i++) {
      if (i > 0) {
        where.append(" OR ");
      }
      where.append("(");
      for (int j = 0; j < i; j++) {
        where.append(sortKeys.get(j).sqlExpr()).append(" IS NOT DISTINCT FROM ? AND ");
        params.add(toBindValue(sortKeys.get(j), cursorValues.get(j)));
      }
      String op = sortKeys.get(i).ascending() ? ">" : "<";
      where.append(sortKeys.get(i).sqlExpr()).append(" ").append(op).append(" ?");
      params.add(toBindValue(sortKeys.get(i), cursorValues.get(i)));
      where.append(")");
    }
    where.append(")");
  }

  @Nullable
  private static Object toBindValue(@Nonnull SortKey key, @Nullable Object raw) {
    if (raw == null) {
      return null;
    }
    if (key.valueKind() == SortValueKind.EVENT_TIME) {
      if (raw instanceof java.sql.Timestamp) {
        return raw;
      }
      if (raw instanceof Number) {
        return new java.sql.Timestamp(((Number) raw).longValue());
      }
      return java.sql.Timestamp.from(
          java.time.Instant.ofEpochMilli(Long.parseLong(raw.toString())));
    }
    return raw.toString();
  }

  @Nonnull
  private static List<Object> readSortValues(@Nonnull ResultSet rs, @Nonnull List<SortKey> sortKeys)
      throws SQLException {
    List<Object> values = new ArrayList<>(sortKeys.size());
    for (int i = 0; i < sortKeys.size(); i++) {
      String col = "_sk" + i;
      if (sortKeys.get(i).valueKind() == SortValueKind.EVENT_TIME) {
        java.sql.Timestamp ts = rs.getTimestamp(col);
        values.add(rs.wasNull() || ts == null ? null : ts.getTime());
      } else {
        String s = rs.getString(col);
        values.add(rs.wasNull() ? null : s);
      }
    }
    return values;
  }

  enum SortValueKind {
    EVENT_TIME,
    MESSAGE_ID,
    DOCUMENT_TEXT
  }

  /** One ORDER BY / keyset column for timeseries scroll. */
  static final class SortKey {
    private final String sqlExpr;
    private final boolean ascending;
    private final SortValueKind valueKind;

    SortKey(String sqlExpr, boolean ascending, SortValueKind valueKind) {
      this.sqlExpr = sqlExpr;
      this.ascending = ascending;
      this.valueKind = valueKind;
    }

    String sqlExpr() {
      return sqlExpr;
    }

    boolean ascending() {
      return ascending;
    }

    SortValueKind valueKind() {
      return valueKind;
    }

    static SortKey fromCriterion(SortCriterion sc) {
      String f = TimeseriesPgJsonPaths.stripKeywordSuffix(sc.getField());
      boolean ascending = sc.getOrder() == SortOrder.ASCENDING;
      if (MappingsBuilder.TIMESTAMP_MILLIS_FIELD.equals(f)
          || MappingsBuilder.TIMESTAMP_FIELD.equals(f)) {
        return new SortKey("event_time", ascending, SortValueKind.EVENT_TIME);
      }
      if (MappingsBuilder.MESSAGE_ID_FIELD.equals(f)) {
        return new SortKey("message_id", ascending, SortValueKind.MESSAGE_ID);
      }
      return new SortKey(
          PostgresTimeseriesAggregatedStatsDao.documentTextPathSql(f),
          ascending,
          SortValueKind.DOCUMENT_TEXT);
    }
  }

  /** Keyset cursor encoding active sort values (ES {@code search_after} analogue). */
  private static final class ScrollCursor {
    private ScrollCursor() {}

    @Nullable
    static List<Object> decode(@Nullable String scrollId, @Nonnull List<SortKey> sortKeys) {
      if (scrollId == null || scrollId.isBlank()) {
        return null;
      }
      try {
        String json = new String(Base64.getUrlDecoder().decode(scrollId), StandardCharsets.UTF_8);
        ObjectMapper om = new ObjectMapper();
        JsonNode n = om.readTree(json);
        JsonNode v = n.get("v");
        if (v == null || !v.isArray() || v.size() != sortKeys.size()) {
          return null;
        }
        List<Object> values = new ArrayList<>(sortKeys.size());
        for (int i = 0; i < sortKeys.size(); i++) {
          JsonNode el = v.get(i);
          if (el == null || el.isNull()) {
            values.add(null);
          } else if (sortKeys.get(i).valueKind() == SortValueKind.EVENT_TIME) {
            values.add(el.asLong());
          } else {
            values.add(el.asText());
          }
        }
        return values;
      } catch (Exception e) {
        return null;
      }
    }

    @Nullable
    static String encode(@Nonnull List<Object> values) {
      try {
        ObjectMapper om = new ObjectMapper();
        ObjectNode o = om.createObjectNode();
        ArrayNode arr = o.putArray("v");
        for (Object value : values) {
          if (value == null) {
            arr.addNull();
          } else if (value instanceof Number) {
            arr.add(((Number) value).longValue());
          } else {
            arr.add(value.toString());
          }
        }
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
