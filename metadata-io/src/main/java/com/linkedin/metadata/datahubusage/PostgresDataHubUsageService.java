package com.linkedin.metadata.datahubusage;

import static com.linkedin.metadata.Constants.DATAHUB_USAGE_EVENT_INDEX;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.linkedin.metadata.datahubusage.event.UsageEventResult;
import com.linkedin.metadata.datahubusage.postgres.PostgresUsageEventsStore;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

@Slf4j
@RequiredArgsConstructor
public class PostgresDataHubUsageService implements DataHubUsageService {

  private final PostgresUsageEventsStore store;
  private final IndexConvention indexConvention;

  @Override
  public String getUsageIndexName() {
    return indexConvention.getIndexName(DATAHUB_USAGE_EVENT_INDEX);
  }

  @Override
  public ExternalAuditEventsSearchResponse externalAuditEventsSearch(
      OperationContext opContext,
      ExternalAuditEventsSearchRequest externalAuditEventsSearchRequest) {
    AuditClause clause = auditWhere(externalAuditEventsSearchRequest);
    int total = auditCount(clause.predicateSql(), clause.bindParams());

    PaginatedAudit page;
    try {
      page =
          auditPage(
              clause.predicateSql(),
              clause.bindParams(),
              externalAuditEventsSearchRequest,
              opContext);
    } catch (SQLException e) {
      throw new RuntimeException("PostgreSQL audit query failed:", e);
    }

    ExternalAuditEventsSearchResponse.ExternalAuditEventsSearchResponseBuilder b =
        ExternalAuditEventsSearchResponse.builder();
    b.count(page.events.size()).total(total).usageEvents(page.events);
    b.nextScrollId(page.nextScrollId);
    return b.build();
  }

  private record AuditClause(String predicateSql, List<Object> bindParams) {}

  private AuditClause auditWhere(ExternalAuditEventsSearchRequest req) {
    List<String> fragments = new ArrayList<>();
    List<Object> bindParams = new ArrayList<>();
    long startMillis = normalizeStartMillis(req.getStartTime());
    long endMillis = normalizeEndMillis(req.getEndTime());
    fragments.add("timestamp_ms >= ?");
    bindParams.add(startMillis);
    fragments.add("timestamp_ms < ?");
    bindParams.add(endMillis);
    fragments.add("usage_source = ?");
    bindParams.add(DataHubUsageEventConstants.BACKEND_SOURCE);

    if (CollectionUtils.isNotEmpty(req.getEventTypes())) {
      fragments.add(inClause(PostgresUsageEventsStore.COL_EVENT_TYPE, req.getEventTypes().size()));
      bindParams.addAll(req.getEventTypes());
    }
    if (CollectionUtils.isNotEmpty(req.getAspectTypes())) {
      fragments.add(
          inClause(PostgresUsageEventsStore.COL_ASPECT_NAME, req.getAspectTypes().size()));
      bindParams.addAll(req.getAspectTypes());
    }
    if (CollectionUtils.isNotEmpty(req.getEntityTypes())) {
      fragments.add(
          inClause(PostgresUsageEventsStore.COL_ENTITY_TYPE, req.getEntityTypes().size()));
      bindParams.addAll(req.getEntityTypes());
    }
    if (CollectionUtils.isNotEmpty(req.getActorUrns())) {
      fragments.add(inClause(PostgresUsageEventsStore.COL_ACTOR_URN, req.getActorUrns().size()));
      bindParams.addAll(req.getActorUrns());
    }

    return new AuditClause(String.join(" AND ", fragments), bindParams);
  }

  private static String inClause(String column, int n) {
    StringBuilder sb = new StringBuilder();
    sb.append(column).append(" IN (");
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append('?');
    }
    sb.append(')');
    return sb.toString();
  }

  private static long normalizeStartMillis(long start) {
    if (start < 0) {
      return Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();
    }
    return start;
  }

  private static long normalizeEndMillis(long end) {
    return end <= 0 ? Instant.now().toEpochMilli() : end;
  }

  private int auditCount(String wherePredicate, List<Object> bindParams) {
    String sql =
        "SELECT COUNT(*) FROM " + store.qualifiedParentTable() + " WHERE " + wherePredicate;
    try (Connection c = store.getDatabase().dataSource().getConnection()) {
      c.setReadOnly(true);
      try (PreparedStatement ps = c.prepareStatement(sql)) {
        bindAll(ps, bindParams);
        try (ResultSet rs = ps.executeQuery()) {
          // (int) cast: ExternalAuditEventsSearchResponse.total() is int by API
          // contract; usage-event retention (~30d default) keeps row counts well
          // below Integer.MAX_VALUE.
          return rs.next() ? (int) rs.getLong(1) : 0;
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("PostgreSQL audit COUNT failed:", e);
    }
  }

  private PaginatedAudit auditPage(
      String wherePredicate,
      List<Object> bindParams,
      ExternalAuditEventsSearchRequest req,
      OperationContext opContext)
      throws SQLException {

    SearchAfterWrapper after =
        StringUtils.isBlank(req.getScrollId())
            ? null
            : SearchAfterWrapper.fromScrollId(req.getScrollId());

    StringBuilder fullWhere = new StringBuilder(wherePredicate);
    List<Object> fullBinds = new ArrayList<>(bindParams);

    if (after != null && after.getSort() != null && after.getSort().length >= 3) {
      fullWhere
          .append(" AND (timestamp_ms < ? OR ")
          .append("(timestamp_ms = ? AND (COALESCE(event_type,'') > ? ")
          .append(
              " OR (COALESCE(event_type,'') IS NOT DISTINCT FROM ? AND COALESCE(actor_urn,'') > ?))))");
      Long tsPivot = coerceLong(after.getSort()[0]);
      String typePivot = sortComponentToSqlString(after.getSort()[1]);
      String actorPivot = sortComponentToSqlString(after.getSort()[2]);
      Objects.requireNonNull(tsPivot);
      fullBinds.add(tsPivot);
      fullBinds.add(tsPivot);
      fullBinds.add(typePivot == null ? "" : typePivot);
      fullBinds.add(typePivot == null ? "" : typePivot);
      fullBinds.add(actorPivot == null ? "" : actorPivot);
    }

    String sql =
        "SELECT "
            + PostgresUsageEventsStore.COL_DOCUMENT
            + " FROM "
            + store.qualifiedParentTable()
            + " WHERE "
            + fullWhere
            + " ORDER BY timestamp_ms DESC, COALESCE(event_type,'') ASC, COALESCE(actor_urn,'') ASC "
            + "LIMIT ?";
    List<LinkedHashMap<String, Object>> rawMaps = new ArrayList<>();
    try (Connection c = store.getDatabase().dataSource().getConnection()) {
      c.setReadOnly(true);
      try (PreparedStatement ps = c.prepareStatement(sql)) {
        int bindIdx = bindAll(ps, fullBinds);
        ps.setInt(bindIdx, req.getSize());
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            try {
              rawMaps.add(
                  opContext.getObjectMapper().readValue(rs.getString(1), new TypeReference<>() {}));
            } catch (JsonProcessingException e) {
              throw new IllegalStateException("Failed to parse usage event JSON row", e);
            }
          }
        }
      }
    }

    List<UsageEventResult> mapped = new ArrayList<>();
    for (LinkedHashMap<String, Object> m : rawMaps) {
      mapped.add(DataHubUsageEventResultMapper.fromSourceMap(opContext, m));
    }

    String nextScrollId =
        mapped.size() == req.getSize() && !rawMaps.isEmpty()
            ? encodeNext(rawMaps.get(rawMaps.size() - 1))
            : null;
    return new PaginatedAudit(mapped, nextScrollId);
  }

  private static String encodeNext(LinkedHashMap<String, Object> last) {
    Object ts = last.get(DataHubUsageEventConstants.TIMESTAMP);
    SearchAfterWrapper w =
        new SearchAfterWrapper(
            new Object[] {
              ts instanceof Number ? ((Number) ts).longValue() : Long.parseLong(ts.toString()),
              last.get(DataHubUsageEventConstants.TYPE),
              last.get(DataHubUsageEventConstants.ACTOR_URN)
            },
            null,
            0L);
    return w.toScrollId();
  }

  private static Long coerceLong(Object o) {
    if (o == null) {
      return null;
    }
    if (o instanceof Number) {
      return ((Number) o).longValue();
    }
    try {
      return Long.parseLong(String.valueOf(o));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static String sortComponentToSqlString(@Nullable Object o) {
    if (o == null) {
      return null;
    }
    return String.valueOf(o);
  }

  /**
   * @return index of next free parameter placeholder (one-based JDBC); index after binds.
   */
  private static int bindAll(PreparedStatement ps, List<Object> params) throws SQLException {
    int idx = 1;
    for (Object p : params) {
      bindOne(ps, idx++, p);
    }
    return idx;
  }

  private static void bindOne(PreparedStatement ps, int idx, Object p) throws SQLException {
    if (p instanceof Long) {
      ps.setLong(idx, (Long) p);
    } else if (p instanceof Integer) {
      ps.setInt(idx, (Integer) p);
    } else if (p == null) {
      ps.setString(idx, null);
    } else {
      ps.setString(idx, p.toString());
    }
  }

  private record PaginatedAudit(List<UsageEventResult> events, @Nullable String nextScrollId) {}
}
