package com.linkedin.metadata.datahubusage.postgres;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.analytics.postgres.AnalyticsMetricFamilies;
import com.linkedin.metadata.analytics.postgres.PgAnalyticsStoreRegistry;
import com.linkedin.metadata.analytics.postgres.PostgresAnalyticsStore;
import com.linkedin.metadata.datahubusage.DataHubUsageEventConstants;
import com.linkedin.metadata.datahubusage.DataHubUsageService;
import com.linkedin.metadata.datahubusage.ExternalAuditEventsSearchRequest;
import com.linkedin.metadata.datahubusage.ExternalAuditEventsSearchResponse;
import com.linkedin.metadata.datahubusage.InternalUsageEventResult;
import com.linkedin.metadata.datahubusage.event.EventSource;
import com.linkedin.metadata.datahubusage.event.LoginSource;
import com.linkedin.metadata.datahubusage.event.UsageEventResult;
import com.linkedin.metadata.datahubusage.postgres.PostgresAuditEventsSql.Cursor;
import com.linkedin.metadata.datahubusage.postgres.PostgresAuditEventsSql.FilterBinds;
import com.linkedin.metadata.datahubusage.postgres.PostgresAuditEventsSql.SqlPlan;
import com.linkedin.metadata.datahubusage.postgres.PostgresAuditEventsSql.TimeRange;
import com.linkedin.metadata.postgres.jdbc.PostgresPreparedBinder;
import io.datahubproject.metadata.context.OperationContext;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Audit-event search over pgAnalytics {@code {prefix}_event}. Matches the Elasticsearch usage-index
 * filters (time range, event/aspect/entity/actor, {@code usageSource=backend}) and maps {@code
 * document} JSONB the same way ES maps {@code _source}.
 */
@Slf4j
public class PostgresDataHubUsageService implements DataHubUsageService {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TypeReference<LinkedHashMap<String, Object>> MAP_TYPE =
      new TypeReference<>() {};

  @Nonnull private final PostgresAnalyticsStore store;

  public PostgresDataHubUsageService(@Nonnull PgAnalyticsStoreRegistry registry) {
    this(registry.resolve(AnalyticsMetricFamilies.DATAHUB_USAGE).getStore());
  }

  PostgresDataHubUsageService(@Nonnull PostgresAnalyticsStore store) {
    this.store = Objects.requireNonNull(store, "store");
  }

  @Override
  public String getUsageIndexName(@Nonnull OperationContext opContext) {
    return store.qualifiedEventTable();
  }

  @Override
  public ExternalAuditEventsSearchResponse externalAuditEventsSearch(
      OperationContext opContext, ExternalAuditEventsSearchRequest request) {
    TimeRange range =
        PostgresAuditEventsSql.resolveTimeRange(request.getStartTime(), request.getEndTime());
    FilterBinds filters = PostgresAuditEventsSql.filterBinds(range, request);
    String table = store.qualifiedEventTable();
    int size = Math.max(request.getSize(), 0);
    Cursor cursor = PostgresAuditEventsSql.decodeCursor(request.getScrollId());

    long total = executeCount(PostgresAuditEventsSql.countPlan(table, filters));
    SqlPlan page = PostgresAuditEventsSql.pagePlan(table, filters, cursor, size + 1);
    List<Hit> hits = executePage(page);

    boolean hasMore = size > 0 && hits.size() > size;
    if (hasMore) {
      hits = new ArrayList<>(hits.subList(0, size));
    }
    String nextScrollId = null;
    if (hasMore && !hits.isEmpty()) {
      Hit last = hits.get(hits.size() - 1);
      nextScrollId =
          PostgresAuditEventsSql.encodeCursor(
              new Cursor(last.eventTime, last.eventType, last.actorUrn, last.eventId));
    }

    List<UsageEventResult> events = new ArrayList<>(hits.size());
    for (Hit hit : hits) {
      events.add(mapUsageEvent(opContext, hit.documentJson, request.isIncludeRaw()));
    }
    return ExternalAuditEventsSearchResponse.builder()
        .count(events.size())
        .total((int) Math.min(total, PostgresAuditEventsSql.totalHitCap()))
        .nextScrollId(nextScrollId)
        .usageEvents(events)
        .build();
  }

  private long executeCount(SqlPlan plan) {
    try (Connection c = readConnection();
        PreparedStatement ps = c.prepareStatement(plan.getSql())) {
      PostgresPreparedBinder.bind(ps, plan.getParams());
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return rs.getLong(1);
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException("PostgreSQL audit-events count failed", e);
    }
    return 0L;
  }

  @Nonnull
  private List<Hit> executePage(SqlPlan plan) {
    List<Hit> hits = new ArrayList<>();
    try (Connection c = readConnection();
        PreparedStatement ps = c.prepareStatement(plan.getSql())) {
      PostgresPreparedBinder.bind(ps, plan.getParams());
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          Timestamp ts = rs.getTimestamp(2);
          hits.add(
              new Hit(
                  rs.getString(1),
                  ts != null ? ts.toInstant() : Instant.EPOCH,
                  rs.getString(3),
                  rs.getString(4),
                  rs.getString(5)));
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException("PostgreSQL audit-events search failed", e);
    }
    return hits;
  }

  @Nonnull
  private Connection readConnection() throws SQLException {
    Connection c = store.getDatabase().dataSource().getConnection();
    c.setAutoCommit(true);
    return c;
  }

  @Nonnull
  private static UsageEventResult mapUsageEvent(
      @Nonnull OperationContext opContext, @Nullable String documentJson, boolean includeRaw) {
    LinkedHashMap<String, Object> source = new LinkedHashMap<>();
    if (documentJson != null && !documentJson.isBlank()) {
      try {
        source = MAPPER.readValue(documentJson, MAP_TYPE);
      } catch (Exception e) {
        log.warn("Skipping unreadable usage-event document", e);
      }
    }
    InternalUsageEventResult.InternalUsageEventResultBuilder builder =
        InternalUsageEventResult.builder();
    if (includeRaw) {
      builder.rawUsageEvent(new LinkedHashMap<>(source));
    }
    Object type = source.get(DataHubUsageEventConstants.TYPE);
    if (type instanceof String) {
      builder.eventType((String) type);
    }
    Object actor = source.get(DataHubUsageEventConstants.ACTOR_URN);
    if (actor instanceof String) {
      builder.actorUrn((String) actor);
    }
    Object timestamp = source.get(DataHubUsageEventConstants.TIMESTAMP);
    if (timestamp instanceof Number) {
      builder.timestamp(((Number) timestamp).longValue());
    }
    Object sourceIp = source.get(DataHubUsageEventConstants.SOURCE_IP);
    if (sourceIp instanceof String) {
      builder.sourceIP((String) sourceIp);
    }
    Object eventSource = source.get(DataHubUsageEventConstants.EVENT_SOURCE);
    if (eventSource instanceof String) {
      builder.eventSource(EventSource.getSource((String) eventSource));
    }
    Object loginSource = source.get(DataHubUsageEventConstants.LOGIN_SOURCE);
    if (loginSource instanceof String) {
      builder.loginSource(LoginSource.getSource((String) loginSource));
    }
    Object entityType = source.get(DataHubUsageEventConstants.ENTITY_TYPE);
    if (entityType instanceof String) {
      builder.entityType((String) entityType);
    }
    Object entityUrn = source.get(DataHubUsageEventConstants.ENTITY_URN);
    if (entityUrn instanceof String) {
      builder.entityUrn((String) entityUrn);
    }
    Object aspectName = source.get(DataHubUsageEventConstants.ASPECT_NAME);
    if (aspectName instanceof String) {
      builder.aspectName((String) aspectName);
    }
    Object traceId = source.get(DataHubUsageEventConstants.TRACE_ID);
    if (traceId instanceof String) {
      builder.telemetryTraceId((String) traceId);
    }
    Object userAgent = source.get(DataHubUsageEventConstants.USER_AGENT);
    if (userAgent instanceof String) {
      builder.userAgent((String) userAgent);
    }
    return opContext.getObjectMapper().convertValue(builder.build(), UsageEventResult.class);
  }

  private record Hit(
      String documentJson, Instant eventTime, String eventType, String actorUrn, String eventId) {}
}
