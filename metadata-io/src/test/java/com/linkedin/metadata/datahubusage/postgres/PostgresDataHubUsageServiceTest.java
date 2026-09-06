package com.linkedin.metadata.datahubusage.postgres;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.analytics.postgres.PostgresAnalyticsStore;
import com.linkedin.metadata.config.postgres.PgAnalyticsStoreOptions;
import com.linkedin.metadata.datahubusage.DataHubUsageEventConstants;
import com.linkedin.metadata.datahubusage.ExternalAuditEventsSearchRequest;
import com.linkedin.metadata.datahubusage.ExternalAuditEventsSearchResponse;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.testng.annotations.Test;

public class PostgresDataHubUsageServiceTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  @Test
  public void getUsageIndexNameIsQualifiedEventTable() throws Exception {
    PostgresDataHubUsageService service = new PostgresDataHubUsageService(store(new ScriptedRs()));
    assertEquals(service.getUsageIndexName(OP_CONTEXT), "public.metadata_analytics_event");
  }

  @Test
  public void emptyPageHasNoScrollId() throws Exception {
    ScriptedRs rs = new ScriptedRs();
    rs.count = 0;
    PostgresDataHubUsageService service = new PostgresDataHubUsageService(store(rs));
    ExternalAuditEventsSearchResponse response =
        service.externalAuditEventsSearch(
            OP_CONTEXT,
            ExternalAuditEventsSearchRequest.builder().size(10).includeRaw(true).build());
    assertEquals(response.getCount(), 0);
    assertEquals(response.getTotal(), 0);
    assertNull(response.getNextScrollId());
    assertTrue(rs.lastSql.get().contains("usage_source = ?"));
  }

  @Test
  public void mapsDocumentAndIssuesScrollWhenMoreRowsExist() throws Exception {
    ScriptedRs rs = new ScriptedRs();
    rs.count = 3;
    rs.pageRows = 2;
    rs.documentJson =
        "{\"type\":\"LogInEvent\",\"actorUrn\":\"urn:li:corpuser:datahub\",\"timestamp\":1700000000000}";
    rs.eventTime = Timestamp.from(Instant.ofEpochMilli(1_700_000_000_000L));
    rs.eventType = "LogInEvent";
    rs.actorUrn = "urn:li:corpuser:datahub";
    rs.eventId = "evt-1";

    PostgresDataHubUsageService service = new PostgresDataHubUsageService(store(rs));
    ExternalAuditEventsSearchResponse response =
        service.externalAuditEventsSearch(
            OP_CONTEXT,
            ExternalAuditEventsSearchRequest.builder().size(1).includeRaw(true).build());

    assertEquals(response.getCount(), 1);
    assertEquals(response.getTotal(), 3);
    assertNotNull(response.getNextScrollId());
    assertEquals(response.getUsageEvents().get(0).getEventType(), "LogInEvent");
    assertEquals(response.getUsageEvents().get(0).getActorUrn(), "urn:li:corpuser:datahub");
    assertNotNull(response.getUsageEvents().get(0).getRawUsageEvent());
    assertEquals(
        response.getUsageEvents().get(0).getRawUsageEvent().get(DataHubUsageEventConstants.TYPE),
        "LogInEvent");
  }

  @Test
  public void includeRawFalseOmitsSourceMap() throws Exception {
    ScriptedRs rs = new ScriptedRs();
    rs.count = 1;
    rs.pageRows = 1;
    rs.documentJson =
        "{\"type\":\"LogInEvent\",\"actorUrn\":\"urn:li:corpuser:datahub\",\"timestamp\":1}";
    rs.eventTime = Timestamp.from(Instant.EPOCH);
    rs.eventType = "LogInEvent";
    rs.actorUrn = "urn:li:corpuser:datahub";
    rs.eventId = "evt-1";

    PostgresDataHubUsageService service = new PostgresDataHubUsageService(store(rs));
    ExternalAuditEventsSearchResponse response =
        service.externalAuditEventsSearch(
            OP_CONTEXT,
            ExternalAuditEventsSearchRequest.builder().size(10).includeRaw(false).build());
    assertNull(response.getUsageEvents().get(0).getRawUsageEvent());
  }

  private static PostgresAnalyticsStore store(ScriptedRs script) throws Exception {
    DataSource dataSource = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(ps.getConnection()).thenReturn(connection);
    when(connection.createArrayOf(anyString(), org.mockito.ArgumentMatchers.any()))
        .thenReturn(mock(java.sql.Array.class));
    when(connection.prepareStatement(anyString()))
        .thenAnswer(
            inv -> {
              script.lastSql.set(inv.getArgument(0));
              return ps;
            });
    when(ps.executeQuery())
        .thenAnswer(
            inv -> {
              ResultSet rs = mock(ResultSet.class);
              String sql = script.lastSql.get();
              if (sql != null && sql.startsWith("SELECT COUNT")) {
                when(rs.next()).thenReturn(true, false);
                when(rs.getLong(1)).thenReturn(script.count);
              } else {
                AtomicInteger remaining = new AtomicInteger(script.pageRows);
                when(rs.next()).thenAnswer(n -> remaining.getAndDecrement() > 0);
                when(rs.getString(1)).thenReturn(script.documentJson);
                when(rs.getTimestamp(2)).thenReturn(script.eventTime);
                when(rs.getString(3)).thenReturn(script.eventType);
                when(rs.getString(4)).thenReturn(script.actorUrn);
                when(rs.getString(5)).thenReturn(script.eventId);
              }
              return rs;
            });
    Database database = mock(Database.class);
    when(database.dataSource()).thenReturn(dataSource);
    return new PostgresAnalyticsStore(
        database,
        PgAnalyticsStoreOptions.builder()
            .name("default")
            .schema("public")
            .tablePrefix("metadata_analytics")
            .partmanPartitionInterval("1 day")
            .partmanPremake(4)
            .forceOverwritePartmanConfig(false)
            .rawMaxAgeSeconds(1)
            .hourlyMaxAgeSeconds(1)
            .dailyMaxAgeSeconds(1)
            .monthlyMaxAgeSeconds(1)
            .inputLagSeconds(0)
            .maintenanceCronEnabled(false)
            .maintenanceIntervalSeconds(3600)
            .apiUsageFlushEnabled(false)
            .entityCountEnabled(false)
            .poolMinConnections(1)
            .poolMaxConnections(12)
            .poolMaxInactiveTimeSeconds(120)
            .poolMaxAgeMinutes(120)
            .poolLeakTimeMinutes(15)
            .poolWaitTimeoutMillis(1000)
            .build());
  }

  private static final class ScriptedRs {
    final AtomicReference<String> lastSql = new AtomicReference<>();
    long count;
    int pageRows;
    String documentJson;
    Timestamp eventTime;
    String eventType;
    String actorUrn;
    String eventId;
  }
}
