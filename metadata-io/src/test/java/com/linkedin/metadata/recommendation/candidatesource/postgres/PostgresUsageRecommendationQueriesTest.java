package com.linkedin.metadata.recommendation.candidatesource.postgres;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.analytics.postgres.PostgresAnalyticsStore;
import com.linkedin.metadata.config.postgres.PgAnalyticsStoreOptions;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.testng.annotations.Test;

public class PostgresUsageRecommendationQueriesTest {

  @Test
  public void recentEntityUrnsGroupsByEntityAndOrdersByLastEvent() throws Exception {
    AtomicReference<String> sql = new AtomicReference<>();
    PostgresAnalyticsStore store = storeCapturingSql(sql, "urn:li:dataset:1");

    List<String> urns =
        PostgresUsageRecommendationQueries.recentEntityUrns(
            store, "urn:li:corpuser:datahub", "EntityViewEvent", 5, 30);

    assertEquals(urns, List.of("urn:li:dataset:1"));
    assertTrue(sql.get().contains("GROUP BY entity_urn"));
    assertTrue(sql.get().contains("ORDER BY MAX(event_time) DESC"));
    assertTrue(sql.get().contains("event_time >= ?"));
  }

  @Test
  public void mostViewedRestrictsToPeerActorsWhenProvided() throws Exception {
    AtomicReference<String> sql = new AtomicReference<>();
    PostgresAnalyticsStore store = storeCapturingSql(sql, "urn:li:dataset:2");

    PostgresUsageRecommendationQueries.mostViewedEntityUrns(
        store, "EntityViewEvent", 5, List.of("urn:li:corpuser:a"), 30);

    assertTrue(sql.get().contains("actor_urn = ANY(?)"));
    assertTrue(sql.get().contains("ORDER BY COUNT(*) DESC"));
  }

  private static PostgresAnalyticsStore storeCapturingSql(
      AtomicReference<String> sql, String result) throws Exception {
    DataSource dataSource = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(ps.getConnection()).thenReturn(connection);
    when(connection.createArrayOf(anyString(), org.mockito.ArgumentMatchers.any()))
        .thenReturn(mock(java.sql.Array.class));
    when(connection.prepareStatement(anyString()))
        .thenAnswer(
            inv -> {
              sql.set(inv.getArgument(0));
              return ps;
            });
    when(ps.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true, false);
    when(rs.getString(1)).thenReturn(result);

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
}
