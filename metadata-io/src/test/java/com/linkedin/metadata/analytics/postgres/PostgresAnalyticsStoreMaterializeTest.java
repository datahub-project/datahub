package com.linkedin.metadata.analytics.postgres;

import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.postgres.PgAnalyticsStoreOptions;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class PostgresAnalyticsStoreMaterializeTest {

  @Test
  public void materialize_commitsDeleteAndInsertsTogether() throws Exception {
    DataSource dataSource = mock(DataSource.class);
    Connection readConn = mock(Connection.class);
    Connection writeConn = mock(Connection.class);
    PreparedStatement selectPs = mock(PreparedStatement.class);
    PreparedStatement deletePs = mock(PreparedStatement.class);
    PreparedStatement insertPs = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    AtomicBoolean autoCommit = new AtomicBoolean(true);
    when(dataSource.getConnection()).thenReturn(readConn, writeConn);
    when(readConn.prepareStatement(contains("SELECT"))).thenReturn(selectPs);
    when(selectPs.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true, false);
    when(rs.getString(1)).thenReturn("PageViewEvent");
    when(rs.getLong(2)).thenReturn(5L);

    when(writeConn.prepareStatement(contains("DELETE"))).thenReturn(deletePs);
    when(writeConn.prepareStatement(contains("INSERT"))).thenReturn(insertPs);
    when(writeConn.getAutoCommit()).thenAnswer(inv -> autoCommit.get());
    org.mockito.Mockito.doAnswer(
            inv -> {
              autoCommit.set(inv.getArgument(0));
              return null;
            })
        .when(writeConn)
        .setAutoCommit(org.mockito.ArgumentMatchers.anyBoolean());

    Database database = mock(Database.class);
    when(database.dataSource()).thenReturn(dataSource);
    PostgresAnalyticsStore store =
        new PostgresAnalyticsStore(
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

    store.materializeDatahubUsageHourlyFromRaw(Instant.parse("2024-06-15T10:00:00Z"));

    ArgumentCaptor<Boolean> autoCommitCaptor = ArgumentCaptor.forClass(Boolean.class);
    verify(writeConn, atLeastOnce()).setAutoCommit(autoCommitCaptor.capture());
    assertTrue(autoCommitCaptor.getAllValues().contains(false));
    verify(writeConn).commit();
    verify(writeConn, never()).rollback();
    verify(deletePs).executeUpdate();
    verify(insertPs).executeUpdate();
  }
}
