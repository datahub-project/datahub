package com.linkedin.metadata.analytics.postgres.compaction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.analytics.compaction.AnalyticsCompactionRequest;
import com.linkedin.metadata.analytics.compaction.AnalyticsCompactionResult;
import com.linkedin.metadata.analytics.postgres.PgAnalyticsStoreRegistry;
import com.linkedin.metadata.analytics.postgres.PostgresAnalyticsStore;
import com.linkedin.metadata.config.postgres.PgAnalyticsStoreOptions;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.testng.annotations.Test;

public class PostgresAnalyticsCompactionServiceTest {

  @Test
  public void compact_returnsLockNotAcquiredWhenBusy() throws Exception {
    DataSource dataSource = mock(DataSource.class);
    Connection lockConn = mock(Connection.class);
    Statement timeoutStmt = mock(Statement.class);
    PreparedStatement lockPs = mock(PreparedStatement.class);
    ResultSet lockRs = mock(ResultSet.class);
    when(dataSource.getConnection()).thenReturn(lockConn);
    when(lockConn.createStatement()).thenReturn(timeoutStmt);
    when(lockConn.prepareStatement(anyString())).thenReturn(lockPs);
    when(lockPs.executeQuery()).thenReturn(lockRs);
    when(lockRs.next()).thenReturn(true);
    when(lockRs.getBoolean(1)).thenReturn(false);

    Database database = mock(Database.class);
    when(database.dataSource()).thenReturn(dataSource);
    PostgresAnalyticsStore store = mock(PostgresAnalyticsStore.class);
    when(store.getDatabase()).thenReturn(database);

    PgAnalyticsStoreOptions options =
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
            .inputLagSeconds(900)
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
            .build();
    PgAnalyticsStoreRegistry.StoreHandle handle =
        new PgAnalyticsStoreRegistry.StoreHandle(options, database, store);
    Map<String, PgAnalyticsStoreRegistry.StoreHandle> stores = new LinkedHashMap<>();
    stores.put("default", handle);
    PgAnalyticsStoreRegistry registry = mock(PgAnalyticsStoreRegistry.class);
    when(registry.allStores()).thenReturn(stores);

    AnalyticsCompactor compactor = mock(AnalyticsCompactor.class);
    PostgresAnalyticsCompactionService service =
        new PostgresAnalyticsCompactionService(registry, compactor);

    AnalyticsCompactionResult result = service.compact(AnalyticsCompactionRequest.defaults());
    assertTrue(result.isLockNotAcquired());
    assertTrue(result.isMoreWorkRemaining());
    assertFalse(result.isFailed());
    assertEquals(result.getImplementation(), "pgAnalytics");
    verify(compactor, org.mockito.Mockito.never()).compact(any());
    verify(timeoutStmt, atLeastOnce()).execute(contains("statement_timeout TO DEFAULT"));
  }

  @Test
  public void compact_marksFailedWhenAdvisoryLockSetupThrows() throws Exception {
    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getConnection()).thenThrow(new java.sql.SQLException("pool exhausted"));

    Database database = mock(Database.class);
    when(database.dataSource()).thenReturn(dataSource);
    PostgresAnalyticsStore store = mock(PostgresAnalyticsStore.class);
    when(store.getDatabase()).thenReturn(database);

    PgAnalyticsStoreOptions options =
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
            .inputLagSeconds(900)
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
            .build();
    PgAnalyticsStoreRegistry.StoreHandle handle =
        new PgAnalyticsStoreRegistry.StoreHandle(options, database, store);
    Map<String, PgAnalyticsStoreRegistry.StoreHandle> stores = new LinkedHashMap<>();
    stores.put("default", handle);
    PgAnalyticsStoreRegistry registry = mock(PgAnalyticsStoreRegistry.class);
    when(registry.allStores()).thenReturn(stores);

    AnalyticsCompactor compactor = mock(AnalyticsCompactor.class);
    PostgresAnalyticsCompactionService service =
        new PostgresAnalyticsCompactionService(registry, compactor);

    AnalyticsCompactionResult result = service.compact(AnalyticsCompactionRequest.defaults());
    assertTrue(result.isFailed());
    assertTrue(result.isMoreWorkRemaining());
    assertTrue(result.getMessage().contains("pool exhausted"));
    verify(compactor, org.mockito.Mockito.never()).compact(any());
  }
}
