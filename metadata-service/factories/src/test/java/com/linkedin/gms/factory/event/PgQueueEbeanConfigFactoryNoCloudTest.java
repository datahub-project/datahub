package com.linkedin.gms.factory.event;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.ebean.datasource.DataSourceConfig;
import java.sql.Connection;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Verifies the dedicated pgQueue connection pool: fall-through defaults to {@code ebean.*},
 * explicit {@code postgres.pgQueue.pool.*} overrides, and pool-level {@code READ_COMMITTED}
 * isolation. The pgQueue pool is the sole consumer of its own connections, so pool-level isolation
 * is safe (no per-tx override race) and avoids ever calling {@code Connection.setIsolationLevel}
 * mid-transaction.
 *
 * <p>This test instantiates the factory directly (no Spring context) because the factory's {@code
 * pgQueueEbeanServer} bean attempts a real Postgres connection, which we can't satisfy in unit
 * tests. We exercise {@link PgQueueEbeanConfigFactory#buildDataSourceConfig(String, MetricUtils)}
 * directly which is the unit responsible for translating bound properties into the pool's {@link
 * DataSourceConfig}.
 */
public class PgQueueEbeanConfigFactoryNoCloudTest {

  private PgQueueEbeanConfigFactory factory;
  private MetricUtils metricUtils;

  @BeforeMethod
  public void setUp() {
    factory = new PgQueueEbeanConfigFactory();
    metricUtils = mock(MetricUtils.class);

    // Simulate the production fall-through: every pool.* property defaults to the matching
    // ebean.* in application.yaml. Spring resolves those placeholders at bind time, so by the
    // time the factory's @Value fields are populated, they already hold the resolved ebean.* value.
    ReflectionTestUtils.setField(factory, "username", "mainuser");
    ReflectionTestUtils.setField(factory, "password", "mainpass");
    ReflectionTestUtils.setField(factory, "driver", "org.postgresql.Driver");
    ReflectionTestUtils.setField(factory, "url", "jdbc:postgresql://localhost:5432/datahub");
    ReflectionTestUtils.setField(factory, "minConnections", 1);
    ReflectionTestUtils.setField(factory, "maxConnections", 8);
    ReflectionTestUtils.setField(factory, "maxInactiveTimeSecs", 120);
    ReflectionTestUtils.setField(factory, "maxAgeMinutes", 120);
    ReflectionTestUtils.setField(factory, "leakTimeMinutes", 15);
    ReflectionTestUtils.setField(factory, "waitTimeoutMillis", 1000);
    ReflectionTestUtils.setField(factory, "postgresUseIamAuth", false);
    ReflectionTestUtils.setField(factory, "useIamAuth", false);
    ReflectionTestUtils.setField(factory, "cloudProvider", "auto");
  }

  @Test
  public void testFallsThroughToEbeanCredentialsAndUrl() {
    DataSourceConfig config = factory.buildDataSourceConfig(metricUtils);

    assertNotNull(config);
    assertEquals(config.getUrl(), "jdbc:postgresql://localhost:5432/datahub");
    assertEquals(config.getUsername(), "mainuser");
    assertEquals(config.getPassword(), "mainpass");
    assertEquals(config.getDriver(), "org.postgresql.Driver");
  }

  @Test
  public void testDefaultPoolSizingIsSmallerThanMain() {
    DataSourceConfig config = factory.buildDataSourceConfig(metricUtils);

    // Defaults: pgQueue is meant to be modest because queue traffic is bursty rather than heavy.
    assertEquals(config.getMinConnections(), Integer.valueOf(1));
    assertEquals(config.getMaxConnections(), Integer.valueOf(8));
    assertEquals(config.getMaxInactiveTimeSecs(), Integer.valueOf(120));
    assertEquals(config.getMaxAgeMinutes(), Integer.valueOf(120));
    assertEquals(config.getLeakTimeMinutes(), Integer.valueOf(15));
    assertEquals(config.getWaitTimeoutMillis(), Integer.valueOf(1000));
  }

  /**
   * Pool-level READ_COMMITTED is set so the queue store never has to call {@code
   * Connection.setTransactionIsolation()} per-transaction. Postgres rejects mid-transaction
   * isolation changes, so this is the safe place to pin it.
   */
  @Test
  public void testPoolLevelIsolationIsReadCommitted() {
    DataSourceConfig config = factory.buildDataSourceConfig(metricUtils);

    assertEquals(config.getIsolationLevel(), Connection.TRANSACTION_READ_COMMITTED);
  }

  @Test
  public void testExplicitOverrideUrlIsHonored() {
    DataSourceConfig config =
        factory.buildDataSourceConfig("jdbc:postgresql://other-host:5432/queueonly", metricUtils);

    assertEquals(config.getUrl(), "jdbc:postgresql://other-host:5432/queueonly");
    // Other fields still come from the @Value-bound state, proving overriding the URL alone works
    // without disturbing credentials.
    assertEquals(config.getUsername(), "mainuser");
    assertEquals(config.getPassword(), "mainpass");
  }
}
