package com.linkedin.gms.factory.graph;

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
 * Verifies the dedicated pgGraph connection pool: fall-through defaults to {@code ebean.*},
 * explicit {@code postgres.pgGraph.pool.*} overrides, and pool-level {@code READ_COMMITTED}
 * isolation. The pgGraph pool is the sole consumer of its own connections, so pool-level isolation
 * is safe and avoids per-tx isolation overrides on graph DAOs.
 *
 * <p>Test instantiates the factory directly (no Spring context) because the factory's {@code
 * pgGraphEbeanServer} bean attempts a real Postgres connection, which we can't satisfy in unit
 * tests.
 */
public class PgGraphEbeanConfigFactoryNoCloudTest {

  private PgGraphEbeanConfigFactory factory;
  private MetricUtils metricUtils;

  @BeforeMethod
  public void setUp() {
    factory = new PgGraphEbeanConfigFactory();
    metricUtils = mock(MetricUtils.class);

    ReflectionTestUtils.setField(factory, "username", "mainuser");
    ReflectionTestUtils.setField(factory, "password", "mainpass");
    ReflectionTestUtils.setField(factory, "driver", "org.postgresql.Driver");
    ReflectionTestUtils.setField(factory, "url", "jdbc:postgresql://localhost:5432/datahub");
    ReflectionTestUtils.setField(factory, "minConnections", 1);
    ReflectionTestUtils.setField(factory, "maxConnections", 12);
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
  public void testDefaultPoolSizing() {
    DataSourceConfig config = factory.buildDataSourceConfig(metricUtils);

    // pgGraph defaults are slightly larger than pgQueue because graph DAOs serve interactive
    // reads in graphService.type=postgres mode AND batched writes via PostgresGraphWriteSink.
    assertEquals(config.getMinConnections(), Integer.valueOf(1));
    assertEquals(config.getMaxConnections(), Integer.valueOf(12));
    assertEquals(config.getMaxInactiveTimeSecs(), Integer.valueOf(120));
    assertEquals(config.getMaxAgeMinutes(), Integer.valueOf(120));
    assertEquals(config.getLeakTimeMinutes(), Integer.valueOf(15));
    assertEquals(config.getWaitTimeoutMillis(), Integer.valueOf(1000));
  }

  /** Pool-level READ_COMMITTED — graph DAOs are the only consumers of this pool. */
  @Test
  public void testPoolLevelIsolationIsReadCommitted() {
    DataSourceConfig config = factory.buildDataSourceConfig(metricUtils);

    assertEquals(config.getIsolationLevel(), Connection.TRANSACTION_READ_COMMITTED);
  }

  @Test
  public void testExplicitOverrideUrlIsHonored() {
    DataSourceConfig config =
        factory.buildDataSourceConfig("jdbc:postgresql://other-host:5432/graphonly", metricUtils);

    assertEquals(config.getUrl(), "jdbc:postgresql://other-host:5432/graphonly");
    assertEquals(config.getUsername(), "mainuser");
    assertEquals(config.getPassword(), "mainpass");
  }
}
