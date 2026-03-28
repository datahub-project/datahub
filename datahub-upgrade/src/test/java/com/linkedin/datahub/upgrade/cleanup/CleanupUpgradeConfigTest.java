package com.linkedin.datahub.upgrade.cleanup;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;

import com.linkedin.datahub.upgrade.sqlsetup.DatabaseType;
import com.linkedin.datahub.upgrade.sqlsetup.SqlSetupArgs;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import io.ebean.Database;
import java.lang.reflect.Field;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CleanupUpgradeConfigTest {

  @Mock private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents;
  @Mock private ConfigurationProvider configurationProvider;
  @Mock private Database ebeanServer;

  private KafkaProperties kafkaProperties;

  @BeforeMethod
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    kafkaProperties = new KafkaProperties();

    when(configurationProvider.getKafka()).thenReturn(new KafkaConfiguration());
  }

  @AfterMethod
  public void tearDown() {
    // Clear any system properties set during tests
    System.clearProperty("CLEANUP_ELASTICSEARCH_ENABLED");
    System.clearProperty("CLEANUP_KAFKA_ENABLED");
    System.clearProperty("CLEANUP_SQL_ENABLED");
  }

  @Test
  public void testAllStepsAddedWhenAllComponentsAvailable() throws Exception {
    CleanupUpgradeConfig config = new CleanupUpgradeConfig();
    inject(config, "esComponents", esComponents);
    inject(config, "configurationProvider", configurationProvider);
    inject(config, "kafkaProperties", kafkaProperties);
    inject(config, "ebeanServer", ebeanServer);
    inject(config, "sqlSetupArgs", makeSqlSetupArgs());

    Cleanup cleanup = config.createCleanup();

    assertEquals(cleanup.steps().size(), 3);
  }

  @Test
  public void testEsStepSkippedWhenEsComponentsNull() throws Exception {
    CleanupUpgradeConfig config = new CleanupUpgradeConfig();
    inject(config, "configurationProvider", configurationProvider);
    inject(config, "kafkaProperties", kafkaProperties);
    inject(config, "ebeanServer", ebeanServer);
    inject(config, "sqlSetupArgs", makeSqlSetupArgs());

    Cleanup cleanup = config.createCleanup();

    assertEquals(cleanup.steps().size(), 2);
  }

  @Test
  public void testKafkaStepSkippedWhenKafkaPropertiesNull() throws Exception {
    CleanupUpgradeConfig config = new CleanupUpgradeConfig();
    inject(config, "esComponents", esComponents);
    inject(config, "configurationProvider", configurationProvider);
    inject(config, "ebeanServer", ebeanServer);
    inject(config, "sqlSetupArgs", makeSqlSetupArgs());

    Cleanup cleanup = config.createCleanup();

    assertEquals(cleanup.steps().size(), 2);
  }

  @Test
  public void testSqlStepSkippedWhenDbNull() throws Exception {
    CleanupUpgradeConfig config = new CleanupUpgradeConfig();
    inject(config, "esComponents", esComponents);
    inject(config, "configurationProvider", configurationProvider);
    inject(config, "kafkaProperties", kafkaProperties);

    Cleanup cleanup = config.createCleanup();

    assertEquals(cleanup.steps().size(), 2);
  }

  @Test
  public void testNoStepsWhenAllComponentsNull() throws Exception {
    CleanupUpgradeConfig config = new CleanupUpgradeConfig();

    Cleanup cleanup = config.createCleanup();

    assertEquals(cleanup.steps().size(), 0);
  }

  @Test
  public void testEsStepSkippedWhenDisabledViaSystemProperty() throws Exception {
    System.setProperty("CLEANUP_ELASTICSEARCH_ENABLED", "false");

    CleanupUpgradeConfig config = new CleanupUpgradeConfig();
    inject(config, "esComponents", esComponents);
    inject(config, "configurationProvider", configurationProvider);
    inject(config, "kafkaProperties", kafkaProperties);
    inject(config, "ebeanServer", ebeanServer);
    inject(config, "sqlSetupArgs", makeSqlSetupArgs());

    Cleanup cleanup = config.createCleanup();

    assertEquals(cleanup.steps().size(), 2);
  }

  @Test
  public void testKafkaStepSkippedWhenDisabledViaSystemProperty() throws Exception {
    System.setProperty("CLEANUP_KAFKA_ENABLED", "false");

    CleanupUpgradeConfig config = new CleanupUpgradeConfig();
    inject(config, "esComponents", esComponents);
    inject(config, "configurationProvider", configurationProvider);
    inject(config, "kafkaProperties", kafkaProperties);
    inject(config, "ebeanServer", ebeanServer);
    inject(config, "sqlSetupArgs", makeSqlSetupArgs());

    Cleanup cleanup = config.createCleanup();

    assertEquals(cleanup.steps().size(), 2);
  }

  @Test
  public void testSqlStepSkippedWhenDisabledViaSystemProperty() throws Exception {
    System.setProperty("CLEANUP_SQL_ENABLED", "false");

    CleanupUpgradeConfig config = new CleanupUpgradeConfig();
    inject(config, "esComponents", esComponents);
    inject(config, "configurationProvider", configurationProvider);
    inject(config, "kafkaProperties", kafkaProperties);
    inject(config, "ebeanServer", ebeanServer);
    inject(config, "sqlSetupArgs", makeSqlSetupArgs());

    Cleanup cleanup = config.createCleanup();

    assertEquals(cleanup.steps().size(), 2);
  }

  private static void inject(Object target, String fieldName, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static SqlSetupArgs makeSqlSetupArgs() {
    return new SqlSetupArgs(
        true,
        true,
        false,
        false,
        DatabaseType.MYSQL,
        false,
        null,
        null,
        null,
        null,
        "localhost",
        3306,
        "testdb");
  }
}
