package com.linkedin.gms.factory.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.gms.factory.common.LocalEbeanConfigFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.ReadPoolConfiguration;
import io.ebean.config.DatabaseConfig;
import io.ebean.datasource.DataSourceConfig;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EbeanReadPoolConfigFactoryTest {

  private EbeanReadPoolConfigFactory factory;
  private ConfigurationProvider configurationProvider;
  private LocalEbeanConfigFactory localEbeanConfigFactory;

  @BeforeMethod
  public void setUp() {
    factory = new EbeanReadPoolConfigFactory();
    configurationProvider = mock(ConfigurationProvider.class);
    localEbeanConfigFactory = mock(LocalEbeanConfigFactory.class);
    ReflectionTestUtils.setField(factory, "configurationProvider", configurationProvider);
    ReflectionTestUtils.setField(factory, "localEbeanConfigFactory", localEbeanConfigFactory);
  }

  @Test
  public void ebeanReadPoolDataSourceConfig_splitPool_marksReadOnly() {
    EbeanConfiguration ebeanConfiguration = new EbeanConfiguration();
    ebeanConfiguration.setUrl("jdbc:mysql://primary:3306/datahub");
    ReadPoolConfiguration readPool = new ReadPoolConfiguration();
    readPool.setEnabled(true);
    readPool.setMinConnections(3);
    readPool.setMaxConnections(10);
    readPool.setMaxInactiveTimeSeconds(30);
    readPool.setMaxAgeMinutes(60);
    readPool.setLeakTimeMinutes(5);
    readPool.setWaitTimeoutMillis(500);
    ebeanConfiguration.setReadPool(readPool);
    when(configurationProvider.getEbean()).thenReturn(ebeanConfiguration);

    DataSourceConfig built = new DataSourceConfig();
    when(localEbeanConfigFactory.buildDataSourceConfig(any(), isNull())).thenReturn(built);

    DataSourceConfig readConfig = factory.ebeanReadPoolDataSourceConfig(new DataSourceConfig());

    assertTrue(readConfig.isReadOnly());
    assertTrue(readConfig.isAutoCommit());
    assertEquals(readConfig.getMinConnections(), 3);
    assertEquals(readConfig.getMaxConnections(), 10);
    assertEquals(readConfig.getMaxInactiveTimeSecs(), 30);
    assertEquals(readConfig.getMaxAgeMinutes(), 60);
    assertEquals(readConfig.getLeakTimeMinutes(), 5);
    assertEquals(readConfig.getWaitTimeoutMillis(), 500);
  }

  @Test
  public void ebeanReadPoolDataSourceConfig_distinctUrl_usesReplicaEndpoint() {
    EbeanConfiguration ebeanConfiguration = new EbeanConfiguration();
    ebeanConfiguration.setUrl("jdbc:mysql://primary:3306/datahub");
    ReadPoolConfiguration readPool = new ReadPoolConfiguration();
    readPool.setEnabled(true);
    readPool.setUrl("jdbc:mysql://replica:3306/datahub");
    ebeanConfiguration.setReadPool(readPool);
    when(configurationProvider.getEbean()).thenReturn(ebeanConfiguration);

    DataSourceConfig built = new DataSourceConfig();
    when(localEbeanConfigFactory.buildDataSourceConfig("jdbc:mysql://replica:3306/datahub", null))
        .thenReturn(built);

    DataSourceConfig readConfig = factory.ebeanReadPoolDataSourceConfig(new DataSourceConfig());

    assertEquals(readConfig, built);
  }

  @Test
  public void gmsEbeanReadPoolDatabaseConfig_wiresDataSource() {
    DataSourceConfig readPoolDataSourceConfig = new DataSourceConfig();
    DatabaseConfig databaseConfig =
        factory.gmsEbeanReadPoolDatabaseConfig(readPoolDataSourceConfig);

    assertEquals(databaseConfig.getDataSourceConfig(), readPoolDataSourceConfig);
    assertEquals(databaseConfig.getName(), "gmsEbeanReadPoolDatabaseConfig");
    assertTrue(!databaseConfig.isDefaultServer());
  }
}
