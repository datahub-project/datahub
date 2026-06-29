package com.linkedin.gms.factory.entity;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datastax.oss.driver.api.core.CqlSession;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.CassandraConfiguration;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.ReadPoolConfiguration;
import com.linkedin.metadata.entity.storage.PrimaryStorageResolver;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.StorageTarget;
import io.ebean.Database;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PrimaryStorageResolverFactoryTest {

  private PrimaryStorageResolverFactory factory;
  private ConfigurationProvider configurationProvider;

  @BeforeMethod
  public void setUp() {
    factory = new PrimaryStorageResolverFactory();
    configurationProvider = mock(ConfigurationProvider.class);
    DataHubConfiguration dataHubConfiguration = new DataHubConfiguration();
    when(configurationProvider.getDatahub()).thenReturn(dataHubConfiguration);
    ReflectionTestUtils.setField(factory, "configurationProvider", configurationProvider);
    ReflectionTestUtils.setField(factory, "metricUtils", mock(MetricUtils.class));
  }

  @Test
  public void buildResolver_splitPool_registersReadTarget() {
    EbeanConfiguration ebeanConfiguration = new EbeanConfiguration();
    ebeanConfiguration.setUrl("jdbc:mysql://primary:3306/datahub");
    ReadPoolConfiguration readPool = new ReadPoolConfiguration();
    readPool.setEnabled(true);
    ebeanConfiguration.setReadPool(readPool);

    Database primary = mock(Database.class);
    Database read = mock(Database.class);
    @SuppressWarnings("unchecked")
    ObjectProvider<Database> readPoolServer = mock(ObjectProvider.class);
    when(readPoolServer.getIfAvailable()).thenReturn(read);

    PrimaryStorageResolver resolver =
        ReflectionTestUtils.invokeMethod(
            factory, "buildResolver", primary, readPoolServer, ebeanConfiguration, false, "ebean");

    assertTrue(resolver.getRegistry().has(StorageTarget.READ));
    assertFalse(resolver.getRegistry().isDistinctReadEndpoint());
    assertEquals(resolver.resolveEbeanPrimary(), primary);
  }

  @Test
  public void buildResolver_distinctReplicaUrl_setsDistinctEndpoint() {
    EbeanConfiguration ebeanConfiguration = new EbeanConfiguration();
    ebeanConfiguration.setUrl("jdbc:mysql://primary:3306/datahub");
    ReadPoolConfiguration readPool = new ReadPoolConfiguration();
    readPool.setEnabled(true);
    readPool.setUrl("jdbc:mysql://replica:3306/datahub");
    ebeanConfiguration.setReadPool(readPool);

    Database primary = mock(Database.class);
    @SuppressWarnings("unchecked")
    ObjectProvider<Database> readPoolServer = mock(ObjectProvider.class);
    when(readPoolServer.getIfAvailable()).thenReturn(mock(Database.class));

    PrimaryStorageResolver resolver =
        ReflectionTestUtils.invokeMethod(
            factory, "buildResolver", primary, readPoolServer, ebeanConfiguration, false, "ebean");

    assertTrue(resolver.getRegistry().isDistinctReadEndpoint());
  }

  @Test
  public void buildResolver_readOnlyMode_skipsReadRegistration() {
    EbeanConfiguration ebeanConfiguration = new EbeanConfiguration();
    ReadPoolConfiguration readPool = new ReadPoolConfiguration();
    readPool.setEnabled(true);
    ebeanConfiguration.setReadPool(readPool);

    Database primary = mock(Database.class);
    @SuppressWarnings("unchecked")
    ObjectProvider<Database> readPoolServer = mock(ObjectProvider.class);
    when(readPoolServer.getIfAvailable()).thenReturn(mock(Database.class));

    PrimaryStorageResolver resolver =
        ReflectionTestUtils.invokeMethod(
            factory, "buildResolver", primary, readPoolServer, ebeanConfiguration, true, "ebean");

    assertFalse(resolver.getRegistry().has(StorageTarget.READ));
  }

  @Test
  public void cassandraPrimaryStorageResolver_distinctHosts() {
    CassandraConfiguration cassandra = new CassandraConfiguration();
    cassandra.setHosts("cassandra-primary");
    cassandra.setPort("9042");
    cassandra.setDatacenter("dc1");
    ReadPoolConfiguration readPool = new ReadPoolConfiguration();
    readPool.setEnabled(true);
    readPool.setHosts("cassandra-replica");
    cassandra.setReadPool(readPool);
    when(configurationProvider.getCassandra()).thenReturn(cassandra);

    CqlSession primary = mock(CqlSession.class);
    CqlSession read = mock(CqlSession.class);
    @SuppressWarnings("unchecked")
    ObjectProvider<CqlSession> readPoolSession = mock(ObjectProvider.class);
    when(readPoolSession.getIfAvailable()).thenReturn(read);

    PrimaryStorageResolver resolver =
        factory.cassandraPrimaryStorageResolver(primary, readPoolSession);

    assertTrue(resolver.getRegistry().has(StorageTarget.READ));
    assertTrue(resolver.getRegistry().isDistinctReadEndpoint());
    assertEquals(resolver.resolveCassandraPrimary(), primary);
  }

  @Test
  public void cassandraPrimaryStorageResolver_readOnly_skipsReadPool() {
    DataHubConfiguration readOnlyDatahub = mock(DataHubConfiguration.class);
    when(readOnlyDatahub.isReadOnly()).thenReturn(true);
    when(configurationProvider.getDatahub()).thenReturn(readOnlyDatahub);

    CassandraConfiguration cassandra = new CassandraConfiguration();
    cassandra.setHosts("cassandra");
    ReadPoolConfiguration readPool = new ReadPoolConfiguration();
    readPool.setEnabled(true);
    cassandra.setReadPool(readPool);
    when(configurationProvider.getCassandra()).thenReturn(cassandra);

    CqlSession primary = mock(CqlSession.class);
    @SuppressWarnings("unchecked")
    ObjectProvider<CqlSession> readPoolSession = mock(ObjectProvider.class);
    when(readPoolSession.getIfAvailable()).thenReturn(mock(CqlSession.class));

    PrimaryStorageResolver resolver =
        factory.cassandraPrimaryStorageResolver(primary, readPoolSession);

    assertFalse(resolver.getRegistry().has(StorageTarget.READ));
  }
}
