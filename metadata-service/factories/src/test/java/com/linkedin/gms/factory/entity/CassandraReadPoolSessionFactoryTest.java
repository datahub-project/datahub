package com.linkedin.gms.factory.entity;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;

import com.datastax.oss.driver.api.core.CqlSession;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.CassandraConfiguration;
import com.linkedin.metadata.config.ReadPoolConfiguration;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.cassandra.CassandraContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class CassandraReadPoolSessionFactoryTest {

  private CassandraContainer cassandraContainer;
  private CassandraReadPoolSessionFactory factory;
  private ConfigurationProvider configurationProvider;

  @BeforeClass
  public void startCassandra() {
    cassandraContainer = new CassandraContainer("cassandra:3.11");
    cassandraContainer.start();

    factory = new CassandraReadPoolSessionFactory();
    configurationProvider = mock(ConfigurationProvider.class);
    ReflectionTestUtils.setField(factory, "configurationProvider", configurationProvider);

    CassandraConfiguration cassandra = new CassandraConfiguration();
    cassandra.setHosts(cassandraContainer.getHost());
    cassandra.setPort(String.valueOf(cassandraContainer.getFirstMappedPort()));
    cassandra.setDatacenter("datacenter1");
    cassandra.setDatasourceUsername("cassandra");
    cassandra.setDatasourcePassword("cassandra");
    cassandra.setKeyspace("system");
    ReadPoolConfiguration readPool = new ReadPoolConfiguration();
    readPool.setEnabled(true);
    cassandra.setReadPool(readPool);
    when(configurationProvider.getCassandra()).thenReturn(cassandra);
  }

  @AfterClass
  public void stopCassandra() {
    if (cassandraContainer != null) {
      cassandraContainer.stop();
    }
  }

  @Test
  public void createReadPoolSession_splitPool_connects() {
    CqlSession session = factory.createReadPoolSession();
    assertNotNull(session);
    session.close();
  }

  @Test
  public void createReadPoolSession_distinctHosts_usesReadPoolHosts() {
    CassandraConfiguration cassandra = configurationProvider.getCassandra();
    ReadPoolConfiguration readPool = cassandra.getReadPool();
    readPool.setHosts(cassandraContainer.getHost());
    readPool.setPort(String.valueOf(cassandraContainer.getFirstMappedPort()));

    CqlSession session = factory.createReadPoolSession();
    assertNotNull(session);
    session.close();
  }
}
