package com.linkedin.gms.factory.entity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.CassandraConfiguration;
import com.linkedin.metadata.config.ReadPoolConfiguration;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.Test;

/** Checks how both Cassandra session factories turn a {@code hosts} list into contact points. */
public class CassandraContactPointsTest {

  private static final List<InetSocketAddress> EXPECTED =
      List.of(new InetSocketAddress("127.0.0.1", 9042), new InetSocketAddress("127.0.0.2", 9042));

  @Test
  public void testPrimarySessionTrimsHostsAndSkipsBlankEntries() {
    CassandraSessionFactory factory = new CassandraSessionFactory();
    ReflectionTestUtils.setField(
        factory,
        "sessionConfig",
        Map.of(
            "hosts", " 127.0.0.1, , 127.0.0.2 ",
            "port", "9042",
            "datacenter", "dc1",
            "keyspace", "datahub",
            "username", "u",
            "password", "p"));

    assertThat(contactPointsOf(factory::createSession)).containsExactlyElementsOf(EXPECTED);
  }

  @Test
  public void testReadPoolSessionTrimsHostsAndSkipsBlankEntries() {
    CassandraConfiguration cassandra = new CassandraConfiguration();
    cassandra.setHosts("cassandra");
    cassandra.setPort("9042");
    cassandra.setDatacenter("dc1");
    cassandra.setKeyspace("datahub");
    ReadPoolConfiguration readPool = new ReadPoolConfiguration();
    readPool.setEnabled(true);
    readPool.setHosts("127.0.0.1,, 127.0.0.2,");
    cassandra.setReadPool(readPool);
    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    when(configurationProvider.getCassandra()).thenReturn(cassandra);
    CassandraReadPoolSessionFactory factory = new CassandraReadPoolSessionFactory();
    ReflectionTestUtils.setField(factory, "configurationProvider", configurationProvider);

    assertThat(contactPointsOf(factory::createReadPoolSession)).containsExactlyElementsOf(EXPECTED);
  }

  @Test
  public void testHostsWithNoEntriesAreRejected() {
    assertThatThrownBy(() -> CassandraContactPoints.parse(" , ", 9042, "cassandra.hosts"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /** Builds a session with {@link CqlSession#builder()} mocked and returns its contact points. */
  @SuppressWarnings("unchecked")
  private static List<InetSocketAddress> contactPointsOf(Supplier<CqlSession> createSession) {
    CqlSessionBuilder builder = mock(CqlSessionBuilder.class, RETURNS_SELF);
    when(builder.build()).thenReturn(mock(CqlSession.class));
    try (MockedStatic<CqlSession> cqlSession = mockStatic(CqlSession.class)) {
      cqlSession.when(CqlSession::builder).thenReturn(builder);
      createSession.get();
    }
    ArgumentCaptor<Collection<InetSocketAddress>> captor =
        ArgumentCaptor.forClass(Collection.class);
    verify(builder).addContactPoints(captor.capture());
    return new ArrayList<>(captor.getValue());
  }
}
