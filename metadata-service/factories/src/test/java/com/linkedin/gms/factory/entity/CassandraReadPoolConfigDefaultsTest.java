package com.linkedin.gms.factory.entity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.ReadPoolConfiguration;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testng.annotations.Test;

/**
 * Binds the production {@code application.yaml} and checks that, when {@code cassandra.readPool.*}
 * is not set, the read pool inherits the primary {@code cassandra.port} and {@code
 * cassandra.datacenter} however they were set, not only through the {@code CASSANDRA_PORT} / {@code
 * CASSANDRA_DATACENTER} keys. The Cassandra driver's session builder is the only mock.
 */
public class CassandraReadPoolConfigDefaultsTest {

  private static ApplicationContextRunner runner() {
    return new ApplicationContextRunner()
        .withInitializer(new ConfigDataApplicationContextInitializer())
        .withUserConfiguration(ConfigurationProvider.class, CassandraReadPoolSessionFactory.class)
        .withPropertyValues(
            "entityService.impl=cassandra",
            "cassandra.readPool.enabled=true",
            "cassandra.hosts=127.0.0.1,127.0.0.2",
            "cassandra.port=9142",
            "cassandra.datacenter=dc-a");
  }

  @Test
  public void testSplitReadPoolInheritsBoundPortAndDatacenter() {
    runWithBuilders(
        runner(),
        (context, builders) -> {
          assertThat(context).hasNotFailed().hasBean("cassandraReadPoolSession");
          ReadPoolConfiguration readPool =
              context.getBean(ConfigurationProvider.class).getCassandra().getReadPool();
          assertThat(readPool.getPort()).isEqualTo("9142");
          assertThat(readPool.getDatacenter()).isEqualTo("dc-a");

          assertThat(builders).hasSize(1);
          CqlSessionBuilder session = builders.get(0);
          assertThat(contactPoints(session))
              .containsExactly(
                  new InetSocketAddress("127.0.0.1", 9142),
                  new InetSocketAddress("127.0.0.2", 9142));
          verify(session).withLocalDatacenter("dc-a");
        });
  }

  @Test
  public void testReadPoolPortAndDatacenterStillOverrideThePrimary() {
    runWithBuilders(
        runner()
            .withPropertyValues(
                "cassandra.readPool.port=9242", "cassandra.readPool.datacenter=dc-read"),
        (context, builders) -> {
          assertThat(context).hasNotFailed();
          assertThat(builders).hasSize(1);
          CqlSessionBuilder session = builders.get(0);
          assertThat(contactPoints(session))
              .containsExactly(
                  new InetSocketAddress("127.0.0.1", 9242),
                  new InetSocketAddress("127.0.0.2", 9242));
          verify(session).withLocalDatacenter("dc-read");
        });
  }

  /** Runs the context with {@link CqlSession#builder()} handing out recording builders. */
  private static void runWithBuilders(
      ApplicationContextRunner runner,
      BiConsumer<AssertableApplicationContext, List<CqlSessionBuilder>> assertions) {
    List<CqlSessionBuilder> builders = new ArrayList<>();
    try (MockedStatic<CqlSession> cqlSession = mockStatic(CqlSession.class)) {
      cqlSession
          .when(CqlSession::builder)
          .thenAnswer(
              invocation -> {
                CqlSessionBuilder builder = mock(CqlSessionBuilder.class, RETURNS_SELF);
                when(builder.build()).thenReturn(mock(CqlSession.class));
                builders.add(builder);
                return builder;
              });
      runner.run(context -> assertions.accept(context, builders));
    }
  }

  @SuppressWarnings("unchecked")
  private static List<InetSocketAddress> contactPoints(CqlSessionBuilder builder) {
    ArgumentCaptor<Collection<InetSocketAddress>> captor =
        ArgumentCaptor.forClass(Collection.class);
    verify(builder).addContactPoints(captor.capture());
    return new ArrayList<>(captor.getValue());
  }
}
