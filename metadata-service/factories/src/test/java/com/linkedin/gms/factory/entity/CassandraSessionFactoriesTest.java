package com.linkedin.gms.factory.entity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.linkedin.gms.factory.ProductionConfigContextRunner;
import com.linkedin.gms.factory.common.LocalCassandraSessionConfigFactory;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import javax.net.ssl.SSLContext;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testng.annotations.Test;

/**
 * Loads {@link LocalCassandraSessionConfigFactory}, {@link CassandraSessionFactory} and {@link
 * CassandraReadPoolSessionFactory} with the production {@code application.yaml}. Checks that the
 * sessions exist only under {@code entityService.impl=cassandra} (the read pool also needs {@code
 * cassandra.readPool.enabled} and a writable GMS), and that each one is built from the {@code
 * cassandra.*} settings. The Cassandra driver's session builder is the only mock.
 */
public class CassandraSessionFactoriesTest {

  private static final String CASSANDRA = "entityService.impl=cassandra";
  private static final String PRIMARY_HOSTS = "cassandra.hosts=127.0.0.1,127.0.0.2";
  private static final String READ_POOL_ON = "cassandra.readPool.enabled=true";

  private static ApplicationContextRunner runner() {
    return ProductionConfigContextRunner.create()
        .withUserConfiguration(
            LocalCassandraSessionConfigFactory.class,
            CassandraSessionFactory.class,
            CassandraReadPoolSessionFactory.class);
  }

  @Test
  public void testProductionDefaultsBuildNoSessionButBindTheServiceConfig() {
    runWithBuilders(
        runner(),
        (context, builders) -> {
          assertThat(context).hasNotFailed();
          assertThat(context).doesNotHaveBean("cassandraSession");
          assertThat(context).doesNotHaveBean("cassandraReadPoolSession");
          assertThat(builders).isEmpty();
          assertThat(context.getBean("gmsCassandraServiceConfig", Map.class))
              .containsEntry("hosts", "cassandra")
              .containsEntry("port", "9042")
              .containsEntry("datacenter", "datacenter1")
              .containsEntry("keyspace", "datahub")
              .containsEntry("username", "cassandra")
              .containsEntry("useSsl", "false");
        });
  }

  @Test
  public void testCassandraImplBuildsThePrimarySessionFromConfig() {
    runWithBuilders(
        runner()
            .withPropertyValues(
                CASSANDRA,
                PRIMARY_HOSTS,
                "cassandra.port=9142",
                "cassandra.datacenter=dc-a",
                "cassandra.keyspace=my_keyspace",
                "cassandra.datasourceUsername=my_user",
                "cassandra.datasourcePassword=my_password"),
        (context, builders) -> {
          assertThat(context).hasNotFailed().hasBean("cassandraSession");
          assertThat(context).doesNotHaveBean("cassandraReadPoolSession");
          assertThat(builders).hasSize(1);
          CqlSessionBuilder primary = builders.get(0);
          assertThat(contactPoints(primary))
              .containsExactly(
                  new InetSocketAddress("127.0.0.1", 9142),
                  new InetSocketAddress("127.0.0.2", 9142));
          verify(primary).withLocalDatacenter("dc-a");
          verify(primary).withKeyspace("my_keyspace");
          verify(primary).withAuthCredentials("my_user", "my_password");
          verify(primary, never()).withSslContext(any());
        });
  }

  @Test
  public void testUseSslAddsTheDefaultSslContext() {
    runWithBuilders(
        runner().withPropertyValues(CASSANDRA, PRIMARY_HOSTS, "cassandra.useSsl=true"),
        (context, builders) -> {
          assertThat(context).hasNotFailed();
          verify(builders.get(0)).withSslContext(any(SSLContext.class));
        });
  }

  @Test
  public void testReadPoolWithItsOwnHostsBuildsASecondSession() {
    runWithBuilders(
        runner()
            .withPropertyValues(
                CASSANDRA,
                PRIMARY_HOSTS,
                READ_POOL_ON,
                "cassandra.readPool.hosts=127.0.0.3, 127.0.0.4",
                "cassandra.readPool.port=9242",
                "cassandra.readPool.datacenter=dc-read"),
        (context, builders) -> {
          assertThat(context).hasNotFailed().hasBean("cassandraReadPoolSession");
          assertThat(builders).hasSize(2);
          CqlSessionBuilder readPool = builderFor(builders, "127.0.0.3");
          assertThat(contactPoints(readPool))
              .containsExactly(
                  new InetSocketAddress("127.0.0.3", 9242),
                  new InetSocketAddress("127.0.0.4", 9242));
          verify(readPool).withLocalDatacenter("dc-read");
          verify(readPool).withKeyspace("datahub");
        });
  }

  @Test
  public void testReadPoolWithoutItsOwnHostsSplitsThePrimaryHosts() {
    runWithBuilders(
        // Blank the read-pool keys and pin the CASSANDRA_PORT / CASSANDRA_DATACENTER keys that both
        // sessions fall back to, so the environment can't change the result.
        runner()
            .withPropertyValues(
                CASSANDRA,
                PRIMARY_HOSTS,
                READ_POOL_ON,
                "CASSANDRA_READ_POOL_HOSTS=",
                "CASSANDRA_READ_POOL_PORT=",
                "CASSANDRA_READ_POOL_DATACENTER=",
                "CASSANDRA_PORT=9142",
                "CASSANDRA_DATACENTER=dc-shared"),
        (context, builders) -> {
          assertThat(context).hasNotFailed().hasBean("cassandraReadPoolSession");
          assertThat(builders).hasSize(2);
          for (CqlSessionBuilder builder : builders) {
            assertThat(contactPoints(builder))
                .containsExactly(
                    new InetSocketAddress("127.0.0.1", 9142),
                    new InetSocketAddress("127.0.0.2", 9142));
            verify(builder).withLocalDatacenter("dc-shared");
          }
        });
  }

  @Test
  public void testReadOnlyGmsBuildsNoReadPool() {
    runWithBuilders(
        runner()
            .withPropertyValues(CASSANDRA, PRIMARY_HOSTS, READ_POOL_ON, "datahub.readOnly=true"),
        (context, builders) -> {
          assertThat(context).hasNotFailed().hasBean("cassandraSession");
          assertThat(context).doesNotHaveBean("cassandraReadPoolSession");
          assertThat(builders).hasSize(1);
        });
  }

  @Test
  public void testReadPoolNeedsTheCassandraImpl() {
    runWithBuilders(
        runner().withPropertyValues(READ_POOL_ON),
        (context, builders) -> {
          assertThat(context).doesNotHaveBean("cassandraReadPoolSession");
          assertThat(builders).isEmpty();
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

  private static CqlSessionBuilder builderFor(List<CqlSessionBuilder> builders, String host) {
    return builders.stream()
        .filter(builder -> contactPoints(builder).get(0).getHostString().equals(host))
        .findFirst()
        .orElseThrow();
  }
}
