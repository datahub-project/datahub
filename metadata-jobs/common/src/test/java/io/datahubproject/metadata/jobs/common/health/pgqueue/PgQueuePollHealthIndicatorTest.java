package io.datahubproject.metadata.jobs.common.health.pgqueue;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kafka.ConsumerConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import com.linkedin.metadata.pgqueue.PgQueuePollWorkersBootstrap;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;
import org.testng.annotations.Test;

public class PgQueuePollHealthIndicatorTest {

  @SuppressWarnings("unchecked")
  private PgQueuePollHealthIndicator build(
      PgQueuePollWorkersBootstrap bootstrap, boolean healthCheckEnabled) {
    ObjectProvider<PgQueuePollWorkersBootstrap> provider = mock(ObjectProvider.class);
    when(provider.getIfAvailable()).thenReturn(bootstrap);

    ConfigurationProvider configProvider = mock(ConfigurationProvider.class);
    KafkaConfiguration kafkaConfig = mock(KafkaConfiguration.class);
    ConsumerConfiguration consumerConfig = mock(ConsumerConfiguration.class);
    when(configProvider.getKafka()).thenReturn(kafkaConfig);
    when(kafkaConfig.getConsumer()).thenReturn(consumerConfig);
    when(consumerConfig.isHealthCheckEnabled()).thenReturn(healthCheckEnabled);

    return new PgQueuePollHealthIndicator(provider, configProvider);
  }

  @Test
  public void testNoBootstrapBeanReportsUp() throws Exception {
    PgQueuePollHealthIndicator indicator = build(null, true);
    Health.Builder builder = new Health.Builder();
    indicator.doHealthCheck(builder);
    Health health = builder.build();

    assertEquals(health.getStatus(), Status.UP);
    assertEquals(health.getDetails().get("pollWorkerThreads"), 0);
  }

  @Test
  public void testAllThreadsAliveReportsUp() throws Exception {
    PgQueuePollWorkersBootstrap bootstrap = mock(PgQueuePollWorkersBootstrap.class);
    when(bootstrap.pollWorkerThreadCount()).thenReturn(3);
    when(bootstrap.allPollWorkerThreadsAlive()).thenReturn(true);

    PgQueuePollHealthIndicator indicator = build(bootstrap, true);
    Health.Builder builder = new Health.Builder();
    indicator.doHealthCheck(builder);
    Health health = builder.build();

    assertEquals(health.getStatus(), Status.UP);
    assertEquals(health.getDetails().get("pollWorkerThreads"), 3);
    assertEquals(health.getDetails().get("allThreadsAlive"), true);
  }

  @Test
  public void testDeadThreadsWithHealthCheckEnabledReportsDown() throws Exception {
    PgQueuePollWorkersBootstrap bootstrap = mock(PgQueuePollWorkersBootstrap.class);
    when(bootstrap.pollWorkerThreadCount()).thenReturn(3);
    when(bootstrap.allPollWorkerThreadsAlive()).thenReturn(false);

    PgQueuePollHealthIndicator indicator = build(bootstrap, true);
    Health.Builder builder = new Health.Builder();
    indicator.doHealthCheck(builder);
    Health health = builder.build();

    assertEquals(health.getStatus(), Status.DOWN);
  }

  @Test
  public void testDeadThreadsWithHealthCheckDisabledReportsUp() throws Exception {
    PgQueuePollWorkersBootstrap bootstrap = mock(PgQueuePollWorkersBootstrap.class);
    when(bootstrap.pollWorkerThreadCount()).thenReturn(3);
    when(bootstrap.allPollWorkerThreadsAlive()).thenReturn(false);

    PgQueuePollHealthIndicator indicator = build(bootstrap, false);
    Health.Builder builder = new Health.Builder();
    indicator.doHealthCheck(builder);
    Health health = builder.build();

    assertEquals(health.getStatus(), Status.UP);
  }

  @Test
  public void testZeroThreadsWithDeadCheckReportsUp() throws Exception {
    PgQueuePollWorkersBootstrap bootstrap = mock(PgQueuePollWorkersBootstrap.class);
    when(bootstrap.pollWorkerThreadCount()).thenReturn(0);
    when(bootstrap.allPollWorkerThreadsAlive()).thenReturn(false);

    PgQueuePollHealthIndicator indicator = build(bootstrap, true);
    Health.Builder builder = new Health.Builder();
    indicator.doHealthCheck(builder);
    Health health = builder.build();

    // count == 0 means the condition `count > 0 && !alive` is false
    assertEquals(health.getStatus(), Status.UP);
  }
}
