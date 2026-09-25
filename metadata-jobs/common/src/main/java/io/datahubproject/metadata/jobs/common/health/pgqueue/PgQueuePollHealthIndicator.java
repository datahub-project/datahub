package io.datahubproject.metadata.jobs.common.health.pgqueue;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.messaging.PgQueueMessagingTransportCondition;
import com.linkedin.metadata.pgqueue.PgQueuePollWorkersBootstrap;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.health.contributor.AbstractHealthIndicator;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;

/**
 * Reports whether pgQueue poll worker threads are alive. When no {@link
 * PgQueuePollWorkersBootstrap} is registered in the context, reports UP with zero workers (e.g. GMS
 * without embedded pollers).
 */
@Component
@Slf4j
@Conditional(PgQueueMessagingTransportCondition.class)
public class PgQueuePollHealthIndicator extends AbstractHealthIndicator {

  private final ObjectProvider<PgQueuePollWorkersBootstrap> pollWorkersBootstrap;
  private final ConfigurationProvider configurationProvider;

  public PgQueuePollHealthIndicator(
      ObjectProvider<PgQueuePollWorkersBootstrap> pollWorkersBootstrap,
      ConfigurationProvider configurationProvider) {
    this.pollWorkersBootstrap = pollWorkersBootstrap;
    this.configurationProvider = configurationProvider;
  }

  @Override
  protected void doHealthCheck(Health.Builder builder) throws Exception {
    Status status = Status.UP;
    Map<String, Object> details = new LinkedHashMap<>();
    details.put("transport", "pgqueue");
    PgQueuePollWorkersBootstrap bootstrap = pollWorkersBootstrap.getIfAvailable();
    if (bootstrap == null) {
      details.put("pollWorkerThreads", 0);
      details.put("note", "No PgQueuePollWorkersBootstrap bean in this process");
      builder.status(status).withDetails(details).build();
      return;
    }
    int count = bootstrap.pollWorkerThreadCount();
    boolean alive = bootstrap.allPollWorkerThreadsAlive();
    details.put("pollWorkerThreads", count);
    details.put("allThreadsAlive", alive);
    if (configurationProvider.getKafka().getConsumer().isHealthCheckEnabled()
        && count > 0
        && !alive) {
      status = Status.DOWN;
    }
    builder.status(status).withDetails(details).build();
  }
}
