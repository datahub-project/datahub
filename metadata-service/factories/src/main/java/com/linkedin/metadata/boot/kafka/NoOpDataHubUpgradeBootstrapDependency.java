package com.linkedin.metadata.boot.kafka;

import com.linkedin.metadata.boot.dependencies.BootstrapDependency;
import com.linkedin.metadata.config.messaging.MessagingTransport;
import com.linkedin.metadata.config.messaging.NonKafkaNonPgQueueMessagingTransportCondition;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;

/**
 * Stand-in for {@link DataHubUpgradeKafkaListener} when metadata messaging is neither Kafka nor
 * pgQueue. GMS and consumers still wire {@link
 * com.linkedin.metadata.boot.factories.BootstrapManagerFactory} steps that require a {@code
 * dataHubUpgradeKafkaListener} bean; upgrade ordering is enforced by jobs (for example Docker
 * {@code depends_on}) rather than upgrade-history messaging.
 */
@Component("dataHubUpgradeKafkaListener")
@Slf4j
@Conditional(NonKafkaNonPgQueueMessagingTransportCondition.class)
public class NoOpDataHubUpgradeBootstrapDependency implements BootstrapDependency {

  @Override
  public boolean waitForBootstrap() {
    log.info(
        "Skipping DataHub upgrade-history wait because {} is neither '{}' nor '{}'.",
        MessagingTransport.PROPERTY,
        MessagingTransport.KAFKA,
        MessagingTransport.PGQUEUE);
    return true;
  }
}
