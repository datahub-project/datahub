package com.linkedin.datahub.upgrade.loadindices;

import com.linkedin.datahub.upgrade.config.OpenTelemetryConfig;
import com.linkedin.gms.factory.event.ExternalEventsServiceFactory;
import com.linkedin.gms.factory.event.KafkaConsumerPoolFactory;
import com.linkedin.gms.factory.event.KafkaExternalEventsPollHandlerConfiguration;
import com.linkedin.gms.factory.kafka.CDCConsumerFactory;
import com.linkedin.gms.factory.kafka.KafkaEventConsumerFactory;
import com.linkedin.gms.factory.kafka.SimpleKafkaConsumerFactory;
import com.linkedin.gms.factory.kafka.common.KafkaInitializationManager;
import com.linkedin.gms.factory.kafka.trace.KafkaTraceReaderFactory;
import org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;

/**
 * Configuration for LoadIndices upgrade. The configured-transport event producer is wired in (Kafka
 * or pgQueue) so that {@link com.linkedin.metadata.entity.EntityServiceImpl} resolves its {@code
 * kafkaEventProducer} dependency through normal channels. Kafka <em>consumer</em> factories are
 * excluded so the upgrade does not register MAE/MCL listeners, but the producer factory is allowed
 * because {@code KafkaProducer} construction is lazy and never opens a connection during
 * LoadIndices itself (LoadIndices does not emit events).
 */
@Configuration
@Import({MetricsAutoConfiguration.class, OpenTelemetryConfig.class})
@ComponentScan(
    basePackages = {
      "com.linkedin.datahub.upgrade.loadindices.config",
      "com.linkedin.gms.factory.config",
      "com.linkedin.gms.factory.common",
      "com.linkedin.gms.factory.entity",
      "com.linkedin.gms.factory.entityclient",
      "com.linkedin.gms.factory.event",
      "com.linkedin.gms.factory.kafka",
      "com.linkedin.gms.factory.kafka.common",
      "com.linkedin.gms.factory.kafka.schemaregistry",
      "com.linkedin.gms.factory.plugins",
      "com.linkedin.gms.factory.entityregistry",
      "com.linkedin.gms.factory.search",
      "com.linkedin.gms.factory.timeseries",
      "com.linkedin.gms.factory.context",
      "com.linkedin.gms.factory.system_telemetry",
      "com.linkedin.metadata.dao.producer"
    },
    excludeFilters = {
      @ComponentScan.Filter(
          type = FilterType.ASSIGNABLE_TYPE,
          classes = {
            KafkaEventConsumerFactory.class,
            SimpleKafkaConsumerFactory.class,
            CDCConsumerFactory.class,
            KafkaConsumerPoolFactory.class,
            KafkaExternalEventsPollHandlerConfiguration.class,
            ExternalEventsServiceFactory.class,
            KafkaTraceReaderFactory.class,
            KafkaInitializationManager.class
          })
    })
public class LoadIndicesUpgradeConfig {}
