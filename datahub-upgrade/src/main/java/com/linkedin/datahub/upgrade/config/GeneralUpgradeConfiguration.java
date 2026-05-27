package com.linkedin.datahub.upgrade.config;

import com.linkedin.gms.factory.auth.AuthorizerChainFactory;
import com.linkedin.gms.factory.auth.DataHubAuthorizerFactory;
import com.linkedin.gms.factory.event.ExternalEventsServiceFactory;
import com.linkedin.gms.factory.event.KafkaConsumerPoolFactory;
import com.linkedin.gms.factory.event.KafkaExternalEventsPollHandlerConfiguration;
import com.linkedin.gms.factory.graphql.GraphQLEngineFactory;
import com.linkedin.gms.factory.kafka.KafkaEventConsumerFactory;
import com.linkedin.gms.factory.kafka.SimpleKafkaConsumerFactory;
import com.linkedin.gms.factory.kafka.trace.KafkaTraceReaderFactory;
import com.linkedin.gms.factory.messaging.KafkaConsumerLagPort;
import com.linkedin.gms.factory.messaging.PgQueueConsumerLagPort;
import com.linkedin.gms.factory.telemetry.ScheduledAnalyticsFactory;
import com.linkedin.gms.factory.trace.TraceServiceFactory;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;

/**
 * Configuration for general upgrades that includes most components but excludes some that are not
 * typically needed for upgrade operations.
 *
 * <p>Consumer lag ports and their trace-reader dependencies are excluded because the system-update
 * context excludes {@link KafkaTraceReaderFactory} and {@link TraceServiceFactory}.
 */
@Configuration
@EnableAutoConfiguration
@ComponentScan(
    basePackages = {
      "com.linkedin.gms.factory",
      "com.linkedin.datahub.upgrade.config",
      "com.linkedin.datahub.upgrade.system.cdc",
      "com.linkedin.metadata.dao.producer"
    },
    excludeFilters = {
      @ComponentScan.Filter(
          type = FilterType.ASSIGNABLE_TYPE,
          classes = {
            ScheduledAnalyticsFactory.class,
            AuthorizerChainFactory.class,
            DataHubAuthorizerFactory.class,
            SimpleKafkaConsumerFactory.class,
            KafkaEventConsumerFactory.class,
            GraphQLEngineFactory.class,
            KafkaTraceReaderFactory.class,
            TraceServiceFactory.class,
            KafkaConsumerPoolFactory.class,
            KafkaExternalEventsPollHandlerConfiguration.class,
            ExternalEventsServiceFactory.class,
            KafkaConsumerLagPort.class,
            PgQueueConsumerLagPort.class
          })
    })
public class GeneralUpgradeConfiguration {}
