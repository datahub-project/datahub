package com.linkedin.datahub.upgrade.config;

import com.linkedin.gms.factory.auth.AuthorizerChainFactory;
import com.linkedin.gms.factory.auth.DataHubAuthorizerFactory;
import com.linkedin.gms.factory.event.ExternalEventsServiceFactory;
import com.linkedin.gms.factory.event.KafkaConsumerPoolFactory;
import com.linkedin.gms.factory.graphql.GraphQLEngineFactory;
import com.linkedin.gms.factory.kafka.KafkaEventConsumerFactory;
import com.linkedin.gms.factory.kafka.SimpleKafkaConsumerFactory;
import com.linkedin.gms.factory.kafka.trace.KafkaTraceReaderFactory;
import com.linkedin.gms.factory.telemetry.ScheduledAnalyticsFactory;
import com.linkedin.gms.factory.test.TestEngineFactory;
import com.linkedin.gms.factory.trace.TraceServiceFactory;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;

/**
 * Configuration for general upgrades that includes most components but excludes some that are not
 * typically needed for upgrade operations.
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
            ExternalEventsServiceFactory.class,
            // ACRYL-ONLY: Exclude TestEngineFactory to prevent metadata tests from loading
            // during system-update. TestEngine starts a background ScheduledExecutorService
            // for cache refresh which is not needed and causes unnecessary resource usage.
            // The EvaluateTests upgrade explicitly imports TestEngineFactory when needed.
            // See: commit 03d0daa9f2 "prevent metadata tests load during system-update"
            TestEngineFactory.class
          })
    })
public class GeneralUpgradeConfiguration {}
