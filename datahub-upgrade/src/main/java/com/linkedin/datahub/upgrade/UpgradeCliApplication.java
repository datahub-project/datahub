package com.linkedin.datahub.upgrade;

import com.linkedin.gms.factory.auth.AuthorizerChainFactory;
import com.linkedin.gms.factory.auth.DataHubAuthorizerFactory;
import com.linkedin.gms.factory.graphql.GraphQLEngineFactory;
import com.linkedin.gms.factory.kafka.KafkaEventConsumerFactory;
import com.linkedin.gms.factory.kafka.SimpleKafkaConsumerFactory;
import com.linkedin.gms.factory.kafka.trace.KafkaTraceReaderFactory;
import com.linkedin.gms.factory.telemetry.ScheduledAnalyticsFactory;
import com.linkedin.gms.factory.trace.TraceServiceFactory;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchRestClientAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@SpringBootApplication(exclude = {ElasticsearchRestClientAutoConfiguration.class})
@ComponentScan(
    basePackages = {
      "com.linkedin.gms.factory",
      "com.linkedin.datahub.upgrade.config",
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
            TraceServiceFactory.class
          })
    })
public class UpgradeCliApplication {
  public static void main(String[] args) {
    new SpringApplicationBuilder(UpgradeCliApplication.class, UpgradeCli.class)
        .web(WebApplicationType.NONE)
        .run(args);
  }
}
