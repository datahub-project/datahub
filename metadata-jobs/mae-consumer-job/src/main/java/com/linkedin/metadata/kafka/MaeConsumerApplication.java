package com.linkedin.metadata.kafka;

import com.linkedin.gms.factory.common.SiblingGraphServiceFactory;
import com.linkedin.gms.factory.telemetry.ScheduledAnalyticsFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.solr.SolrHealthContributorAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchRestClientAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@SpringBootApplication(
    exclude = {
      ElasticsearchRestClientAutoConfiguration.class,
      CassandraAutoConfiguration.class,
      SolrHealthContributorAutoConfiguration.class
    })
@ComponentScan(
    basePackages = {
      // "com.linkedin.gms.factory.config",
      // "com.linkedin.gms.factory.common",
      "com.linkedin.gms.factory.kafka",
      "com.linkedin.metadata.boot.kafka",
      "com.linkedin.metadata.kafka",
      "com.linkedin.metadata.dao.producer",
      "com.linkedin.gms.factory.config",
      "com.linkedin.gms.factory.entity.update.indices"
    },
    excludeFilters = {
      @ComponentScan.Filter(
          type = FilterType.ASSIGNABLE_TYPE,
          classes = ScheduledAnalyticsFactory.class),
      @ComponentScan.Filter(
          type = FilterType.ASSIGNABLE_TYPE,
          classes = SiblingGraphServiceFactory.class)
    })
public class MaeConsumerApplication {
  public static void main(String[] args) {
    Class<?>[] primarySources = {MaeConsumerApplication.class, MclConsumerConfig.class};
    SpringApplication.run(primarySources, args);
  }
}
