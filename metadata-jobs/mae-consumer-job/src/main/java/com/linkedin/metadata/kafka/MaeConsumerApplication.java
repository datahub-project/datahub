package com.linkedin.metadata.kafka;

import com.linkedin.gms.factory.common.SiblingGraphServiceFactory;
import com.linkedin.gms.factory.telemetry.ScheduledAnalyticsFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.cassandra.autoconfigure.CassandraAutoConfiguration;
import org.springframework.boot.elasticsearch.autoconfigure.ElasticsearchClientAutoConfiguration;
import org.springframework.boot.elasticsearch.autoconfigure.ElasticsearchRestClientAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@SpringBootApplication(
    exclude = {
      ElasticsearchClientAutoConfiguration.class,
      ElasticsearchRestClientAutoConfiguration.class,
      CassandraAutoConfiguration.class
    })
@ComponentScan(
    basePackages = {
      "com.linkedin.gms.factory.common",
      "com.linkedin.metadata.service",
      "com.datahub.event",
      "com.linkedin.gms.factory.event",
      "com.linkedin.gms.factory.kafka",
      "com.linkedin.metadata.boot.kafka",
      "com.linkedin.metadata.boot.pgqueue",
      "com.linkedin.metadata.kafka",
      "com.linkedin.metadata.pgqueue",
      "com.linkedin.metadata.dao.producer",
      "com.linkedin.gms.factory.config",
      // Parent package supplies EntityServiceFactory, which the ZDU dual-write bean in
      // .update.indices requires. Scanning only the subpackage gave MAE the bean without its
      // dependency. Needs the mysqlConnector dep and EBEAN_DATASOURCE_* env to work.
      "com.linkedin.gms.factory.entity",
      "com.linkedin.gms.factory.entity.update.indices",
      "com.linkedin.gms.factory.search",
      "com.linkedin.gms.factory.entityclient",
      "com.linkedin.gms.factory.form",
      "com.linkedin.gms.factory.incident",
      "com.linkedin.gms.factory.timeline.eventgenerator",
      "io.datahubproject.metadata.jobs.common.health.kafka",
      "io.datahubproject.metadata.jobs.common.health.pgqueue",
      "com.linkedin.gms.factory.context",
      "com.linkedin.gms.factory.timeseries",
      "com.linkedin.gms.factory.assertion",
      "com.linkedin.gms.factory.plugins",
      "com.linkedin.gms.factory.change",
      "com.datahub.event.hook",
      "com.linkedin.gms.factory.notifications",
      "com.linkedin.gms.factory.system_telemetry"
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
