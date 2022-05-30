package com.linkedin.metadata.kafka;

import com.linkedin.gms.factory.telemetry.ScheduledAnalyticsFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchRestClientAutoConfiguration;


@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@SpringBootApplication(exclude = {ElasticsearchRestClientAutoConfiguration.class, CassandraAutoConfiguration.class,
    ScheduledAnalyticsFactory.class})
public class MceConsumerApplication {

  public static void main(String[] args) {
    SpringApplication.run(MceConsumerApplication.class, args);
  }
}
