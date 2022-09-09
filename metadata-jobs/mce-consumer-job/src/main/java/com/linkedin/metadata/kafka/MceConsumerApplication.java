package com.linkedin.metadata.kafka;

import com.linkedin.gms.factory.telemetry.ScheduledAnalyticsFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.solr.SolrHealthContributorAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchRestClientAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;


@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@SpringBootApplication(exclude = {ElasticsearchRestClientAutoConfiguration.class, CassandraAutoConfiguration.class,
    SolrHealthContributorAutoConfiguration.class})
@ComponentScan(excludeFilters = {
        @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = ScheduledAnalyticsFactory.class)})
public class MceConsumerApplication {

  public static void main(String[] args) {
    SpringApplication.run(MceConsumerApplication.class, args);
  }
}
