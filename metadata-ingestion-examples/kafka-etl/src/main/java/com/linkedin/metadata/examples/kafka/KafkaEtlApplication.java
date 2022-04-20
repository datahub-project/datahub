package com.linkedin.metadata.examples.kafka;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchRestClientAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;


@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@SpringBootApplication(exclude = {ElasticsearchRestClientAutoConfiguration.class}, scanBasePackages = {
    "com.linkedin.metadata.examples.configs", "com.linkedin.metadata.examples.kafka"})
public class KafkaEtlApplication {
  public static void main(String[] args) {
    new SpringApplicationBuilder(KafkaEtlApplication.class).web(WebApplicationType.NONE).run(args);
  }
}
