package com.linkedin.metadata.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchRestClientAutoConfiguration;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@SpringBootApplication(exclude = {ElasticsearchRestClientAutoConfiguration.class})
public class MaeConsumerApplication {

  public static void main(String[] args) {
    Class<?>[] primarySources = {MaeConsumerApplication.class, MclConsumerConfig.class};
    SpringApplication.run(primarySources, args);
  }
}