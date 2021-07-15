package com.linkedin.metadata.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.elasticsearch.rest.RestClientAutoConfiguration;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@SpringBootApplication(exclude = {RestClientAutoConfiguration.class})
public class MaeConsumerApplication {

  public static void main(String[] args) {
    Class<?>[] primarySources = {MaeConsumerApplication.class, com.linkedin.metadata.kafka.MaeConsumerConfig.class};
    SpringApplication.run(primarySources, args);
  }
}