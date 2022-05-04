package com.linkedin.metadata.examples.cli;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchRestClientAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;


@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@SpringBootApplication(exclude = {ElasticsearchRestClientAutoConfiguration.class}, scanBasePackages = {
    "com.linkedin.metadata.examples.configs", "com.linkedin.metadata.examples.cli"})
public class MceCliApplication {
  public static void main(String[] args) {
    new SpringApplicationBuilder(MceCliApplication.class).web(WebApplicationType.NONE).run(args);
  }
}
