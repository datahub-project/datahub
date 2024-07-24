package com.linkedin.gms;

import com.datahub.auth.authentication.filter.AuthenticationFilter;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * Common configuration for all servlets. Generally this list also includes dependencies of the
 * embedded MAE/MCE consumers.
 */
@ComponentScan(
    basePackages = {
      "com.linkedin.metadata.boot",
      "com.linkedin.metadata.service",
      "com.datahub.event",
      "com.linkedin.gms.factory.config",
      "com.linkedin.gms.factory.entityregistry",
      "com.linkedin.gms.factory.common",
      "com.linkedin.gms.factory.entity",
      "com.linkedin.gms.factory.kafka",
      "com.linkedin.gms.factory.kafka.common",
      "com.linkedin.gms.factory.kafka.schemaregistry",
      "com.linkedin.metadata.boot.kafka",
      "com.linkedin.metadata.kafka",
      "com.linkedin.metadata.dao.producer",
      "com.linkedin.gms.factory.entity.update.indices",
      "com.linkedin.gms.factory.entityclient",
      "com.linkedin.gms.factory.form",
      "com.linkedin.gms.factory.incident",
      "com.linkedin.gms.factory.timeline.eventgenerator",
      "io.datahubproject.metadata.jobs.common.health.kafka",
      "com.linkedin.gms.factory.context",
      "com.linkedin.gms.factory.auth",
      "com.linkedin.gms.factory.search",
      "com.linkedin.gms.factory.secret",
      "com.linkedin.gms.factory.timeseries",
      "com.linkedin.gms.factory.plugins"
    })
@PropertySource(value = "classpath:/application.yaml", factory = YamlPropertySourceFactory.class)
@Configuration
public class CommonApplicationConfig {

  @Bean("authenticationFilter")
  public AuthenticationFilter authenticationFilter() {
    return new AuthenticationFilter();
  }
}
