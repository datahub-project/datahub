package com.linkedin.gms.servlet;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@ComponentScan(basePackages = {"com.linkedin.restli.server"})
@Configuration
public class RestliServletConfig {
  @Bean
  @ConditionalOnBean
  public ElasticSearchConfiguration elasticSearchConfiguration(
      final ConfigurationProvider configurationProvider) {
    return configurationProvider.getElasticSearch();
  }
}
