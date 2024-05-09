package com.linkedin.metadata.aspect.plugins.spring.validation;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CustomDataQualityRulesConfig {
  @Bean("myCustomMessage")
  public String myCustomMessage() {
    return "Spring injection works!";
  }
}
