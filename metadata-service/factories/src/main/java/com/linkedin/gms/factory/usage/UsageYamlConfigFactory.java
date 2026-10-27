package com.linkedin.gms.factory.usage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.usage.UsageYamlMapper;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class UsageYamlConfigFactory {

  @Bean(name = "usageYamlMapper")
  @Nonnull
  public ObjectMapper usageYamlMapper() {
    return UsageYamlMapper.create();
  }
}
