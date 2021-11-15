package com.datahub.authentication;

import com.google.common.collect.ImmutableMap;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class ConfigProviderFactory {

  @Bean(name = "configProvider")
  @Scope("singleton")
  @Nonnull
  protected ConfigProvider getInstance() {
    return new ConfigProvider(ImmutableMap.of(
        "systemClientId",
        "__datahub_frontend",
        "systemClientSecret",
        "YouKnowNothing"
    ));
  }
}
