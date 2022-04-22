package com.linkedin.gms.factory.common;

import com.linkedin.datahub.graphql.generated.VisualConfiguration;
import javax.annotation.Nonnull;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class VisualConfigFactory {
  @Value("${visualConfig.assets.logoUrl}")
  private String logoUrl;

  @Nonnull
  @Bean(name = "visualConfig")
  protected VisualConfiguration getInstance() {
    VisualConfiguration config = new VisualConfiguration();
    config.setLogoUrl(logoUrl);

    return config;
  }
}
