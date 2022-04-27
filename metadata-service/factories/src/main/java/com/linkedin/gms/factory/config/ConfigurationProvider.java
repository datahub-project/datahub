package com.linkedin.gms.factory.config;

import com.datahub.authentication.AuthenticationConfiguration;
import com.datahub.authorization.AuthorizationConfiguration;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;


@Configuration
@ConfigurationProperties
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
@Data
public class ConfigurationProvider {
  /**
   * Authentication related configs
   */
  private AuthenticationConfiguration authentication;
  /**
   * Authentication related configs
   */
  private AuthorizationConfiguration authorization;
  /**
   * Ingestion related configs
   */
  private IngestionConfiguration ingestion;

  /**
   * Telemetry related configs
   */
  private TelemetryConfiguration telemetry;
}