package com.linkedin.gms.factory.config;

import com.datahub.authentication.AuthenticationConfiguration;
import com.datahub.authorization.AuthorizationConfiguration;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

@EqualsAndHashCode(callSuper = true)
@Component
// Include extra kafka properties
@EnableConfigurationProperties(KafkaProperties.class)
@ConfigurationProperties
@Data
public class ConfigurationProvider extends DataHubAppConfiguration {
  /** Authentication related configs */
  private AuthenticationConfiguration authentication;

  /** Authorizer related configs */
  private AuthorizationConfiguration authorization;

  /** Configuration for the health check server */
  private HealthCheckConfiguration healthCheck;

  /** Structured properties related configurations */
  private StructuredPropertiesConfiguration structuredProperties;
}
