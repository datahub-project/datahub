package io.datahubproject.openapi.config;

import com.linkedin.metadata.config.kubernetes.KubernetesJacksonConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Exposes {@link KubernetesJacksonConfig} as a Spring bean so the OpenAPI Kubernetes controller and
 * other components use the shared K8 serialization config via dependency injection.
 */
@Configuration
public class KubernetesObjectMapperConfig {

  @Bean
  public KubernetesJacksonConfig kubernetesJacksonConfig() {
    return new KubernetesJacksonConfig();
  }
}
