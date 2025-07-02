package com.linkedin.gms.factory.system_info;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class K8ClientFactory {

  @Bean
  public KubernetesClient kubernetesClient() {
    try {
      return new KubernetesClientBuilder().build();
    } catch (Exception e) {
      // Return null if not in K8s environment
      return null;
    }
  }
}
