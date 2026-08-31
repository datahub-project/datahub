package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.kubernetes.KubernetesScaleDown;
import com.linkedin.datahub.upgrade.kubernetes.KubernetesScaleDownCleanupStep;
import com.linkedin.datahub.upgrade.kubernetes.KubernetesScaleDownStep;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.kubernetes.KubernetesScaleDownConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

@Configuration
@Conditional(SystemUpdateCondition.BlockingSystemUpdateCondition.class)
public class KubernetesScaleDownConfig {

  @Order(0)
  @Bean(name = "kubernetesScaleDown")
  public BlockingSystemUpgrade kubernetesScaleDown(ConfigurationProvider configurationProvider) {
    KubernetesScaleDownConfiguration configuration = resolveConfiguration(configurationProvider);
    if (configurationProvider.getKubernetes() != null) {
      configuration.setKubernetesServiceHost(
          configurationProvider.getKubernetes().getServiceHost());
    }
    KubernetesScaleDownStep step = new KubernetesScaleDownStep(configuration);
    KubernetesScaleDownCleanupStep cleanupStep = new KubernetesScaleDownCleanupStep(configuration);
    return new KubernetesScaleDown(step, cleanupStep);
  }

  /**
   * Returns scale-down config from provider. Requires systemUpdate.kubernetesScaleDown in
   * application.yaml.
   */
  private static KubernetesScaleDownConfiguration resolveConfiguration(
      ConfigurationProvider configurationProvider) {
    if (configurationProvider.getSystemUpdate() == null
        || configurationProvider.getSystemUpdate().getKubernetesScaleDown() == null) {
      throw new IllegalStateException(
          "systemUpdate.kubernetesScaleDown configuration is required. "
              + "Ensure application.yaml defines systemUpdate.kubernetesScaleDown with the required properties.");
    }
    return configurationProvider.getSystemUpdate().getKubernetesScaleDown();
  }
}
