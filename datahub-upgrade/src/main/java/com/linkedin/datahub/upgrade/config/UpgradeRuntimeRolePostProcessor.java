package com.linkedin.datahub.upgrade.config;

import com.linkedin.metadata.config.runtime.RuntimeRole;
import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * The upgrade image accepts {@code client} or {@code upgrade} only. {@code upgrade} currently uses
 * the same Hazelcast policy as {@code client}; it exists so Helm can select the role without this
 * process becoming the metadata service.
 */
public class UpgradeRuntimeRolePostProcessor implements EnvironmentPostProcessor, Ordered {

  @Override
  public void postProcessEnvironment(
      ConfigurableEnvironment environment, SpringApplication application) {
    RuntimeRole role = RuntimeRole.from(environment);
    if (role == RuntimeRole.SERVICE) {
      throw new IllegalStateException(
          "datahub-upgrade accepts datahub.runtime.role client or upgrade, not service."
              + " Unset DATAHUB_RUNTIME_ROLE or set it to client or upgrade.");
    }
  }

  @Override
  public int getOrder() {
    return Ordered.LOWEST_PRECEDENCE;
  }
}
