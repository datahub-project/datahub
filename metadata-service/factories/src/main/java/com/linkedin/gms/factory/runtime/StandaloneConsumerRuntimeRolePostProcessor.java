package com.linkedin.gms.factory.runtime;

import com.linkedin.metadata.config.runtime.RuntimeRole;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/**
 * Standalone MCL and MCP only. Registered from those job jars, not from GMS, so {@code
 * DATAHUB_RUNTIME_ROLE} cannot turn this process into the metadata service. Runs before {@link
 * RuntimeRolePolicyPostProcessor} so that processor sees {@code client}.
 */
public class StandaloneConsumerRuntimeRolePostProcessor
    implements EnvironmentPostProcessor, Ordered {

  static final String PROPERTY_SOURCE_NAME = "standaloneConsumerRuntimeRole";

  private static final Logger LOG =
      LoggerFactory.getLogger(StandaloneConsumerRuntimeRolePostProcessor.class);

  @Override
  public void postProcessEnvironment(
      ConfigurableEnvironment environment, SpringApplication application) {
    String previous = environment.getProperty(RuntimeRole.PROPERTY);
    if (previous != null && !previous.trim().equalsIgnoreCase(RuntimeRole.CLIENT.wireName())) {
      LOG.warn(
          "datahub.runtime.role={} ignored on standalone consumer (forced to client)",
          previous.trim());
    }
    Map<String, Object> forced = new LinkedHashMap<>();
    forced.put(RuntimeRole.PROPERTY, RuntimeRole.CLIENT.wireName());
    environment.getPropertySources().addFirst(new MapPropertySource(PROPERTY_SOURCE_NAME, forced));
  }

  @Override
  public int getOrder() {
    return Ordered.LOWEST_PRECEDENCE - 1;
  }
}
