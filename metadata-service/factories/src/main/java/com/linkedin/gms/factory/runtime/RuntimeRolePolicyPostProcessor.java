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
import org.springframework.core.env.MutablePropertySources;

/**
 * Applies {@link RuntimeRole#DISTRIBUTED_CACHE_OVERRIDES} for {@code client} and {@code upgrade}
 * after config data (including Helm env vars resolved through {@code application.yaml}) is loaded.
 * The source is inserted first so it wins over those values.
 */
public class RuntimeRolePolicyPostProcessor implements EnvironmentPostProcessor, Ordered {

  static final String PROPERTY_SOURCE_NAME = "runtimeRolePolicy";

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeRolePolicyPostProcessor.class);

  @Override
  public void postProcessEnvironment(
      ConfigurableEnvironment environment, SpringApplication application) {
    RuntimeRole role = RuntimeRole.from(environment);
    if (!role.disablesDistributedCaches()) {
      LOG.info("Resolved datahub.runtime.role={}", role.wireName());
      return;
    }

    Map<String, Object> forced = new LinkedHashMap<>();
    for (RuntimeRole.ForcedProperty override : RuntimeRole.DISTRIBUTED_CACHE_OVERRIDES) {
      String previous = environment.getProperty(override.key());
      if (previous != null && !previous.trim().equalsIgnoreCase(override.value())) {
        LOG.warn(
            "datahub.runtime.role={} ignores {}={} (forced to {})",
            role.wireName(),
            override.key(),
            previous.trim(),
            override.value());
      }
      forced.put(override.key(), override.value());
    }

    MutablePropertySources sources = environment.getPropertySources();
    sources.addFirst(new MapPropertySource(PROPERTY_SOURCE_NAME, forced));
    LOG.info("Resolved datahub.runtime.role={}; distributed caches disabled", role.wireName());
  }

  @Override
  public int getOrder() {
    // After ConfigDataEnvironmentPostProcessor so application.yaml and env placeholders are
    // visible.
    return Ordered.LOWEST_PRECEDENCE;
  }
}
