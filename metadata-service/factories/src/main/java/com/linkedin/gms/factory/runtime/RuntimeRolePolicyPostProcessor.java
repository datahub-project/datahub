package com.linkedin.gms.factory.runtime;

import com.linkedin.metadata.config.runtime.RuntimeRole;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class RuntimeRolePolicyPostProcessor implements EnvironmentPostProcessor, Ordered {

  static final String PROPERTY_SOURCE_NAME = "runtimeRolePolicy";

  @Override
  public void postProcessEnvironment(
      ConfigurableEnvironment environment, SpringApplication application) {
    RuntimeRole role = RuntimeRole.from(environment);
    rejectUnlessService(role, gmsApplicationPresent());
    if (!role.disablesDistributedCaches()) {
      log.info("Resolved datahub.runtime.role={}", role.wireName());
      return;
    }

    Map<String, Object> forced = new LinkedHashMap<>();
    for (RuntimeRole.ForcedProperty override : RuntimeRole.DISTRIBUTED_CACHE_OVERRIDES) {
      String previous = environment.getProperty(override.key());
      if (previous != null && !previous.trim().equalsIgnoreCase(override.value())) {
        log.warn(
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
    log.info("Resolved datahub.runtime.role={}; distributed caches disabled", role.wireName());
  }

  /**
   * GMS hosts access-token revocation in Hazelcast. A non-service role there drops that map with no
   * other startup failure, so it is rejected. Standalone consumers do not load {@code
   * GMSApplication}.
   */
  static void rejectUnlessService(RuntimeRole role, boolean gmsApplicationPresent) {
    if (gmsApplicationPresent && role != RuntimeRole.SERVICE) {
      throw new IllegalStateException(
          "GMS accepts only datahub.runtime.role=service, but was '"
              + role.wireName()
              + "'. Unset DATAHUB_RUNTIME_ROLE.");
    }
  }

  private static boolean gmsApplicationPresent() {
    try {
      Class.forName("com.linkedin.gms.GMSApplication");
      return true;
    } catch (ClassNotFoundException | LinkageError e) {
      return false;
    }
  }

  @Override
  public int getOrder() {
    // After ConfigDataEnvironmentPostProcessor so application.yaml and env placeholders are
    // visible.
    return Ordered.LOWEST_PRECEDENCE;
  }
}
