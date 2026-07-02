package com.linkedin.gms.factory.ratelimit;

import com.hazelcast.core.HazelcastInstance;
import com.linkedin.metadata.config.GMSConfiguration;
import com.linkedin.metadata.config.ratelimit.RateLimitConfigValidator;
import com.linkedin.metadata.config.ratelimit.RateLimitProperties;
import com.linkedin.metadata.ratelimit.RateLimitEngine;
import com.linkedin.metadata.ratelimit.RateLimitFilter;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.utils.BasePathUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.micrometer.core.instrument.MeterRegistry;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

@Slf4j
@Configuration
// Loads the optional operator override (Tier 2); defaults live in application.yaml. Declared on the
// factory so it applies wherever the engine is built (GMS, datahub-upgrade).
// RATE_LIMITS_CONFIG_FILE
// is a Spring resource URI (e.g. file:/etc/datahub/rate-limits.yaml); when unset it resolves to the
// bundled empty rate-limit-config.yaml. ignoreResourceNotFound tolerates a set-but-missing path.
@PropertySource(
    name = "rateLimitConfigOverride",
    value = "${RATE_LIMITS_CONFIG_FILE:classpath:/rate-limit-config.yaml}",
    ignoreResourceNotFound = true,
    factory = YamlPropertySourceFactory.class)
public class RateLimitEngineFactory {

  static final String CONFIG_FILE_ENV = "RATE_LIMITS_CONFIG_FILE";

  @Bean
  @Nonnull
  public RateLimitEngine rateLimitEngine(
      GMSConfiguration gmsConfiguration,
      @Autowired(required = false) MeterRegistry meterRegistry,
      @Autowired(required = false) @Qualifier("hazelcastInstance")
          HazelcastInstance hazelcastInstance,
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      Environment environment) {
    // Spring binds the full config (application.yaml toggles + rate-limit-config.yaml policy, with
    // ${ENV} placeholders resolved and any mounted RATE_LIMITS_CONFIG_FILE overlaid). Validate the
    // bound bean up front so a bad config fails startup instead of silently mis-limiting traffic.
    warnIfConfigFileLikelyUnresolved(environment);
    RateLimitProperties config =
        gmsConfiguration.getRateLimits() != null
            ? gmsConfiguration.getRateLimits()
            : new RateLimitProperties();
    RateLimitConfigValidator.validate(config);
    String basePath =
        BasePathUtils.resolveBasePath(
            gmsConfiguration.getBasePathEnabled(), gmsConfiguration.getBasePath());
    return new RateLimitEngine(
        config,
        basePath,
        meterRegistry,
        hazelcastInstance,
        systemOperationContext.getObjectMapper());
  }

  @Bean
  @Nonnull
  public RateLimitFilter rateLimitFilter(RateLimitEngine rateLimitEngine) {
    return new RateLimitFilter(rateLimitEngine);
  }

  /**
   * RATE_LIMITS_CONFIG_FILE is consumed as a Spring resource URI by the @PropertySource overlay in
   * CommonApplicationConfig, which uses ignoreResourceNotFound — so a value without a resource
   * scheme (e.g. a bare {@code /etc/datahub/rate-limits.yaml} instead of {@code
   * file:/etc/datahub/rate-limits.yaml}) is silently skipped and the bundled defaults load instead.
   * Warn loudly so that misconfiguration isn't invisible (the old file loader logged on miss).
   */
  private static void warnIfConfigFileLikelyUnresolved(Environment environment) {
    String configFile = environment.getProperty(CONFIG_FILE_ENV);
    if (configFile == null || configFile.isBlank()) {
      return;
    }
    if (!configFile.startsWith("file:") && !configFile.startsWith("classpath:")) {
      log.warn(
          "{}='{}' has no Spring resource scheme; it will be ignored (overlay skipped) and the "
              + "bundled rate-limit defaults will load. Use a scheme, e.g. file:{}",
          CONFIG_FILE_ENV,
          configFile,
          configFile);
    }
  }
}
